//! Concrete implementations of Kernel-Defined Functions (KDFs).
//!
//! Each KDF has four functions:
//! - `*_create`: Create initial empty state
//! - `*_apply`: Apply the filter to a batch, returning mutated selection array
//! - `*_serialize`: Serialize state for distribution
//! - `*_deserialize`: Deserialize state on executor
//! - `*_free`: Free the state when done

use std::collections::HashSet;

use crate::arrow::array::BooleanArray;
use crate::log_replay::FileActionKey;
use crate::kernel_data::{serialize_remove_set, deserialize_remove_set};
use crate::{DeltaResult, EngineData, Error};

// =============================================================================
// AddRemoveDedup - Deduplicates add/remove file actions during commit log replay
// =============================================================================

/// State type for AddRemoveDedup: HashSet of seen file keys
type AddRemoveDedupState = HashSet<FileActionKey>;

/// Create initial empty state for AddRemoveDedup.
pub fn add_remove_dedup_create() -> DeltaResult<u64> {
    let state = Box::new(AddRemoveDedupState::new());
    Ok(Box::into_raw(state) as u64)
}

/// Apply AddRemoveDedup filter to a batch.
///
/// For each row where selection[i] is true:
/// - Extract file key (path, dv_unique_id)
/// - If already seen: set selection[i] = false (filter out)
/// - Else: add to seen set, keep selection[i] = true
///
/// Returns the mutated selection array with duplicates filtered out.
pub fn add_remove_dedup_apply(
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray> {
    use crate::arrow::array::{Array, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::AsAny;
    
    let state = unsafe { &mut *(state_ptr as *mut AddRemoveDedupState) };
    
    // Try to get the batch as ArrowEngineData
    let arrow_data = batch.any_ref()
        .downcast_ref::<ArrowEngineData>()
        .ok_or_else(|| Error::generic("Expected ArrowEngineData for KDF apply"))?;
    
    let record_batch = arrow_data.record_batch();
    let num_rows = record_batch.num_rows();
    
    // Get the path column - try "path" or "add.path"
    let path_col = record_batch.column_by_name("path")
        .or_else(|| record_batch.column_by_name("add.path"));
    
    let path_array = match path_col {
        Some(col) => col.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| Error::generic("path column is not a string array"))?,
        None => {
            // No path column - return selection unchanged
            return Ok(selection);
        }
    };
    
    // Get optional deletion vector column for unique ID
    let dv_col = record_batch.column_by_name("deletionVector")
        .or_else(|| record_batch.column_by_name("add.deletionVector"));
    
    let dv_array = dv_col.and_then(|col| col.as_any().downcast_ref::<StringArray>());
    
    // Build new selection by checking each row
    let mut result: Vec<bool> = Vec::with_capacity(num_rows);
    
    for i in 0..num_rows {
        // If already filtered out, keep it filtered
        if !selection.value(i) {
            result.push(false);
            continue;
        }
        
        // Get the path
        let path = if path_array.is_null(i) {
            result.push(false);
            continue;
        } else {
            path_array.value(i).to_string()
        };
        
        // Get optional deletion vector unique ID
        let dv_unique_id = dv_array.and_then(|arr| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            }
        });
        
        // Create the file action key
        let key = FileActionKey { path, dv_unique_id };
        
        // Check if we've seen this key before
        if state.contains(&key) {
            // Duplicate - filter out
            result.push(false);
        } else {
            // New - add to seen set and keep
            state.insert(key);
            result.push(true);
        }
    }
    
    Ok(BooleanArray::from(result))
}

/// Serialize AddRemoveDedup state for distribution.
pub fn add_remove_dedup_serialize(state_ptr: u64) -> DeltaResult<Vec<u8>> {
    let state = unsafe { &*(state_ptr as *const AddRemoveDedupState) };
    let kernel_data = serialize_remove_set(state)?;
    let arrow_batch = kernel_data.to_arrow()?;
    
    // Serialize to Arrow IPC format
    let mut buffer = Vec::new();
    {
        use crate::arrow::ipc::writer::StreamWriter;
        let mut writer = StreamWriter::try_new(&mut buffer, &arrow_batch.schema())
            .map_err(|e| Error::generic(format!("Failed to create IPC writer: {}", e)))?;
        writer.write(&arrow_batch)
            .map_err(|e| Error::generic(format!("Failed to write batch: {}", e)))?;
        writer.finish()
            .map_err(|e| Error::generic(format!("Failed to finish IPC stream: {}", e)))?;
    }
    
    Ok(buffer)
}

/// Deserialize AddRemoveDedup state on executor.
pub fn add_remove_dedup_deserialize(bytes: &[u8]) -> DeltaResult<u64> {
    use crate::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    
    // Deserialize from Arrow IPC format
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::generic(format!("Failed to create IPC reader: {}", e)))?;
    
    let batch = reader.next()
        .ok_or_else(|| Error::generic("No batch in IPC stream"))?
        .map_err(|e| Error::generic(format!("Failed to read batch: {}", e)))?;
    
    let state = deserialize_remove_set(&batch)?;
    Ok(Box::into_raw(Box::new(state)) as u64)
}

/// Free AddRemoveDedup state.
pub fn add_remove_dedup_free(state_ptr: u64) {
    if state_ptr != 0 {
        unsafe { drop(Box::from_raw(state_ptr as *mut AddRemoveDedupState)) };
    }
}

// =============================================================================
// CheckpointDedup - Deduplicates file actions when reading checkpoint files
// =============================================================================

/// State type for CheckpointDedup: same as AddRemoveDedup
type CheckpointDedupState = HashSet<FileActionKey>;

/// Create initial empty state for CheckpointDedup.
pub fn checkpoint_dedup_create() -> DeltaResult<u64> {
    let state = Box::new(CheckpointDedupState::new());
    Ok(Box::into_raw(state) as u64)
}

/// Apply CheckpointDedup filter to a batch.
///
/// Returns the mutated selection array with duplicates filtered out.
pub fn checkpoint_dedup_apply(
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray> {
    let state = unsafe { &mut *(state_ptr as *mut CheckpointDedupState) };
    
    // TODO: Implement checkpoint dedup logic
    // Similar to AddRemoveDedup but for checkpoint files
    let _ = (state, batch);
    
    // Return selection unchanged for now (placeholder)
    Ok(selection)
}

/// Serialize CheckpointDedup state for distribution.
pub fn checkpoint_dedup_serialize(state_ptr: u64) -> DeltaResult<Vec<u8>> {
    // Same serialization as AddRemoveDedup
    add_remove_dedup_serialize(state_ptr)
}

/// Deserialize CheckpointDedup state on executor.
pub fn checkpoint_dedup_deserialize(bytes: &[u8]) -> DeltaResult<u64> {
    // Same deserialization as AddRemoveDedup
    add_remove_dedup_deserialize(bytes)
}

/// Free CheckpointDedup state.
pub fn checkpoint_dedup_free(state_ptr: u64) {
    if state_ptr != 0 {
        unsafe { drop(Box::from_raw(state_ptr as *mut CheckpointDedupState)) };
    }
}

// =============================================================================
// StatsSkipping - Filters files based on min/max statistics
// =============================================================================

/// State type for StatsSkipping
/// This is a placeholder - real implementation would contain parsed predicate info
#[derive(Default)]
pub struct StatsSkippingState {
    // TODO: Add fields for predicate information
}

/// Create initial state for StatsSkipping.
pub fn stats_skipping_create() -> DeltaResult<u64> {
    let state = Box::new(StatsSkippingState::default());
    Ok(Box::into_raw(state) as u64)
}

/// Apply StatsSkipping filter to a batch.
///
/// Returns the mutated selection array with non-matching files filtered out.
pub fn stats_skipping_apply(
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray> {
    let state = unsafe { &*(state_ptr as *const StatsSkippingState) };
    
    // TODO: Implement stats skipping logic
    // Would compare file statistics against predicate
    let _ = (state, batch);
    
    // Return selection unchanged for now (placeholder)
    Ok(selection)
}

/// Serialize StatsSkipping state for distribution.
pub fn stats_skipping_serialize(state_ptr: u64) -> DeltaResult<Vec<u8>> {
    let _state = unsafe { &*(state_ptr as *const StatsSkippingState) };
    
    // TODO: Implement proper serialization
    // For now, return empty bytes as placeholder
    Ok(Vec::new())
}

/// Deserialize StatsSkipping state on executor.
pub fn stats_skipping_deserialize(bytes: &[u8]) -> DeltaResult<u64> {
    let _ = bytes;
    
    // TODO: Implement proper deserialization
    stats_skipping_create()
}

/// Free StatsSkipping state.
pub fn stats_skipping_free(state_ptr: u64) {
    if state_ptr != 0 {
        unsafe { drop(Box::from_raw(state_ptr as *mut StatsSkippingState)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::engine::arrow_data::ArrowEngineData;
    use std::sync::Arc;

    /// Create a test batch with file paths
    fn create_test_batch(paths: &[&str]) -> ArrowEngineData {
        let schema = Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
        ]);
        let path_array = StringArray::from(paths.to_vec());
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(path_array)],
        ).unwrap();
        ArrowEngineData::new(batch)
    }

    /// Create a selection vector with all true
    fn create_selection(len: usize) -> BooleanArray {
        BooleanArray::from(vec![true; len])
    }

    #[test]
    fn test_add_remove_dedup_create_and_free() {
        let state_ptr = add_remove_dedup_create().unwrap();
        assert_ne!(state_ptr, 0);
        add_remove_dedup_free(state_ptr);
    }

    #[test]
    fn test_add_remove_dedup_filters_duplicates() {
        // Create state
        let state_ptr = add_remove_dedup_create().unwrap();
        
        // Batch 1: file1, file2 (both new)
        let batch1 = create_test_batch(&["file1.parquet", "file2.parquet"]);
        let selection1 = create_selection(2);
        
        let result1 = add_remove_dedup_apply(state_ptr, &batch1, selection1).unwrap();
        
        // Both should be selected (first time seeing them)
        assert!(result1.value(0), "file1 should be selected (first add)");
        assert!(result1.value(1), "file2 should be selected (first add)");
        
        // Batch 2: file1 (duplicate), file3 (new)
        let batch2 = create_test_batch(&["file1.parquet", "file3.parquet"]);
        let selection2 = create_selection(2);
        
        let result2 = add_remove_dedup_apply(state_ptr, &batch2, selection2).unwrap();
        
        // file1 is duplicate, file3 is new
        assert!(!result2.value(0), "file1 should be filtered (duplicate)");
        assert!(result2.value(1), "file3 should be selected (first add)");
        
        // Batch 3: file2 (duplicate), file3 (duplicate), file4 (new)
        let batch3 = create_test_batch(&["file2.parquet", "file3.parquet", "file4.parquet"]);
        let selection3 = create_selection(3);
        
        let result3 = add_remove_dedup_apply(state_ptr, &batch3, selection3).unwrap();
        
        assert!(!result3.value(0), "file2 should be filtered (duplicate)");
        assert!(!result3.value(1), "file3 should be filtered (duplicate)");
        assert!(result3.value(2), "file4 should be selected (first add)");
        
        add_remove_dedup_free(state_ptr);
    }

    #[test]
    fn test_add_remove_dedup_respects_existing_selection() {
        let state_ptr = add_remove_dedup_create().unwrap();
        
        // Batch with 3 files, but middle one already filtered out
        let batch = create_test_batch(&["file1.parquet", "file2.parquet", "file3.parquet"]);
        let selection = BooleanArray::from(vec![true, false, true]);
        
        let result = add_remove_dedup_apply(state_ptr, &batch, selection).unwrap();
        
        // file1: new, should be selected
        assert!(result.value(0), "file1 should be selected");
        // file2: was already filtered, stays filtered
        assert!(!result.value(1), "file2 should stay filtered");
        // file3: new, should be selected
        assert!(result.value(2), "file3 should be selected");
        
        // Verify file2 was NOT added to state (since it was filtered)
        // by checking if it appears as new in next batch
        let batch2 = create_test_batch(&["file2.parquet"]);
        let selection2 = create_selection(1);
        
        let result2 = add_remove_dedup_apply(state_ptr, &batch2, selection2).unwrap();
        
        // file2 should be selected because it wasn't seen before (was filtered)
        assert!(result2.value(0), "file2 should be selected (was filtered before, never seen)");
        
        add_remove_dedup_free(state_ptr);
    }

    #[test]
    fn test_add_remove_dedup_serialize_roundtrip_with_data() {
        // Create state and add some files
        let state_ptr = add_remove_dedup_create().unwrap();
        
        let batch = create_test_batch(&["file1.parquet", "file2.parquet"]);
        let selection = create_selection(2);
        let _ = add_remove_dedup_apply(state_ptr, &batch, selection).unwrap();
        
        // Serialize state
        let bytes = add_remove_dedup_serialize(state_ptr).unwrap();
        assert!(!bytes.is_empty());
        
        // Deserialize on "executor"
        let executor_state = add_remove_dedup_deserialize(&bytes).unwrap();
        
        // Now on executor, file1 and file2 should be recognized as duplicates
        let batch2 = create_test_batch(&["file1.parquet", "file3.parquet"]);
        let selection2 = create_selection(2);
        
        let result = add_remove_dedup_apply(executor_state, &batch2, selection2).unwrap();
        
        // file1 was seen on driver, should be filtered
        assert!(!result.value(0), "file1 should be filtered (seen on driver)");
        // file3 is new
        assert!(result.value(1), "file3 should be selected (new)");
        
        add_remove_dedup_free(state_ptr);
        add_remove_dedup_free(executor_state);
    }

    #[test]
    fn test_checkpoint_dedup_create_and_free() {
        let state_ptr = checkpoint_dedup_create().unwrap();
        assert_ne!(state_ptr, 0);
        checkpoint_dedup_free(state_ptr);
    }

    #[test]
    fn test_stats_skipping_create_and_free() {
        let state_ptr = stats_skipping_create().unwrap();
        assert_ne!(state_ptr, 0);
        stats_skipping_free(state_ptr);
    }

    #[test]
    fn test_add_remove_dedup_serialize_empty() {
        let state_ptr = add_remove_dedup_create().unwrap();
        let bytes = add_remove_dedup_serialize(state_ptr).unwrap();
        assert!(!bytes.is_empty()); // Should have IPC header even if empty
        
        let new_state_ptr = add_remove_dedup_deserialize(&bytes).unwrap();
        assert_ne!(new_state_ptr, 0);
        
        add_remove_dedup_free(state_ptr);
        add_remove_dedup_free(new_state_ptr);
    }
}

