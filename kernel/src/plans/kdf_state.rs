//! Strongly typed KDF state - enum of structs approach.
//!
//! The enum variant IS the function identity - no separate function_id field.
//! State is always created in Rust; Java holds only opaque u64 handles.
//!
//! # Design Principles
//!
//! 1. Each state type has its own struct with monomorphized apply method
//! 2. The wrapper enum provides storage and FFI boundary conversion
//! 3. One match per FFI call, then monomorphized execution (no per-tuple dispatch)
//! 4. u64 conversion only happens at FFI boundaries

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::array::BooleanArray;
use crate::log_replay::FileActionKey;
use crate::schema::SchemaRef;
use crate::{DeltaResult, EngineData, Error};

// =============================================================================
// Serialization Helpers for HashSet<FileActionKey>
// =============================================================================

/// Serialize a HashSet of FileActionKeys to Arrow IPC bytes.
///
/// Converts the set to two parallel StringArray columns (path, dv_unique_id)
/// and writes as an Arrow IPC stream.
fn serialize_file_action_keys(keys: &HashSet<FileActionKey>) -> DeltaResult<Vec<u8>> {
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::arrow::ipc::writer::StreamWriter;

    // Collect paths and dv_unique_ids from the HashSet
    let paths: Vec<&str> = keys.iter().map(|k| k.path.as_str()).collect();
    let dv_ids: Vec<Option<&str>> = keys.iter().map(|k| k.dv_unique_id.as_deref()).collect();

    // Create Arrow arrays
    let path_array = StringArray::from(paths);
    let dv_id_array = StringArray::from(dv_ids);

    // Create schema and RecordBatch
    let schema = Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("dv_unique_id", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(path_array), Arc::new(dv_id_array)],
    )
    .map_err(|e| Error::generic(format!("Failed to create RecordBatch: {}", e)))?;

    // Write to IPC stream
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| Error::generic(format!("Failed to create IPC writer: {}", e)))?;
        writer
            .write(&batch)
            .map_err(|e| Error::generic(format!("Failed to write batch: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::generic(format!("Failed to finish IPC stream: {}", e)))?;
    }

    Ok(buffer)
}

/// Deserialize a HashSet of FileActionKeys from Arrow IPC bytes.
///
/// Reads an Arrow IPC stream and reconstructs the HashSet from the
/// path and dv_unique_id columns.
fn deserialize_file_action_keys(bytes: &[u8]) -> DeltaResult<HashSet<FileActionKey>> {
    use crate::arrow::array::{Array, StringArray};
    use crate::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::generic(format!("Failed to create IPC reader: {}", e)))?;

    let batch = reader
        .next()
        .ok_or_else(|| Error::generic("No batch in IPC stream"))?
        .map_err(|e| Error::generic(format!("Failed to read batch: {}", e)))?;

    // Extract columns
    let path_col = batch
        .column_by_name("path")
        .ok_or_else(|| Error::generic("Missing 'path' column in serialized state"))?;
    let path_array = path_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::generic("'path' column is not a StringArray"))?;

    let dv_id_col = batch
        .column_by_name("dv_unique_id")
        .ok_or_else(|| Error::generic("Missing 'dv_unique_id' column in serialized state"))?;
    let dv_id_array = dv_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::generic("'dv_unique_id' column is not a StringArray"))?;

    // Reconstruct HashSet
    let mut keys = HashSet::new();
    for i in 0..batch.num_rows() {
        let path = path_array.value(i).to_string();
        let dv_unique_id = if dv_id_array.is_null(i) {
            None
        } else {
            Some(dv_id_array.value(i).to_string())
        };
        keys.insert(FileActionKey { path, dv_unique_id });
    }

    Ok(keys)
}

// =============================================================================
// AddRemoveDedupState - Deduplicates add/remove file actions during commit replay
// =============================================================================

/// State for AddRemoveDedup KDF - deduplicates add/remove actions during commit log replay.
///
/// Tracks seen file keys (path + optional deletion vector unique ID) and filters out
/// duplicates. State is mutable and accumulates across batches.
#[derive(Debug, Clone, Default)]
pub struct AddRemoveDedupState {
    seen_keys: HashSet<FileActionKey>,
}

impl AddRemoveDedupState {
    /// Create new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply dedup filter to a batch - monomorphized, no dispatch overhead.
    ///
    /// For each row where selection[i] is true:
    /// - Extract file key (path, dv_unique_id)
    /// - If already seen: set selection[i] = false (filter out)
    /// - Else: add to seen set, keep selection[i] = true
    #[inline]
    pub fn apply(
        &mut self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        use crate::arrow::array::{Array, StringArray};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::AsAny;

        // Try to get the batch as ArrowEngineData
        let arrow_data = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::generic("Expected ArrowEngineData for KDF apply"))?;

        let record_batch = arrow_data.record_batch();
        let num_rows = record_batch.num_rows();

        // Get the path column - try "path" or "add.path"
        let path_col = record_batch
            .column_by_name("path")
            .or_else(|| record_batch.column_by_name("add.path"));

        let path_array = match path_col {
            Some(col) => col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::generic("path column is not a string array"))?,
            None => {
                // No path column - return selection unchanged
                return Ok(selection);
            }
        };

        // Get optional deletion vector column for unique ID
        let dv_col = record_batch
            .column_by_name("deletionVector")
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
            if self.seen_keys.contains(&key) {
                // Duplicate - filter out
                result.push(false);
            } else {
                // New - add to seen set and keep
                self.seen_keys.insert(key);
                result.push(true);
            }
        }

        Ok(BooleanArray::from(result))
    }

    /// Serialize state for distribution to executors.
    pub fn serialize(&self) -> DeltaResult<Vec<u8>> {
        serialize_file_action_keys(&self.seen_keys)
    }

    /// Deserialize state from bytes (received from driver).
    pub fn deserialize(bytes: &[u8]) -> DeltaResult<Self> {
        let seen_keys = deserialize_file_action_keys(bytes)?;
        Ok(Self { seen_keys })
    }

    /// Check if a key has been seen.
    pub fn contains(&self, key: &FileActionKey) -> bool {
        self.seen_keys.contains(key)
    }

    /// Insert a key into the seen set.
    pub fn insert(&mut self, key: FileActionKey) -> bool {
        self.seen_keys.insert(key)
    }

    /// Get the number of seen keys.
    pub fn len(&self) -> usize {
        self.seen_keys.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.seen_keys.is_empty()
    }
}

// =============================================================================
// CheckpointDedupState - Deduplicates file actions when reading checkpoint files
// =============================================================================

/// State for CheckpointDedup KDF - filters out file actions already seen during commit phase.
///
/// Unlike `AddRemoveDedupState`, this is an **immutable** state that only probes
/// the HashSet without modifying it. This design enables:
/// - **Thread-safe parallel execution**: Multiple threads can probe the same state concurrently
/// - **Distribution to executors**: State is serialized once by the driver and deserialized
///   by each executor, which then probes it read-only
///
/// # Usage Pattern
///
/// 1. Driver builds `AddRemoveDedupState` during commit phase (mutable, accumulates keys)
/// 2. Driver serializes the accumulated keys and distributes to executors
/// 3. Each executor deserializes into `CheckpointDedupState` (immutable)
/// 4. Executors probe the state to filter checkpoint files in parallel
#[derive(Debug, Clone, Default)]
pub struct CheckpointDedupState {
    seen_keys: HashSet<FileActionKey>,
}

impl CheckpointDedupState {
    /// Create new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create state from an existing HashSet of file action keys.
    ///
    /// This is the primary constructor for checkpoint dedup, typically called
    /// after deserializing keys that were accumulated during the commit phase.
    pub fn from_hashset(seen_keys: HashSet<FileActionKey>) -> Self {
        Self { seen_keys }
    }

    /// Apply dedup filter to a batch - **read-only**, probes without mutation.
    ///
    /// For each row where `selection[i]` is true:
    /// - Extract file key (path, dv_unique_id) from the batch
    /// - If key exists in `seen_keys`: set `selection[i] = false` (filter out)
    /// - Else: keep `selection[i] = true` (keep the row)
    ///
    /// This method takes `&self` (not `&mut self`) because it only probes the
    /// HashSet. This enables safe concurrent access from multiple threads.
    #[inline]
    pub fn apply(
        &self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        use crate::arrow::array::{Array, StringArray};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::AsAny;

        // Try to get the batch as ArrowEngineData
        let arrow_data = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::generic("Expected ArrowEngineData for KDF apply"))?;

        let record_batch = arrow_data.record_batch();
        let num_rows = record_batch.num_rows();

        // Get the path column - try "path" or "add.path"
        let path_col = record_batch
            .column_by_name("path")
            .or_else(|| record_batch.column_by_name("add.path"));

        let path_array = match path_col {
            Some(col) => col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::generic("path column is not a string array"))?,
            None => {
                // No path column - return selection unchanged
                return Ok(selection);
            }
        };

        // Get optional deletion vector column for unique ID
        let dv_col = record_batch
            .column_by_name("deletionVector")
            .or_else(|| record_batch.column_by_name("add.deletionVector"));

        let dv_array = dv_col.and_then(|col| col.as_any().downcast_ref::<StringArray>());

        // Build new selection by checking each row (read-only probe)
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

            // Probe only - DO NOT insert (read-only access)
            if self.seen_keys.contains(&key) {
                // Already seen during commit phase - filter out
                result.push(false);
            } else {
                // Not seen - keep the row
                result.push(true);
            }
        }

        Ok(BooleanArray::from(result))
    }

    /// Serialize state for distribution to executors.
    ///
    /// Converts the HashSet to Arrow IPC bytes for efficient transfer.
    pub fn serialize(&self) -> DeltaResult<Vec<u8>> {
        serialize_file_action_keys(&self.seen_keys)
    }

    /// Deserialize state from bytes (received from driver).
    ///
    /// Reconstructs the HashSet from Arrow IPC bytes.
    pub fn deserialize(bytes: &[u8]) -> DeltaResult<Self> {
        let seen_keys = deserialize_file_action_keys(bytes)?;
        Ok(Self { seen_keys })
    }

    /// Check if a key has been seen (read-only probe).
    pub fn contains(&self, key: &FileActionKey) -> bool {
        self.seen_keys.contains(key)
    }

    /// Get the number of seen keys.
    pub fn len(&self) -> usize {
        self.seen_keys.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.seen_keys.is_empty()
    }
}

// =============================================================================
// SchemaStoreState - Stores a schema reference for later retrieval
// =============================================================================

/// State for SchemaStore - stores a schema reference for later retrieval.
///
/// Used for schema query operations where we need to capture schema from
/// parquet file footers.
#[derive(Debug, Clone, Default)]
pub struct SchemaStoreState {
    schema: Option<SchemaRef>,
}

impl SchemaStoreState {
    /// Create new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a schema.
    pub fn store(&mut self, schema: SchemaRef) {
        self.schema = Some(schema);
    }

    /// Get the stored schema, if any.
    pub fn get(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Take the stored schema, leaving None.
    pub fn take(&mut self) -> Option<SchemaRef> {
        self.schema.take()
    }
}

// =============================================================================
// FilterKdfState - Wrapper enum (variant IS the function identity)
// =============================================================================

/// Filter KDF state - the enum variant IS the function identity.
///
/// No separate `function_id` field is needed. The variant encodes which
/// filter function to apply, and contains the typed state for that function.
///
/// # FFI Boundary
///
/// This enum is converted to/from u64 only at FFI boundaries:
/// - `into_raw()` when sending plan to Java
/// - `borrow_mut_from_raw()` when Java calls kdf_apply
/// - `from_raw()` when receiving executed plan back from Java
#[derive(Debug, Clone)]
pub enum FilterKdfState {
    /// Deduplicates add/remove file actions during commit log replay
    AddRemoveDedup(AddRemoveDedupState),
    /// Deduplicates file actions when reading checkpoint files
    CheckpointDedup(CheckpointDedupState),
}

impl FilterKdfState {
    /// Apply the filter - ONE match here, then monomorphized execution.
    ///
    /// Each branch calls the concrete state's apply method, which is inlined
    /// and monomorphized. This avoids per-tuple dispatch overhead.
    ///
    /// Note: Takes `&mut self` for `AddRemoveDedup` which mutates state.
    /// `CheckpointDedup` only probes (read-only) but is called through this
    /// unified interface for consistency.
    #[inline]
    pub fn apply(
        &mut self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        match self {
            Self::AddRemoveDedup(state) => state.apply(batch, selection),
            Self::CheckpointDedup(state) => state.apply(batch, selection),
        }
    }

    /// Serialize state for distribution.
    pub fn serialize(&self) -> DeltaResult<Vec<u8>> {
        match self {
            Self::AddRemoveDedup(state) => state.serialize(),
            Self::CheckpointDedup(state) => state.serialize(),
        }
    }

    // =========================================================================
    // FFI Boundary Methods - the ONLY places where u64 is used
    // =========================================================================

    /// Convert to raw pointer for FFI (transfers ownership to the pointer).
    ///
    /// Called when serializing a plan to proto for Java.
    pub fn into_raw(self) -> u64 {
        Box::into_raw(Box::new(self)) as u64
    }

    /// Borrow from raw pointer without taking ownership.
    ///
    /// Called when Java invokes kdf_apply - the state stays alive,
    /// Java just holds the pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been created by `into_raw()`
    /// - `ptr` must still be valid (not freed)
    /// - The returned reference must not outlive the pointer's validity
    pub unsafe fn borrow_mut_from_raw<'a>(ptr: u64) -> &'a mut Self {
        &mut *(ptr as *mut Self)
    }

    /// Borrow immutably from raw pointer.
    ///
    /// # Safety
    ///
    /// Same requirements as `borrow_mut_from_raw`.
    pub unsafe fn borrow_from_raw<'a>(ptr: u64) -> &'a Self {
        &*(ptr as *const Self)
    }

    /// Reconstruct from raw pointer (takes ownership back).
    ///
    /// Called when receiving an executed plan back from Java.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been created by `into_raw()`
    /// - `ptr` must still be valid (not freed)
    /// - After this call, `ptr` is invalid and must not be used
    pub unsafe fn from_raw(ptr: u64) -> Self {
        *Box::from_raw(ptr as *mut Self)
    }

    /// Free a raw pointer.
    ///
    /// Called when state is no longer needed.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been created by `into_raw()`
    /// - `ptr` must not have been freed already
    pub unsafe fn free_raw(ptr: u64) {
        if ptr != 0 {
            drop(Box::from_raw(ptr as *mut Self));
        }
    }
}

// =============================================================================
// SchemaReaderState - Wrapper enum for schema reader KDFs
// =============================================================================

/// Schema reader KDF state - the enum variant IS the function identity.
#[derive(Debug, Clone)]
pub enum SchemaReaderState {
    /// Stores a schema reference for later retrieval
    SchemaStore(SchemaStoreState),
}

impl SchemaReaderState {
    /// Convert to raw pointer for FFI.
    pub fn into_raw(self) -> u64 {
        Box::into_raw(Box::new(self)) as u64
    }

    /// Borrow from raw pointer.
    ///
    /// # Safety
    ///
    /// Same requirements as `FilterKdfState::borrow_mut_from_raw`.
    pub unsafe fn borrow_mut_from_raw<'a>(ptr: u64) -> &'a mut Self {
        &mut *(ptr as *mut Self)
    }

    /// Reconstruct from raw pointer.
    ///
    /// # Safety
    ///
    /// Same requirements as `FilterKdfState::from_raw`.
    pub unsafe fn from_raw(ptr: u64) -> Self {
        *Box::from_raw(ptr as *mut Self)
    }

    /// Free a raw pointer.
    ///
    /// # Safety
    ///
    /// Same requirements as `FilterKdfState::free_raw`.
    pub unsafe fn free_raw(ptr: u64) {
        if ptr != 0 {
            drop(Box::from_raw(ptr as *mut Self));
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::engine::arrow_data::ArrowEngineData;
    use std::sync::Arc;

    fn create_test_batch(paths: &[&str]) -> ArrowEngineData {
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let path_array = StringArray::from(paths.to_vec());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(path_array)]).unwrap();
        ArrowEngineData::new(batch)
    }

    fn create_selection(len: usize) -> BooleanArray {
        BooleanArray::from(vec![true; len])
    }

    #[test]
    fn test_add_remove_dedup_state_new() {
        let state = AddRemoveDedupState::new();
        assert!(state.is_empty());
    }

    #[test]
    fn test_add_remove_dedup_state_apply() {
        let mut state = AddRemoveDedupState::new();

        // Batch 1: file1, file2 (both new)
        let batch1 = create_test_batch(&["file1.parquet", "file2.parquet"]);
        let selection1 = create_selection(2);

        let result1 = state.apply(&batch1, selection1).unwrap();

        assert!(result1.value(0), "file1 should be selected");
        assert!(result1.value(1), "file2 should be selected");
        assert_eq!(state.len(), 2);

        // Batch 2: file1 (duplicate), file3 (new)
        let batch2 = create_test_batch(&["file1.parquet", "file3.parquet"]);
        let selection2 = create_selection(2);

        let result2 = state.apply(&batch2, selection2).unwrap();

        assert!(!result2.value(0), "file1 should be filtered (duplicate)");
        assert!(result2.value(1), "file3 should be selected");
        assert_eq!(state.len(), 3);
    }

    #[test]
    fn test_add_remove_dedup_state_serialize_deserialize() {
        let mut state = AddRemoveDedupState::new();
        state.insert(FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        });
        state.insert(FileActionKey {
            path: "file2.parquet".to_string(),
            dv_unique_id: Some("dv123".to_string()),
        });

        // Serialize
        let bytes = state.serialize().unwrap();
        assert!(!bytes.is_empty());

        // Deserialize
        let restored = AddRemoveDedupState::deserialize(&bytes).unwrap();

        // Verify same contents
        assert_eq!(state.len(), restored.len());
        assert!(restored.contains(&FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        }));
        assert!(restored.contains(&FileActionKey {
            path: "file2.parquet".to_string(),
            dv_unique_id: Some("dv123".to_string()),
        }));
    }

    #[test]
    fn test_filter_kdf_state_enum_dispatch() {
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());

        let batch = create_test_batch(&["file1.parquet"]);
        let selection = create_selection(1);

        let result = state.apply(&batch, selection).unwrap();
        assert!(result.value(0));
    }

    #[test]
    fn test_filter_kdf_state_raw_pointer_roundtrip() {
        let state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());

        // Convert to raw
        let ptr = state.into_raw();
        assert_ne!(ptr, 0);

        // Borrow and mutate
        unsafe {
            let borrowed = FilterKdfState::borrow_mut_from_raw(ptr);
            let batch = create_test_batch(&["file1.parquet"]);
            let selection = create_selection(1);
            let _ = borrowed.apply(&batch, selection).unwrap();
        }

        // Reconstruct
        let reconstructed = unsafe { FilterKdfState::from_raw(ptr) };
        match reconstructed {
            FilterKdfState::AddRemoveDedup(s) => {
                assert_eq!(s.len(), 1, "State should have been mutated");
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_schema_store_state() {
        use crate::schema::{DataType, StructField, StructType};

        let mut state = SchemaStoreState::new();
        assert!(state.get().is_none());

        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::LONG,
        )]));

        state.store(schema.clone());
        assert!(state.get().is_some());
        assert_eq!(state.get().unwrap().fields().len(), 1);
    }

    // =========================================================================
    // CheckpointDedupState Tests
    // =========================================================================

    #[test]
    fn test_checkpoint_dedup_state_new() {
        let state = CheckpointDedupState::new();
        assert!(state.is_empty());
    }

    #[test]
    fn test_checkpoint_dedup_state_from_hashset() {
        let mut keys = HashSet::new();
        keys.insert(FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        });
        keys.insert(FileActionKey {
            path: "file2.parquet".to_string(),
            dv_unique_id: Some("dv1".to_string()),
        });

        let state = CheckpointDedupState::from_hashset(keys);
        assert_eq!(state.len(), 2);
        assert!(state.contains(&FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        }));
        assert!(state.contains(&FileActionKey {
            path: "file2.parquet".to_string(),
            dv_unique_id: Some("dv1".to_string()),
        }));
    }

    #[test]
    fn test_checkpoint_dedup_state_apply_readonly() {
        // Create state with pre-populated keys (simulating commit phase results)
        let mut keys = HashSet::new();
        keys.insert(FileActionKey {
            path: "seen_file.parquet".to_string(),
            dv_unique_id: None,
        });
        let state = CheckpointDedupState::from_hashset(keys);

        // Batch with seen and unseen files
        let batch = create_test_batch(&["seen_file.parquet", "new_file.parquet"]);
        let selection = create_selection(2);

        // Apply should filter out seen file, keep unseen file
        let result = state.apply(&batch, selection).unwrap();

        assert!(!result.value(0), "seen_file should be filtered out");
        assert!(result.value(1), "new_file should be kept");

        // State should NOT have changed (read-only)
        assert_eq!(state.len(), 1, "State should not have grown");
        assert!(
            !state.contains(&FileActionKey {
                path: "new_file.parquet".to_string(),
                dv_unique_id: None,
            }),
            "new_file should NOT have been added to state"
        );
    }

    #[test]
    fn test_checkpoint_dedup_state_serialize_deserialize() {
        // Create state with keys
        let mut keys = HashSet::new();
        keys.insert(FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        });
        keys.insert(FileActionKey {
            path: "file2.parquet".to_string(),
            dv_unique_id: Some("dv_abc".to_string()),
        });
        let original = CheckpointDedupState::from_hashset(keys);

        // Serialize
        let bytes = original.serialize().unwrap();
        assert!(!bytes.is_empty());

        // Deserialize
        let restored = CheckpointDedupState::deserialize(&bytes).unwrap();

        // Verify same contents
        assert_eq!(original.len(), restored.len());
        assert!(restored.contains(&FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        }));
        assert!(restored.contains(&FileActionKey {
            path: "file2.parquet".to_string(),
            dv_unique_id: Some("dv_abc".to_string()),
        }));
    }

    #[test]
    fn test_checkpoint_dedup_state_thread_safe() {
        use std::thread;

        // Create shared state
        let mut keys = HashSet::new();
        keys.insert(FileActionKey {
            path: "file1.parquet".to_string(),
            dv_unique_id: None,
        });
        let state = Arc::new(CheckpointDedupState::from_hashset(keys));

        // Spawn multiple threads that probe the state concurrently
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let state_clone = Arc::clone(&state);
                thread::spawn(move || {
                    // Each thread probes the state
                    assert!(state_clone.contains(&FileActionKey {
                        path: "file1.parquet".to_string(),
                        dv_unique_id: None,
                    }));
                    assert!(!state_clone.contains(&FileActionKey {
                        path: "nonexistent.parquet".to_string(),
                        dv_unique_id: None,
                    }));
                })
            })
            .collect();

        // All threads should complete successfully
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
