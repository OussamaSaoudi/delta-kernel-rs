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

use url::Url;

use crate::arrow::array::BooleanArray;
use crate::log_replay::FileActionKey;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::{DeltaResult, EngineData, Error, FileMeta, Version};

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
// LogSegmentBuilderState - Builds LogSegment from file listing results
// =============================================================================

/// State for LogSegmentBuilder consumer KDF - builds a LogSegment from file listing.
///
/// This is a **Consumer KDF** that processes batches of file metadata and accumulates
/// them into the components needed to construct a `ListedLogFiles` / `LogSegment`.
///
/// Unlike Filter KDFs that return per-row selection vectors, Consumer KDFs:
/// - Return a single `bool` for the entire batch: `true` = Continue, `false` = Break
/// - Accumulate state across batches
/// - May store errors to be surfaced during `advance()`
///
/// # Usage Pattern
///
/// 1. Create state with target `log_root` and optional `end_version`
/// 2. Feed batches of file metadata through `apply()`
/// 3. `apply()` returns `Ok(true)` to continue, `Ok(false)` to stop
/// 4. If an error occurs, it's stored in `error` and `apply()` returns `Ok(false)`
/// 5. After completion, extract `ListedLogFiles` via `into_listed_files()`
#[derive(Debug, Clone)]
pub struct LogSegmentBuilderState {
    /// Log directory root URL
    pub log_root: Url,
    /// Optional end version to stop at
    pub end_version: Option<Version>,
    /// Sorted commit files in the log segment (ascending)
    pub ascending_commit_files: Vec<ParsedLogPath>,
    /// Sorted compaction files in the log segment (ascending)
    pub ascending_compaction_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment
    pub checkpoint_parts: Vec<ParsedLogPath>,
    /// Latest CRC (checksum) file
    pub latest_crc_file: Option<ParsedLogPath>,
    /// Latest commit file (may not be in contiguous segment)
    pub latest_commit_file: Option<ParsedLogPath>,
    /// Stored error to surface during advance()
    pub error: Option<String>,
    /// Current group version for checkpoint grouping
    current_group_version: Option<Version>,
    /// New checkpoint parts being accumulated for current version
    new_checkpoint_parts: Vec<ParsedLogPath>,
}

impl LogSegmentBuilderState {
    /// Create new state for building a LogSegment.
    pub fn new(log_root: Url, end_version: Option<Version>) -> Self {
        Self {
            log_root,
            end_version,
            ascending_commit_files: Vec::new(),
            ascending_compaction_files: Vec::new(),
            checkpoint_parts: Vec::new(),
            latest_crc_file: None,
            latest_commit_file: None,
            error: None,
            current_group_version: None,
            new_checkpoint_parts: Vec::new(),
        }
    }

    /// Apply consumer to a batch of file metadata.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data)
    /// - `Ok(false)` = Break (stop iteration, possibly due to error)
    ///
    /// The batch is expected to contain file metadata with columns:
    /// - `path`: String - file path
    /// - `size`: Long - file size in bytes
    /// - `modificationTime`: Long - modification timestamp
    #[inline]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::arrow::array::{Array, Int64Array, StringArray};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::AsAny;

        // If we already have an error, stop processing
        if self.error.is_some() {
            return Ok(false);
        }

        // Try to get the batch as ArrowEngineData
        let arrow_data = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::generic("Expected ArrowEngineData for Consumer KDF apply"))?;

        let record_batch = arrow_data.record_batch();
        let num_rows = record_batch.num_rows();

        if num_rows == 0 {
            return Ok(true); // Continue with empty batch
        }

        // Get required columns
        let path_col = record_batch
            .column_by_name("path")
            .ok_or_else(|| Error::generic("Missing 'path' column in file listing batch"))?;
        let path_array = path_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::generic("'path' column is not a StringArray"))?;

        let size_col = record_batch
            .column_by_name("size")
            .ok_or_else(|| Error::generic("Missing 'size' column in file listing batch"))?;
        let size_array = size_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::generic("'size' column is not an Int64Array"))?;

        let mod_time_col = record_batch
            .column_by_name("modificationTime")
            .ok_or_else(|| {
                Error::generic("Missing 'modificationTime' column in file listing batch")
            })?;
        let mod_time_array = mod_time_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::generic("'modificationTime' column is not an Int64Array"))?;

        // Process each row
        for i in 0..num_rows {
            if path_array.is_null(i) {
                continue;
            }

            let path_str = path_array.value(i);
            let size = if size_array.is_null(i) {
                0
            } else {
                size_array.value(i) as u64
            };
            let mod_time = if mod_time_array.is_null(i) {
                0
            } else {
                mod_time_array.value(i)
            };

            // Parse the path into a URL
            let file_url = match self.log_root.join(path_str) {
                Ok(url) => url,
                Err(e) => {
                    self.error = Some(format!("Failed to parse file path '{}': {}", path_str, e));
                    return Ok(false);
                }
            };

            // Create FileMeta
            let file_meta = FileMeta {
                location: file_url,
                last_modified: mod_time,
                size,
            };

            // Try to parse as ParsedLogPath
            let parsed_path = match ParsedLogPath::try_from(file_meta) {
                Ok(Some(path)) => path,
                Ok(None) => continue, // Not a valid log path, skip
                Err(e) => {
                    self.error = Some(format!("Failed to parse log path '{}': {}", path_str, e));
                    return Ok(false);
                }
            };

            // Check if we should stop based on end_version
            if let Some(end_version) = self.end_version {
                if parsed_path.version > end_version {
                    // Flush any pending checkpoint group before stopping
                    if let Some(group_version) = self.current_group_version {
                        self.flush_checkpoint_group(group_version);
                    }
                    return Ok(false); // Break - we've passed the end version
                }
            }

            // Process the file based on its type
            self.process_file(parsed_path);
        }

        Ok(true) // Continue
    }

    /// Process a single parsed log file.
    fn process_file(&mut self, file: ParsedLogPath) {
        // Check if version changed - need to flush checkpoint group
        if let Some(group_version) = self.current_group_version {
            if file.version != group_version {
                self.flush_checkpoint_group(group_version);
            }
        }
        self.current_group_version = Some(file.version);

        match &file.file_type {
            LogPathFileType::Commit | LogPathFileType::StagedCommit => {
                self.ascending_commit_files.push(file);
            }
            LogPathFileType::CompactedCommit { hi } => {
                // Only include if within end_version bounds
                if self.end_version.is_none_or(|end| *hi <= end) {
                    self.ascending_compaction_files.push(file);
                }
            }
            LogPathFileType::SinglePartCheckpoint
            | LogPathFileType::UuidCheckpoint
            | LogPathFileType::MultiPartCheckpoint { .. } => {
                self.new_checkpoint_parts.push(file);
            }
            LogPathFileType::Crc => {
                self.latest_crc_file = Some(file);
            }
            LogPathFileType::Unknown => {
                // Ignore unknown file types
            }
        }
    }

    /// Flush accumulated checkpoint parts for a version.
    fn flush_checkpoint_group(&mut self, version: Version) {
        if self.new_checkpoint_parts.is_empty() {
            return;
        }

        // Group and find complete checkpoint
        let new_parts = std::mem::take(&mut self.new_checkpoint_parts);
        if let Some(complete_checkpoint) = self.find_complete_checkpoint(new_parts) {
            self.checkpoint_parts = complete_checkpoint;
            // Save latest commit at checkpoint version if exists
            self.latest_commit_file = self
                .ascending_commit_files
                .pop()
                .filter(|commit| commit.version == version);
            // Clear commits/compactions before checkpoint
            self.ascending_commit_files.clear();
            self.ascending_compaction_files.clear();
        }
    }

    /// Find a complete checkpoint from parts.
    fn find_complete_checkpoint(
        &self,
        parts: Vec<ParsedLogPath>,
    ) -> Option<Vec<ParsedLogPath>> {
        use std::collections::HashMap;

        let mut checkpoints: HashMap<u32, Vec<ParsedLogPath>> = HashMap::new();

        for part_file in parts {
            match &part_file.file_type {
                LogPathFileType::SinglePartCheckpoint
                | LogPathFileType::UuidCheckpoint
                | LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 1,
                } => {
                    // Single-file checkpoints are equivalent, keep one
                    checkpoints.insert(1, vec![part_file]);
                }
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts,
                } => {
                    checkpoints.insert(*num_parts, vec![part_file]);
                }
                LogPathFileType::MultiPartCheckpoint { part_num, num_parts } => {
                    if let Some(part_files) = checkpoints.get_mut(num_parts) {
                        if *part_num as usize == 1 + part_files.len() {
                            part_files.push(part_file);
                        }
                    }
                }
                _ => {}
            }
        }

        // Find first complete checkpoint
        checkpoints
            .into_iter()
            .find(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
            .map(|(_, parts)| parts)
    }

    /// Finalize the state and check for errors.
    ///
    /// Call this after iteration completes to flush any pending state.
    pub fn finalize(&mut self) {
        // Flush final checkpoint group
        if let Some(group_version) = self.current_group_version {
            self.flush_checkpoint_group(group_version);
        }

        // Update latest_commit_file if we have commits after checkpoint
        if let Some(commit_file) = self.ascending_commit_files.last() {
            self.latest_commit_file = Some(commit_file.clone());
        }
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }
}

// =============================================================================
// ConsumerKdfState - Wrapper enum for consumer KDFs
// =============================================================================

/// Consumer KDF state - consumes batches and returns Continue/Break.
///
/// Unlike Filter KDFs that return per-row selection vectors (`BooleanArray`),
/// Consumer KDFs return a single `bool` for the entire batch:
/// - `true` = Continue (keep feeding data)
/// - `false` = Break (stop iteration)
///
/// The enum variant IS the function identity - no separate function_id needed.
#[derive(Debug, Clone)]
pub enum ConsumerKdfState {
    /// Builds a LogSegment from file listing results
    LogSegmentBuilder(LogSegmentBuilderState),
}

impl ConsumerKdfState {
    /// Apply consumer to a batch.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data)
    /// - `Ok(false)` = Break (stop iteration)
    #[inline]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<bool> {
        match self {
            Self::LogSegmentBuilder(state) => state.apply(batch),
        }
    }

    /// Finalize the consumer state after iteration completes.
    pub fn finalize(&mut self) {
        match self {
            Self::LogSegmentBuilder(state) => state.finalize(),
        }
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        match self {
            Self::LogSegmentBuilder(state) => state.has_error(),
        }
    }

    /// Take the error as a DeltaResult, if any.
    pub fn take_error(&mut self) -> Option<Error> {
        match self {
            Self::LogSegmentBuilder(state) => {
                state.take_error().map(Error::generic)
            }
        }
    }

    // =========================================================================
    // FFI Boundary Methods
    // =========================================================================

    /// Convert to raw pointer for FFI (transfers ownership to the pointer).
    pub fn into_raw(self) -> u64 {
        Box::into_raw(Box::new(self)) as u64
    }

    /// Borrow from raw pointer without taking ownership.
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

    // =========================================================================
    // ConsumerKdfState and LogSegmentBuilderState Tests
    // =========================================================================

    fn create_file_listing_batch(files: &[(&str, i64, i64)]) -> ArrowEngineData {
        use crate::arrow::array::Int64Array;

        let schema = Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modificationTime", DataType::Int64, false),
        ]);
        let path_array = StringArray::from(files.iter().map(|(p, _, _)| *p).collect::<Vec<_>>());
        let size_array = Int64Array::from(files.iter().map(|(_, s, _)| *s).collect::<Vec<_>>());
        let mod_time_array = Int64Array::from(files.iter().map(|(_, _, m)| *m).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(path_array),
                Arc::new(size_array),
                Arc::new(mod_time_array),
            ],
        )
        .unwrap();
        ArrowEngineData::new(batch)
    }

    // =========================================================================
    // Basic State Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_state_new() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state = LogSegmentBuilderState::new(log_root.clone(), None);

        assert_eq!(state.log_root, log_root);
        assert!(state.end_version.is_none());
        assert!(state.ascending_commit_files.is_empty());
        assert!(state.checkpoint_parts.is_empty());
        assert!(state.error.is_none());
    }

    #[test]
    fn test_log_segment_builder_state_with_end_version() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state = LogSegmentBuilderState::new(log_root.clone(), Some(10));

        assert_eq!(state.log_root, log_root);
        assert_eq!(state.end_version, Some(10));
    }

    #[test]
    fn test_log_segment_builder_empty_batch() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[]);

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Empty batch should return Continue (true)");
        assert!(!state.has_error());
    }

    // =========================================================================
    // Commit File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_commit_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        // Version 0, 1, 2 commit files
        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.json", 1200, 300),
        ]);

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Should return Continue (true)");

        state.finalize();

        assert_eq!(state.ascending_commit_files.len(), 3);
        assert_eq!(state.ascending_commit_files[0].version, 0);
        assert_eq!(state.ascending_commit_files[1].version, 1);
        assert_eq!(state.ascending_commit_files[2].version, 2);
        
        // Latest commit should be version 2
        assert!(state.latest_commit_file.is_some());
        assert_eq!(state.latest_commit_file.as_ref().unwrap().version, 2);
    }

    #[test]
    fn test_log_segment_builder_multiple_batches() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        // First batch
        let batch1 = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
        ]);
        assert!(state.apply(&batch1).unwrap());

        // Second batch
        let batch2 = create_file_listing_batch(&[
            ("00000000000000000002.json", 1200, 300),
            ("00000000000000000003.json", 1300, 400),
        ]);
        assert!(state.apply(&batch2).unwrap());

        state.finalize();

        assert_eq!(state.ascending_commit_files.len(), 4);
        assert_eq!(state.latest_commit_file.as_ref().unwrap().version, 3);
    }

    // =========================================================================
    // Checkpoint File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_single_part_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.checkpoint.parquet", 5000, 300),
            ("00000000000000000002.json", 1200, 300),
            ("00000000000000000003.json", 1300, 400),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Checkpoint at version 2 should clear commits before it
        // Only commits at version 2 and after should remain
        assert_eq!(state.checkpoint_parts.len(), 1);
        assert_eq!(state.checkpoint_parts[0].version, 2);
        
        // Commits before checkpoint are cleared, only v3 remains
        assert_eq!(state.ascending_commit_files.len(), 1);
        assert_eq!(state.ascending_commit_files[0].version, 3);
        
        // Latest commit is v3
        assert_eq!(state.latest_commit_file.as_ref().unwrap().version, 3);
    }

    #[test]
    fn test_log_segment_builder_multipart_checkpoint_complete() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        // Multi-part checkpoint with 3 parts - all present
        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.checkpoint.0000000001.0000000003.parquet", 5000, 300),
            ("00000000000000000002.checkpoint.0000000002.0000000003.parquet", 5000, 301),
            ("00000000000000000002.checkpoint.0000000003.0000000003.parquet", 5000, 302),
            ("00000000000000000003.json", 1300, 400),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Complete 3-part checkpoint should be recognized
        assert_eq!(state.checkpoint_parts.len(), 3);
        assert!(state.checkpoint_parts.iter().all(|p| p.version == 2));
        
        // Only commit after checkpoint remains
        assert_eq!(state.ascending_commit_files.len(), 1);
        assert_eq!(state.ascending_commit_files[0].version, 3);
    }

    #[test]
    fn test_log_segment_builder_multipart_checkpoint_incomplete() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        // Multi-part checkpoint with 3 parts - only 2 present (incomplete)
        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.checkpoint.0000000001.0000000003.parquet", 5000, 300),
            ("00000000000000000002.checkpoint.0000000002.0000000003.parquet", 5000, 301),
            // Missing part 3!
            ("00000000000000000003.json", 1300, 400),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Incomplete checkpoint should not be used
        assert!(state.checkpoint_parts.is_empty());
        
        // All commits should be kept (v0, v1, v3 - no v2 commit in the test data)
        assert_eq!(state.ascending_commit_files.len(), 3);
    }

    #[test]
    fn test_log_segment_builder_checkpoint_clears_earlier_commits() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.json", 1200, 300),
            ("00000000000000000005.checkpoint.parquet", 5000, 500),
            ("00000000000000000005.json", 1500, 500),
            ("00000000000000000006.json", 1600, 600),
            ("00000000000000000007.json", 1700, 700),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Checkpoint at v5 clears commits before it
        assert_eq!(state.checkpoint_parts.len(), 1);
        assert_eq!(state.checkpoint_parts[0].version, 5);
        
        // Only commits after checkpoint (v6, v7) remain
        assert_eq!(state.ascending_commit_files.len(), 2);
        assert_eq!(state.ascending_commit_files[0].version, 6);
        assert_eq!(state.ascending_commit_files[1].version, 7);
        
        // Latest commit is v7
        assert_eq!(state.latest_commit_file.as_ref().unwrap().version, 7);
    }

    #[test]
    fn test_log_segment_builder_later_checkpoint_replaces_earlier() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000002.checkpoint.parquet", 5000, 200),
            ("00000000000000000002.json", 1200, 200),
            ("00000000000000000003.json", 1300, 300),
            ("00000000000000000005.checkpoint.parquet", 5000, 500),
            ("00000000000000000005.json", 1500, 500),
            ("00000000000000000006.json", 1600, 600),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Later checkpoint at v5 should replace the one at v2
        assert_eq!(state.checkpoint_parts.len(), 1);
        assert_eq!(state.checkpoint_parts[0].version, 5);
        
        // Only commit after v5 checkpoint remains
        assert_eq!(state.ascending_commit_files.len(), 1);
        assert_eq!(state.ascending_commit_files[0].version, 6);
    }

    // =========================================================================
    // CRC File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_crc_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000000.crc", 100, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000001.crc", 100, 200),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Latest CRC should be version 1
        assert!(state.latest_crc_file.is_some());
        assert_eq!(state.latest_crc_file.as_ref().unwrap().version, 1);
    }

    // =========================================================================
    // Compaction File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_compaction_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            // Compacted commits from version 0-5
            ("00000000000000000000.00000000000000000005.compacted.json", 3000, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        assert_eq!(state.ascending_compaction_files.len(), 1);
        assert_eq!(state.ascending_compaction_files[0].version, 0);
    }

    #[test]
    fn test_log_segment_builder_compaction_respects_end_version() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        // End version is 3, compaction goes to 5
        let mut state = LogSegmentBuilderState::new(log_root, Some(3));

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            // Compacted commits from version 0-5 - should be excluded because hi > end_version
            ("00000000000000000000.00000000000000000005.compacted.json", 3000, 300),
            // Compacted commits from version 0-3 - should be included
            ("00000000000000000000.00000000000000000003.compacted.json", 3000, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Only the compaction with hi <= end_version should be included
        assert_eq!(state.ascending_compaction_files.len(), 1);
    }

    // =========================================================================
    // End Version Filtering Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_end_version_stops_processing() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, Some(2));

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.json", 1200, 300),
            ("00000000000000000003.json", 1300, 400), // Beyond end_version
            ("00000000000000000004.json", 1400, 500), // Beyond end_version
        ]);

        // Should return false (Break) when hitting version > end_version
        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(!result.unwrap(), "Should return Break (false) after end_version");

        state.finalize();

        // Only versions 0, 1, 2 should be included
        assert_eq!(state.ascending_commit_files.len(), 3);
        assert_eq!(state.ascending_commit_files[2].version, 2);
    }

    #[test]
    fn test_log_segment_builder_end_version_with_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, Some(5));

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000002.checkpoint.parquet", 5000, 200),
            ("00000000000000000002.json", 1200, 200),
            ("00000000000000000003.json", 1300, 300),
            ("00000000000000000004.json", 1400, 400),
            ("00000000000000000005.json", 1500, 500),
            ("00000000000000000006.json", 1600, 600), // Beyond end_version
        ]);

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(!result.unwrap());

        state.finalize();

        // Checkpoint at v2 should be kept
        assert_eq!(state.checkpoint_parts.len(), 1);
        assert_eq!(state.checkpoint_parts[0].version, 2);
        
        // Commits v3, v4, v5 after checkpoint should be kept
        assert_eq!(state.ascending_commit_files.len(), 3);
    }

    // =========================================================================
    // Unknown and Invalid File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_unknown_files_ignored() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("some_random_file.txt", 500, 150), // Unknown file type
            ("00000000000000000001.json", 1100, 200),
            (".hidden_file", 100, 250), // Hidden file
            ("00000000000000000002.json", 1200, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Only commit files should be collected
        assert_eq!(state.ascending_commit_files.len(), 3);
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_error_handling() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        // Manually set an error
        state.error = Some("Test error".to_string());

        let batch = create_file_listing_batch(&[("00000000000000000001.json", 1000, 100)]);
        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Should return Break (false) when error exists"
        );

        assert!(state.has_error());
        let error = state.take_error();
        assert_eq!(error, Some("Test error".to_string()));
    }

    // =========================================================================
    // ConsumerKdfState Enum Tests
    // =========================================================================

    #[test]
    fn test_consumer_kdf_state_enum() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = ConsumerKdfState::LogSegmentBuilder(LogSegmentBuilderState::new(
            log_root.clone(),
            None,
        ));

        let batch = create_file_listing_batch(&[("00000000000000000001.json", 1000, 100)]);

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(result.unwrap());

        state.finalize();
        assert!(!state.has_error());
    }

    #[test]
    fn test_consumer_kdf_state_raw_pointer_roundtrip() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state =
            ConsumerKdfState::LogSegmentBuilder(LogSegmentBuilderState::new(log_root, None));

        let ptr = state.into_raw();
        assert_ne!(ptr, 0);

        unsafe {
            let borrowed = ConsumerKdfState::borrow_mut_from_raw(ptr);
            let batch = create_file_listing_batch(&[("00000000000000000001.json", 1000, 100)]);
            let _ = borrowed.apply(&batch);
        }

        let reconstructed = unsafe { ConsumerKdfState::from_raw(ptr) };
        match reconstructed {
            ConsumerKdfState::LogSegmentBuilder(s) => {
                assert!(!s.has_error());
            }
        }
    }

    // =========================================================================
    // Edge Case Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_checkpoint_at_version_zero() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.checkpoint.parquet", 5000, 100),
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Checkpoint at v0
        assert_eq!(state.checkpoint_parts.len(), 1);
        assert_eq!(state.checkpoint_parts[0].version, 0);
        
        // Only v1 commit after checkpoint
        assert_eq!(state.ascending_commit_files.len(), 1);
        assert_eq!(state.ascending_commit_files[0].version, 1);
    }

    #[test]
    fn test_log_segment_builder_only_checkpoint_no_commits_after() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.checkpoint.parquet", 5000, 200),
            ("00000000000000000002.json", 1200, 200),
            // No commits after version 2
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Checkpoint at v2
        assert_eq!(state.checkpoint_parts.len(), 1);
        
        // No commits after checkpoint
        assert!(state.ascending_commit_files.is_empty());
        
        // But latest_commit_file should be v2 (at checkpoint version)
        assert!(state.latest_commit_file.is_some());
        assert_eq!(state.latest_commit_file.as_ref().unwrap().version, 2);
    }

    #[test]
    fn test_log_segment_builder_no_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.json", 1200, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // No checkpoint
        assert!(state.checkpoint_parts.is_empty());
        
        // All commits kept
        assert_eq!(state.ascending_commit_files.len(), 3);
        
        // Latest commit is v2
        assert_eq!(state.latest_commit_file.as_ref().unwrap().version, 2);
    }

    #[test]
    fn test_log_segment_builder_uuid_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None);

        // UUID checkpoint format
        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.checkpoint.80a083e8-7026-4e79-8571-3f7e8a0001b3.parquet", 5000, 200),
            ("00000000000000000003.json", 1300, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // UUID checkpoint should be recognized
        assert_eq!(state.checkpoint_parts.len(), 1);
        assert_eq!(state.checkpoint_parts[0].version, 2);
        
        // Commits after checkpoint
        assert_eq!(state.ascending_commit_files.len(), 1);
        assert_eq!(state.ascending_commit_files[0].version, 3);
    }
}
