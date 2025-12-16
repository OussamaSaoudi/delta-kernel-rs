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
use std::sync::{Arc, Mutex};

use url::Url;

use crate::arrow::array::{BooleanArray, Int32Array};
use crate::listed_log_files::ListedLogFiles;
use crate::log_replay::FileActionKey;
use crate::log_segment::LogSegment;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::{DeltaResult, EngineData, Error, FileMeta, Version};

#[inline]
fn selection_value_or_true(selection: &BooleanArray, row: usize) -> bool {
    if row < selection.len() {
        selection.value(row)
    } else {
        true
    }
}

macro_rules! column_names_and_types {
    ($($dt:expr => $col:expr),+ $(,)?) => {{
        static NAMES_AND_TYPES: std::sync::LazyLock<crate::schema::ColumnNamesAndTypes> =
            std::sync::LazyLock::new(|| {
                let types_and_names = vec![$(($dt, $col)),+];
                let (types, names) = types_and_names.into_iter().unzip();
                (names, types).into()
            });
        NAMES_AND_TYPES.as_ref()
    }};
}

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
/// duplicates. Uses `Arc<Mutex<...>>` for interior mutability so state mutations are
/// visible even when the state is cloned (e.g., when moved into an iterator closure).
#[derive(Debug, Clone)]
pub struct AddRemoveDedupState {
    seen_keys: Arc<std::sync::Mutex<HashSet<FileActionKey>>>,
}

impl Default for AddRemoveDedupState {
    fn default() -> Self {
        Self {
            seen_keys: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }
}

impl AddRemoveDedupState {
    /// Create new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply dedup filter to a batch - monomorphized, no dispatch overhead.
    ///
    /// This processes BOTH add and remove actions:
    /// - Both add and remove file keys are recorded in seen_keys
    /// - Only add actions pass through (remove actions are filtered out but recorded)
    /// - This ensures that files removed in commits don't appear from checkpoints
    ///
    /// For each row where selection[i] is true:
    /// - Extract file key from add.path or remove.path
    /// - If already seen: set selection[i] = false (filter out)
    /// - Else: add to seen set
    ///   - If it's an add: keep selection[i] = true
    ///   - If it's a remove: set selection[i] = false (don't return removes, just record them)
    #[inline]
    pub fn apply(
        &mut self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::log_replay::FileActionDeduplicator;
        use crate::schema::{ColumnName, DataType};

        // Commit/log batch behavior (AddRemoveDedupState is only used in commit phase).
        const IS_LOG_BATCH: bool = true;

        // ---- Fast path: reuse FileActionDeduplicator with full add/remove + DV columns ----
        struct DedupVisitor<'seen> {
            dedup: FileActionDeduplicator<'seen>,
            input_selection: BooleanArray,
            out_selection: Vec<bool>,
        }

        impl crate::engine_data::RowVisitor for DedupVisitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                // Keep ordering consistent with the indices passed to FileActionDeduplicator below.
                const STRING: DataType = DataType::STRING;
                const INTEGER: DataType = DataType::INTEGER;
                column_names_and_types![
                    STRING => crate::schema::column_name!("add.path"),
                    STRING => crate::schema::column_name!("add.deletionVector.storageType"),
                    STRING => crate::schema::column_name!("add.deletionVector.pathOrInlineDv"),
                    INTEGER => crate::schema::column_name!("add.deletionVector.offset"),
                    STRING => crate::schema::column_name!("remove.path"),
                    STRING => crate::schema::column_name!("remove.deletionVector.storageType"),
                    STRING => crate::schema::column_name!("remove.deletionVector.pathOrInlineDv"),
                    INTEGER => crate::schema::column_name!("remove.deletionVector.offset"),
                ]
            }

            fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
                self.out_selection.resize(row_count, false);
                for i in 0..row_count {
                    if !selection_value_or_true(&self.input_selection, i) {
                        self.out_selection[i] = false;
                        continue;
                    }
                    let Some((file_key, is_add)) =
                        self.dedup.extract_file_action(i, getters, /*skip_removes*/ false)?
                    else {
                        self.out_selection[i] = false;
                        continue;
                    };
                    // Record both adds and removes, but only emit surviving adds.
                    self.out_selection[i] = !(self.dedup.check_and_record_seen(file_key) || !is_add);
                }
                Ok(())
            }
        }

        let mut seen_keys_guard = self
            .seen_keys
            .lock()
            .map_err(|_| Error::generic("Lock poisoned"))?;

        // Strict: this KDF expects Delta action batches containing add/remove and DV fields.
        let mut full_visitor = DedupVisitor {
            dedup: FileActionDeduplicator::new(
                &mut seen_keys_guard,
                IS_LOG_BATCH,
                /*add_path_index*/ 0,
                /*remove_path_index*/ 4,
                /*add_dv_start_index*/ 1,
                /*remove_dv_start_index*/ 5,
            ),
            input_selection: selection.clone(),
            out_selection: vec![],
        };

        full_visitor.visit_rows_of(batch)?;
        Ok(BooleanArray::from(full_visitor.out_selection))
    }

    /// Serialize state for distribution to executors.
    pub fn serialize(&self) -> DeltaResult<Vec<u8>> {
        let seen_keys = self.seen_keys.lock().map_err(|_| Error::generic("Lock poisoned"))?;
        serialize_file_action_keys(&seen_keys)
    }

    /// Deserialize state from bytes (received from driver).
    pub fn deserialize(bytes: &[u8]) -> DeltaResult<Self> {
        let seen_keys = deserialize_file_action_keys(bytes)?;
        Ok(Self { seen_keys: Arc::new(std::sync::Mutex::new(seen_keys)) })
    }

    /// Check if a key has been seen.
    pub fn contains(&self, key: &FileActionKey) -> bool {
        self.seen_keys.lock().map(|s| s.contains(key)).unwrap_or(false)
    }

    /// Insert a key into the seen set.
    pub fn insert(&self, key: FileActionKey) -> bool {
        self.seen_keys.lock().map(|mut s| s.insert(key)).unwrap_or(false)
    }

    /// Get the number of seen keys.
    pub fn len(&self) -> usize {
        self.seen_keys.lock().map(|s| s.len()).unwrap_or(0)
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.seen_keys.lock().map(|s| s.is_empty()).unwrap_or(true)
    }

    /// Get a clone of the seen keys.
    /// Used to transfer state to CheckpointDedupState for checkpoint phase.
    pub fn seen_keys(&self) -> HashSet<FileActionKey> {
        self.seen_keys.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Convert to a CheckpointDedupState for the checkpoint phase.
    /// The resulting state contains the same keys and can be used for read-only probing.
    pub fn to_checkpoint_dedup_state(&self) -> CheckpointDedupState {
        CheckpointDedupState::from_hashset(self.seen_keys())
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

// =============================================================================
// PartitionPruneState - Partition-only pruning using add.partitionValues
// =============================================================================

/// State for PartitionPrune KDF - prunes Add actions based on partition values.
///
/// This evaluates the scan predicate against directory-derived partition values (from
/// `add.partitionValues`). It is conservative: rows are pruned only when the predicate can be
/// proven false from partition values alone. Non-add rows (e.g. removes) are never pruned.
#[derive(Debug, Clone)]
pub struct PartitionPruneState {
    pub(crate) predicate: crate::expressions::PredicateRef,
    pub(crate) transform_spec: Arc<crate::transforms::TransformSpec>,
    pub(crate) logical_schema: crate::schema::SchemaRef,
    pub(crate) column_mapping_mode: crate::table_features::ColumnMappingMode,
}

impl PartitionPruneState {
    #[inline]
    pub fn apply(
        &self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, KernelPredicateEvaluator as _};
        use crate::schema::{ColumnName, DataType, MapType};
        use crate::transforms::parse_partition_values;
        use std::collections::HashMap;

        struct Visitor<'a> {
            state: &'a PartitionPruneState,
            input_selection: &'a BooleanArray,
            out: Vec<bool>,
        }

        impl crate::engine_data::RowVisitor for Visitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                const STRING: DataType = DataType::STRING;
                column_names_and_types![
                    STRING => crate::schema::column_name!("add.path"),
                    MapType::new(STRING, STRING, true).into() => crate::schema::column_name!("add.partitionValues"),
                ]
            }

            fn visit<'a2>(&mut self, row_count: usize, getters: &[&'a2 dyn GetData<'a2>]) -> DeltaResult<()> {
                self.out.resize(row_count, true);
                for i in 0..row_count {
                    // Treat missing selection entries as true (selected) per FilteredEngineData contract.
                    let selected = selection_value_or_true(self.input_selection, i);
                    if !selected {
                        self.out[i] = false;
                        continue;
                    }

                    // Only prune Add rows. Removes/non-adds should stay selected.
                    let is_add = getters[0].get_str(i, "add.path")?.is_some();
                    if !is_add {
                        self.out[i] = true;
                        continue;
                    }

                    let partition_values: HashMap<String, String> =
                        getters[1].get(i, "add.partitionValues")?;
                    let parsed = parse_partition_values(
                        &self.state.logical_schema,
                        &self.state.transform_spec,
                        &partition_values,
                        self.state.column_mapping_mode,
                    )?;
                    let partition_values_for_eval: HashMap<_, _> = parsed
                        .values()
                        .map(|(k, v)| (ColumnName::new([k]), v.clone()))
                        .collect();
                    let evaluator = DefaultKernelPredicateEvaluator::from(partition_values_for_eval);
                    if evaluator.eval_sql_where(&self.state.predicate) == Some(false) {
                        self.out[i] = false;
                    } else {
                        self.out[i] = true;
                    }
                }
                Ok(())
            }
        }

        let mut visitor = Visitor {
            state: self,
            input_selection: &selection,
            out: vec![],
        };
        visitor.visit_rows_of(batch)?;
        Ok(BooleanArray::from(visitor.out))
    }
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
        use crate::engine_data::{GetData, RowVisitor};
        use crate::log_replay::FileActionDeduplicator;
        use crate::schema::{ColumnName, DataType};

        // Checkpoint dedup is a read-only probe. It should match legacy behavior:
        // - Only consider Add actions (checkpoint files are reconciled)
        // - Key by (path, dv_unique_id) (not path-only)
        struct ProbeVisitor<'seen> {
            dedup: FileActionDeduplicator<'seen>,
            input_selection: BooleanArray,
            out_selection: Vec<bool>,
        }

        impl crate::engine_data::RowVisitor for ProbeVisitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                const STRING: DataType = DataType::STRING;
                const INTEGER: DataType = DataType::INTEGER;
                column_names_and_types![
                    STRING => crate::schema::column_name!("add.path"),
                    STRING => crate::schema::column_name!("add.deletionVector.storageType"),
                    STRING => crate::schema::column_name!("add.deletionVector.pathOrInlineDv"),
                    INTEGER => crate::schema::column_name!("add.deletionVector.offset"),
                ]
            }

            fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
                self.out_selection.resize(row_count, false);
                for i in 0..row_count {
                    if !selection_value_or_true(&self.input_selection, i) {
                        self.out_selection[i] = false;
                        continue;
                    }
                    let Some((file_key, _is_add)) =
                        self.dedup.extract_file_action(i, getters, /*skip_removes*/ true)?
                    else {
                        self.out_selection[i] = false;
                        continue;
                    };
                    // Probe only: is_log_batch=false means check_and_record_seen() won't insert.
                    self.out_selection[i] = !self.dedup.check_and_record_seen(file_key);
                }
                Ok(())
            }
        }

        // In checkpoint mode we must not record new keys.
        const IS_LOG_BATCH: bool = false;

        // Strict: checkpoint dedup expects action batches containing add.path (+ optional DV fields).
        let mut seen_keys = self.seen_keys.clone();
        let mut visitor = ProbeVisitor {
            dedup: FileActionDeduplicator::new(
                &mut seen_keys,
                IS_LOG_BATCH,
                /*add_path_index*/ 0,
                /*remove_path_index*/ 0, // unused when skip_removes=true
                /*add_dv_start_index*/ 1,
                /*remove_dv_start_index*/ 1, // unused when skip_removes=true
            ),
            input_selection: selection.clone(),
            out_selection: vec![],
        };

        visitor.visit_rows_of(batch)?;
        Ok(BooleanArray::from(visitor.out_selection))
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
#[derive(Debug, Clone)]
pub struct SchemaStoreState {
    /// Uses Arc<OnceLock> for thread-safe, one-time initialization during execution.
    /// Arc ensures that when the state is cloned (for execution), both the original
    /// and clone share the same underlying storage.
    schema: Arc<std::sync::OnceLock<SchemaRef>>,
}

impl Default for SchemaStoreState {
    fn default() -> Self {
        Self {
            schema: Arc::new(std::sync::OnceLock::new()),
        }
    }
}

impl SchemaStoreState {
    /// Create new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a schema. Can only be called once; subsequent calls are ignored.
    pub fn store(&self, schema: SchemaRef) {
        let _ = self.schema.set(schema);
    }

    /// Get the stored schema, if any.
    pub fn get(&self) -> Option<&SchemaRef> {
        self.schema.get()
    }
}

// =============================================================================
// LogSegmentBuilderState - Builds LogSegment from file listing results
// =============================================================================

/// Inner mutable state for LogSegmentBuilder.
#[derive(Debug)]
struct LogSegmentBuilderInner {
    /// Log directory root URL
    log_root: Url,
    /// Optional end version to stop at
    end_version: Option<Version>,
    /// Checkpoint hint version from `_last_checkpoint` file
    /// When set, the builder knows where to expect a valid checkpoint
    checkpoint_hint_version: Option<Version>,
    /// Sorted commit files in the log segment (ascending)
    ascending_commit_files: Vec<ParsedLogPath>,
    /// Sorted compaction files in the log segment (ascending)
    ascending_compaction_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment
    checkpoint_parts: Vec<ParsedLogPath>,
    /// Latest CRC (checksum) file
    latest_crc_file: Option<ParsedLogPath>,
    /// Latest commit file (may not be in contiguous segment)
    latest_commit_file: Option<ParsedLogPath>,
    /// Stored error to surface during advance()
    error: Option<String>,
    /// Current group version for checkpoint grouping
    current_group_version: Option<Version>,
    /// New checkpoint parts being accumulated for current version
    new_checkpoint_parts: Vec<ParsedLogPath>,
}

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
///
/// Uses interior mutability via `Arc<Mutex<>>` to allow `apply(&self)` signature,
/// enabling use in lazy iterators.
#[derive(Debug, Clone)]
pub struct LogSegmentBuilderState {
    inner: Arc<Mutex<LogSegmentBuilderInner>>,
}

impl LogSegmentBuilderState {
    /// Create new state for building a LogSegment.
    ///
    /// # Arguments
    /// * `log_root` - The log directory root URL
    /// * `end_version` - Optional end version to stop at
    /// * `checkpoint_hint_version` - Optional checkpoint hint version from `_last_checkpoint`
    pub fn new(
        log_root: Url,
        end_version: Option<Version>,
        checkpoint_hint_version: Option<Version>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogSegmentBuilderInner {
                log_root,
                end_version,
                checkpoint_hint_version,
                ascending_commit_files: Vec::new(),
                ascending_compaction_files: Vec::new(),
                checkpoint_parts: Vec::new(),
                latest_crc_file: None,
                latest_commit_file: None,
                error: None,
                current_group_version: None,
                new_checkpoint_parts: Vec::new(),
            })),
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
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::arrow::array::{Array, Int64Array, StringArray};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::AsAny;

        let mut inner = self.inner.lock().unwrap();

        // If we already have an error, stop processing
        if inner.error.is_some() {
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
            let file_url = match inner.log_root.join(path_str) {
                Ok(url) => url,
                Err(e) => {
                    inner.error = Some(format!("Failed to parse file path '{}': {}", path_str, e));
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
                    inner.error = Some(format!("Failed to parse log path '{}': {}", path_str, e));
                    return Ok(false);
                }
            };

            // Check if we should stop based on end_version
            if let Some(end_version) = inner.end_version {
                if parsed_path.version > end_version {
                    // Flush any pending checkpoint group before stopping
                    if let Some(group_version) = inner.current_group_version {
                        Self::flush_checkpoint_group_inner(&mut inner, group_version);
                    }
                    return Ok(false); // Break - we've passed the end version
                }
            }

            // Process the file based on its type
            Self::process_file_inner(&mut inner, parsed_path);
        }

        Ok(true) // Continue
    }

    /// Process a single parsed log file (operates on inner state).
    fn process_file_inner(inner: &mut LogSegmentBuilderInner, file: ParsedLogPath) {
        // Check if version changed - need to flush checkpoint group
        if let Some(group_version) = inner.current_group_version {
            if file.version != group_version {
                Self::flush_checkpoint_group_inner(inner, group_version);
            }
        }
        inner.current_group_version = Some(file.version);

        match &file.file_type {
            LogPathFileType::Commit | LogPathFileType::StagedCommit => {
                inner.ascending_commit_files.push(file);
            }
            LogPathFileType::CompactedCommit { hi } => {
                // Only include if within end_version bounds
                if inner.end_version.is_none_or(|end| *hi <= end) {
                    inner.ascending_compaction_files.push(file);
                }
            }
            LogPathFileType::SinglePartCheckpoint
            | LogPathFileType::UuidCheckpoint
            | LogPathFileType::MultiPartCheckpoint { .. } => {
                inner.new_checkpoint_parts.push(file);
            }
            LogPathFileType::Crc => {
                inner.latest_crc_file = Some(file);
            }
            LogPathFileType::Unknown => {
                // Ignore unknown file types
            }
        }
    }

    /// Flush accumulated checkpoint parts for a version (operates on inner state).
    fn flush_checkpoint_group_inner(inner: &mut LogSegmentBuilderInner, version: Version) {
        if inner.new_checkpoint_parts.is_empty() {
            return;
        }

        // Group and find complete checkpoint
        let new_parts = std::mem::take(&mut inner.new_checkpoint_parts);
        if let Some(complete_checkpoint) = Self::find_complete_checkpoint_inner(new_parts) {
            inner.checkpoint_parts = complete_checkpoint;
            // Save latest commit at checkpoint version if exists
            inner.latest_commit_file = inner
                .ascending_commit_files
                .pop()
                .filter(|commit| commit.version == version);
            // Clear commits/compactions before checkpoint
            inner.ascending_commit_files.clear();
            inner.ascending_compaction_files.clear();
        }
    }

    /// Find a complete checkpoint from parts (static helper).
    fn find_complete_checkpoint_inner(
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
    pub fn finalize(&self) {
        let mut inner = self.inner.lock().unwrap();
        // Flush final checkpoint group
        if let Some(group_version) = inner.current_group_version {
            Self::flush_checkpoint_group_inner(&mut inner, group_version);
        }

        // Update latest_commit_file if we have commits after checkpoint
        if let Some(commit_file) = inner.ascending_commit_files.last() {
            inner.latest_commit_file = Some(commit_file.clone());
        }
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Convert the accumulated state into a LogSegment.
    ///
    /// This finalizes the state and builds a LogSegment from the accumulated files.
    /// Returns an error if there was a processing error or if the log segment is invalid.
    ///
    /// Note: This takes ownership of the inner data via locking. Multiple clones of
    /// this state may exist (due to plan cloning), but only the first to call this
    /// method will get the data - subsequent calls will see empty vectors.
    pub fn into_log_segment(&self) -> DeltaResult<LogSegment> {
        // Finalize first
        self.finalize();

        // Take ownership of inner data via lock
        let mut guard = self.inner.lock().unwrap();

        // Check for errors
        if let Some(error) = guard.error.take() {
            return Err(Error::generic(error));
        }

        // Take the accumulated data (leaving empty vecs behind)
        let ascending_commit_files = std::mem::take(&mut guard.ascending_commit_files);
        let ascending_compaction_files = std::mem::take(&mut guard.ascending_compaction_files);
        let checkpoint_parts = std::mem::take(&mut guard.checkpoint_parts);
        let latest_crc_file = guard.latest_crc_file.take();
        let latest_commit_file = guard.latest_commit_file.take();
        let log_root = guard.log_root.clone();
        let end_version = guard.end_version;

        // Build ListedLogFiles
        let listed_files = ListedLogFiles::try_new(
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
            latest_commit_file,
        )?;

        // Build LogSegment
        LogSegment::try_new(listed_files, log_root, end_version)
    }

    // Test-only accessors for internal state
    #[cfg(test)]
    pub fn log_root(&self) -> Url {
        self.inner.lock().unwrap().log_root.clone()
    }

    #[cfg(test)]
    pub fn end_version(&self) -> Option<Version> {
        self.inner.lock().unwrap().end_version
    }

    #[cfg(test)]
    pub fn ascending_commit_files(&self) -> Vec<ParsedLogPath> {
        self.inner.lock().unwrap().ascending_commit_files.clone()
    }

    #[cfg(test)]
    pub fn checkpoint_parts(&self) -> Vec<ParsedLogPath> {
        self.inner.lock().unwrap().checkpoint_parts.clone()
    }

    #[cfg(test)]
    pub fn latest_commit_file(&self) -> Option<ParsedLogPath> {
        self.inner.lock().unwrap().latest_commit_file.clone()
    }

    #[cfg(test)]
    pub fn error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.clone()
    }

    #[cfg(test)]
    pub fn latest_crc_file(&self) -> Option<ParsedLogPath> {
        self.inner.lock().unwrap().latest_crc_file.clone()
    }

    #[cfg(test)]
    pub fn ascending_compaction_files(&self) -> Vec<ParsedLogPath> {
        self.inner.lock().unwrap().ascending_compaction_files.clone()
    }

    #[cfg(test)]
    pub fn set_error(&self, error: String) {
        self.inner.lock().unwrap().error = Some(error);
    }
}

// =============================================================================
// CheckpointHintReaderState - Extracts checkpoint hint from _last_checkpoint
// =============================================================================

/// Inner mutable state for CheckpointHintReader.
#[derive(Debug, Default)]
struct CheckpointHintReaderInner {
    /// The extracted checkpoint version
    version: Option<Version>,
    /// The number of actions stored in the checkpoint
    size: Option<i64>,
    /// The number of fragments if the checkpoint was written in multiple parts
    parts: Option<i32>,
    /// The number of bytes of the checkpoint
    size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint
    num_of_add_files: Option<i64>,
    /// Whether we've processed the hint (only expect one row)
    processed: bool,
    /// Stored error to surface during advance()
    error: Option<String>,
}

/// State for CheckpointHintReader consumer KDF - extracts checkpoint hint from scan results.
///
/// This is a **Consumer KDF** that processes the scan results of the `_last_checkpoint`
/// file and extracts the checkpoint hint information (version, size, parts, etc.).
///
/// Uses interior mutability via `Arc<Mutex<>>` to allow `apply(&self)` signature.
///
/// # Usage Pattern
///
/// 1. Create state
/// 2. Feed scanned rows from `_last_checkpoint` file through `apply()`
/// 3. `apply()` returns `Ok(true)` to continue, `Ok(false)` after extracting hint
/// 4. After completion, extract the hint via `take_hint()`
#[derive(Debug, Clone)]
pub struct CheckpointHintReaderState {
    inner: Arc<Mutex<CheckpointHintReaderInner>>,
}

impl Default for CheckpointHintReaderState {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointHintReaderState {
    /// Create new state for reading checkpoint hint.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CheckpointHintReaderInner::default())),
        }
    }

    /// Apply consumer to a batch of checkpoint hint data.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data)
    /// - `Ok(false)` = Break (stop iteration, we have the hint)
    ///
    /// The batch is expected to contain columns from the `_last_checkpoint` JSON:
    /// - `version`: Long - checkpoint version
    /// - `size`: Long - number of actions
    /// - `parts`: Integer - number of parts (optional)
    /// - `sizeInBytes`: Long - size in bytes (optional)
    /// - `numOfAddFiles`: Long - number of add files (optional)
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::arrow::array::{Array, Int32Array, Int64Array};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::AsAny;

        let mut inner = self.inner.lock().unwrap();

        // If we already processed a hint, stop
        if inner.processed {
            return Ok(false);
        }

        // If we have an error, stop processing
        if inner.error.is_some() {
            return Ok(false);
        }

        // Try to get the batch as ArrowEngineData
        let arrow_data = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::generic("Expected ArrowEngineData for CheckpointHintReader"))?;

        let record_batch = arrow_data.record_batch();
        let num_rows = record_batch.num_rows();

        if num_rows == 0 {
            return Ok(true); // Continue with empty batch
        }

        // We only expect one row in the _last_checkpoint file
        // Extract version (required)
        if let Some(version_col) = record_batch.column_by_name("version") {
            if let Some(version_array) = version_col.as_any().downcast_ref::<Int64Array>() {
                if !version_array.is_null(0) {
                    inner.version = Some(version_array.value(0) as Version);
                }
            }
        }

        // Extract size (optional but commonly present)
        if let Some(size_col) = record_batch.column_by_name("size") {
            if let Some(size_array) = size_col.as_any().downcast_ref::<Int64Array>() {
                if !size_array.is_null(0) {
                    inner.size = Some(size_array.value(0));
                }
            }
        }

        // Extract parts (optional)
        if let Some(parts_col) = record_batch.column_by_name("parts") {
            if let Some(parts_array) = parts_col.as_any().downcast_ref::<Int32Array>() {
                if !parts_array.is_null(0) {
                    inner.parts = Some(parts_array.value(0));
                }
            }
        }

        // Extract sizeInBytes (optional)
        if let Some(size_in_bytes_col) = record_batch.column_by_name("sizeInBytes") {
            if let Some(size_in_bytes_array) = size_in_bytes_col.as_any().downcast_ref::<Int64Array>()
            {
                if !size_in_bytes_array.is_null(0) {
                    inner.size_in_bytes = Some(size_in_bytes_array.value(0));
                }
            }
        }

        // Extract numOfAddFiles (optional)
        if let Some(num_add_col) = record_batch.column_by_name("numOfAddFiles") {
            if let Some(num_add_array) = num_add_col.as_any().downcast_ref::<Int64Array>() {
                if !num_add_array.is_null(0) {
                    inner.num_of_add_files = Some(num_add_array.value(0));
                }
            }
        }

        inner.processed = true;
        
        // Return false to indicate we're done (Break)
        Ok(false)
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&self) {
        // Nothing special needed for finalization
    }

    /// Check if a hint was successfully extracted.
    pub fn has_hint(&self) -> bool {
        self.inner.lock().unwrap().version.is_some()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Get the extracted version, if any.
    pub fn get_version(&self) -> Option<Version> {
        self.inner.lock().unwrap().version
    }

    /// Get the extracted parts count, if any.
    pub fn get_parts(&self) -> Option<i32> {
        self.inner.lock().unwrap().parts
    }

    // Test-only accessors for internal state
    #[cfg(test)]
    pub fn version(&self) -> Option<Version> {
        self.inner.lock().unwrap().version
    }

    #[cfg(test)]
    pub fn parts(&self) -> Option<i32> {
        self.inner.lock().unwrap().parts
    }

    #[cfg(test)]
    pub fn size(&self) -> Option<i64> {
        self.inner.lock().unwrap().size
    }

    #[cfg(test)]
    pub fn error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.clone()
    }

    #[cfg(test)]
    pub fn size_in_bytes(&self) -> Option<i64> {
        self.inner.lock().unwrap().size_in_bytes
    }

    #[cfg(test)]
    pub fn num_of_add_files(&self) -> Option<i64> {
        self.inner.lock().unwrap().num_of_add_files
    }
}

// =============================================================================
// MetadataProtocolReaderState - Extracts metadata and protocol from log files
// =============================================================================

use crate::actions::{Metadata, Protocol};

/// Inner mutable state for MetadataProtocolReader.
#[derive(Debug, Default)]
struct MetadataProtocolReaderInner {
    /// The extracted protocol action (complete or None)
    protocol: Option<Protocol>,
    /// The extracted metadata action (complete or None)
    metadata: Option<Metadata>,
    /// Stored error to surface during advance()
    error: Option<String>,
}

/// State for MetadataProtocolReader consumer KDF - extracts metadata and protocol actions.
///
/// This is a **Consumer KDF** that processes the scan results of log files (JSON commits
/// or Parquet checkpoints) and extracts the first non-null metadata and protocol actions
/// needed for snapshot construction.
///
/// Uses interior mutability via `Arc<Mutex<>>` to allow `apply(&self)` signature.
///
/// This follows the same pattern as `MetadataVisitor` and `ProtocolVisitor` - we use the
/// existing `Metadata::try_new_from_data` and `Protocol::try_new_from_data` methods which
/// guarantee atomic extraction (either we get a complete action or nothing).
///
/// # Usage Pattern
///
/// 1. Create state
/// 2. Feed scanned rows from log files through `apply()`
/// 3. `apply()` returns `Ok(true)` to continue, `Ok(false)` after extracting both actions
/// 4. After completion, extract the metadata and protocol via `take_*` accessors
#[derive(Debug, Clone)]
pub struct MetadataProtocolReaderState {
    inner: Arc<Mutex<MetadataProtocolReaderInner>>,
}

impl Default for MetadataProtocolReaderState {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataProtocolReaderState {
    /// Create new state for reading metadata and protocol.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetadataProtocolReaderInner::default())),
        }
    }

    /// Apply consumer to a batch of log file data.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data, still looking for protocol/metadata)
    /// - `Ok(false)` = Break (found both protocol and metadata, or error)
    ///
    /// Uses the existing `Protocol::try_new_from_data` and `Metadata::try_new_from_data`
    /// visitor methods which guarantee atomic extraction - we get a complete action or nothing.
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        let mut inner = self.inner.lock().unwrap();

        // If we already have both, stop
        if inner.protocol.is_some() && inner.metadata.is_some() {
            return Ok(false);
        }

        // If we have an error, stop processing
        if inner.error.is_some() {
            return Ok(false);
        }

        // Try to extract protocol if not found yet
        if inner.protocol.is_none() {
            match Protocol::try_new_from_data(batch) {
                Ok(Some(protocol)) => {
                    inner.protocol = Some(protocol);
                }
                Ok(None) => {
                    // No protocol in this batch, continue
                }
                Err(e) => {
                    inner.error = Some(format!("Failed to extract protocol: {}", e));
                    return Ok(false);
                }
            }
        }

        // Try to extract metadata if not found yet
        if inner.metadata.is_none() {
            match Metadata::try_new_from_data(batch) {
                Ok(Some(metadata)) => {
                    inner.metadata = Some(metadata);
                }
                Ok(None) => {
                    // No metadata in this batch, continue
                }
                Err(e) => {
                    inner.error = Some(format!("Failed to extract metadata: {}", e));
                    return Ok(false);
                }
            }
        }

        // Return false (Break) if we found both, otherwise continue
        Ok(!(inner.protocol.is_some() && inner.metadata.is_some()))
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&self) {
        // Nothing special needed for finalization
    }

    /// Check if both protocol and metadata were successfully extracted.
    pub fn is_complete(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.protocol.is_some() && inner.metadata.is_some()
    }

    /// Check if protocol was found.
    pub fn has_protocol(&self) -> bool {
        self.inner.lock().unwrap().protocol.is_some()
    }

    /// Check if metadata was found.
    pub fn has_metadata(&self) -> bool {
        self.inner.lock().unwrap().metadata.is_some()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Take the extracted protocol, if any.
    pub fn take_protocol(&self) -> Option<Protocol> {
        self.inner.lock().unwrap().protocol.take()
    }

    /// Take the extracted metadata, if any.
    pub fn take_metadata(&self) -> Option<Metadata> {
        self.inner.lock().unwrap().metadata.take()
    }

    /// Get a clone of the extracted protocol, if any.
    pub fn get_protocol(&self) -> Option<Protocol> {
        self.inner.lock().unwrap().protocol.clone()
    }

    /// Get a clone of the extracted metadata, if any.
    pub fn get_metadata(&self) -> Option<Metadata> {
        self.inner.lock().unwrap().metadata.clone()
    }

    // Test-only accessors for internal state
    #[cfg(test)]
    pub fn protocol(&self) -> Option<Protocol> {
        self.inner.lock().unwrap().protocol.clone()
    }

    #[cfg(test)]
    pub fn metadata(&self) -> Option<Metadata> {
        self.inner.lock().unwrap().metadata.clone()
    }

    #[cfg(test)]
    pub fn error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.clone()
    }
}

// =============================================================================
// SidecarCollectorState - Collects sidecar file paths from manifest scan
// =============================================================================

/// Inner mutable state for SidecarCollector.
#[derive(Debug)]
struct SidecarCollectorInner {
    /// Log directory root URL (used to construct full sidecar file paths)
    log_root: Url,
    /// Collected sidecar files
    sidecar_files: Vec<FileMeta>,
    /// Stored error to surface during advance()
    error: Option<String>,
}

/// State for SidecarCollector consumer KDF - collects sidecar file paths from V2 checkpoint manifest.
///
/// This is a **Consumer KDF** that processes the scan results of a V2 checkpoint manifest
/// and accumulates the sidecar file paths for subsequent checkpoint leaf reading.
///
/// Uses interior mutability via `Arc<Mutex<>>` to allow `apply(&self)` signature.
///
/// # Usage Pattern
///
/// 1. Create state with `log_root` URL
/// 2. Feed scanned rows from the checkpoint manifest through `apply()`
/// 3. `apply()` returns `Ok(true)` to continue processing more rows
/// 4. After completion, extract the sidecar files via `take_sidecar_files()`
#[derive(Debug, Clone)]
pub struct SidecarCollectorState {
    inner: Arc<Mutex<SidecarCollectorInner>>,
}

impl SidecarCollectorState {
    /// Create new state for collecting sidecar files.
    ///
    /// # Arguments
    /// * `log_root` - The log directory root URL
    pub fn new(log_root: Url) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SidecarCollectorInner {
                log_root,
                sidecar_files: Vec::new(),
                error: None,
            })),
        }
    }

    /// Apply consumer to a batch of manifest data.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data)
    /// - `Ok(false)` = Break (stop iteration, possibly due to error)
    ///
    /// The batch is expected to contain sidecar action data with columns:
    /// - `sidecar.path`: String - relative path to sidecar file
    /// - `sidecar.sizeInBytes`: Long - size of sidecar file in bytes
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::actions::visitors::SidecarVisitor;
        use crate::engine_data::RowVisitor as _;

        let mut inner = self.inner.lock().unwrap();

        // If we already have an error, stop processing
        if inner.error.is_some() {
            return Ok(false);
        }

        // Reuse the canonical sidecar extractor used elsewhere in the kernel.
        let mut visitor = SidecarVisitor::default();
        visitor.visit_rows_of(batch)?;

        // Clone log_root before the loop to avoid borrow conflicts
        let log_root = inner.log_root.clone();
        for sidecar in visitor.sidecars {
            inner.sidecar_files.push(sidecar.to_filemeta(&log_root)?);
        }

        Ok(true) // Continue processing
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&self) {
        // Nothing special needed for finalization
    }

    /// Check if any sidecar files were collected.
    pub fn has_sidecars(&self) -> bool {
        !self.inner.lock().unwrap().sidecar_files.is_empty()
    }

    /// Get the number of collected sidecar files.
    pub fn sidecar_count(&self) -> usize {
        self.inner.lock().unwrap().sidecar_files.len()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Take the collected sidecar files.
    pub fn take_sidecar_files(&self) -> Vec<FileMeta> {
        std::mem::take(&mut self.inner.lock().unwrap().sidecar_files)
    }

    /// Get a clone of the collected sidecar files.
    pub fn get_sidecar_files(&self) -> Vec<FileMeta> {
        self.inner.lock().unwrap().sidecar_files.clone()
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
    /// Extracts checkpoint hint from _last_checkpoint scan
    CheckpointHintReader(CheckpointHintReaderState),
    /// Extracts metadata and protocol from log files
    MetadataProtocolReader(MetadataProtocolReaderState),
    /// Collects sidecar file paths from V2 checkpoint manifest
    SidecarCollector(SidecarCollectorState),
}

impl ConsumerKdfState {
    /// Apply consumer to a batch.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data)
    /// - `Ok(false)` = Break (stop iteration)
    ///
    /// Uses interior mutability - takes `&self` to enable use in lazy iterators.
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        match self {
            Self::LogSegmentBuilder(state) => state.apply(batch),
            Self::CheckpointHintReader(state) => state.apply(batch),
            Self::MetadataProtocolReader(state) => state.apply(batch),
            Self::SidecarCollector(state) => state.apply(batch),
        }
    }

    /// Finalize the consumer state after iteration completes.
    pub fn finalize(&self) {
        match self {
            Self::LogSegmentBuilder(state) => state.finalize(),
            Self::CheckpointHintReader(state) => state.finalize(),
            Self::MetadataProtocolReader(state) => state.finalize(),
            Self::SidecarCollector(state) => state.finalize(),
        }
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        match self {
            Self::LogSegmentBuilder(state) => state.has_error(),
            Self::CheckpointHintReader(state) => state.has_error(),
            Self::MetadataProtocolReader(state) => state.has_error(),
            Self::SidecarCollector(state) => state.has_error(),
        }
    }

    /// Take the error as a DeltaResult, if any.
    pub fn take_error(&self) -> Option<Error> {
        match self {
            Self::LogSegmentBuilder(state) => {
                state.take_error().map(Error::generic)
            }
            Self::CheckpointHintReader(state) => {
                state.take_error().map(Error::generic)
            }
            Self::MetadataProtocolReader(state) => {
                state.take_error().map(Error::generic)
            }
            Self::SidecarCollector(state) => {
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
    /// Prunes add actions using partition values (add.partitionValues) and the scan predicate
    PartitionPrune(PartitionPruneState),
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
            Self::PartitionPrune(state) => state.apply(batch, selection),
        }
    }

    /// Serialize state for distribution.
    pub fn serialize(&self) -> DeltaResult<Vec<u8>> {
        match self {
            Self::AddRemoveDedup(state) => state.serialize(),
            Self::CheckpointDedup(state) => state.serialize(),
            // PartitionPrune is not distributable today; if needed, implement serde/arrow IPC for its fields.
            Self::PartitionPrune(_) => Err(Error::generic(
                "PartitionPruneState serialization is not supported",
            )),
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
    use crate::arrow::array::{Int32Array, RecordBatch, StringArray, StructArray};
    use crate::arrow::datatypes::{DataType, Field, Fields, Schema};
    use crate::engine::arrow_data::ArrowEngineData;
    use std::sync::Arc;

    fn create_test_batch(paths: &[&str]) -> ArrowEngineData {
        let len = paths.len();

        // add.path
        let add_path_array = StringArray::from(paths.to_vec());

        // add.deletionVector.{storageType,pathOrInlineDv,offset} (all nulls)
        let add_dv_storage = StringArray::from(vec![None::<&str>; len]);
        let add_dv_path = StringArray::from(vec![None::<&str>; len]);
        let add_dv_offset = Int32Array::from(vec![None::<i32>; len]);
        let add_dv_fields = Fields::from(vec![
            Field::new("storageType", DataType::Utf8, true),
            Field::new("pathOrInlineDv", DataType::Utf8, true),
            Field::new("offset", DataType::Int32, true),
        ]);
        let add_dv_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("storageType", DataType::Utf8, true)),
                Arc::new(add_dv_storage) as Arc<dyn crate::arrow::array::Array>,
            ),
            (
                Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                Arc::new(add_dv_path) as Arc<dyn crate::arrow::array::Array>,
            ),
            (
                Arc::new(Field::new("offset", DataType::Int32, true)),
                Arc::new(add_dv_offset) as Arc<dyn crate::arrow::array::Array>,
            ),
        ]);

        let add_fields = Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("deletionVector", DataType::Struct(add_dv_fields.clone()), true),
        ]);
        let add_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(add_path_array) as Arc<dyn crate::arrow::array::Array>,
            ),
            (
                Arc::new(Field::new(
                    "deletionVector",
                    DataType::Struct(add_dv_fields.clone()),
                    true,
                )),
                Arc::new(add_dv_struct) as Arc<dyn crate::arrow::array::Array>,
            ),
        ]);

        // remove struct (all nulls)
        let remove_path = StringArray::from(vec![None::<&str>; len]);
        let remove_dv_storage = StringArray::from(vec![None::<&str>; len]);
        let remove_dv_path = StringArray::from(vec![None::<&str>; len]);
        let remove_dv_offset = Int32Array::from(vec![None::<i32>; len]);
        let remove_dv_fields = Fields::from(vec![
            Field::new("storageType", DataType::Utf8, true),
            Field::new("pathOrInlineDv", DataType::Utf8, true),
            Field::new("offset", DataType::Int32, true),
        ]);
        let remove_dv_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("storageType", DataType::Utf8, true)),
                Arc::new(remove_dv_storage) as Arc<dyn crate::arrow::array::Array>,
            ),
            (
                Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                Arc::new(remove_dv_path) as Arc<dyn crate::arrow::array::Array>,
            ),
            (
                Arc::new(Field::new("offset", DataType::Int32, true)),
                Arc::new(remove_dv_offset) as Arc<dyn crate::arrow::array::Array>,
            ),
        ]);
        let remove_fields = Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("deletionVector", DataType::Struct(remove_dv_fields.clone()), true),
        ]);
        let remove_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(remove_path) as Arc<dyn crate::arrow::array::Array>,
            ),
            (
                Arc::new(Field::new(
                    "deletionVector",
                    DataType::Struct(remove_dv_fields.clone()),
                    true,
                )),
                Arc::new(remove_dv_struct) as Arc<dyn crate::arrow::array::Array>,
            ),
        ]);

        let schema = Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields), true),
            Field::new("remove", DataType::Struct(remove_fields), true),
        ]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(add_struct), Arc::new(remove_struct)])
                .unwrap();
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
        let state = LogSegmentBuilderState::new(log_root.clone(), None, None);

        assert_eq!(state.log_root(), log_root);
        assert!(state.end_version().is_none());
        assert!(state.ascending_commit_files().is_empty());
        assert!(state.checkpoint_parts().is_empty());
        assert!(state.error().is_none());
    }

    #[test]
    fn test_log_segment_builder_state_with_end_version() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state = LogSegmentBuilderState::new(log_root.clone(), Some(10), None);

        assert_eq!(state.log_root(), log_root);
        assert_eq!(state.end_version(), Some(10));
    }

    #[test]
    fn test_log_segment_builder_empty_batch() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state = LogSegmentBuilderState::new(log_root, None, None);

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
        let state = LogSegmentBuilderState::new(log_root, None, None);

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

        let commit_files = state.ascending_commit_files();
        assert_eq!(commit_files.len(), 3);
        assert_eq!(commit_files[0].version, 0);
        assert_eq!(commit_files[1].version, 1);
        assert_eq!(commit_files[2].version, 2);
        
        // Latest commit should be version 2
        let latest = state.latest_commit_file();
        assert!(latest.is_some());
        assert_eq!(latest.as_ref().unwrap().version, 2);
    }

    #[test]
    fn test_log_segment_builder_multiple_batches() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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

        assert_eq!(state.ascending_commit_files().len(), 4);
        assert_eq!(state.latest_commit_file().as_ref().unwrap().version, 3);
    }

    // =========================================================================
    // Checkpoint File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_single_part_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.checkpoint_parts().len(), 1);
        assert_eq!(state.checkpoint_parts()[0].version, 2);
        
        // Commits before checkpoint are cleared, only v3 remains
        assert_eq!(state.ascending_commit_files().len(), 1);
        assert_eq!(state.ascending_commit_files()[0].version, 3);
        
        // Latest commit is v3
        assert_eq!(state.latest_commit_file().as_ref().unwrap().version, 3);
    }

    #[test]
    fn test_log_segment_builder_multipart_checkpoint_complete() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.checkpoint_parts().len(), 3);
        assert!(state.checkpoint_parts().iter().all(|p| p.version == 2));
        
        // Only commit after checkpoint remains
        assert_eq!(state.ascending_commit_files().len(), 1);
        assert_eq!(state.ascending_commit_files()[0].version, 3);
    }

    #[test]
    fn test_log_segment_builder_multipart_checkpoint_incomplete() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert!(state.checkpoint_parts().is_empty());
        
        // All commits should be kept (v0, v1, v3 - no v2 commit in the test data)
        assert_eq!(state.ascending_commit_files().len(), 3);
    }

    #[test]
    fn test_log_segment_builder_checkpoint_clears_earlier_commits() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.checkpoint_parts().len(), 1);
        assert_eq!(state.checkpoint_parts()[0].version, 5);
        
        // Only commits after checkpoint (v6, v7) remain
        assert_eq!(state.ascending_commit_files().len(), 2);
        assert_eq!(state.ascending_commit_files()[0].version, 6);
        assert_eq!(state.ascending_commit_files()[1].version, 7);
        
        // Latest commit is v7
        assert_eq!(state.latest_commit_file().as_ref().unwrap().version, 7);
    }

    #[test]
    fn test_log_segment_builder_later_checkpoint_replaces_earlier() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.checkpoint_parts().len(), 1);
        assert_eq!(state.checkpoint_parts()[0].version, 5);
        
        // Only commit after v5 checkpoint remains
        assert_eq!(state.ascending_commit_files().len(), 1);
        assert_eq!(state.ascending_commit_files()[0].version, 6);
    }

    // =========================================================================
    // CRC File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_crc_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000000.crc", 100, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000001.crc", 100, 200),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Latest CRC should be version 1
        assert!(state.latest_crc_file().is_some());
        assert_eq!(state.latest_crc_file().as_ref().unwrap().version, 1);
    }

    // =========================================================================
    // Compaction File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_compaction_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            // Compacted commits from version 0-5
            ("00000000000000000000.00000000000000000005.compacted.json", 3000, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        assert_eq!(state.ascending_compaction_files().len(), 1);
        assert_eq!(state.ascending_compaction_files()[0].version, 0);
    }

    #[test]
    fn test_log_segment_builder_compaction_respects_end_version() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        // End version is 3, compaction goes to 5
        let mut state = LogSegmentBuilderState::new(log_root, Some(3), None);

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
        assert_eq!(state.ascending_compaction_files().len(), 1);
    }

    // =========================================================================
    // End Version Filtering Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_end_version_stops_processing() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, Some(2), None);

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
        assert_eq!(state.ascending_commit_files().len(), 3);
        assert_eq!(state.ascending_commit_files()[2].version, 2);
    }

    #[test]
    fn test_log_segment_builder_end_version_with_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, Some(5), None);

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
        assert_eq!(state.checkpoint_parts().len(), 1);
        assert_eq!(state.checkpoint_parts()[0].version, 2);
        
        // Commits v3, v4, v5 after checkpoint should be kept
        assert_eq!(state.ascending_commit_files().len(), 3);
    }

    // =========================================================================
    // Unknown and Invalid File Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_unknown_files_ignored() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.ascending_commit_files().len(), 3);
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    #[test]
    fn test_log_segment_builder_error_handling() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state = LogSegmentBuilderState::new(log_root, None, None);

        // Manually set an error
        state.set_error("Test error".to_string());

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
            ConsumerKdfState::LogSegmentBuilder(LogSegmentBuilderState::new(log_root, None, None));

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
            ConsumerKdfState::CheckpointHintReader(s) => {
                assert!(!s.has_error());
            }
            ConsumerKdfState::MetadataProtocolReader(s) => {
                assert!(!s.has_error());
            }
            ConsumerKdfState::SidecarCollector(s) => {
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
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.checkpoint.parquet", 5000, 100),
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // Checkpoint at v0
        assert_eq!(state.checkpoint_parts().len(), 1);
        assert_eq!(state.checkpoint_parts()[0].version, 0);
        
        // Only v1 commit after checkpoint
        assert_eq!(state.ascending_commit_files().len(), 1);
        assert_eq!(state.ascending_commit_files()[0].version, 1);
    }

    #[test]
    fn test_log_segment_builder_only_checkpoint_no_commits_after() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.checkpoint_parts().len(), 1);
        
        // No commits after checkpoint
        assert!(state.ascending_commit_files().is_empty());
        
        // But latest_commit_file should be v2 (at checkpoint version)
        assert!(state.latest_commit_file().is_some());
        assert_eq!(state.latest_commit_file().as_ref().unwrap().version, 2);
    }

    #[test]
    fn test_log_segment_builder_no_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

        let batch = create_file_listing_batch(&[
            ("00000000000000000000.json", 1000, 100),
            ("00000000000000000001.json", 1100, 200),
            ("00000000000000000002.json", 1200, 300),
        ]);

        assert!(state.apply(&batch).unwrap());
        state.finalize();

        // No checkpoint
        assert!(state.checkpoint_parts().is_empty());
        
        // All commits kept
        assert_eq!(state.ascending_commit_files().len(), 3);
        
        // Latest commit is v2
        assert_eq!(state.latest_commit_file().as_ref().unwrap().version, 2);
    }

    #[test]
    fn test_log_segment_builder_uuid_checkpoint() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = LogSegmentBuilderState::new(log_root, None, None);

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
        assert_eq!(state.checkpoint_parts().len(), 1);
        assert_eq!(state.checkpoint_parts()[0].version, 2);
        
        // Commits after checkpoint
        assert_eq!(state.ascending_commit_files().len(), 1);
        assert_eq!(state.ascending_commit_files()[0].version, 3);
    }

    // =========================================================================
    // CheckpointHintReaderState Tests
    // =========================================================================

    /// Helper function to create a checkpoint hint batch
    fn create_checkpoint_hint_batch(
        version: Option<i64>,
        size: Option<i64>,
        parts: Option<i32>,
        size_in_bytes: Option<i64>,
        num_of_add_files: Option<i64>,
    ) -> ArrowEngineData {
        use crate::arrow::array::{Int32Array, Int64Array, RecordBatch};
        use crate::arrow::datatypes::{DataType, Field, Schema};

        let schema = Schema::new(vec![
            Field::new("version", DataType::Int64, true),
            Field::new("size", DataType::Int64, true),
            Field::new("parts", DataType::Int32, true),
            Field::new("sizeInBytes", DataType::Int64, true),
            Field::new("numOfAddFiles", DataType::Int64, true),
        ]);

        let version_array: Int64Array = vec![version].into();
        let size_array: Int64Array = vec![size].into();
        let parts_array: Int32Array = vec![parts].into();
        let size_in_bytes_array: Int64Array = vec![size_in_bytes].into();
        let num_of_add_files_array: Int64Array = vec![num_of_add_files].into();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(version_array),
                Arc::new(size_array),
                Arc::new(parts_array),
                Arc::new(size_in_bytes_array),
                Arc::new(num_of_add_files_array),
            ],
        )
        .unwrap();

        ArrowEngineData::new(batch)
    }

    /// Helper function to create an empty checkpoint hint batch (simulates file not found)
    fn create_empty_checkpoint_hint_batch() -> ArrowEngineData {
        use crate::arrow::array::{Int32Array, Int64Array, RecordBatch};
        use crate::arrow::datatypes::{DataType, Field, Schema};

        let schema = Schema::new(vec![
            Field::new("version", DataType::Int64, true),
            Field::new("size", DataType::Int64, true),
            Field::new("parts", DataType::Int32, true),
            Field::new("sizeInBytes", DataType::Int64, true),
            Field::new("numOfAddFiles", DataType::Int64, true),
        ]);

        // Empty arrays
        let version_array = Int64Array::from(Vec::<Option<i64>>::new());
        let size_array = Int64Array::from(Vec::<Option<i64>>::new());
        let parts_array = Int32Array::from(Vec::<Option<i32>>::new());
        let size_in_bytes_array = Int64Array::from(Vec::<Option<i64>>::new());
        let num_of_add_files_array = Int64Array::from(Vec::<Option<i64>>::new());

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(version_array),
                Arc::new(size_array),
                Arc::new(parts_array),
                Arc::new(size_in_bytes_array),
                Arc::new(num_of_add_files_array),
            ],
        )
        .unwrap();

        ArrowEngineData::new(batch)
    }

    #[test]
    fn test_checkpoint_hint_reader_file_not_found_empty_batch() {
        // Simulates when _last_checkpoint file is not found - scanner returns empty batch
        let mut state = CheckpointHintReaderState::new();

        let batch = create_empty_checkpoint_hint_batch();
        
        // Empty batch should return Ok(true) to continue (maybe more batches coming)
        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(result.unwrap()); // true = continue
        
        // No hint should be extracted
        assert!(!state.has_hint());
        assert!(state.version().is_none());
        assert!(!state.has_error());
    }

    #[test]
    fn test_checkpoint_hint_reader_file_not_found_no_batches() {
        // Simulates when _last_checkpoint file is not found - no batches at all
        let state = CheckpointHintReaderState::new();

        // Never called apply - state should be clean
        assert!(!state.has_hint());
        assert!(state.version().is_none());
        assert!(!state.has_error());
        assert!(state.get_version().is_none());
    }

    #[test]
    fn test_checkpoint_hint_reader_valid_hint() {
        let mut state = CheckpointHintReaderState::new();

        let batch = create_checkpoint_hint_batch(
            Some(42),   // version
            Some(1000), // size
            Some(3),    // parts
            Some(5000), // sizeInBytes
            Some(500),  // numOfAddFiles
        );

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = break, we got the hint

        // All fields should be extracted
        assert!(state.has_hint());
        assert_eq!(state.version(), Some(42));
        assert_eq!(state.size(), Some(1000));
        assert_eq!(state.parts(), Some(3));
        assert_eq!(state.size_in_bytes(), Some(5000));
        assert_eq!(state.num_of_add_files(), Some(500));
        assert!(!state.has_error());
    }

    #[test]
    fn test_checkpoint_hint_reader_minimal_hint() {
        // Only version is present (minimal valid hint)
        let mut state = CheckpointHintReaderState::new();

        let batch = create_checkpoint_hint_batch(
            Some(10), // version
            None,     // size
            None,     // parts
            None,     // sizeInBytes
            None,     // numOfAddFiles
        );

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = break

        assert!(state.has_hint());
        assert_eq!(state.version(), Some(10));
        assert!(state.size().is_none());
        assert!(state.parts().is_none());
    }

    #[test]
    fn test_checkpoint_hint_reader_null_version() {
        // Version is null - this is invalid, no hint should be extracted
        let mut state = CheckpointHintReaderState::new();

        let batch = create_checkpoint_hint_batch(
            None,       // version is null
            Some(1000), // size
            Some(3),    // parts
            None,
            None,
        );

        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = processed but no valid version

        // No valid hint (version is required)
        assert!(!state.has_hint());
        assert!(state.version().is_none());
    }

    #[test]
    fn test_checkpoint_hint_reader_multiple_empty_batches() {
        // Multiple empty batches followed by valid data
        let mut state = CheckpointHintReaderState::new();

        // First empty batch
        let empty_batch = create_empty_checkpoint_hint_batch();
        let result = state.apply(&empty_batch);
        assert!(result.unwrap()); // true = continue

        // Second empty batch
        let result = state.apply(&empty_batch);
        assert!(result.unwrap()); // true = continue

        // No hint yet
        assert!(!state.has_hint());

        // Valid batch
        let valid_batch = create_checkpoint_hint_batch(Some(100), Some(500), None, None, None);
        let result = state.apply(&valid_batch);
        assert!(!result.unwrap()); // false = got hint

        // Now we have the hint
        assert!(state.has_hint());
        assert_eq!(state.version(), Some(100));
    }

    #[test]
    fn test_checkpoint_hint_reader_only_processes_first_row() {
        // If somehow we get multiple rows, only the first should be processed
        let mut state = CheckpointHintReaderState::new();

        // Create batch with version 42
        let batch = create_checkpoint_hint_batch(Some(42), None, None, None, None);
        let result = state.apply(&batch);
        assert!(!result.unwrap()); // false = got hint

        // Try to apply another batch - should be ignored
        let batch2 = create_checkpoint_hint_batch(Some(99), None, None, None, None);
        let result = state.apply(&batch2);
        assert!(!result.unwrap()); // false = already processed

        // Original version should be retained
        assert_eq!(state.version(), Some(42));
    }

    #[test]
    fn test_checkpoint_hint_reader_finalize() {
        let mut state = CheckpointHintReaderState::new();

        let batch = create_checkpoint_hint_batch(Some(50), Some(100), None, None, None);
        state.apply(&batch).unwrap();
        
        // Finalize should not change anything
        state.finalize();

        assert!(state.has_hint());
        assert_eq!(state.version(), Some(50));
    }

    #[test]
    fn test_checkpoint_hint_reader_get_accessors() {
        let mut state = CheckpointHintReaderState::new();

        // Before applying
        assert!(state.get_version().is_none());
        assert!(state.get_parts().is_none());

        // Apply valid batch
        let batch = create_checkpoint_hint_batch(Some(25), None, Some(5), None, None);
        state.apply(&batch).unwrap();

        // After applying
        assert_eq!(state.get_version(), Some(25));
        assert_eq!(state.get_parts(), Some(5));
    }

    #[test]
    fn test_checkpoint_hint_consumer_kdf_state_empty_batch() {
        // Test through the ConsumerKdfState wrapper
        let mut state = ConsumerKdfState::CheckpointHintReader(CheckpointHintReaderState::new());

        let batch = create_empty_checkpoint_hint_batch();
        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(result.unwrap()); // true = continue

        assert!(!state.has_error());
    }

    #[test]
    fn test_checkpoint_hint_consumer_kdf_state_valid_batch() {
        let mut state = ConsumerKdfState::CheckpointHintReader(CheckpointHintReaderState::new());

        let batch = create_checkpoint_hint_batch(Some(77), Some(200), Some(2), None, None);
        let result = state.apply(&batch);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = got hint

        state.finalize();
        assert!(!state.has_error());

        // Verify hint was extracted
        if let ConsumerKdfState::CheckpointHintReader(inner) = state {
            assert!(inner.has_hint());
            assert_eq!(inner.version(), Some(77));
            assert_eq!(inner.parts(), Some(2));
        }
    }

    // =========================================================================
    // MetadataProtocolReaderState Tests
    // =========================================================================

    /// Helper to create a batch with protocol and/or metadata actions
    fn create_protocol_metadata_batch(
        include_protocol: bool,
        include_metadata: bool,
    ) -> Box<dyn crate::EngineData> {
        use crate::arrow::array::StringArray;
        use crate::utils::test_utils::parse_json_batch;

        let mut json_strings = Vec::new();

        if include_protocol {
            json_strings.push(
                r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#
            );
        }

        if include_metadata {
            json_strings.push(
                r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"},"createdTime":1677811175819}}"#
            );
        }

        // Add a filler action if we have no protocol/metadata to avoid empty batch
        if json_strings.is_empty() {
            json_strings.push(
                r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE"}}"#
            );
        }

        let string_array: StringArray = json_strings.into();
        parse_json_batch(string_array)
    }

    #[test]
    fn test_metadata_protocol_reader_new() {
        let state = MetadataProtocolReaderState::new();
        assert!(state.protocol().is_none());
        assert!(state.metadata().is_none());
        assert!(!state.is_complete());
        assert!(!state.has_protocol());
        assert!(!state.has_metadata());
        assert!(!state.has_error());
    }

    #[test]
    fn test_metadata_protocol_reader_extracts_both() {
        let mut state = MetadataProtocolReaderState::new();

        // Batch with both protocol and metadata
        let batch = create_protocol_metadata_batch(true, true);
        let result = state.apply(batch.as_ref());

        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = Break, we found both

        assert!(state.is_complete());
        assert!(state.has_protocol());
        assert!(state.has_metadata());
        assert!(!state.has_error());

        // Verify protocol was extracted correctly
        let protocol = state.get_protocol().unwrap();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);

        // Verify metadata was extracted correctly
        let metadata = state.get_metadata().unwrap();
        assert_eq!(metadata.id(), "testId");
    }

    #[test]
    fn test_metadata_protocol_reader_protocol_only() {
        let mut state = MetadataProtocolReaderState::new();

        // Batch with only protocol
        let batch = create_protocol_metadata_batch(true, false);
        let result = state.apply(batch.as_ref());

        assert!(result.is_ok());
        assert!(result.unwrap()); // true = Continue, still need metadata

        assert!(!state.is_complete());
        assert!(state.has_protocol());
        assert!(!state.has_metadata());
    }

    #[test]
    fn test_metadata_protocol_reader_metadata_only() {
        let mut state = MetadataProtocolReaderState::new();

        // Batch with only metadata
        let batch = create_protocol_metadata_batch(false, true);
        let result = state.apply(batch.as_ref());

        assert!(result.is_ok());
        assert!(result.unwrap()); // true = Continue, still need protocol

        assert!(!state.is_complete());
        assert!(!state.has_protocol());
        assert!(state.has_metadata());
    }

    #[test]
    fn test_metadata_protocol_reader_empty_batch() {
        let mut state = MetadataProtocolReaderState::new();

        // Batch with neither protocol nor metadata (just commitInfo)
        let batch = create_protocol_metadata_batch(false, false);
        let result = state.apply(batch.as_ref());

        assert!(result.is_ok());
        assert!(result.unwrap()); // true = Continue, need both

        assert!(!state.is_complete());
        assert!(!state.has_protocol());
        assert!(!state.has_metadata());
    }

    #[test]
    fn test_metadata_protocol_reader_multiple_batches() {
        let mut state = MetadataProtocolReaderState::new();

        // First batch: only protocol
        let batch1 = create_protocol_metadata_batch(true, false);
        let result = state.apply(batch1.as_ref());
        assert!(result.unwrap()); // true = Continue

        assert!(state.has_protocol());
        assert!(!state.has_metadata());

        // Second batch: only metadata
        let batch2 = create_protocol_metadata_batch(false, true);
        let result = state.apply(batch2.as_ref());
        assert!(!result.unwrap()); // false = Break, now have both

        assert!(state.is_complete());
        assert!(state.has_protocol());
        assert!(state.has_metadata());
    }

    #[test]
    fn test_metadata_protocol_reader_stops_after_complete() {
        let mut state = MetadataProtocolReaderState::new();

        // First batch: both protocol and metadata
        let batch = create_protocol_metadata_batch(true, true);
        let result = state.apply(batch.as_ref());
        assert!(!result.unwrap()); // false = Break

        // Get the extracted values
        let protocol_version = state.get_protocol().unwrap().min_reader_version();
        let metadata_id = state.get_metadata().unwrap().id().to_string();

        // Try to apply another batch - should be ignored
        let batch2 = create_protocol_metadata_batch(true, true);
        let result = state.apply(batch2.as_ref());
        assert!(!result.unwrap()); // false = already complete

        // Values should be unchanged
        assert_eq!(state.get_protocol().unwrap().min_reader_version(), protocol_version);
        assert_eq!(state.get_metadata().unwrap().id(), metadata_id);
    }

    #[test]
    fn test_metadata_protocol_reader_take_methods() {
        let mut state = MetadataProtocolReaderState::new();

        // Extract both
        let batch = create_protocol_metadata_batch(true, true);
        state.apply(batch.as_ref()).unwrap();

        assert!(state.is_complete());

        // Take protocol
        let protocol = state.take_protocol();
        assert!(protocol.is_some());
        assert!(state.protocol().is_none()); // Now None after take
        assert!(!state.has_protocol());

        // Take metadata
        let metadata = state.take_metadata();
        assert!(metadata.is_some());
        assert!(state.metadata().is_none()); // Now None after take
        assert!(!state.has_metadata());

        // Second take returns None
        assert!(state.take_protocol().is_none());
        assert!(state.take_metadata().is_none());
    }

    #[test]
    fn test_metadata_protocol_reader_consumer_kdf_state() {
        // Test through the ConsumerKdfState wrapper
        let mut state = ConsumerKdfState::MetadataProtocolReader(MetadataProtocolReaderState::new());

        // Batch with both
        let batch = create_protocol_metadata_batch(true, true);
        let result = state.apply(batch.as_ref());

        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = Break

        state.finalize();
        assert!(!state.has_error());

        // Verify extraction
        if let ConsumerKdfState::MetadataProtocolReader(inner) = state {
            assert!(inner.is_complete());
            assert!(inner.has_protocol());
            assert!(inner.has_metadata());
        } else {
            panic!("Expected MetadataProtocolReader variant");
        }
    }

    #[test]
    fn test_metadata_protocol_reader_consumer_kdf_state_partial() {
        // Test partial extraction through wrapper
        let mut state = ConsumerKdfState::MetadataProtocolReader(MetadataProtocolReaderState::new());

        // Batch with only protocol
        let batch = create_protocol_metadata_batch(true, false);
        let result = state.apply(batch.as_ref());

        assert!(result.is_ok());
        assert!(result.unwrap()); // true = Continue

        // Verify partial state
        if let ConsumerKdfState::MetadataProtocolReader(inner) = &state {
            assert!(!inner.is_complete());
            assert!(inner.has_protocol());
            assert!(!inner.has_metadata());
        } else {
            panic!("Expected MetadataProtocolReader variant");
        }
    }
}
