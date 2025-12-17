//! CheckpointDedup Filter KDF - filters out file actions already seen during commit phase.

use std::collections::HashSet;

use crate::arrow::array::BooleanArray;
use crate::log_replay::FileActionKey;
use crate::{DeltaResult, EngineData};

use super::super::serialization::{deserialize_file_action_keys, serialize_file_action_keys, selection_value_or_true};

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
                crate::column_names_and_types![
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


