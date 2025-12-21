//! AddRemoveDedup Filter KDF - deduplicates add/remove actions during commit log replay.

use std::collections::HashSet;

use crate::arrow::array::BooleanArray;
use crate::log_replay::FileActionKey;
use crate::{DeltaResult, EngineData};

use super::super::serialization::{
    deserialize_file_action_keys, selection_value_or_true, serialize_file_action_keys,
};

/// State for AddRemoveDedup KDF - deduplicates add/remove actions during commit log replay.
///
/// Tracks seen file keys (path + optional deletion vector unique ID) and filters out
/// duplicates. Each partition owns its state directly (no locking needed) and states
/// are merged after execution completes.
#[derive(Default, Debug, Clone)]
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
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                // Keep ordering consistent with the indices passed to FileActionDeduplicator below.
                const STRING: DataType = DataType::STRING;
                const INTEGER: DataType = DataType::INTEGER;
                crate::column_names_and_types![
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

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                self.out_selection.resize(row_count, false);
                for i in 0..row_count {
                    if !selection_value_or_true(&self.input_selection, i) {
                        self.out_selection[i] = false;
                        continue;
                    }
                    let Some((file_key, is_add)) = self
                        .dedup
                        .extract_file_action(i, getters, /*skip_removes*/ false)?
                    else {
                        self.out_selection[i] = false;
                        continue;
                    };
                    // Record both adds and removes, but only emit surviving adds.
                    self.out_selection[i] = !self.dedup.check_and_record_seen(file_key) && is_add
                }
                Ok(())
            }
        }

        // Strict: this KDF expects Delta action batches containing add/remove and DV fields.
        let mut full_visitor = DedupVisitor {
            dedup: FileActionDeduplicator::new(
                &mut self.seen_keys,
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
        serialize_file_action_keys(&self.seen_keys)
    }

    /// Deserialize state from bytes (received from driver).
    pub fn deserialize(bytes: &[u8]) -> DeltaResult<Self> {
        let seen_keys = deserialize_file_action_keys(bytes)?;
        Ok(Self { seen_keys })
    }

    /// Check if a key has been seen.
    pub(crate) fn contains(&self, key: &FileActionKey) -> bool {
        self.seen_keys.contains(key)
    }

    /// Insert a key into the seen set.
    pub(crate) fn insert(&mut self, key: FileActionKey) -> bool {
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

    /// Get a clone of the seen keys.
    /// Used to transfer state to CheckpointDedupState for checkpoint phase.
    pub(crate) fn seen_keys(&self) -> HashSet<FileActionKey> {
        self.seen_keys.clone()
    }

    /// Convert to a CheckpointDedupState for the checkpoint phase (borrows and clones keys).
    /// 
    /// Use this when the state may be needed for multiple phases (e.g., JsonCheckpoint
    /// followed by CheckpointLeaf). For single-use cases, prefer `into_checkpoint_dedup_state`.
    pub fn to_checkpoint_dedup_state(&self) -> super::CheckpointDedupState {
        super::CheckpointDedupState::from_hashset(self.seen_keys())
    }

    /// Convert to a CheckpointDedupState by taking ownership (zero-copy).
    ///
    /// Use this when the `AddRemoveDedupState` is no longer needed after conversion.
    /// More efficient than `to_checkpoint_dedup_state` as it avoids cloning the keys.
    pub fn into_checkpoint_dedup_state(self) -> super::CheckpointDedupState {
        super::CheckpointDedupState::from_hashset(self.seen_keys)
    }
}
