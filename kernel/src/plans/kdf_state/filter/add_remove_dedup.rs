//! AddRemoveDedup Filter KDF - deduplicates add/remove actions during commit log replay.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::arrow::array::BooleanArray;
use crate::log_replay::FileActionKey;
use crate::{DeltaResult, EngineData, Error};

use super::super::serialization::{deserialize_file_action_keys, serialize_file_action_keys, selection_value_or_true};

/// State for AddRemoveDedup KDF - deduplicates add/remove actions during commit log replay.
///
/// Tracks seen file keys (path + optional deletion vector unique ID) and filters out
/// duplicates. Uses `Arc<Mutex<...>>` for interior mutability so state mutations are
/// visible even when the state is cloned (e.g., when moved into an iterator closure).
#[derive(Debug, Clone)]
pub struct AddRemoveDedupState {
    seen_keys: Arc<Mutex<HashSet<FileActionKey>>>,
}

impl Default for AddRemoveDedupState {
    fn default() -> Self {
        Self {
            seen_keys: Arc::new(Mutex::new(HashSet::new())),
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
        Ok(Self { seen_keys: Arc::new(Mutex::new(seen_keys)) })
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
    pub fn to_checkpoint_dedup_state(&self) -> super::CheckpointDedupState {
        super::CheckpointDedupState::from_hashset(self.seen_keys())
    }
}


