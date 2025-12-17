//! Filter KDF states.
//!
//! Filter KDFs process row batches and return selection vectors indicating which rows to keep.

mod add_remove_dedup;
mod checkpoint_dedup;
mod partition_prune;

pub use add_remove_dedup::AddRemoveDedupState;
pub use checkpoint_dedup::CheckpointDedupState;
pub use partition_prune::PartitionPruneState;

use crate::arrow::array::BooleanArray;
use crate::{DeltaResult, EngineData};

/// Filter KDF state - the enum variant IS the function identity.
///
/// No separate `function_id` field is needed. The variant encodes which
/// filter function to apply, and contains the typed state for that function.
#[derive(Debug, Clone)]
pub enum FilterKdfState {
    /// Deduplicates add/remove file actions during commit log replay
    AddRemoveDedup(AddRemoveDedupState),
    /// Deduplicates file actions when reading checkpoint files
    CheckpointDedup(CheckpointDedupState),
    /// Prunes add actions using partition values and the scan predicate
    PartitionPrune(PartitionPruneState),
}

impl FilterKdfState {
    /// Apply the filter - ONE match here, then monomorphized execution.
    ///
    /// Each branch calls the concrete state's apply method, which is inlined
    /// and monomorphized. This avoids per-tuple dispatch overhead.
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
            Self::PartitionPrune(_) => Err(crate::Error::generic(
                "PartitionPruneState serialization is not supported",
            )),
        }
    }
}

// Implement FFI conversion using the macro
crate::impl_ffi_convertible!(FilterKdfState);

