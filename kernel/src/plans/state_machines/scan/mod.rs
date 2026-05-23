//! Declarative Full Snapshot Read (FSR): [`full_state`] builds the multi-phase
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM)
//! consumed by [`crate::snapshot::Snapshot::full_state_builder`].

mod file_scan;
pub mod full_state;
mod reconciliation;
mod scan_plan;

pub use full_state::{FullState, FullStateBuilder};
pub use reconciliation::CommitFileMeta;
