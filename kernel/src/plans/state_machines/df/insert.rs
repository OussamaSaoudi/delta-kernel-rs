//! Insert write SM — declarative [`crate::plans::ir::nodes::SinkType::Write`] driven through
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM).
//!
//! The SM is intentionally trivial: a single phase that yields one
//! [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan and completes once the
//! executor finishes draining it. Per-file `numRecords` (data-skipping stats, row-tracking
//! `baseRowId` assignment, `commitInfo.operationMetrics.numOutputRows`, ...) are produced by the
//! engine inside each `Add` action's `stats` JSON, not via a separate channel back to the SM.

use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::Plan;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;

/// Single-phase SM: drive one [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan
/// to completion.
pub fn insert_write_sm(plan: Plan) -> Result<CoroutineSM<()>, DeltaError> {
    CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);
        phase
            .execute(PhaseOperation::Plans(vec![plan]), "insert_write")
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "insert_write_sm::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        Ok(())
    })
}
