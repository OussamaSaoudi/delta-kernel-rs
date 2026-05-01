//! Insert write SM — declarative [`crate::plans::ir::nodes::SinkType::Write`] driven through
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM).
//!
//! Row counts are **not** produced by the write sink IR; the DataFusion executor submits a
//! synthetic [`FinishedHandle`](crate::plans::kdf::FinishedHandle) keyed by the
//! [`Extractor<u64>`](crate::plans::ir::Extractor) token when executing write sinks
//! (`delta_kernel_datafusion_engine` passes the cloned token via driver options).

use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::{Extractor, Plan};
use crate::plans::kdf::typed::{downcast_all, take_single, ExtractFn};
use crate::plans::kdf::KdfStateToken;
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;

/// Stable [`KdfStateToken::new`] prefix for insert telemetry paired with
/// [`insert_write_extractor`].
pub(crate) const INSERT_WRITE_ROWS_KDF_ID: &str = "sm.df.insert.write_rows";

/// Erased telemetry payload submitted into [`PhaseState`] by the DataFusion executor after
/// draining a write sink.
///
/// [`PhaseState`]: crate::plans::state_machines::framework::phase_state::PhaseState
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRowCount(pub u64);

/// Build an [`Extractor<u64>`] keyed by a  insert-write telemetry token.
///
/// The executor uses the same token (passed through driver options) when submitting the
/// [`WriteRowCount`] for a [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan;
/// this extractor recovers the row count from the resulting
/// [`PhaseState`](crate::plans::state_machines::framework::phase_state::PhaseState).
pub fn insert_write_extractor() -> (KdfStateToken, Extractor<u64>) {
    let token = KdfStateToken::new(INSERT_WRITE_ROWS_KDF_ID);
    let tok_clone = token.clone();
    let extract: ExtractFn<u64> = Box::new(move |parts| {
        let xs = downcast_all::<WriteRowCount>(parts, &tok_clone)?;
        let one = take_single(xs, &tok_clone)?;
        Ok(one.0)
    });
    (token.clone(), Extractor::new(token, extract))
}

/// Single-phase SM: execute one [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write)
/// plan and return the row count the executor submitted as
/// [`WriteRowCount`].
///
/// Returns the [`CoroutineSM`] paired with the telemetry [`KdfStateToken`] the executor uses
/// to attach the row count it observed at the write sink. The kernel does not interpret the
/// token; engines (e.g. the DataFusion executor) plumb it through their own drive options so
/// the resulting [`WriteRowCount`] shows up in
/// [`PhaseState`](crate::plans::state_machines::framework::phase_state::PhaseState) under that
/// token.
pub fn insert_write_sm(plan: Plan) -> Result<(CoroutineSM<u64>, KdfStateToken), DeltaError> {
    let (token, extractorfresh) = insert_write_extractor();
    let sm = CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);
        let state = phase
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
        extractor.extract(&state).map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "insert_write_sm::extract",
                detail = e.display_with_source_chain(),
                source = e,
            )
        })
    })?;
    Ok((sm, token))
}
