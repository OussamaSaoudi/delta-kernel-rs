//! Insert write SM — declarative [`crate::plans::ir::nodes::SinkType::Write`] driven through
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::engine::CoroutineSM).
//!
//! Row counts are **not** produced by the write sink IR; the DataFusion executor submits a
//! synthetic [`FinishedHandle`](crate::plans::kdf::FinishedHandle) keyed by the
//! [`crate::plans::ir::Prepared`] token when executing write sinks
//! (`delta_kernel_datafusion_engine` passes the cloned prepared token via driver options).

use crate::plans::errors::DeltaError;
use crate::plans::ir::{Plan, Prepared};
use crate::plans::kdf::typed::{downcast_all, take_single, ExtractFn};
use crate::plans::kdf::KdfStateToken;
use crate::plans::state_machines::framework::coroutine::engine::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;

/// Stable [`KdfStateToken::new`] prefix for insert telemetry paired with
/// [`insert_write_rows_prepared`].
pub(crate) const INSERT_WRITE_ROWS_KDF_ID: &str = "sm.df.insert.write_rows";

/// Erased telemetry payload merged into
/// [`crate::plans::state_machines::framework::phase_kdf_state::PhaseKdfState`] by the DataFusion
/// executor after draining a write sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRowCount(pub u64);

/// [`Prepared`] extractor wiring for one
/// [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan plus executor-supplied
/// [`WriteRowCount`].
pub fn insert_write_rows_prepared(plan: Plan) -> Prepared<u64> {
    let token = KdfStateToken::new(INSERT_WRITE_ROWS_KDF_ID);
    let tok_clone = token.clone();
    let extract: ExtractFn<u64> = Box::new(move |parts| {
        let xs = downcast_all::<WriteRowCount>(parts, &tok_clone)?;
        let one = take_single(xs, &tok_clone)?;
        Ok(one.0)
    });
    Prepared::new(plan, token, extract)
}

/// Single-phase SM: execute the insert write [`Prepared`] (dispatch + await).
///
/// Must be driven by an executor that submits [`WriteRowCount`] for the prepared token when
/// executing write sinks (see `delta_kernel_datafusion_engine`).
pub fn insert_write_sm(prepared: Prepared<u64>) -> Result<CoroutineSM<u64>, DeltaError> {
    CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);
        phase.execute(prepared, "insert_write").await
    })
}
