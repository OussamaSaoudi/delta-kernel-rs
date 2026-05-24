//! Buffered collectors over [`crate::DataFusionExecutor`] used by integration tests.
//!
//! These helpers run a kernel `ResultPlan` (or a `CoroutineSM<ResultPlan>`) end-to-end and
//! return the materialized [`RecordBatch`]es so test code can compare against a frozen
//! expected string without re-implementing the drive/collect dance.

use datafusion_common::arrow::array::RecordBatch;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::plan::ResultPlan;
use delta_kernel::plans::state_machines::framework::coroutine::CoroutineSM;
use futures::TryStreamExt;

use crate::error::DfResultIntoDelta;
use crate::DataFusionExecutor;

/// Compile `rp` via [`DataFusionExecutor::result_plan_to_dataframe`] and collect every
/// batch produced by the resulting `DataFrame` into a `Vec<RecordBatch>`.
pub async fn collect_result_plan(
    executor: &DataFusionExecutor,
    rp: ResultPlan,
) -> Result<Vec<RecordBatch>, DeltaError> {
    let df = executor.result_plan_to_dataframe(&rp)?;
    let stream = df.execute_stream().await.into_delta()?;
    stream.try_collect::<Vec<_>>().await.into_delta()
}

/// Drive `sm` to its terminal `ResultPlan` and collect the materialized batches.
pub async fn collect_coroutine(
    executor: &DataFusionExecutor,
    sm: CoroutineSM<ResultPlan>,
) -> Result<Vec<RecordBatch>, DeltaError> {
    let df = executor.drive_to_dataframe(sm).await?;
    let stream = df.execute_stream().await.into_delta()?;
    stream.try_collect::<Vec<_>>().await.into_delta()
}
