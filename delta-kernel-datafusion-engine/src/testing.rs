//! Buffered plan collector for tests and examples.

use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::plans::ir::plan::Plan;
use delta_kernel::Error;

use crate::error::DfResultIntoDelta;
use crate::DataFusionExecutor;

/// Compiles `plan` and collects all output batches.
pub async fn collect_plan(
    executor: &DataFusionExecutor,
    plan: &Plan,
) -> Result<Vec<RecordBatch>, Error> {
    executor
        .plan_to_dataframe(plan)?
        .collect()
        .await
        .into_delta()
}
