//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.

use std::sync::Arc;

use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::Plan;
use futures::TryStreamExt;

use crate::compile::{compile_plan, CompileContext};
use crate::error::LiftDeltaErr;
use crate::exec::RelationBatchRegistry;

/// Minimal executor: holds a [`TaskContext`] for [`ExecutionPlan::execute`] calls.
///
/// Phase 1.1 does not yet wire a full [`SessionContext`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html);
/// literal execution only needs the default task context.
pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    relation_registry: Arc<RelationBatchRegistry>,
}

impl DataFusionExecutor {
    /// Builds an executor backed by [`TaskContext::default()`].
    pub fn try_new() -> Result<Self, DeltaError> {
        let relation_registry = Arc::new(RelationBatchRegistry::new());
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            relation_registry,
        })
    }

    /// Compile a [`Plan`] into a physical node when Phase 1.1 dispatch accepts it.
    pub fn compile_plan(&self, plan: &Plan) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        compile_plan(
            plan,
            &CompileContext::new(Arc::clone(&self.relation_registry)),
        )
    }

    /// Compile and execute partition `0`.
    pub async fn execute_plan_to_stream(
        &self,
        plan: Plan,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        let physical = self.compile_plan(&plan)?;
        physical.execute(0, Arc::clone(&self.task_ctx)).lift()
    }

    /// Convenience helper for tests / tiny literals.
    pub async fn execute_plan_collect(
        &self,
        plan: Plan,
    ) -> Result<Vec<delta_kernel::arrow::array::RecordBatch>, DeltaError> {
        let stream = self.execute_plan_to_stream(plan).await?;
        stream.try_collect().await.lift()
    }

    pub fn task_context(&self) -> &Arc<TaskContext> {
        &self.task_ctx
    }

    pub fn relation_batch_registry(&self) -> &Arc<RelationBatchRegistry> {
        &self.relation_registry
    }
}
