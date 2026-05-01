//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! Relation sinks ([`delta_kernel::plans::ir::nodes::SinkType::Relation`]) materialize batches into
//! [`crate::exec::RelationBatchRegistry`] when their stream is drained; subsequent plans read via a
//! [`DeclarativePlanNode::Relation`](delta_kernel::plans::ir::DeclarativePlanNode::Relation) leaf.
//! [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) drains through a
//! [`KernelConsumeByKdfExec`](crate::exec::KernelConsumeByKdfExec); harvest the finalized handle with
//! [`DataFusionExecutor::take_last_kdf_finished`] after fully draining the stream.

use std::sync::{Arc, Mutex};

use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::Plan;
use delta_kernel::plans::kdf::FinishedHandle;
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
    kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
}

impl DataFusionExecutor {
    /// Builds an executor backed by [`TaskContext::default()`].
    pub fn try_new() -> Result<Self, DeltaError> {
        let relation_registry = Arc::new(RelationBatchRegistry::new());
        let kdf_harvest_slot = Arc::new(Mutex::new(None));
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            relation_registry,
            kdf_harvest_slot,
        })
    }

    /// Compile a [`Plan`] into a physical node when Phase 1.1 dispatch accepts it.
    pub fn compile_plan(&self, plan: &Plan) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        compile_plan(
            plan,
            &CompileContext::new(
                Arc::clone(&self.relation_registry),
                Arc::clone(&self.kdf_harvest_slot),
            ),
        )
    }

    /// Compile and execute partition `0`.
    pub async fn execute_plan_to_stream(
        &self,
        plan: Plan,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        {
            let mut guard = self
                .kdf_harvest_slot
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = None;
        }
        let physical = self.compile_plan(&plan)?;
        physical.execute(0, Arc::clone(&self.task_ctx)).lift()
    }

    /// Take the finalized [`FinishedHandle`] produced by the last fully-drained `ConsumeByKdf` sink plan.
    pub fn take_last_kdf_finished(&self) -> Option<FinishedHandle> {
        self.kdf_harvest_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
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
