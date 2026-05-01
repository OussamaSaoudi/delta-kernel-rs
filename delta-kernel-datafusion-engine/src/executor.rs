//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! Relation sinks ([`delta_kernel::plans::ir::nodes::SinkType::Relation`]) materialize batches into
//! [`crate::exec::RelationBatchRegistry`] when their stream is drained; subsequent plans read via a
//! [`DeclarativePlanNode::Relation`](delta_kernel::plans::ir::DeclarativePlanNode::Relation) leaf.
//! [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) drains
//! through a [`KernelConsumeByKdfExec`](crate::exec::KernelConsumeByKdfExec); harvest the finalized
//! handle with [`DataFusionExecutor::take_last_kdf_finished`] after fully draining the stream.
//! [`SinkType::Write`](delta_kernel::plans::ir::nodes::SinkType::Write) lowers to DataFusion file
//! sinks (`ParquetSink` / `JsonSink`) behind `DataSinkExec` in single-file mode; draining the plan
//! yields one batch with a `count` column (rows written). Bridging Parquet footer statistics into
//! Delta-style file metadata is not implemented yet.
//! [`SinkType::PartitionedWrite`](delta_kernel::plans::ir::nodes::SinkType::PartitionedWrite)
//! (`KernelPartitionedWriteExec`) writes Hive-style partitions under a `file://` URL and yields no
//! output batches once drained.
//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) is not lowered yet (compile
//! returns [`crate::error::unsupported`]). Metadata-only parquet footer reads align with
//! [`PhaseOperation::SchemaQuery`](delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation::SchemaQuery)
//! via [`DataFusionExecutor::read_parquet_footer_schema`] on this executor's [`Engine`].

use std::sync::{Arc, Mutex};

use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::Plan;
use delta_kernel::plans::kdf::FinishedHandle;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::{Engine, FileMeta};
use futures::TryStreamExt;

use crate::compile::{compile_plan, CompileContext};
use crate::error::LiftDeltaErr;
use crate::exec::RelationBatchRegistry;

fn default_kernel_engine() -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

/// Minimal executor: holds a [`TaskContext`] for [`ExecutionPlan::execute`] calls.
///
/// Phase 1.1 does not yet wire a full [`SessionContext`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html);
/// literal execution only needs the default task context.
pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    relation_registry: Arc<RelationBatchRegistry>,
    kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    engine: Arc<dyn Engine>,
}

impl DataFusionExecutor {
    /// Builds an executor backed by [`TaskContext::default()`] and a local-filesystem
    /// [`DefaultEngine`](delta_kernel::engine::default::DefaultEngine).
    pub fn try_new() -> Result<Self, DeltaError> {
        Self::try_new_with_engine(default_kernel_engine())
    }

    /// Builds an executor that uses the provided kernel [`Engine`] for IO helpers such as
    /// [`Self::read_parquet_footer_schema`].
    pub fn try_new_with_engine(engine: Arc<dyn Engine>) -> Result<Self, DeltaError> {
        let relation_registry = Arc::new(RelationBatchRegistry::new());
        let kdf_harvest_slot = Arc::new(Mutex::new(None));
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            relation_registry,
            kdf_harvest_slot,
            engine,
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

    /// Take the finalized [`FinishedHandle`] produced by the last fully-drained `ConsumeByKdf` sink
    /// plan.
    pub fn take_last_kdf_finished(&self) -> Option<FinishedHandle> {
        self.kdf_harvest_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// Read a parquet footer using [`ParquetHandler::read_parquet_footer`](delta_kernel::ParquetHandler::read_parquet_footer).
    ///
    /// Matches the intent of [`PhaseOperation::SchemaQuery`](delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation::SchemaQuery): metadata-only schema extraction without scanning rows.
    pub fn read_parquet_footer_schema(&self, file: &FileMeta) -> Result<KernelSchemaRef, DeltaError> {
        self.engine
            .parquet_handler()
            .read_parquet_footer(file)
            .map(|footer| footer.schema)
            .map_err(|e| crate::error::internal_error(e.to_string()))
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

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }
}
