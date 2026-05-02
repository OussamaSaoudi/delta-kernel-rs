//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! Relation sinks ([`delta_kernel::plans::ir::nodes::SinkType::Relation`]) materialize batches into
//! [`crate::exec::RelationBatchRegistry`] when their stream is drained; subsequent plans read via a
//! [`DeclarativePlanNode::RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef)
//! leaf. [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) drains
//! through a [`KernelConsumeByKdfExec`](crate::exec::KernelConsumeByKdfExec); harvest the finalized
//! handle with [`DataFusionExecutor::take_last_kdf_finished`] after fully draining the stream.
//! [`SinkType::Write`](delta_kernel::plans::ir::nodes::SinkType::Write) lowers to DataFusion file
//! sinks (`ParquetSink` / `JsonSink`) behind `DataSinkExec` in single-file mode. Bridging Parquet
//! footer statistics into Delta-style `Add.stats` (`numRecords`, min/max, ...) is not implemented
//! yet.
//! [`SinkType::PartitionedWrite`](delta_kernel::plans::ir::nodes::SinkType::PartitionedWrite)
//! (`KernelPartitionedWriteExec`) writes Hive-style partitions under a `file://` URL and yields no
//! output batches once drained.
//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) materializes per-row parquet
//! or JSON reads via [`KernelLoadSinkExec`](crate::exec::KernelLoadSinkExec) and the kernel's
//! parquet/json handlers.
//!
//! Phase 3.2 submits [`SinkType::ConsumeByKdf`] finalized handles into [`PhaseState`] during
//! [`Self::execute_phase_operation`] using [`crate::compile::CompileContext::phase_state`].
//! Phase 3.3 wires classic checkpoint parquet materialization via
//! [`Self::checkpoint_write_classic_parquet_and_finalize`] (kernel
//! [`delta_kernel::plans::state_machines::df::checkpoint_write`] plans + SM phase
//! `checkpoint_parquet_write`).

use std::sync::{Arc, Mutex};

use datafusion_execution::config::SessionConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::Plan;
use delta_kernel::plans::kdf::FinishedHandle;
use delta_kernel::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use delta_kernel::plans::state_machines::framework::engine_error::{EngineError, EngineErrorKind};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::plans::state_machines::framework::state_machine::{AdvanceResult, StateMachine};
use delta_kernel::{Engine, Error as KernelError};
use futures::TryStreamExt;
use url::Url;

use crate::compile::{compile_plan, CompileContext};
use crate::error::{datafusion_err_to_delta, LiftDeltaErr};
use crate::exec::RelationBatchRegistry;

fn default_kernel_engine() -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

fn resolve_schema_query_url(path: &str) -> Result<Url, EngineError> {
    Url::parse(path).or_else(|_| {
        Url::from_file_path(std::path::Path::new(path)).map_err(|_| {
            EngineError::new(EngineErrorKind::IoError {
                message: format!("invalid schema-query location string: {path}"),
            })
        })
    })
}

fn map_kernel_err(err: KernelError) -> EngineError {
    match err {
        KernelError::FileNotFound(path) => EngineError::new(EngineErrorKind::FileNotFound { path }),
        other => EngineError::internal(other),
    }
}

fn execute_schema_query_phase(
    engine: &Arc<dyn Engine>,
    node: SchemaQueryNode,
) -> Result<PhaseState, EngineError> {
    let url = resolve_schema_query_url(&node.file_path)?;
    let meta = engine
        .storage_handler()
        .head(&url)
        .map_err(map_kernel_err)?;
    let footer = engine
        .parquet_handler()
        .read_parquet_footer(&meta)
        .map_err(map_kernel_err)?;
    let state = PhaseState::empty();
    state.submit_schema(footer.schema);
    Ok(state)
}

/// Minimal executor: holds a [`TaskContext`] for [`ExecutionPlan::execute`] calls.
///
/// Phase 1.1 does not yet wire a full [`SessionContext`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html);
/// literal execution only needs the default task context.
pub struct DataFusionExecutor {
    task_ctx: Arc<TaskContext>,
    session_config: SessionConfig,
    physical_optimizer: PhysicalOptimizer,
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

    /// Builds an executor that uses the provided kernel [`Engine`] for IO helpers (object-store,
    /// parquet handler, etc).
    pub fn try_new_with_engine(engine: Arc<dyn Engine>) -> Result<Self, DeltaError> {
        let session_config = SessionConfig::new();
        let relation_registry = Arc::new(RelationBatchRegistry::new());
        let kdf_harvest_slot = Arc::new(Mutex::new(None));
        Ok(Self {
            task_ctx: Arc::new(TaskContext::default()),
            session_config,
            physical_optimizer: PhysicalOptimizer::new(),
            relation_registry,
            kdf_harvest_slot,
            engine,
        })
    }

    /// Compile a [`Plan`] into a physical node when Phase 1.1 dispatch accepts it.
    pub fn compile_plan(&self, plan: &Plan) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        let physical = compile_plan(
            plan,
            &CompileContext::new(
                Arc::clone(&self.relation_registry),
                Arc::clone(&self.kdf_harvest_slot),
                Arc::clone(&self.engine),
            ),
        )?;
        self.optimize_physical_plan(physical)
    }

    fn optimize_physical_plan(
        &self,
        mut plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
        for rule in &self.physical_optimizer.rules {
            plan = rule
                .optimize(plan, self.session_config.options())
                .map_err(|e| {
                    crate::error::internal_error(format!(
                        "DataFusion physical optimizer `{}` failed: {e}",
                        rule.name()
                    ))
                })?;
        }
        Ok(plan)
    }

    /// Drain a [`PhaseOperation`] against the executor and return the resulting [`PhaseState`].
    pub async fn execute_phase_operation(
        &self,
        op: PhaseOperation,
    ) -> Result<PhaseState, EngineError> {
        let (state, _) = self
            .execute_phase_operation_with_results_capture(op)
            .await?;
        Ok(state)
    }

    /// Same draining semantics as [`Self::execute_phase_operation`], plus capture of the batches
    /// produced by the **last** [`SinkType::Results`] plan in the [`PhaseOperation::Plans`] slice
    /// (when present). Used by [`Self::drive_coroutine_sm_collecting_results`] and in-crate tests
    /// that need to verify row content of an SM-driven `Results` sink without rewriting the SM's
    /// `Output` contract.
    async fn execute_phase_operation_with_results_capture(
        &self,
        op: PhaseOperation,
    ) -> Result<(PhaseState, Option<Vec<RecordBatch>>), EngineError> {
        self.clear_kdf_harvest_slot();

        match op {
            PhaseOperation::Plans(plans) => {
                let state = PhaseState::empty();
                let ctx = CompileContext {
                    relation_registry: Arc::clone(&self.relation_registry),
                    kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                    phase_state: Some(state.clone()),
                    engine: Arc::clone(&self.engine),
                };
                let mut last_results_batches: Option<Vec<RecordBatch>> = None;
                for plan in plans {
                    let physical = compile_plan(&plan, &ctx)
                        .and_then(|p| self.optimize_physical_plan(p))
                        .map_err(EngineError::internal)?;
                    let batches = self
                        .collect_all_partitions(physical)
                        .await
                        .map_err(EngineError::internal)?;

                    if matches!(plan.sink.sink_type, SinkType::Results) {
                        last_results_batches = Some(batches);
                    }
                }
                Ok((state, last_results_batches))
            }
            PhaseOperation::SchemaQuery(node) => {
                let state = execute_schema_query_phase(&self.engine, node)?;
                Ok((state, None))
            }
        }
    }

    fn clear_kdf_harvest_slot(&self) {
        let mut guard = self
            .kdf_harvest_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = None;
    }

    /// Drive a [`CoroutineSM`] until [`AdvanceResult::Done`].
    pub async fn drive_coroutine_sm<R: Send + 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<R, DeltaError> {
        loop {
            let op = sm.get_operation()?;
            let phase_result = self.execute_phase_operation(op).await;
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(v) => return Ok(v),
            }
        }
    }

    /// Drive a [`CoroutineSM`] until [`AdvanceResult::Done`] *and* capture the
    /// [`RecordBatch`] stream of the **last** [`SinkType::Results`] plan executed across all
    /// `PhaseOperation::Plans` yields. SMs that never yield a `Results` sink return
    /// `(value, vec![])`.
    ///
    /// Intended for callers (e.g. `Snapshot::full_state` consumers) that want both the SM
    /// outcome and the materialized scan-row batches without having to inspect the relation
    /// registry or rewrite the SM's `Output` contract.
    pub async fn drive_coroutine_sm_collecting_results<R: Send + 'static>(
        &self,
        mut sm: CoroutineSM<R>,
    ) -> Result<(R, Vec<RecordBatch>), DeltaError> {
        let mut last_results: Vec<RecordBatch> = Vec::new();
        loop {
            let op = sm.get_operation()?;
            let phase_result = self
                .execute_phase_operation_with_results_capture(op)
                .await
                .map(|(state, batches)| {
                    if let Some(b) = batches {
                        last_results = b;
                    }
                    state
                });
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(v) => return Ok((v, last_results)),
            }
        }
    }

    /// Phase 3.4 helper — drive
    /// [`delta_kernel::plans::state_machines::df::insert_write_sm`] to completion.
    pub async fn drive_insert_write_sm(&self, plan: Plan) -> Result<(), DeltaError> {
        let sm = delta_kernel::plans::state_machines::df::insert_write_sm(plan)?;
        self.drive_coroutine_sm(sm).await
    }

    /// Phase 3.3 helper — drive
    /// [`delta_kernel::plans::state_machines::df::checkpoint_classic_parquet_write_sm`] to
    /// completion.
    pub async fn drive_checkpoint_classic_parquet_write_sm(
        &self,
        plan: Plan,
    ) -> Result<(), DeltaError> {
        let sm =
            delta_kernel::plans::state_machines::df::checkpoint_classic_parquet_write_sm(plan)?;
        self.drive_coroutine_sm(sm).await
    }

    /// Classic single-file checkpoint parquet via declarative [`Plan`] plus `_last_checkpoint`
    /// finalize.
    ///
    /// Requires this executor's [`Engine`] to match the [`CheckpointWriter`] snapshot storage (same
    /// as [`Snapshot::checkpoint`](delta_kernel::Snapshot::checkpoint)).
    ///
    /// `num_sidecars` stays `0` until multipart checkpoint shard plans land (see kernel
    /// `plans::state_machines::df::checkpoint_write` module docs).
    pub async fn checkpoint_write_classic_parquet_and_finalize(
        &self,
        writer: delta_kernel::checkpoint::CheckpointWriter,
    ) -> Result<(), DeltaError> {
        use delta_kernel::checkpoint::LastCheckpointHintStats;
        use delta_kernel::plans::state_machines::df::{
            checkpoint_classic_parquet_write_plan,
            prepare_classic_checkpoint_parquet_materialization,
        };

        let engine = self.engine.as_ref();
        let (handle, batches, dest, state) =
            prepare_classic_checkpoint_parquet_materialization(engine, &writer)
                .map_err(|e| crate::error::internal_error(e.to_string()))?;
        self.relation_registry.register(handle.id, batches);
        let plan = checkpoint_classic_parquet_write_plan(handle, dest.clone());
        self.drive_checkpoint_classic_parquet_write_sm(plan).await?;
        let meta = engine
            .storage_handler()
            .head(&dest)
            .map_err(|e| crate::error::internal_error(e.to_string()))?;
        let state = std::sync::Arc::into_inner(state).ok_or_else(|| {
            crate::error::internal_error(
                "checkpoint reconciliation state Arc still referenced after draining checkpoint iterator",
            )
        })?;
        let stats = LastCheckpointHintStats::from_reconciliation_state(state, meta.size, 0)
            .map_err(|e| crate::error::internal_error(e.to_string()))?;
        writer
            .finalize(engine, &stats)
            .map_err(|e| crate::error::internal_error(e.to_string()))?;
        Ok(())
    }

    /// Compile and execute partition `0`.
    pub async fn execute_plan_to_stream(
        &self,
        plan: Plan,
    ) -> Result<SendableRecordBatchStream, DeltaError> {
        self.clear_kdf_harvest_slot();
        let mut physical = self.compile_plan(&plan)?;
        if physical.output_partitioning().partition_count() > 1 {
            physical = Arc::new(CoalescePartitionsExec::new(physical));
        }
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

    /// Convenience helper for tests / tiny literals.
    pub async fn execute_plan_collect(
        &self,
        plan: Plan,
    ) -> Result<Vec<delta_kernel::arrow::array::RecordBatch>, DeltaError> {
        self.clear_kdf_harvest_slot();
        let physical = self.compile_plan(&plan)?;
        self.collect_all_partitions(physical).await
    }

    pub fn relation_batch_registry(&self) -> &Arc<RelationBatchRegistry> {
        &self.relation_registry
    }

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    async fn collect_all_partitions(
        &self,
        physical: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DeltaError> {
        let mut out = Vec::new();
        let partitions = physical.output_partitioning().partition_count();
        for partition in 0..partitions {
            let stream = match physical.execute(partition, Arc::clone(&self.task_ctx)) {
                Ok(stream) => stream,
                Err(err)
                    if partition > 0
                        && err
                            .to_string()
                            .contains("supports partition 0 only") =>
                {
                    break;
                }
                Err(err) => return Err(datafusion_err_to_delta(err)),
            };
            let mut batches: Vec<RecordBatch> = stream.try_collect().await.lift()?;
            out.append(&mut batches);
        }
        Ok(out)
    }
}
