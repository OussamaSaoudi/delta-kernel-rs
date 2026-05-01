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
//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) materializes per-row parquet
//! or JSON reads via [`KernelLoadSinkExec`](crate::exec::KernelLoadSinkExec) and the kernel's
//! parquet/json handlers.
//! Metadata-only parquet footer reads (SchemaQuery-shaped) use
//! [`DataFusionExecutor::read_parquet_footer_schema`].
//!
//! Phase 3.2 submits [`SinkType::ConsumeByKdf`] finalized handles into [`PhaseKdfState`] during
//! [`Self::execute_phase_operation_with_drive_opts`] using
//! [`crate::compile::CompileContext::phase_kdf_accumulator`]. Phase 3.3 wires classic checkpoint
//! parquet materialization via [`Self::checkpoint_write_classic_parquet_and_finalize`] (kernel
//! [`delta_kernel::plans::state_machines::df::checkpoint_write`] plans + SM phase
//! `checkpoint_parquet_write`). Phase 3.4 extends [`DriveOpts`] so [`SinkType::Write`] sinks emit
//! synthetic [`delta_kernel::plans::state_machines::df::WriteRowCount`] under the paired
//! [`delta_kernel::plans::ir::Prepared`] token ([`Self::drive_insert_write_sm`]).

use std::sync::{Arc, Mutex};

use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::array::AsArray;
use delta_kernel::arrow::datatypes::UInt64Type;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::Plan;
use delta_kernel::plans::kdf::{FinishedHandle, KdfStateToken, TraceContext};
use delta_kernel::plans::state_machines::df::WriteRowCount;
use delta_kernel::plans::state_machines::framework::coroutine::engine::CoroutineSM;
use delta_kernel::plans::state_machines::framework::engine_error::{EngineError, EngineErrorKind};
use delta_kernel::plans::state_machines::framework::phase_kdf_state::PhaseKdfState;
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::plans::state_machines::framework::state_machine::{AdvanceResult, StateMachine};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::{Engine, Error as KernelError, FileMeta};
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
) -> Result<PhaseKdfState, EngineError> {
    let token = KdfStateToken::parse_display(&node.token).map_err(EngineError::internal)?;
    let url = resolve_schema_query_url(&node.file_path)?;
    let meta = engine
        .storage_handler()
        .head(&url)
        .map_err(map_kernel_err)?;
    let footer = engine
        .parquet_handler()
        .read_parquet_footer(&meta)
        .map_err(map_kernel_err)?;
    let accum = PhaseKdfState::empty();
    let struct_ty = footer.schema.as_ref().clone();
    accum.submit(FinishedHandle {
        token,
        ctx: TraceContext::new("delta-kernel-datafusion-engine", "schema_query"),
        partition: 0,
        erased: Box::new(struct_ty),
    });
    Ok(accum)
}

/// Options for [`DataFusionExecutor::drive_coroutine_sm`].
#[derive(Clone, Debug, Default)]
pub struct DriveOpts {
    /// When set, draining a [`SinkType::Write`] plan submits [`WriteRowCount`] keyed by this token
    /// (cloned from [`delta_kernel::plans::ir::Prepared::token`] by
    /// [`DataFusionExecutor::drive_insert_write_sm`]).
    pub insert_write_telemetry_token: Option<KdfStateToken>,
}

fn sum_write_sink_row_counts(batches: &[RecordBatch]) -> Result<u64, EngineError> {
    let mut sum = 0u64;
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        let col = b.column(0);
        let arr = col.as_primitive_opt::<UInt64Type>().ok_or_else(|| {
            EngineError::internal(delta_kernel::Error::generic(
                "write sink summary batch: expected UInt64 row counts in column 0",
            ))
        })?;
        for i in 0..b.num_rows() {
            sum = sum.saturating_add(arr.value(i));
        }
    }
    Ok(sum)
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
                Arc::clone(&self.engine),
            ),
        )
    }

    /// Equivalent to [`Self::execute_phase_operation_with_drive_opts`] with default [`DriveOpts`].
    pub async fn execute_phase_operation(
        &self,
        op: PhaseOperation,
    ) -> Result<PhaseKdfState, EngineError> {
        self.execute_phase_operation_with_drive_opts(op, &DriveOpts::default())
            .await
    }

    /// Like [`Self::execute_phase_operation`], but [`DriveOpts::insert_write_telemetry_token`]
    /// enables synthetic [`WriteRowCount`] payloads for [`SinkType::Write`] plans (Phase 3.4
    /// insert SM).
    pub async fn execute_phase_operation_with_drive_opts(
        &self,
        op: PhaseOperation,
        opts: &DriveOpts,
    ) -> Result<PhaseKdfState, EngineError> {
        let (accum, _) = self
            .execute_phase_operation_with_results_capture(op, opts)
            .await?;
        Ok(accum)
    }

    /// Same draining semantics as [`Self::execute_phase_operation_with_drive_opts`], plus optional
    /// capture of batches produced by the **last** [`SinkType::Results`] plan in the
    /// [`PhaseOperation::Plans`] slice (when present).
    ///
    /// Intended for integration tests and diagnostics that need row-level verification without
    /// changing [`CoroutineSM`] harness contracts.
    pub async fn execute_phase_operation_with_results_batches(
        &self,
        op: PhaseOperation,
        opts: &DriveOpts,
    ) -> Result<(PhaseKdfState, Option<Vec<RecordBatch>>), EngineError> {
        self.execute_phase_operation_with_results_capture(op, opts)
            .await
    }

    async fn execute_phase_operation_with_results_capture(
        &self,
        op: PhaseOperation,
        opts: &DriveOpts,
    ) -> Result<(PhaseKdfState, Option<Vec<RecordBatch>>), EngineError> {
        self.clear_kdf_harvest_slot();

        match op {
            PhaseOperation::Plans(plans) => {
                let accum = PhaseKdfState::empty();
                let ctx = CompileContext {
                    relation_registry: Arc::clone(&self.relation_registry),
                    kdf_harvest_slot: Arc::clone(&self.kdf_harvest_slot),
                    phase_kdf_accumulator: Some(accum.clone()),
                    engine: Arc::clone(&self.engine),
                };
                let mut last_results_batches: Option<Vec<RecordBatch>> = None;
                for plan in plans {
                    let physical = compile_plan(&plan, &ctx).map_err(EngineError::internal)?;
                    let stream = physical
                        .execute(0, Arc::clone(&self.task_ctx))
                        .map_err(EngineError::internal)?;
                    let batches: Vec<RecordBatch> = stream
                        .try_collect()
                        .await
                        .map_err(|e| EngineError::internal(datafusion_err_to_delta(e)))?;

                    if let SinkType::Write(_) = &plan.sink.sink_type {
                        if let Some(tok) = opts.insert_write_telemetry_token.clone() {
                            let n = sum_write_sink_row_counts(&batches)?;
                            accum.submit(FinishedHandle {
                                token: tok,
                                ctx: TraceContext::new(
                                    "delta-kernel-datafusion-engine",
                                    "insert_write",
                                ),
                                partition: 0,
                                erased: Box::new(WriteRowCount(n)),
                            });
                        }
                    }
                    if matches!(plan.sink.sink_type, SinkType::Results) {
                        last_results_batches = Some(batches);
                    }
                }
                Ok((accum, last_results_batches))
            }
            PhaseOperation::SchemaQuery(node) => {
                let accum = execute_schema_query_phase(&self.engine, node)?;
                Ok((accum, None))
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
        opts: DriveOpts,
    ) -> Result<R, DeltaError> {
        loop {
            let op = sm.get_operation()?;
            let phase_result = self
                .execute_phase_operation_with_drive_opts(op, &opts)
                .await;
            match sm.advance(phase_result)? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(v) => return Ok(v),
            }
        }
    }

    /// Phase 3.4 helper — pairs
    /// [`delta_kernel::plans::state_machines::df::insert_write_rows_prepared`] with
    /// [`DriveOpts::insert_write_telemetry_token`].
    pub async fn drive_insert_write_sm(
        &self,
        prepared: delta_kernel::plans::ir::Prepared<u64>,
    ) -> Result<u64, DeltaError> {
        let tok = prepared.token().clone();
        let sm = delta_kernel::plans::state_machines::df::insert_write_sm(prepared)?;
        self.drive_coroutine_sm(
            sm,
            DriveOpts {
                insert_write_telemetry_token: Some(tok),
            },
        )
        .await
    }

    /// Phase 3.3 helper — pairs [`delta_kernel::plans::state_machines::df::checkpoint_write`] with
    /// write-row telemetry (same [`DriveOpts`] contract as [`Self::drive_insert_write_sm`]).
    pub async fn drive_checkpoint_classic_parquet_write_sm(
        &self,
        prepared: delta_kernel::plans::ir::Prepared<u64>,
    ) -> Result<u64, DeltaError> {
        let tok = prepared.token().clone();
        let sm =
            delta_kernel::plans::state_machines::df::checkpoint_classic_parquet_write_sm(prepared)?;
        self.drive_coroutine_sm(
            sm,
            DriveOpts {
                insert_write_telemetry_token: Some(tok),
            },
        )
        .await
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
            checkpoint_classic_parquet_write_plan, checkpoint_parquet_write_rows_prepared,
            prepare_classic_checkpoint_parquet_materialization,
        };

        let engine = self.engine.as_ref();
        let (handle, batches, dest, state) =
            prepare_classic_checkpoint_parquet_materialization(engine, &writer)
                .map_err(|e| crate::error::internal_error(e.to_string()))?;
        self.relation_registry.register(handle.id, batches);
        let plan = checkpoint_classic_parquet_write_plan(handle, dest.clone());
        let prepared = checkpoint_parquet_write_rows_prepared(plan);
        let _written_rows = self
            .drive_checkpoint_classic_parquet_write_sm(prepared)
            .await?;
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

    /// Read a parquet footer using
    /// [`ParquetHandler::read_parquet_footer`](delta_kernel::ParquetHandler::read_parquet_footer).
    ///
    /// Matches the intent of
    /// [`PhaseOperation::SchemaQuery`](delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation::SchemaQuery):
    /// metadata-only schema extraction without scanning rows.
    pub fn read_parquet_footer_schema(
        &self,
        file: &FileMeta,
    ) -> Result<KernelSchemaRef, DeltaError> {
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
