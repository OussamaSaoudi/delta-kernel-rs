//! DataFusion-backed [`DataFusionExecutor`] for compiling kernel [`Plan`] values.
//!
//! Relation sinks ([`delta_kernel::plans::ir::nodes::SinkType::Relation`]) materialize batches into
//! [`crate::exec::RelationBatchRegistry`] when their stream is drained; subsequent plans read via a
//! [`DeclarativePlanNode::RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef) leaf.
//! [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) drains
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

use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
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

    /// Drain a [`PhaseOperation`] against the executor and return the resulting [`PhaseState`].
    pub async fn execute_phase_operation(
        &self,
        op: PhaseOperation,
    ) -> Result<PhaseState, EngineError> {
        let (state, _) = self.execute_phase_operation_with_results_capture(op).await?;
        Ok(state)
    }

    /// Same draining semantics as [`Self::execute_phase_operation`], plus capture of the batches
    /// produced by the **last** [`SinkType::Results`] plan in the [`PhaseOperation::Plans`] slice
    /// (when present). Used by in-crate tests that need to verify row content of an SM-driven
    /// `Results` sink without rewriting the SM's `Output` contract.
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
                    let physical = compile_plan(&plan, &ctx).map_err(EngineError::internal)?;
                    let stream = physical
                        .execute(0, Arc::clone(&self.task_ctx))
                        .map_err(EngineError::internal)?;
                    let batches: Vec<RecordBatch> = stream
                        .try_collect()
                        .await
                        .map_err(|e| EngineError::internal(datafusion_err_to_delta(e)))?;

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

    /// Convenience helper for tests / tiny literals.
    pub async fn execute_plan_collect(
        &self,
        plan: Plan,
    ) -> Result<Vec<delta_kernel::arrow::array::RecordBatch>, DeltaError> {
        let stream = self.execute_plan_to_stream(plan).await?;
        stream.try_collect().await.lift()
    }

    pub fn relation_batch_registry(&self) -> &Arc<RelationBatchRegistry> {
        &self.relation_registry
    }

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }
}

#[cfg(test)]
mod scan_log_replay_tests {
    //! End-to-end check that the SM-driven scan rows match fixture paths. Lives inside `src/` so it
    //! can use the private [`DataFusionExecutor::execute_phase_operation_with_results_capture`]
    //! helper to inspect the last `Results` sink batches without exposing batch capture as
    //! public API.

    use std::path::Path;

    use delta_kernel::arrow::array::{Array, AsArray};
    use delta_kernel::arrow::datatypes::DataType as ArrowPhysicalType;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::plans::state_machines::df::scan_log_replay_sm;
    use delta_kernel::plans::state_machines::framework::state_machine::StateMachine;
    use delta_kernel::scan::scan_row_schema;
    use delta_kernel::Snapshot;

    use super::*;

    #[tokio::test]
    async fn scan_log_replay_sm_no_checkpoint_scan_rows_match_fixture_paths() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let table_root = manifest_dir.join("../kernel/tests/data/app-txn-no-checkpoint");
        let table_root =
            std::fs::canonicalize(table_root).expect("canonicalize kernel fixture path");

        let url = Url::from_directory_path(&table_root).expect("table URL");
        let engine = Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
        let snapshot = Snapshot::builder_for(url.as_str())
            .build(engine.as_ref())
            .expect("snapshot");

        let mut sm = scan_log_replay_sm(snapshot).expect("build SM");
        let exec = DataFusionExecutor::try_new_with_engine(engine).expect("executor");

        let mut last_results_batches: Vec<RecordBatch> = Vec::new();

        loop {
            let op = sm.get_operation().expect("phase op");
            let (accum, batches_opt) = exec
                .execute_phase_operation_with_results_capture(op)
                .await
                .expect("phase drain");

            if let Some(bs) = batches_opt {
                last_results_batches = bs;
            }

            match sm.advance(Ok(accum)).expect("advance") {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(()) => break,
            }
        }

        let batch = last_results_batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .expect("non-empty scan rows");

        let scan_schema = scan_row_schema();
        assert_eq!(
            batch.num_columns(),
            scan_schema.fields().len(),
            "scan-row projection column count must match scan_row_schema"
        );

        let path_idx = batch
            .schema()
            .index_of("path")
            .expect("'path' column from scan_row_schema");
        let paths = batch.column(path_idx).as_string::<i32>();
        let collected: Vec<String> = (0..paths.len())
            .map(|i| paths.value(i).to_string())
            .collect();
        assert!(
            collected.iter().any(|p| p.contains("modified=2021-02-01")),
            "expected fixture add path partition 2021-02-01 in {collected:?}",
        );
        assert!(
            collected.iter().any(|p| p.contains("modified=2021-02-02")),
            "expected fixture add path partition 2021-02-02 in {collected:?}",
        );

        let fcv_idx = batch
            .schema()
            .index_of("fileConstantValues")
            .expect("nested fileConstantValues column");
        assert!(
            matches!(
                batch.column(fcv_idx).data_type(),
                ArrowPhysicalType::Struct(_)
            ),
            "fileConstantValues must be a struct column in the DataFusion batch"
        );
    }
}
