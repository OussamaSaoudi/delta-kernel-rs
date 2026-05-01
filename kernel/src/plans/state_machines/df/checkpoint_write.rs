//! Classic single-file checkpoint parquet write plans for DataFusion-backed SM drivers (Phase 3.3).
//!
//! ## Supported path
//!
//! Classic-named parquet checkpoints (`NNNN.checkpoint.parquet`), including tables with the
//! `v2Checkpoint` feature where checkpoint rows include a trailing `checkpointMetadata` batch.
//!
//! ## Explicit gaps (TODO)
//!
//! - **Multipart V2 checkpoints / sidecars**: emitting shard parquet files plus manifest sidecars
//!   is not modeled as declarative plans yet. [`crate::checkpoint::CheckpointWriter`] continues to
//!   use single-file classic paths only; DF [`crate::plans::ir::nodes::SinkType::PartitionedWrite`]
//!   is not wired for checkpoint shards.
//! - **`_last_checkpoint` extras**: [`crate::checkpoint::create_last_checkpoint_data`] documents
//!   protocol TODOs (`checkpoint_schema`, `tags`, checksum, ...).

use std::sync::Arc;

use url::Url;

use crate::action_reconciliation::ActionReconciliationIteratorState;
use crate::arrow::record_batch::RecordBatch;
use crate::checkpoint::CheckpointWriter;
use crate::engine::arrow_data::EngineDataArrowExt;
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::{RelationHandle, WriteSink};
use crate::plans::ir::{DeclarativePlanNode, Plan, Prepared};
use crate::plans::state_machines::df::insert::insert_write_rows_prepared;
use crate::plans::state_machines::framework::coroutine::engine::CoroutineSM;
use crate::{DeltaResult, Engine};

/// Materialize checkpoint rows as Arrow batches and mint a [`RelationHandle`] for DF consumption.
///
/// Drains [`CheckpointWriter::checkpoint_data`]. Callers register `(handle.id, batches)` on the DF
/// executor relation registry before compiling [`checkpoint_classic_parquet_write_plan`].
///
/// Returns shared reconciliation iterator state for [`crate::checkpoint::LastCheckpointHintStats`]
/// once the parquet write completes (single-file checkpoints use `num_sidecars = 0` today).
pub fn prepare_classic_checkpoint_parquet_materialization(
    engine: &dyn Engine,
    writer: &CheckpointWriter,
) -> DeltaResult<(
    RelationHandle,
    Vec<RecordBatch>,
    Url,
    Arc<ActionReconciliationIteratorState>,
)> {
    let schema = writer.checkpoint_output_schema(engine)?;
    let handle = RelationHandle::fresh("checkpoint_parquet_rows", schema);
    let destination = writer.checkpoint_path()?;
    let iter = writer.checkpoint_data(engine)?;
    let state = iter.state();
    let mut batches = Vec::new();
    for batch in iter {
        let rb = batch?.apply_selection_vector()?.try_into_record_batch()?;
        batches.push(rb);
    }
    Ok((handle, batches, destination, state))
}

/// Declarative plan: stream registered checkpoint batches into the classic parquet checkpoint path.
pub fn checkpoint_classic_parquet_write_plan(handle: RelationHandle, destination: Url) -> Plan {
    DeclarativePlanNode::Relation(handle).into_write(WriteSink::parquet(destination))
}

/// Pair a checkpoint parquet [`Plan`] with write-row telemetry (`Prepared<u64>`).
/// Equivalent to [`super::insert_write_rows_prepared`]; checkpoint code paths import this alias so
/// DF drivers can attach write-row telemetry tokens alongside semantic checkpoint naming.
pub fn checkpoint_parquet_write_rows_prepared(plan: Plan) -> Prepared<u64> {
    insert_write_rows_prepared(plan)
}

/// Single-phase SM: dispatch checkpoint parquet [`Prepared`] (telemetry keyed like insert writes).
///
/// Drivers must clone [`Prepared::token`] into the executor write telemetry slot when draining
/// write sinks (same contract as [`super::insert_write_sm`]).
pub fn checkpoint_classic_parquet_write_sm(
    prepared: Prepared<u64>,
) -> Result<CoroutineSM<u64>, DeltaError> {
    use crate::plans::state_machines::framework::coroutine::phase::Phase;

    CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);
        phase.execute(prepared, "checkpoint_parquet_write").await
    })
}
