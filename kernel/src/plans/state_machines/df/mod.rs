//! Declarative-plan state machines exercised by the DataFusion executor slice.
//!
//! - **Phase 3.4 — insert**: a one-phase SM that runs a single
//!   [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan and recovers the row count
//!   via an [`Extractor<u64>`](crate::plans::ir::Extractor) keyed by the same telemetry token the
//!   executor populates.
//! - **Phase 3.5 — commit-action emission**: stream literal JSON envelopes through a consumer KDF.
//!
//! - **Phase 3.3 — checkpoint classic parquet write**: [`checkpoint_write`] materializes checkpoint
//!   rows into the DF relation registry and streams them through
//!   [`crate::plans::ir::nodes::SinkType::Write`].
//!
//! - **Phase 4.x — scan log replay**: [`scan_log_replay_sm`] builds a
//!   [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM) for
//!   JSON-commit dedup plus optional checkpoint anti-join (implementation in `scan_log_replay.rs`).
//!   [`ScanLogReplayAntiJoinSM`] remains as a deprecated
//!   [`StateMachine`](crate::plans::state_machines::framework::state_machine::StateMachine) shim.

mod checkpoint_write;
mod commit_emit;
mod insert;
mod scan_log_replay;

pub use checkpoint_write::{
    checkpoint_classic_parquet_write_plan, checkpoint_classic_parquet_write_sm,
    prepare_classic_checkpoint_parquet_materialization,
};
pub use commit_emit::{
    commit_action_emit_sm, commit_action_envelopes_literal, commit_action_json_schema,
    CommitEnvelopeCollector,
};
pub use insert::{insert_write_extractor, insert_write_sm, WriteRowCount};
pub use scan_log_replay::scan_log_replay_sm;
#[allow(deprecated)]
pub use scan_log_replay::ScanLogReplayAntiJoinSM;
