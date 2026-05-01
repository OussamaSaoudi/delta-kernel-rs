//! Declarative-plan state machines exercised by the DataFusion executor slice.
//!
//! - **Phase 3.4 — insert**: paired [`crate::plans::ir::Prepared`] + telemetry keyed by token so
//!   the engine can attach row counts for [`crate::plans::ir::nodes::SinkType::Write`] plans.
//! - **Phase 3.5 — commit-action emission**: stream literal JSON envelopes through a consumer KDF.
//!
//! - **Phase 3.3 — checkpoint classic parquet write**: [`checkpoint_write`] materializes checkpoint
//!   rows into the DF relation registry and streams them through
//!   [`crate::plans::ir::nodes::SinkType::Write`].

mod checkpoint_write;
mod commit_emit;
mod insert;

pub use checkpoint_write::{
    checkpoint_classic_parquet_write_plan, checkpoint_classic_parquet_write_sm,
    checkpoint_parquet_write_rows_prepared, prepare_classic_checkpoint_parquet_materialization,
};
pub use commit_emit::{
    commit_action_emit_sm, commit_action_envelopes_literal, commit_action_json_schema,
    CommitEnvelopeCollector,
};
pub use insert::{insert_write_rows_prepared, insert_write_sm, WriteRowCount};
