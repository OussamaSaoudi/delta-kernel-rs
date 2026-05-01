//! Declarative-plan state machines exercised by the DataFusion executor slice.
//!
//! - **Phase 3.4 — insert**: a one-phase SM that drives a single
//!   [`SinkType::Write`](crate::plans::ir::nodes::SinkType::Write) plan to completion
//!   ([`insert_write_sm`]).
//!
//! - **Phase 3.3 — checkpoint classic parquet write**: [`checkpoint_write`] materializes checkpoint
//!   rows into the DF relation registry and streams them through
//!   [`crate::plans::ir::nodes::SinkType::Write`].

mod checkpoint_write;
mod insert;

pub use checkpoint_write::{
    checkpoint_classic_parquet_write_plan, checkpoint_classic_parquet_write_sm,
    prepare_classic_checkpoint_parquet_materialization,
};
pub use insert::insert_write_sm;
