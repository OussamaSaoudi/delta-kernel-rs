//! Plan intermediate representation.
//!
//! - [`nodes`] — per-variant payload structs that the [`plan::NodeKind`] enum wraps
//!   ([`nodes::LoadNode`], [`nodes::ScanParquetNode`], [`nodes::ScanJsonNode`],
//!   [`nodes::ProjectNode`], etc.) plus the shared [`nodes::ReduceSink`] referenced by
//!   [`EngineRequest::Reduce`].
//!
//! [`EngineRequest::Reduce`]: crate::plans::state_machines::framework::state_machine::EngineRequest::Reduce
//! - [`plan`] — typed [`plan::Plan`] / [`plan::PlanNode`] / [`plan::NodeKind`] / [`plan::Ref`] /
//!   [`plan::ResultPlan`]. Pure compute; no sinks, no named relations, no engine state side
//!   effects. The canonical kernel plan IR.
pub mod nodes;
pub mod plan;

pub use crate::plans::kernel_reducers::Extractor;
