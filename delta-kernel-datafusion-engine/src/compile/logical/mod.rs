//! Kernel plan -> DataFusion [`LogicalPlan`] lowering.
//!
//! See [`compile_plan`] for the entry point. The submodules host per-shape lowering
//! helpers (file listings, scans, projections, ordered union, output canonicalization).
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan

mod lower;
mod project;
mod scan;

pub use lower::compile_plan;
pub(crate) use project::expand_patch;
