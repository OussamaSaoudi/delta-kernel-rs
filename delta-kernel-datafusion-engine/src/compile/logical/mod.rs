//! Kernel plan -> DataFusion [`datafusion_expr::LogicalPlan`] lowering.
//!
//! See [`lower::compile_plan`] for the entry point. The submodules host per-shape lowering
//! helpers (file listings, scans, projections, ordered union, output canonicalization).

mod canonicalize;
mod lower;
mod ordered_union;
mod project;
mod providers;
mod scan;

pub use lower::compile_plan;
