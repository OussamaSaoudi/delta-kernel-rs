//! DataFusion execution scaffold for kernel plans.
//!
//! Currently provides:
//! - [`compile`]: kernel `Expression`/`Predicate` -> DataFusion `Expr` translation, the
//!   plan-compilation entry-point hooks, JSON parsing UDFs, and `Plan` -> [`LogicalPlan`]
//!   lowering (`compile::logical`). Lowering wires `NodeKind::Load` through the
//!   `LoadTableProvider`.
//! - [`exec`]: physical operators -- `FileListingExec` for `NodeKind::ListFiles` and the
//!   streaming `LoadExec` / `LoadTableProvider` pair for `NodeKind::Load` (no-DV /
//!   no-column-mapping subset; DV + field-id support lands once DataFusion 54 ships the
//!   virtual-column and expression-adapter plumbing).
//! - [`executor`]: the [`DataFusionExecutor`] that drives kernel coroutine state machines
//!   (scan / scan_metadata / full_state) and compiles `ResultPlan` -> `DataFrame`.
//! - [`error`]: typed bridges between `datafusion_common::DataFusionError` and
//!   [`delta_kernel::plans::errors::DeltaError`].
//! - [`testing`]: buffered collectors over [`DataFusionExecutor`] for use by integration
//!   tests in this crate and downstream consumers. Always compiled; the `test-utils`
//!   feature is currently a no-op marker reserved for future test-only surface
//!   (e.g. an in-memory engine factory).
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan

pub mod compile;
pub mod error;
pub mod exec;
pub mod executor;
pub mod testing;

pub use executor::DataFusionExecutor;
