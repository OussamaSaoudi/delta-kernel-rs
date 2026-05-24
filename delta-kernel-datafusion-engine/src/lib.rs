//! DataFusion execution scaffold for kernel plans.
//!
//! Currently provides:
//! - [`compile`]: kernel `Expression`/`Predicate` -> DataFusion `Expr` translation, the
//!   plan-compilation entry-point hooks, JSON parsing UDFs, and `Plan` -> [`LogicalPlan`]
//!   lowering (`compile::logical`).
//! - [`exec`]: scaffolding for physical operators consumed by the lowered logical plan.
//! - [`error`]: typed bridges between `datafusion_common::DataFusionError` and
//!   [`delta_kernel::plans::errors::DeltaError`].
//!
//! Subsequent PRs add the `DataFusionExecutor` driver that compiles `ResultPlan` ->
//! `DataFrame` and runs the state-machine step loop.
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan

pub mod compile;
pub mod error;
pub mod exec;
