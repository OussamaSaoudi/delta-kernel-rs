//! DataFusion execution scaffold for kernel plans.
//!
//! Currently provides:
//! - [`compile`]: kernel `Expression`/`Predicate` -> DataFusion `Expr` translation, the
//!   plan-compilation entry-point hooks, and the JSON parsing UDF.
//! - [`error`]: typed bridges between `datafusion_common::DataFusionError` and
//!   [`delta_kernel::plans::errors::DeltaError`].
//!
//! Subsequent PRs add the physical execs, the logical-plan lowering, and the
//! `DataFusionExecutor` driver that compiles `ResultPlan` -> `DataFrame`.

pub mod compile;
pub mod error;
