# delta-kernel-datafusion-engine

A DataFusion-backed executor for [`delta_kernel`](https://crates.io/crates/delta_kernel)
declarative plans.

The kernel emits an engine-agnostic plan IR (`delta_kernel::plans::ir::plan::Plan`).
The full crate will compile those plans into a DataFusion `LogicalPlan` and expose a
`DataFusionExecutor` that runs them against a connector-provided `delta_kernel::Engine`;
the executor and physical operators land in follow-up PRs. This first slice ships the
expression translator, the JSON parsing UDFs, and the DataFusion <-> kernel error bridges.

This crate is feature-gated by the kernel's `declarative-plans` feature. It targets
**DataFusion 53**; features that need DataFusion 54 (virtual `_row_number` column,
field-id-aware physical adapters) are intentionally absent in this slice and surface as
typed `plan_compilation` / `Unsupported` errors at compile time.

## What ships in this PR

| Module | Purpose |
| --- | --- |
| `compile::expr_translator` | `delta_kernel::expressions::Expression` / `Predicate` -> DataFusion `Expr`. |
| `compile::stamp_udf` | Internal "stamp" UDF used by reducer sinks. |
| `compile::json_parse` (internal) | JSON-stats column extraction UDFs over the kernel `StructType`. |
| `error` | Typed bridges (`DataFusionError` <-> `DeltaError`), `DfResultIntoDelta` extension trait. |

Subsequent PRs add the logical-plan lowering, the `LoadExec` physical operator, and the
`DataFusionExecutor` driver.

## Status

Experimental. Public surface is unstable while the executor lands.
