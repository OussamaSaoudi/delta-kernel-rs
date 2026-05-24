# delta-kernel-datafusion-engine

A DataFusion-backed executor for [`delta_kernel`](https://crates.io/crates/delta_kernel)
declarative plans.

The kernel emits an engine-agnostic plan IR (`delta_kernel::plans::ir::plan::Plan`).
This crate compiles those plans into a DataFusion `LogicalPlan` and (in a downstream
slice) exposes a `DataFusionExecutor` that runs them against a connector-provided
`delta_kernel::Engine`. The physical `LoadExec` operator and the executor driver land in
downstream slices of this stack.

This crate is feature-gated by the kernel's `declarative-plans` feature. It targets
**DataFusion 53**; features that need DataFusion 54 (virtual `_row_number` column,
field-id-aware physical adapters) are intentionally absent and surface as typed
`plan_compilation` / `Unsupported` errors at compile time.

## What ships in this slice

| Module | Purpose |
| --- | --- |
| `compile::expr_translator` | `delta_kernel::expressions::Expression` / `Predicate` -> DataFusion `Expr`. |
| `compile::stamp_udf` | Internal "stamp" UDF used by reducer sinks. |
| `compile::json_parse` (internal) | JSON-stats column extraction UDFs over the kernel `StructType`. |
| `compile::logical` | Per-`NodeKind` logical-plan lowering: `Values`, `Filter`, `Project`, `Union`, `MaxByVersion`, `EquiJoin`, plus a `compile_plan` entry point that walks the IR. `Load` lowering is intentionally rejected until `LoadExec` lands. |
| `exec::file_listing` | `FileListingExec` physical operator over a static slice of files (path + size + modification_time). Used by `Scan` lowering. |
| `error` | Typed bridges (`DataFusionError` <-> `DeltaError`), `DfResultIntoDelta` extension trait. |

The remaining downstream slices add the `LoadExec` physical operator and the
`DataFusionExecutor` driver.

## Status

Experimental. Public surface is unstable while the executor lands.
