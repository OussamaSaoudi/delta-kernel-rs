# delta-kernel-datafusion-engine

A DataFusion-backed executor for [`delta_kernel`](https://crates.io/crates/delta_kernel)
declarative plans.

The kernel emits an engine-agnostic plan IR (`delta_kernel::plans::ir::plan::Plan`). This
crate compiles those plans into a DataFusion `LogicalPlan` and exposes a
`DataFusionExecutor` that runs them against a connector-provided
`delta_kernel::Engine`.

The crate is feature-gated by the kernel's `declarative-plans` feature. It targets
**DataFusion 53**; features that need DataFusion 54 (virtual `_row_number` column,
field-id-aware physical adapters, deletion-vector decoding inside `LoadExec`) are
intentionally absent and surface as typed `plan_compilation` / `Unsupported` errors at
compile time so connectors fail fast instead of silently reading the wrong data.

## What ships in this slice

| Module | Purpose |
| --- | --- |
| `compile::expr_translator` | `delta_kernel::expressions::Expression` / `Predicate` -> DataFusion `Expr`. |
| `compile::stamp_udf` | Internal "stamp" UDF used by reducer sinks. |
| `compile::json_parse` (internal) | JSON-stats column extraction UDFs over the kernel `StructType`. |
| `compile::logical` | Per-`NodeKind` logical-plan lowering: `Values`, `Filter`, `Project`, `Union`, `MaxByVersion`, `EquiJoin`, and `Load`, with a `compile_plan` entry point that walks the IR. |
| `exec` | `FileListingExec` and the streaming `LoadExec` / `LoadTableProvider` pair (no-DV / no-column-mapping subset). |
| `executor` | `DataFusionExecutor`: drives kernel coroutine state machines (scan / scan_metadata / full_state) and compiles `ResultPlan` -> `DataFrame`. |
| `error` | Typed bridges (`DataFusionError` <-> `DeltaError`), `DfResultIntoDelta` extension trait. |
| `testing` | Buffered collectors over `DataFusionExecutor` for integration tests. |

## Status

Experimental. Public surface is unstable while the executor matures. Deletion vectors and
field-id-aware Parquet reads land once DataFusion 54 ships.
