# DataFusion Integration - Initial Implementation Summary

## What We Built

### New Crate: `delta-kernel-datafusion`

Created a separate workspace crate (`kernel-datafusion/`) that provides DataFusion-backed async execution for Delta Kernel plans.

### Core Components Implemented

1. **`DataFusionExecutor`** (`executor.rs`)
   - Main executor with `SessionState` management
   - `execute_to_stream(plan) -> SendableRecordBatchStream` API
   - Can compile and execute `DeclarativePlanNode` trees

2. **`results_stream` Driver** (`driver.rs`)
   - Async equivalent of kernel's sync `ResultsDriver`
   - Drives state machines by repeatedly getting plans, executing via DF, and advancing
   - Handles both `Results` sinks (yield batches) and `Drop` sinks (drain for side effects)
   - Returns `Stream<Item = DeltaResult<FilteredEngineData>>`

3. **Plan Compiler** (`compile.rs`)
   - `compile_plan(DeclarativePlanNode) -> Arc<dyn ExecutionPlan>`
   - Matches on each node type and dispatches to lowering functions
   - Currently all lowering functions are placeholders returning `Unsupported` errors

4. **Expression Lowering** (`expr.rs`)
   - `lower_expression(kernel::Expression) -> datafusion::Expr`
   - `lower_predicate(kernel::Predicate) -> datafusion::Expr`
   - Placeholder implementations

5. **Custom Execution Nodes** (`exec/`)
   - `KdfFilterExec`: applies KDF filters with per-KDF concurrency policies
   - `ConsumeKdfExec`: applies consumer KDFs with Continue/Break semantics
   - `FileListingExec`: lists files via DF object_store, outputs as RecordBatch rows
   - `SchemaQueryExec`: reads parquet footers, stores schema in state
   - All currently have `todo!()` implementations

6. **Error Handling** (`error.rs`)
   - `DfError` enum covering DF, kernel, arrow, object_store errors
   - `DfResult<T>` type alias
   - Conversions to/from kernel `Error`

## Dependencies

- **DataFusion 51**: Compatible with arrow-57 (same as kernel)
- **Delta Kernel**: With `default-engine-rustls` feature (includes arrow-conversion, engine modules)
- **async-stream**: For ergonomic async generator syntax
- **Tokio**: Async runtime

## Key Design Decisions

### Separate Crate
- Keeps kernel core clean
- Makes DF dependency opt-in
- Clear separation of sync (kernel) vs async (DF) paths

### Plan Lowering Strategy
| Kernel Node | Target DF Lowering |
|-------------|-------------------|
| Scan | Native ParquetExec / NdJsonExec |
| FilterByExpression | Native FilterExec |
| Select | Native ProjectionExec |
| FilterByKDF | Custom KdfFilterExec (with concurrency policy) |
| ConsumeByKDF | Custom ConsumeKdfExec |
| FileListing | Custom FileListingExec (DF object_store) |
| SchemaQuery | Custom SchemaQueryExec |
| ParseJson/FirstNonNull | Native DF functions or custom exec |

### KDF Concurrency
- **Mutating KDFs** (e.g. AddRemoveDedup): enforce `SinglePartition` distribution
- **Parallel-safe KDFs** (e.g. CheckpointDedup): allow multi-partition execution
- Enforced via DataFusion's `required_input_distribution` API

## Code Statistics

**Total new LOC**: ~960

- `executor.rs`: ~65 LOC
- `driver.rs`: ~170 LOC
- `compile.rs`: ~150 LOC
- `expr.rs`: ~15 LOC (stubs)
- `exec/*.rs`: ~350 LOC (stubs with ExecutionPlan trait impls)
- `error.rs`: ~40 LOC
- `lib.rs`: ~45 LOC
- `Cargo.toml` + `README.md`: ~130 LOC

## Current Status

✅ **Compiles successfully**
✅ Basic structure in place
✅ Clear extension points for remaining work

❌ No actual plan execution yet (all lowering functions return `Unsupported`)
❌ No tests

## Next Steps (Remaining TODOs)

1. **Expression Lowering** (~600-1200 LOC estimated)
   - Map kernel Expression/Predicate to DF Expr
   - Handle built-in ops, columns, literals, opaque ops (as DF function calls)

2. **Native Scan Lowering** (~300-700 LOC)
   - Parquet: create DF ParquetExec from kernel ScanNode
   - JSON: create DF NdJsonExec
   - Handle object_store URL registration

3. **Native Filter/Project Lowering** (~200-400 LOC)
   - Use expression lowering to create FilterExec / ProjectionExec

4. **KDF Exec Implementations** (~600-1200 LOC)
   - Implement `execute()` for KdfFilterExec (apply filter, physical filtering)
   - Implement `execute()` for ConsumeKdfExec (apply consumer, handle early break)
   - Add `properties()` with correct partitioning requirements

5. **File Listing Exec** (~250-600 LOC)
   - Use DF object_store to list files
   - Ensure deterministic ordering (sorted)
   - Output as RecordBatch

6. **Schema Query Exec** (~200-450 LOC)
   - Read parquet footer/metadata
   - Store schema in node state
   - Make idempotent (guard against DF retries)

7. **ParseJson/FirstNonNull** (~200-800 LOC)
   - Try native DF solutions first
   - Fall back to custom exec if needed

8. **Tests** (~400-900 LOC)
   - Async equivalence tests (DF vs sync path)
   - Partitioning constraint tests
   - Expression lowering tests

**Estimated remaining**: ~2.5k - 5.5k LOC

## Files Created

```
kernel-datafusion/
├── Cargo.toml
├── README.md
└── src/
    ├── lib.rs
    ├── error.rs
    ├── executor.rs
    ├── driver.rs
    ├── compile.rs
    ├── expr.rs
    └── exec/
        ├── mod.rs
        ├── kdf_filter.rs
        ├── kdf_consume.rs
        ├── file_listing.rs
        └── schema_query.rs
```

## Git Commit

```
commit a6f808e6
feat: Add delta-kernel-datafusion crate with initial scaffold
```

## How to Test

```bash
cd kernel-datafusion
cargo check  # ✅ passes
cargo test   # (no tests yet)
```

## Architecture Diagram

```
┌──────────────────────────────────────┐
│      State Machine (Kernel)         │
│  (ScanStateMachine, SnapshotSM...)   │
└──────────┬───────────────────────────┘
           │ get_plan()
           ▼
┌──────────────────────────────────────┐
│     DeclarativePlanNode (Kernel)     │
│  (Scan, Filter, KDF, Sink, ...)      │
└──────────┬───────────────────────────┘
           │
           │ compile_plan()
           ▼
┌──────────────────────────────────────┐
│   DataFusion ExecutionPlan           │
│  (ParquetExec, FilterExec, Custom)   │
└──────────┬───────────────────────────┘
           │ execute(partition, ctx)
           ▼
┌──────────────────────────────────────┐
│   SendableRecordBatchStream          │
│  (async RecordBatch stream)          │
└──────────┬───────────────────────────┘
           │
           │ adapt to FilteredEngineData
           ▼
┌──────────────────────────────────────┐
│   results_stream (async driver)      │
│  Stream<DeltaResult<FilteredData>>   │
└──────────────────────────────────────┘
```

