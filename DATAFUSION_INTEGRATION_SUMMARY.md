# DataFusion Executor Integration - Summary

## Overview

Successfully implemented a DataFusion-based executor for delta-kernel-rs declarative plans. The integration allows DataFusion to execute kernel plans natively, maximizing use of DataFusion's query engine while preserving ordering guarantees critical for Kernel-Defined Functions (KDFs).

## Implementation Summary

### Core Components

#### 1. Expression Lowering (`expr.rs`)
- **Purpose**: Converts kernel `Expression` and `Predicate` into DataFusion `Expr`
- **Key Features**:
  - Handles all kernel expression types (literals, columns, unary, binary, variadic)
  - Maps kernel operators to DataFusion equivalents
  - Supports COALESCE, IN predicates, and all comparison operators
  - Converts kernel scalars (integers, floats, strings, decimals, arrays) to DataFusion literals
- **LOC**: ~300 lines

#### 2. Plan Compilation (`compile.rs`)
- **Purpose**: Compiles `DeclarativePlanNode` trees into DataFusion `ExecutionPlan`s
- **Native DataFusion Mappings**:
  - `FilterByExpression` → `FilterExec`
  - `Select` → `ProjectionExec`
  - `Scan` → `DataSourceExec` (with native Parquet/JSON readers)
- **Custom Operators**:
  - `FilterByKDF` → `KdfFilterExec`
  - `ConsumeByKDF` → `ConsumeKdfExec`
  - `FileListing` → `FileListingExec`
  - `SchemaQuery` → `SchemaQueryExec` (stubbed)
- **Not Yet Implemented**:
  - `ParseJson` (requires JSON parsing UDF)
  - `FirstNonNull` (requires `first_value` UDAF with ORDER BY)
- **LOC**: ~190 lines

#### 3. Native Scan Implementation (`scan.rs`)
- **Purpose**: Compiles kernel `ScanNode` to DataFusion's native file format readers
- **Key Features**:
  - Uses `datafusion-datasource-parquet::ParquetSource` for Parquet files
  - Uses `datafusion-datasource-json::JsonSource` for JSON files
  - Single `FileGroup` to preserve file ordering (critical for KDFs)
  - Proper `ObjectStoreUrl` handling (scheme + authority only)
- **LOC**: ~110 lines

#### 4. KDF Filter Executor (`exec/kdf_filter.rs`)
- **Purpose**: Custom DataFusion operator for `FilterByKDF` nodes
- **Key Features**:
  - Wraps kernel `FilterKdfState` (e.g., `AddRemoveDedupState`, `CheckpointDedupState`)
  - **Ordering Validation**: Checks descending version order if `version` column present
  - **Parallelization**: Marks parallel-safe KDFs (e.g., `CheckpointDedupState`) for distributed execution
  - **Stateful Execution**: Single-threaded for stateful KDFs (e.g., `AddRemoveDedupState`)
  - Applies KDF filter per batch using `ArrowEngineData` wrapper
- **LOC**: ~150 lines

#### 5. KDF Consumer Executor (`exec/kdf_consume.rs`)
- **Purpose**: Custom DataFusion operator for `ConsumeByKDF` nodes
- **Key Features**:
  - Wraps kernel `ConsumerKdfState` (e.g., `LogSegmentBuilderState`)
  - **Hard Stop**: Terminates stream immediately when KDF returns `false`
  - Passes through batches while applying side effects
  - Always single-threaded (consumers are inherently stateful)
- **LOC**: ~160 lines

#### 6. File Listing Executor (`exec/file_listing.rs`)
- **Purpose**: Executes `FileListingNode` by listing files from storage
- **Key Features**:
  - Calls `StorageHandler::list_from`
  - Converts `FileMeta` to `RecordBatch` with schema `(path: Utf8, size: Int64, last_modified: Int64)`
  - Single-batch output
- **LOC**: ~160 lines

#### 7. Schema Query Executor (`exec/schema_query.rs`)
- **Status**: Stubbed (not implemented)
- **Reason**: Requires deep integration with kernel's `ParquetHandler` trait
- **LOC**: ~50 lines (stub)

#### 8. Async Stream Driver (`driver.rs`)
- **Purpose**: Provides `results_stream` helper for async stream execution
- **Key Features**:
  - Compiles plan and executes via DataFusion
  - **Drop Sink Handling**: Drains stream for side effects when `is_drop_sink()`
  - **Results Sink Handling**: Yields batches when `is_results_sink()`
  - Returns `SendableRecordBatchStream`
- **LOC**: ~170 lines

#### 9. High-Level Executor API (`executor.rs`)
- **Purpose**: User-facing API for DataFusion executor
- **Key Types**:
  - `DataFusionExecutor`: Wraps DataFusion `SessionState`
  - `DataFusionResultsDriver`: Async stream driver for plans
- **LOC**: ~70 lines

#### 10. Error Handling (`error.rs`)
- **Purpose**: Custom error types for DataFusion integration
- **Error Variants**:
  - `DataFusion`: Wraps DataFusion errors
  - `Kernel`: Wraps kernel errors
  - `ExpressionLowering`: Expression conversion errors
  - `PlanCompilation`: Plan compilation errors
  - `Unsupported`: Features not yet implemented
- **LOC**: ~40 lines

#### 11. Module Organization (`lib.rs`)
- **Purpose**: Public exports and feature gating
- **LOC**: ~50 lines

### Total Implementation Size
- **Core Implementation**: ~1,450 LOC
- **Tests**: ~200 LOC
- **Total**: ~1,650 LOC

## Ordering Guarantees

### Critical for Correctness
KDFs depend on batches arriving in descending version order. Three mechanisms ensure this:

1. **Scan-Level Ordering**:
   - All files in a `ScanNode` go into a single `FileGroup`
   - DataFusion processes single `FileGroup` sequentially
   - File order preserved as provided by kernel

2. **KDF Validation**:
   - `KdfFilterExec` validates version descending order
   - Checks `version` column if present
   - Returns error if ordering violated

3. **Single Partitioning**:
   - Stateful KDFs force `UnknownPartitioning(1)`
   - Prevents DataFusion from re-partitioning
   - Ensures all batches flow through single executor instance

## Parallelization Strategy

### KDF Parallelization Matrix

| KDF | Type | Parallelizable? | Reason |
|-----|------|-----------------|--------|
| `AddRemoveDedupState` | Filter | ❌ No | Mutates internal hashmap, order-dependent |
| `CheckpointDedupState` | Filter | ✅ Yes | Immutable after construction, serializable |
| `PartitionPruneState` | Filter | ✅ Yes | Immutable filter evaluation |
| `LogSegmentBuilderState` | Consumer | ❌ No | Builds stateful log segment |
| `CheckpointHintReaderState` | Consumer | ❌ No | Side effect: reads checkpoint hint |
| `MetadataProtocolReaderState` | Consumer | ❌ No | Side effect: reads protocol/metadata |
| `SidecarCollectorState` | Consumer | ❌ No | Side effect: collects sidecar files |

### Implementation
- `is_filter_kdf_parallel_safe()` determines if KDF can be parallelized
- Parallel-safe KDFs preserve child's `output_partitioning`
- Non-parallel KDFs force `UnknownPartitioning(1)`
- `CheckpointDedupState` already has custom `serialize()`/`deserialize()` for distribution

## Testing

### Integration Tests (`tests/integration_test.rs`)
All tests pass ✅

1. **test_parquet_scan_compilation**: Verifies Parquet scans compile to `DataSourceExec`
2. **test_json_scan_compilation**: Verifies JSON scans compile to `DataSourceExec`
3. **test_kdf_filter_compilation**: Verifies KDF filters compile to `KdfFilterExec`
4. **test_scan_preserves_file_order**: Verifies single partition for ordering
5. **test_schema_conversion**: Verifies kernel → Arrow schema conversion
6. **test_executor_creation**: Verifies executor instantiation
7. **test_simple_expression_lowering**: Verifies expression conversion
8. **test_predicate_lowering**: Verifies predicate conversion

```bash
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Compilation Status

✅ **Full project compiles successfully with DataFusion 51**

```bash
$ cargo build --lib
   Compiling delta_kernel_datafusion v0.18.1
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

## Known Limitations

### Not Yet Implemented
1. **ParseJson**: Requires custom UDF or integration with DataFusion's JSON parsing
2. **FirstNonNull**: Requires `first_value` UDAF with ORDER BY support
3. **SchemaQuery**: Requires kernel `ParquetHandler` integration (reads schema without data)

### Design Decisions
1. **No Custom Scan**: Uses DataFusion's native `ParquetSource`/`JsonSource` as explicitly requested
2. **Single FileGroup**: Preserves ordering but may limit parallelism for independent scans
3. **Version Validation**: Runtime check (not compile-time) for ordering violations
4. **ArrowEngineData Wrapper**: Required to satisfy `EngineData` trait for KDF apply

## Risky Areas

### 1. Ordering Guarantees (MITIGATED)
- **Risk**: DataFusion might reorder batches
- **Mitigation**: Single `FileGroup` + single partition + runtime validation
- **Status**: ✅ Design reviewed, validation implemented

### 2. KDF Parallelization (NEEDS TESTING)
- **Risk**: Parallel execution might violate KDF assumptions
- **Mitigation**: Conservative `is_parallel_safe` check, explicit partitioning control
- **Status**: ⚠️ Needs distributed execution testing with real Delta tables

### 3. Object Store Integration (PARTIAL)
- **Risk**: `ObjectStoreUrl` handling for various storage backends (S3, ADLS, GCS)
- **Mitigation**: Uses DataFusion's `ObjectStoreUrl` parsing
- **Status**: ⚠️ Only tested with `file://` URLs, needs cloud storage testing

### 4. Schema Compatibility (TESTED)
- **Risk**: Kernel schema → Arrow schema conversion edge cases
- **Mitigation**: Uses kernel's `try_into_arrow()` conversion
- **Status**: ✅ Conversion tested, compiles successfully

### 5. Not Implemented Features (KNOWN)
- **Risk**: `ParseJson`, `FirstNonNull`, `SchemaQuery` will fail at runtime
- **Mitigation**: Return `Unsupported` errors with clear messages
- **Status**: ⚠️ Need to implement or document workarounds

## Next Steps

### Short Term (Required for Production)
1. ✅ ~~Implement native DataFusion scan~~
2. ⬜ Test with real Delta tables (acceptance tests)
3. ⬜ Implement `FirstNonNull` as UDAF
4. ⬜ Implement `ParseJson` or integrate with DataFusion's JSON parsing
5. ⬜ Implement `SchemaQuery` via kernel `ParquetHandler`

### Medium Term (Performance & Robustness)
1. ⬜ Distributed execution testing with `CheckpointDedupState`
2. ⬜ Cloud storage integration testing (S3, ADLS, GCS)
3. ⬜ Benchmark vs. sync executor
4. ⬜ Optimize single-partition scans (consider streaming vs. batch)

### Long Term (Advanced Features)
1. ⬜ Custom predicate pushdown for KDFs
2. ⬜ Adaptive parallelization based on data characteristics
3. ⬜ Integration with DataFusion's cost-based optimizer

## Files Modified/Created

### Created
- `kernel-datafusion/src/lib.rs` (module root)
- `kernel-datafusion/src/error.rs` (error types)
- `kernel-datafusion/src/expr.rs` (expression lowering)
- `kernel-datafusion/src/compile.rs` (plan compilation)
- `kernel-datafusion/src/scan.rs` (native scan implementation)
- `kernel-datafusion/src/executor.rs` (high-level API)
- `kernel-datafusion/src/driver.rs` (async stream driver)
- `kernel-datafusion/src/exec/mod.rs` (exec module)
- `kernel-datafusion/src/exec/kdf_filter.rs` (KDF filter operator)
- `kernel-datafusion/src/exec/kdf_consume.rs` (KDF consumer operator)
- `kernel-datafusion/src/exec/file_listing.rs` (file listing operator)
- `kernel-datafusion/src/exec/schema_query.rs` (schema query stub)
- `kernel-datafusion/tests/integration_test.rs` (integration tests)
- `kernel-datafusion/Cargo.toml` (new crate)

### Modified
- `kernel/Cargo.toml` (added `datafusion` feature, `chrono` dep)
- `kernel/src/plans/mod.rs` (added `#[cfg(feature = "datafusion")] pub mod df_executor`)

## Conclusion

The DataFusion executor integration is **functionally complete** for the core use cases:
- ✅ Native DataFusion scan (Parquet & JSON)
- ✅ Expression and predicate lowering
- ✅ KDF execution with ordering guarantees
- ✅ Async stream API
- ✅ Parallelization strategy for stateless KDFs
- ✅ All integration tests passing
- ✅ Full project compilation

**Remaining work** focuses on:
1. Acceptance testing with real Delta tables
2. Implementing remaining plan nodes (`FirstNonNull`, `ParseJson`, `SchemaQuery`)
3. Production hardening (error handling, cloud storage, distributed testing)

The implementation prioritizes **correctness** (ordering guarantees) and **native DataFusion integration** (no custom scan) as explicitly requested. The design is extensible for future optimizations while maintaining compatibility with the existing sync executor.


