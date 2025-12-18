# delta-kernel-datafusion

DataFusion integration for Delta Kernel - provides async plan execution via native DataFusion operators.

## Overview

This crate implements a DataFusion-backed async executor for Delta Kernel's `DeclarativePlanNode` trees. It compiles kernel plans into mostly-native DataFusion physical execution plans, avoiding kernel `Engine` handlers where possible for better performance and parallelism.

## Architecture

### Dual execution paths

- **Sync** (in `delta_kernel`): `DeclarativePlanExecutor` + `ResultsDriver` (Iterator) - uses kernel `Engine` traits
- **Async** (this crate): `DataFusionExecutor` + `results_stream` (Stream) - uses native DataFusion execution

### Plan lowering strategy

| Kernel Node | DataFusion Lowering |
|-------------|---------------------|
| `Scan` (Parquet/JSON) | Native `ParquetExec` / `NdJsonExec` |
| `FilterByExpression` | Native `FilterExec` (after expression translation) |
| `SelectNode` | Native `ProjectionExec` (after expression translation) |
| `FilterByKDF` | Custom `KdfFilterExec` with concurrency policy |
| `ConsumeByKDF` | Custom `ConsumeKdfExec` with concurrency policy |
| `FileListingNode` | Custom leaf exec using DF `object_store` |
| `SchemaQueryNode` | Custom side-effect exec for parquet footer reads |
| `ParseJsonNode` | Native DF JSON function or custom UDF |
| `FirstNonNullNode` | Native DF aggregate or custom exec |
| `Sink` | Transparent (handled by driver) |

## Status

### Implemented ✅
- Separate `delta-kernel-datafusion` crate
- Basic `DataFusionExecutor` with session state management
- `execute_to_stream` API returning DataFusion `SendableRecordBatchStream`
- Async `results_stream` function for state machine execution
- Plan compiler skeleton with placeholder implementations
- Custom exec node stubs (KDF filter/consumer, file listing, schema query)
- Expression lowering skeleton

### TODO 📋
- Expression/predicate lowering (kernel → DataFusion `Expr`)
- Native scan lowering (Parquet/JSON via DF datasources)
- Native filter/projection lowering
- Full KDF exec implementations with partitioning policies
- File listing exec using DF object_store
- Schema query exec for footer reads
- ParseJson/FirstNonNull lowering
- Async integration tests

## Usage Example

```rust
use delta_kernel_datafusion::{DataFusionExecutor, results_stream};
use delta_kernel::plans::state_machines::ScanStateMachine;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = DataFusionExecutor::new()?;
    let sm = ScanStateMachine::from_scan_config(/* ... */)?;
    
    let mut stream = results_stream(sm, executor);
    
    while let Some(batch) = stream.next().await {
        let filtered_data = batch?;
        // process batch
    }
    
    Ok(())
}
```

## KDF Concurrency Policies

Different KDFs have different parallelism requirements:

- **Mutating/Single-threaded** (e.g. `AddRemoveDedup`): enforce `SinglePartition` distribution
- **Parallel-safe** (e.g. `CheckpointDedup`): allow multi-partition execution

The custom KDF exec nodes enforce these via DataFusion's `required_input_distribution`.

## Development

```bash
# Check the crate
cd kernel-datafusion
cargo check

# Run tests (once implemented)
cargo test

# Build with the parent workspace
cd ..
cargo build -p delta_kernel_datafusion
```

## Dependencies

- DataFusion 51 (compatible with arrow-57)
- Delta Kernel with `default-engine-rustls` feature
- Tokio async runtime
- async-stream for ergonomic stream creation

## License

Apache-2.0 (same as delta-kernel-rs)

