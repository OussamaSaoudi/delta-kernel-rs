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
- **High-level APIs** (mirrors delta-kernel-rs patterns):
  - `Snapshot::async_builder()` - async snapshot builder via `SnapshotAsyncBuilderExt` trait
  - `AsyncSnapshotBuilder` - configurable builder with `.with_version()` support
  - `scan.scan_metadata_async()` - async scan execution via `ScanAsyncExt` trait
- **Low-level APIs**:
  - `build_snapshot_async`, `build_snapshot_at_version_async`
  - `scan_metadata_stream_async` with `ScanState` API
- Plan compiler with native DataFusion lowering for most nodes
- Custom exec nodes (KDF filter/consumer, file listing, schema query)
- Expression lowering (partial - Struct expressions TODO)

### TODO 📋
- Struct expression lowering
- Full expression/predicate lowering coverage
- Performance optimizations
- Additional integration tests

## Usage Examples

### High-Level API (Recommended)

The high-level API mirrors delta-kernel-rs patterns with async builder methods:

```rust
use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
use delta_kernel::Snapshot;
use futures::StreamExt;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(DataFusionExecutor::new()?);
    let table_url = url::Url::parse("file:///path/to/delta/table/")?;
    
    // Build snapshot using async builder (mirrors Snapshot::builder_for())
    let snapshot = Snapshot::async_builder(table_url)
        .with_version(5)  // optional: target specific version
        .build(&executor)
        .await?;
    
    println!("Table version: {}", snapshot.version());
    
    // Build and execute scan
    let scan = Arc::new(snapshot).scan_builder()
        .with_predicate(predicate)  // optional
        .build()?;
    
    // Stream scan metadata asynchronously using extension trait
    let mut stream = std::pin::pin!(scan.scan_metadata_async(executor));
    while let Some(result) = stream.next().await {
        let metadata = result?;
        // Process metadata.scan_files and metadata.scan_file_transforms
    }
    
    Ok(())
}
```

### Building a Snapshot (Alternative)

You can also use the standalone functions directly:

```rust
use delta_kernel_datafusion::{DataFusionExecutor, build_snapshot_async};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(DataFusionExecutor::new()?);
    let table_url = url::Url::parse("file:///path/to/delta/table/")?;
    
    let snapshot = build_snapshot_async(&executor, table_url).await?;
    println!("Table version: {}", snapshot.version());
    
    Ok(())
}
```

### Streaming Scan Metadata (Alternative)

The `scan_metadata_stream_async` function provides lower-level control:

```rust
use delta_kernel_datafusion::{DataFusionExecutor, build_snapshot_async, scan_metadata_stream_async};
use futures::StreamExt;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(DataFusionExecutor::new()?);
    let table_url = url::Url::parse("file:///path/to/delta/table/")?;
    
    // Build snapshot and scan
    let snapshot = Arc::new(build_snapshot_async(&executor, table_url).await?);
    let scan = snapshot.scan_builder().build()?;
    
    // Get scan state (owns state machine + transform computer)
    let scan_state = scan.into_scan_state()?;
    
    // Stream scan metadata asynchronously
    let mut stream = std::pin::pin!(scan_metadata_stream_async(scan_state, executor));
    
    while let Some(result) = stream.next().await {
        let scan_metadata = result?;
        // scan_metadata.scan_files - FilteredEngineData with file info
        // scan_metadata.scan_file_transforms - Transform expressions per file
        
        // Process files...
    }
    
    Ok(())
}
```

### Low-level State Machine Execution

For advanced use cases, you can work directly with state machines:

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


