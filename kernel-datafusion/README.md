# delta-kernel-datafusion

DataFusion integration for Delta Kernel - provides async plan execution via native DataFusion operators.

## Overview

This crate implements a DataFusion-backed async executor for Delta Kernel's `DeclarativePlanNode` trees. It compiles kernel plans into mostly-native DataFusion physical execution plans.

## Quick Start

```rust
use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
use delta_kernel::Snapshot;
use futures::StreamExt;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(DataFusionExecutor::new()?);
    let table_url = url::Url::parse("file:///path/to/delta/table/")?;

    // Build snapshot
    let snapshot = Snapshot::async_builder(table_url)
        .with_version(5)  // optional
        .build(&executor)
        .await?;

    // Build and execute scan
    let scan = Arc::new(snapshot).scan_builder().build()?;
    let mut stream = std::pin::pin!(scan.scan_metadata_async(executor));

    while let Some(result) = stream.next().await {
        let metadata = result?;
        // Process metadata.scan_files
    }

    Ok(())
}
```

## Development

```bash
cargo check
cargo test
```

## License

Apache-2.0


