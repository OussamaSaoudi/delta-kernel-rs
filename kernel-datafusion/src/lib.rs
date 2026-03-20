//! DataFusion-based executor for Delta Kernel declarative plans.
//!
//! This crate provides an async streaming execution path for `DeclarativePlanNode` trees
//! by compiling them into mostly-native DataFusion physical plans.
//!
//! # Architecture
//!
//! - **Sync path** (in `delta_kernel`): `DeclarativePlanExecutor` + `ResultsDriver` (Iterator)
//! - **Async path** (this crate): `DataFusionExecutor` + `ResultsStreamDriver` (Stream)
//!
//! # Features
//!
//! - Native DataFusion scans for Parquet/NDJSON (no kernel Engine handlers)
//! - Native DataFusion filter/projection via expression lowering
//! - Custom physical operators for KDFs with per-KDF concurrency policies
//! - Custom operators for Delta-specific nodes (FileListing, SchemaQuery)
//!
//! # High-Level API Example
//!
//! The recommended API mirrors delta-kernel-rs patterns with async builder methods:
//!
//! ```ignore
//! use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
//! use delta_kernel::Snapshot;
//! use futures::StreamExt;
//! use std::sync::Arc;
//!
//! let executor = Arc::new(DataFusionExecutor::new()?);
//! let table_url = url::Url::parse("file:///path/to/delta/table/")?;
//!
//! // Build snapshot using async builder (mirrors Snapshot::builder_for())
//! let snapshot = Snapshot::async_builder(table_url)
//!     .with_version(5)  // optional: target specific version
//!     .build(&executor)
//!     .await?;
//!
//! // Build and execute scan
//! let scan = Arc::new(snapshot).scan_builder()
//!     .with_predicate(predicate)  // optional
//!     .build()?;
//!
//! // Stream scan metadata asynchronously
//! let mut stream = std::pin::pin!(scan.scan_metadata_async(executor));
//! while let Some(result) = stream.next().await {
//!     let metadata = result?;
//!     // Process metadata.scan_files and metadata.scan_file_transforms
//! }
//! ```
//!
//! # Low-Level State Machine Example
//!
//! For advanced use cases, you can work directly with state machines:
//!
//! ```ignore
//! use delta_kernel_datafusion::{DataFusionExecutor, results_stream};
//! use delta_kernel::plans::state_machines::ScanStateMachine;
//!
//! let executor = DataFusionExecutor::new()?;
//! let sm = ScanStateMachine::from_scan_config(...)?;
//! let mut stream = results_stream(sm, executor);
//!
//! while let Some(batch) = stream.next().await {
//!     let filtered_data = batch?;
//!     // process batch
//! }
//! ```

pub mod executor;
pub mod compile;
pub mod scan;
pub mod expr;
pub mod json_parse;
pub mod exec;
pub mod driver;
pub mod error;
pub mod snapshot_builder;
pub mod scan_ext;

// Re-export main APIs
pub use executor::DataFusionExecutor;
pub use driver::{
    results_stream,
    ResultsStreamDriver,
    execute_state_machine_async,
    build_snapshot_async,
    build_snapshot_at_version_async,
    scan_metadata_stream_async,
};
pub use error::{DfResult, DfError};

// Re-export high-level async builder APIs
pub use snapshot_builder::{AsyncSnapshotBuilder, SnapshotAsyncBuilderExt};
pub use scan_ext::ScanAsyncExt;

