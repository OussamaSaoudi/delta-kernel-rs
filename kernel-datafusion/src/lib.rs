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
//! # Example
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

