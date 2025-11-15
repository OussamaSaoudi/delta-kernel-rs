//! Benchmarking infrastructure for Delta Kernel.
//!
//! This module provides a framework for running structured benchmarks using JSON workload
//! specifications. It supports multiple workload types (read, snapshot construction, etc.)
//! and integrates with Criterion for performance measurement.
//!
//! # Architecture
//!
//! - **Models**: Data structures for workload specifications (JSON deserialization)
//! - **Runners**: Execute specific workload types (read, snapshot, etc.)
//! - **Utils**: Helper functions for loading and managing workload specs
//!
//! # Example
//!
//! ```rust,ignore
//! use delta_kernel::benchmarks::{load_all_workloads, WorkloadSpec};
//!
//! let specs = load_all_workloads("benches/workload_specs")?;
//! for spec in specs {
//!     let runner = spec.create_runner(engine);
//!     runner.setup()?;
//!     runner.execute()?;
//! }
//! ```

pub mod models;
pub mod runners;
pub mod utils;

pub use models::{
    ReadOperationType, ReadSpec, SnapshotConstructionSpec, TableInfo, WorkloadSpec,
    WorkloadSpecVariant,
};
pub use runners::{create_runner, ReadMetadataRunner, SnapshotConstructionRunner, WorkloadRunner};
pub use utils::{load_all_workloads, WorkloadLoadError};

