//! Async snapshot builder for DataFusion-backed execution.
//!
//! This module provides an async builder pattern for constructing Delta table
//! snapshots using the DataFusion executor, mirroring the sync `SnapshotBuilder`
//! API from delta-kernel-rs.
//!
//! # Example
//!
//! ```ignore
//! use delta_kernel_datafusion::{DataFusionExecutor, AsyncSnapshotBuilder};
//! use std::sync::Arc;
//!
//! let executor = Arc::new(DataFusionExecutor::new()?);
//! let table_url = url::Url::parse("file:///path/to/delta/table/")?;
//!
//! // Build latest snapshot
//! let snapshot = AsyncSnapshotBuilder::new(table_url.clone())
//!     .build(&executor)
//!     .await?;
//!
//! // Build snapshot at specific version
//! let snapshot_v5 = AsyncSnapshotBuilder::new(table_url)
//!     .with_version(5)
//!     .build(&executor)
//!     .await?;
//! ```

use delta_kernel::plans::state_machines::SnapshotStateMachine;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Version};
use url::Url;

use crate::driver::execute_state_machine_async;
use crate::executor::DataFusionExecutor;

/// Async builder for constructing Delta table snapshots.
///
/// This builder provides a fluent API for configuring and building snapshots
/// using the DataFusion executor for async plan execution.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_datafusion::AsyncSnapshotBuilder;
///
/// let snapshot = AsyncSnapshotBuilder::new(table_url)
///     .with_version(5)  // optional: target specific version
///     .build(&executor)
///     .await?;
/// ```
pub struct AsyncSnapshotBuilder {
    table_root: Url,
    version: Option<Version>,
}

impl AsyncSnapshotBuilder {
    /// Create a new async snapshot builder for the given table root URL.
    ///
    /// By default, this will build a snapshot at the latest table version.
    /// Use [`with_version`](Self::with_version) to target a specific version.
    pub fn new(table_root: Url) -> Self {
        Self {
            table_root,
            version: None,
        }
    }

    /// Set the target version for the snapshot.
    ///
    /// If not set, the builder will construct a snapshot at the latest version.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = AsyncSnapshotBuilder::new(table_url)
    ///     .with_version(5)
    ///     .build(&executor)
    ///     .await?;
    /// assert_eq!(snapshot.version(), 5);
    /// ```
    pub fn with_version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Optionally set the target version for the snapshot.
    ///
    /// If `version` is `None`, this is a no-op and the builder will target
    /// the latest version.
    pub fn with_version_opt(mut self, version: Option<Version>) -> Self {
        self.version = version;
        self
    }

    /// Build the snapshot asynchronously using the provided DataFusion executor.
    ///
    /// This method creates a `SnapshotStateMachine` and drives it to completion
    /// using DataFusion for plan execution.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table does not exist at the specified location
    /// - The specified version does not exist
    /// - There are issues reading the Delta log
    pub async fn build(self, executor: &DataFusionExecutor) -> DeltaResult<Snapshot> {
        let sm = match self.version {
            Some(version) => {
                // SnapshotStateMachine::with_version expects i64
                let version_i64: i64 = version.try_into().map_err(|_| {
                    delta_kernel::Error::generic(format!(
                        "Version {} is too large to convert to i64",
                        version
                    ))
                })?;
                SnapshotStateMachine::with_version(self.table_root, version_i64)?
            }
            None => SnapshotStateMachine::new(self.table_root)?,
        };
        execute_state_machine_async(executor, sm).await
    }
}

impl std::fmt::Debug for AsyncSnapshotBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncSnapshotBuilder")
            .field("table_root", &self.table_root.as_str())
            .field("version", &self.version)
            .finish()
    }
}

/// Extension trait for creating async snapshot builders from URLs.
///
/// This trait provides a convenient `async_builder()` method that mirrors
/// the delta-kernel-rs `Snapshot::builder_for()` API pattern.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_datafusion::SnapshotAsyncBuilderExt;
///
/// let snapshot = Snapshot::async_builder(table_url)
///     .with_version(5)
///     .build(&executor)
///     .await?;
/// ```
pub trait SnapshotAsyncBuilderExt {
    /// Create an async builder for constructing a snapshot at this table location.
    ///
    /// This is the async equivalent of `Snapshot::builder_for()` from delta-kernel-rs.
    fn async_builder(table_root: Url) -> AsyncSnapshotBuilder;
}

impl SnapshotAsyncBuilderExt for Snapshot {
    fn async_builder(table_root: Url) -> AsyncSnapshotBuilder {
        AsyncSnapshotBuilder::new(table_root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_creation() {
        let url = Url::parse("file:///path/to/table/").unwrap();
        let builder = AsyncSnapshotBuilder::new(url.clone());
        assert_eq!(builder.table_root, url);
        assert_eq!(builder.version, None);
    }

    #[test]
    fn test_builder_with_version() {
        let url = Url::parse("file:///path/to/table/").unwrap();
        let builder = AsyncSnapshotBuilder::new(url).with_version(5);
        assert_eq!(builder.version, Some(5));
    }

    #[test]
    fn test_builder_with_version_opt_some() {
        let url = Url::parse("file:///path/to/table/").unwrap();
        let builder = AsyncSnapshotBuilder::new(url).with_version_opt(Some(10));
        assert_eq!(builder.version, Some(10));
    }

    #[test]
    fn test_builder_with_version_opt_none() {
        let url = Url::parse("file:///path/to/table/").unwrap();
        let builder = AsyncSnapshotBuilder::new(url).with_version_opt(None);
        assert_eq!(builder.version, None);
    }

    #[test]
    fn test_builder_debug() {
        let url = Url::parse("file:///path/to/table/").unwrap();
        let builder = AsyncSnapshotBuilder::new(url).with_version(5);
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("AsyncSnapshotBuilder"));
        assert!(debug_str.contains("version: Some(5)"));
    }
}

