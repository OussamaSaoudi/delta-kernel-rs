//! Async scan extensions for DataFusion-backed execution.
//!
//! This module provides extension traits for executing Delta table scans
//! asynchronously using the DataFusion executor.
//!
//! # Example
//!
//! ```ignore
//! use delta_kernel_datafusion::{DataFusionExecutor, ScanAsyncExt, SnapshotAsyncBuilderExt};
//! use delta_kernel::Snapshot;
//! use futures::StreamExt;
//! use std::sync::Arc;
//!
//! let executor = Arc::new(DataFusionExecutor::new()?);
//! let table_url = url::Url::parse("file:///path/to/delta/table/")?;
//!
//! // Build snapshot
//! let snapshot = Snapshot::async_builder(table_url)
//!     .build(&executor)
//!     .await?;
//!
//! // Build and execute scan
//! let scan = Arc::new(snapshot).scan_builder().build()?;
//! let mut stream = std::pin::pin!(scan.scan_metadata_async(executor));
//!
//! while let Some(result) = stream.next().await {
//!     let metadata = result?;
//!     // Process scan_metadata.scan_files and scan_metadata.scan_file_transforms
//! }
//! ```

use std::sync::Arc;

use delta_kernel::scan::{Scan, ScanMetadata};
use delta_kernel::DeltaResult;
use futures::Stream;

use crate::driver::scan_metadata_stream_async;
use crate::executor::DataFusionExecutor;

/// Extension trait for async scan operations.
///
/// This trait provides async methods for executing Delta table scans
/// using the DataFusion executor, mirroring the sync `Scan::scan_metadata`
/// API from delta-kernel-rs.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_datafusion::ScanAsyncExt;
///
/// let scan = snapshot.scan_builder().build()?;
/// let mut stream = std::pin::pin!(scan.scan_metadata_async(executor));
///
/// while let Some(result) = stream.next().await {
///     let metadata = result?;
///     // Process metadata...
/// }
/// ```
pub trait ScanAsyncExt {
    /// Get an async stream of [`ScanMetadata`] for this scan.
    ///
    /// This is the async equivalent of [`Scan::scan_metadata`] but uses DataFusion
    /// for plan execution instead of the sync kernel engine.
    ///
    /// # Arguments
    ///
    /// * `executor` - The DataFusion executor to use for plan execution
    ///
    /// # Returns
    ///
    /// A stream of `ScanMetadata` items, each containing:
    /// - `scan_files`: Filtered engine data with file information
    /// - `scan_file_transforms`: Row-level transformations for each file
    ///
    /// # Errors
    ///
    /// The stream yields errors if:
    /// - There are issues reading the Delta log
    /// - Plan execution fails
    /// - Transform computation fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// use delta_kernel_datafusion::ScanAsyncExt;
    /// use futures::StreamExt;
    ///
    /// let scan = snapshot.scan_builder().build()?;
    /// let mut stream = std::pin::pin!(scan.scan_metadata_async(executor));
    ///
    /// while let Some(result) = stream.next().await {
    ///     let metadata = result?;
    ///     println!("Files in batch: {}", metadata.scan_files.data().len());
    /// }
    /// ```
    fn scan_metadata_async(
        &self,
        executor: Arc<DataFusionExecutor>,
    ) -> impl Stream<Item = DeltaResult<ScanMetadata>> + Send;
}

impl ScanAsyncExt for Scan {
    fn scan_metadata_async(
        &self,
        executor: Arc<DataFusionExecutor>,
    ) -> impl Stream<Item = DeltaResult<ScanMetadata>> + Send {
        // We need to handle the Result from into_scan_state
        // Create a wrapper stream that handles the initial error case
        let scan_state_result = self.into_scan_state();

        async_stream::try_stream! {
            // Handle the potential error from into_scan_state
            let scan_state = scan_state_result?;
            
            // Delegate to the existing scan_metadata_stream_async function
            let inner_stream = scan_metadata_stream_async(scan_state, executor);
            futures::pin_mut!(inner_stream);
            
            while let Some(result) = futures::StreamExt::next(&mut inner_stream).await {
                yield result?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Integration tests for ScanAsyncExt are in the tests/integration_test.rs file
    // since they require actual Delta tables and async runtime setup.

    #[test]
    fn test_trait_is_object_safe() {
        // Verify the trait can be used with the Scan type
        fn _accepts_scan_ext<T: ScanAsyncExt>(_: &T) {}
    }
}

