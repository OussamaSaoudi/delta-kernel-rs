//! Executor (Phase 2) log replay composition for distributed execution.
//!
//! This module provides executor-side execution that processes a partition of
//! sidecar or multi-part checkpoint files.

use std::sync::Arc;

use crate::distributed::phases::SidecarPhase;
use crate::log_replay::LogReplayProcessor;
use crate::{DeltaResult, Engine, FileMeta};

/// Executor-side log replay (Phase 2) for distributed execution.
///
/// This iterator processes a partition of sidecar or multi-part checkpoint files
/// that were distributed from the driver.
///
/// # Example
///
/// ```ignore
/// // On executor: receive from driver
/// let (serialized_processor, file_partition) = receive_from_driver()?;
///
/// // Deserialize processor
/// let processor = ScanLogReplayProcessor::deserialize(&serialized_processor, engine)?;
///
/// // Create executor with partition
/// let mut executor = ExecutorV2::new(processor, file_partition, engine)?;
///
/// // Process partition
/// for batch in executor {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// // Extract final processor state
/// let final_processor = executor.finish();
/// ```
pub(crate) struct ExecutorV2<P: LogReplayProcessor> {
    phase: SidecarPhase<P>,
}

impl<P: LogReplayProcessor> ExecutorV2<P> {
    /// Create a new executor-side log replay iterator.
    ///
    /// # Parameters
    /// - `processor`: The deserialized log replay processor from the driver
    /// - `files`: Partition of sidecar/leaf files to process on this executor
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        files: Vec<FileMeta>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let phase = SidecarPhase::new(processor, files, engine)?;
        Ok(Self { phase })
    }

    /// Extract the processor after processing completes.
    ///
    /// The processor contains the final state after processing this partition.
    pub fn finish(self) -> P {
        self.phase.into_processor()
    }
}

impl<P: LogReplayProcessor> Iterator for ExecutorV2<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.phase.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests will be added with test fixtures
}

