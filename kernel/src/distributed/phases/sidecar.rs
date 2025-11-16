//! Sidecar phase for log replay - processes sidecar/leaf parquet files.

use std::sync::Arc;

use crate::log_replay::{ActionsBatch, LogReplayProcessor};
use crate::scan::CHECKPOINT_READ_SCHEMA;
use crate::{DeltaResult, Engine, FileMeta};

/// Phase that processes sidecar or leaf parquet files.
///
/// This phase is distributable - you can partition `files` and create multiple
/// instances on different executors.
pub(crate) struct SidecarPhase<P: LogReplayProcessor> {
    processor: P,
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

impl<P: LogReplayProcessor> SidecarPhase<P> {
    /// Create a new sidecar phase from processor + file list.
    ///
    /// Processes parquet files using `CHECKPOINT_READ_SCHEMA`.
    ///
    /// # Distributability
    ///
    /// This phase is designed to be distributable. To distribute:
    /// 1. Partition `files` across N executors
    /// 2. Serialize the processor state
    /// 3. Create N `SidecarPhase` instances, one per executor with its file partition
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `files`: Sidecar/leaf files to process
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        files: Vec<FileMeta>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let actions = if files.is_empty() {
            Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>
        } else {
            let actions = engine
                .parquet_handler()
                .read_parquet_files(&files, CHECKPOINT_READ_SCHEMA.clone(), None)?
                .map(|batch| batch.map(|b| ActionsBatch::new(b, false)));
            Box::new(actions) as Box<dyn Iterator<Item = _> + Send>
        };

        Ok(Self { processor, actions })
    }

    /// Extract the processor after processing completes.
    pub fn into_processor(self) -> P {
        self.processor
    }
}

impl<P: LogReplayProcessor> Iterator for SidecarPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.actions
            .next()
            .map(|batch| batch.and_then(|b| self.processor.process_actions_batch(b)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests will be added with test fixtures
}

