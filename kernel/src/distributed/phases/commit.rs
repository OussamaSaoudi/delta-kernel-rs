//! Commit phase for log replay - processes JSON commit files.

use std::sync::Arc;

use url::Url;

use crate::log_replay::{ActionsBatch, LogReplayProcessor};
use crate::log_segment::LogSegment;
use crate::scan::COMMIT_READ_SCHEMA;
use crate::{DeltaResult, Engine, EngineData, FileMeta};

/// Phase that processes JSON commit files.
pub(crate) struct CommitPhase<P: LogReplayProcessor> {
    processor: P,
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

/// Possible transitions after CommitPhase completes.
pub(crate) enum AfterCommit<P: LogReplayProcessor> {
    /// Single-part checkpoint → process manifest file
    Manifest {
        processor: P,
        manifest_file: FileMeta,
        log_root: Url,
    },
    /// Multi-part checkpoint → treat as leaf manifest files
    LeafManifest {
        processor: P,
        leaf_files: Vec<FileMeta>,
    },
    /// No checkpoint
    Done(P),
}

impl<P: LogReplayProcessor> CommitPhase<P> {
    /// Create a new commit phase from a log segment.
    ///
    /// Processes JSON commit files using `COMMIT_READ_SCHEMA`.
    pub fn new(
        processor: P,
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit_files = log_segment.find_commit_cover();
        let actions = engine
            .json_handler()
            .read_json_files(&commit_files, COMMIT_READ_SCHEMA.clone(), None)?
            .map(|batch| batch.map(|b| ActionsBatch::new(b, true)));

        Ok(Self {
            processor,
            actions: Box::new(actions),
        })
    }

    /// Create commit phase with cached EngineData chained after commits.
    ///
    /// Use case: incremental update where new commits (x+1, x+2) are processed
    /// first to update the RemoveSet, then cached checkpoint metadata from version x
    /// is replayed with the updated deduplication state.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `commit_files`: New commit files to process
    /// - `cached_data`: Pre-loaded checkpoint data iterator
    /// - `engine`: Engine for reading files
    pub fn with_cached(
        processor: P,
        commit_files: Vec<FileMeta>,
        cached_data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit_actions = engine
            .json_handler()
            .read_json_files(&commit_files, COMMIT_READ_SCHEMA.clone(), None)?
            .map(|batch| batch.map(|b| ActionsBatch::new(b, true)));

        // Cached data is checkpoint data (is_log_batch = false)
        let cached_actions = cached_data.map(|batch| batch.map(|b| ActionsBatch::new(b, false)));

        let chained = commit_actions.chain(cached_actions);

        Ok(Self {
            processor,
            actions: Box::new(chained),
        })
    }

    /// Transition to the next phase based on checkpoint configuration.
    ///
    /// Returns an enum indicating what comes next:
    /// - `Manifest`: Single-part checkpoint file to process
    /// - `LeafManifest`: Multi-part checkpoint files (distributed as sidecars)
    /// - `Done`: No checkpoint
    pub fn into_next(self, log_segment: &LogSegment) -> DeltaResult<AfterCommit<P>> {
        let checkpoint_parts = &log_segment.checkpoint_parts;

        match checkpoint_parts.len() {
            0 => Ok(AfterCommit::Done(self.processor)),
            1 => Ok(AfterCommit::Manifest {
                processor: self.processor,
                manifest_file: checkpoint_parts[0].location.clone(),
                log_root: log_segment.log_root.clone(),
            }),
            _ => {
                let leaf_files = checkpoint_parts
                    .iter()
                    .map(|p| p.location.clone())
                    .collect();
                Ok(AfterCommit::LeafManifest {
                    processor: self.processor,
                    leaf_files,
                })
            }
        }
    }

    /// Extract the processor without transitioning to the next phase.
    ///
    /// Useful for flows that don't need to continue to the next phase
    /// (e.g., incremental updates that only process commits + cached data).
    pub fn into_processor(self) -> P {
        self.processor
    }
}

impl<P: LogReplayProcessor> Iterator for CommitPhase<P> {
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

