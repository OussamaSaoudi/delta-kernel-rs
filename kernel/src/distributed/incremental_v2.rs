//! Incremental update composition for snapshot updates.
//!
//! This module provides execution mode for incremental snapshot updates where
//! new commits are processed before cached checkpoint metadata.

use std::sync::Arc;

use crate::distributed::phases::CommitPhase;
use crate::log_replay::LogReplayProcessor;
use crate::{DeltaResult, Engine, EngineData, FileMeta};

/// Incremental update log replay for snapshot updates.
///
/// Processes new commits first (to update RemoveSet), then replays cached
/// checkpoint metadata with the updated deduplication state.
///
/// # Use Case
///
/// When a snapshot at version X exists with cached checkpoint metadata, and new
/// commits X+1, X+2 arrive:
/// 1. Process commits X+1, X+2 (updates RemoveSet with new removes)
/// 2. Replay cached checkpoint metadata from version X (deduplicated against updated RemoveSet)
///
/// This avoids re-reading the checkpoint file.
///
/// # Example
///
/// ```ignore
/// let incremental = IncrementalUpdateV2::new(
///     processor,
///     new_commit_files,  // X+1, X+2
///     cached_checkpoint_data,  // From version X
///     engine,
/// )?;
///
/// for batch in incremental {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// let final_processor = incremental.finish();
/// ```
pub(crate) struct IncrementalUpdateV2<P: LogReplayProcessor> {
    phase: CommitPhase<P>,
}

impl<P: LogReplayProcessor> IncrementalUpdateV2<P> {
    /// Create a new incremental update iterator.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `new_commit_files`: New commit files to process
    /// - `cached_metadata`: Pre-loaded checkpoint metadata iterator
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        new_commit_files: Vec<FileMeta>,
        cached_metadata: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let phase = CommitPhase::with_cached(processor, new_commit_files, cached_metadata, engine)?;
        Ok(Self { phase })
    }

    /// Extract the processor after processing completes.
    pub fn finish(self) -> P {
        self.phase.into_processor()
    }
}

impl<P: LogReplayProcessor> Iterator for IncrementalUpdateV2<P> {
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

