//! Driver (Phase 1) log replay composition for distributed execution.
//!
//! This module provides driver-side execution that processes commits and manifest,
//! then returns processor + files for distribution to executors.

use std::sync::Arc;

use crate::distributed::phases::{AfterCommit, AfterManifest, CommitPhase, ManifestPhase};
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine, Error, FileMeta};

/// Driver-side log replay (Phase 1) for distributed execution.
///
/// This iterator processes:
/// 1. CommitPhase - JSON commit files
/// 2. ManifestPhase - single-part checkpoint manifest (if present)
///
/// After exhaustion, call `finish()` to extract:
/// - The processor (for serialization and distribution)
/// - Files to distribute (sidecars or multi-part checkpoint parts)
///
/// # Example
///
/// ```ignore
/// let mut driver = DriverV2::new(processor, log_segment, engine)?;
///
/// // Iterate over driver-side batches
/// for batch in driver {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// // Extract processor and files for distribution (if needed)
/// match driver.finish()? {
///     Some((processor, files)) => {
///         // Executor phase needed - distribute files
///         let serialized = processor.serialize()?;
///         let partitions = partition_files(files, num_executors);
///         for (executor, partition) in partitions {
///             executor.send(serialized.clone(), partition)?;
///         }
///     }
///     None => {
///         // No executor phase needed - all processing complete
///         println!("Log replay complete on driver");
///     }
/// }
/// ```
pub(crate) struct DriverV2<P: LogReplayProcessor> {
    state: Option<DriverState<P>>,
    log_segment: Arc<LogSegment>,
    engine: Arc<dyn Engine>,
}

enum DriverState<P: LogReplayProcessor> {
    Commit(CommitPhase<P>),
    Manifest(ManifestPhase<P>),
    /// Executor phase needed - has files to distribute
    ExecutorPhase { processor: P, files: Vec<FileMeta> },
    /// Done - no more work needed
    Done,
}

impl<P: LogReplayProcessor> DriverV2<P> {
    /// Create a new driver-side log replay iterator.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `log_segment`: The log segment to process
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        log_segment: Arc<LogSegment>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit = CommitPhase::new(processor, &log_segment, engine.clone())?;
        Ok(Self {
            state: Some(DriverState::Commit(commit)),
            log_segment,
            engine,
        })
    }

    /// Complete driver phase and extract processor + files for distribution.
    ///
    /// Must be called after the iterator is exhausted.
    ///
    /// # Returns
    /// - `Some((processor, files))`: Executor phase needed - distribute files to executors
    /// - `None`: No executor phase needed - all processing complete on driver
    ///
    /// # Errors
    /// Returns an error if called before iterator exhaustion.
    pub fn finish(self) -> DeltaResult<Option<(P, Vec<FileMeta>)>> {
        match self.state {
            Some(DriverState::ExecutorPhase { processor, files }) => Ok(Some((processor, files))),
            Some(DriverState::Done) => Ok(None),
            _ => Err(Error::generic(
                "Must exhaust iterator before calling finish()",
            )),
        }
    }
}

impl<P: LogReplayProcessor> Iterator for DriverV2<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get item from current phase
            match self.state.as_mut()? {
                DriverState::Commit(phase) => {
                    if let Some(item) = phase.next() {
                        return Some(item);
                    }
                }
                DriverState::Manifest(phase) => {
                    if let Some(item) = phase.next() {
                        return Some(item);
                    }
                }
                DriverState::ExecutorPhase { .. } | DriverState::Done => return None,
            }

            // Phase exhausted - transition
            let old_state = self.state.take()?;
            match self.transition(old_state) {
                Ok(new_state) => self.state = Some(new_state),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl<P: LogReplayProcessor> DriverV2<P> {
    fn transition(&self, state: DriverState<P>) -> DeltaResult<DriverState<P>> {
        let result = match state {
            DriverState::Commit(phase) => match phase.into_next(&self.log_segment)? {
                AfterCommit::Manifest {
                    processor,
                    manifest_file,
                    log_root,
                } => DriverState::Manifest(ManifestPhase::new(
                    processor,
                    manifest_file,
                    log_root,
                    self.engine.clone(),
                )?),
                AfterCommit::LeafManifest {
                    processor,
                    leaf_files,
                } => {
                    // Multi-part checkpoint - need executor phase
                    DriverState::ExecutorPhase {
                        processor,
                        files: leaf_files,
                    }
                }
                AfterCommit::Done(_) => {
                    // No checkpoint - done
                    DriverState::Done
                }
            },

            DriverState::Manifest(phase) => match phase.into_next()? {
                AfterManifest::Sidecars {
                    processor,
                    sidecars,
                } => {
                    // Has sidecars - need executor phase
                    DriverState::ExecutorPhase {
                        processor,
                        files: sidecars,
                    }
                }
                AfterManifest::Done(_) => {
                    // No sidecars - done
                    DriverState::Done
                }
            },

            DriverState::ExecutorPhase { processor, files } => {
                // Already in final state
                DriverState::ExecutorPhase { processor, files }
            }
            DriverState::Done => DriverState::Done,
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests will be added with test fixtures
}

