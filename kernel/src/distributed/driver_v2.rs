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
/// // Extract processor and files for distribution
/// let (processor, files) = driver.finish()?;
///
/// // Serialize processor and partition files across executors
/// let serialized = processor.serialize()?;
/// let partitions = partition_files(files, num_executors);
/// for (executor, partition) in partitions {
///     executor.send(serialized.clone(), partition)?;
/// }
/// ```
pub(crate) struct DriverV2<P: LogReplayProcessor> {
    state: DriverState<P>,
    log_segment: Arc<LogSegment>,
    engine: Arc<dyn Engine>,
}

enum DriverState<P: LogReplayProcessor> {
    Commit(CommitPhase<P>),
    Manifest(ManifestPhase<P>),
    Done { processor: P, files: Vec<FileMeta> },
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
            state: DriverState::Commit(commit),
            log_segment,
            engine,
        })
    }

    /// Complete driver phase and extract processor + files for distribution.
    ///
    /// Must be called after the iterator is exhausted.
    ///
    /// # Returns
    /// - `processor`: The log replay processor (ready for serialization)
    /// - `files`: Files to distribute to executors (sidecars or multi-part checkpoint parts)
    ///
    /// # Errors
    /// Returns an error if called before iterator exhaustion.
    pub fn finish(self) -> DeltaResult<(P, Vec<FileMeta>)> {
        match self.state {
            DriverState::Done { processor, files } => Ok((processor, files)),
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
            match &mut self.state {
                DriverState::Commit(phase) => {
                    if let Some(item) = phase.next() {
                        return Some(item);
                    }

                    // Phase exhausted, transition to next
                    let DriverState::Commit(phase) = std::mem::replace(
                        &mut self.state,
                        DriverState::Done {
                            processor: unsafe { std::mem::zeroed() }, // Temporary
                            files: vec![],
                        },
                    ) else {
                        unreachable!()
                    };

                    self.state = match phase.into_next(&self.log_segment) {
                        Ok(AfterCommit::Manifest {
                            processor,
                            manifest_file,
                            log_root,
                        }) => {
                            match ManifestPhase::new(
                                processor,
                                manifest_file,
                                log_root,
                                self.engine.clone(),
                            ) {
                                Ok(m) => DriverState::Manifest(m),
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(AfterCommit::LeafManifest {
                            processor,
                            leaf_files,
                        }) => {
                            // Multi-part checkpoint - return as files to distribute
                            DriverState::Done {
                                processor,
                                files: leaf_files,
                            }
                        }
                        Ok(AfterCommit::Done(p)) => DriverState::Done {
                            processor: p,
                            files: vec![],
                        },
                        Err(e) => return Some(Err(e)),
                    };
                }

                DriverState::Manifest(phase) => {
                    if let Some(item) = phase.next() {
                        return Some(item);
                    }

                    let DriverState::Manifest(phase) = std::mem::replace(
                        &mut self.state,
                        DriverState::Done {
                            processor: unsafe { std::mem::zeroed() },
                            files: vec![],
                        },
                    ) else {
                        unreachable!()
                    };

                    self.state = match phase.into_next() {
                        Ok(AfterManifest::Sidecars {
                            processor,
                            sidecars,
                        }) => DriverState::Done {
                            processor,
                            files: sidecars,
                        },
                        Ok(AfterManifest::Done(p)) => DriverState::Done {
                            processor: p,
                            files: vec![],
                        },
                        Err(e) => return Some(Err(e)),
                    };

                    return None;
                }

                DriverState::Done { .. } => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests will be added with test fixtures
}

