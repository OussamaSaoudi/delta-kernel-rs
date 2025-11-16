//! Single-node log replay composition using phase-based architecture.
//!
//! This module provides a single-node execution mode that chains all phases:
//! Commit → Manifest/LeafManifest → Sidecar (if needed)

use std::sync::Arc;

use crate::distributed::phases::{AfterCommit, AfterManifest, CommitPhase, ManifestPhase, SidecarPhase};
use crate::log_replay::LogReplayProcessor;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine};

/// Single-node log replay that processes all phases locally.
///
/// This iterator chains:
/// 1. CommitPhase - processes JSON commit files
/// 2. ManifestPhase - processes single-part checkpoint manifest (if any)
/// 3. SidecarPhase - processes sidecar files or multi-part checkpoint parts
///
/// # Example
///
/// ```ignore
/// let single_node = SingleNodeV2::new(processor, log_segment, engine)?;
/// 
/// for batch in single_node {
///     let metadata = batch?;
///     // Process batch
/// }
/// ```
pub(crate) struct SingleNodeV2<P: LogReplayProcessor> {
    state: Option<SingleNodeState<P>>,
    log_segment: Arc<LogSegment>,
    engine: Arc<dyn Engine>,
}

enum SingleNodeState<P: LogReplayProcessor> {
    Commit(CommitPhase<P>),
    Manifest(ManifestPhase<P>),
    Sidecar(SidecarPhase<P>),
    Done,
}

impl<P: LogReplayProcessor> SingleNodeV2<P> {
    /// Create a new single-node log replay iterator.
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
            state: Some(SingleNodeState::Commit(commit)),
            log_segment,
            engine,
        })
    }
}

impl<P: LogReplayProcessor> Iterator for SingleNodeV2<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get item from current phase
            match self.state.as_mut()? {
                SingleNodeState::Commit(phase) => {
                    if let Some(item) = phase.next() {
                        return Some(item);
                    }
                }
                SingleNodeState::Manifest(phase) => {
                    if let Some(item) = phase.next() {
                        return Some(item);
                    }
                }
                SingleNodeState::Sidecar(phase) => return phase.next(),
                SingleNodeState::Done => return None,
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

impl<P: LogReplayProcessor> SingleNodeV2<P> {
    fn transition(&self, state: SingleNodeState<P>) -> DeltaResult<SingleNodeState<P>> {
        let result = match state {
            SingleNodeState::Commit(phase) => {
                match phase.into_next(&self.log_segment)? {
                    AfterCommit::Manifest {
                        processor,
                        manifest_file,
                        log_root,
                    } => SingleNodeState::Manifest(ManifestPhase::new(
                        processor,
                        manifest_file,
                        log_root,
                        self.engine.clone(),
                    )?),
                    AfterCommit::LeafManifest {
                        processor,
                        leaf_files,
                    } => SingleNodeState::Sidecar(SidecarPhase::new(
                        processor,
                        leaf_files,
                        self.engine.clone(),
                    )?),
                    AfterCommit::Done(_) => SingleNodeState::Done,
                }
            }

            SingleNodeState::Manifest(phase) => match phase.into_next()? {
                AfterManifest::Sidecars {
                    processor,
                    sidecars,
                } => SingleNodeState::Sidecar(SidecarPhase::new(
                    processor,
                    sidecars,
                    self.engine.clone(),
                )?),
                AfterManifest::Done(_) => SingleNodeState::Done,
            },

            SingleNodeState::Sidecar(_) | SingleNodeState::Done => SingleNodeState::Done,
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests will be added with test fixtures
}

