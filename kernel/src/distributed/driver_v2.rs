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
    processor: P,
    state: Option<DriverState>,
    log_segment: Arc<LogSegment>,
    engine: Arc<dyn Engine>,
    /// Pre-initialized next phase for concurrent IO (based on phase hint)
    next_stage_hint: Option<DriverState>,
}

enum DriverState {
    Commit(CommitPhase),
    Manifest(ManifestPhase),
    /// Executor phase needed - has files to distribute
    ExecutorPhase { files: Vec<FileMeta> },
    /// Done - no more work needed
    Done,
}

impl DriverState {
    /// Convert AfterCommit hint into appropriate DriverState
    fn from_after_commit(
        after_commit: AfterCommit,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        match after_commit {
            AfterCommit::Manifest { manifest_file, log_root } => {
                Ok(DriverState::Manifest(ManifestPhase::new(
                    manifest_file,
                    log_root,
                    engine,
                )?))
            }
            AfterCommit::LeafManifest { leaf_files } => {
                Ok(DriverState::ExecutorPhase { files: leaf_files })
            }
            AfterCommit::Done => Ok(DriverState::Done),
        }
    }

    /// Convert AfterManifest into appropriate DriverState
    fn from_after_manifest(after_manifest: AfterManifest) -> Self {
        match after_manifest {
            AfterManifest::Sidecars { sidecars } => {
                DriverState::ExecutorPhase { files: sidecars }
            }
            AfterManifest::Done => DriverState::Done,
        }
    }
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
        let commit = CommitPhase::new(&log_segment, engine.clone())?;
        
        // Use next_phase_hint to enable concurrent IO for all checkpoint types
        let next_stage_hint = Some(DriverState::from_after_commit(
            commit.next_phase_hint(&log_segment)?,
            engine.clone(),
        )?);
        
        Ok(Self {
            processor,
            state: Some(DriverState::Commit(commit)),
            log_segment,
            engine,
            next_stage_hint,
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
            Some(DriverState::ExecutorPhase { files }) => Ok(Some((self.processor, files))),
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
            let batch_result = match self.state.as_mut()? {
                DriverState::Commit(phase) => phase.next(),
                DriverState::Manifest(phase) => phase.next(),
                DriverState::ExecutorPhase { .. } | DriverState::Done => return None,
            };

            match batch_result {
                Some(Ok(batch)) => {
                    // Process the batch through the processor
                    return Some(self.processor.process_actions_batch(batch));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    // Phase exhausted - transition
                    let old_state = self.state.take()?;
                    match self.transition(old_state) {
                        Ok(new_state) => self.state = Some(new_state),
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }
}

impl<P: LogReplayProcessor> DriverV2<P> {
    fn transition(&mut self, state: DriverState) -> DeltaResult<DriverState> {
        // Try using pre-initialized hint first
        if let Some(hint) = self.next_stage_hint.take() {
            return Ok(hint);
        }

        // Otherwise, compute transition from current state
        let result = match state {
            DriverState::Commit(phase) => {
                DriverState::from_after_commit(
                    phase.into_next(&self.log_segment)?,
                    self.engine.clone(),
                )?
            }

            DriverState::Manifest(phase) => {
                DriverState::from_after_manifest(phase.into_next()?)
            }

            DriverState::ExecutorPhase { files } => {
                // Already in final state
                DriverState::ExecutorPhase { files }
            }
            DriverState::Done => DriverState::Done,
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::state_info::StateInfo;
    use std::path::PathBuf;
    use std::sync::Arc as StdArc;
    use object_store::local::LocalFileSystem;
    use crate::engine::default::DefaultEngine;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;

    fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(
        StdArc<DefaultEngine<TokioBackgroundExecutor>>,
        StdArc<crate::Snapshot>,
        url::Url,
    )> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(table_name);
        
        let path = std::fs::canonicalize(path)
            .map_err(|e| crate::Error::Generic(format!("Failed to canonicalize path: {}", e)))?;
        
        let url = url::Url::from_directory_path(path)
            .map_err(|_| crate::Error::Generic("Failed to create URL from path".to_string()))?;
        
        let store = StdArc::new(LocalFileSystem::new());
        let engine = StdArc::new(DefaultEngine::new(store));
        let snapshot = crate::Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
        
        Ok((engine, snapshot, url))
    }

    #[test]
    fn test_driver_v2_with_commits_only() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverV2::new(processor, log_segment, engine.clone())?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();
        
        while let Some(result) = driver.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // table-without-dv-small has exactly 1 commit
        assert_eq!(batch_count, 1, "DriverV2 should process exactly 1 batch for table-without-dv-small");
        
        file_paths.sort();
        let expected_files = vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "DriverV2 should find exactly the expected file"
        );

        // No executor phase needed for commits-only table
        let result = driver.finish()?;
        assert!(result.is_none(), "DriverV2 should return None for commits-only table (no executor phase needed)");

        Ok(())
    }

    #[test]
    fn test_driver_v2_with_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverV2::new(processor, log_segment, engine.clone())?;

        let mut driver_batch_count = 0;
        let mut driver_file_paths = Vec::new();
        
        while let Some(result) = driver.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            driver_file_paths.extend(paths);
            driver_batch_count += 1;
        }

        // Driver processes commits after checkpoint (v7-v12) in batches, then manifest
        // For v2-checkpoints-json-with-sidecars: checkpoint at v6, commits 7-12 exist
        // The commits 7-12 contain no new add actions (only removes/metadata updates)
        // So driver produces batches from commits, but those batches contain 0 files
        // Note: A single batch may contain multiple commits
        assert!(driver_batch_count >= 1, "DriverV2 should process at least 1 batch");
        
        // The driver should process 0 files (all adds are in the checkpoint sidecars, commits after checkpoint have no new adds)
        driver_file_paths.sort();
        assert_eq!(
            driver_file_paths.len(), 0,
            "DriverV2 should find 0 files (all adds are in checkpoint sidecars, commits 7-12 have no new add actions)"
        );

        // Should have executor phase with sidecars from the checkpoint
        let result = driver.finish()?;
        assert!(result.is_some(), "DriverV2 should return Some for table with sidecars (executor phase needed)");
        
        let (_processor, files) = result.unwrap();
        assert_eq!(files.len(), 2, "DriverV2 should collect exactly 2 sidecar files from checkpoint for distribution");

        Ok(())
    }
}

