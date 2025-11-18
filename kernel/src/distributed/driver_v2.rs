//! Driver (Phase 1) log replay composition for distributed execution.
//!
//! This module provides driver-side execution that processes commits and manifest,
//! then returns processor/reducer + files for distribution to executors.
//!
//! Supports both streaming (via `LogReplayProcessor`) and folding (via `LogReplayReducer`) operations.

use std::sync::Arc;

use crate::actions::get_commit_schema;
use crate::distributed::phases::{AfterCommit, AfterManifest, CommitPhase, ManifestPhase, NextPhase};
use crate::log_replay::{LogReplayProcessor, LogReplayReducer, LogReplaySchemaProvider};
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
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
pub(crate) struct DriverV2<P> {
    processor: P,
    state: Option<DriverState>,
    log_segment: Arc<LogSegment>,
    engine: Arc<dyn Engine>,
    /// Schema for reading checkpoint/sidecar files (executor phase)
    checkpoint_schema: SchemaRef,
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
        checkpoint_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        match after_commit {
            AfterCommit::Manifest { manifest_file, log_root } => {
                Ok(DriverState::Manifest(ManifestPhase::new(
                    manifest_file,
                    log_root,
                    engine,
                    checkpoint_schema,
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

// Unified constructor for both streaming and folding
impl<P: LogReplaySchemaProvider> DriverV2<P> {
    /// Create a new driver-side log replay.
    ///
    /// Works for both streaming (`LogReplayProcessor`) and folding (`LogReplayReducer`) operations.
    /// Extracts schema requirements from the processor/reducer and passes them to phases.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor or reducer
    /// - `log_segment`: The log segment to process
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        log_segment: Arc<LogSegment>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        // Extract schemas from processor/reducer
        let commit_schema = get_commit_schema().project(processor.required_commit_columns())?;
        let checkpoint_schema = get_commit_schema().project(processor.required_checkpoint_columns())?;
        
        let mut commit = CommitPhase::new(&log_segment, engine.clone(), commit_schema)?;
        
        // Use next_phase_hint to enable concurrent IO for all checkpoint types
        let next_stage_hint = Some(DriverState::from_after_commit(
            commit.next_phase_hint(&log_segment)?,
            engine.clone(),
            checkpoint_schema.clone(),
        )?);
        
        Ok(Self {
            processor,
            state: Some(DriverState::Commit(commit)),
            log_segment,
            engine,
            checkpoint_schema,
            next_stage_hint,
        })
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

// Shared implementation (no trait bounds - used by both processor and reducer)
impl<P> DriverV2<P> {
    fn transition(&mut self, state: DriverState) -> DeltaResult<DriverState> {
        let result = match state {
            DriverState::Commit(phase) => {
                match phase.into_next(&self.log_segment)? {
                    NextPhase::UsePreinitialized => {
                        // Take the pre-initialized hint
                        self.next_stage_hint.take()
                            .ok_or_else(|| Error::generic("UsePreinitialized but no hint available"))?
                    }
                    NextPhase::Computed(after_commit) => {
                        // Convert the computed AfterCommit
                        DriverState::from_after_commit(
                            after_commit,
                            self.engine.clone(),
                            self.checkpoint_schema.clone(),
                        )?
                    }
                }
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

// ============================================================================
// Streaming API: available when P implements LogReplayProcessor
// ============================================================================

impl<P: LogReplayProcessor> DriverV2<P> {
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

// ============================================================================
// Fold/Reduce API: available when P implements LogReplayReducer
// ============================================================================

/// Result of driver-side fold operation.
pub(crate) enum DriverV2ReducerResult<R: LogReplayReducer> {
    /// Fold operation complete on driver - no executor phase needed.
    Complete(R::Accumulated),
    /// Executor phase needed - distribute files to executors for parallel processing.
    NeedsExecutorPhase {
        reducer: R,
        files: Vec<FileMeta>,
    },
}

impl<R: LogReplayReducer> DriverV2<R> {
    /// Process all driver phases (commits + manifest).
    ///
    /// Accumulates state in the reducer until either:
    /// - Early termination (reducer signals completion via `is_complete()`)
    /// - All driver phases exhausted
    ///
    /// After calling this, use [`into_result`](Self::into_result) to get the result or executor files.
    pub fn run_to_completion(&mut self) -> DeltaResult<()> {
        loop {
            // Check if reducer signals early completion
            if self.processor.is_complete() {
                break;
            }

            // Try to get next batch from current phase
            let batch_result = match self.state.as_mut() {
                Some(DriverState::Commit(phase)) => phase.next(),
                Some(DriverState::Manifest(phase)) => phase.next(),
                Some(DriverState::ExecutorPhase { .. }) | Some(DriverState::Done) => {
                    // Reached executor phase or done - stop driver processing
                    break;
                }
                None => break,
            };

            match batch_result {
                Some(Ok(batch)) => {
                    // Process the batch through the reducer
                    self.processor.process_batch_for_reduce(batch)?;
                }
                Some(Err(e)) => return Err(e),
                None => {
                    // Phase exhausted - transition
                    let old_state = self.state.take().ok_or_else(|| {
                        Error::generic("State missing during phase transition")
                    })?;
                    self.state = Some(self.transition(old_state)?);
                }
            }
        }
        
        Ok(())
    }

    /// Extract the result or files for executors after processing completes.
    ///
    /// Must be called after [`run_to_completion`](Self::run_to_completion).
    ///
    /// # Returns
    /// - `Complete`: Fold operation complete - all processing done on driver
    /// - `NeedsExecutorPhase`: Executor phase needed - distribute files to executors
    pub fn into_result(self) -> DeltaResult<DriverV2ReducerResult<R>> {
        // Extract result or executor files
        match self.state {
            Some(DriverState::ExecutorPhase { files }) => {
                Ok(DriverV2ReducerResult::NeedsExecutorPhase {
                    reducer: self.processor,
                    files,
                })
            }
            Some(DriverState::Done) | None => {
                // All done - extract accumulated result
                Ok(DriverV2ReducerResult::Complete(self.processor.into_accumulated()?))
            }
            Some(_) => Err(Error::generic(
                "Must call run_to_completion() before calling into_result()",
            )),
        }
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

    // ========================================================================
    // Reducer tests
    // ========================================================================

    fn test_table_path(name: &str) -> PathBuf {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        PathBuf::from(manifest_dir).join("tests/data").join(name)
    }

    #[test]
    fn test_pm_driver_commits_only() {
        use crate::distributed::pm_processor::PMProcessor;
        
        // Table with P&M in commits only - should complete on driver
        let table_path = test_table_path("table-without-dv-small");
        let table_url = url::Url::from_directory_path(table_path).unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        
        let snapshot = crate::Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .unwrap();
        let log_segment = snapshot.log_segment().clone();
        
        let pm_processor = PMProcessor::new();
        let mut driver = DriverV2::new(pm_processor, Arc::new(log_segment), engine).unwrap();
        
        driver.run_to_completion().unwrap();
        
        match driver.into_result().unwrap() {
            DriverV2ReducerResult::Complete((protocol, metadata)) => {
                // Validate protocol
                assert_eq!(protocol.min_reader_version(), 1);
                
                // Validate metadata
                assert_eq!(metadata.name(), Some("table-without-dv-small"));
            }
            DriverV2ReducerResult::NeedsExecutorPhase { .. } => {
                panic!("Expected Complete, got NeedsExecutorPhase");
            }
        }
    }

    #[test]
    fn test_pm_driver_with_checkpoint() {
        use crate::distributed::pm_processor::PMProcessor;
        
        // Table with checkpoint - P&M should be found in driver phase
        let table_path = test_table_path("with_checkpoint");
        let table_url = url::Url::from_directory_path(table_path).unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        
        let snapshot = crate::Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .unwrap();
        let log_segment = snapshot.log_segment().clone();
        
        let pm_processor = PMProcessor::new();
        let mut driver = DriverV2::new(pm_processor, Arc::new(log_segment), engine).unwrap();
        
        driver.run_to_completion().unwrap();
        
        match driver.into_result().unwrap() {
            DriverV2ReducerResult::Complete((protocol, _metadata)) => {
                assert_eq!(protocol.min_reader_version(), 3);
            }
            DriverV2ReducerResult::NeedsExecutorPhase { .. } => {
                panic!("Expected Complete, got NeedsExecutorPhase for checkpoint table");
            }
        }
    }

    #[test]
    fn test_pm_driver_early_termination() {
        use crate::distributed::pm_processor::PMProcessor;
        
        // Verify that driver stops processing once P&M are found
        let table_path = test_table_path("with_checkpoint");
        let table_url = url::Url::from_directory_path(table_path).unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        
        let snapshot = crate::Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .unwrap();
        let log_segment = snapshot.log_segment().clone();
        
        let pm_processor = PMProcessor::new();
        let driver = DriverV2::new(pm_processor, Arc::new(log_segment), engine).unwrap();
        
        // Process and verify completion (early termination)
        match driver.into_result().unwrap() {
            DriverV2ReducerResult::Complete(_) => {
                // Success - early termination worked
            }
            DriverV2ReducerResult::NeedsExecutorPhase { .. } => {
                panic!("Should have completed on driver with early termination");
            }
        }
    }

    #[test]
    fn test_pm_distributed_with_sidecars() {
        use crate::distributed::pm_processor::PMProcessor;
        use crate::distributed::executor_v2::ExecutorV2;
        
        // Test table with sidecars - requires distributed execution
        // Note: Most real tables have P&M in commits/manifest, not sidecars
        // This test validates the distributed path exists even if rarely used
        
        let table_path = test_table_path("table-with-dv-small");
        let table_url = url::Url::from_directory_path(table_path).unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        
        let snapshot = crate::Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .unwrap();
        let log_segment = snapshot.log_segment().clone();
        
        let pm_processor = PMProcessor::new();
        let driver = DriverV2::new(pm_processor, Arc::new(log_segment), engine.clone()).unwrap();
        
        match driver.into_result().unwrap() {
            DriverV2ReducerResult::Complete((protocol, metadata)) => {
                // P&M found in driver phase - validate
                assert_eq!(protocol.min_reader_version(), 3);
                assert_eq!(metadata.name(), Some("table-with-dv-small"));
            }
            DriverV2ReducerResult::NeedsExecutorPhase { reducer, files } => {
                // P&M not in driver phase - need executor processing
                // This would be rare but demonstrates the distributed path
                assert!(!files.is_empty(), "Should have sidecar files");
                
                // Simulate executor processing
                let mut executor = ExecutorV2::new(reducer, files, engine).unwrap();
                executor.run_to_completion().unwrap();
                let (protocol, metadata) = executor.into_result().unwrap();
                assert_eq!(protocol.min_reader_version(), 3);
                assert_eq!(metadata.name(), Some("table-with-dv-small"));
            }
        }
    }
}

