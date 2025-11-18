//! Single-node log replay composition using phase-based architecture.
//!
//! This module provides a single-node execution mode that chains all phases:
//! Commit → Manifest/LeafManifest → Sidecar (if needed)

use std::sync::Arc;

use crate::actions::get_commit_schema;
use crate::distributed::phases::{AfterCommit, AfterManifest, CommitPhase, ManifestPhase, NextPhase, SidecarPhase};
use crate::log_replay::{ActionsBatch, LogReplayProcessor, LogReplayReducer, LogReplaySchemaProvider};
use crate::log_segment::LogSegment;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine};

/// Single-node log replay that processes all phases locally.
///
/// This supports both streaming (via `Iterator`) and folding (via `into_accumulated()`)
/// operations depending on which traits the processor implements:
///
/// - **Streaming**: When `P: LogReplayProcessor`, use as an `Iterator`
/// - **Folding**: When `P: LogReplayReducer`, call `into_accumulated()`
/// - **Hybrid**: When `P` implements both, use both APIs
///
/// Chains these phases:
/// 1. CommitPhase - processes JSON commit files
/// 2. ManifestPhase - processes single-part checkpoint manifest (if any)
/// 3. SidecarPhase - processes sidecar files or multi-part checkpoint parts
///
/// # Examples
///
/// **Streaming (Scan):**
/// ```ignore
/// let single_node = SingleNodeV2::new(scan_processor, log_segment, engine)?;
/// for metadata in single_node {
///     process(metadata?);
/// }
/// ```
///
/// **Folding (P&M):**
/// ```ignore
/// let single_node = SingleNodeV2::new(pm_processor, log_segment, engine)?;
/// let (protocol, metadata) = single_node.into_accumulated()?;
/// ```
pub(crate) struct SingleNodeV2<P> {
    processor: P,
    state: Option<SingleNodeState>,
    log_segment: Arc<LogSegment>,
    engine: Arc<dyn Engine>,
    /// Schema for reading checkpoint/sidecar files
    schema: SchemaRef,
    /// Pre-initialized next phase for concurrent IO (based on phase hint)
    next_stage_hint: Option<SingleNodeState>,
}

enum SingleNodeState {
    Commit(CommitPhase),
    Manifest(ManifestPhase),
    Sidecar(SidecarPhase),
    Done,
}

impl SingleNodeState {
    /// Convert AfterCommit hint into appropriate SingleNodeState
    fn from_after_commit(
        after_commit: AfterCommit,
        engine: Arc<dyn Engine>,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        match after_commit {
            AfterCommit::Manifest { manifest_file, log_root } => {
                Ok(SingleNodeState::Manifest(ManifestPhase::new(
                    manifest_file,
                    log_root,
                    engine,
                    schema,
                )?))
            }
            AfterCommit::LeafManifest { leaf_files } => {
                Ok(SingleNodeState::Sidecar(SidecarPhase::new(
                    leaf_files,
                    engine,
                    schema,
                )?))
            }
            AfterCommit::Done => Ok(SingleNodeState::Done),
        }
    }

    /// Convert AfterManifest into appropriate SingleNodeState
    fn from_after_manifest(
        after_manifest: AfterManifest,
        engine: Arc<dyn Engine>,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        match after_manifest {
            AfterManifest::Sidecars { sidecars } => {
                Ok(SingleNodeState::Sidecar(SidecarPhase::new(
                    sidecars,
                    engine,
                    schema,
                )?))
            }
            AfterManifest::Done => Ok(SingleNodeState::Done),
        }
    }
}

// Unified constructor for both streaming and folding
impl<P: LogReplaySchemaProvider> SingleNodeV2<P> {
    /// Create a new single-node log replay.
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
        let commit_schema = get_commit_schema().project(processor.required_commit_columns())?;
        let checkpoint_schema = get_commit_schema().project(processor.required_checkpoint_columns())?;
        
        let mut commit = CommitPhase::new(&log_segment, engine.clone(), commit_schema)?;
        
        // Use next_phase_hint to enable concurrent IO for all checkpoint types
        let next_stage_hint = Some(SingleNodeState::from_after_commit(
            commit.next_phase_hint(&log_segment)?,
            engine.clone(),
            checkpoint_schema.clone(),
        )?);
        
        Ok(Self {
            processor,
            state: Some(SingleNodeState::Commit(commit)),
            log_segment,
            engine,
            schema: checkpoint_schema,
            next_stage_hint,
        })
    }
}

// Core shared implementation (no trait bounds)
impl<P> SingleNodeV2<P> {

    /// Get next batch from current phase, handling phase transitions.
    /// Returns None when all phases are complete.
    fn next_batch(&mut self) -> Option<DeltaResult<ActionsBatch>> {
        loop {
            // Try to get batch from current phase
            let batch_result = match self.state.as_mut()? {
                SingleNodeState::Commit(phase) => phase.next(),
                SingleNodeState::Manifest(phase) => phase.next(),
                SingleNodeState::Sidecar(phase) => phase.next(),
                SingleNodeState::Done => return None,
            };
            
            match batch_result {
                Some(result) => return Some(result),  // Got a batch (Ok or Err)
                None => {
                    // Current phase exhausted, transition to next
                    let old_state = match self.state.take() {
                        Some(state) => state,
                        None => return None,
                    };
                    
                    match self.transition(old_state) {
                        Ok(new_state) => {
                            self.state = Some(new_state);
                            continue;  // Try next phase
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }

    fn transition(&mut self, state: SingleNodeState) -> DeltaResult<SingleNodeState> {
        let result = match state {
            SingleNodeState::Commit(phase) => {
                match phase.into_next(&self.log_segment)? {
                    NextPhase::UsePreinitialized => {
                        // Take the pre-initialized hint
                        self.next_stage_hint.take()
                            .ok_or_else(|| crate::Error::generic("UsePreinitialized but no hint available"))?
                    }
                    NextPhase::Computed(after_commit) => {
                        // Convert the computed AfterCommit
                        SingleNodeState::from_after_commit(after_commit, self.engine.clone(), self.schema.clone())?
                    }
                }
            }

            SingleNodeState::Manifest(phase) => {
                SingleNodeState::from_after_manifest(phase.into_next()?, self.engine.clone(), self.schema.clone())?
            }

            SingleNodeState::Sidecar(_) | SingleNodeState::Done => SingleNodeState::Done,
        };

        Ok(result)
    }
}

// ============================================================================
// Streaming API: available when P implements LogReplayProcessor
// ============================================================================
impl<P: LogReplayProcessor> Iterator for SingleNodeV2<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        // Shared: get next batch from phases
        let batch_result = self.next_batch()?;
        
        // Streaming-specific: process batch (pure map - always produces output)
        Some(batch_result.and_then(|batch| self.processor.process_actions_batch(batch)))
    }
}

// ============================================================================
// Fold/Reduce API: available when P implements LogReplayReducer
// ============================================================================

impl<P: LogReplayReducer> SingleNodeV2<P> {
    /// Process all batches through the reducer.
    ///
    /// This method processes batches from all phases (Commit → Manifest → Sidecar)
    /// until either:
    /// - All phases are exhausted, OR
    /// - [`LogReplayReducer::is_complete`] returns true (early termination)
    ///
    /// After calling this, use [`into_accumulated`](Self::into_accumulated) to extract the result.
    pub fn run_to_completion(&mut self) -> DeltaResult<()> {
        loop {
            // Check for early termination
            if self.processor.is_complete() {
                break;
            }
            
            // Shared: get next batch from phases
            match self.next_batch() {
                Some(Ok(batch)) => {
                    // Fold-specific: accumulate in reducer (may trigger early completion)
                    self.processor.process_batch_for_reduce(batch)?;
                }
                Some(Err(e)) => return Err(e),
                None => break,  // All phases exhausted
            }
        }
        
        Ok(())
    }

    /// Extract the accumulated result after processing completes.
    ///
    /// Must be called after [`run_to_completion`](Self::run_to_completion).
    ///
    /// # Example
    /// ```ignore
    /// let mut pm = SingleNodeV2::new(pm_processor, log_segment, engine)?;
    /// pm.run_to_completion()?;
    /// let (protocol, metadata) = pm.into_accumulated()?;
    /// ```
    pub fn into_accumulated(self) -> DeltaResult<P::Accumulated> {
        self.processor.into_accumulated()
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
    fn test_single_node_v2_with_commits_only() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let single_node = SingleNodeV2::new(processor, log_segment, engine.clone())?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();
        
        for result in single_node {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // table-without-dv-small has exactly 1 commit
        assert_eq!(batch_count, 1, "SingleNodeV2 should process exactly 1 batch for table-without-dv-small");
        
        file_paths.sort();
        let expected_files = vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "SingleNodeV2 should find exactly the expected file"
        );

        Ok(())
    }

    #[test]
    fn test_single_node_v2_with_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let single_node = SingleNodeV2::new(processor, log_segment, engine.clone())?;

        let mut file_paths = Vec::new();
        
        for result in single_node {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            file_paths.extend(paths);
        }

        file_paths.sort();
        
        // v2-checkpoints-json-with-sidecars has exactly 101 files total
        assert_eq!(
            file_paths.len(), 101,
            "SingleNodeV2 should process exactly 101 files for v2-checkpoints-json-with-sidecars"
        );
        
        // Verify first few files match expected
        let expected_first_files = vec![
            "test%25file%25prefix-part-00000-01086c52-1b86-48d0-8889-517fe626849d-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-0fd71c0e-fd08-4685-87d6-aae77532d3ea-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-2710dd7f-9fa5-429d-b3fb-c005ba16e062-c000.snappy.parquet",
        ];
        
        assert_eq!(
            &file_paths[..3], &expected_first_files[..],
            "SingleNodeV2 should process files in expected order"
        );

        Ok(())
    }
}

