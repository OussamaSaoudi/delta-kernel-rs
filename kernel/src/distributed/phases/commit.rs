//! Commit phase for log replay - processes JSON commit files.

use std::sync::Arc;

use url::Url;

use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
use crate::scan::COMMIT_READ_SCHEMA;
use crate::{DeltaResult, Engine, EngineData, FileMeta};

/// Phase that processes JSON commit files.
pub(crate) struct CommitPhase {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

/// Possible transitions after CommitPhase completes.
pub(crate) enum AfterCommit {
    /// Single-part checkpoint → process manifest file
    Manifest {
        manifest_file: FileMeta,
        log_root: Url,
    },
    /// Multi-part checkpoint → treat as leaf manifest files
    LeafManifest {
        leaf_files: Vec<FileMeta>,
    },
    /// No checkpoint
    Done,
}

impl CommitPhase {
    /// Create a new commit phase from a log segment.
    ///
    /// Processes JSON commit files using `COMMIT_READ_SCHEMA`.
    pub fn new(
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let commit_files = log_segment.find_commit_cover();
        let actions = engine
            .json_handler()
            .read_json_files(&commit_files, COMMIT_READ_SCHEMA.clone(), None)?
            .map(|batch| batch.map(|b| ActionsBatch::new(b, true)));

        Ok(Self {
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
    /// - `commit_files`: New commit files to process
    /// - `cached_data`: Pre-loaded checkpoint data iterator
    /// - `engine`: Engine for reading files
    pub fn with_cached(
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
            actions: Box::new(chained),
        })
    }

    /// Provide a hint about the next phase without consuming self.
    ///
    /// This enables parallel IO by allowing orchestrators to pre-initialize
    /// the next phase before the current phase completes.
    ///
    /// Returns the same enum as `into_next()` but without consuming `self`.
    pub fn next_phase_hint(&self, log_segment: &LogSegment) -> DeltaResult<AfterCommit> {
        let checkpoint_parts = &log_segment.checkpoint_parts;

        match checkpoint_parts.len() {
            0 => Ok(AfterCommit::Done),
            1 => Ok(AfterCommit::Manifest {
                manifest_file: checkpoint_parts[0].location.clone(),
                log_root: log_segment.log_root.clone(),
            }),
            _ => {
                let leaf_files = checkpoint_parts
                    .iter()
                    .map(|p| p.location.clone())
                    .collect();
                Ok(AfterCommit::LeafManifest {
                    leaf_files,
                })
            }
        }
    }

    /// Transition to the next phase based on checkpoint configuration.
    ///
    /// Returns an enum indicating what comes next:
    /// - `Manifest`: Single-part checkpoint file to process
    /// - `LeafManifest`: Multi-part checkpoint files (distributed as sidecars)
    /// - `Done`: No checkpoint
    pub fn into_next(self, log_segment: &LogSegment) -> DeltaResult<AfterCommit> {
        // Delegate to next_phase_hint since logic is identical
        self.next_phase_hint(log_segment)
    }
}

impl Iterator for CommitPhase {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.actions.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_replay::LogReplayProcessor;
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
        
        Ok((engine, snapshot.into(), url))
    }

    #[test]
    fn test_commit_phase_processes_commits() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let mut processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut commit_phase = CommitPhase::new(&log_segment, engine.clone())?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();
        
        while let Some(result) = commit_phase.next() {
            let batch = result?;
            let metadata = processor.process_actions_batch(batch)?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // table-without-dv-small has exactly 1 commit file
        assert_eq!(batch_count, 1, "table-without-dv-small should have exactly 1 commit batch");
        
        // table-without-dv-small has exactly 1 add file
        file_paths.sort();
        let expected_files = vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "CommitPhase should find exactly the expected file"
        );

        Ok(())
    }

    #[test]
    fn test_commit_phase_transition_with_checkpoint() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("with_checkpoint_no_last_checkpoint")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let mut commit_phase = CommitPhase::new(&log_segment, engine.clone())?;

        // Drain the phase
        while commit_phase.next().is_some() {}

        // Transition should indicate checkpoint exists
        let next = commit_phase.into_next(&log_segment)?;
        assert!(
            matches!(next, AfterCommit::Manifest { .. }),
            "Should transition to Manifest when checkpoint exists"
        );

        Ok(())
    }
}

