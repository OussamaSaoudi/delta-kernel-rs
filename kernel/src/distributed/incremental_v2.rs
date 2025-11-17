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
    fn test_incremental_v2_with_commits() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        
        // Simulate incremental update: process new commits with empty cached data
        let new_commit_files = snapshot.log_segment()
            .ascending_commit_files
            .iter()
            .map(|parsed| parsed.location.clone())
            .collect();
        
        // Empty cached data iterator
        let cached_data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::empty());

        let incremental = IncrementalUpdateV2::new(
            processor,
            new_commit_files,
            cached_data,
            engine.clone(),
        )?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();
        
        for result in incremental {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // table-without-dv-small has exactly 1 commit
        assert_eq!(batch_count, 1, "IncrementalUpdateV2 should process exactly 1 batch");
        
        file_paths.sort();
        let expected_files = vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        assert_eq!(
            file_paths, expected_files,
            "IncrementalUpdateV2 should find exactly the expected file"
        );

        Ok(())
    }
}

