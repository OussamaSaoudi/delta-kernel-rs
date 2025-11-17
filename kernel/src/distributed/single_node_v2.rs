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

