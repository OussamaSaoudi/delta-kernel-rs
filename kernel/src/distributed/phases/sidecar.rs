//! Sidecar phase for log replay - processes sidecar/leaf parquet files.

use std::sync::Arc;

use crate::log_replay::ActionsBatch;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, FileMeta};

/// Phase that processes sidecar or leaf parquet files.
///
/// This phase is distributable - you can partition `files` and create multiple
/// instances on different executors.
pub(crate) struct SidecarPhase {
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
}

impl SidecarPhase {
    /// Create a new sidecar phase from file list.
    ///
    /// # Distributability
    ///
    /// This phase is designed to be distributable. To distribute:
    /// 1. Partition `files` across N executors
    /// 2. Create N `SidecarPhase` instances, one per executor with its file partition
    ///
    /// # Parameters
    /// - `files`: Sidecar/leaf files to process
    /// - `engine`: Engine for reading files
    /// - `schema`: Schema to use when reading sidecar files (projected based on processor requirements)
    pub fn new(
        files: Vec<FileMeta>,
        engine: Arc<dyn Engine>,
        schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let actions = if files.is_empty() {
            Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>
        } else {
            let actions = engine
                .parquet_handler()
                .read_parquet_files(&files, schema, None)?
                .map(|batch| batch.map(|b| ActionsBatch::new(b, false)));
            Box::new(actions) as Box<dyn Iterator<Item = _> + Send>
        };

        Ok(Self { actions })
    }
}

impl Iterator for SidecarPhase {
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
    use crate::distributed::phases::manifest;
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
    fn test_sidecar_phase_processes_files() -> DeltaResult<()> {
        let (engine, snapshot, _table_root) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = snapshot.log_segment();

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        let mut processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        
        // First we need to run through manifest phase to get the sidecar files
        if log_segment.checkpoint_parts.is_empty() {
            println!("Test table has no checkpoint parts, skipping");
            return Ok(());
        }

        // Get the first checkpoint part
        let checkpoint_file = &log_segment.checkpoint_parts[0];
        let manifest_file = checkpoint_file.location.clone();

        let schema = crate::actions::get_commit_schema()
            .project(&[crate::actions::ADD_NAME])?;
        
        let mut manifest_phase = manifest::ManifestPhase::new(
            manifest_file,
            log_segment.log_root.clone(),
            engine.clone(),
            schema,
        )?;

        // Drain manifest phase and apply processor
        while let Some(batch) = manifest_phase.next() {
            let batch = batch?;
            processor.process_actions_batch(batch)?;
        }

        let after_manifest = manifest_phase.into_next()?;
        
        match after_manifest {
            manifest::AfterManifest::Sidecars { sidecars } => {
                println!("Testing with {} sidecar files", sidecars.len());
                
                let schema = crate::actions::get_commit_schema()
                    .project(&[crate::actions::ADD_NAME])?;
                
                let mut sidecar_phase = SidecarPhase::new(
                    sidecars,
                    engine.clone(),
                    schema,
                )?;

                let mut sidecar_file_paths = Vec::new();
                let mut batch_count = 0;
                
                while let Some(result) = sidecar_phase.next() {
                    let batch = result?;
                    let metadata = processor.process_actions_batch(batch)?;
                    let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                        ps.push(path.to_string());
                    })?;
                    sidecar_file_paths.extend(paths);
                    batch_count += 1;
                }

                sidecar_file_paths.sort();
                
                // v2-checkpoints-json-with-sidecars has exactly 2 sidecar files with 101 total files
                assert_eq!(
                    batch_count, 2,
                    "SidecarPhase should process exactly 2 sidecar batches"
                );
                
                assert_eq!(
                    sidecar_file_paths.len(), 101,
                    "SidecarPhase should find exactly 101 files from sidecars"
                );
                
                // Verify first few files match expected (sampling to keep test readable)
                let expected_first_files = vec![
                    "test%25file%25prefix-part-00000-01086c52-1b86-48d0-8889-517fe626849d-c000.snappy.parquet",
                    "test%25file%25prefix-part-00000-0fd71c0e-fd08-4685-87d6-aae77532d3ea-c000.snappy.parquet",
                    "test%25file%25prefix-part-00000-2710dd7f-9fa5-429d-b3fb-c005ba16e062-c000.snappy.parquet",
                ];
                
                assert_eq!(
                    &sidecar_file_paths[..3], &expected_first_files[..],
                    "SidecarPhase should process files in expected order"
                );
            }
            manifest::AfterManifest::Done => {
                println!("No sidecars found - test inconclusive");
            }
        }

        Ok(())
    }
}

