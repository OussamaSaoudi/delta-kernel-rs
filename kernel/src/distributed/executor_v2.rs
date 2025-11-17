//! Executor (Phase 2) log replay composition for distributed execution.
//!
//! This module provides executor-side execution that processes a partition of
//! sidecar or multi-part checkpoint files.

use std::sync::Arc;

use crate::distributed::phases::SidecarPhase;
use crate::log_replay::LogReplayProcessor;
use crate::{DeltaResult, Engine, FileMeta};

/// Executor-side log replay (Phase 2) for distributed execution.
///
/// This iterator processes a partition of sidecar or multi-part checkpoint files
/// that were distributed from the driver.
///
/// # Example
///
/// ```ignore
/// // On executor: receive from driver
/// let (serialized_processor, file_partition) = receive_from_driver()?;
///
/// // Deserialize processor
/// let processor = ScanLogReplayProcessor::deserialize(&serialized_processor, engine)?;
///
/// // Create executor with partition
/// let mut executor = ExecutorV2::new(processor, file_partition, engine)?;
///
/// // Process partition
/// for batch in executor {
///     let metadata = batch?;
///     // Process metadata
/// }
///
/// // Extract final processor state
/// let final_processor = executor.finish();
/// ```
pub(crate) struct ExecutorV2<P: LogReplayProcessor> {
    phase: SidecarPhase<P>,
}

impl<P: LogReplayProcessor> ExecutorV2<P> {
    /// Create a new executor-side log replay iterator.
    ///
    /// # Parameters
    /// - `processor`: The deserialized log replay processor from the driver
    /// - `files`: Partition of sidecar/leaf files to process on this executor
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        files: Vec<FileMeta>,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let phase = SidecarPhase::new(processor, files, engine)?;
        Ok(Self { phase })
    }

    /// Extract the processor after processing completes.
    ///
    /// The processor contains the final state after processing this partition.
    pub fn finish(self) -> P {
        self.phase.into_processor()
    }
}

impl<P: LogReplayProcessor> Iterator for ExecutorV2<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.phase.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::driver_v2::DriverV2;
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
    fn test_executor_v2_processes_sidecars() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = StdArc::new(snapshot.log_segment().clone());

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        // First run driver to get processor and files
        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let mut driver = DriverV2::new(processor, log_segment, engine.clone())?;

        // Drain driver
        while driver.next().is_some() {}

        // Get executor state
        let result = driver.finish()?;
        assert!(result.is_some(), "Should have executor phase");
        
        let (processor, files) = result.unwrap();
        assert_eq!(files.len(), 2, "Should have 2 sidecar files");

        // Now run executor
        let mut executor = ExecutorV2::new(processor, files, engine.clone())?;

        let mut executor_file_paths = Vec::new();
        
        while let Some(result) = executor.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            executor_file_paths.extend(paths);
        }

        executor_file_paths.sort();
        
        // v2-checkpoints-json-with-sidecars has exactly 101 files in sidecars
        assert_eq!(
            executor_file_paths.len(), 101,
            "ExecutorV2 should process exactly 101 files from sidecars"
        );
        
        // Verify first few files match expected
        let expected_first_files = vec![
            "test%25file%25prefix-part-00000-01086c52-1b86-48d0-8889-517fe626849d-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-0fd71c0e-fd08-4685-87d6-aae77532d3ea-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-2710dd7f-9fa5-429d-b3fb-c005ba16e062-c000.snappy.parquet",
        ];
        
        assert_eq!(
            &executor_file_paths[..3], &expected_first_files[..],
            "ExecutorV2 should process files in expected order"
        );

        Ok(())
    }
}

