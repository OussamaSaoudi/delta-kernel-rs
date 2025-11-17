use std::sync::Arc;

use crate::{
    distributed::distributor::{Distributor, DistributorState},
    log_replay::LogReplayProcessor,
    scan::{log_replay::ScanLogReplayProcessor, state_info::StateInfo},
    utils::require,
    DeltaResult, Engine, Error, FileMeta, SnapshotRef,
};

struct DriverLogReplay {
    engine: Arc<dyn Engine>,
    state: Distributor<ScanLogReplayProcessor>,
}

struct ExecutorLogReplayInfo {
    files: Vec<FileMeta>,
    processor: ScanLogReplayProcessor,
    snapshot: SnapshotRef,
}

struct ExecutorLogReplay {
    state: Distributor<ScanLogReplayProcessor>,
}

impl ExecutorLogReplay {
    fn try_new(engine: Arc<dyn Engine>, info: ExecutorLogReplayInfo) -> DeltaResult<Self> {
        let distributor_state = DistributorState::LeafManifest(info.files);
        let state = Distributor::try_new_from_state(
            engine,
            info.snapshot,
            info.processor,
            distributor_state,
        )?;
        Ok(Self { state })
    }
}

impl Iterator for ExecutorLogReplay {
    type Item = DeltaResult<<ScanLogReplayProcessor as LogReplayProcessor>::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.next()
    }
}

impl DriverLogReplay {
    fn try_new(
        engine: Arc<dyn Engine>,
        snapshot: SnapshotRef,
        state_info: Arc<StateInfo>,
    ) -> DeltaResult<Self> {
        let processor = ScanLogReplayProcessor::new(engine.as_ref(), state_info)?;
        let distribuor = Distributor::try_new(engine.clone(), snapshot, processor)?;
        Ok(Self {
            state: distribuor,
            engine,
        })
    }

    fn finalize(mut self) -> DeltaResult<ExecutorLogReplayInfo> {
        require!(
            self.next().is_none(),
            Error::generic_err("Iterator should be exausted before moving to the next phase")
        );
        require!(
            matches!(self.state.curr_state(), DistributorState::RootManifest(_),),
            Error::generic_err("Invariant violation")
        );
        self.state.next_state(self.engine)?;
        let DistributorState::LeafManifest(file_meta) = self.state.state else {
            return Err(Error::generic_err("Invariant violation"));
        };
        Ok(ExecutorLogReplayInfo {
            files: file_meta,
            processor: self.state.processor,
            snapshot: self.state.snapshot,
        })
    }
}

impl Iterator for DriverLogReplay {
    type Item = DeltaResult<<ScanLogReplayProcessor as LogReplayProcessor>::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.state.next();
        if next.is_some() {
            return next;
        }

        match self.state.curr_state() {
            DistributorState::Commit => self
                .state
                .next_state(self.engine.clone())
                .map(|_| self.state.next())
                .transpose()
                .map(|x| x?),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::log_replay::ScanLogReplayProcessor;
    use crate::scan::ScanBuilder;
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
    fn test_two_phase_driver_replay() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("table-without-dv-small")?;

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);
        
        let mut driver_replay = DriverLogReplay::try_new(
            engine.clone(),
            snapshot.clone(),
            state_info.clone(),
        )?;

        let mut batch_count = 0;
        let mut file_paths = Vec::new();
        
        while let Some(result) = driver_replay.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            file_paths.extend(paths);
            batch_count += 1;
        }

        // table-without-dv-small has exactly 1 commit, so driver processes 1 batch
        assert_eq!(batch_count, 1, "Driver should process exactly 1 batch for table-without-dv-small");
        
        // table-without-dv-small has exactly 1 file
        file_paths.sort();
        let expected_files = vec!["part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"];
        
        println!("Driver replay results:");
        println!("  - Batches processed: {}", batch_count);
        println!("  - Files found: {}", file_paths.len());
        println!("  - Files: {:?}", file_paths);
        
        assert_eq!(
            file_paths, expected_files,
            "Driver should find exactly the expected file"
        );

        Ok(())
    }

    #[test]
    fn test_two_phase_executor_replay() -> DeltaResult<()> {
        let (engine, snapshot, _url) = load_test_table("v2-checkpoints-json-with-sidecars")?;

        let state_info = StdArc::new(StateInfo::try_new(
            snapshot.schema(),
            snapshot.table_configuration(),
            None,
            (),
        )?);

        // First get the file metadata from driver
        let mut driver_replay = DriverLogReplay::try_new(
            engine.clone(),
            snapshot.clone(),
            state_info.clone(),
        )?;

        // Drain the driver to process commits
        let mut driver_batch_count = 0;
        while let Some(result) = driver_replay.next() {
            result?;
            driver_batch_count += 1;
        }

        println!("Driver processed {} batches", driver_batch_count);

        // Finalize to get executor state
        let executor_state = driver_replay.finalize()?;

        // Now run executor replay
        let mut executor_replay = ExecutorLogReplay::try_new(
            engine.clone(),
            executor_state,
        )?;

        let mut executor_file_paths = Vec::new();
        
        while let Some(result) = executor_replay.next() {
            let metadata = result?;
            let paths = metadata.visit_scan_files(vec![], |ps: &mut Vec<String>, path, _, _, _, _, _| {
                ps.push(path.to_string());
            })?;
            executor_file_paths.extend(paths);
        }

        executor_file_paths.sort();
        
        // v2-checkpoints-json-with-sidecars has exactly 101 files (verified from commit logs)
        assert_eq!(
            executor_file_paths.len(), 101,
            "Executor should process exactly 101 files for v2-checkpoints-json-with-sidecars"
        );
        
        // Verify first few files match expected (sampling to keep test readable)
        let expected_first_files = vec![
            "test%25file%25prefix-part-00000-01086c52-1b86-48d0-8889-517fe626849d-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-0fd71c0e-fd08-4685-87d6-aae77532d3ea-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-2710dd7f-9fa5-429d-b3fb-c005ba16e062-c000.snappy.parquet",
        ];
        
        println!("Executor replay results:");
        println!("  - Files found: {}", executor_file_paths.len());
        println!("  - First 3 executor files: {:?}", &executor_file_paths[..3]);
        
        assert_eq!(
            &executor_file_paths[..3], &expected_first_files[..],
            "Executor should process files in expected order"
        );

        Ok(())
    }
}
