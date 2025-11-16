use std::sync::Arc;

use crate::{
    distributed::distributor::{self, Distributor, DistributorState},
    log_replay::LogReplayProcessor,
    log_segment::Phase1LogReplay,
    scan::{log_replay::ScanLogReplayProcessor, state, state_info::StateInfo},
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
