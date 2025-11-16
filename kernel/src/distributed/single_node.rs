use std::sync::Arc;

use crate::{
    distributed::distributor::Distributor,
    log_replay::LogReplayProcessor,
    scan::{log_replay::ScanLogReplayProcessor, state_info::StateInfo},
    DeltaResult, Engine, SnapshotRef,
};

pub(crate) struct SingleNode {
    state: Distributor<ScanLogReplayProcessor>,
    engine: Arc<dyn Engine>,
}

impl SingleNode {
    pub fn try_new(
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
}

impl Iterator for SingleNode {
    type Item = DeltaResult<<ScanLogReplayProcessor as LogReplayProcessor>::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.state.next();
        if next.is_some() {
            return next;
        }
        self.state
            .next_state(self.engine.clone())
            .map(|_| self.state.next())
            .transpose()
            .map(|x| x?)
    }
}
