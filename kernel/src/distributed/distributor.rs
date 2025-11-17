use std::sync::Arc;

use itertools::Itertools;

use crate::{
    actions::visitors::SidecarVisitor,
    log_replay::{ActionsBatch, LogReplayProcessor},
    log_segment::LogSegment,
    scan::{CHECKPOINT_READ_SCHEMA, COMMIT_READ_SCHEMA, MANIFEST_READ_SCHEMA},
    utils::require,
    DeltaResult, Engine, Error, FileMeta, RowVisitor, SnapshotRef,
};

type ProcessorResult<P: LogReplayProcessor> = DeltaResult<P::Output>;
type ProcessorResultIter<P: LogReplayProcessor> = Box<dyn Iterator<Item = DeltaResult<P::Output>>>;

pub enum DistributorState {
    Cached(Option<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>>>>),
    Commit,
    RootManifest(SidecarVisitor),
    LeafManifest(Vec<FileMeta>),
    Done,
}

impl DistributorState {
    fn next_state(&self, log_segment: &LogSegment) -> DeltaResult<Self> {
        let next = match self {
            DistributorState::Cached(_) => DistributorState::Commit,
            DistributorState::Commit => match log_segment.checkpoint_parts.len() {
                1 => DistributorState::RootManifest(SidecarVisitor { sidecars: vec![] }),
                0 => DistributorState::Done,
                _ => {
                    let parts = log_segment
                        .checkpoint_parts
                        .iter()
                        .map(|path| path.location.clone())
                        .collect_vec();
                    DistributorState::LeafManifest(parts)
                }
            },
            DistributorState::RootManifest(sidecar_visitor) => {
                let sidecars = sidecar_visitor
                    .sidecars
                    .iter()
                    .map(|sidecar| sidecar.to_filemeta(&log_segment.log_root))
                    .try_collect()?;
                DistributorState::LeafManifest(sidecars)
            }
            DistributorState::LeafManifest(_) => DistributorState::Done,
            DistributorState::Done => {
                return Err(Error::generic(
                    "Should not call next on a completed state machine",
                ))
            }
        };
        Ok(next)
    }
    fn get_iter(
        &mut self,
        engine: Arc<dyn Engine>,
        log_segment: &LogSegment,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>>>> {
        match self {
            DistributorState::Cached(iterator) => match iterator.take() {
                Some(iterator) => Ok(iterator),
                None => return Err(Error::generic("Cached iterator is empty!")),
            },
            DistributorState::Commit => {
                let commits_and_compactions = log_segment.find_commit_cover();
                let commit_stream = engine
                    .json_handler()
                    .read_json_files(&commits_and_compactions, COMMIT_READ_SCHEMA.clone(), None)?
                    .map(|batch| -> DeltaResult<_> { Ok(ActionsBatch::new(batch?, true)) });
                Ok(Box::new(commit_stream))
            }
            DistributorState::RootManifest(_) => {
                let checkpoint_file_meta: Vec<_> = log_segment
                    .checkpoint_parts
                    .iter()
                    .map(|f| f.location.clone())
                    .collect();

                // Multi-part checkpoits are treated as leaf-level manifest files
                if checkpoint_file_meta.len() > 1 {
                    return Ok(Box::new(std::iter::empty()));
                }

                // Historically, we had a shared file reader trait for JSON and Parquet handlers,
                // but it was removed to avoid unnecessary coupling. This is a concrete case
                // where it *could* have been useful, but for now, we're keeping them separate.
                // If similar patterns start appearing elsewhere, we should reconsider that decision.
                let actions = match log_segment.checkpoint_parts.first() {
                    Some(parsed_log_path) if parsed_log_path.extension == "json" => {
                        engine.json_handler().read_json_files(
                            &checkpoint_file_meta,
                            MANIFEST_READ_SCHEMA.clone(),
                            None,
                        )?
                    }
                    Some(parsed_log_path) if parsed_log_path.extension == "parquet" => {
                        engine.parquet_handler().read_parquet_files(
                            &checkpoint_file_meta,
                            MANIFEST_READ_SCHEMA.clone(),
                            None,
                        )?
                    }
                    Some(parsed_log_path) => {
                        return Err(Error::generic(format!(
                            "Unsupported checkpoint file type: {}",
                            parsed_log_path.extension,
                        )));
                    }
                    // This is the case when there are no checkpoints in the log segment
                    // so we return an empty iterator
                    None => Box::new(std::iter::empty()),
                };

                let actions = actions.map(|batch| Ok(ActionsBatch::new(batch?, false)));
                Ok(Box::new(actions))
            }
            DistributorState::LeafManifest(leaf_files) => {
                let actions = engine
                    .parquet_handler()
                    .read_parquet_files(&leaf_files, CHECKPOINT_READ_SCHEMA.clone(), None)?
                    .map(|batch| -> DeltaResult<_> { Ok(ActionsBatch::new(batch?, false)) });
                Ok(Box::new(actions))
            }
            DistributorState::Done => Ok(Box::new(None.into_iter())),
        }
    }
}

pub(crate) struct Distributor<P: LogReplayProcessor> {
    pub snapshot: SnapshotRef,
    pub processor: P,
    curr_iter: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>>>,
    pub state: DistributorState,
}

impl<P: LogReplayProcessor> Distributor<P> {
    pub fn try_new(
        engine: Arc<dyn Engine>,
        snapshot: SnapshotRef,
        processor: P,
    ) -> DeltaResult<Self> {
        let mut state = DistributorState::Commit;
        let curr_iter = state.get_iter(engine, snapshot.log_segment())?;
        Ok(Self {
            snapshot,
            processor,
            curr_iter,
            state,
        })
    }

    pub fn try_new_from_state(
        engine: Arc<dyn Engine>,
        snapshot: SnapshotRef,
        processor: P,
        mut state: DistributorState,
    ) -> DeltaResult<Self> {
        let curr_iter = state.get_iter(engine, snapshot.log_segment())?;
        Ok(Self {
            snapshot,
            processor,
            curr_iter,
            state,
        })
    }
    pub fn next_state(&mut self, engine: Arc<dyn Engine>) -> DeltaResult<()> {
        require!(
            self.curr_iter.next().is_none(),
            Error::generic("stage not exhasted")
        );
        let mut new_state = self.state.next_state(self.snapshot.log_segment())?;
        let iter = new_state.get_iter(engine, self.snapshot.log_segment())?;
        self.curr_iter = iter;
        self.state = new_state;
        Ok(())
    }

    pub fn curr_state(&self) -> &DistributorState {
        &self.state
    }
}

impl<P: LogReplayProcessor> Iterator for Distributor<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut batch_res = self.curr_iter.next()?;
        if let DistributorState::RootManifest(sidecar_visitor) = &mut self.state {
            batch_res = batch_res.and_then(|batch| {
                sidecar_visitor.visit_rows_of(batch.actions())?;
                Ok(batch)
            });
        }
        Some(batch_res.and_then(|batch| self.processor.process_actions_batch(batch)))
    }
}
