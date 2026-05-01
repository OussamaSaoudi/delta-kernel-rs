//! The [`CoroutineSM`] driver: compiles [`PhaseYield`] sequences into the
//! [`StateMachine`] contract.
//!
//! The coroutine yields one operation at a time; the driver:
//!
//! 1. drains [`PhaseYield::Dispatch`] yields into a single `Vec<Plan>` batch, tracking per-task
//!    ids;
//! 2. presents the batch as a [`PhaseOperation::Plans`] to the engine;
//! 3. on `advance`, resolves same-batch awaits in-memory (no engine round-trip) and drains any
//!    fresh dispatches before returning [`AdvanceResult::Continue`] or [`AdvanceResult::Done`].
//!
//! **Invariant:** a successfully constructed [`CoroutineSM`] always has
//! at least one concrete phase queued. Zero-phase coroutines are invalid;
//! the builder must short-circuit before constructing an SM.
//!
//! `expect_used` is allowed in this module for the same reason as in
//! [`super::generator`]: protocol invariants whose violation indicates a
//! kernel bug. User-facing failures flow through [`DeltaError`].

#![allow(clippy::expect_used)]

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use super::super::engine_error::EngineError;
use super::super::phase_kdf_state::PhaseKdfState;
use super::super::phase_operation::PhaseOperation;
use super::super::state_machine::{AdvanceResult, StateMachine};
use super::generator::{Co, Gen, GeneratorState};
use super::phase::{PhaseCo, PhaseResume, PhaseYield};
use crate::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt};
use crate::{bail_delta, DeltaResult, Error};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type InnerGen<R> = Gen<PhaseYield, PhaseResume, Result<R, DeltaError>>;

/// A state machine driven by a hand-rolled stackless coroutine.
///
/// The coroutine yields [`PhaseYield`] at each phase and receives
/// [`PhaseResume`] on resume. The driver acts as a *compiler*: it drains
/// all `Dispatch` yields (collecting plans) until an `Await`, then
/// presents the batch to the engine via the
/// [`StateMachine::get_operation`] / [`StateMachine::advance`] interface.
///
/// This means the engine sees `Plans(Vec<Plan>)` as before — the only
/// difference is that the vec may now contain more plans (from multiple
/// dispatches). Plans within a batch may be executed concurrently by the
/// engine.
pub struct CoroutineSM<R: Send + 'static> {
    gen: InnerGen<R>,
    /// The yield we're currently positioned at (after draining).
    current: Option<PhaseYield>,
    /// Monotonic task ID counter.
    next_task_id: u64,
    /// Task ID range dispatched during the current drain pass.
    current_batch_task_range: Option<Range<u64>>,
    /// Cached engine result for the current batch; used to resolve
    /// same-batch awaits in-memory without an extra engine round-trip.
    current_batch_kdf: Option<Result<PhaseKdfState, EngineError>>,
    /// Stored terminal result when the coroutine completes during a drain
    /// loop; handed out via [`StateMachine::advance`].
    final_result: Option<Result<R, DeltaError>>,
    /// Plans accumulated during the current drain pass. Cleared when a
    /// fresh batch begins.
    drained_plans: Arc<[crate::plans::ir::Plan]>,
}

impl<R: Send + 'static> CoroutineSM<R> {
    /// Construct and prime a coroutine state machine.
    ///
    /// The producer runs until its first yield, which becomes the initial
    /// phase. If the producer completes without ever yielding, construction
    /// fails — the caller is expected to short-circuit that case in its
    /// builder rather than fake a plan.
    pub(crate) fn new<F, Fut>(producer: F) -> Result<Self, DeltaError>
    where
        F: FnOnce(PhaseCo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, DeltaError>> + Send + 'static,
    {
        let mut gen: InnerGen<R> = Gen::new(move |co: Co<PhaseYield, PhaseResume>| producer(co));
        // Prime with an empty `Completed` so bodies that immediately await
        // a "previous" result don't panic. In practice the body runs until
        // its first `yield_`, at which point the prime value is discarded
        // (see `generator.rs` documentation).
        let prime = PhaseResume::Completed(Ok(PhaseKdfState::empty()));
        match gen.resume_with(prime) {
            GeneratorState::Yielded(phase) => {
                let mut sm = Self {
                    gen,
                    current: Some(phase),
                    next_task_id: 0,
                    current_batch_task_range: None,
                    current_batch_kdf: None,
                    final_result: None,
                    drained_plans: Arc::from(Vec::<crate::plans::ir::Plan>::new()),
                };
                sm.drain_dispatches()
                    .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
                // Invariant: a constructed SM always has work to do. If the
                // coroutine yielded a single `Dispatch` then immediately
                // completed without any plans, the builder should have
                // short-circuited before constructing the SM.
                if sm.final_result.is_some() && sm.drained_plans.is_empty() && sm.current.is_none()
                {
                    bail_delta!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "CoroutineSM::new",
                        detail = "coroutine completed after priming without yielding any work; \
                                  the caller should short-circuit before constructing an SM",
                    );
                }
                Ok(sm)
            }
            GeneratorState::Complete(_) => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::new",
                detail = "coroutine completed during priming without yielding any work; \
                          the caller should short-circuit before constructing an SM",
            ),
        }
    }

    /// `true` once the coroutine has terminated and [`advance`](Self::advance)
    /// has handed out its final result.
    pub fn is_done(&self) -> bool {
        self.current.is_none() && self.final_result.is_none()
    }

    /// Drain [`PhaseYield::Dispatch`] yields from the coroutine, collecting
    /// plans and task IDs until an [`Await`](PhaseYield::Await),
    /// [`SchemaQuery`](PhaseYield::SchemaQuery), or completion.
    fn drain_dispatches(&mut self) -> DeltaResult<()> {
        let mut drained_plans = Vec::new();
        let batch_start = self.next_task_id;
        self.current_batch_kdf = None;

        loop {
            match self.current.take() {
                Some(PhaseYield::Dispatch {
                    plan,
                    phase_name: _,
                }) => {
                    let task_id = self.next_task_id;
                    self.next_task_id += 1;
                    drained_plans.push(plan);

                    match self.gen.resume_with(PhaseResume::Dispatched { task_id }) {
                        GeneratorState::Yielded(next) => {
                            self.current = Some(next);
                            continue;
                        }
                        GeneratorState::Complete(result) => {
                            self.final_result = Some(result);
                            self.current_batch_task_range = if drained_plans.is_empty() {
                                None
                            } else {
                                Some(batch_start..self.next_task_id)
                            };
                            self.drained_plans = Arc::from(drained_plans);
                            return Ok(());
                        }
                    }
                }
                Some(other) => {
                    // `Await` or `SchemaQuery` — stop draining.
                    self.current_batch_task_range = if drained_plans.is_empty() {
                        None
                    } else {
                        Some(batch_start..self.next_task_id)
                    };
                    self.drained_plans = Arc::from(drained_plans);
                    self.current = Some(other);
                    return Ok(());
                }
                None => {
                    self.current_batch_task_range = if drained_plans.is_empty() {
                        None
                    } else {
                        Some(batch_start..self.next_task_id)
                    };
                    self.drained_plans = Arc::from(drained_plans);
                    return Ok(());
                }
            }
        }
    }

    /// `true` when `task_id` was dispatched during the current drain pass.
    fn is_in_current_batch(&self, task_id: u64) -> bool {
        self.current_batch_task_range
            .as_ref()
            .is_some_and(|range| range.contains(&task_id))
    }

    /// Resolve awaits on tasks from the current batch without an engine
    /// round-trip. When the coroutine dispatched A and B in one batch and
    /// then awaits A, the engine returns a merged KDF; subsequent awaits
    /// for tasks in the same batch can immediately replay that result.
    fn resolve_same_batch_awaits(&mut self) -> DeltaResult<()> {
        loop {
            match &self.current {
                Some(PhaseYield::Await { task_id, .. }) if self.is_in_current_batch(*task_id) => {
                    let kdf = self
                        .current_batch_kdf
                        .as_ref()
                        .map(clone_phase_result)
                        .ok_or_else(|| {
                            Error::generic("CoroutineSM: no batch KDF for same-batch await")
                        })?;

                    match self.gen.resume_with(PhaseResume::Completed(kdf)) {
                        GeneratorState::Yielded(next) => {
                            self.current = Some(next);
                            continue;
                        }
                        GeneratorState::Complete(final_result) => {
                            self.current = None;
                            self.final_result = Some(final_result);
                            return Ok(());
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }
}

impl<R: Send + 'static> StateMachine for CoroutineSM<R> {
    type Result = R;

    fn get_operation(&mut self) -> Result<PhaseOperation, DeltaError> {
        // Fire-and-forget: coroutine completed but left plans queued. Hand
        // them out; next `advance` will produce `Done` with the stored
        // result.
        if self.final_result.is_some() && !self.drained_plans.is_empty() {
            return Ok(PhaseOperation::Plans(self.drained_plans.to_vec()));
        }
        // By the `CoroutineSM::new` invariant, a constructed SM with
        // `final_result` set always has `drained_plans` non-empty at this
        // point (fire-and-forget). Reaching here with final_result set and
        // no plans means an `advance` was skipped.
        debug_assert!(
            self.final_result.is_none(),
            "CoroutineSM: get_operation called after terminal advance"
        );
        match &self.current {
            Some(PhaseYield::Await { .. }) => {
                if self.drained_plans.is_empty() {
                    bail_delta!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "CoroutineSM::get_operation",
                        detail = "await without any dispatched plans",
                    );
                }
                Ok(PhaseOperation::Plans(self.drained_plans.to_vec()))
            }
            Some(PhaseYield::SchemaQuery { node, .. }) => {
                Ok(PhaseOperation::SchemaQuery(node.clone()))
            }
            Some(PhaseYield::Dispatch { .. }) => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::get_operation",
                detail = "found undrained Dispatch (internal bug)",
            ),
            None => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::get_operation",
                detail = "state machine already completed",
            ),
        }
    }

    fn advance(
        &mut self,
        result: Result<PhaseKdfState, EngineError>,
    ) -> Result<AdvanceResult<R>, DeltaError> {
        // If the coroutine already completed during a prior drain, the
        // engine just finished its fire-and-forget plans. Return the
        // stored final result.
        if let Some(final_result) = self.final_result.take() {
            self.current = None;
            self.drained_plans = Arc::from(Vec::<crate::plans::ir::Plan>::new());
            return final_result.map(AdvanceResult::Done);
        }

        if self.current.is_none() {
            bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::advance",
                detail = "cannot advance, already completed",
            );
        }

        // Cache the batch KDF for same-batch await resolution.
        self.current_batch_kdf = Some(clone_phase_result(&result));
        let resume = PhaseResume::Completed(result);

        match self.gen.resume_with(resume) {
            GeneratorState::Yielded(next) => {
                self.current = Some(next);
                // Same-batch awaits: immediately satisfy from cache.
                self.resolve_same_batch_awaits()
                    .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
                // Drain any new dispatches past the await.
                self.drain_dispatches()
                    .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
                // If draining surfaced a completed coroutine, return now.
                if let Some(result) = self.final_result.take() {
                    self.current = None;
                    return result.map(AdvanceResult::Done);
                }
                if self.current.is_some() {
                    Ok(AdvanceResult::Continue)
                } else {
                    bail_delta!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "CoroutineSM::advance",
                        detail = "unexpected state after drain",
                    );
                }
            }
            GeneratorState::Complete(final_result) => {
                self.current = None;
                final_result.map(AdvanceResult::Done)
            }
        }
    }

    fn phase_name(&self) -> &'static str {
        match &self.current {
            Some(PhaseYield::Dispatch { phase_name, .. }) => phase_name,
            Some(PhaseYield::SchemaQuery { phase_name, .. }) => phase_name,
            Some(PhaseYield::Await { phase_name, .. }) => phase_name,
            None => "complete",
        }
    }
}

/// Clone a `Result<PhaseKdfState, EngineError>` for same-batch re-use.
/// `EngineError::clone` drops the source chain (see that module for
/// rationale); `PhaseKdfState::clone` is a cheap Arc clone.
fn clone_phase_result(
    r: &Result<PhaseKdfState, EngineError>,
) -> Result<PhaseKdfState, EngineError> {
    match r {
        Ok(kdf) => Ok(kdf.clone()),
        Err(e) => Err(e.clone()),
    }
}

// Silence `BoxFuture` unused-type warning on non-test builds; kept as a
// documentation aid for the shape the generator expects internally.
#[allow(dead_code)]
fn _assert_box_future_shape() {
    let _f: BoxFuture<Result<(), DeltaError>> = Box::pin(async { Ok(()) });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::plans::ir::nodes::{ScanNode, SinkNode, SinkType};
    use crate::plans::ir::{DeclarativePlanNode, Plan};
    use crate::plans::state_machines::framework::engine_error::{EngineError, EngineErrorKind};

    // A throwaway plan whose identity we don't care about — the SM tests
    // only look at the shape of `PhaseOperation`, not what's inside.
    fn toy_plan() -> Plan {
        let schema = Arc::new(crate::schema::StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        let root = DeclarativePlanNode::Scan(ScanNode::new(
            crate::plans::ir::nodes::FileType::Parquet,
            Vec::new(),
            schema,
        ));
        let sink = SinkNode {
            sink_type: SinkType::Results,
        };
        Plan::new(root, sink)
    }

    /// Drive a two-phase SM: yield plan A, collect empty KDF, yield plan B,
    /// complete.
    #[test]
    fn two_phase_sm_dispatches_and_advances() {
        let mut sm = CoroutineSM::<i64>::new(|co| async move {
            // Phase 1: raw yield of a dispatch.
            let r0 = co
                .yield_(PhaseYield::Dispatch {
                    plan: toy_plan(),
                    phase_name: "phase_a",
                })
                .await;
            let id0 = match r0 {
                PhaseResume::Dispatched { task_id } => task_id,
                _ => panic!("expected Dispatched"),
            };
            let _r1 = co
                .yield_(PhaseYield::Await {
                    task_id: id0,
                    phase_name: "phase_a",
                })
                .await;
            // Phase 2.
            let r2 = co
                .yield_(PhaseYield::Dispatch {
                    plan: toy_plan(),
                    phase_name: "phase_b",
                })
                .await;
            let id2 = match r2 {
                PhaseResume::Dispatched { task_id } => task_id,
                _ => panic!("expected Dispatched"),
            };
            let _r3 = co
                .yield_(PhaseYield::Await {
                    task_id: id2,
                    phase_name: "phase_b",
                })
                .await;
            Ok(42)
        })
        .unwrap();

        assert_eq!(sm.phase_name(), "phase_a");
        let op = sm.get_operation().unwrap();
        match op {
            PhaseOperation::Plans(plans) => assert_eq!(plans.len(), 1),
            _ => panic!("expected Plans"),
        }

        // Advance past phase A.
        let r = sm.advance(Ok(PhaseKdfState::empty())).unwrap();
        assert!(matches!(r, AdvanceResult::Continue));
        assert_eq!(sm.phase_name(), "phase_b");

        let op = sm.get_operation().unwrap();
        assert!(matches!(op, PhaseOperation::Plans(p) if p.len() == 1));

        let r = sm.advance(Ok(PhaseKdfState::empty())).unwrap();
        match r {
            AdvanceResult::Done(v) => assert_eq!(v, 42),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }

    /// Two dispatches before the first await → one batch of two plans.
    #[test]
    fn same_batch_dispatches_are_drained_into_single_plans_op() {
        let mut sm = CoroutineSM::<()>::new(|co| async move {
            let r0 = co
                .yield_(PhaseYield::Dispatch {
                    plan: toy_plan(),
                    phase_name: "a",
                })
                .await;
            let id0 = match r0 {
                PhaseResume::Dispatched { task_id } => task_id,
                _ => panic!(),
            };
            let r1 = co
                .yield_(PhaseYield::Dispatch {
                    plan: toy_plan(),
                    phase_name: "b",
                })
                .await;
            let id1 = match r1 {
                PhaseResume::Dispatched { task_id } => task_id,
                _ => panic!(),
            };
            // Await the first — the driver should resolve the second in
            // the same batch without an extra engine round-trip.
            let _ = co
                .yield_(PhaseYield::Await {
                    task_id: id0,
                    phase_name: "a",
                })
                .await;
            let _ = co
                .yield_(PhaseYield::Await {
                    task_id: id1,
                    phase_name: "b",
                })
                .await;
            Ok(())
        })
        .unwrap();

        let op = sm.get_operation().unwrap();
        match op {
            PhaseOperation::Plans(plans) => assert_eq!(plans.len(), 2),
            _ => panic!("expected Plans"),
        }

        // Single advance should drain both awaits and complete.
        let r = sm.advance(Ok(PhaseKdfState::empty())).unwrap();
        assert!(matches!(r, AdvanceResult::Done(())));
    }

    /// Engine errors flow through as `Err(EngineError)` on resume; the SM
    /// body receives them and decides what to do.
    #[test]
    fn engine_error_flows_to_body_as_resume_err() {
        let mut sm = CoroutineSM::<String>::new(|co| async move {
            let r = co
                .yield_(PhaseYield::Dispatch {
                    plan: toy_plan(),
                    phase_name: "p",
                })
                .await;
            let id = match r {
                PhaseResume::Dispatched { task_id } => task_id,
                _ => panic!(),
            };
            match co
                .yield_(PhaseYield::Await {
                    task_id: id,
                    phase_name: "p",
                })
                .await
            {
                PhaseResume::Completed(Err(e)) => Ok(format!("got: {}", e.kind)),
                other => panic!("unexpected: {other:?}"),
            }
        })
        .unwrap();

        let _ = sm.get_operation().unwrap();
        let err = EngineError::new(EngineErrorKind::FileNotFound { path: "/x".into() });
        let r = sm.advance(Err(err)).unwrap();
        match r {
            AdvanceResult::Done(s) => assert!(s.contains("file not found: /x")),
            _ => panic!("expected Done"),
        }
    }

    /// Coroutines that complete without yielding a single plan should be
    /// rejected at construction.
    #[test]
    fn zero_phase_coroutine_is_rejected() {
        let r = CoroutineSM::<i32>::new(|_co| async move { Ok(7) });
        assert!(r.is_err(), "no-yield coroutine should fail at new()");
    }
}
