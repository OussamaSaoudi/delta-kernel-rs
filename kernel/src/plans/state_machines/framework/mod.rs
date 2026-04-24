//! State-machine framework: the shared infrastructure every kernel SM uses.
//!
//! - [`state_machine`] — the [`StateMachine`](state_machine::StateMachine)
//!   trait + [`AdvanceResult`](state_machine::AdvanceResult) the executor
//!   drives.
//! - [`phase_operation`] / [`operation_type`] — typed "unit of work"
//!   handed from SM to executor each step.
//! - [`phase_kdf_state`] — thread-safe container accumulating
//!   per-partition [`FinishedHandle`](crate::plans::kdf::FinishedHandle)s
//!   that the executor returns to the SM.
//! - [`engine_error`] — [`EngineError`](engine_error::EngineError), the
//!   typed failure the engine surfaces to the SM (distinct from
//!   [`DeltaError`](crate::plans::errors::DeltaError), which is the
//!   kernel-to-caller error).
//! - [`coroutine`] — [`CoroutineSM`](coroutine::engine::CoroutineSM), the
//!   async-fn-backed `StateMachine` impl + its hand-rolled `Gen`/`Co`
//!   generator shim.

pub mod coroutine;
pub mod engine_error;
pub mod operation_type;
pub mod phase_kdf_state;
pub mod phase_operation;
pub mod state_machine;
