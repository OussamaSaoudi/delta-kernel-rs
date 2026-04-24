//! Coroutine-backed `StateMachine` implementation.
//!
//! - [`generator`] — minimal hand-rolled stackless coroutine shim.
//!   Replaces `genawaiter` with a no-deps `Co<Y, R>` / `Gen<Y, R, O>` pair.
//! - [`phase`] — async [`Phase<'a>`](phase::Phase) surface SM authors use
//!   (`phase.execute(prepared).await`), plus the [`PhaseYield`](phase::PhaseYield)
//!   / [`PhaseResume`](phase::PhaseResume) protocol that flows through the
//!   generator.
//! - [`engine`] — the [`CoroutineSM<R>`](engine::CoroutineSM) driver that
//!   compiles [`PhaseYield`](phase::PhaseYield) sequences into the
//!   [`StateMachine`](super::state_machine::StateMachine) contract.

pub mod engine;
pub mod generator;
pub mod phase;
