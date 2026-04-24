//! State machines — kernel-authored coroutine bodies that orchestrate
//! plan execution.
//!
//! - [`framework`] — the framework SMs are built on:
//!   the [`StateMachine`](framework::state_machine::StateMachine) trait, the
//!   [`CoroutineSM`](framework::coroutine::engine::CoroutineSM) driver, the
//!   typed `Phase` / `PhaseKdfState` / `Prepared<O>` surface, and the
//!   hand-rolled coroutine shim that replaces `genawaiter`.
//!
//! Concrete SMs (Snapshot, Scan, DML) land in follow-on PRs. Until then,
//! much of the framework surface has no in-tree caller — `dead_code` is
//! allowed subtree-wide to keep CI green while the remaining PRs land.

#![allow(dead_code)]

pub mod framework;
