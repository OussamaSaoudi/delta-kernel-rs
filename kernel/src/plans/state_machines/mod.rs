//! State machines -- kernel-authored coroutine bodies that orchestrate plan execution.
//!
//! - [`framework`] -- the framework SMs are built on: the
//!   [`StateMachine`](framework::state_machine::StateMachine) trait, the
//!   [`CoroutineSM`](framework::coroutine::CoroutineSM) driver (a thin shell over
//!   `genawaiter2::rc::GenBoxed`), the typed [`Context`](framework::plan_context::Context) /
//!   [`EngineResponse`] surface, and the `Extractor<O>` typed adapter.
//!
//! [`EngineResponse`]: framework::state_machine::EngineResponse

pub mod framework;
pub mod scan;
pub mod snapshot;
