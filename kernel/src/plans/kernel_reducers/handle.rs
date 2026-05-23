//! Runtime state for KDF reducers.
//!
//! [`ReducerHandle`] is the executor's working buffer for one [`ReduceSink`]: created when a
//! phase starts, fed batches via [`ReducerHandle::apply`], finalized via
//! [`ReducerHandle::finish`] when the child is exhausted. Type-erased into [`FinishedHandle`]
//! and returned to the state machine as
//! [`EngineResponse::Reducer`](crate::plans::state_machines::framework::step_payload::EngineResponse::Reducer).
//!
//! Handles dispatch in-process and never cross a serialization boundary.
//!
//! Callers recover typed output from a [`FinishedHandle`] via the paired [`Extractor`], minted
//! at plan-build time and threaded through to the SM body.
//!
//! [`ReduceSink`]: crate::plans::ir::nodes::ReduceSink

use std::any::Any;

use super::reducer::{KdfControl, KernelReducer, KernelReducerOutput, KernelReducerToken};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::{delta_error, DeltaResult, EngineData};

// === Runtime handle ===

/// Runtime state carrier. Holds the mutable reducer working buffer and the token that joins
/// its eventual finalized state back to the plan-tree node.
#[derive(Debug)]
pub struct ReducerHandle {
    token: KernelReducerToken,
    inner: Box<dyn KernelReducer>,
}

impl ReducerHandle {
    /// Construct a handle from a fresh token and a cloned initial state.
    pub fn new(token: KernelReducerToken, inner: Box<dyn KernelReducer>) -> Self {
        Self { token, inner }
    }

    /// Apply the reducer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kernel_reducer.apply",
        skip(self, batch),
        ret,
        fields(kind = %self.inner.kind(), token_id = self.token.id),
    )]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.inner.apply(batch)
    }

    /// Consume the handle, returning the finalized token-stamped state.
    #[tracing::instrument(
        level = "debug",
        name = "kernel_reducer.finish",
        skip(self),
        fields(kind = %self.inner.kind(), token_id = self.token.id),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kernel reducer handle finished");
        FinishedHandle {
            token: self.token,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`ReducerHandle::finish`] -- carries the token and the type-erased final state.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KernelReducerToken,
    pub erased: Box<dyn Any + Send>,
}

// === Typed extraction ===

/// A typed adapter for pulling the typed output of a single reduce sink
/// out of a [`FinishedHandle`].
///
/// SM bodies build an `Extractor` while planting a [`EngineRequest::Reduce`] (via
/// [`PlanBuilder::reduce`](crate::plans::state_machines::framework::plan_context::Context::reduce))
/// and feed the engine's [`FinishedHandle`] back through [`Self::extract`] on resume.
///
/// [`EngineRequest::Reduce`]: crate::plans::state_machines::framework::step::EngineRequest::Reduce
pub struct Extractor<O> {
    token: KernelReducerToken,
    extract: fn(Box<dyn Any + Send>) -> Result<O, DeltaError>,
}

impl<O: Send + 'static> Extractor<O> {
    /// Build an `Extractor` for KDF state `S` at `token`. The stored function pointer
    /// downcasts the erased payload back to `S` and runs `S::into_output`.
    pub(crate) fn for_reducer<S>(token: KernelReducerToken) -> Self
    where
        S: KernelReducerOutput<Output = O> + 'static,
    {
        Self {
            token,
            extract: extract_reducer::<S>,
        }
    }

    /// Decode `handle`'s payload into the typed output `O`.
    ///
    /// Sanity-checks that `handle.token` matches this extractor's token (cross-wired
    /// finished handles surface as an internal error) and runs the typed reduction.
    /// Decoding failures are wrapped in [`EngineError::internal`] so SM bodies can
    /// uniformly handle them on the engine-error path.
    pub fn extract(self, handle: FinishedHandle) -> Result<O, EngineError> {
        if handle.token != self.token {
            return Err(EngineError::internal(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "kernel_reducer::extract: token mismatch -- handle token `{handle}` vs \
                 expected `{expected}`",
                handle = handle.token,
                expected = self.token,
            )));
        }
        (self.extract)(handle.erased).map_err(EngineError::internal)
    }
}

impl<O> std::fmt::Debug for Extractor<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extractor")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// Downcast the erased payload back to `S` and run its typed reduction. Bound to a
/// concrete `S` via `Extractor::for_reducer`'s generic fn-pointer coercion.
fn extract_reducer<S>(erased: Box<dyn Any + Send>) -> Result<S::Output, DeltaError>
where
    S: KernelReducerOutput + 'static,
{
    let single = erased.downcast::<S>().map(|b| *b).map_err(|_| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "kernel_reducer::extract: expected `{}`",
            std::any::type_name::<S>(),
        )
    })?;
    single.into_output()
}
