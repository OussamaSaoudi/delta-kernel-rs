//! Runtime state for KDF consumers.
//!
//! [`ConsumerHandle`] is the executor's working buffer for one [`ConsumeSink`]: created when a
//! phase starts, fed batches via [`ConsumerHandle::apply`], finalized via
//! [`ConsumerHandle::finish`] when the child is exhausted. Type-erased into [`FinishedHandle`]
//! and returned to the state machine as
//! [`EngineResponse::Consumer`](crate::plans::state_machines::framework::step_payload::EngineResponse::Consumer).
//!
//! Handles dispatch in-process and never cross a serialization boundary.
//!
//! Callers recover typed output from a [`FinishedHandle`] via the paired [`Extractor`], minted
//! at plan-build time and threaded through to the SM body.
//!
//! [`ConsumeSink`]: crate::plans::ir::nodes::ConsumeSink

use std::any::Any;

use super::consumer::{KdfControl, KernelConsumer, KernelConsumerOutput, KernelConsumerToken};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::{delta_error, DeltaResult, EngineData};

// === Runtime handle ===

/// Runtime state carrier. Holds the mutable consumer working buffer and the token that joins
/// its eventual finalized state back to the plan-tree node.
#[derive(Debug)]
pub struct ConsumerHandle {
    token: KernelConsumerToken,
    inner: Box<dyn KernelConsumer>,
}

impl ConsumerHandle {
    /// Construct a handle from a fresh token and a cloned initial state.
    pub fn new(token: KernelConsumerToken, inner: Box<dyn KernelConsumer>) -> Self {
        Self { token, inner }
    }

    /// Apply the consumer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kernel_consumer.apply",
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
        name = "kernel_consumer.finish",
        skip(self),
        fields(kind = %self.inner.kind(), token_id = self.token.id),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kernel consumer handle finished");
        FinishedHandle {
            token: self.token,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`ConsumerHandle::finish`] -- carries the token and the type-erased final state.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KernelConsumerToken,
    pub erased: Box<dyn Any + Send>,
}

// === Typed extraction ===

/// A typed adapter for pulling the typed output of a single consume sink
/// out of a [`FinishedHandle`].
///
/// SM bodies build an `Extractor` while planting a [`EngineRequest::Consume`] (via
/// [`PlanBuilder::consume`](crate::plans::state_machines::framework::plan_context::Context::consume))
/// and feed the engine's [`FinishedHandle`] back through [`Self::extract`] on resume.
///
/// [`EngineRequest::Consume`]: crate::plans::state_machines::framework::step::EngineRequest::Consume
pub struct Extractor<O> {
    token: KernelConsumerToken,
    extract: fn(Box<dyn Any + Send>) -> Result<O, DeltaError>,
}

impl<O: Send + 'static> Extractor<O> {
    /// Build an `Extractor` for KDF state `S` at `token`. The stored function pointer
    /// downcasts the erased payload back to `S` and runs `S::into_output`.
    pub(crate) fn for_consumer<S>(token: KernelConsumerToken) -> Self
    where
        S: KernelConsumerOutput<Output = O> + 'static,
    {
        Self {
            token,
            extract: extract_consumer::<S>,
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
                "kernel_consumer::extract: token mismatch -- handle token `{handle}` vs \
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
/// concrete `S` via `Extractor::for_consumer`'s generic fn-pointer coercion.
fn extract_consumer<S>(erased: Box<dyn Any + Send>) -> Result<S::Output, DeltaError>
where
    S: KernelConsumerOutput + 'static,
{
    let single = erased.downcast::<S>().map(|b| *b).map_err(|_| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "kernel_consumer::extract: expected `{}`",
            std::any::type_name::<S>(),
        )
    })?;
    single.into_output()
}
