//! Per-partition runtime state for KDFs.
//!
//! [`Handle<K>`] is the executor's working buffer for one partition of one
//! KDF. It's created from a [`ConsumeByKdfSink`] template when a phase starts,
//! fed batches via [`Handle::apply_consumer`], and finalized via [`Handle::finish`]
//! when the child is exhausted.
//!
//! [`ConsumeByKdfSink`]: crate::plans::ir::nodes::ConsumeByKdfSink
//!
//! Generic over `K: Kdf + ?Sized`; executor code uses `Handle<dyn ConsumerKdf>`
//! directly. KDFs ride on the
//! [`SinkType::ConsumeByKdf`](crate::plans::ir::nodes::SinkType::ConsumeByKdf)
//! sink, which is dispatched in-process — handles never need to cross a
//! serialization boundary.

use std::any::Any;

use super::token::KdfStateToken;
use super::trace::TraceContext;
use super::traits::{ConsumerKdf, Kdf, KdfControl};
use crate::{DeltaResult, EngineData};

/// Per-partition runtime state carrier. Generic over the KDF kind; executor
/// code holds `Handle<dyn ConsumerKdf>`.
///
/// `inner` is the mutable working buffer (a `Box<dyn ConsumerKdf>`). `token`
/// joins this handle's eventual finalized state back to the plan-tree node.
/// `ctx` + `partition` identify the execution context for tracing, metrics,
/// and error attribution.
#[derive(Debug)]
pub struct Handle<K: Kdf + ?Sized> {
    token: KdfStateToken,
    ctx: TraceContext,
    partition: u32,
    inner: Box<K>,
}

impl<K: Kdf + ?Sized> Handle<K> {
    /// Construct a handle. The executor calls this per (node, partition)
    /// pair when a phase starts, stamping the trace context and partition
    /// number at creation time.
    pub fn new(token: KdfStateToken, ctx: TraceContext, partition: u32, inner: Box<K>) -> Self {
        Self {
            token,
            ctx,
            partition,
            inner,
        }
    }

    pub fn token(&self) -> &KdfStateToken {
        &self.token
    }
    pub fn ctx(&self) -> &TraceContext {
        &self.ctx
    }
    pub fn partition(&self) -> u32 {
        self.partition
    }
    pub fn kdf_id(&self) -> &'static str {
        self.inner.kdf_id()
    }
}

impl<K: ConsumerKdf + ?Sized> Handle<K> {
    /// Apply the consumer to a batch.
    #[tracing::instrument(
        level = "trace",
        name = "kdf.apply",
        skip(self, batch),
        fields(
            sm = self.ctx.sm,
            phase = self.ctx.phase,
            partition = self.partition,
            kdf_id = self.inner.kdf_id(),
            serial = self.token.serial,
        ),
    )]
    pub fn apply_consumer(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let out = self.inner.apply(batch)?;
        tracing::trace!(control = ?out, "consumer batch applied");
        Ok(out)
    }
}

impl<K: Kdf + ?Sized> Handle<K> {
    /// Consume the handle, returning `(token, ctx, partition, erased state)`.
    /// The submission-time record in `PhaseKdfState` uses the (ctx, partition)
    /// for its safety cross-check.
    #[tracing::instrument(
        level = "debug",
        name = "kdf.finish",
        skip(self),
        fields(
            sm = self.ctx.sm,
            phase = self.ctx.phase,
            partition = self.partition,
            kdf_id = self.inner.kdf_id(),
            serial = self.token.serial,
        ),
    )]
    pub fn finish(self) -> FinishedHandle {
        tracing::debug!("kdf handle finished");
        FinishedHandle {
            token: self.token,
            ctx: self.ctx,
            partition: self.partition,
            erased: self.inner.finish(),
        }
    }
}

/// Output of [`Handle::finish`] — carries the token, execution context,
/// partition number, and erased final state. Consumed by `PhaseKdfState::submit`.
#[derive(Debug)]
pub struct FinishedHandle {
    pub token: KdfStateToken,
    pub ctx: TraceContext,
    pub partition: u32,
    pub erased: Box<dyn Any + Send>,
}

// `Clone` for `Handle<dyn ConsumerKdf>` comes from
// `dyn_clone::clone_trait_object!` on the subtrait — `Box<K>: Clone` is
// automatic, so this is a single generic impl.

impl<K: Kdf + ?Sized> Clone for Handle<K>
where
    Box<K>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            ctx: self.ctx.clone(),
            partition: self.partition,
            inner: self.inner.clone(),
        }
    }
}
