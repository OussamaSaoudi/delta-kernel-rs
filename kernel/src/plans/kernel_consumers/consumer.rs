//! Core `KernelConsumer` trait, identity types, and typed-output companion.
//!
//! Adding a new KDF: declare the struct, derive `Clone`, write
//! `impl KernelConsumer for T { ... }` with `kind`, `apply`, `finish`, and pair it with a
//! `KernelConsumerOutput` impl declaring the typed output. KDFs ride on the
//! [`EngineRequest::Consume`](crate::plans::state_machines::framework::step::EngineRequest::Consume)
//! step, which is dispatched in-process and never serialized.
//!
//! # Object-safety notes
//!
//! - Associated types are NOT on [`KernelConsumer`] -- `Box<dyn KernelConsumer>` must be
//!   heterogeneous across concrete consumer implementations (the executor mixes consumers with
//!   different state types). Typed output lives on the [`KernelConsumerOutput`] companion trait via
//!   static dispatch.
//! - `finish(self: Box<Self>) -> Box<dyn Any + Send>` erases the per-impl state type to keep the
//!   trait object-safe. Typed factories downcast inside their extract closure.

use std::any::Any;

use dyn_clone::DynClone;
use strum::Display as StrumDisplay;
use uuid::Uuid;

use crate::plans::errors::DeltaError;
use crate::{DeltaResult, EngineData};

// === Control flow ===

/// Loop control returned by a consumer after each batch.
///
/// `Break` lets a consumer stop driving more input once it has everything
/// it needs (e.g. `CheckpointHintReader` after the first row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KdfControl {
    Continue,
    Break,
}

// === Identity ===

/// Stable diagnostic identifier per consumer impl. Used in tracing spans, metrics labels,
/// panic messages, and [`KernelConsumerToken`] construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum KernelConsumerKind {
    CheckpointHint,
    MetadataProtocol,
    SidecarCollector,
}

/// Identity for a kernel-consumer entry on a finished handle.
///
/// Stamped at plan-build time when a [`EngineRequest::Consume`] step is constructed. The fresh
/// UUID `id` ensures stale handles from a prior plan can't be confused with current
/// ones -- a [`FinishedHandle`] arriving with a token from a dead plan fails the
/// [`Extractor`] sanity check at decode time.
///
/// `Display` emits `<kind>#<id>`.
///
/// [`EngineRequest::Consume`]: crate::plans::state_machines::framework::step::EngineRequest::Consume
/// [`FinishedHandle`]: super::handle::FinishedHandle
/// [`Extractor`]: super::handle::Extractor
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KernelConsumerToken {
    pub kind: KernelConsumerKind,
    pub id: String,
}

impl KernelConsumerToken {
    /// Mint a fresh token for a kernel consumer with a UUID id.
    pub fn new(kind: KernelConsumerKind) -> Self {
        Self {
            kind,
            id: Uuid::new_v4().to_string(),
        }
    }
}

impl std::fmt::Display for KernelConsumerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.kind, self.id)
    }
}

// === Consumer trait ===

/// Stateful observer over batches. Returns [`KdfControl`] per batch for
/// early termination.
///
/// Useful for accumulating log segments, reading hint files, collecting
/// sidecar references, etc.
///
/// `Box<dyn KernelConsumer>` is cloneable via the [`DynClone`] supertrait -- the
/// blanket `impl<T: Clone> DynClone for T` covers every concrete KDF state
/// that derives `Clone`, so impls never write their own `clone_boxed`.
pub trait KernelConsumer: DynClone + Send + Sync + std::fmt::Debug {
    /// Diagnostic identifier, stamped into the [`KernelConsumerToken`] at plan-build time.
    fn kind(&self) -> KernelConsumerKind;

    /// Observe one batch. Return [`KdfControl::Break`] to stop driving
    /// further input; the kernel treats it as "child exhausted."
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl>;

    /// Consume the finalized KDF, returning its state erased to
    /// `Box<dyn Any + Send>`. Typed factories downcast inside their extract
    /// closure back to the concrete state type.
    ///
    /// Signature takes `Box<Self>` rather than `self` so it's object-safe;
    /// concrete impls are usually `fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
    /// Box::new(*self) }`.
    fn finish(self: Box<Self>) -> Box<dyn Any + Send>;
}

// `Clone` for `Box<dyn KernelConsumer>` -- delegates to `DynClone` (which every
// concrete `Clone` impl gets for free via the blanket).
dyn_clone::clone_trait_object!(KernelConsumer);

// === Typed-output companion ===

/// Typed-output companion. Each KDF state impls this once, declaring the
/// typed output callers receive and how the finalized state reduces to it.
///
/// ```ignore
/// impl KernelConsumerOutput for SidecarCollector {
///     type Output = Vec<FileMeta>;
///     fn into_output(self) -> Result<Self::Output, DeltaError> {
///         /* project on self */
///     }
/// }
/// ```
pub trait KernelConsumerOutput: KernelConsumer + Any + Sized + 'static {
    /// What downstream callers receive after `phase.execute(...)` completes.
    type Output: Send + 'static;

    /// Reduce the finalized state to [`Self::Output`].
    ///
    /// Each consume sink is single-partition by construction (the executor
    /// drains one root partition; the planner pins `target_partitions = 1`),
    /// so this consumes `Self` directly. Token-keyed identity validation
    /// happens upstream in [`Extractor::for_consumer`]'s closure.
    ///
    /// [`Extractor::for_consumer`]: super::handle::Extractor::for_consumer
    fn into_output(self) -> Result<Self::Output, DeltaError>;
}
