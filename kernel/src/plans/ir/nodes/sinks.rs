//! Sink IR types — relation piping and [`ConsumeByKdf`] sinks.

use std::sync::Arc;

use crate::expressions::Expression;
use crate::plans::kdf::{ConsumerKdf, Handle, KdfStateToken, TraceContext};
use crate::schema::SchemaRef;

use super::OrderingSpec;

/// Template for draining a row stream into a [`ConsumerKdf`] via
/// [`SinkType::ConsumeByKdf`].
///
/// Fields mirror the former plan-tree `KdfNode<dyn ConsumerKdf>` payload; KDF
/// lives exclusively under sinks in the IR.
///
/// - `initial_state`: cloned per partition via [`DynClone`](dyn_clone::DynClone)
///   into a [`Handle`](crate::plans::kdf::Handle).
/// - `partitionable_by`: optional hash-partition expression (unused by consumers today).
/// - `requires_ordering`: stamped from [`ConsumerKdf::required_ordering`] at construction.
/// - `token`: joins finalized state back to phase `PhaseKdfState`.
pub struct ConsumeByKdfSink {
    pub initial_state: Box<dyn ConsumerKdf>,
    pub partitionable_by: Option<Arc<Expression>>,
    pub requires_ordering: Option<OrderingSpec>,
    pub token: KdfStateToken,
}

impl ConsumeByKdfSink {
    /// Construct from a concrete consumer. Mints a fresh token from the KDF's
    /// `kdf_id` and reads ordering from [`ConsumerKdf::required_ordering`].
    pub fn new_consumer<C: ConsumerKdf + 'static>(state: C) -> Self {
        let requires_ordering = state.required_ordering();
        let token = KdfStateToken::new(state.kdf_id());
        Self {
            initial_state: Box::new(state),
            partitionable_by: None,
            requires_ordering,
            token,
        }
    }

    /// Mint a per-partition runtime [`Handle`] for this sink template.
    pub fn new_handle(&self, ctx: TraceContext, partition: u32) -> Handle<dyn ConsumerKdf> {
        Handle::new(
            self.token.clone(),
            ctx,
            partition,
            self.initial_state.clone(),
        )
    }
}

impl Clone for ConsumeByKdfSink {
    fn clone(&self) -> Self {
        Self {
            initial_state: self.initial_state.clone(),
            partitionable_by: self.partitionable_by.clone(),
            requires_ordering: self.requires_ordering.clone(),
            token: self.token.clone(),
        }
    }
}

impl std::fmt::Debug for ConsumeByKdfSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumeByKdfSink")
            .field("kdf_id", &self.initial_state.kdf_id())
            .field("requires_ordering", &self.requires_ordering)
            .field("token", &self.token)
            .finish()
    }
}

/// Identifier for a relation produced by one plan and consumed by another in
/// the same `PhaseOperation::Plans(...)`. Created via [`RelationHandle::fresh`];
/// each handle is unique across all kernel plans for the lifetime of the
/// process (id-based comparison).
///
/// Handles connect a [`SinkType::Relation`] in one plan to a
/// [`crate::plans::ir::declarative::DeclarativePlanNode::Relation`] leaf in another.
/// The executor allocates a bounded channel per handle, the producing plan's
/// sink writes to it, and the consuming plan's source reads from it —
/// streaming end-to-end, not materialized.
///
/// The handle also carries the producing plan's output [`SchemaRef`] so that
/// consuming sources (`Relation` / `HashJoin` / `Union`) can publish
/// a static output schema during pipeline construction; that unblocks
/// operators like `FilterByExpression` and `Select` that need an input
/// schema to build their evaluators. Schema is metadata, not identity:
/// equality and hashing key on `id` only.
#[derive(Debug, Clone)]
pub struct RelationHandle {
    /// Diagnostic name (used in tracing spans, error messages); not part of
    /// equality / hashing.
    pub name: String,
    /// Process-wide monotonic id. Drives equality and hashing so that two
    /// freshly-minted handles with the same name are still distinct.
    pub id: u64,
    /// Output schema of the producing plan. Carried alongside the handle so
    /// that consuming sources publish a static schema to downstream
    /// operators without an external lookup. Not part of equality / hashing.
    pub schema: SchemaRef,
}

impl RelationHandle {
    /// Mint a fresh handle with the given diagnostic name and producer
    /// output schema. Distinct ids regardless of `name` collisions, so names
    /// are diagnostic-only — relations are identified by id.
    pub fn fresh(name: impl Into<String>, schema: SchemaRef) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self {
            name: name.into(),
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            schema,
        }
    }
}

// `id` alone drives equality and hashing — `name` is diagnostic and `schema`
// is metadata. The `fresh` constructor guarantees process-wide unique ids.
impl PartialEq for RelationHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for RelationHandle {}
impl std::hash::Hash for RelationHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// What the engine does with the terminal row stream.
///
/// This branch ships three sink shapes: `Results` for terminal pipelines that
/// stream batches back to the caller, `Relation` for piping a plan's output
/// into another plan in the same `PhaseOperation::Plans(...)`, and
/// `ConsumeByKdf` for draining a stream into a [`ConsumerKdf`] state machine.
/// `Load` / `Write` / `PartitionedWrite` land with their respective stacks.
#[derive(Debug, Clone)]
pub enum SinkType {
    /// Stream every output batch to the caller.
    Results,
    /// Stream every output batch to the named [`RelationHandle`]. Another
    /// plan in the same phase consumes via
    /// [`crate::plans::ir::declarative::DeclarativePlanNode::Relation`].
    Relation(RelationHandle),
    /// Drain every output batch through the wrapped consumer KDF. The KDF's
    /// finalized per-partition state is harvested by the engine via phase
    /// `PhaseKdfState` (or the typed
    /// [`Prepared`](crate::plans::ir::declarative::Prepared) extractor that yields
    /// `O = ConsumerKdf::Output`).
    ConsumeByKdf(ConsumeByKdfSink),
}

impl PartialEq for SinkType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SinkType::Results, SinkType::Results) => true,
            (SinkType::Relation(a), SinkType::Relation(b)) => a == b,
            (SinkType::ConsumeByKdf(a), SinkType::ConsumeByKdf(b)) => a.token == b.token,
            _ => false,
        }
    }
}

impl Eq for SinkType {}

/// Terminal node of a [`crate::plans::ir::plan::Plan`], carrying a [`SinkType`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkNode {
    pub sink_type: SinkType,
}

impl SinkNode {
    /// `Results`-sink convenience constructor.
    pub fn results() -> Self {
        Self {
            sink_type: SinkType::Results,
        }
    }

    /// `Relation`-sink convenience constructor.
    pub fn relation(handle: RelationHandle) -> Self {
        Self {
            sink_type: SinkType::Relation(handle),
        }
    }

    /// `ConsumeByKdf`-sink convenience constructor.
    pub fn consume_by_kdf(node: ConsumeByKdfSink) -> Self {
        Self {
            sink_type: SinkType::ConsumeByKdf(node),
        }
    }
}
