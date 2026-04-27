//! The [`DeclarativePlanNode`] tree and its chain construction API.
//!
//! # Construction API
//!
//! The prototype uses a fluent-builder style directly on the enum — static
//! constructors for leaves, chain methods for unary transforms and n-ary
//! combinators, and terminal methods that produce a [`Plan`] (untyped) or a
//! [`Prepared<O>`] (typed, via a consumer KDF that also impls
//! [`crate::plans::kdf::KdfOutput`]).
//!
//! ```ignore
//! use delta_kernel::plans::ir::DeclarativePlanNode;
//!
//! // Untyped pipeline: scan JSON, project to a sub-schema, stream results.
//! let plan = DeclarativePlanNode::scan_json_as::<CheckpointHintRecord>(files)
//!     .project(projection, output_schema)
//!     .results();
//!
//! // Typed pipeline: scan JSON, drain into a typed consumer KDF, harvest
//! // the output.
//! let prepared = DeclarativePlanNode::scan_json_as::<CheckpointHintRecord>(files)
//!     .consume(CheckpointHintReader::default());
//! ```
//!
//! ## Leaves
//!
//! - [`DeclarativePlanNode::scan`] / [`scan_json`] / [`scan_parquet`] —
//!   explicit-schema scans.
//! - [`DeclarativePlanNode::scan_as`] / [`scan_json_as`] / [`scan_parquet_as`]
//!   — schemas inferred from a struct via [`crate::schema::ToSchema`].
//! - [`DeclarativePlanNode::listing`] — storage prefix listing.
//! - [`DeclarativePlanNode::literal`] / [`literal_row`] — kernel-provided
//!   constant rows. Aligned with [`crate::EvaluationHandler::create_many`].
//! - [`DeclarativePlanNode::union`] / [`union_unordered`] — concatenate N children.
//!
//! ## Transforms
//!
//! - [`DeclarativePlanNode::with_predicate`] / [`with_row_index`] /
//!   [`with_ordered`] — scan modifiers.
//! - [`DeclarativePlanNode::filter`] — predicate filter.
//! - [`DeclarativePlanNode::project`] — projection.
//! - [`DeclarativePlanNode::apply_opt`] — conditional chain composition.
//!
//! ## Terminals
//!
//! - [`DeclarativePlanNode::into_plan`] — explicit sink.
//! - [`DeclarativePlanNode::results`] — sugar for
//!   `into_plan(SinkType::Results)`.
//! - [`DeclarativePlanNode::into_relation`] — pipe output into a named
//!   [`RelationHandle`](super::nodes::RelationHandle) for another plan in
//!   the same `PhaseOperation::Plans(...)` to consume.
//! - [`DeclarativePlanNode::consume`] — typed KDF consumer terminal; returns
//!   a [`Prepared<O>`] whose plan terminates in
//!   [`SinkType::ConsumeByKdf`](super::nodes::SinkType::ConsumeByKdf).
//!
//! ## Escape hatches
//!
//! - [`DeclarativePlanNode::consume_by_kdf`] — terminate `self` into a
//!   [`SinkType::ConsumeByKdf`] sink from a pre-built
//!   [`ConsumeByKdfSink`](super::nodes::ConsumeByKdfSink) without a typed extractor.
//!
//! [`scan_json`]: DeclarativePlanNode::scan_json
//! [`scan_parquet`]: DeclarativePlanNode::scan_parquet
//! [`scan_json_as`]: DeclarativePlanNode::scan_json_as
//! [`scan_parquet_as`]: DeclarativePlanNode::scan_parquet_as
//! [`literal_row`]: DeclarativePlanNode::literal_row
//! [`union_unordered`]: DeclarativePlanNode::union_unordered
//! [`with_row_index`]: DeclarativePlanNode::with_row_index
//! [`with_ordered`]: DeclarativePlanNode::with_ordered

use std::sync::Arc;

use url::Url;

use super::nodes::*;
use super::plan::Plan;
use crate::expressions::{Expression, Scalar};
use crate::plans::kdf::typed::ExtractFn;
use crate::plans::kdf::{downcast_all, ConsumerKdf, KdfOutput, KdfStateToken};
use crate::schema::{SchemaRef, StructType, ToSchema};
use crate::{DeltaResult, Error, FileMeta};

/// A single node in the declarative plan tree.
///
/// Trees are transforms-only: every complete pipeline terminates in a
/// [`Plan`] via one of the terminal methods (`into_plan`, `results`,
/// `into_relation`, `consume_by_kdf`) or in a [`Prepared<O>`] via
/// [`DeclarativePlanNode::consume`].
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    // Leaves
    Scan(ScanNode),
    FileListing(FileListingNode),
    Literal(LiteralNode),
    /// Read batches from a relation produced by another plan in the same
    /// `PhaseOperation::Plans(...)`. Wired up by the executor to a bounded
    /// channel whose sender is held by the producing plan's
    /// [`SinkType::Relation`](super::nodes::SinkType::Relation).
    Relation(RelationHandle),

    // Unary
    Filter {
        child: Box<DeclarativePlanNode>,
        node: FilterNode,
    },
    Project {
        child: Box<DeclarativePlanNode>,
        node: ProjectNode,
    },

    // N-ary
    Union {
        children: Vec<DeclarativePlanNode>,
        node: UnionNode,
    },
}

// ============================================================================
// Leaves — static constructors
// ============================================================================

impl DeclarativePlanNode {
    /// Scan a fixed file list with an explicit schema.
    pub fn scan(file_type: FileType, files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self::Scan(ScanNode::new(file_type, files, schema))
    }

    /// Scan JSON files with an explicit schema.
    pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self::scan(FileType::Json, files, schema)
    }

    /// Scan Parquet files with an explicit schema.
    pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self::scan(FileType::Parquet, files, schema)
    }

    /// Scan a fixed file list with a schema inferred from `T`.
    ///
    /// Shortcut for [`Self::scan`] with `schema = Arc::new(T::to_schema())`.
    pub fn scan_as<T: ToSchema>(file_type: FileType, files: Vec<FileMeta>) -> Self {
        Self::scan(file_type, files, Arc::new(T::to_schema()))
    }

    /// Scan JSON files with a schema inferred from `T`.
    pub fn scan_json_as<T: ToSchema>(files: Vec<FileMeta>) -> Self {
        Self::scan_as::<T>(FileType::Json, files)
    }

    /// Scan Parquet files with a schema inferred from `T`.
    pub fn scan_parquet_as<T: ToSchema>(files: Vec<FileMeta>) -> Self {
        Self::scan_as::<T>(FileType::Parquet, files)
    }

    /// List files under a storage prefix.
    pub fn listing(path: Url) -> Self {
        Self::FileListing(FileListingNode { path })
    }

    /// Read batches from a relation produced by another plan in the same
    /// `PhaseOperation::Plans(...)`. The producing plan terminates in
    /// [`SinkType::Relation`](super::nodes::SinkType::Relation) referencing
    /// the same handle.
    pub fn relation(handle: RelationHandle) -> Self {
        Self::Relation(handle)
    }

    /// Empty literal — zero rows with an empty-struct schema.
    ///
    /// Useful as a placeholder for pipelines that must exist structurally but
    /// produce no rows.
    pub fn empty() -> Self {
        let schema = Arc::new(StructType::new_unchecked(
            Vec::<crate::schema::StructField>::new(),
        ));
        Self::Literal(LiteralNode {
            schema,
            rows: Vec::new(),
        })
    }

    /// Multi-row literal. Rows × columns layout; see [`LiteralNode`] invariants.
    pub fn literal(schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> DeltaResult<Self> {
        Ok(Self::Literal(LiteralNode::try_new(schema, rows)?))
    }

    /// Single-row literal. Sugar for the common one-row case.
    pub fn literal_row(schema: SchemaRef, values: Vec<Scalar>) -> DeltaResult<Self> {
        Ok(Self::Literal(LiteralNode::try_new_row(schema, values)?))
    }

    /// Ordered union of N child streams; children are consumed in declaration
    /// order.
    ///
    /// Zero children yields an empty-struct output. With one child, the union
    /// wrapper is omitted.
    pub fn union<I>(children: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        let mut children: Vec<Self> = children.into_iter().collect();
        if children.len() == 1 {
            return children
                .pop()
                .ok_or_else(|| Error::generic("internal: single-child union drained empty"));
        }
        Ok(Self::Union {
            children,
            node: UnionNode { ordered: true },
        })
    }

    /// Unordered union — the engine may interleave or reorder children freely.
    pub fn union_unordered<I>(children: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        let children: Vec<Self> = children.into_iter().collect();
        Ok(Self::Union {
            children,
            node: UnionNode { ordered: false },
        })
    }
}

// ============================================================================
// Scan modifiers
// ============================================================================

impl DeclarativePlanNode {
    /// Attach a row-index column to a [`ScanNode`].
    ///
    /// Returns an error if `self` is not a scan — these modifiers are only
    /// meaningful immediately after a scan constructor.
    pub fn with_row_index(mut self, col: impl Into<String>) -> DeltaResult<Self> {
        match &mut self {
            Self::Scan(scan) => {
                scan.row_index_column = Some(col.into());
                Ok(self)
            }
            other => Err(Error::generic(format!(
                "with_row_index requires a Scan node, got {}",
                node_kind_name(other),
            ))),
        }
    }

    /// Attach a pushdown predicate hint to a [`ScanNode`].
    ///
    /// Returns an error if `self` is not a scan.
    pub fn with_predicate(mut self, predicate: Arc<Expression>) -> DeltaResult<Self> {
        match &mut self {
            Self::Scan(scan) => {
                scan.predicate = Some(predicate);
                Ok(self)
            }
            other => Err(Error::generic(format!(
                "with_predicate requires a Scan node, got {}",
                node_kind_name(other),
            ))),
        }
    }

    /// Mark a [`ScanNode`] as ordered (cross-file emission preserves the
    /// `files` order). See [`ScanNode`]'s ordering doc for the SQL-equivalence
    /// contract.
    ///
    /// Returns an error if `self` is not a scan.
    pub fn with_ordered(mut self) -> DeltaResult<Self> {
        match &mut self {
            Self::Scan(scan) => {
                scan.ordered = true;
                Ok(self)
            }
            other => Err(Error::generic(format!(
                "with_ordered requires a Scan node, got {}",
                node_kind_name(other),
            ))),
        }
    }
}

// ============================================================================
// Transforms
// ============================================================================

impl DeclarativePlanNode {
    /// Wrap `self` in a [`Filter`](Self::Filter).
    pub fn filter(self, predicate: Arc<Expression>) -> Self {
        Self::Filter {
            child: Box::new(self),
            node: FilterNode { predicate },
        }
    }

    /// Wrap `self` in a [`Project`](Self::Project).
    pub fn project(self, columns: Vec<Arc<Expression>>, output_schema: SchemaRef) -> Self {
        Self::Project {
            child: Box::new(self),
            node: ProjectNode {
                columns,
                output_schema,
            },
        }
    }

    /// Typed KDF consumer terminal. Wraps `self` in a [`Plan`] terminating in
    /// [`SinkType::ConsumeByKdf`] and returns a [`Prepared<O>`] carrying the
    /// typed extractor.
    pub fn consume<S>(self, state: S) -> Prepared<S::Output>
    where
        S: ConsumerKdf + KdfOutput + 'static,
    {
        let node = ConsumeByKdfSink::new_consumer(state);
        let token = node.token.clone();
        let extract = make_extract::<S>(token.clone());
        Prepared {
            plan: self.into_plan(SinkType::ConsumeByKdf(node)),
            token,
            extract,
        }
    }

    /// Power-user escape hatch: terminate `self` in a [`Plan`] with a
    /// [`SinkType::ConsumeByKdf`] from a pre-built consumer-KDF node. No typed
    /// extractor is threaded forward — caller harvests partition states directly.
    ///
    /// Use [`Self::consume`] instead when the state type also implements
    /// [`KdfOutput`].
    pub fn consume_by_kdf(self, node: ConsumeByKdfSink) -> Plan {
        self.into_plan(SinkType::ConsumeByKdf(node))
    }
}

/// Build the typed extract closure for a state `S` at a given token. The
/// closure downcasts each per-partition erased state back to `S` and reduces
/// via [`KdfOutput::into_output`].
fn make_extract<S>(token: KdfStateToken) -> ExtractFn<S::Output>
where
    S: KdfOutput + 'static,
{
    Box::new(move |parts| {
        let states = downcast_all::<S>(parts, &token)?;
        S::into_output(states)
    })
}

// ============================================================================
// Typed wrapper: Prepared<O>
// ============================================================================

/// A fully-assembled [`Plan`] paired with a typed post-execution extractor.
///
/// Produced by [`DeclarativePlanNode::consume`]. The executor runs `plan`;
/// the caller harvests the typed output via [`Prepared::extract`].
pub struct Prepared<O> {
    /// The assembled plan the executor drives.
    pub plan: Plan,
    token: KdfStateToken,
    extract: ExtractFn<O>,
}

impl<O: Send + 'static> Prepared<O> {
    /// Token identifying the KDF state this extractor will consume.
    pub fn token(&self) -> &KdfStateToken {
        &self.token
    }

    /// Consume the prepared plan, producing the typed output from the
    /// finalized per-partition states the executor handed back.
    pub fn extract(
        self,
        parts: Vec<Box<dyn std::any::Any + Send>>,
    ) -> Result<O, crate::plans::errors::DeltaError> {
        (self.extract)(parts)
    }

    /// Split into the executable plan and its extractor. Useful for executor
    /// boundaries where the two live in different scopes.
    pub fn into_parts(self) -> (Plan, ExtractFn<O>) {
        (self.plan, self.extract)
    }
}

impl<O> std::fmt::Debug for Prepared<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("plan", &self.plan)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

// ============================================================================
// Chain helpers
// ============================================================================

impl DeclarativePlanNode {
    /// Apply `f` to the chain only if `opt` is `Some`.
    ///
    /// Useful for optional sub-pipelines (data skipping, etc.) that leave the
    /// chain unchanged when absent.
    ///
    /// ```ignore
    /// let plan = base
    ///     .apply_opt(data_skipping, |node, ds| {
    ///         node.project(ds.select_columns, ds.select_schema)
    ///             .filter(ds.predicate)
    ///     })
    ///     .project(final_cols, final_schema)
    ///     .results();
    /// ```
    pub fn apply_opt<T, F>(self, opt: Option<T>, f: F) -> Self
    where
        F: FnOnce(Self, T) -> Self,
    {
        match opt {
            Some(t) => f(self, t),
            None => self,
        }
    }
}

// ============================================================================
// Terminals
// ============================================================================

impl DeclarativePlanNode {
    /// Wrap this tree in a [`Plan`] with the given sink.
    pub fn into_plan(self, sink_type: SinkType) -> Plan {
        Plan::new(self, SinkNode { sink_type })
    }

    /// Terminal: stream every output batch to the caller.
    pub fn results(self) -> Plan {
        self.into_plan(SinkType::Results)
    }

    /// Terminal: stream every output batch to the named relation. Another
    /// plan in the same `PhaseOperation::Plans(...)` consumes the relation
    /// via [`Self::relation`].
    pub fn into_relation(self, handle: RelationHandle) -> Plan {
        self.into_plan(SinkType::Relation(handle))
    }
}

// ============================================================================
// Introspection
// ============================================================================

impl DeclarativePlanNode {
    /// Borrowed child subtrees, in left-to-right declaration order.
    pub fn children(&self) -> Vec<&DeclarativePlanNode> {
        match self {
            Self::Scan(..) | Self::FileListing(..) | Self::Literal(..) | Self::Relation(..) => {
                Vec::new()
            }
            Self::Filter { child, .. } | Self::Project { child, .. } => {
                vec![child.as_ref()]
            }
            Self::Union { children, .. } => children.iter().collect(),
        }
    }

    /// True for leaf node variants (no child subtrees).
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::Scan(..) | Self::FileListing(..) | Self::Literal(..) | Self::Relation(..)
        )
    }
}

/// Static label for a node variant; used in error messages and diagnostics.
fn node_kind_name(node: &DeclarativePlanNode) -> &'static str {
    match node {
        DeclarativePlanNode::Scan(..) => "Scan",
        DeclarativePlanNode::FileListing(..) => "FileListing",
        DeclarativePlanNode::Literal(..) => "Literal",
        DeclarativePlanNode::Relation(..) => "Relation",
        DeclarativePlanNode::Filter { .. } => "Filter",
        DeclarativePlanNode::Project { .. } => "Project",
        DeclarativePlanNode::Union { .. } => "Union",
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;
    use crate::expressions::Expression;
    use crate::plans::errors::DeltaError;
    use crate::plans::kdf::{ConsumerKdf, Kdf, KdfControl, KdfOutput};
    use crate::schema::{DataType, StructField, StructType};

    fn simple_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "version",
            DataType::LONG,
        )]))
    }

    #[test]
    fn scan_has_no_children() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema());
        assert!(plan.is_leaf());
        assert_eq!(plan.children().len(), 0);
    }

    #[test]
    fn filter_wraps_child() {
        let predicate = Arc::new(Expression::literal(true));
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).filter(predicate);

        assert!(!plan.is_leaf());
        assert_eq!(plan.children().len(), 1);
        assert!(plan.children()[0].is_leaf());
    }

    #[test]
    fn union_with_single_child_unwraps() {
        let only = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let unioned = DeclarativePlanNode::union(vec![only]).unwrap();
        assert!(matches!(unioned, DeclarativePlanNode::Scan(..)));
    }

    #[test]
    fn union_zero_children_is_empty_struct() {
        let unioned = DeclarativePlanNode::union(Vec::<DeclarativePlanNode>::new()).unwrap();
        assert!(matches!(unioned, DeclarativePlanNode::Union { .. }));
    }

    #[test]
    fn apply_opt_none_is_noop() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .apply_opt(None::<()>, |node, _| {
                node.filter(Arc::new(Expression::literal(true)))
            });
        assert!(plan.is_leaf());
    }

    #[test]
    fn apply_opt_some_applies() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .apply_opt(Some(()), |node, _| {
                node.filter(Arc::new(Expression::literal(true)))
            });
        assert!(matches!(plan, DeclarativePlanNode::Filter { .. }));
    }

    #[test]
    fn results_terminal() {
        let base = DeclarativePlanNode::scan_json(vec![], simple_schema());
        let results_plan = base.results();
        assert_eq!(results_plan.sink.sink_type, SinkType::Results);
    }

    #[test]
    fn with_ordered_sets_scan_ordered_bit() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .with_ordered()
            .unwrap();
        match plan {
            DeclarativePlanNode::Scan(s) => assert!(s.ordered),
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn with_ordered_errors_off_scan() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .filter(Arc::new(Expression::literal(true)));
        let err = plan.with_ordered().unwrap_err();
        assert!(format!("{err}").contains("requires a Scan node"));
    }

    #[test]
    fn relation_leaf_is_a_leaf() {
        let h = RelationHandle::fresh("r0");
        let plan = DeclarativePlanNode::relation(h.clone());
        assert!(plan.is_leaf());
        assert_eq!(plan.children().len(), 0);
        match plan {
            DeclarativePlanNode::Relation(got) => assert_eq!(got, h),
            _ => panic!("expected Relation leaf"),
        }
    }

    #[test]
    fn into_relation_terminal_emits_relation_sink() {
        let h = RelationHandle::fresh("output");
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).into_relation(h.clone());
        match plan.sink.sink_type {
            SinkType::Relation(got) => assert_eq!(got, h),
            other => panic!("expected SinkType::Relation, got {other:?}"),
        }
    }

    #[test]
    fn fresh_relation_handles_are_distinct() {
        let a = RelationHandle::fresh("dup");
        let b = RelationHandle::fresh("dup");
        assert_ne!(a, b);
        assert_eq!(a.name, b.name);
    }

    #[test]
    fn literal_row_validates_field_count() {
        let schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
        ]));
        DeclarativePlanNode::literal_row(schema.clone(), vec![Scalar::Long(1), Scalar::Long(2)])
            .unwrap();
        let err = DeclarativePlanNode::literal_row(schema, vec![Scalar::Long(1)]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("schema expects"), "message was: {msg}");
    }

    #[test]
    fn literal_multirow_roundtrips_rows() {
        let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "v",
            DataType::LONG,
        )]));
        let rows = vec![
            vec![Scalar::Long(1)],
            vec![Scalar::Long(2)],
            vec![Scalar::Long(3)],
        ];
        let plan = DeclarativePlanNode::literal(schema, rows.clone()).unwrap();
        match plan {
            DeclarativePlanNode::Literal(lit) => assert_eq!(lit.rows, rows),
            _ => panic!("expected Literal"),
        }
    }

    #[test]
    fn literal_rejects_too_many_rows() {
        let schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "v",
            DataType::LONG,
        )]));
        let too_many: Vec<Vec<Scalar>> = (0..LITERAL_NODE_MAX_ROWS + 1)
            .map(|_| vec![Scalar::Long(0)])
            .collect();
        let err = DeclarativePlanNode::literal(schema, too_many).unwrap_err();
        assert!(format!("{err}").contains("max rows"));
    }

    #[test]
    fn with_row_index_errors_off_scan() {
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema())
            .filter(Arc::new(Expression::literal(true)));
        let err = plan.with_row_index("rowidx").unwrap_err();
        assert!(format!("{err}").contains("requires a Scan node"));
    }

    // === Consumer-KDF tests (validated via a test-local consumer state) ===

    #[derive(Debug, Clone)]
    struct NoopConsumer;
    impl Kdf for NoopConsumer {
        fn kdf_id(&self) -> &'static str {
            "consumer.noop"
        }
        fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
            Box::new(*self)
        }
    }
    impl ConsumerKdf for NoopConsumer {
        fn apply(&mut self, _batch: &dyn crate::EngineData) -> DeltaResult<KdfControl> {
            Ok(KdfControl::Break)
        }
    }
    impl KdfOutput for NoopConsumer {
        type Output = bool;
        fn into_output(parts: Vec<Self>) -> Result<bool, DeltaError> {
            Ok(!parts.is_empty())
        }
    }

    #[test]
    fn consume_by_kdf_escape_hatch_terminates_in_sink() {
        let node = ConsumeByKdfSink::new_consumer(NoopConsumer);
        let plan = DeclarativePlanNode::scan_json(vec![], simple_schema()).consume_by_kdf(node);
        assert!(matches!(plan.sink.sink_type, SinkType::ConsumeByKdf(_)));
    }

    #[test]
    fn consume_produces_prepared_with_kdf_sink() {
        let prepared =
            DeclarativePlanNode::scan_json(vec![], simple_schema()).consume(NoopConsumer);
        assert!(matches!(
            prepared.plan.sink.sink_type,
            SinkType::ConsumeByKdf(_)
        ));
        assert_eq!(prepared.token().kdf_id, "consumer.noop");
        let parts: Vec<Box<dyn Any + Send>> = vec![Box::new(NoopConsumer)];
        assert!(prepared.extract(parts).unwrap());
    }

    #[test]
    fn token_embeds_kdf_id() {
        let node = ConsumeByKdfSink::new_consumer(NoopConsumer);
        assert_eq!(node.token.kdf_id, "consumer.noop");
        let s = node.token.to_string();
        assert!(s.starts_with("consumer.noop#"), "got: {s}");
    }
}
