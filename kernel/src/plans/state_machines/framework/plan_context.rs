//! Plan-construction context and builder.
//!
//! [`Context`] owns the in-flight plan and a per-Ref schema table. [`PlanBuilder`] is a
//! shared handle to a specific [`Ref`] in that program; builder methods append new
//! [`PlanNode`]s and return new [`PlanBuilder`]s threading the freshly minted Refs. Cursors are
//! [`Clone`] (cheap `Rc` clone) so branching is explicit.
//!
//! State is shared via `Rc<RefCell<ContextState>>` so transform methods can mutate without
//! requiring `&mut Context`. SMs are CPU-only sequencers and never need to cross thread
//! boundaries, so `Rc` (vs. `Arc`) is the right shape. See the
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM) module
//! docs for the `!Send` rationale.
//!
//! # Stale-builder protection
//!
//! Each builder captures the context's `session_id` at mint time. Dispatch methods
//! ([`Context::reduce`], [`Context::schema_query`]) bump the id to invalidate cursors
//! held across yields; [`Context::into_result_plan`] consumes `self`, which moots the
//! concern for terminal dispatch.
//!
//! # `RefCell` discipline
//!
//! PlanBuilder methods borrow_mut, mutate state, and drop the guard before returning. Dispatch
//! methods drop the borrow before any `engine.yield_(...).await`. Holding a `Ref` / `RefMut`
//! across an await would panic at runtime; this is structurally avoided by every method
//! in this module.
//!
//! [`PlanNode`]: crate::plans::ir::plan::PlanNode
//! [`Ref`]: crate::plans::ir::plan::Ref

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use url::Url;

use crate::expressions::{
    ColumnName, Expression, ExpressionRef, IntoColumnName, PredicateRef, Scalar,
};
use crate::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt};
use crate::plans::ir::nodes::{
    DvRef, EquiJoinNode, FileType, FilterNode, ListFilesNode, LoadNode, MaxByVersionNode,
    ProjectNode, ReduceSink, ScanFileColumns, ScanJsonNode, ScanParquetNode, UnionNode, ValuesNode,
};
use crate::plans::ir::plan::{JoinKind, NodeKind, Plan, PlanNode, Ref, ResultPlan};
use crate::plans::ir::schema_inference::infer_expression_type;
use crate::plans::kernel_reducers::{Extractor, KernelReducer, KernelReducerOutput};
use crate::plans::state_machines::framework::coroutine::context::{Engine, StepResume, StepYield};
use crate::plans::state_machines::framework::step::{EngineRequest, SchemaQuery};
use crate::plans::state_machines::framework::step_payload::EngineResponse;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{delta_error, FileMeta};

// ============================================================================
// Shared state
// ============================================================================

/// In-flight plan being built, the per-Ref schema table, the Ref counter, and a
/// session counter.
#[derive(Debug)]
struct ContextState {
    plan: Plan,
    /// Per-Ref output schemas. Indexed by `Ref.0` during construction (Refs are
    /// minted sequentially by `mint_ref`).
    ref_schemas: Vec<SchemaRef>,
    /// Next Ref id to mint. Owned by Context (not Plan) so the IR stays a pure
    /// data container. Reset to 0 by `reduce` when the in-flight plan is taken.
    next_ref: u32,
    /// Bumped by dispatch methods to invalidate cursors held across yields.
    session_id: u32,
}

impl ContextState {
    /// Append a node to the in-flight plan, mint its output Ref, and record
    /// `schema` for the new Ref.
    fn push_node(&mut self, kind: NodeKind, inputs: Vec<Ref>, schema: SchemaRef) -> Ref {
        let output = Ref(self.next_ref);
        self.next_ref += 1;
        self.plan.stmts.push(PlanNode {
            kind,
            inputs,
            output,
        });
        self.ref_schemas.push(schema);
        debug_assert_eq!(self.ref_schemas.len(), self.plan.stmts.len());
        output
    }
}

// ============================================================================
// Context
// ============================================================================

/// Plan-construction context.
///
/// Source methods mint root [`PlanBuilder`]s, dispatch methods
/// ([`Self::reduce`] / [`Self::schema_query`]) yield steps through a coroutine, and
/// [`Self::into_result_plan`] is the terminal sync drain (backward-reachability DCE).
pub struct Context {
    state: Rc<RefCell<ContextState>>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    /// Construct a fresh context with an empty plan.
    pub fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(ContextState {
                plan: Plan::new(),
                ref_schemas: Vec::new(),
                next_ref: 0,
                session_id: 0,
            })),
        }
    }

    /// Append a [`NodeKind::ListFiles`] source. Output schema is the engine-canonical listing
    /// shape supplied by the caller (the kernel does not pin it; consumers wire to the
    /// engine's listing schema).
    pub fn list_files(
        &self,
        start_from: Url,
        listing_schema: SchemaRef,
    ) -> Result<PlanBuilder, DeltaError> {
        push_node(
            &self.state,
            NodeKind::ListFiles(ListFilesNode { start_from }),
            &[],
            listing_schema,
        )
    }

    /// Append a [`NodeKind::ScanParquet`] source over Parquet `files`. Output schema
    /// is the caller-declared `schema`.
    pub fn scan_parquet(
        &self,
        files: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> Result<PlanBuilder, DeltaError> {
        let s = Arc::clone(&schema);
        push_node(
            &self.state,
            NodeKind::ScanParquet(ScanParquetNode { files, schema }),
            &[],
            s,
        )
    }

    /// Append a [`NodeKind::ScanJson`] source over newline-delimited JSON `files`. Output
    /// schema is the caller-declared `schema`.
    pub fn scan_json(
        &self,
        files: Vec<FileMeta>,
        schema: SchemaRef,
    ) -> Result<PlanBuilder, DeltaError> {
        let s = Arc::clone(&schema);
        push_node(
            &self.state,
            NodeKind::ScanJson(ScanJsonNode { files, schema }),
            &[],
            s,
        )
    }

    /// Append a [`NodeKind::Values`] source.
    pub fn values(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<Scalar>>,
    ) -> Result<PlanBuilder, DeltaError> {
        let s = Arc::clone(&schema);
        push_node(
            &self.state,
            NodeKind::Values(ValuesNode { schema, rows }),
            &[],
            s,
        )
    }

    /// Drain the accumulator into a [`EngineRequest::Reduce`] yielded through `engine` and recover
    /// the reducer's typed output via [`Extractor`].
    ///
    /// Backward-reachability DCE narrows the plan to stmts transitively reachable
    /// from `builder`. After the yield resumes, the accumulator is reset (empty plan +
    /// empty schema table) and `session_id` is bumped so any cursors held across the
    /// boundary fail at runtime.
    ///
    /// Borrows the shared state only briefly (mutation + drain) and drops the borrow
    /// before the awaited yield, so the await never holds a [`RefMut`](std::cell::RefMut).
    // Currently exercised only by tests; PR6 wires it into FSR.
    #[allow(dead_code)]
    pub(crate) async fn reduce<S>(
        &self,
        engine: &mut Engine,
        builder: PlanBuilder,
        reducer: S,
        step_name: &'static str,
    ) -> Result<S::Output, DeltaError>
    where
        S: KernelReducer + KernelReducerOutput + 'static,
    {
        let sink = ReduceSink::new_reducer(reducer);
        let extractor = Extractor::for_reducer::<S>(sink.token.clone());
        let terminal = builder.ref_id;
        let stmts: Vec<crate::plans::ir::plan::PlanNode> = {
            let mut s = self.state.borrow_mut();
            ensure_fresh(&s, &builder)?;
            let plan = std::mem::take(&mut s.plan);
            s.ref_schemas.clear();
            // Reset the Ref counter alongside the plan/schema reset: no Refs
            // survive the reduce boundary (the plan ships to the engine and the
            // session id bump invalidates any held PlanBuilders), so restarting
            // from 0 is safe and keeps subsequent Refs compact.
            s.next_ref = 0;
            s.session_id = s.session_id.wrapping_add(1);
            plan.reachable_from(terminal).stmts
        };
        // Drop the builder handle so the only remaining Rc on `state` is `self`'s.
        drop(builder);

        let StepResume(result) = engine
            .yield_(StepYield {
                operation: EngineRequest::Reduce {
                    stmts,
                    terminal,
                    sink,
                },
                step_name,
            })
            .await;
        let payload = result.map_err(|e| e.into_delta_typed())?;
        let EngineResponse::Reducer(handle) = payload else {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "reduce: executor returned non-Reducer payload `{payload:?}` for \
                 EngineRequest::Reduce",
            ));
        };
        extractor.extract(handle).map_err(|e| e.into_delta_typed())
    }

    /// Yield a [`EngineRequest::SchemaQuery`] and return the schema delivered by the engine.
    ///
    /// Bumps `session_id` so any cursors held across the boundary fail at runtime.
    /// The accumulator is preserved -- callers typically use the returned schema to
    /// build fresh sources, and old refs become unreachable from new cursors (the next
    /// `reduce`'s DCE prunes them).
    // Currently exercised only by tests; PR6 wires it into FSR.
    #[allow(dead_code)]
    pub(crate) async fn schema_query(
        &self,
        engine: &mut Engine,
        path: impl Into<String>,
        step_name: &'static str,
    ) -> Result<SchemaRef, DeltaError> {
        {
            let mut s = self.state.borrow_mut();
            s.session_id = s.session_id.wrapping_add(1);
        }
        let StepResume(result) = engine
            .yield_(StepYield {
                operation: EngineRequest::SchemaQuery(SchemaQuery::new(path.into())),
                step_name,
            })
            .await;
        let payload = result.map_err(|e| e.into_delta_typed())?;
        let EngineResponse::Schema(schema) = payload else {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "schema_query: executor returned non-Schema payload `{payload:?}` for \
                 EngineRequest::SchemaQuery",
            ));
        };
        Ok(schema)
    }

    /// Drain the accumulator into a [`ResultPlan`] keyed at `builder`. Performs
    /// backward-reachability DCE; only stmts transitively reachable from
    /// `builder.ref_id()` survive.
    ///
    /// Returns an error if `builder` is from a different context (or stale across a
    /// dispatch). All cursors must be dropped before this call -- the context state is
    /// solely owned at terminal time.
    pub fn into_result_plan(self, builder: PlanBuilder) -> Result<ResultPlan, DeltaError> {
        // Validate session before tearing down. Drop the borrow before `Rc::try_unwrap`.
        {
            let s = self.state.borrow();
            ensure_fresh(&s, &builder)?;
        }
        // Drop the builder's Rc handle so try_unwrap can succeed.
        drop(builder.state);
        let result = builder.ref_id;
        let inner = Rc::try_unwrap(self.state)
            .map(|cell| cell.into_inner())
            .map_err(|_| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "into_result_plan: outstanding PlanBuilder handles -- drop other Cursors before \
                     calling this method",
                )
            })?;
        let plan = inner.plan.reachable_from(result);
        Ok(ResultPlan { plan, result })
    }
}

// ============================================================================
// PlanBuilder
// ============================================================================

/// Handle to a Ref in the in-flight plan. PlanBuilder methods append further
/// stmts and return new Cursors threading the minted Refs.
#[derive(Clone, Debug)]
pub struct PlanBuilder {
    state: Rc<RefCell<ContextState>>,
    ref_id: Ref,
    session_id: u32,
}

impl PlanBuilder {
    /// Mint a fresh handle for `ref_id` against `state`. Captures `session_id` so the
    /// handle can be invalidated by dispatch methods (see module-level docs).
    fn new(state: &Rc<RefCell<ContextState>>, ref_id: Ref, session_id: u32) -> Self {
        Self {
            state: Rc::clone(state),
            ref_id,
            session_id,
        }
    }

    /// The Ref this builder points at.
    pub fn ref_id(&self) -> Ref {
        self.ref_id
    }

    /// The schema of the value at this Ref.
    ///
    /// Returns an error if the builder is stale.
    pub fn schema(&self) -> Result<SchemaRef, DeltaError> {
        let state = self.state.borrow();
        ensure_fresh(&state, self)?;
        state
            .ref_schemas
            .get(self.ref_id.0 as usize)
            .cloned()
            .ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "schema: ref {ref_id} has no recorded schema",
                    ref_id = self.ref_id.0,
                )
            })
    }

    // === Filter / Project ====================================================

    /// Append a [`NodeKind::Filter`]. Output schema is unchanged.
    pub fn filter(self, predicate: impl Into<PredicateRef>) -> Result<Self, DeltaError> {
        let predicate = predicate.into();
        let schema = self.schema()?;
        push_node(
            &self.state,
            NodeKind::Filter(FilterNode { predicate }),
            &[&self],
            schema,
        )
    }

    /// Append a [`NodeKind::Project`] with inferred output schema.
    ///
    /// Each pair becomes one output field `(name, infer_type(expr))`. Inference is
    /// narrow -- unsupported expressions error out and direct callers to
    /// [`Self::project_with_schema`].
    pub fn project<I, K, V>(self, named_exprs: I) -> Result<Self, DeltaError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<ExpressionRef>,
    {
        let input_schema = self.schema()?;
        let pairs: Vec<(String, Arc<Expression>)> = named_exprs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let mut fields = Vec::with_capacity(pairs.len());
        for (name, expr) in &pairs {
            let ty = infer_expression_type(expr.as_ref(), &input_schema)?;
            fields.push(StructField::nullable(name.clone(), ty));
        }
        let output_schema: SchemaRef = arc_struct_or_invariant(fields)?;
        push_node(
            &self.state,
            NodeKind::Project(ProjectNode {
                named_exprs: pairs,
                output_schema: Arc::clone(&output_schema),
            }),
            &[&self],
            output_schema,
        )
    }

    /// Append a [`NodeKind::Project`] with caller-supplied output schema. Positional:
    /// `exprs[i]` becomes `schema.fields()[i]`. No type inference -- the schema declares
    /// names and types directly. Use when both lists are produced together (e.g. physical
    /// to logical column mapping).
    pub fn project_with_schema<I, E>(self, exprs: I, schema: SchemaRef) -> Result<Self, DeltaError>
    where
        I: IntoIterator<Item = E>,
        E: Into<ExpressionRef>,
    {
        let exprs: Vec<ExpressionRef> = exprs.into_iter().map(Into::into).collect();
        let field_count = schema.fields().count();
        if exprs.len() != field_count {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "project_with_schema: expected {field_count} expressions, got {got}",
                got = exprs.len(),
            ));
        }
        let pairs: Vec<(String, Arc<Expression>)> = schema
            .fields()
            .map(|f| f.name().clone())
            .zip(exprs)
            .collect();
        push_node(
            &self.state,
            NodeKind::Project(ProjectNode {
                named_exprs: pairs,
                output_schema: Arc::clone(&schema),
            }),
            &[&self],
            schema,
        )
    }

    // === Schema-edit sugar (over project) ====================================

    /// Identity-by-name projection: every field in `schema` becomes `col(name)` against
    /// the input. Validates that each name exists in the input and that types match.
    pub fn select(self, schema: SchemaRef) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let pairs: Vec<(String, ExpressionRef)> = schema
            .fields()
            .map(|f| {
                let name = f.name();
                let input_field = input_schema.field(name).ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "select: field {name:?} not present in input schema",
                    )
                })?;
                if input_field.data_type() != f.data_type() {
                    return Err(delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "select: field {name:?} type mismatch -- input {input:?} vs target {target:?}",
                        input = input_field.data_type(),
                        target = f.data_type(),
                    ));
                }
                Ok(identity_named_expr(name.clone()))
            })
            .collect::<Result<_, DeltaError>>()?;
        push_node(
            &self.state,
            NodeKind::Project(ProjectNode {
                named_exprs: pairs,
                output_schema: Arc::clone(&schema),
            }),
            &[&self],
            schema,
        )
    }

    /// Append a caller-typed `(field, expr)` after the existing fields. Use when the
    /// expression's output type can't be derived by [narrow
    /// inference](crate::plans::ir::schema_inference) (e.g. a `CaseWhen` returning an
    /// `Array`, where inference returns an error and forces the caller into
    /// [`Self::project_with_schema`]). The supplied [`StructField`] declares both the
    /// new column's name and its full nullability + data type.
    pub fn append_col_typed(
        self,
        field: StructField,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let mut named_exprs = identity_named_exprs(&input_schema);
        let mut fields: Vec<StructField> = input_schema.fields().cloned().collect();
        named_exprs.push((field.name().clone(), expr.into()));
        fields.push(field);
        let output_schema: SchemaRef = arc_struct_or_invariant(fields)?;
        push_node(
            &self.state,
            NodeKind::Project(ProjectNode {
                named_exprs,
                output_schema: Arc::clone(&output_schema),
            }),
            &[&self],
            output_schema,
        )
    }

    /// Insert `leaf` immediately after `sibling`'s position in its parent struct.
    ///
    /// `sibling`'s prefix (all components except the last) names the parent struct;
    /// `leaf.name()` is the new column's name. Errors if `sibling` doesn't resolve, if
    /// the parent isn't a struct, or if `leaf.name()` collides with an existing sibling.
    /// `sibling` accepts any [`IntoColumnName`] value -- a `&str` for top-level fields,
    /// a tuple/array or [`column_name!`](crate::column_name) macro for nested paths.
    pub fn insert_col_after(
        self,
        sibling: impl IntoColumnName,
        leaf: StructField,
        expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let path = sibling.into_column_name();
        let expr = expr.into();
        let schema = self.schema()?;
        let (parent_path, sibling_leaf) = split_parent_leaf(&path, "insert_col_after")?;
        let op = FieldOp::InsertAfter {
            sibling_leaf,
            new_field: leaf,
            new_expr: expr,
        };
        apply_field_op(self, &schema, parent_path, op)
    }

    /// Replace the field at `path` with `new_field` (allowing rename + retype) and
    /// substitute `new_expr` in its place.
    ///
    /// The path's leaf and `new_field.name()` may differ (rename) or match (in-place
    /// retype). Errors if `path` doesn't resolve, if its parent isn't a struct, or if a
    /// rename would collide with an existing sibling. `path` accepts any
    /// [`IntoColumnName`] value.
    pub fn replace_col(
        self,
        path: impl IntoColumnName,
        new_field: StructField,
        new_expr: impl Into<ExpressionRef>,
    ) -> Result<Self, DeltaError> {
        let path = path.into_column_name();
        let new_expr = new_expr.into();
        let schema = self.schema()?;
        let (parent_path, target_leaf) = split_parent_leaf(&path, "replace_col")?;
        let op = FieldOp::Replace {
            target_leaf,
            new_field,
            new_expr,
        };
        apply_field_op(self, &schema, parent_path, op)
    }

    /// Remove the field at `path` from its parent struct.
    ///
    /// Errors if `path` doesn't resolve or if its parent isn't a struct. `path` accepts
    /// any [`IntoColumnName`] value -- a `&str` for top-level fields, a tuple/array or
    /// [`column_name!`](crate::column_name) macro for nested paths.
    pub fn drop_col(self, path: impl IntoColumnName) -> Result<Self, DeltaError> {
        let path = path.into_column_name();
        let schema = self.schema()?;
        let (parent_path, target_leaf) = split_parent_leaf(&path, "drop_col")?;
        let op = FieldOp::Drop { target_leaf };
        apply_field_op(self, &schema, parent_path, op)
    }

    // === Load / aggregate / join / union =====================================

    /// File-reader transform. Output schema is `spec.file_schema` plus a field per
    /// `passthrough_columns` entry (each field's type is taken from the input schema).
    pub fn load(self, spec: LoadSpec) -> Result<Self, DeltaError> {
        let input_schema = self.schema()?;
        let mut fields: Vec<StructField> = spec.file_schema.fields().cloned().collect();
        for col in &spec.passthrough_columns {
            let path = col.path();
            let leaf_name = path
                .last()
                .ok_or_else(|| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "load: passthrough column path is empty",
                    )
                })?
                .clone();
            let walk = input_schema
                .walk_column_fields(col)
                .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
            let leaf = walk.last().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "load: passthrough column resolved to empty path",
                )
            })?;
            fields.push(StructField::nullable(leaf_name, leaf.data_type().clone()));
        }
        let output_schema = arc_struct_or_invariant(fields)?;
        let LoadSpec {
            file_schema,
            file_type,
            base_url,
            passthrough_columns,
            file_meta,
            dv_ref,
        } = spec;
        push_node(
            &self.state,
            NodeKind::Load(LoadNode {
                file_schema,
                file_type,
                base_url,
                passthrough_columns,
                file_meta,
                dv_ref,
            }),
            &[&self],
            output_schema,
        )
    }

    /// Top-1-per-group aggregate ordered by `version` descending. The group-by expressions
    /// and the version column are aggregation-internal: the output projects only the
    /// listed `value_columns`, lifted from the input schema in declared order.
    ///
    /// The validator ensures every name in `value_columns` resolves in the input. Group-by
    /// expression types are inferred (and must therefore be supported by
    /// [`infer_expression_type`]) but do not contribute fields to the output -- they are
    /// validated for the lowering's benefit only.
    pub fn max_by_version<G, V, C, S>(
        self,
        group_by: G,
        version: V,
        value_columns: C,
    ) -> Result<Self, DeltaError>
    where
        G: IntoIterator,
        G::Item: Into<ExpressionRef>,
        V: Into<ExpressionRef>,
        C: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let group_by: Vec<ExpressionRef> = group_by.into_iter().map(Into::into).collect();
        let version_column: ExpressionRef = version.into();
        let value_columns: Vec<String> = value_columns.into_iter().map(Into::into).collect();
        let input_schema = self.schema()?;
        for expr in &group_by {
            // Validate group expressions resolve against the input schema. Inferred type
            // is discarded -- the output schema is derived solely from `value_columns`.
            infer_expression_type(expr.as_ref(), &input_schema)?;
        }
        let mut fields: Vec<StructField> = Vec::with_capacity(value_columns.len());
        for name in &value_columns {
            let f = input_schema.field(name).ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "max_by_version: value column {name:?} not in input schema",
                )
            })?;
            fields.push(f.clone());
        }
        let output_schema = arc_struct_or_invariant(fields)?;
        push_node(
            &self.state,
            NodeKind::MaxByVersion(MaxByVersionNode {
                group_by,
                version_column,
                value_columns,
            }),
            &[&self],
            output_schema,
        )
    }

    /// Left-anti join: rows of `self` whose key matches no row of `other`. Output schema
    /// mirrors `self`.
    pub fn left_anti_join<I, L, R>(
        self,
        other: PlanBuilder,
        key_pairs: I,
    ) -> Result<Self, DeltaError>
    where
        I: IntoIterator<Item = (L, R)>,
        L: Into<ExpressionRef>,
        R: Into<ExpressionRef>,
    {
        let key_pairs: Vec<(ExpressionRef, ExpressionRef)> = key_pairs
            .into_iter()
            .map(|(l, r)| (l.into(), r.into()))
            .collect();
        ensure_same_context(&self.state, &other)?;
        let output_schema = self.schema()?;
        push_node(
            &self.state,
            NodeKind::EquiJoin(EquiJoinNode {
                kind: JoinKind::LeftAnti,
                key_pairs,
            }),
            &[&self, &other],
            output_schema,
        )
    }

    /// Unordered union with one or more other cursors. All inputs must share the same
    /// schema (strict equality).
    pub fn union_all(self, others: &[PlanBuilder]) -> Result<Self, DeltaError> {
        push_union(&self.state, &self, others, /* ordered= */ false)
    }

    /// Order-preserving union with one or more other cursors. All inputs must share the
    /// same schema.
    pub fn union_ordered(self, others: &[PlanBuilder]) -> Result<Self, DeltaError> {
        push_union(&self.state, &self, others, /* ordered= */ true)
    }
}

// ============================================================================
// LoadSpec
// ============================================================================

/// Configuration for [`PlanBuilder::load`]. Mirrors the fields of [`NodeKind::Load`].
#[derive(Debug, Clone)]
pub struct LoadSpec {
    pub file_schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub passthrough_columns: Vec<ColumnName>,
    pub file_meta: ScanFileColumns,
    pub dv_ref: Option<DvRef>,
}

// ============================================================================
// Helpers
// ============================================================================

fn ensure_fresh(state: &ContextState, builder: &PlanBuilder) -> Result<(), DeltaError> {
    if state.session_id != builder.session_id {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "stale builder: builder session={cs} expected {ss}",
            cs = builder.session_id,
            ss = state.session_id,
        ));
    }
    Ok(())
}

fn ensure_same_context(
    state: &Rc<RefCell<ContextState>>,
    builder: &PlanBuilder,
) -> Result<(), DeltaError> {
    if !Rc::ptr_eq(state, &builder.state) {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "builder from a different context cannot be combined",
        ));
    }
    Ok(())
}

/// Validate the input builders against `state`, append `kind` (with the inputs' Refs) to
/// the in-flight plan, record `schema` for the freshly minted Ref, and return a handle
/// to it. Source nodes pass `inputs: &[]`, unary `&[self]`, binary `&[self, other]`,
/// and union the full input list.
fn push_node(
    state: &Rc<RefCell<ContextState>>,
    kind: NodeKind,
    inputs: &[&PlanBuilder],
    schema: SchemaRef,
) -> Result<PlanBuilder, DeltaError> {
    for b in inputs {
        ensure_same_context(state, b)?;
    }
    let mut s = state.borrow_mut();
    for b in inputs {
        ensure_fresh(&s, b)?;
    }
    let input_refs = inputs.iter().map(|b| b.ref_id).collect();
    let r = s.push_node(kind, input_refs, schema);
    Ok(PlanBuilder::new(state, r, s.session_id))
}

fn push_union(
    state: &Rc<RefCell<ContextState>>,
    first: &PlanBuilder,
    others: &[PlanBuilder],
    ordered: bool,
) -> Result<PlanBuilder, DeltaError> {
    let first_schema = first.schema()?;
    for o in others {
        if o.schema()? != first_schema {
            return Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "union: input schemas disagree",
            ));
        }
    }
    let mut inputs: Vec<&PlanBuilder> = Vec::with_capacity(1 + others.len());
    inputs.push(first);
    inputs.extend(others.iter());
    push_node(
        state,
        NodeKind::Union(UnionNode { ordered }),
        &inputs,
        first_schema,
    )
}

fn arc_struct_or_invariant(fields: Vec<StructField>) -> Result<SchemaRef, DeltaError> {
    StructType::try_new(fields)
        .map(Arc::new)
        .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)
}

/// `(name, col(name))` -- one identity entry for a top-level projection list.
fn identity_named_expr(name: impl Into<String>) -> (String, ExpressionRef) {
    let name = name.into();
    let expr = Arc::new(Expression::column([&name]));
    (name, expr)
}

/// Identity projection list: every field becomes `(name, col(name))`.
fn identity_named_exprs(schema: &StructType) -> Vec<(String, ExpressionRef)> {
    schema
        .fields()
        .map(|f| identity_named_expr(f.name().clone()))
        .collect()
}

// === Point-edit primitives over nested paths =================================

/// Result of rebuilding a struct under a [`FieldOp`]: the per-field projection list
/// (output name + expression) plus the matching output [`StructField`]s in declaration
/// order. Pairs `0..n` of one mirror pairs `0..n` of the other.
type RewrittenStruct = (Vec<(String, ExpressionRef)>, Vec<StructField>);

/// Field-level edit applied to the parent struct identified by a path prefix.
enum FieldOp {
    InsertAfter {
        sibling_leaf: String,
        new_field: StructField,
        new_expr: ExpressionRef,
    },
    Replace {
        target_leaf: String,
        new_field: StructField,
        new_expr: ExpressionRef,
    },
    Drop {
        target_leaf: String,
    },
}

/// Split a path into `(parent_path, leaf)`. The leaf is the last component; the parent
/// path is empty for top-level fields. Errors if the path is empty.
fn split_parent_leaf<'a>(
    path: &'a ColumnName,
    op_name: &'static str,
) -> Result<(&'a [String], String), DeltaError> {
    let components = path.path();
    let (leaf, parent) = components.split_last().ok_or_else(|| {
        delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "{op_name}: path is empty",
        )
    })?;
    Ok((parent, leaf.clone()))
}

/// Apply a field-level edit at the struct identified by `parent_path` within
/// `input_schema`, then push a top-level [`NodeKind::Project`] that materializes the result.
///
/// Walks the schema down `parent_path`, identity-projecting siblings via `col(...)`
/// references, then applies `op` at the targeted struct. Each ancestor struct above the
/// edit is rebuilt with [`Expression::struct_from`].
fn apply_field_op(
    builder: PlanBuilder,
    input_schema: &StructType,
    parent_path: &[String],
    op: FieldOp,
) -> Result<PlanBuilder, DeltaError> {
    let (named_exprs, fields) = rewrite_at(input_schema, &[], parent_path, &op)?;
    let output_schema = arc_struct_or_invariant(fields)?;
    push_node(
        &builder.state,
        NodeKind::Project(ProjectNode {
            named_exprs,
            output_schema: Arc::clone(&output_schema),
        }),
        &[&builder],
        output_schema,
    )
}

/// Validate that `op` can be applied at `schema`, the parent struct identified by
/// `path_so_far`. All existence and collision checks live here, separated from the
/// per-field iteration in `rewrite_at`.
fn validate_op(
    schema: &StructType,
    path_so_far: &[String],
    op: &FieldOp,
) -> Result<(), DeltaError> {
    match op {
        FieldOp::InsertAfter {
            sibling_leaf,
            new_field,
            ..
        } => {
            if schema.field(sibling_leaf).is_none() {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "insert_col_after: sibling field {sibling_leaf:?} not found at path \
                     {path_so_far:?}",
                ));
            }
            if schema.field(new_field.name()).is_some() {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "insert_col_after: field {name:?} already exists at path {path_so_far:?}",
                    name = new_field.name(),
                ));
            }
        }
        FieldOp::Replace {
            target_leaf,
            new_field,
            ..
        } => {
            if schema.field(target_leaf).is_none() {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "replace_col: field {target_leaf:?} not found at path {path_so_far:?}",
                ));
            }
            if target_leaf != new_field.name() && schema.field(new_field.name()).is_some() {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "replace_col: rename target {name:?} already exists at path {path_so_far:?}",
                    name = new_field.name(),
                ));
            }
        }
        FieldOp::Drop { target_leaf } => {
            if schema.field(target_leaf).is_none() {
                return Err(delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "drop_col: field {target_leaf:?} not found at path {path_so_far:?}",
                ));
            }
        }
    }
    Ok(())
}

/// Walk `schema` along `remaining`, rebuilding each ancestor struct on the way down and
/// applying `op` once `remaining` is exhausted (the empty-`remaining` boundary identifies
/// the parent struct the op acts on).
///
/// Off-path siblings are identity-projected via `col(...)`; the on-path child is
/// recursively rebuilt and re-emitted via `Expression::struct_from`. At the leaf level
/// (`remaining.is_empty()`), per-field op semantics apply: `Drop` skips the target,
/// `Replace` swaps it, `InsertAfter` pushes the new field after its sibling.
fn rewrite_at(
    schema: &StructType,
    path_so_far: &[String],
    remaining: &[String],
    op: &FieldOp,
) -> Result<RewrittenStruct, DeltaError> {
    if remaining.is_empty() {
        validate_op(schema, path_so_far, op)?;
    } else if schema.field(&remaining[0]).is_none() {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "rewrite_at: field {target:?} not found in schema at path {path_so_far:?}",
            target = remaining[0],
        ));
    }
    let cap = schema.fields().count() + 1;
    let mut named_exprs: Vec<(String, ExpressionRef)> = Vec::with_capacity(cap);
    let mut fields: Vec<StructField> = Vec::with_capacity(cap);
    for f in schema.fields() {
        let fname = f.name();
        // Recursive case: this field is on the path -- recurse and re-emit.
        if let Some((target, rest)) = remaining.split_first() {
            if fname == target {
                let sub = match f.data_type() {
                    DataType::Struct(s) => s.as_ref(),
                    other => {
                        return Err(delta_error!(
                            DeltaErrorCode::DeltaCommandInvariantViolation,
                            "rewrite_at: field {fname:?} is not a struct (found {other:?})",
                        ));
                    }
                };
                let mut sub_path = path_so_far.to_vec();
                sub_path.push(fname.clone());
                let (sub_named, sub_fields) = rewrite_at(sub, &sub_path, rest, op)?;
                let sub_struct = StructType::try_new(sub_fields.clone())
                    .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
                fields.push(StructField::new(
                    fname.clone(),
                    DataType::Struct(Box::new(sub_struct)),
                    f.is_nullable(),
                ));
                let exprs: Vec<ExpressionRef> = sub_named.into_iter().map(|(_, e)| e).collect();
                named_exprs.push((fname.clone(), Arc::new(Expression::struct_from(exprs))));
                continue;
            }
        } else {
            // Leaf case: per-field op semantics.
            match op {
                FieldOp::Drop { target_leaf } if fname == target_leaf => continue,
                FieldOp::Replace {
                    target_leaf,
                    new_field,
                    new_expr,
                } if fname == target_leaf => {
                    named_exprs.push((new_field.name().clone(), Arc::clone(new_expr)));
                    fields.push(new_field.clone());
                    continue;
                }
                _ => {}
            }
        }
        // Identity-project this field (off-path sibling, or leaf non-target).
        named_exprs.push((fname.clone(), col_ref_at(path_so_far, fname)));
        fields.push(f.clone());
        // At the leaf level, InsertAfter emits the new field immediately after its sibling.
        if remaining.is_empty() {
            if let FieldOp::InsertAfter {
                sibling_leaf,
                new_field,
                new_expr,
            } = op
            {
                if fname == sibling_leaf {
                    named_exprs.push((new_field.name().clone(), Arc::clone(new_expr)));
                    fields.push(new_field.clone());
                }
            }
        }
    }
    Ok((named_exprs, fields))
}

/// Build a `col(...)` reference to `leaf` within the struct rooted at `prefix`. When
/// `prefix` is empty this is a top-level column reference.
fn col_ref_at(prefix: &[String], leaf: &str) -> ExpressionRef {
    let mut path: Vec<String> = Vec::with_capacity(prefix.len() + 1);
    path.extend(prefix.iter().cloned());
    path.push(leaf.to_string());
    Arc::new(Expression::column(path))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;
    use crate::expressions::{col, Predicate};
    use crate::plans::ir::plan::JoinKind;
    use crate::plans::kernel_reducers::{
        FinishedHandle, KdfControl, KernelReducerKind, KernelReducerOutput,
    };
    use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
    use crate::plans::state_machines::framework::state_machine::{NextStep, StateMachine};
    use crate::plans::state_machines::framework::step_payload::EngineResponse;
    use crate::schema::DataType;
    use crate::DeltaResult;

    fn id_ts_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::STRING),
                StructField::nullable("ts", DataType::LONG),
            ])
            .unwrap(),
        )
    }

    /// Sources mint sequential Refs with the declared output schema.
    #[test]
    fn sources_mint_refs_and_record_schemas() {
        let ctx = Context::new();
        let v = ctx.values(id_ts_schema(), vec![]).unwrap();
        let j = ctx.scan_json(vec![], id_ts_schema()).unwrap();
        let p = ctx.scan_parquet(vec![], id_ts_schema()).unwrap();
        assert_eq!(v.ref_id(), Ref(0));
        assert_eq!(j.ref_id(), Ref(1));
        assert_eq!(p.ref_id(), Ref(2));
        assert_eq!(*v.schema().unwrap(), *id_ts_schema());
    }

    /// `into_result_plan` runs DCE and returns ResultPlan with the surviving stmts.
    #[test]
    fn into_result_plan_runs_dce() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let dead = ctx.values(id_ts_schema(), vec![]).unwrap(); // unreachable
        let kept = src.filter(Arc::new(col("id").is_not_null())).unwrap();
        // Drop the dead branch handle so try_unwrap works and DCE removes it.
        drop(dead);
        let result_plan = ctx.into_result_plan(kept).unwrap();
        let outputs: Vec<u32> = result_plan.plan.stmts.iter().map(|s| s.output.0).collect();
        assert_eq!(outputs, vec![0, 2]); // src=0 + filter=2; dead values=1 dropped
        assert_eq!(result_plan.result, Ref(2));
    }

    /// Filter passes through schema unchanged.
    #[test]
    fn filter_preserves_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let f = src
            .filter(Arc::new(Predicate::BooleanExpression(col("id"))))
            .unwrap();
        assert_eq!(*f.schema().unwrap(), *id_ts_schema());
    }

    /// `project` derives output schema from inferred expression types.
    #[test]
    fn project_infers_output_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let projected = src
            .project([
                ("id_out", Arc::new(col("id")) as ExpressionRef),
                ("ts_out", Arc::new(col("ts")) as ExpressionRef),
            ])
            .unwrap();
        let schema = projected.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id_out", "ts_out"]);
        assert_eq!(
            schema.field("id_out").unwrap().data_type(),
            &DataType::STRING
        );
        assert_eq!(schema.field("ts_out").unwrap().data_type(), &DataType::LONG);
    }

    /// `project_with_schema` honors the caller-supplied output schema and reports a
    /// length mismatch when exprs vs fields disagree.
    #[test]
    fn project_with_schema_validates_length_and_uses_caller_schema() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();

        let target = Arc::new(
            StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap(),
        );
        let exprs: Vec<ExpressionRef> = vec![Arc::new(col("id"))];
        let p = src
            .clone()
            .project_with_schema(exprs, Arc::clone(&target))
            .unwrap();
        assert_eq!(*p.schema().unwrap(), *target);

        let bad: Vec<ExpressionRef> = vec![Arc::new(col("id")), Arc::new(col("ts"))];
        let err = src.project_with_schema(bad, target).unwrap_err();
        assert!(
            err.to_string().contains("expected 1 expressions"),
            "got: {err}"
        );
    }

    // === Point-edit primitives over nested paths ============================

    /// Schema with an `add` struct, used to exercise nested point-edits.
    fn nested_add_schema() -> SchemaRef {
        let add = StructType::try_new(vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("stats", DataType::STRING),
        ])
        .unwrap();
        Arc::new(
            StructType::try_new(vec![
                StructField::nullable("commitInfo", DataType::STRING),
                StructField::nullable("add", DataType::Struct(Box::new(add))),
                StructField::nullable("remove", DataType::STRING),
            ])
            .unwrap(),
        )
    }

    /// Top-level `insert_col_after` keeps existing fields and appends the new leaf
    /// immediately after the sibling.
    #[test]
    fn insert_col_after_top_level_inserts_after_sibling() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let out = src
            .insert_col_after(
                "id",
                StructField::nullable("mid", DataType::LONG),
                col("ts"),
            )
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "mid", "ts"]);
        assert_eq!(schema.field("mid").unwrap().data_type(), &DataType::LONG);
    }

    /// `insert_col_after` walks into a nested struct and inserts `add.stats_parsed`
    /// after `add.stats`.
    #[test]
    fn insert_col_after_nested_inserts_in_parent_struct() {
        let ctx = Context::new();
        let src = ctx.values(nested_add_schema(), vec![]).unwrap();
        let stats_struct =
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap();
        let parsed_field = StructField::nullable(
            "stats_parsed",
            DataType::Struct(Box::new(stats_struct.clone())),
        );
        let out = src
            .insert_col_after(["add", "stats"], parsed_field, col(["add", "stats"]))
            .unwrap();
        let schema = out.schema().unwrap();
        let add = match schema.field("add").unwrap().data_type() {
            DataType::Struct(s) => s.as_ref(),
            other => panic!("expected struct, got {other:?}"),
        };
        let names: Vec<&str> = add.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["path", "stats", "stats_parsed"]);
        assert_eq!(
            add.field("stats_parsed").unwrap().data_type(),
            &DataType::Struct(Box::new(stats_struct)),
        );
    }

    /// `insert_col_after` errors when the sibling does not exist at the requested path.
    #[test]
    fn insert_col_after_rejects_missing_sibling() {
        let ctx = Context::new();
        let src = ctx.values(nested_add_schema(), vec![]).unwrap();
        let err = src
            .insert_col_after(
                ["add", "missing"],
                StructField::nullable("x", DataType::LONG),
                col(["add", "stats"]),
            )
            .unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    /// `insert_col_after` errors when the new leaf collides with an existing sibling.
    #[test]
    fn insert_col_after_rejects_name_collision() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let err = src
            .insert_col_after("id", StructField::nullable("ts", DataType::LONG), col("id"))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    /// `replace_col` retypes a field in place when the new field's name matches.
    #[test]
    fn replace_col_in_place_retypes_field() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let out = src
            .replace_col(
                "ts",
                StructField::nullable("ts", DataType::STRING),
                col("id"),
            )
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "ts"]);
        assert_eq!(schema.field("ts").unwrap().data_type(), &DataType::STRING);
    }

    /// `replace_col` with a nested path renames + retypes a child field, walking the
    /// parent struct with `col(...)` references for unchanged siblings.
    #[test]
    fn replace_col_nested_renames_and_retypes() {
        let ctx = Context::new();
        let src = ctx.values(nested_add_schema(), vec![]).unwrap();
        let stats_struct =
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap();
        let new_field = StructField::nullable(
            "stats_parsed",
            DataType::Struct(Box::new(stats_struct.clone())),
        );
        let out = src
            .replace_col(["add", "stats"], new_field, col(["add", "stats"]))
            .unwrap();
        let schema = out.schema().unwrap();
        let add = match schema.field("add").unwrap().data_type() {
            DataType::Struct(s) => s.as_ref(),
            other => panic!("expected struct, got {other:?}"),
        };
        let names: Vec<&str> = add.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["path", "stats_parsed"]);
        assert_eq!(
            add.field("stats_parsed").unwrap().data_type(),
            &DataType::Struct(Box::new(stats_struct)),
        );
    }

    /// `replace_col` errors when the path does not resolve.
    #[test]
    fn replace_col_rejects_missing_path() {
        let ctx = Context::new();
        let src = ctx.values(nested_add_schema(), vec![]).unwrap();
        let err = src
            .replace_col(
                ["add", "missing"],
                StructField::nullable("missing", DataType::STRING),
                col(["add", "path"]),
            )
            .unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    /// `drop_col` removes a top-level field.
    #[test]
    fn drop_col_top_level_removes_field() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let out = src.drop_col("ts").unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id"]);
    }

    /// `drop_col` errors when the path does not resolve.
    #[test]
    fn drop_col_rejects_missing_path() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let err = src.drop_col("missing").unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    /// `select` rejects target fields not in the input schema.
    #[test]
    fn select_rejects_missing_field() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let target = Arc::new(
            StructType::try_new(vec![StructField::nullable("missing", DataType::STRING)]).unwrap(),
        );
        let err = src.select(target).unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    /// `left_anti_join` output schema mirrors the left input.
    #[test]
    fn left_anti_join_output_mirrors_left() {
        let ctx = Context::new();
        let l = ctx.values(id_ts_schema(), vec![]).unwrap();
        let r_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("rid", DataType::STRING),
                StructField::nullable("rts", DataType::LONG),
            ])
            .unwrap(),
        );
        let r = ctx.values(r_schema, vec![]).unwrap();

        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("rid")))];

        let anti = l.left_anti_join(r, key).unwrap();
        assert_eq!(*anti.schema().unwrap(), *id_ts_schema());
    }

    /// `union_all` validates all input schemas match.
    #[test]
    fn union_all_requires_matching_schemas() {
        let ctx = Context::new();
        let a = ctx.values(id_ts_schema(), vec![]).unwrap();
        let b = ctx.values(id_ts_schema(), vec![]).unwrap();
        let unioned = a.clone().union_all(&[b]).unwrap();
        assert_eq!(*unioned.schema().unwrap(), *id_ts_schema());

        let other_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("x", DataType::STRING)]).unwrap(),
        );
        let bad = ctx.values(other_schema, vec![]).unwrap();
        let err = a.union_all(&[bad]).unwrap_err();
        assert!(err.to_string().contains("disagree"), "got: {err}");
    }

    /// Cursors from different contexts cannot be combined.
    #[test]
    fn cross_context_cursors_rejected() {
        let ctx_a = Context::new();
        let ctx_b = Context::new();
        let a = ctx_a.values(id_ts_schema(), vec![]).unwrap();
        let b = ctx_b.values(id_ts_schema(), vec![]).unwrap();
        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("id")))];
        let err = a.left_anti_join(b, key).unwrap_err();
        assert!(err.to_string().contains("different context"), "got: {err}");
    }

    /// `into_result_plan` fails when other cursors still hold the state Rc.
    #[test]
    fn into_result_plan_fails_with_outstanding_cursor() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let _other = ctx.values(id_ts_schema(), vec![]).unwrap();
        let err = ctx.into_result_plan(src).unwrap_err();
        assert!(
            err.to_string().contains("outstanding PlanBuilder"),
            "got: {err}",
        );
    }

    /// `max_by_version` output schema is exactly `value_columns` lifted from the input
    /// (group_by exprs and version are aggregation-internal and not surfaced).
    #[test]
    fn max_by_version_output_schema_is_value_columns_only() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let out = src
            .max_by_version(
                vec![Arc::new(col("id"))],
                Arc::new(col("ts")),
                vec!["ts".to_string()],
            )
            .unwrap();
        let schema = out.schema().unwrap();
        let names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["ts"]);
        assert_eq!(schema.field("ts").unwrap().data_type(), &DataType::LONG);
    }

    /// `max_by_version` rejects value columns that are not present in the input schema.
    #[test]
    fn max_by_version_rejects_unknown_value_column() {
        let ctx = Context::new();
        let src = ctx.values(id_ts_schema(), vec![]).unwrap();
        let err = src
            .max_by_version(
                vec![Arc::new(col("id"))],
                Arc::new(col("ts")),
                vec!["missing".to_string()],
            )
            .unwrap_err();
        assert!(err.to_string().contains("missing"), "got: {err}");
    }

    // === CoroutineSM dispatch tests ============================================

    /// Test KernelReducer that echoes its constructor input as the output value.
    #[derive(Debug, Clone)]
    struct EchoReducer {
        value: i64,
    }

    impl KernelReducer for EchoReducer {
        fn kind(&self) -> KernelReducerKind {
            KernelReducerKind::CheckpointHint
        }
        fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
            Box::new(*self)
        }
        fn apply(&mut self, _batch: &dyn crate::EngineData) -> DeltaResult<KdfControl> {
            Ok(KdfControl::Break)
        }
    }

    impl KernelReducerOutput for EchoReducer {
        type Output = i64;
        fn into_output(self) -> Result<i64, DeltaError> {
            Ok(self.value)
        }
    }

    /// `reduce` yields a `EngineRequest::Reduce` with the DCE'd stmts and the builder's terminal
    /// Ref, the engine resumes with the reducer's finished handle, and the SM body
    /// recovers the typed output.
    #[test]
    fn reduce_yields_step_reduce_and_recovers_typed_output() {
        let mut sm = CoroutineSM::<i64>::new("test", |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let _dead = ctx.values(id_ts_schema(), vec![]).unwrap();
            let builder = ctx
                .values(id_ts_schema(), vec![])
                .unwrap()
                .filter(Arc::new(col("id").is_not_null()))
                .unwrap();
            ctx.reduce(&mut engine, builder, EchoReducer { value: 7 }, "drain")
                .await
        })
        .unwrap();

        // First yielded step is the Reduce.
        assert_eq!(sm.step_name(), "drain");
        let token = match sm.get_step().unwrap() {
            EngineRequest::Reduce {
                stmts,
                terminal,
                sink,
            } => {
                // DCE: only `values(reachable)` and `filter` remain; `_dead` is dropped.
                let outputs: Vec<u32> = stmts.iter().map(|s| s.output.0).collect();
                assert_eq!(outputs, vec![1, 2]);
                assert_eq!(terminal, Ref(2));
                sink.token.clone()
            }
            other => panic!("expected EngineRequest::Reduce, got {other:?}"),
        };

        // Engine drains the sink and resumes with a finished handle.
        let payload = EngineResponse::Reducer(FinishedHandle {
            token,
            erased: Box::new(EchoReducer { value: 7 }),
        });
        let next = sm.submit(Ok(payload)).unwrap();
        match next {
            NextStep::Done(v) => assert_eq!(v, 7),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }

    /// `Context::reduce` resets the Ref counter so the next source minted after
    /// the reduce boundary starts at `Ref(0)` again. The full plan ships to the
    /// engine and the session-id bump invalidates any held cursors, so reusing
    /// Ref ids across the boundary is safe and keeps post-reduce Refs compact.
    #[test]
    fn reduce_resets_ref_counter() {
        let mut sm = CoroutineSM::<u32>::new("test", |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let v0 = ctx.values(id_ts_schema(), vec![]).unwrap();
            let v1 = v0.filter(Arc::new(col("id").is_not_null())).unwrap();
            assert_eq!(v1.ref_id(), Ref(1));
            let _: i64 = ctx
                .reduce(&mut engine, v1, EchoReducer { value: 0 }, "drain")
                .await?;
            let post = ctx.values(id_ts_schema(), vec![]).unwrap();
            Ok(post.ref_id().0)
        })
        .unwrap();

        let token = match sm.get_step().unwrap() {
            EngineRequest::Reduce { sink, .. } => sink.token.clone(),
            other => panic!("expected EngineRequest::Reduce, got {other:?}"),
        };
        let payload = EngineResponse::Reducer(FinishedHandle {
            token,
            erased: Box::new(EchoReducer { value: 0 }),
        });
        match sm.submit(Ok(payload)).unwrap() {
            NextStep::Done(v) => assert_eq!(v, 0, "post-reduce Ref must be 0"),
            other => panic!("expected Done, got {other:?}"),
        }
    }

    /// Resuming a `EngineRequest::Reduce` with a non-Reducer payload surfaces an internal
    /// error -- the executor produced a payload that doesn't match the yielded step.
    #[test]
    fn reduce_resume_with_wrong_payload_variant_errors() {
        let mut sm = CoroutineSM::<i64>::new("test", |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let builder = ctx.values(id_ts_schema(), vec![]).unwrap();
            ctx.reduce(&mut engine, builder, EchoReducer { value: 1 }, "drain")
                .await
        })
        .unwrap();
        let _ = sm.get_step().unwrap();
        let err = sm
            .submit(Ok(EngineResponse::Schema(id_ts_schema())))
            .unwrap_err();
        assert!(
            err.to_string().contains("non-Reducer payload"),
            "got: {err}",
        );
    }

    /// `schema_query` yields a `EngineRequest::SchemaQuery`, the engine resumes with a schema,
    /// and the SM body receives it.
    #[test]
    fn schema_query_yields_step_schemaquery_and_returns_schema() {
        let target_schema = id_ts_schema();
        let target_clone = Arc::clone(&target_schema);
        let mut sm = CoroutineSM::<bool>::new("test", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let s = ctx
                .schema_query(&mut engine, "/cp.parquet", "footer")
                .await?;
            Ok(s == target_clone)
        })
        .unwrap();

        assert_eq!(sm.step_name(), "footer");
        match sm.get_step().unwrap() {
            EngineRequest::SchemaQuery(node) => assert_eq!(node.file_path, "/cp.parquet"),
            other => panic!("expected SchemaQuery, got {other:?}"),
        }

        match sm
            .submit(Ok(EngineResponse::Schema(target_schema)))
            .unwrap()
        {
            NextStep::Done(v) => assert!(v),
            _ => panic!("expected Done(true)"),
        }
    }

    /// Resuming a `EngineRequest::SchemaQuery` with a non-Schema payload surfaces an internal
    /// error -- the executor produced a payload that doesn't match the yielded step.
    #[test]
    fn schema_query_resume_with_wrong_payload_variant_errors() {
        let mut sm = CoroutineSM::<()>::new("test", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let _ = ctx
                .schema_query(&mut engine, "/x.parquet", "footer")
                .await?;
            Ok(())
        })
        .unwrap();
        let _ = sm.get_step().unwrap();
        let err = sm.submit(Ok(EngineResponse::Empty)).unwrap_err();
        assert!(err.to_string().contains("non-Schema payload"), "got: {err}",);
    }

    /// After `schema_query` bumps the session, cursors built before the dispatch are
    /// stale and operations on them fail.
    #[test]
    fn schema_query_invalidates_pre_dispatch_cursors() {
        let target_schema = id_ts_schema();
        let mut sm = CoroutineSM::<()>::new("test", move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let pre = ctx.values(id_ts_schema(), vec![]).unwrap();
            let _ = ctx
                .schema_query(&mut engine, "/x.parquet", "footer")
                .await?;
            // Using `pre` after dispatch must fail with a stale-builder error.
            let err = pre
                .filter(Arc::new(Predicate::BooleanExpression(col("id"))))
                .unwrap_err();
            assert!(err.to_string().contains("stale builder"), "got: {err}");
            Ok(())
        })
        .unwrap();

        // Drive through the schema_query yield.
        let _ = sm.get_step().unwrap();
        match sm
            .submit(Ok(EngineResponse::Schema(target_schema)))
            .unwrap()
        {
            NextStep::Done(()) => {}
            _ => panic!("expected Done"),
        }
    }

    /// Plan stmt for `left_anti_join` records `[left, right]` as inputs in that order.
    #[test]
    fn left_anti_join_records_left_right_inputs() {
        let ctx = Context::new();
        let left = ctx.values(id_ts_schema(), vec![]).unwrap();
        let right_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("rid", DataType::STRING),
                StructField::nullable("rts", DataType::LONG),
            ])
            .unwrap(),
        );
        let right = ctx.values(right_schema, vec![]).unwrap();
        let left_ref = left.ref_id();
        let right_ref = right.ref_id();
        let key: Vec<(ExpressionRef, ExpressionRef)> =
            vec![(Arc::new(col("id")), Arc::new(col("rid")))];
        let joined = left.left_anti_join(right, key).unwrap();
        let joined_ref = joined.ref_id();
        let plan = ctx.into_result_plan(joined).unwrap();
        let stmt = plan.plan.node(joined_ref).unwrap();
        assert_eq!(stmt.inputs, vec![left_ref, right_ref]);
        assert!(matches!(
            stmt.kind,
            NodeKind::EquiJoin(EquiJoinNode {
                kind: JoinKind::LeftAnti,
                ..
            })
        ));
    }
}
