//! Kernel [`Plan`] -> DataFusion [`LogicalPlan`] lowering.
//!
//! Topological walk over [`PlanNode`]s. Each statement's output [`Ref`] is mapped to a freshly
//! built [`LogicalPlan`]. Inputs are guaranteed to be earlier in `plan.stmts` than outputs by
//! [`Plan::push`], so a single forward pass works.
//!
//! Every [`NodeKind`] variant wraps a payload struct defined in the kernel IR nodes module;
//! engine helpers consume those payload structs by reference (`&LoadNode`, `&ScanParquetNode`,
//! etc.) without repacking. Cross-statement data flow happens entirely through DataFusion's
//! logical-plan tree (no relation registry, no named handles).
//!
//! [`Plan`]: delta_kernel::plans::ir::plan::Plan
//! [`PlanNode`]: PlanNode
//! [`Ref`]: Ref
//! [`LogicalPlan`]: LogicalPlan
//! [`Plan::push`]: delta_kernel::plans::ir::plan::Plan::push
//! [`NodeKind`]: NodeKind
//!
//! # Schema policy
//!
//! Kernel `Plan`s do not carry per-Ref kernel schemas (those live on the plan builder only
//! during IR construction). DataFusion derives output schemas from `LogicalPlan` shape and
//! arrow types; the only place a kernel [`SchemaRef`] is reconstructed engine-side is
//! [`NodeKind::Load`], whose output schema is computed from the upstream's arrow shape via
//! [`StructType::try_from_arrow`] and threaded into [`LoadTableProvider::try_new`].
//!
//! [`NodeKind::Load`]: NodeKind::Load
//! [`SchemaRef`]: SchemaRef
//! [`StructType::try_from_arrow`]: delta_kernel::engine::arrow_conversion::TryFromArrow
//! [`LoadTableProvider`]: crate::exec::LoadTableProvider

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan, Values};
use datafusion_expr::{lit, Expr, ExprFunctionExt, JoinType as DfJoinType, LogicalPlanBuilder};
use datafusion_functions_window::row_number::row_number;
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::expressions::Expression;
use delta_kernel::plans::ir::nodes::{
    EquiJoinNode, LoadNode, MaxByVersionNode, UnionNode, ValuesNode,
};
use delta_kernel::plans::ir::plan::{JoinKind, NodeKind, PlanNode, Ref};
use delta_kernel::plans::schema_expr::field_op::load_output_schema;
use delta_kernel::schema::{ColumnMetadataKey, DataType, StructField, StructType};

use super::ordered_union::compile_ordered_union;
use super::project::compile_project_node;
use super::providers::file_listing_to_logical_plan;
use super::scan::{scan_json_to_logical_plan, scan_parquet_to_logical_plan};
use crate::compile::expr_translator::{
    kernel_expr_to_df_untyped, kernel_exprs_to_df_untyped, kernel_pred_to_df,
};
use crate::compile::CompileContext;
use crate::error::plan_compilation;
use crate::exec::LoadTableProvider;

/// Compile a slice of [`PlanNode`]s to a DataFusion [`LogicalPlan`] rooted at `terminal`.
///
/// Walks `stmts` in order, lowering each statement and threading the resulting `LogicalPlan`
/// into a `Ref`-keyed map. The plan returned for `terminal` is then handed back. Statements
/// unreachable from `terminal` are still compiled (DCE is the builder's job, not the engine's);
/// engines relying on dead-code elimination should call [`Plan::reachable_from`] before passing
/// the stmts in. Taking `&[PlanNode]` rather than `&Plan` avoids needing a `Plan::from_stmts`
/// constructor; both the [`ResultPlan`]-returning drive path (where the caller already has a
/// `Plan`) and the [`EngineRequest::Reduce`] dispatch (where the executor only sees raw stmts)
/// share this entry point.
///
/// [`Plan::reachable_from`]: delta_kernel::plans::ir::plan::Plan::reachable_from
/// [`ResultPlan`]: delta_kernel::plans::ir::plan::ResultPlan
/// [`EngineRequest::Reduce`]: delta_kernel::plans::state_machines::framework::state_machine::EngineRequest::Reduce
pub fn compile_plan(
    stmts: &[PlanNode],
    terminal: Ref,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let mut built: HashMap<Ref, LogicalPlan> = HashMap::with_capacity(stmts.len());
    for stmt in stmts {
        let logical = lower_stmt(stmt, &built, ctx)?;
        built.insert(stmt.output, logical);
    }
    built.remove(&terminal).ok_or_else(|| {
        plan_compilation(format!(
            "compile_plan: terminal {terminal:?} is not produced by any stmt in the plan",
        ))
    })
}

/// Look up a compiled child plan; the caller clones for ownership.
fn lookup(built: &HashMap<Ref, LogicalPlan>, r: Ref) -> Result<&LogicalPlan, DataFusionError> {
    built.get(&r).ok_or_else(|| {
        plan_compilation(format!(
            "compile_plan: input {r:?} not compiled (out-of-order stmts?)",
        ))
    })
}

fn lower_stmt(
    stmt: &PlanNode,
    built: &HashMap<Ref, LogicalPlan>,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    match &stmt.kind {
        // === Sources ====================================================================
        NodeKind::ListFiles(node) => file_listing_to_logical_plan(node),
        NodeKind::ScanParquet(node) => scan_parquet_to_logical_plan(node),
        NodeKind::ScanJson(node) => scan_json_to_logical_plan(node),
        NodeKind::Values(node) => lower_values(node),

        // === Unary transforms ===========================================================
        NodeKind::Filter(node) => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            let pred = kernel_pred_to_df(node.predicate.as_ref())?;
            LogicalPlanBuilder::from(child).filter(pred)?.build()
        }
        NodeKind::Project(node) => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            compile_project_node(child, node)
        }
        NodeKind::Load(node) => lower_load(built, expect_one_input(stmt)?, node, ctx),
        NodeKind::MaxByVersion(node) => {
            let child = lookup(built, expect_one_input(stmt)?)?.clone();
            lower_max_by_version(child, node)
        }

        // === N-ary ======================================================================
        NodeKind::Union(node) => lower_union(stmt, built, node),
        NodeKind::EquiJoin(node) => lower_equi_join(stmt, built, node),
    }
}

fn lower_union(
    stmt: &PlanNode,
    built: &HashMap<Ref, LogicalPlan>,
    node: &UnionNode,
) -> Result<LogicalPlan, DataFusionError> {
    if stmt.inputs.is_empty() {
        return Err(plan_compilation(
            "compile_plan: Union with zero inputs is not a valid plan shape",
        ));
    }
    let children: Vec<LogicalPlan> = stmt
        .inputs
        .iter()
        .map(|r| lookup(built, *r).cloned())
        .collect::<Result<_, _>>()?;
    if children.len() == 1 {
        return children
            .into_iter()
            .next()
            .ok_or_else(|| plan_compilation("compile_plan: internal: Union lost children"));
    }
    if node.ordered {
        compile_ordered_union(children)
    } else {
        let mut iter = children.into_iter();
        let first = iter
            .next()
            .ok_or_else(|| plan_compilation("compile_plan: internal: Union lost children"))?;
        iter.try_fold(first, |acc, right| {
            LogicalPlanBuilder::from(acc).union(right)?.build()
        })
    }
}

fn expect_one_input(stmt: &PlanNode) -> Result<Ref, DataFusionError> {
    match stmt.inputs.as_slice() {
        [r] => Ok(*r),
        other => Err(plan_compilation(format!(
            "compile_plan: {:?} expects exactly one input, got {}",
            stmt.kind,
            other.len()
        ))),
    }
}

fn lower_values(node: &ValuesNode) -> Result<LogicalPlan, DataFusionError> {
    let arrow_schema: ArrowSchema = node.schema.as_ref().try_into_arrow().map_err(|e| {
        plan_compilation(format!(
            "compile_plan: Values arrow schema conversion failed: {e}"
        ))
    })?;
    let df_schema = Arc::new(
        DFSchema::try_from(arrow_schema)
            .map_err(|e| plan_compilation(format!("compile_plan: Values DF schema: {e}")))?,
    );
    let translated = node
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|s| kernel_expr_to_df_untyped(&Expression::literal(s.clone())))
                .collect::<Result<Vec<_>, DataFusionError>>()
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    Ok(if translated.is_empty() {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        })
    } else {
        LogicalPlan::Values(Values {
            schema: df_schema,
            values: translated,
        })
    })
}

/// Convert a [`LogicalPlan`]'s arrow schema back to a kernel [`StructType`]. Used at
/// compile time when downstream lowerings (Load's output schema) need a kernel-typed view
/// of the upstream output.
fn kernel_schema_from_logical(plan: &LogicalPlan) -> Result<StructType, DataFusionError> {
    let arrow: ArrowSchema = plan.schema().as_arrow().clone();
    StructType::try_from_arrow(&arrow).map_err(|e| {
        plan_compilation(format!(
            "compile_plan: arrow -> kernel schema conversion failed: {e}",
        ))
    })
}

/// Lower [`NodeKind::Load`] -- the streaming parquet/json loader. The DV-driven row filter
/// and field-id reshape paths require DataFusion 54 and are rejected at
/// [`crate::exec::LoadExec::new`] construction; this PR only supports the no-DV /
/// no-column-mapping subset.
///
/// DV-annotated and column-mapping-bearing loads are also rejected here at compile time
/// (in addition to the physical-planning guards in `LoadExec::new`) so the lowering
/// surface is symmetric with `lower_scan_*` and callers get the rejection before any
/// later DataFusion planning runs. The duplicate column-mapping walker shared with
/// `scan.rs`'s helper can be deduplicated once a third caller motivates extracting it
/// into a shared module.
fn lower_load(
    built: &HashMap<Ref, LogicalPlan>,
    upstream_ref: Ref,
    node: &LoadNode,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    // Reject DV-annotated loads at compile time, symmetric with the column-mapping check
    // below and with `LoadExec::new`'s physical-planning guard. A DV-annotated `LoadNode`
    // that reaches `LoadExec::new` fails at physical-planning time, but we surface the
    // rejection during logical lowering too so partial pipelines fail fast and consumers
    // can react during plan compilation.
    if node.dv_ref.is_some() {
        return Err(plan_compilation(
            "Load: deletion-vector-annotated loads require DataFusion 54 \
             (virtual `_row_number` column); the engine currently targets DataFusion 53",
        ));
    }
    if schema_has_column_mapping(node.file_schema.fields()) {
        return Err(plan_compilation(
            "Load: column-mapping schemas require DataFusion 54 \
             (FieldIdPhysicalExprAdapterFactory); the engine currently targets DataFusion 53",
        ));
    }
    let upstream_logical = lookup(built, upstream_ref)?.clone();
    let upstream_kernel = kernel_schema_from_logical(&upstream_logical)?;
    let output_kernel_schema = load_output_schema(
        &node.file_schema,
        &node.passthrough_columns,
        &upstream_kernel,
    )
    .map_err(|e| plan_compilation(format!("compile_plan: Load output schema: {e}")))?;
    let provider: Arc<dyn TableProvider> = Arc::new(LoadTableProvider::try_new(
        upstream_logical,
        Arc::new(node.clone()),
        Arc::clone(&ctx.engine),
        output_kernel_schema,
    )?);
    LogicalPlanBuilder::scan("kernel_load", provider_as_source(provider), None)?.build()
}

fn schema_has_column_mapping<'a>(fields: impl IntoIterator<Item = &'a StructField>) -> bool {
    fields.into_iter().any(field_has_column_mapping)
}

fn field_has_column_mapping(field: &StructField) -> bool {
    if field
        .metadata
        .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref())
        || field
            .metadata
            .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        return true;
    }
    data_type_has_column_mapping(field.data_type())
}

fn data_type_has_column_mapping(dt: &DataType) -> bool {
    match dt {
        DataType::Struct(inner) => schema_has_column_mapping(inner.fields()),
        DataType::Array(arr) => data_type_has_column_mapping(arr.element_type()),
        DataType::Map(map) => {
            data_type_has_column_mapping(map.key_type())
                || data_type_has_column_mapping(map.value_type())
        }
        _ => false,
    }
}

/// Lower `NodeKind::MaxByVersion` to `row_number() OVER (PARTITION BY ... ORDER BY version DESC)`
/// followed by `WHERE rn = 1` and a final projection narrowing to the `value_columns`. DataFusion
/// mints a long version-dependent schema name for the window column (e.g. `row_number() PARTITION
/// BY [...] ROWS BETWEEN ...`); rather than try to synthesize that name we read it back from the
/// resulting plan's schema (it's the last column appended by [`LogicalPlanBuilder::window_plan`]).
fn lower_max_by_version(
    child: LogicalPlan,
    node: &MaxByVersionNode,
) -> Result<LogicalPlan, DataFusionError> {
    if node.value_columns.is_empty() {
        return Err(plan_compilation(
            "compile_plan: MaxByVersion with zero value_columns is invalid",
        ));
    }
    let partition_by = kernel_exprs_to_df_untyped(&node.group_by)?;
    let order_by_expr = kernel_expr_to_df_untyped(node.version_column.as_ref())?;
    let row_number_expr = row_number()
        .partition_by(partition_by)
        .order_by(vec![order_by_expr.sort(false /* descending */, false)])
        .build()?;
    let window_plan = LogicalPlanBuilder::window_plan(child, vec![row_number_expr])?;
    let rn_column = window_plan
        .schema()
        .columns()
        .into_iter()
        .next_back()
        .ok_or_else(|| {
            plan_compilation("compile_plan: MaxByVersion window_plan produced an empty schema")
        })?;
    let filtered = LogicalPlanBuilder::from(window_plan)
        .filter(Expr::Column(rn_column).eq(lit(1u64)))?
        .build()?;
    let projection: Vec<Expr> = node
        .value_columns
        .iter()
        .map(|n| Expr::Column(Column::new_unqualified(n)))
        .collect();
    LogicalPlanBuilder::from(filtered)
        .project(projection)?
        .build()
}

fn lower_equi_join(
    stmt: &PlanNode,
    built: &HashMap<Ref, LogicalPlan>,
    node: &EquiJoinNode,
) -> Result<LogicalPlan, DataFusionError> {
    if stmt.inputs.len() != 2 {
        return Err(plan_compilation(format!(
            "compile_plan: EquiJoin expects 2 inputs, got {}",
            stmt.inputs.len()
        )));
    }
    if node.key_pairs.is_empty() {
        return Err(plan_compilation(
            "compile_plan: EquiJoin requires at least one key pair",
        ));
    }
    let left_plan = lookup(built, stmt.inputs[0])?.clone();
    let right_plan = lookup(built, stmt.inputs[1])?.clone();
    let left_keys: Vec<Expr> = node
        .key_pairs
        .iter()
        .map(|(l, _)| kernel_expr_to_df_untyped(l.as_ref()))
        .collect::<Result<_, _>>()?;
    let right_keys: Vec<Expr> = node
        .key_pairs
        .iter()
        .map(|(_, r)| kernel_expr_to_df_untyped(r.as_ref()))
        .collect::<Result<_, _>>()?;
    let df_kind = match node.kind {
        // `LeftAnti`: emit each left row whose key matches no right row. Output schema mirrors
        // the left side. DataFusion's `LeftAnti` semantics match this directly with build = left.
        JoinKind::LeftAnti => DfJoinType::LeftAnti,
    };
    LogicalPlanBuilder::from(left_plan)
        .join_with_expr_keys(right_plan, df_kind, (left_keys, right_keys), None)?
        .build()
}
