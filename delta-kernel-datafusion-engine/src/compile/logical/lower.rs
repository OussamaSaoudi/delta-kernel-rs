//! Kernel [`Plan`] -> DataFusion [`LogicalPlan`] lowering.
//!
//! Topological walk over [`PlanNode`]s. Each node's output [`RefId`] is mapped to a freshly
//! built [`LogicalPlan`]. Inputs are guaranteed to be earlier in `plan.nodes` than outputs by
//! the plan builder, so a single forward pass works.
//!
//! Every [`NodeKind`] variant wraps a payload struct defined in the kernel IR nodes module;
//! engine helpers consume those payload structs by reference (`&LoadNode`, `&ScanParquetNode`,
//! etc.) without repacking. Cross-node data flow happens entirely through DataFusion's
//! logical-plan tree (no relation registry, no named handles).
//!
//! [`Plan`]: delta_kernel::plans::ir::plan::Plan
//! [`PlanNode`]: PlanNode
//! [`RefId`]: RefId
//! [`LogicalPlan`]: LogicalPlan
//! [`NodeKind`]: NodeKind
//!
//! # Schema policy
//!
//! Kernel `Plan`s do not carry per-RefId kernel schemas (those live on the plan builder only
//! during IR construction). DataFusion derives output schemas from `LogicalPlan` shape and arrow
//! types; the only place a kernel [`SchemaRef`] is reconstructed engine-side is
//! [`NodeKind::Load`], whose output schema is computed here from the upstream's arrow shape
//! via [`StructType::try_from_arrow`] and threaded into [`LoadTableProvider::try_new`].
//!
//! [`NodeKind::Load`]: NodeKind::Load
//! [`SchemaRef`]: SchemaRef
//! [`StructType::try_from_arrow`]: delta_kernel::engine::arrow_conversion::TryFromArrow
//! [`LoadTableProvider`]: LoadTableProvider

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
    EquiJoinNode, LoadNode, MaxByVersionNode, UnionAllNode, ValuesNode,
};
use delta_kernel::plans::ir::plan::{JoinKind, NodeKind, PlanNode, RefId};
use delta_kernel::plans::schema_expr::field_op::load_output_schema;
use delta_kernel::schema::StructType;

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
/// Walks `nodes` in order, lowering each plan node and threading the resulting `LogicalPlan`
/// into a `RefId`-keyed map. The plan returned for `terminal` is then handed back. Plan nodes
/// unreachable from `terminal` are still compiled (DCE is the builder's job, not the engine's).
/// Taking `&[PlanNode]` rather than `&Plan` lets both the [`ResultPlan`]-returning drive path
/// (where the caller already has a `Plan`) and the [`EngineRequest::Reduce`] dispatch (where
/// the executor only sees raw nodes) share this entry point.
///
/// [`ResultPlan`]: delta_kernel::plans::ir::plan::ResultPlan
/// [`EngineRequest::Reduce`]: delta_kernel::plans::state_machines::framework::state_machine::EngineRequest::Reduce
pub fn compile_plan(
    nodes: &[PlanNode],
    terminal: RefId,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let mut built: HashMap<RefId, LogicalPlan> = HashMap::with_capacity(nodes.len());
    for node in nodes {
        let logical = lower_node(node, &built, ctx)?;
        built.insert(node.output, logical);
    }
    built.remove(&terminal).ok_or_else(|| {
        plan_compilation(format!(
            "compile_plan: terminal {terminal:?} is not produced by any node in the plan",
        ))
    })
}

/// Look up a compiled child plan; the caller clones for ownership.
fn lookup(built: &HashMap<RefId, LogicalPlan>, r: RefId) -> Result<&LogicalPlan, DataFusionError> {
    built.get(&r).ok_or_else(|| {
        plan_compilation(format!(
            "compile_plan: input {r:?} not compiled (out-of-order nodes?)",
        ))
    })
}

fn lower_node(
    plan_node: &PlanNode,
    built: &HashMap<RefId, LogicalPlan>,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    match &plan_node.kind {
        // === Sources ====================================================================
        NodeKind::ListFiles(node) => file_listing_to_logical_plan(node),
        NodeKind::ScanParquet(node) => scan_parquet_to_logical_plan(node),
        NodeKind::ScanJson(node) => scan_json_to_logical_plan(node),
        NodeKind::Values(node) => lower_values(node),

        // === Unary transforms ===========================================================
        NodeKind::Filter(node) => {
            let child = lookup(built, expect_one_input(plan_node)?)?.clone();
            let pred = kernel_pred_to_df(node.predicate.as_ref())?;
            LogicalPlanBuilder::from(child).filter(pred)?.build()
        }
        NodeKind::Project(node) => {
            let child = lookup(built, expect_one_input(plan_node)?)?.clone();
            compile_project_node(child, node)
        }
        NodeKind::Load(node) => lower_load(built, expect_one_input(plan_node)?, node, ctx),
        NodeKind::MaxByVersion(node) => {
            let child = lookup(built, expect_one_input(plan_node)?)?.clone();
            lower_max_by_version(child, node)
        }

        // === N-ary ======================================================================
        NodeKind::UnionAll(node) => lower_union(plan_node, built, node),
        NodeKind::EquiJoin(node) => lower_equi_join(plan_node, built, node),
    }
}

fn lower_union(
    plan_node: &PlanNode,
    built: &HashMap<RefId, LogicalPlan>,
    node: &UnionAllNode,
) -> Result<LogicalPlan, DataFusionError> {
    if plan_node.inputs.is_empty() {
        return Err(plan_compilation(
            "compile_plan: UnionAll with zero inputs is not a valid plan shape",
        ));
    }
    let children: Vec<LogicalPlan> = plan_node
        .inputs
        .iter()
        .map(|r| lookup(built, *r).cloned())
        .collect::<Result<_, _>>()?;
    if children.len() == 1 {
        return children
            .into_iter()
            .next()
            .ok_or_else(|| plan_compilation("compile_plan: internal: UnionAll lost children"));
    }
    if node.ordered {
        compile_ordered_union(children)
    } else {
        let mut iter = children.into_iter();
        let first = iter
            .next()
            .ok_or_else(|| plan_compilation("compile_plan: internal: UnionAll lost children"))?;
        iter.try_fold(first, |acc, right| {
            LogicalPlanBuilder::from(acc).union(right)?.build()
        })
    }
}

fn expect_one_input(plan_node: &PlanNode) -> Result<RefId, DataFusionError> {
    match plan_node.inputs.as_slice() {
        [r] => Ok(*r),
        other => Err(plan_compilation(format!(
            "compile_plan: {:?} expects exactly one input, got {}",
            plan_node.kind,
            other.len()
        ))),
    }
}

/// Convert a [`LogicalPlan`]'s arrow schema back to a kernel [`StructType`]. Used at
/// compile time when downstream lowerings (Project's collision avoidance, Load's output
/// schema) need a kernel-typed view of the upstream output.
fn kernel_schema_from_logical(plan: &LogicalPlan) -> Result<StructType, DataFusionError> {
    let arrow: ArrowSchema = plan.schema().as_arrow().clone();
    StructType::try_from_arrow(&arrow).map_err(|e| {
        plan_compilation(format!(
            "compile_plan: arrow -> kernel schema conversion failed: {e}",
        ))
    })
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

fn lower_load(
    built: &HashMap<RefId, LogicalPlan>,
    upstream_ref: RefId,
    node: &LoadNode,
    ctx: &CompileContext,
) -> Result<LogicalPlan, DataFusionError> {
    let upstream_logical = lookup(built, upstream_ref)?.clone();
    let upstream_kernel = kernel_schema_from_logical(&upstream_logical)?;
    let output_kernel_schema = load_output_schema(
        &node.file_schema,
        &node.metadata_derived_columns,
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

/// Lower `NodeKind::MaxByVersion` to `row_number() OVER (PARTITION BY ... ORDER BY version DESC)`
/// followed by `WHERE rn = 1` and a final projection narrowing to the `output_schema` fields.
/// DataFusion mints a long version-dependent schema name for the window column (e.g.
/// `row_number() PARTITION BY [...] ROWS BETWEEN ...`); rather than try to synthesize that name
/// we read it back from the resulting plan's schema (it's the last column appended by
/// [`LogicalPlanBuilder::window_plan`]).
fn lower_max_by_version(
    child: LogicalPlan,
    node: &MaxByVersionNode,
) -> Result<LogicalPlan, DataFusionError> {
    if node.output_schema.fields().count() == 0 {
        return Err(plan_compilation(
            "compile_plan: MaxByVersion with empty output_schema is invalid",
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
        .output_schema
        .fields()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    LogicalPlanBuilder::from(filtered)
        .project(projection)?
        .build()
}

fn lower_equi_join(
    plan_node: &PlanNode,
    built: &HashMap<RefId, LogicalPlan>,
    node: &EquiJoinNode,
) -> Result<LogicalPlan, DataFusionError> {
    if plan_node.inputs.len() != 2 {
        return Err(plan_compilation(format!(
            "compile_plan: EquiJoin expects 2 inputs, got {}",
            plan_node.inputs.len()
        )));
    }
    if node.left_keys.is_empty() {
        return Err(plan_compilation(
            "compile_plan: EquiJoin requires at least one key pair",
        ));
    }
    if node.left_keys.len() != node.right_keys.len() {
        return Err(plan_compilation(format!(
            "compile_plan: EquiJoin left_keys ({}) and right_keys ({}) length mismatch",
            node.left_keys.len(),
            node.right_keys.len(),
        )));
    }
    let left_plan = lookup(built, plan_node.inputs[0])?.clone();
    let right_plan = lookup(built, plan_node.inputs[1])?.clone();
    let left_keys: Vec<Expr> = node
        .left_keys
        .iter()
        .map(|l| kernel_expr_to_df_untyped(l.as_ref()))
        .collect::<Result<_, _>>()?;
    let right_keys: Vec<Expr> = node
        .right_keys
        .iter()
        .map(|r| kernel_expr_to_df_untyped(r.as_ref()))
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
