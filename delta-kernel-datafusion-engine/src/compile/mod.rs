//! Declarative [`Plan`] -> DataFusion [`ExecutionPlan`] compilation.
//!
//! Phase 1.7 extends sinks beyond [`SinkType::Results`]:
//! [`SinkType::Relation`] (materialize into [`RelationBatchRegistry`]),
//! [`SinkType::ConsumeByKdf`] (drain via [`KernelConsumeByKdfExec`]).
//!
//! Phase 2.1 adds [`SinkType::Write`] for single-file Parquet / JsonLines via DataFusion
//! [`DataSinkExec`](datafusion_datasource::sink::DataSinkExec) (see [`write_sink`]).
//!
//! Phase 2.2 adds [`SinkType::PartitionedWrite`] ([`KernelPartitionedWriteExec`]): Hive-style
//! directories under a `file://` destination with Parquet or newline-delimited JSON.
//!
//! Phase 2.3 adds [`SinkType::Load`] ([`crate::exec::KernelLoadSinkExec`]): per-row parquet/json
//! reads via kernel handlers into [`crate::exec::RelationBatchRegistry`].
//!
//! Phase 1.2 extends the scaffold with leaf support:
//! - `Values`
//! - `Scan`
//! - `FileListing`
//! - `RelationRef`

use std::sync::{Arc, Mutex};

use datafusion_common::DFSchema;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_fn::cast;
use datafusion_expr::lit;
use datafusion_functions::core::expr_fn::named_struct;
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{JoinType, RelationHandle, SinkType, WriteSink};
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};
use delta_kernel::plans::kdf::FinishedHandle;
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::schema::SchemaRef;
use delta_kernel::Engine;

use crate::exec::{
    FileListingExec, KernelAssertExec, KernelConsumeByKdfExec, KernelPartitionedWriteExec,
    LiteralExec, OrderedUnionExec, RelationBatchRegistry, RelationSinkExec, build_relation_ref_exec,
};

pub mod expr_translator;
mod join;
mod load_sink;
pub mod scan;
mod window;
mod write_sink;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    pub relation_registry: Arc<RelationBatchRegistry>,
    /// Latest finalized [`FinishedHandle`] from a [`SinkType::ConsumeByKdf`] plan run on this
    /// executor.
    pub kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    /// When [`Some`], [`SinkType::ConsumeByKdf`] pipelines submit finalized handles into this
    /// [`PhaseState`] for
    /// [`StateMachine`](delta_kernel::plans::state_machines::framework::state_machine::StateMachine)
    /// phases instead of populating [`Self::kdf_harvest_slot`] only.
    ///
    /// [`None`] preserves single-plan harvesting via
    /// [`crate::executor::DataFusionExecutor::take_last_kdf_finished`].
    pub phase_state: Option<PhaseState>,
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers ([`SinkType::Load`]).
    pub engine: Arc<dyn Engine>,
}

impl CompileContext {
    pub fn new(
        relation_registry: Arc<RelationBatchRegistry>,
        kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
        engine: Arc<dyn Engine>,
    ) -> Self {
        Self {
            relation_registry,
            kdf_harvest_slot,
            phase_state: None,
            engine,
        }
    }
}

/// Compile a complete [`Plan`] when the sink envelope is supported.
pub fn compile_plan(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match &plan.sink.sink_type {
        SinkType::Results => compile_declarative_node(&plan.root, ctx),
        SinkType::Relation(handle) => {
            let mut inner = compile_declarative_node(&plan.root, ctx)?;
            if inner.output_partitioning().partition_count() > 1 {
                inner = Arc::new(CoalescePartitionsExec::new(inner));
            }
            Ok(Arc::new(RelationSinkExec::new(
                inner,
                handle.id,
                Arc::clone(&ctx.relation_registry),
            )))
        }
        SinkType::ConsumeByKdf(sink) => {
            let inner = compile_declarative_node(&plan.root, ctx)?;
            Ok(Arc::new(KernelConsumeByKdfExec::try_new(
                inner,
                sink.clone(),
                Arc::clone(&ctx.kdf_harvest_slot),
                ctx.phase_state.clone(),
            )?))
        }
        SinkType::Load(_) => load_sink::compile_load_terminal(plan, ctx),
        SinkType::Write(write) => compile_write_terminal(&plan.root, ctx, write),
        SinkType::PartitionedWrite(sink) => {
            let inner = compile_declarative_node(&plan.root, ctx)?;
            Ok(Arc::new(KernelPartitionedWriteExec::try_new(
                inner,
                sink.clone(),
            )?))
        }
    }
}

fn compile_write_terminal(
    root: &DeclarativePlanNode,
    ctx: &CompileContext,
    sink: &WriteSink,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let inner = compile_declarative_node(root, ctx)?;
    write_sink::compile_write_sink(inner, sink)
}

pub(super) fn compile_declarative_node(
    node: &DeclarativePlanNode,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match node {
        DeclarativePlanNode::Values(n) => Ok(Arc::new(LiteralExec::try_new(
            n.schema.clone(),
            n.rows.clone(),
        )?)),
        DeclarativePlanNode::Scan(node) => scan::compile_scan(node),
        DeclarativePlanNode::FileListing(node) => {
            Ok(Arc::new(FileListingExec::new(node.path.clone())))
        }
        DeclarativePlanNode::RelationRef(handle) => compile_relation(handle, ctx),
        DeclarativePlanNode::Filter { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            compile_native_filter(child_plan, &input_schema, node.predicate.as_ref())
        }
        DeclarativePlanNode::Project { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            compile_native_projection(
                child_plan,
                &input_schema,
                &node.columns,
                &node.output_schema,
            )
        }
        DeclarativePlanNode::Assert { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            Ok(Arc::new(KernelAssertExec::try_new(
                child_plan,
                input_schema,
                &node.checks,
            )?))
        }
        DeclarativePlanNode::Window { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            window::compile_window_node(child_plan, input_schema, node)
        }
        DeclarativePlanNode::Union { children, node } => {
            if children.is_empty() {
                return Err(crate::error::unsupported(
                    "Union with zero children is not supported yet",
                ));
            }
            let child_plans = children
                .iter()
                .map(|child| compile_declarative_node(child, ctx))
                .collect::<Result<Vec<_>, DeltaError>>()?;
            if node.ordered {
                let coalesced = child_plans
                    .into_iter()
                    .map(|plan| {
                        Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>
                    })
                    .collect::<Vec<_>>();
                Ok(Arc::new(
                    OrderedUnionExec::try_new(coalesced)
                        .map_err(crate::error::datafusion_err_to_delta)?,
                ))
            } else {
                let unioned = UnionExec::try_new(child_plans)
                    .map_err(crate::error::datafusion_err_to_delta)?;
                Ok(Arc::new(CoalescePartitionsExec::new(unioned)))
            }
        }
        DeclarativePlanNode::Join { build, probe, node } => {
            join::compile_join(build.as_ref(), probe.as_ref(), node, ctx)
        }
    }
}

fn node_output_schema(node: &DeclarativePlanNode) -> Result<SchemaRef, DeltaError> {
    match node {
        DeclarativePlanNode::Scan(n) => Ok(n.schema.clone()),
        DeclarativePlanNode::Values(n) => Ok(n.schema.clone()),
        DeclarativePlanNode::RelationRef(h) => Ok(h.schema.clone()),
        DeclarativePlanNode::Project { node, .. } => Ok(node.output_schema.clone()),
        DeclarativePlanNode::Union { children, .. } => {
            let Some(first) = children.first() else {
                return Err(crate::error::unsupported("Union has no children"));
            };
            node_output_schema(first)
        }
        DeclarativePlanNode::Filter { child, .. } => node_output_schema(child),
        DeclarativePlanNode::Assert { child, .. } => node_output_schema(child),
        DeclarativePlanNode::Window { child, node } => {
            let child_schema = node_output_schema(child)?;
            window::window_output_kernel_schema(&child_schema, node)
        }
        DeclarativePlanNode::Join { build, probe, node } => match node.join_type {
            JoinType::LeftAnti => node_output_schema(probe),
            JoinType::Inner => {
                let build_schema = node_output_schema(build)?;
                let probe_schema = node_output_schema(probe)?;
                build_schema
                    .as_ref()
                    .add(probe_schema.fields().cloned())
                    .map(Arc::new)
                    .map_err(|e| {
                        crate::error::plan_compilation(format!(
                            "inner join combined output schema is invalid: {e}"
                        ))
                    })
            }
            other_join => Err(crate::error::unsupported(format!(
                "Schema inference for join type {other_join:?} is not implemented yet",
            ))),
        },
        DeclarativePlanNode::FileListing(_) => Err(crate::error::unsupported(
            "FileListing schema inference for Filter/Project is not wired yet",
        )),
    }
}

fn compile_relation(
    handle: &RelationHandle,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    build_relation_ref_exec(handle, &ctx.relation_registry)
}

fn compile_native_filter(
    child_plan: Arc<dyn ExecutionPlan>,
    input_schema: &SchemaRef,
    predicate: &delta_kernel::expressions::Expression,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let arrow_schema: datafusion_common::arrow::datatypes::Schema =
        input_schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::plan_compilation(format!(
                "Filter compilation failed to convert schema to Arrow: {e}"
            ))
        })?;
    let df_schema = DFSchema::try_from(arrow_schema).map_err(|e| {
        crate::error::plan_compilation(format!("Filter compilation failed to build DFSchema: {e}"))
    })?;
    let props = ExecutionProps::new();
    let logical = expr_translator::kernel_expr_to_df(predicate)?;
    let physical = create_physical_expr(&logical, &df_schema, &props).map_err(|e| {
        crate::error::plan_compilation(format!(
            "Filter predicate is not translatable to DataFusion: {e}"
        ))
    })?;
    let native =
        FilterExec::try_new(physical, child_plan).map_err(crate::error::datafusion_err_to_delta)?;
    Ok(Arc::new(native))
}

fn compile_native_projection(
    child_plan: Arc<dyn ExecutionPlan>,
    input_schema: &SchemaRef,
    columns: &[Arc<delta_kernel::expressions::Expression>],
    output_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let expanded_columns = expand_projection_columns(columns, output_schema.fields().count())?;

    let arrow_schema: datafusion_common::arrow::datatypes::Schema =
        input_schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::plan_compilation(format!(
                "Projection compilation failed to convert schema to Arrow: {e}"
            ))
        })?;
    let df_schema = DFSchema::try_from(arrow_schema).map_err(|e| {
        crate::error::plan_compilation(format!(
            "Projection compilation failed to build DFSchema: {e}"
        ))
    })?;
    let props = ExecutionProps::new();
    let output_arrow_schema: datafusion_common::arrow::datatypes::Schema =
        output_schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::plan_compilation(format!(
                "Projection compilation failed to convert output schema to Arrow: {e}"
            ))
        })?;
    let projection_exprs = expanded_columns
        .iter()
        .zip(output_schema.fields().zip(output_arrow_schema.fields()))
        .map(|(kernel_expr, (field, output_arrow_field))| {
            let base_logical = translate_projection_expr(kernel_expr.as_ref(), field)?;
            let logical = if matches!(
                (kernel_expr.as_ref(), field.data_type()),
                (
                    delta_kernel::expressions::Expression::Column(_),
                    delta_kernel::schema::DataType::Struct(_)
                )
            ) || matches!(
                (kernel_expr.as_ref(), field.data_type()),
                (
                    delta_kernel::expressions::Expression::Struct(_, _),
                    delta_kernel::schema::DataType::Struct(_)
                )
            ) {
                base_logical
            } else {
                cast(base_logical, output_arrow_field.data_type().clone())
            };
            let physical = create_physical_expr(&logical, &df_schema, &props).map_err(|e| {
                crate::error::plan_compilation(format!(
                    "Projection expression `{}` is not translatable to DataFusion: {e}",
                    field.name()
                ))
            })?;
            Ok((physical, field.name().to_string()))
        })
        .collect::<Result<Vec<_>, DeltaError>>()?;
    let native = ProjectionExec::try_new(projection_exprs, child_plan)
        .map_err(crate::error::datafusion_err_to_delta)?;
    Ok(Arc::new(native))
}

fn expand_projection_columns(
    columns: &[Arc<delta_kernel::expressions::Expression>],
    expected_output_fields: usize,
) -> Result<Vec<Arc<delta_kernel::expressions::Expression>>, DeltaError> {
    let mut expanded = Vec::new();
    for (idx, expr) in columns.iter().enumerate() {
        let remaining_output = expected_output_fields
            .checked_sub(expanded.len())
            .ok_or_else(|| crate::error::plan_compilation("Projection expansion overflow"))?;
        let remaining_expr = columns.len() - idx;
        let extra_needed = remaining_output
            .checked_sub(remaining_expr)
            .ok_or_else(|| {
                crate::error::plan_compilation(format!(
                    "Projection has too many expressions: expected {expected_output_fields} output fields, got at least {}",
                    expanded.len() + remaining_expr
                ))
            })?;

        match expr.as_ref() {
            delta_kernel::expressions::Expression::Struct(children, _) => {
                let spread_extra = children.len().saturating_sub(1);
                if spread_extra > 0 && spread_extra <= extra_needed {
                    expanded.extend(children.iter().cloned());
                } else {
                    expanded.push(Arc::clone(expr));
                }
            }
            _ => expanded.push(Arc::clone(expr)),
        }
    }

    if expanded.len() != expected_output_fields {
        return Err(crate::error::plan_compilation(format!(
            "Projection output schema has {} fields but expanded to {} expressions",
            expected_output_fields,
            expanded.len()
        )));
    }
    Ok(expanded)
}

fn translate_projection_expr(
    expr: &delta_kernel::expressions::Expression,
    output_field: &delta_kernel::schema::StructField,
) -> Result<datafusion_expr::Expr, DeltaError> {
    if let delta_kernel::expressions::Expression::Struct(children, nullability_predicate) = expr {
        if nullability_predicate.is_some() {
            return Err(crate::error::unsupported(
                "Struct projection with nullability predicate is not yet supported",
            ));
        }
        if let delta_kernel::schema::DataType::Struct(target_struct) = output_field.data_type() {
            if target_struct.fields().count() == children.len() {
                let mut args = Vec::with_capacity(children.len() * 2);
                for (child_expr, child_field) in children.iter().zip(target_struct.fields()) {
                    args.push(lit(child_field.name().to_string()));
                    args.push(expr_translator::kernel_expr_to_df(child_expr.as_ref())?);
                }
                return Ok(named_struct(args));
            }
        }
    }
    expr_translator::kernel_expr_to_df(expr)
}
