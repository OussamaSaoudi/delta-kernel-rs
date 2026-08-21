//! Kernel [`Plan`] to DataFusion [`LogicalPlan`] lowering.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::expr::Case;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan, Values as DfValues};
use datafusion_expr::{lit, Expr, JoinType, LogicalPlanBuilder};
use datafusion_functions_aggregate::expr_fn::{count, first_value, max, min, sum};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Expression;
use delta_kernel::plans::ir::nodes::{Agg, Aggregate, NonNullByOperands, Operator, Values};
use delta_kernel::plans::ir::plan::Plan;

use super::project::compile_project_node;
use super::scan::{scan_json_to_logical_plan, scan_parquet_to_logical_plan};
use crate::compile::expr_translator::{
    kernel_expr_to_df_untyped, kernel_pred_to_df, scalar_value_to_df,
};
use crate::compile::CompileContext;
use crate::error::plan_compilation;
use crate::exec::{LoadTableProvider, SharedNodeTableProvider};

fn input(
    built: &[LogicalPlan],
    index: usize,
    consumer: usize,
) -> Result<&LogicalPlan, DataFusionError> {
    built.get(index).ok_or_else(|| {
        plan_compilation(format!(
            "plan node {consumer} references unavailable input node {index}"
        ))
    })
}

/// Compiles the terminal node of a topologically ordered kernel plan.
pub fn compile_plan(plan: &Plan, context: &CompileContext) -> Result<LogicalPlan, DataFusionError> {
    if plan.nodes.is_empty() {
        return Err(plan_compilation("cannot compile an empty plan"));
    }
    let mut consumers = vec![0usize; plan.nodes.len()];
    for node in &plan.nodes {
        for input in &node.inputs {
            if let Some(count) = consumers.get_mut(*input) {
                *count += 1;
            }
        }
    }
    let mut built = Vec::with_capacity(plan.nodes.len());
    for (index, node) in plan.nodes.iter().enumerate() {
        let logical = match &node.op {
            Operator::ScanParquet(scan) => {
                expect_inputs(index, &node.inputs, 0)?;
                scan_parquet_to_logical_plan(scan)?
            }
            Operator::ScanJson(scan) => {
                expect_inputs(index, &node.inputs, 0)?;
                scan_json_to_logical_plan(scan)?
            }
            Operator::Values(values) => {
                expect_inputs(index, &node.inputs, 0)?;
                lower_values(values)?
            }
            Operator::Project(project) => {
                expect_inputs(index, &node.inputs, 1)?;
                compile_project_node(input(&built, node.inputs[0], index)?.clone(), project)?
            }
            Operator::Filter(filter) => {
                expect_inputs(index, &node.inputs, 1)?;
                LogicalPlanBuilder::from(input(&built, node.inputs[0], index)?.clone())
                    .filter(kernel_pred_to_df(filter.predicate.as_ref())?)?
                    .build()?
            }
            Operator::DynamicScan(scan) => {
                expect_inputs(index, &node.inputs, 1)?;
                let provider: Arc<dyn TableProvider> = Arc::new(LoadTableProvider::try_new(
                    input(&built, node.inputs[0], index)?.clone(),
                    Arc::new(scan.clone()),
                    Arc::clone(&context.engine),
                )?);
                LogicalPlanBuilder::scan("delta_dynamic_scan", provider_as_source(provider), None)?
                    .build()?
            }
            Operator::Aggregate(aggregate) => {
                expect_inputs(index, &node.inputs, 1)?;
                lower_aggregate(input(&built, node.inputs[0], index)?.clone(), aggregate)?
            }
            Operator::SemiJoin(join) => {
                expect_inputs(index, &node.inputs, 2)?;
                if join.probe_keys.len() != join.build_keys.len() {
                    return Err(plan_compilation("SemiJoin key counts do not match"));
                }
                let probe_keys = join
                    .probe_keys
                    .iter()
                    .map(column_name_to_df_column)
                    .collect::<Result<Vec<_>, _>>()?;
                let build_keys = join
                    .build_keys
                    .iter()
                    .map(column_name_to_df_column)
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(input(&built, node.inputs[0], index)?.clone())
                    .join(
                        input(&built, node.inputs[1], index)?.clone(),
                        if join.inverted {
                            JoinType::LeftAnti
                        } else {
                            JoinType::LeftSemi
                        },
                        (probe_keys, build_keys),
                        None,
                    )?
                    .build()?
            }
            Operator::UnionAll(_) => {
                if node.inputs.is_empty() {
                    return Err(plan_compilation("UnionAll requires at least one input"));
                }
                let mut children = node.inputs.iter();
                let first = children.next().ok_or_else(|| {
                    plan_compilation("UnionAll unexpectedly lost its first input")
                })?;
                children.try_fold(input(&built, *first, index)?.clone(), |left, child| {
                    LogicalPlanBuilder::from(left)
                        .union(input(&built, *child, index)?.clone())?
                        .build()
                })?
            }
        };
        let logical = if consumers[index] > 1 {
            let provider: Arc<dyn TableProvider> =
                Arc::new(SharedNodeTableProvider::new(logical, index));
            LogicalPlanBuilder::scan(
                format!("shared_delta_node_{index}"),
                provider_as_source(provider),
                None,
            )?
            .build()?
        } else {
            logical
        };
        built.push(logical);
    }
    built
        .pop()
        .ok_or_else(|| plan_compilation("compiled plan has no terminal node"))
}

fn expect_inputs(index: usize, inputs: &[usize], expected: usize) -> Result<(), DataFusionError> {
    if inputs.len() != expected {
        return Err(plan_compilation(format!(
            "plan node {index} expects {expected} inputs, got {}",
            inputs.len()
        )));
    }
    Ok(())
}

pub(super) fn lower_values(values: &Values) -> Result<LogicalPlan, DataFusionError> {
    let arrow_schema: ArrowSchema = values
        .schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| plan_compilation(format!("Values schema conversion failed: {e}")))?;
    let schema = Arc::new(DFSchema::try_from(arrow_schema)?);
    if values.rows.is_empty() {
        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        }));
    }
    let rows = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|scalar| scalar_value_to_df(scalar).map(|value| Expr::Literal(value, None)))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LogicalPlan::Values(DfValues {
        schema,
        values: rows,
    }))
}

fn column_name_to_expr(
    name: &delta_kernel::expressions::ColumnName,
) -> Result<Expr, DataFusionError> {
    kernel_expr_to_df_untyped(&Expression::Column(name.clone()))
}

fn column_name_to_df_column(
    name: &delta_kernel::expressions::ColumnName,
) -> Result<Column, DataFusionError> {
    let [name] = name.path() else {
        return Err(plan_compilation(format!(
            "join key `{name}` must be a top-level column"
        )));
    };
    Ok(Column::new_unqualified(name))
}

fn non_null_by(operands: &NonNullByOperands, ascending: bool) -> Result<Expr, DataFusionError> {
    let value = column_name_to_expr(&operands.value)?;
    let sentinel = column_name_to_expr(&operands.null_sentinel)?;
    let key = column_name_to_expr(&operands.key)?;
    let conditioned_key = Expr::Case(Case::new(
        None,
        vec![(Box::new(Expr::IsNotNull(Box::new(sentinel))), Box::new(key))],
        Some(Box::new(Expr::Literal(ScalarValue::Null, None))),
    ));
    Ok(first_value(
        value,
        vec![conditioned_key.sort(ascending, false)],
    ))
}

fn lower_aggregate(
    child: LogicalPlan,
    aggregate: &Aggregate,
) -> Result<LogicalPlan, DataFusionError> {
    let groups = aggregate
        .group_by
        .iter()
        .map(column_name_to_expr)
        .collect::<Result<Vec<_>, _>>()?;
    let output_fields = aggregate.schema.fields().skip(aggregate.group_by.len());
    let aggs = aggregate
        .aggs
        .iter()
        .zip(output_fields)
        .map(|(agg, field)| {
            let expr = match agg {
                Agg::Min(value) => min(column_name_to_expr(value)?),
                Agg::Max(value) => max(column_name_to_expr(value)?),
                Agg::Sum(value) => sum(column_name_to_expr(value)?),
                Agg::Count(value) => count(column_name_to_expr(value)?),
                Agg::CountStar => count(lit(1i64)),
                Agg::MinNonNullBy(operands) => non_null_by(operands, true)?,
                Agg::MaxNonNullBy(operands) => non_null_by(operands, false)?,
            };
            Ok::<_, DataFusionError>(expr.alias(field.name()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    LogicalPlanBuilder::from(child)
        .aggregate(groups, aggs)?
        .build()
}
