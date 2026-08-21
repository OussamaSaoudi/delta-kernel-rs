//! Lowering for kernel [`Project`] operators.

use std::sync::Arc;

use datafusion_common::arrow::datatypes::{FieldRef, Schema as ArrowSchema};
use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{ExprSchemable, LogicalPlanBuilder};
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::expressions::{ColumnName, Expression, ExpressionRef, ExpressionStructPatch};
use delta_kernel::plans::ir::nodes::Project;
use delta_kernel::schema::{DataType, StructType};

use crate::compile::expr_translator::{kernel_expr_to_df, TranslationContext};
use crate::compile::stamp_udf::StampFieldUdf;
use crate::error::plan_compilation;

fn materialize_patch_expr(
    expr: &ExpressionRef,
    input_schema: &StructType,
) -> Result<ExpressionRef, DataFusionError> {
    match expr.as_ref() {
        Expression::StructPatch(patch) => Ok(Arc::new(Expression::Struct(
            expand_patch(patch, input_schema)?,
            None,
        ))),
        _ => Ok(Arc::clone(expr)),
    }
}

pub(crate) fn expand_patch(
    patch: &ExpressionStructPatch,
    input_schema: &StructType,
) -> Result<Vec<ExpressionRef>, DataFusionError> {
    let source = match patch.input_path.as_ref() {
        Some(path) => match input_schema
            .field_at(path)
            .map_err(|e| plan_compilation(format!("Project StructPatch input path `{path}`: {e}")))?
            .data_type()
        {
            DataType::Struct(source) => source.as_ref(),
            other => {
                return Err(plan_compilation(format!(
                    "Project StructPatch input path `{path}` is {other}, not a struct"
                )))
            }
        },
        None => input_schema,
    };
    let prefix = patch.input_path.as_ref();
    let input_column = |name: &str| {
        let leaf = ColumnName::new([name]);
        Arc::new(Expression::Column(match prefix {
            Some(prefix) => prefix.join(&leaf),
            None => leaf,
        }))
    };

    let mut output = Vec::new();
    for expr in &patch.prepended_fields {
        output.push(materialize_patch_expr(expr, input_schema)?);
    }
    for field in source.fields() {
        match patch.field_patches.get(field.name()) {
            Some(field_patch) => {
                if field_patch.keep_input {
                    output.push(input_column(field.name()));
                }
                for expr in &field_patch.insertions {
                    output.push(materialize_patch_expr(expr, input_schema)?);
                }
            }
            None => output.push(input_column(field.name())),
        }
    }
    for expr in &patch.appended_fields {
        output.push(materialize_patch_expr(expr, input_schema)?);
    }
    Ok(output)
}

fn dense_projection(
    expr: &Expression,
    input_schema: &StructType,
) -> Result<Vec<ExpressionRef>, DataFusionError> {
    match expr {
        Expression::Struct(children, _) => Ok(children.clone()),
        Expression::StructPatch(patch) => expand_patch(patch, input_schema),
        other => Err(plan_compilation(format!(
            "Project expression must be Struct or StructPatch, got {other:?}"
        ))),
    }
}

/// Compiles a kernel projection over an already-lowered child plan.
pub(super) fn compile_project_node(
    child_plan: LogicalPlan,
    node: &Project,
) -> Result<LogicalPlan, DataFusionError> {
    let child_arrow: ArrowSchema = child_plan.schema().as_arrow().clone();
    let input_schema = StructType::try_from_arrow(&child_arrow)
        .map_err(|e| plan_compilation(format!("Project input schema conversion failed: {e}")))?;
    let columns = dense_projection(node.expr.as_ref(), &input_schema)?;
    if columns.len() != node.schema.fields().count() {
        return Err(plan_compilation(format!(
            "Project output has {} fields but expression expanded to {} columns",
            node.schema.fields().count(),
            columns.len()
        )));
    }

    let working_plan = child_plan;

    let working_arrow: ArrowSchema = working_plan.schema().as_arrow().clone();
    let working_df = working_plan.schema().clone();
    let projection = columns
        .iter()
        .zip(node.schema.fields())
        .map(|(kernel_expr, field)| {
            let translated = kernel_expr_to_df(
                kernel_expr.as_ref(),
                &TranslationContext::typed(field, &working_arrow),
            )?;
            let target: FieldRef = Arc::new(field.try_into_arrow().map_err(|e| {
                plan_compilation(format!(
                    "Project field `{}` conversion failed: {e}",
                    field.name()
                ))
            })?);
            let natural = translated
                .to_field(working_df.as_ref())
                .ok()
                .map(|(_, field)| field.data_type().clone());
            let translated = if natural.as_ref() != Some(target.data_type()) {
                StampFieldUdf::new(target).call(translated)
            } else {
                translated
            };
            Ok::<_, DataFusionError>(translated.alias(field.name()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    LogicalPlanBuilder::from(working_plan)
        .project(projection)?
        .build()
}
