//! Plan compilation: DeclarativePlanNode -> DataFusion ExecutionPlan.

use std::sync::Arc;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_plan::{filter::FilterExec, projection::ProjectionExec};

use delta_kernel::plans::DeclarativePlanNode;
use crate::error::{DfResult, DfError};
use crate::expr::lower_expression;
use crate::scan::compile_scan as compile_scan_impl;

/// Compile a declarative plan node into a DataFusion physical plan.
pub fn compile_plan(
    plan: &DeclarativePlanNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    match plan {
        DeclarativePlanNode::Scan(node) => {
            compile_scan(node, session_state)
        }
        DeclarativePlanNode::FileListing(node) => {
            compile_file_listing(node, session_state)
        }
        DeclarativePlanNode::SchemaQuery(node) => {
            compile_schema_query(node, session_state)
        }
        DeclarativePlanNode::FilterByKDF { child, node } => {
            compile_filter_by_kdf(child, node, session_state)
        }
        DeclarativePlanNode::ConsumeByKDF { child, node } => {
            compile_consume_by_kdf(child, node, session_state)
        }
        DeclarativePlanNode::FilterByExpression { child, node } => {
            compile_filter_by_expr(child, node, session_state)
        }
        DeclarativePlanNode::Select { child, node } => {
            compile_select(child, node, session_state)
        }
        DeclarativePlanNode::ParseJson { child, node } => {
            compile_parse_json(child, node, session_state)
        }
        DeclarativePlanNode::FirstNonNull { child, node } => {
            compile_first_non_null(child, node, session_state)
        }
        DeclarativePlanNode::Sink { child, node } => {
            // Sinks are transparent in DataFusion - just compile the child
            // The driver handles Results vs Drop semantics
            compile_sink(child, node, session_state)
        }
    }
}

// Placeholder implementations - to be filled in subsequent todos

fn compile_sink(
    child: &DeclarativePlanNode,
    _node: &delta_kernel::plans::SinkNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    // Sinks are transparent in DataFusion - just compile the child
    // The driver handles Results vs Drop semantics
    compile_plan(child, session_state)
}

fn compile_scan(
    node: &delta_kernel::plans::ScanNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    compile_scan_impl(node, session_state)
}

fn compile_file_listing(
    node: &delta_kernel::plans::FileListingNode,
    _session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    Err(DfError::Unsupported(format!("FileListing compilation not yet implemented: {:?}", node.path)))
}

fn compile_schema_query(
    node: &delta_kernel::plans::SchemaQueryNode,
    _session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    Err(DfError::Unsupported(format!("SchemaQuery compilation not yet implemented: {}", node.file_path)))
}

fn compile_filter_by_kdf(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::FilterByKDF,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let _child_plan = compile_plan(child, session_state)?;
    Err(DfError::Unsupported(format!("FilterByKDF compilation not yet implemented: {:?}", node)))
}

fn compile_consume_by_kdf(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::ConsumeByKDF,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let _child_plan = compile_plan(child, session_state)?;
    Err(DfError::Unsupported(format!("ConsumeByKDF compilation not yet implemented: {:?}", node)))
}

fn compile_filter_by_expr(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::FilterByExpressionNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let child_plan = compile_plan(child, session_state)?;
    
    // FilterByExpressionNode stores the predicate as Arc<Expression>
    // Lower it to a DataFusion Expr
    let predicate_expr = lower_expression(&node.predicate)?;
    
    // Convert logical Expr to physical PhysicalExpr
    // This requires the schema from the child plan
    let schema = child_plan.schema();
    let df_schema = datafusion_common::DFSchema::try_from_qualified_schema("", &schema)?;
    let physical_expr = datafusion_physical_expr::create_physical_expr(
        &predicate_expr,
        &df_schema,
        session_state.execution_props(),
    )?;
    
    // Create a FilterExec with the physical predicate
    let filter_exec = FilterExec::try_new(physical_expr, child_plan)?;
    Ok(Arc::new(filter_exec))
}

fn compile_select(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::SelectNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let child_plan = compile_plan(child, session_state)?;
    let schema = child_plan.schema();
    let df_schema = datafusion_common::DFSchema::try_from_qualified_schema("", &schema)?;
    
    // Lower each expression and convert to physical
    let proj_exprs: Result<Vec<_>, DfError> = node.columns.iter()
        .enumerate()
        .map(|(i, expr)| {
            let df_expr = lower_expression(expr)?;
            let physical_expr = datafusion_physical_expr::create_physical_expr(
                &df_expr,
                &df_schema,
                session_state.execution_props(),
            )?;
            // ProjectionExec needs (PhysicalExpr, name) pairs
            Ok::<_, DfError>((physical_expr, format!("col_{}", i)))
        })
        .collect();
    
    let projection_exec = ProjectionExec::try_new(proj_exprs?, child_plan)?;
    Ok(Arc::new(projection_exec))
}

fn compile_parse_json(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::ParseJsonNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let _child_plan = compile_plan(child, session_state)?;
    Err(DfError::Unsupported(format!("ParseJson compilation not yet implemented: {}", node.json_column)))
}

fn compile_first_non_null(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::FirstNonNullNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let _child_plan = compile_plan(child, session_state)?;
    Err(DfError::Unsupported(format!("FirstNonNull compilation not yet implemented ({} columns)", node.columns.len())))
}

