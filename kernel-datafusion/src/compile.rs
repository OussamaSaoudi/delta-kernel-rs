//! Plan compilation: DeclarativePlanNode -> DataFusion ExecutionPlan.

use std::sync::Arc;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;

use delta_kernel::plans::DeclarativePlanNode;
use crate::error::{DfResult, DfError};

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
    _session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    Err(DfError::Unsupported(format!("Scan compilation not yet implemented: {:?}", node.file_type)))
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
    let _child_plan = compile_plan(child, session_state)?;
    Err(DfError::Unsupported(format!("FilterByExpression compilation not yet implemented: {:?}", node.predicate)))
}

fn compile_select(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::SelectNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let _child_plan = compile_plan(child, session_state)?;
    Err(DfError::Unsupported(format!("Select compilation not yet implemented ({} columns)", node.columns.len())))
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

