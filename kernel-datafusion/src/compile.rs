//! Plan compilation: DeclarativePlanNode -> DataFusion ExecutionPlan.

use std::sync::Arc;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_plan::{filter::FilterExec, projection::ProjectionExec};
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::Column;
use datafusion_functions_aggregate::first_last::first_value_udaf;

use delta_kernel::plans::DeclarativePlanNode;
use crate::error::{DfResult, DfError};
use crate::expr::lower_expression;
use crate::scan::compile_scan as compile_scan_impl;
use crate::exec::{KdfFilterExec, ConsumeKdfExec, FileListingExec, SchemaQueryExec};

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
    // FileListingExec uses ObjectStore from TaskContext at execution time,
    // so we just need the path at compile time.
    Ok(Arc::new(FileListingExec::new(node.path.clone())))
}

fn compile_schema_query(
    node: &delta_kernel::plans::SchemaQueryNode,
    _session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    // Parse the file path as a URL
    let file_url = url::Url::parse(&node.file_path)
        .map_err(|e| DfError::PlanCompilation(format!("Invalid file path '{}': {}", node.file_path, e)))?;
    
    // SchemaQueryExec uses ObjectStore from TaskContext at execution time,
    // so we just need the path and state at compile time.
    Ok(Arc::new(SchemaQueryExec::new(file_url, node.state.clone())))
}

fn compile_filter_by_kdf(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::FilterByKDF,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let child_plan = compile_plan(child, session_state)?;
    Ok(Arc::new(KdfFilterExec::new(child_plan, node.clone())))
}

fn compile_consume_by_kdf(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::ConsumeByKDF,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let child_plan = compile_plan(child, session_state)?;
    Ok(Arc::new(ConsumeKdfExec::new(child_plan, node.clone())))
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
    use crate::json_parse::{build_nested_column_expr, generate_schema_extractions};
    use datafusion_expr::{col, lit};
    
    let child_plan = compile_plan(child, session_state)?;
    let child_schema = child_plan.schema();
    let df_schema = datafusion_common::DFSchema::try_from_qualified_schema("", &child_schema)?;
    
    // Build expression to access the JSON column (handles nested paths like "add.stats")
    let json_col_expr = build_nested_column_expr(&node.json_column);
    
    // Generate extraction expressions for the target schema
    let extractions = generate_schema_extractions(&json_col_expr, &node.target_schema)?;
    
    // Build projection expressions
    let mut proj_exprs: Vec<(Arc<dyn datafusion_physical_expr::PhysicalExpr>, String)> = Vec::new();
    
    if node.output_column.is_empty() {
        // Mode A: Merge at root level - keep all original columns + add extracted columns
        
        // First, add all original columns from the child schema
        for field in child_schema.fields() {
            let col_expr = col(field.name());
            let physical_expr = datafusion_physical_expr::create_physical_expr(
                &col_expr,
                &df_schema,
                session_state.execution_props(),
            )?;
            proj_exprs.push((physical_expr, field.name().to_string()));
        }
        
        // Then add the extracted JSON fields
        for (expr, name) in extractions {
            let physical_expr = datafusion_physical_expr::create_physical_expr(
                &expr,
                &df_schema,
                session_state.execution_props(),
            )?;
            proj_exprs.push((physical_expr, name));
        }
    } else {
        // Mode B: Add as a struct column with the given name
        
        // First, add all original columns from the child schema
        for field in child_schema.fields() {
            let col_expr = col(field.name());
            let physical_expr = datafusion_physical_expr::create_physical_expr(
                &col_expr,
                &df_schema,
                session_state.execution_props(),
            )?;
            proj_exprs.push((physical_expr, field.name().to_string()));
        }
        
        // Build a named_struct for the extracted columns
        let mut struct_args = Vec::new();
        for (expr, name) in extractions {
            struct_args.push(lit(name.clone()));
            struct_args.push(expr);
        }
        let struct_expr = datafusion_functions::core::expr_fn::named_struct(struct_args);
        
        let physical_expr = datafusion_physical_expr::create_physical_expr(
            &struct_expr,
            &df_schema,
            session_state.execution_props(),
        )?;
        proj_exprs.push((physical_expr, node.output_column.clone()));
    }
    
    let projection_exec = ProjectionExec::try_new(proj_exprs, child_plan)?;
    Ok(Arc::new(projection_exec))
}

/// Compile FirstNonNull using DataFusion's `first_value` aggregate with IGNORE NULLS.
///
/// # Ordering Assumption
///
/// `first_value` returns the first non-null value encountered in input order.
/// This function assumes the input is already ordered correctly (e.g., version DESC
/// for Delta log replay). For example, if scanning log files in order:
///
///   5.json → 4.json → 3.json → ...
///
/// Then `first_value` returns the most recent (highest version) non-null value.
/// This is the correct semantic for extracting protocol/metadata from Delta logs.
fn compile_first_non_null(
    child: &DeclarativePlanNode,
    node: &delta_kernel::plans::FirstNonNullNode,
    _session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let child_plan = compile_plan(child, _session_state)?;
    let schema = child_plan.schema();
    
    // Build aggregate expressions for each column using first_value with IGNORE NULLS
    let aggr_exprs = node.columns.iter()
        .map(|col_name| {
            // Find the column index in the schema
            let col_idx = schema.index_of(col_name).map_err(|e| {
                DfError::PlanCompilation(format!(
                    "Column '{}' not found in schema for FirstNonNull: {}", 
                    col_name, e
                ))
            })?;
            
            // Create physical column expression
            let col_expr: Arc<dyn datafusion_physical_expr::PhysicalExpr> = 
                Arc::new(Column::new(col_name, col_idx));
            
            // Build first_value aggregate with ignore_nulls
            // The data is already ordered (version DESC from upstream), so no ORDER BY needed
            let aggr_expr = AggregateExprBuilder::new(first_value_udaf(), vec![col_expr])
                .schema(schema.clone())
                .alias(col_name)
                .ignore_nulls()
                .build()
                .map_err(|e| DfError::PlanCompilation(format!(
                    "Failed to build first_value aggregate for column '{}': {}", 
                    col_name, e
                )))?;
            
            Ok(Arc::new(aggr_expr))
        })
        .collect::<DfResult<Vec<_>>>()?;
    
    // Empty grouping = global aggregate (single output row with all first non-null values)
    let group_by = PhysicalGroupBy::new_single(vec![]);
    
    // No per-aggregate filters
    let filter_exprs = vec![None; aggr_exprs.len()];
    
    // Create AggregateExec with Single mode (single-pass aggregation)
    let aggregate = AggregateExec::try_new(
        AggregateMode::Single,
        group_by,
        aggr_exprs,
        filter_exprs,
        child_plan.clone(),
        schema.clone(),
    )?;
    
    Ok(Arc::new(aggregate))
}

