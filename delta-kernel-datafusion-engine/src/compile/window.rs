//! Compile [`WindowNode`] (Phase 1.5: `row_number` + stream order only).

use std::sync::Arc;

use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{WindowFunction, WindowNode};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

use crate::exec::KernelRowNumberWindowExec;

pub(crate) fn window_output_kernel_schema(
    input_schema: &SchemaRef,
    node: &WindowNode,
) -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<StructField> = input_schema.fields().cloned().collect();
    for wf in &node.functions {
        fields.push(StructField::new(
            wf.output_col.clone(),
            DataType::LONG,
            false,
        ));
    }
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| crate::error::plan_compilation(format!("window output schema: {e}")))
}

pub(crate) fn compile_window_node(
    child: Arc<dyn ExecutionPlan>,
    input_schema: SchemaRef,
    node: &WindowNode,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    if !node.order_by.is_empty() {
        return Err(crate::error::unsupported(
            "Window with non-empty ORDER BY is not supported by the DataFusion engine yet \
             (empty order_by uses upstream stream order for row_number)",
        ));
    }
    if node.functions.is_empty() {
        return Err(crate::error::plan_compilation(
            "Window node must declare at least one window function",
        ));
    }
    for f in &node.functions {
        validate_row_number_function(f)?;
    }

    let output_schema = window_output_kernel_schema(&input_schema, node)?;
    Ok(Arc::new(KernelRowNumberWindowExec::try_new(
        child,
        input_schema,
        output_schema,
        &node.partition_by,
        node.functions.len(),
    )?))
}

fn validate_row_number_function(f: &WindowFunction) -> Result<(), DeltaError> {
    if !f.function_name.eq_ignore_ascii_case("row_number") {
        return Err(crate::error::unsupported(format!(
            "window function '{}' is not supported (only row_number)",
            f.function_name
        )));
    }
    if !f.args.is_empty() {
        return Err(crate::error::plan_compilation(
            "row_number window function expects empty args",
        ));
    }
    Ok(())
}
