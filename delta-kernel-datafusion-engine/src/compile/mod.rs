//! Kernel plan -> DataFusion [`LogicalPlan`] compilation.
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan

use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use delta_kernel::expressions::Expression;
use delta_kernel::Engine;

use crate::error::plan_compilation;

pub mod expr_translator;
mod json_parse;
pub mod logical;
pub mod stamp_udf;

/// Context shared by the compiler for leaf nodes that need runtime side state.
///
/// Carries only static / shared bits -- there is no per-step mutable accumulator
/// here. Drained reducer state for `Reduce` steps flows directly out of the
/// `DataFusionExecutor::execute_step` as an [`EngineResponse::Reducer`] after the
/// executor finishes the sink locally. (The executor lives in a downstream module.)
///
/// [`EngineResponse::Reducer`]: delta_kernel::plans::state_machines::framework::state_machine::EngineResponse::Reducer
#[derive(Clone)]
pub struct CompileContext {
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers
    /// (`NodeKind::Load`).
    pub engine: Arc<dyn Engine>,
}

impl CompileContext {
    /// Build a context for SM-less inspection / standalone driving (benchmark plan printers,
    /// integration tests that lower a `ResultPlan` directly).
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self { engine }
    }
}

pub(super) fn expand_projection_columns(
    columns: &[Arc<Expression>],
    expected_output_fields: usize,
) -> Result<Vec<Arc<Expression>>, DataFusionError> {
    let mut expanded = Vec::new();
    for (idx, expr) in columns.iter().enumerate() {
        let remaining_output = expected_output_fields
            .checked_sub(expanded.len())
            .ok_or_else(|| plan_compilation("Projection expansion overflow"))?;
        let remaining_expr = columns.len() - idx;
        let extra_needed = remaining_output
            .checked_sub(remaining_expr)
            .ok_or_else(|| {
                plan_compilation(format!(
                    "Projection has too many expressions: expected \
                     {expected_output_fields} output fields, got at least {}",
                    expanded.len() + remaining_expr
                ))
            })?;

        match expr.as_ref() {
            Expression::Struct(children, _) => {
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
        return Err(plan_compilation(format!(
            "Projection output schema has {} fields but expanded to {} expressions",
            expected_output_fields,
            expanded.len()
        )));
    }
    Ok(expanded)
}
