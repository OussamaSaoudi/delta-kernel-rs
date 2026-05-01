//! Declarative join nodes -> DataFusion [`HashJoinExec`].

use std::sync::Arc;

use datafusion_common::{JoinType as DfJoinType, NullEquality};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::expressions::Expression;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{JoinHint, JoinNode, JoinType};
use delta_kernel::plans::ir::DeclarativePlanNode;

use super::{compile_declarative_node, CompileContext};

/// Lower a kernel join to [`HashJoinExec`] for supported join types.
pub(super) fn compile_join(
    build: &DeclarativePlanNode,
    probe: &DeclarativePlanNode,
    node: &JoinNode,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    if node.hint != JoinHint::Hash {
        return Err(crate::error::unsupported(format!(
            "join hint {:?} is not supported by the DataFusion engine (only Hash)",
            node.hint
        )));
    }
    if node.build_keys.is_empty() {
        return Err(crate::error::plan_compilation(
            "join requires non-empty build_keys",
        ));
    }
    if node.build_keys.len() != node.probe_keys.len() {
        return Err(crate::error::plan_compilation(
            "join build_keys and probe_keys must have the same length",
        ));
    }

    let df_join_type = match node.join_type {
        JoinType::Inner => DfJoinType::Inner,
        // Kernel [`JoinType::LeftAnti`] keeps the probe side; DF `LeftAnti` keeps the build (left)
        // side, so use `RightAnti` with HashJoin `(left=build, right=probe)`.
        JoinType::LeftAnti => DfJoinType::RightAnti,
        other => {
            return Err(crate::error::unsupported(format!(
                "join type {:?} is not implemented in the DataFusion engine",
                other
            )));
        }
    };

    let build_plan = compile_declarative_node(build, ctx)?;
    let probe_plan = compile_declarative_node(probe, ctx)?;
    let build_schema = build_plan.schema();
    let probe_schema = probe_plan.schema();

    let mut on = JoinOn::with_capacity(node.build_keys.len());
    for (bk, pk) in node.build_keys.iter().zip(node.probe_keys.iter()) {
        on.push((
            join_key_to_physical(bk, build_schema.as_ref())?,
            join_key_to_physical(pk, probe_schema.as_ref())?,
        ));
    }

    let join_exec = HashJoinExec::try_new(
        build_plan,
        probe_plan,
        on,
        None,
        &df_join_type,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )
    .map_err(crate::error::datafusion_err_to_delta)?;

    Ok(Arc::new(join_exec))
}

fn join_key_to_physical(
    expr: &Arc<Expression>,
    input: &ArrowSchema,
) -> Result<PhysicalExprRef, DeltaError> {
    match expr.as_ref() {
        Expression::Column(name) => {
            let path = name.path();
            if path.len() != 1 {
                return Err(crate::error::unsupported(
                    "only top-level column join keys are supported in the DataFusion engine",
                ));
            }
            let col = Column::new_with_schema(path[0].as_str(), input)
                .map_err(|e| crate::error::plan_compilation(format!("join key column: {e}")))?;
            Ok(Arc::new(col) as PhysicalExprRef)
        }
        _ => Err(crate::error::unsupported(
            "only column expressions are supported as join keys in the DataFusion engine",
        )),
    }
}
