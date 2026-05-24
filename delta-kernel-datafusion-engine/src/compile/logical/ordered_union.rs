//! Order-preserving Union lowering.

use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{lit, Expr, LogicalPlanBuilder};

use crate::error::plan_compilation;

/// Build an ordered union of [`LogicalPlan`]s using stock DataFusion operators: project a
/// literal i64 ordinal onto each child, [`LogicalPlanBuilder::union`] the lot, sort by ordinal,
/// then project to drop it. Same recipe as the physical-layer ordered-union helper but emitted
/// at the logical layer so the optimizer sees it.
pub(super) fn compile_ordered_union(
    children: Vec<LogicalPlan>,
) -> Result<LogicalPlan, DataFusionError> {
    const ORD: &str = "__dk_ord";

    // === 1. tag each child with its position ===
    let tagged: Vec<LogicalPlan> = children
        .into_iter()
        .enumerate()
        .map(|(idx, child)| {
            // Pass through every existing column + append `lit(idx) AS __dk_ord`.
            let cols = child.schema().columns();
            let pass_through = cols.into_iter().map(Expr::Column);
            let ordinal = std::iter::once(lit(idx as i64).alias(ORD));
            LogicalPlanBuilder::from(child)
                .project(pass_through.chain(ordinal))?
                .build()
        })
        .collect::<Result<_, _>>()?;

    // === 2. union-reduce ===
    let mut iter = tagged.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| plan_compilation("compile_ordered_union: empty children"))?;
    let unioned = iter.try_fold(first, |acc, right| {
        LogicalPlanBuilder::from(acc).union(right)?.build()
    })?;

    // === 3. sort by ordinal, then project it away ===
    let ord_col = unioned
        .schema()
        .columns()
        .into_iter()
        .find(|c| c.name == ORD)
        .ok_or_else(|| {
            plan_compilation(format!(
                "compile_ordered_union: missing `{ORD}` after union; schema {:?}",
                unioned.schema()
            ))
        })?;
    let sorted = LogicalPlanBuilder::from(unioned)
        .sort(vec![Expr::Column(ord_col).sort(true, true)])?
        .build()?;
    let final_cols = sorted.schema().columns();
    LogicalPlanBuilder::from(sorted)
        .project(
            final_cols
                .into_iter()
                .filter(|c| c.name != ORD)
                .map(Expr::Column),
        )?
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::logical_plan::EmptyRelation;

    fn empty(field_name: &str, dt: DataType) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new(field_name, dt, true)]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        })
    }

    #[test]
    fn empty_children_is_rejected() {
        let err = compile_ordered_union(Vec::new()).unwrap_err();
        assert!(err.to_string().contains("empty children"));
    }

    #[test]
    fn ordered_union_drops_ordinal_column_from_output() {
        let lhs = empty("c0", DataType::Int32);
        let rhs = empty("c0", DataType::Int32);
        let plan = compile_ordered_union(vec![lhs, rhs]).unwrap();
        let cols: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(cols, vec!["c0"], "ordinal must be projected away");
    }
}
