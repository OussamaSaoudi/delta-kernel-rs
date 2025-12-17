//! PartitionPrune Filter KDF - prunes Add actions based on partition values.

use std::sync::Arc;

use crate::arrow::array::BooleanArray;
use crate::{DeltaResult, EngineData};

use super::super::serialization::selection_value_or_true;

/// State for PartitionPrune KDF - prunes Add actions based on partition values.
///
/// This evaluates the scan predicate against directory-derived partition values (from
/// `add.partitionValues`). It is conservative: rows are pruned only when the predicate can be
/// proven false from partition values alone. Non-add rows (e.g. removes) are never pruned.
#[derive(Debug, Clone)]
pub struct PartitionPruneState {
    pub(crate) predicate: crate::expressions::PredicateRef,
    pub(crate) transform_spec: Arc<crate::transforms::TransformSpec>,
    pub(crate) logical_schema: crate::schema::SchemaRef,
    pub(crate) column_mapping_mode: crate::table_features::ColumnMappingMode,
}

impl PartitionPruneState {
    #[inline]
    pub fn apply(
        &self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, KernelPredicateEvaluator as _};
        use crate::schema::{ColumnName, DataType, MapType};
        use crate::transforms::parse_partition_values;
        use std::collections::HashMap;

        struct Visitor<'a> {
            state: &'a PartitionPruneState,
            input_selection: &'a BooleanArray,
            out: Vec<bool>,
        }

        impl crate::engine_data::RowVisitor for Visitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                const STRING: DataType = DataType::STRING;
                crate::column_names_and_types![
                    STRING => crate::schema::column_name!("add.path"),
                    MapType::new(STRING, STRING, true).into() => crate::schema::column_name!("add.partitionValues"),
                ]
            }

            fn visit<'a2>(&mut self, row_count: usize, getters: &[&'a2 dyn GetData<'a2>]) -> DeltaResult<()> {
                self.out.resize(row_count, true);
                for i in 0..row_count {
                    // Treat missing selection entries as true (selected) per FilteredEngineData contract.
                    let selected = selection_value_or_true(self.input_selection, i);
                    if !selected {
                        self.out[i] = false;
                        continue;
                    }

                    // Only prune Add rows. Removes/non-adds should stay selected.
                    let is_add = getters[0].get_str(i, "add.path")?.is_some();
                    if !is_add {
                        self.out[i] = true;
                        continue;
                    }

                    let partition_values: HashMap<String, String> =
                        getters[1].get(i, "add.partitionValues")?;
                    let parsed = parse_partition_values(
                        &self.state.logical_schema,
                        &self.state.transform_spec,
                        &partition_values,
                        self.state.column_mapping_mode,
                    )?;
                    let partition_values_for_eval: HashMap<_, _> = parsed
                        .values()
                        .map(|(k, v)| (ColumnName::new([k]), v.clone()))
                        .collect();
                    let evaluator = DefaultKernelPredicateEvaluator::from(partition_values_for_eval);
                    if evaluator.eval_sql_where(&self.state.predicate) == Some(false) {
                        self.out[i] = false;
                    } else {
                        self.out[i] = true;
                    }
                }
                Ok(())
            }
        }

        let mut visitor = Visitor {
            state: self,
            input_selection: &selection,
            out: vec![],
        };
        visitor.visit_rows_of(batch)?;
        Ok(BooleanArray::from(visitor.out))
    }
}

