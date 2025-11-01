use std::sync::{Arc, LazyLock};

use dashmap::DashSet;
use itertools::Itertools;

use crate::{
    actions::deletion_vector::DeletionVectorDescriptor,
    engine_data::{self, GetData, TypedGetData as _},
    log_replay::FileActionKey,
    schema::{ColumnName, DataType, Schema, SchemaRef},
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, FileMeta, PredicateRef, RowVisitor,
    Snapshot,
};
use crate::{
    expressions::column_name,
    schema::{ArrayType, ColumnNamesAndTypes, MapType, StructField, StructType},
};

pub trait RowFilter: RowVisitor + Send + Sync {
    fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool>;
}

// Implement RowFilter for Arc<T>
impl<T: RowFilter + ?Sized> RowFilter for Arc<T> {
    fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        (**self).filter_row(i, getters)
    }
}

impl<T: RowVisitor + ?Sized> RowVisitor for Arc<T> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        (**self).selected_column_names_and_types()
    }

    fn visit<'a>(&mut self, _row_count: usize, _getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        Ok(())
    }
}

// Could even become a macro:

// impl AddRemoveDedupFilter {
//     fn apply(&self, add_path: String, remove_path_: String) -> bool {
//         self.ref_mut().hashmap.contains_key(add_path)
//     }
//
// make_filter! {
//     AddRemoveDedupFilter
//     apply,
//     (String, String)
// }

// -->
//
// impl Filter for AddRemoveDedupFilter {
//   fn input_types(&self) -> &'static[DataType] {
//     [DataType::String, DataType::String]
//   }
//
//   fn execute<'b>(&self, row_count: usize, getters: &[&dyn GetData<'b>]) {
//      let selection_vector = vec![];
//      for i in 0..row_count {
//        selection_vector.push(self.apply(getters.get_string(i), getters.get_string(i)));
//      }
//   }
// }
//
// impl RowFilter for AddRemoveDedupVisitor<'_> {
//     fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
//         // let is_log_batch = self.deduplicator.is_log_batch();
//         // let expected_getters = if is_log_batch { 9 } else { 5 };
//         // require!(
//         //     getters.len() == expected_getters,
//         //     Error::InternalError(format!(
//         //         "Wrong number of AddRemoveDedupVisitor getters: {}",
//         //         getters.len()
//         //     ))
//         // );
//         self.is_valid_add(i, getters)
//     }
// }

pub struct FilterVisitor {
    pub filter: Arc<dyn RowFilter>,
    pub selection_vector: Vec<bool>,
}

impl RowVisitor for FilterVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.filter.selected_column_names_and_types()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // let is_log_batch = self.deduplicator.is_log_batch();
        // let expected_getters = if is_log_batch { 9 } else { 5 };
        // require!(
        //     getters.len() == expected_getters,
        //     Error::InternalError(format!(
        //         "Wrong number of AddRemoveDedupVisitor getters: {}",
        //         getters.len()
        //     ))
        // );

        for i in 0..row_count {
            if self.selection_vector[i] {
                // self.selection_vector[i] = self.is_valid_add(i, getters)?;
                self.selection_vector[i] = self.filter.filter_row(i, getters)?;
            }
        }
        Ok(())
    }
}

// This would replace enginedata
pub struct FilteredEngineDataArc {
    pub engine_data: Arc<dyn EngineData>,
    pub selection_vector: Vec<bool>,
}

pub struct Dataframe {
    pub engine_data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineDataArc>>>,
    pub root_schema: Arc<Schema>,
    pub plan: LogicalPlanNode,
}

#[derive(Debug)]
pub enum FileType {
    Json,
    Parquet,
}
#[derive(Debug)]
pub struct ScanNode {
    pub file_type: FileType,
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
}
impl std::fmt::Debug for FilterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterNode")
            .field("child", &self.child)
            .field("filter", &self.filter.selected_column_names_and_types())
            .field("column_names", &self.column_names)
            .finish()
    }
}
pub struct FilterNode {
    pub child: Box<LogicalPlanNode>,
    pub filter: Arc<dyn RowFilter>,
    pub column_names: &'static [ColumnName],
    pub ordered: bool,
}

#[derive(Debug)]
pub struct SelectNode {
    pub child: Box<LogicalPlanNode>,
    pub columns: Vec<Arc<Expression>>,
    pub input_schema: SchemaRef,
    pub output_type: SchemaRef,
}
#[derive(Debug)]
pub struct UnionNode {
    pub a: Box<LogicalPlanNode>,
    pub b: Box<LogicalPlanNode>,
}
#[derive(Debug)]
pub enum LogicalPlanNode {
    Scan(ScanNode),
    Filter(FilterNode),
    Select(SelectNode),
    Union(UnionNode),
    Custom(CustomNode),
    /// Run multiple visitors on the same data stream
    /// Useful for extracting multiple pieces of data (e.g., dedup + sidecar collection)
    DataVisitor(DataVisitorNode),
    /// Parse a JSON column into structured data
    ParseJson(ParseJsonNode),
    /// Filter by evaluating a predicate expression
    FilterByExpression(FilterByExpressionNode),
    /// List files from storage (matches StorageHandler::list_from)
    FileListing(FileListingNode),
    /// Extract first non-null value for specified columns
    FirstNonNull(FirstNonNullNode),
}
#[derive(Debug)]
struct CustomNode {
    node_type: CustomImpl,
    child: Box<LogicalPlanNode>,
}

#[derive(Debug)]
pub enum CustomImpl {
    AddRemoveDedup,
    StatsSkipping,
}

/// Node that runs multiple visitors on the same data stream
pub struct DataVisitorNode {
    pub child: Box<LogicalPlanNode>,
    pub visitors: Vec<Arc<dyn RowVisitor + Send + Sync>>,
}

impl std::fmt::Debug for DataVisitorNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataVisitorNode")
            .field("child", &self.child)
            .field("num_visitors", &self.visitors.len())
            .finish()
    }
}

/// Node that parses a JSON column into structured data
#[derive(Debug)]
pub struct ParseJsonNode {
    pub child: Box<LogicalPlanNode>,
    pub json_column: ColumnName,      // Column containing JSON string (e.g., "add.stats")
    pub target_schema: SchemaRef,     // Schema to parse JSON into
    pub output_column: String,        // Name for the parsed column
}

/// Node that filters rows by evaluating a predicate expression
#[derive(Debug)]
pub struct FilterByExpressionNode {
    pub child: Box<LogicalPlanNode>,
    pub predicate: PredicateRef,      // Predicate to evaluate (e.g., "minValues.x > 10")
}

/// Node that lists files from storage (matches StorageHandler::list_from)
#[derive(Debug)]
pub struct FileListingNode {
    pub path: url::Url,  // Either directory path ending in '/' or file path to start after
}

/// Node that extracts first non-null value for specified columns
#[derive(Debug)]
pub struct FirstNonNullNode {
    pub child: Box<LogicalPlanNode>,
    pub columns: Vec<String>,  // Column names to extract first non-null for
}

type FallibleFilteredDataIter = Box<dyn Iterator<Item = DeltaResult<FilteredEngineDataArc>>>;

pub trait PhysicalPlanExecutor {
    fn execute(&self, plan: LogicalPlanNode) -> DeltaResult<FallibleFilteredDataIter>;
}

pub struct DefaultPlanExecutor {
    pub engine: Arc<dyn Engine>,
}

impl DefaultPlanExecutor {
    fn execute_scan(&self, node: ScanNode) -> DeltaResult<FallibleFilteredDataIter> {
        let ScanNode {
            file_type,
            files,
            schema,
        } = node;

        match file_type {
            FileType::Json => {
                let json_handler = self.engine.as_ref().json_handler();
                let files = json_handler.read_json_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files.map_ok(|file| {
                    let engine_data: Arc<dyn EngineData> = file.into();
                    FilteredEngineDataArc {
                        selection_vector: vec![true; engine_data.len()],
                        engine_data,
                    }
                })))
            }
            FileType::Parquet => {
                let parquet_handler = self.engine.as_ref().parquet_handler();
                let files = parquet_handler.read_parquet_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files.map_ok(|file| {
                    let engine_data: Arc<dyn EngineData> = file.into();
                    FilteredEngineDataArc {
                        selection_vector: vec![true; engine_data.len()],
                        engine_data,
                    }
                })))
            }
        }
    }

    fn execute_filter(&self, node: FilterNode) -> DeltaResult<FallibleFilteredDataIter> {
        let FilterNode {
            child,
            filter,
            column_names,
            ordered,
        } = node;
        let child_iter = self.execute(*child)?;

        let filtered_iter = child_iter.map(move |x| {
            let FilteredEngineDataArc {
                engine_data,
                selection_vector,
            } = x?;
            let filter_clone = filter.clone();
            let mut visitor = FilterVisitor {
                filter: filter_clone,
                selection_vector,
            };
            engine_data.visit_rows(column_names, &mut visitor)?;
            Ok(FilteredEngineDataArc {
                engine_data,
                selection_vector: visitor.selection_vector,
            })
        });
        Ok(Box::new(filtered_iter))
    }

    fn execute_union(&self, node: UnionNode) -> DeltaResult<FallibleFilteredDataIter> {
        let UnionNode { a, b } = node;
        Ok(Box::new(self.execute(*a)?.chain(self.execute(*b)?)))
    }

    fn execute_select(&self, node: SelectNode) -> DeltaResult<FallibleFilteredDataIter> {
        let SelectNode {
            child,
            columns,
            input_schema,
            output_type,
        } = node;

        let eval_handler = self.engine.evaluation_handler();
        let evaluator = eval_handler.new_expression_evaluator(
            input_schema,
            Expression::Struct(columns).into(),
            output_type.into(),
        );

        let child_iter = self.execute(*child)?;
        let res = child_iter.map(move |x| {
            let FilteredEngineDataArc {
                engine_data,
                selection_vector,
            } = x?;
            let new_data = evaluator.evaluate(engine_data.as_ref())?;

            Ok(FilteredEngineDataArc {
                engine_data: new_data.into(),
                selection_vector,
            })
        });
        Ok(Box::new(res))
    }
    fn execute_custom(&self, _node: CustomNode) -> DeltaResult<FallibleFilteredDataIter> {
        return Err(Error::generic("No custom operators implemented"));
    }
}

impl PhysicalPlanExecutor for DefaultPlanExecutor {
    fn execute(
        &self,
        plan: LogicalPlanNode,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FilteredEngineDataArc>>>> {
        match plan {
            LogicalPlanNode::Scan(scan_node) => self.execute_scan(scan_node),
            LogicalPlanNode::Filter(filter_node) => self.execute_filter(filter_node),
            LogicalPlanNode::Select(select_node) => self.execute_select(select_node),
            LogicalPlanNode::Custom(custom_node) => self.execute_custom(custom_node),
            LogicalPlanNode::Union(union_node) => self.execute_union(union_node),
            LogicalPlanNode::DataVisitor(_) => todo!("DataVisitor not yet implemented in DefaultPlanExecutor"),
            LogicalPlanNode::ParseJson(parse_node) => self.execute_parse_json(parse_node),
            LogicalPlanNode::FilterByExpression(filter_node) => self.execute_filter_by_expression(filter_node),
            LogicalPlanNode::FileListing(listing_node) => self.execute_file_listing(listing_node),
            LogicalPlanNode::FirstNonNull(first_node) => self.execute_first_non_null(first_node),
        }
    }
}

impl DefaultPlanExecutor {
    fn execute_filter_by_expression(&self, node: FilterByExpressionNode) -> DeltaResult<FallibleFilteredDataIter> {
        let child_iter = self.execute(*node.child)?;
        let predicate = node.predicate;
        let eval_handler = self.engine.evaluation_handler();
        
        Ok(Box::new(child_iter.map(move |batch_result| {
            let batch = batch_result?;
            
            // Get schema from batch
            use crate::engine::arrow_conversion::TryFromArrow;
            use crate::schema::Schema;
            
            let arrow_data = batch.engine_data.any_ref()
                .downcast_ref::<crate::engine::arrow_data::ArrowEngineData>()
                .ok_or_else(|| Error::generic("Expected ArrowEngineData"))?;
            let input_schema: SchemaRef = Schema::try_from_arrow(arrow_data.record_batch().schema())?.into();
            
            // Create predicate evaluator
            let evaluator = eval_handler.new_predicate_evaluator(
                input_schema.clone(),
                predicate.clone(),
            );
            
            // Evaluate predicate to get boolean results
            let predicate_result = evaluator.evaluate(batch.engine_data.as_ref())?;
            
            // Create filter to convert predicate result to selection vector
            // Use DISTINCT(result, false) - keeps rows where result is true or null
            let filter_pred = Arc::new(crate::Predicate::distinct(
                crate::Expression::column(["output"]),
                crate::Expression::literal(false),
            ));
            let filter_eval = eval_handler.new_predicate_evaluator(
                input_schema.clone(),
                filter_pred,
            );
            let selection_result = filter_eval.evaluate(predicate_result.as_ref())?;
            
            // Convert to Vec<bool>
            use crate::engine_data::RowVisitor;
            use crate::actions::visitors::SelectionVectorVisitor;
            
            let mut visitor = SelectionVectorVisitor::default();
            visitor.visit_rows_of(selection_result.as_ref())?;
            
            // Combine with existing selection vector
            let combined_selection: Vec<bool> = batch.selection_vector.iter()
                .zip(visitor.selection_vector.iter().chain(std::iter::repeat(&true)))
                .map(|(a, b)| *a && *b)
                .collect();
            
            Ok(FilteredEngineDataArc {
                engine_data: batch.engine_data,
                selection_vector: combined_selection,
            })
        })))
    }
    
    fn execute_parse_json(&self, node: ParseJsonNode) -> DeltaResult<FallibleFilteredDataIter> {
        let child_iter = self.execute(*node.child)?;
        let json_column = node.json_column;
        let target_schema = node.target_schema;
        let json_handler = self.engine.json_handler();
        let eval_handler = self.engine.evaluation_handler();
        
        Ok(Box::new(child_iter.map(move |batch_result| {
            let batch = batch_result?;
            
            // Extract JSON column
            let json_expr = Expression::column(json_column.clone());
            
            // Get input schema from batch
            use crate::engine::arrow_conversion::TryFromArrow;
            use crate::schema::Schema;
            
            let arrow_data = batch.engine_data.any_ref()
                .downcast_ref::<crate::engine::arrow_data::ArrowEngineData>()
                .ok_or_else(|| Error::generic("Expected ArrowEngineData"))?;
            let input_schema: SchemaRef = Schema::try_from_arrow(arrow_data.record_batch().schema())?.into();
            
            let eval = eval_handler.new_expression_evaluator(
                input_schema,
                Arc::new(json_expr),
                DataType::STRING,
            );
            let json_data = eval.evaluate(batch.engine_data.as_ref())?;
            
            // Parse JSON
            let parsed = json_handler.parse_json(json_data, target_schema.clone())?;
            
            // Convert parsed to ArrayData for append_columns
            use crate::expressions::ArrayData;
            let parsed_arrow = parsed.any_ref()
                .downcast_ref::<crate::engine::arrow_data::ArrowEngineData>()
                .ok_or_else(|| Error::generic("Expected ArrowEngineData"))?;
            
            let mut parsed_columns = vec![];
            for field in target_schema.fields() {
                let col_idx = parsed_arrow.record_batch()
                    .schema()
                    .column_with_name(field.name())
                    .ok_or_else(|| Error::generic(format!("Missing field {}", field.name())))?
                    .0;
                let array = parsed_arrow.record_batch().column(col_idx);
                
                // Convert Arrow array to kernel ArrayData by extracting scalars
                use crate::expressions::Scalar;
                use crate::arrow::array::{Array, AsArray};
                
                let mut scalar_values = vec![];
                for i in 0..array.len() {
                    let scalar = if array.is_null(i) {
                        Scalar::Null(field.data_type().clone())
                    } else {
                        // Extract scalar based on field type
                        match field.data_type() {
                            &DataType::LONG => {
                                let arr = array.as_primitive::<crate::arrow::datatypes::Int64Type>();
                                Scalar::from(arr.value(i))
                            }
                            DataType::Struct(struct_type) => {
                                // For struct, recursively convert
                                let struct_arr = array.as_struct();
                                let mut struct_values = vec![];
                                for (idx, nested_field) in struct_type.fields().enumerate() {
                                    let nested_array = struct_arr.column(idx);
                                    if nested_array.is_null(i) {
                                        struct_values.push(Scalar::Null(nested_field.data_type().clone()));
                                    } else {
                                        // Extract nested value based on type
                                        let nested_scalar = match nested_field.data_type() {
                                            &DataType::LONG => {
                                                let arr = nested_array.as_primitive::<crate::arrow::datatypes::Int64Type>();
                                                Scalar::from(arr.value(i))
                                            }
                                            &DataType::DOUBLE => {
                                                let arr = nested_array.as_primitive::<crate::arrow::datatypes::Float64Type>();
                                                Scalar::from(arr.value(i))
                                            }
                                            _ => Scalar::Null(nested_field.data_type().clone()),
                                        };
                                        struct_values.push(nested_scalar);
                                    }
                                }
                                Scalar::Struct(crate::expressions::StructData::try_new(
                                    struct_type.fields().cloned().collect(),
                                    struct_values,
                                )?)
                            }
                            _ => Scalar::Null(field.data_type().clone()),
                        }
                    };
                    scalar_values.push(scalar);
                }
                
                let array_type = crate::schema::ArrayType::new(field.data_type().clone(), field.is_nullable());
                let array_data = ArrayData::try_new(array_type, scalar_values)?;
                parsed_columns.push(array_data);
            }
            
            // Append parsed columns to original batch
            let merged = batch.engine_data.append_columns(target_schema.clone(), parsed_columns)?;
            
            Ok(FilteredEngineDataArc {
                engine_data: Arc::from(merged),
                selection_vector: batch.selection_vector,
            })
        })))
    }
    
    fn execute_file_listing(&self, node: FileListingNode) -> DeltaResult<FallibleFilteredDataIter> {
        use crate::FileMeta;
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::arrow::array::{RecordBatch, StringArray, Int64Array};
        use crate::arrow::datatypes::{Schema as ArrowSchema, Field as ArrowField, DataType as ArrowDataType};
        
        // Call storage handler to list files
        let storage = self.engine.storage_handler();
        let file_meta_iter = storage.list_from(&node.path)?;
        
        // Convert Iterator<FileMeta> → Arrow batches
        // Schema: {location: Utf8, last_modified: Int64, size: Int64}
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("location", ArrowDataType::Utf8, false),
            ArrowField::new("last_modified", ArrowDataType::Int64, false),
            ArrowField::new("size", ArrowDataType::Int64, false),
        ]));
        
        // Collect all files (we need to materialize for batch creation)
        let files: Vec<FileMeta> = file_meta_iter.collect::<Result<Vec<_>, _>>()?;
        
        // Build Arrow arrays
        let locations: StringArray = files.iter().map(|f| Some(f.location.as_str())).collect();
        let last_modified: Int64Array = files.iter().map(|f| Some(f.last_modified)).collect();
        let sizes: Int64Array = files.iter().map(|f| Some(f.size as i64)).collect();
        
        let record_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(locations),
                Arc::new(last_modified),
                Arc::new(sizes),
            ],
        )?;
        
        let engine_data = Arc::new(ArrowEngineData::new(record_batch));
        let selection_vector = vec![true; files.len()];
        
        Ok(Box::new(std::iter::once(Ok(FilteredEngineDataArc {
            engine_data,
            selection_vector,
        }))))
    }
    
    fn execute_first_non_null(&self, node: FirstNonNullNode) -> DeltaResult<FallibleFilteredDataIter> {
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::arrow::array::RecordBatch;
        
        let child_iter = self.execute(*node.child)?;
        let columns_to_extract = node.columns;
        
        // Collect all batches and find first non-null for each column
        let mut all_batches = vec![];
        for batch_result in child_iter {
            all_batches.push(batch_result?);
        }
        
        if all_batches.is_empty() {
            return Err(Error::generic("FirstNonNull requires at least one batch"));
        }
        
        // Get schema from first batch
        let first_batch = &all_batches[0];
        let arrow_data = first_batch.engine_data.any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::generic("Expected ArrowEngineData"))?;
        let schema = arrow_data.record_batch().schema();
        
        // For each column, find first non-null value
        let mut result_arrays: Vec<Arc<dyn crate::arrow::array::Array>> = vec![];
        
        for field in schema.fields() {
            let column_name = field.name();
            
            // Check if this is one of the columns we want to extract first non-null for
            let should_extract = columns_to_extract.iter().any(|c| c == column_name);
            
            if should_extract {
                // Find first non-null value
                let mut found = false;
                let mut first_value: Option<Arc<dyn crate::arrow::array::Array>> = None;
                
                for batch in &all_batches {
                    let arrow_data = batch.engine_data.any_ref()
                        .downcast_ref::<ArrowEngineData>()
                        .ok_or_else(|| Error::generic("Expected ArrowEngineData"))?;
                    let rb = arrow_data.record_batch();
                    
                    if let Some(col) = rb.column_by_name(column_name) {
                        // Check each row in this batch
                        for i in 0..rb.num_rows() {
                            if batch.selection_vector.get(i).copied().unwrap_or(true) && !col.is_null(i) {
                                // Found first non-null! Extract just this one value
                                first_value = Some(col.slice(i, 1));
                                found = true;
                                break;
                            }
                        }
                        if found {
                            break;
                        }
                    }
                }
                
                if let Some(value) = first_value {
                    result_arrays.push(value);
                } else {
                    // No non-null value found, create null array
                    use crate::arrow::array::new_null_array;
                    result_arrays.push(new_null_array(field.data_type(), 1));
                }
            } else {
                // For other columns, just take first value from first batch
                let col = arrow_data.record_batch().column_by_name(column_name)
                    .ok_or_else(|| Error::generic(format!("Column {} not found", column_name)))?;
                result_arrays.push(col.slice(0, 1));
            }
        }
        
        // Create single-row result batch
        let result_batch = RecordBatch::try_new(schema.clone(), result_arrays)?;
        let engine_data = Arc::new(ArrowEngineData::new(result_batch));
        
        Ok(Box::new(std::iter::once(Ok(FilteredEngineDataArc {
            engine_data,
            selection_vector: vec![true],
        }))))
    }
}

#[cfg(test)]
mod parse_json_tests {
    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, StructField, StructType};
    use std::path::PathBuf;
    
    #[test]
    fn test_parse_json_node_basic() -> DeltaResult<()> {
        // Test ParseJson on actual Delta table with stats
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-without-dv-small/",
        )).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = Arc::new(SyncEngine::new());
        let snapshot = crate::Snapshot::builder_for(url).build(engine.as_ref())?;
        
        // Build plan: Scan → ParseJson
        let commit_files: Vec<_> = snapshot.log_segment()
            .ascending_commit_files.iter()
            .map(|p| p.location.clone())
            .collect();
        
        let scan_schema = crate::actions::get_commit_schema()
            .project(&["add"])?;
        
        // Define stats schema (simplified)
        let stats_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("numRecords", DataType::LONG),
        ]));
        
        let plan = LogicalPlanNode::scan_json(commit_files, scan_schema)?
            .parse_json_column(
                crate::expressions::column_name!("add.stats"),
                stats_schema.clone(),
                "parsed_stats",
            )?;
        
        // Verify plan schema includes parsed_stats
        let output_schema = plan.schema();
        assert!(output_schema.contains("parsed_stats"), 
            "Output schema should contain parsed_stats column");
        
        println!("✅ ParseJson node created successfully");
        println!("   Output schema contains: {:?}", 
            output_schema.fields().map(|f| f.name()).collect::<Vec<_>>());
        
        // Execute the plan
        let executor = DefaultPlanExecutor { engine };
        let results: Vec<_> = executor.execute(plan)?.collect::<Result<Vec<_>, _>>()?;
        
        println!("✅ ParseJson executed successfully");
        println!("   Produced {} batches", results.len());
        
        // Verify we got parsed stats data
        assert!(!results.is_empty(), "Should produce at least one batch");
        
        Ok(())
    }
}

impl LogicalPlanNode {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlanNode::Scan(scan_node) => scan_node.schema.clone(),
            LogicalPlanNode::Filter(filter_node) => filter_node.child.schema(),
            LogicalPlanNode::Select(select_node) => select_node.output_type.clone(),
            LogicalPlanNode::Union(union_node) => union_node.a.schema().clone(),
            LogicalPlanNode::Custom(_custom_node) => todo!(),
            LogicalPlanNode::DataVisitor(visitor_node) => visitor_node.child.schema(),
            LogicalPlanNode::ParseJson(parse_node) => {
                // Child schema + parsed column
                let mut fields: Vec<_> = parse_node.child.schema().fields().cloned().collect();
                fields.push(StructField::nullable(
                    &parse_node.output_column,
                    DataType::Struct(Box::new(parse_node.target_schema.as_ref().clone())),
                ));
                Arc::new(StructType::new_unchecked(fields))
            }
            LogicalPlanNode::FilterByExpression(filter_node) => filter_node.child.schema(),
            LogicalPlanNode::FileListing(_) => {
                // Returns FileMeta schema: {location: Utf8, last_modified: Int64, size: Int64}
                Arc::new(StructType::new_unchecked([
                    StructField::new("location", DataType::STRING, false),
                    StructField::new("last_modified", DataType::LONG, false),
                    StructField::new("size", DataType::LONG, false),
                ]))
            }
            LogicalPlanNode::FirstNonNull(first_node) => first_node.child.schema(),
        }
    }
    fn select(self, columns: Vec<ExpressionRef>) -> DeltaResult<Self> {
        let input_schema = self.schema();
        let output_type =
            Expression::Struct(columns.clone()).data_type(Some(input_schema.clone()))?;
        let DataType::Struct(output_schema) = output_type else {
            panic!("should be struct");
        };

        Ok(Self::Select(SelectNode {
            child: Box::new(self),
            columns,
            input_schema,
            output_type: output_schema.into(),
        }))
    }

    pub fn filter(self, filter: impl RowFilter + 'static) -> DeltaResult<Self> {
        let column_names = filter.selected_column_names_and_types().0;
        Ok(Self::Filter(FilterNode {
            child: Box::new(self),
            filter: Arc::new(filter),
            column_names,
            ordered: false,
        }))
    }

    pub fn filter_ordered(self, filter: impl RowFilter + 'static) -> DeltaResult<Self> {
        let column_names = filter.selected_column_names_and_types().0;
        Ok(Self::Filter(FilterNode {
            child: Box::new(self),
            filter: Arc::new(filter),
            column_names,
            ordered: true,
        }))
    }

    pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> DeltaResult<Self> {
        Ok(Self::Scan(ScanNode {
            file_type: FileType::Json,
            files,
            schema,
        }))
    }
    
    pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> DeltaResult<Self> {
        Ok(Self::Scan(ScanNode {
            file_type: FileType::Parquet,
            files,
            schema,
        }))
    }
    
    pub fn with_visitors(self, visitors: Vec<Arc<dyn RowVisitor + Send + Sync>>) -> DeltaResult<Self> {
        Ok(Self::DataVisitor(DataVisitorNode {
            child: Box::new(self),
            visitors,
        }))
    }
    
    /// Parse a JSON column into structured data.
    ///
    /// # Arguments
    /// * `json_column` - Name of the column containing JSON string (e.g., "add.stats")
    /// * `target_schema` - Schema to parse the JSON into
    /// * `output_name` - Name for the new parsed column
    pub fn parse_json_column(
        self,
        json_column: impl Into<ColumnName>,
        target_schema: SchemaRef,
        output_name: impl Into<String>,
    ) -> DeltaResult<Self> {
        Ok(Self::ParseJson(ParseJsonNode {
            child: Box::new(self),
            json_column: json_column.into(),
            target_schema,
            output_column: output_name.into(),
        }))
    }
    
    /// Filter by evaluating a predicate expression.
    ///
    /// Unlike `filter()` which uses a RowFilter visitor, this evaluates
    /// a Predicate expression using the engine's predicate evaluator.
    pub fn filter_by_expression(self, predicate: PredicateRef) -> DeltaResult<Self> {
        Ok(Self::FilterByExpression(FilterByExpressionNode {
            child: Box::new(self),
            predicate,
        }))
    }

    fn union(self, other: LogicalPlanNode) -> DeltaResult<Self> {
        if self.schema() != other.schema() {
            return Err(Error::generic("schema mismatch in union"));
        }
        Ok(Self::Union(UnionNode {
            a: self.into(),
            b: other.into(),
        }))
    }
}

impl Snapshot {
    pub fn get_scan_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let mut json_paths = self
            .log_segment()
            .ascending_commit_files
            .iter()
            .map(|log_path| log_path.location.clone())
            .collect_vec();
        json_paths.reverse();
        use crate::actions::get_commit_schema;
        let commit_schema = get_commit_schema().project(&["add", "remove"])?;
        let json_scan = LogicalPlanNode::scan_json(json_paths, commit_schema.clone())?;

        let parquet_paths = self
            .log_segment()
            .checkpoint_parts
            .iter()
            .map(|log_path| log_path.location.clone())
            .collect_vec();
        let parquet_scan =
            LogicalPlanNode::scan_parquet(parquet_paths, commit_schema)?;

        let set = Arc::new(DashSet::new());
        let x = SharedAddRemoveDedupFilter::new(set.clone(), json_scan.schema(), true);
        let y = SharedAddRemoveDedupFilter::new(set, parquet_scan.schema(), false);
        json_scan.filter_ordered(x)?.union(parquet_scan.filter(y)?)
    }
}

#[derive(Clone)]
pub struct SharedFileActionDeduplicator {
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log for deduplication. This is a mutable reference to the set
    /// of seen file keys that persists across multiple log batches.
    seen_file_keys: Arc<DashSet<FileActionKey>>,
    // TODO: Consider renaming to `is_commit_batch`, `deduplicate_batch`, or `save_batch`
    // to better reflect its role in deduplication logic.
    /// Whether we're processing a log batch (as opposed to a checkpoint)
    is_log_batch: bool,
    /// Index of the getter containing the add.path column
    add_path_index: usize,
    /// Index of the getter containing the remove.path column
    remove_path_index: usize,
    /// Starting index for add action deletion vector columns
    add_dv_start_index: usize,
    /// Starting index for remove action deletion vector columns
    remove_dv_start_index: usize,
}

impl SharedFileActionDeduplicator {
    pub(crate) fn new(
        seen_file_keys: Arc<DashSet<FileActionKey>>,
        is_log_batch: bool,
        add_path_index: usize,
        remove_path_index: usize,
        add_dv_start_index: usize,
        remove_dv_start_index: usize,
    ) -> Self {
        Self {
            seen_file_keys,
            is_log_batch,
            add_path_index,
            remove_path_index,
            add_dv_start_index,
            remove_dv_start_index,
        }
    }

    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    /// Returns `true` if we have seen the file and should ignore it, `false` if we have not seen it
    /// and should process it.
    pub(crate) fn check_and_record_seen(&self, key: FileActionKey) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        if self.seen_file_keys.contains(&key) {
            // println!(
            //     "Ignoring duplicate ({}, {:?}) in scan, is log {}",
            //     key.path, key.dv_unique_id, self.is_log_batch
            // );
            true
        } else {
            // println!(
            //     "Including ({}, {:?}) in scan, is log {}",
            //     key.path, key.dv_unique_id, self.is_log_batch
            // );
            if self.is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen_file_keys.insert(key);
            }
            false
        }
    }

    /// Extracts the deletion vector unique ID if it exists.
    ///
    /// This function retrieves the necessary fields for constructing a deletion vector unique ID
    /// by accessing `getters` at `dv_start_index` and the following two indices. Specifically:
    /// - `dv_start_index` retrieves the storage type (`deletionVector.storageType`).
    /// - `dv_start_index + 1` retrieves the path or inline deletion vector (`deletionVector.pathOrInlineDv`).
    /// - `dv_start_index + 2` retrieves the optional offset (`deletionVector.offset`).
    fn extract_dv_unique_id<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        dv_start_index: usize,
    ) -> DeltaResult<Option<String>> {
        match getters[dv_start_index].get_opt(i, "deletionVector.storageType")? {
            Some(storage_type) => {
                let path_or_inline =
                    getters[dv_start_index + 1].get(i, "deletionVector.pathOrInlineDv")?;
                let offset = getters[dv_start_index + 2].get_opt(i, "deletionVector.offset")?;

                Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
                    storage_type,
                    path_or_inline,
                    offset,
                )))
            }
            None => Ok(None),
        }
    }

    /// Extracts a file action key and determines if it's an add operation.
    /// This method examines the data at the given index using the provided getters
    /// to identify whether a file action exists and what type it is.
    ///
    /// # Parameters
    /// - `i`: Index position in the data structure to examine
    /// - `getters`: Collection of data getter implementations used to access the data
    /// - `skip_removes`: Whether to skip remove actions when extracting file actions
    ///
    /// # Returns
    /// - `Ok(Some((key, is_add)))`: When a file action is found, returns the key and whether it's an add operation
    /// - `Ok(None)`: When no file action is found
    /// - `Err(...)`: On any error during extraction
    pub(crate) fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<(FileActionKey, bool)>> {
        // Try to extract an add action by the required path column
        if let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.add_dv_start_index)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), true)));
        }

        // The AddRemoveDedupVisitor skips remove actions when extracting file actions from a checkpoint batch.
        if skip_removes {
            return Ok(None);
        }

        // Try to extract a remove action by the required path column
        if let Some(path) = getters[self.remove_path_index].get_str(i, "remove.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, self.remove_dv_start_index)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), false)));
        }

        // No file action found
        Ok(None)
    }

    /// Returns whether we are currently processing a log batch.
    ///
    /// `true` indicates we are processing a batch from a commit file.
    /// `false` indicates we are processing a batch from a checkpoint.
    pub(crate) fn is_log_batch(&self) -> bool {
        self.is_log_batch
    }
}

/// Dedup filter for COMMIT files - mutates tombstone set
pub struct CommitDedupFilter {
    deduplicator: SharedFileActionDeduplicator,
}

impl CommitDedupFilter {
    pub fn new(tombstone_set: Arc<DashSet<FileActionKey>>) -> Self {
        Self {
            deduplicator: SharedFileActionDeduplicator::new(
                tombstone_set,
                true, // is_log_batch
                0, // add_path_index
                5, // remove_path_index  
                2, // add_dv_start
                6, // remove_dv_start
            ),
        }
    }

    pub fn tombstone_set(&self) -> Arc<DashSet<FileActionKey>> {
        self.deduplicator.seen_file_keys.clone()
    }
}

impl RowFilter for CommitDedupFilter {
    fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(i, getters, false)? else {
            return Ok(false);
        };
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        Ok(true)
    }
}

impl RowVisitor for CommitDedupFilter {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (ss_map, column_name!("add.partitionValues")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, _row_count: usize, _getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        Ok(())
    }
}

/// Dedup filter for CHECKPOINT files - reads frozen tombstone set
pub struct CheckpointDedupFilter {
    deduplicator: SharedFileActionDeduplicator,
}

impl CheckpointDedupFilter {
    pub fn new(tombstone_set: Arc<DashSet<FileActionKey>>) -> Self {
        Self {
            deduplicator: SharedFileActionDeduplicator::new(
                tombstone_set,
                false, // is_log_batch (read-only)
                0, // add_path_index
                0, // remove_path_index (unused)
                2, // add_dv_start
                0, // remove_dv_start (unused)
            ),
        }
    }
}

impl RowFilter for CheckpointDedupFilter {
    fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(i, getters, true)? else {
            return Ok(false);
        };
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        Ok(true)
    }
}

impl RowVisitor for CheckpointDedupFilter {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (ss_map, column_name!("add.partitionValues")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, _row_count: usize, _getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        Ok(())
    }
}

// Keep SharedAddRemoveDedupFilter for backward compat
impl RowFilter for SharedAddRemoveDedupFilter {
    fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        self.is_valid_add(i, getters)
    }
}

pub struct SharedAddRemoveDedupFilter {
    deduplicator: SharedFileActionDeduplicator,
    logical_schema: SchemaRef,
}

impl SharedAddRemoveDedupFilter {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_PARTITION_VALUES_INDEX: usize = 1; // Position of "add.partitionValues" in getters
    const ADD_DV_START_INDEX: usize = 2; // Start position of add deletion vector columns
    const REMOVE_PATH_INDEX: usize = 5; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 6; // Start position of remove deletion vector columns

    pub fn new(
        seen: Arc<DashSet<FileActionKey>>,
        logical_schema: SchemaRef,
        is_log_batch: bool,
    ) -> SharedAddRemoveDedupFilter {
        SharedAddRemoveDedupFilter {
            deduplicator: SharedFileActionDeduplicator::new(
                seen,
                is_log_batch,
                Self::ADD_PATH_INDEX,
                Self::REMOVE_PATH_INDEX,
                Self::ADD_DV_START_INDEX,
                Self::REMOVE_DV_START_INDEX,
            ),
            logical_schema,
        }
    }

    /// Get the tombstone set (set of seen file action keys).
    /// Used by phases to extract accumulated state for transitions.
    pub fn tombstone_set(&self) -> Arc<DashSet<FileActionKey>> {
        self.deduplicator.seen_file_keys.clone()
    }

    /// True if this row contains an Add action that should survive log replay. Skip it if the row
    /// is not an Add action, or the file has already been seen previously.
    pub fn is_valid_add<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        // When processing file actions, we extract path and deletion vector information based on action type:
        // - For Add actions: path is at index 0, followed by DV fields at indexes 2-4
        // - For Remove actions (in log batches only): path is at index 5, followed by DV fields at indexes 6-8
        // The file extraction logic selects the appropriate indexes based on whether we found a valid path.
        // Remove getters are not included when visiting a non-log batch (checkpoint batch), so do
        // not try to extract remove actions in that case.
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(
            i,
            getters,
            !self.deduplicator.is_log_batch(), // skip_removes. true if this is a checkpoint batch
        )?
        else {
            return Ok(false);
        };

        // Check both adds and removes (skipping already-seen), but only transform and return adds
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        Ok(true)
    }
}

impl RowVisitor for SharedAddRemoveDedupFilter {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The visitor assumes a schema with adds first and removes optionally afterward.
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (ss_map, column_name!("add.partitionValues")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        let (names, types) = NAMES_AND_TYPES.as_ref();
        if self.deduplicator.is_log_batch() {
            (names, types)
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So we only need to examine the adds here.
            (&names[..5], &types[..5])
        }
    }

    fn visit<'a>(&mut self, _row_count: usize, _getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // Note: This visit method is not actually called when using filter_row().
        // The FilterVisitor in kernel_df calls filter_row() directly for each row.
        // This is just a placeholder to satisfy the RowVisitor trait.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use object_store::local::LocalFileSystem;
    use std::process::ExitCode;
    use test_utils::{load_test_data, DefaultEngineExtension};
    use tokio::runtime::Runtime;

    use crate::arrow::compute::filter_record_batch;
    use crate::arrow::record_batch::RecordBatch;
    use crate::arrow::util::pretty::print_batches;
    use crate::engine::default::executor::tokio::TokioMultiThreadExecutor;
    use crate::engine::default::DefaultEngine;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::{DeltaResult, Snapshot};

    use itertools::process_results;
    use itertools::Itertools;

    use super::PhysicalPlanExecutor;
    use crate::{engine::sync::SyncEngine, kernel_df::DefaultPlanExecutor};

    #[test]
    fn test_scan_plan() {
        let path =
            std::fs::canonicalize(PathBuf::from("/Users/oussama/projects/code/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let task_executor = TokioMultiThreadExecutor::new(tokio::runtime::Handle::current());
        let object_store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::new(object_store, task_executor.into());

        let snapshot = crate::Snapshot::builder_for(url).build(&engine).unwrap();
        let plan = snapshot.get_scan_plan().unwrap();
        let executor = DefaultPlanExecutor {
            engine: Arc::new(engine),
        };
        let res_iter = executor.execute(plan).unwrap();
        process_results(res_iter, |data| {
            let batches = data
                .map(|batch| {
                    let mask = batch.selection_vector;
                    let data = batch.engine_data;
                    let record_batch: RecordBatch = data
                        .as_any()
                        .downcast::<ArrowEngineData>()
                        .unwrap()
                        .record_batch()
                        .clone();
                    filter_record_batch(&record_batch, &mask.into()).unwrap()
                })
                .collect_vec();
            print_batches(&batches).unwrap();
        })
        .unwrap();
    }
}
