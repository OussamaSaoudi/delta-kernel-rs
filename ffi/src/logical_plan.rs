//! FFI interface for visiting logical plan nodes.
//!
//! Follows the visitor pattern similar to schema and expression visitors.

use std::os::raw::c_void;
use std::sync::Arc;

use delta_kernel::kernel_df::{LogicalPlanNode, RowFilter};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice, SharedSchema};

#[cfg(feature = "default-engine-base")]
use crate::expressions::SharedPredicate;

/// Opaque handle to a logical plan node
#[handle_descriptor(target=LogicalPlanNode, mutable=false, sized=true)]
pub struct SharedLogicalPlan;

/// Opaque handle to a row filter
#[handle_descriptor(target=dyn RowFilter, mutable=false, sized=false)]
pub struct SharedRowFilter;

/// Visitor for traversing logical plan nodes.
///
/// Follows the same pattern as `EngineSchemaVisitor` and `EngineExpressionVisitor`.
/// The visitor builds a C++ representation of the plan through callbacks.
#[repr(C)]
pub struct EnginePlanVisitor {
    /// Opaque engine context
    pub data: *mut c_void,

    /// Allocate a list to hold child plans
    pub make_plan_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,

    /// Visit Scan node (leaf)
    pub visit_scan: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            file_type: u8, // 0 = Parquet, 1 = Json
            file_paths: *const KernelStringSlice,
            num_files: usize,
            schema: Handle<SharedSchema>,
        ),
    >,

    /// Visit Filter node (has child + row filter)
    pub visit_filter: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            child_plan_id: usize,
            filter: Handle<SharedRowFilter>,
        ),
    >,

    /// Visit Select node
    pub visit_select: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            child_plan_id: usize,
            output_schema: Handle<SharedSchema>,
        ),
    >,

    /// Visit Union node
    pub visit_union: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            left_plan_id: usize,
            right_plan_id: usize,
        ),
    >,

    /// Visit ParseJson node
    #[cfg(feature = "default-engine-base")]
    pub visit_parse_json: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            child_plan_id: usize,
            json_column: KernelStringSlice,
            target_schema: Handle<SharedSchema>,
            output_column: KernelStringSlice,
        ),
    >,

    /// Visit FilterByExpression node
    #[cfg(feature = "default-engine-base")]
    pub visit_filter_by_expression: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            child_plan_id: usize,
            predicate: Handle<SharedPredicate>,
        ),
    >,

    /// Visit FileListing node
    #[cfg(feature = "default-engine-base")]
    pub visit_file_listing: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            path: KernelStringSlice,
        ),
    >,

    /// Visit FirstNonNull node
    #[cfg(feature = "default-engine-base")]
    pub visit_first_non_null: Option<
        extern "C" fn(
            data: *mut c_void,
            sibling_list_id: usize,
            child_plan_id: usize,
            column_names: *const KernelStringSlice,
            num_columns: usize,
        ),
    >,
}

/// Visit a logical plan node and build engine representation.
///
/// The visitor will call the appropriate callbacks to allow the C++ engine
/// to build its own representation of the plan.
///
/// # Safety
///
/// - `plan` must be a valid handle to a LogicalPlanNode
/// - `visitor` must have valid function pointers and data pointer
/// - The visitor's `data` pointer will be passed to all callbacks
///
/// # Returns
///
/// The ID of the list containing the root plan node.
#[no_mangle]
pub unsafe extern "C" fn visit_logical_plan(
    plan: Handle<SharedLogicalPlan>,
    visitor: &mut EnginePlanVisitor,
) -> usize {
    let plan = unsafe { plan.as_ref() };
    // Create a list to hold the root plan (reserve 1 slot)
    let root_list_id = (visitor.make_plan_list)(visitor.data, 1);
    visit_plan_impl(plan, visitor, root_list_id)
}

/// Recursive implementation of plan visiting
fn visit_plan_impl(
    plan: &LogicalPlanNode,
    visitor: &mut EnginePlanVisitor,
    sibling_list_id: usize,
) -> usize {
    match plan {
        LogicalPlanNode::Scan(scan) => {
            if let Some(visit_scan) = visitor.visit_scan {
                // Collect file paths
                let paths: Vec<KernelStringSlice> = scan
                    .files
                    .iter()
                    .map(|f| {
                        let location_str = f.location.as_str();
                        kernel_string_slice!(location_str)
                    })
                    .collect();

                let file_type = match scan.file_type {
                    delta_kernel::kernel_df::FileType::Parquet => 0,
                    delta_kernel::kernel_df::FileType::Json => 1,
                };

                visit_scan(
                    visitor.data,
                    sibling_list_id,
                    file_type,
                    paths.as_ptr(),
                    paths.len(),
                    Arc::new(scan.schema.as_ref().clone()).into(),
                );
            }
            sibling_list_id
        }

        LogicalPlanNode::Filter(filter) => {
            if let Some(visit_filter) = visitor.visit_filter {
                // Visit child first
                let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&filter.child, visitor, child_list_id);

                // Create handle for the filter (filter.filter is Arc<dyn RowFilter>)
                let filter_handle = filter.filter.clone().into();
                visit_filter(
                    visitor.data,
                    sibling_list_id,
                    child_list_id,
                    filter_handle,
                );
            }
            sibling_list_id
        }

        LogicalPlanNode::Select(select) => {
            if let Some(visit_select) = visitor.visit_select {
                // Visit child first
                let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&select.child, visitor, child_list_id);

                // Visit this node
                visit_select(
                    visitor.data,
                    sibling_list_id,
                    child_list_id,
                    Arc::new(select.output_type.as_ref().clone()).into(),
                );
            }
            sibling_list_id
        }

        LogicalPlanNode::Union(union) => {
            if let Some(visit_union) = visitor.visit_union {
                // Visit both children
                let left_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&union.a, visitor, left_list_id);

                let right_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&union.b, visitor, right_list_id);

                // Visit this node
                visit_union(
                    visitor.data,
                    sibling_list_id,
                    left_list_id,
                    right_list_id,
                );
            }
            sibling_list_id
        }

        #[cfg(feature = "default-engine-base")]
        LogicalPlanNode::ParseJson(parse_json) => {
            if let Some(visit_parse_json) = visitor.visit_parse_json {
                // Visit child first
                let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&parse_json.child, visitor, child_list_id);

                // Create intermediate variables for macro
                let json_col_string = parse_json.json_column.to_string();
                let json_col = json_col_string.as_str();
                let output_col = parse_json.output_column.as_str();

                // Visit this node
                visit_parse_json(
                    visitor.data,
                    sibling_list_id,
                    child_list_id,
                    kernel_string_slice!(json_col),
                    Arc::new(parse_json.target_schema.as_ref().clone()).into(),
                    kernel_string_slice!(output_col),
                );
            }
            sibling_list_id
        }

        #[cfg(feature = "default-engine-base")]
        LogicalPlanNode::FilterByExpression(filter_expr) => {
            if let Some(visit_filter_by_expression) = visitor.visit_filter_by_expression {
                // Visit child first
                let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&filter_expr.child, visitor, child_list_id);

                // Visit this node
                visit_filter_by_expression(
                    visitor.data,
                    sibling_list_id,
                    child_list_id,
                    filter_expr.predicate.clone().into(),
                );
            }
            sibling_list_id
        }

        #[cfg(feature = "default-engine-base")]
        LogicalPlanNode::FileListing(file_listing) => {
            if let Some(visit_file_listing) = visitor.visit_file_listing {
                let path_str = file_listing.path.as_str();
                visit_file_listing(
                    visitor.data,
                    sibling_list_id,
                    kernel_string_slice!(path_str),
                );
            }
            sibling_list_id
        }

        #[cfg(feature = "default-engine-base")]
        LogicalPlanNode::FirstNonNull(first_non_null) => {
            if let Some(visit_first_non_null) = visitor.visit_first_non_null {
                // Visit child first
                let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
                visit_plan_impl(&first_non_null.child, visitor, child_list_id);

                // Collect column names
                let column_names: Vec<KernelStringSlice> = first_non_null
                    .columns
                    .iter()
                    .map(|name| {
                        let name_str = name.as_str();
                        kernel_string_slice!(name_str)
                    })
                    .collect();

                // Visit this node
                visit_first_non_null(
                    visitor.data,
                    sibling_list_id,
                    child_list_id,
                    column_names.as_ptr(),
                    column_names.len(),
                );
            }
            sibling_list_id
        }

        _ => {
            // Custom nodes and other internal nodes are not exposed to FFI
            // They are internal implementation details
            sibling_list_id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::kernel_df::{FileType, LogicalPlanNode, ScanNode};
    use delta_kernel::schema::{DataType, Schema, StructField, StructType};
    use delta_kernel::FileMeta;
    use std::sync::Arc;
    use url::Url;

    /// Create a simple test plan for FFI testing
    pub(crate) fn create_test_plan() -> LogicalPlanNode {
        // Create a simple schema
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]));

        // Create some file metas
        let files = vec![
            FileMeta {
                location: Url::parse("file:///path/to/file1.parquet").unwrap(),
                last_modified: 0,
                size: 1024,
            },
            FileMeta {
                location: Url::parse("file:///path/to/file2.parquet").unwrap(),
                last_modified: 0,
                size: 2048,
            },
        ];

        LogicalPlanNode::Scan(ScanNode {
            files,
            schema,
            file_type: FileType::Parquet,
        })
    }
}

/// Get a test logical plan for FFI testing
///
/// Creates a complex plan with multiple node types to test the visitor thoroughly.
///
/// # Safety
///
/// The caller must eventually free the returned handle with appropriate cleanup
#[cfg(feature = "test-ffi")]
#[no_mangle]
pub unsafe extern "C" fn get_testing_logical_plan() -> Handle<SharedLogicalPlan> {
    use delta_kernel::kernel_df::{FileType, LogicalPlanNode, ScanNode};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::FileMeta;
    use delta_kernel::{Expression, Predicate};
    use url::Url;

    #[cfg(feature = "default-engine-base")]
    use delta_kernel::kernel_df::{
        FilterByExpressionNode, FirstNonNullNode, ParseJsonNode, SelectNode, UnionNode,
    };

    // Create schemas
    let scan_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
        StructField::new("stats", DataType::STRING, true),
    ]));

    #[cfg(feature = "default-engine-base")]
    let stats_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("numRecords", DataType::LONG, true),
        StructField::new("minValues", DataType::Struct(Box::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, true),
        ]))), true),
    ]));

    #[cfg(feature = "default-engine-base")]
    let select_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ]));

    // Create file metas for first scan
    let files1 = vec![
        FileMeta {
            location: Url::parse("file:///path/to/file1.parquet").unwrap(),
            last_modified: 0,
            size: 1024,
        },
        FileMeta {
            location: Url::parse("file:///path/to/file2.parquet").unwrap(),
            last_modified: 0,
            size: 2048,
        },
    ];

    // Create file metas for second scan (for union)
    let files2 = vec![
        FileMeta {
            location: Url::parse("file:///path/to/file3.json").unwrap(),
            last_modified: 0,
            size: 512,
        },
    ];

    // Build a complex plan:
    // Union(
    //   Select(
    //     FilterByExpression(
    //       ParseJson(
    //         Scan(parquet files)
    //       )
    //     )
    //   ),
    //   FirstNonNull(
    //     Scan(json files)
    //   )
    // )

    #[cfg(feature = "default-engine-base")]
    {
        // Left branch: Scan -> ParseJson -> FilterByExpression -> Select
        let left_scan = LogicalPlanNode::Scan(ScanNode {
            files: files1,
            schema: scan_schema.clone(),
            file_type: FileType::Parquet,
        });

        let parse_json = left_scan.parse_json_column(
            delta_kernel::expressions::column_name!("stats"),
            stats_schema,
            "parsed_stats",
        ).unwrap();

        // Create a simple predicate: id > 10
        let predicate = Arc::new(Predicate::gt(
            Expression::column(delta_kernel::expressions::column_name!("id")),
            Expression::literal(10i32),
        ));

        let filter_by_expr = LogicalPlanNode::FilterByExpression(FilterByExpressionNode {
            child: Box::new(parse_json),
            predicate,
        });

        let select = LogicalPlanNode::Select(SelectNode {
            child: Box::new(filter_by_expr),
            columns: vec![
                Arc::new(Expression::column(delta_kernel::expressions::column_name!("id"))),
                Arc::new(Expression::column(delta_kernel::expressions::column_name!("name"))),
            ],
            input_schema: scan_schema.clone(),
            output_type: select_schema,
        });

        // Right branch: Scan -> FirstNonNull
        let right_scan = LogicalPlanNode::Scan(ScanNode {
            files: files2,
            schema: scan_schema.clone(),
            file_type: FileType::Json,
        });

        let first_non_null = LogicalPlanNode::FirstNonNull(FirstNonNullNode {
            child: Box::new(right_scan),
            columns: vec!["id".to_string(), "name".to_string()],
        });

        // Top level: Union
        let union = LogicalPlanNode::Union(UnionNode {
            a: Box::new(select),
            b: Box::new(first_non_null),
        });

        Arc::new(union).into()
    }

    #[cfg(not(feature = "default-engine-base"))]
    {
        // Fallback to simple scan if default-engine-base not enabled
        let scan = LogicalPlanNode::Scan(ScanNode {
            files: files1,
            schema: scan_schema,
            file_type: FileType::Parquet,
        });
        Arc::new(scan).into()
    }
}

/// Free a logical plan handle
///
/// # Safety
///
/// The caller must pass a valid SharedLogicalPlan handle
#[no_mangle]
pub unsafe extern "C" fn free_logical_plan(plan: Handle<SharedLogicalPlan>) {
    plan.drop_handle();
}

// =============================================================================
// Row Filter FFI Functions
// =============================================================================

/// Get the schema expected by a row filter
///
/// # Safety
///
/// The caller must pass a valid SharedRowFilter handle
///
/// # Returns
///
/// A SharedSchema handle representing the columns and types required by the filter
#[no_mangle]
pub unsafe extern "C" fn row_filter_get_schema(
    filter: Handle<SharedRowFilter>,
) -> Handle<SharedSchema> {
    use delta_kernel::schema::{StructField, StructType};
    
    let filter_ref = unsafe { filter.as_ref() };
    let (names, types) = filter_ref.selected_column_names_and_types();
    
    // Build a StructType from the column names and types
    let fields: Vec<StructField> = names
        .iter()
        .zip(types.iter())
        .map(|(name, data_type)| {
            // Row filters always work with nullable columns (for safety)
            // Convert ColumnName to String using Display trait
            let name_str = name.to_string();
            StructField::new(name_str, data_type.clone(), true)
        })
        .collect();
    
    let schema = Arc::new(StructType::new_unchecked(fields));
    schema.into()
}

/// Free a row filter handle
///
/// # Safety
///
/// The caller must pass a valid SharedRowFilter handle
#[no_mangle]
pub unsafe extern "C" fn free_row_filter(filter: Handle<SharedRowFilter>) {
    filter.drop_handle();
}

/// Apply a row filter to an Arrow batch
///
/// # Safety
///
/// - `filter` must be a valid SharedRowFilter handle
/// - `input_batch` and `input_schema` must be valid Arrow C Data Interface pointers
/// - `selection_vector` can be null, or a valid Arrow boolean array (TRUE = keep row, FALSE = skip row)
/// - `output` must be a valid pointer to write the output boolean array
///
/// # Arguments
///
/// * `filter` - The row filter to apply
/// * `input_batch` - Arrow array containing the input data
/// * `input_schema` - Arrow schema describing the input data
/// * `selection_vector` - Optional pre-filter selection vector (null pointer for no pre-filter)
/// * `output` - Output boolean array (TRUE = row passed filter, FALSE = row filtered out)
///
/// # Returns
///
/// `true` on success, `false` on error (error message printed to stderr for now)
#[no_mangle]
pub unsafe extern "C" fn apply_row_filter(
    filter: Handle<SharedRowFilter>,
    input_batch: *mut delta_kernel::arrow::ffi::FFI_ArrowArray,
    input_schema: *mut delta_kernel::arrow::ffi::FFI_ArrowSchema,
    selection_vector: *mut delta_kernel::arrow::ffi::FFI_ArrowArray,
    output: *mut delta_kernel::arrow::ffi::FFI_ArrowArray,
) -> bool {
    use delta_kernel::arrow::array::{Array, BooleanArray};
    use delta_kernel::arrow::ffi;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::kernel_df::FilterVisitor;
    use delta_kernel::RowVisitor;
    
    let result: Result<(), delta_kernel::Error> = (|| {
        // Get the Arc<dyn RowFilter> from the handle
        // We can't use as_ref because FilterVisitor needs Arc<dyn RowFilter>
        // We need to carefully clone the Arc without consuming the handle
        let filter_arc = unsafe {
            // Get a reference and manually construct an Arc by cloning the inner pointer
            let filter_ref = filter.as_ref();
            // Since we have &dyn RowFilter, we need to reconstruct the Arc
            // This is tricky - we can't clone the Arc from a &dyn RowFilter
            // Instead, let's use the handle's inner Arc directly
            std::mem::ManuallyDrop::new(filter.into_inner()).clone()
        };
        
        // Convert Arrow batch to Rust RecordBatch
        let array_data = unsafe { ffi::from_ffi(input_batch.read(), &*input_schema)? };
        let record_batch = delta_kernel::arrow::array::RecordBatch::from(
            delta_kernel::arrow::array::StructArray::from(array_data)
        );
        
        let num_rows = record_batch.num_rows();
        let engine_data = ArrowEngineData::new(record_batch);
        
        // Parse selection vector if provided
        let mut selection_vec = if !selection_vector.is_null() {
            // Need to create an FFI_ArrowSchema for a boolean array
            let sel_bool_type = delta_kernel::arrow::datatypes::DataType::Boolean;
            let sel_ffi_schema = delta_kernel::arrow::ffi::FFI_ArrowSchema::try_from(&sel_bool_type)?;
            let sel_array_data = ffi::from_ffi(selection_vector.read(), &sel_ffi_schema)?;
            let sel_array = BooleanArray::from(sel_array_data);
            (0..num_rows).map(|i| sel_array.value(i)).collect()
        } else {
            vec![true; num_rows]
        };
        
        // Create a FilterVisitor to apply the filter
        let mut visitor = FilterVisitor {
            filter: std::mem::ManuallyDrop::into_inner(filter_arc),
            selection_vector: selection_vec.clone(),
        };
        
        // Visit rows to apply the filter
        visitor.visit_rows_of(&engine_data)?;
        selection_vec = visitor.selection_vector;
        
        // Build output boolean array
        let output_array = BooleanArray::from(selection_vec);
        let output_array_data = output_array.into_data();
        
        // Convert to Arrow C Data Interface
        let (out_array, out_schema) = ffi::to_ffi(&output_array_data)?;
        
        unsafe {
            *output = out_array;
            // Schema is already in input_schema, we don't need to set it again
            let _ = out_schema; // Suppress warning
        }
        
        Ok(())
    })();
    
    match result {
        Ok(()) => true,
        Err(e) => {
            eprintln!("Error in apply_row_filter: {}", e);
            false
        }
    }
}

// =============================================================================
// Test Functions
// =============================================================================

#[cfg(feature = "test-ffi")]
/// Create a test logical plan with a FilterNode for testing
///
/// Creates a simple plan: Scan -> Filter where filter keeps rows with id > 5
///
/// # Safety
///
/// This is a test function and should only be called in test contexts
#[no_mangle]
pub unsafe extern "C" fn get_testing_filter_plan() -> Handle<SharedLogicalPlan> {
    use delta_kernel::kernel_df::{FileType, FilterNode, ScanNode};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::{Expression, FileMeta, Predicate};
    use url::Url;
    
    // Create a simple schema with id and name columns
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ]));
    
    // Create a dummy file
    let files = vec![FileMeta {
        location: Url::parse("file:///test/data.parquet").unwrap(),
        last_modified: 0,
        size: 1024,
    }];
    
    // Create scan node
    let scan = LogicalPlanNode::Scan(ScanNode {
        files,
        schema: schema.clone(),
        file_type: FileType::Parquet,
    });
    
    // Create a filter: id > 5
    let predicate = Arc::new(Predicate::gt(
        Expression::column(delta_kernel::expressions::column_name!("id")),
        Expression::literal(5i32),
    ));
    
    // Wrap predicate in a RowFilter
    // We'll use a simple wrapper that uses the predicate evaluator
    use delta_kernel::engine_data::{GetData, TypedGetData};
    use delta_kernel::{DeltaResult, RowVisitor};
    use delta_kernel::expressions::ColumnName;
    
    struct SimplePredicateFilter {
        predicate: Arc<Predicate>,
        column_names: Vec<ColumnName>,
        column_types: Vec<DataType>,
    }
    
    impl RowVisitor for SimplePredicateFilter {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            // Leak memory for test purposes - this is only used in tests
            let names: &'static [ColumnName] = Box::leak(self.column_names.clone().into_boxed_slice());
            let types: &'static [DataType] = Box::leak(self.column_types.clone().into_boxed_slice());
            (names, types)
        }
        
        fn visit<'a>(&mut self, _row_count: usize, _getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
            // Not needed for filter - we use filter_row instead
            Ok(())
        }
    }
    
    impl RowFilter for SimplePredicateFilter {
        fn filter_row<'a>(&self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
            // For simplicity, just check if id > 5
            // In a real implementation, we'd evaluate the predicate properly
            let id_getter = getters[0]; // First column is 'id'
            let id_value: i32 = id_getter.get_int(i, "id")?.expect("id should not be null");
            Ok(id_value > 5)
        }
    }
    
    let filter_impl = Arc::new(SimplePredicateFilter {
        predicate: predicate.clone(),
        column_names: vec![
            delta_kernel::expressions::column_name!("id"),
        ],
        column_types: vec![DataType::INTEGER],
    }) as Arc<dyn RowFilter>;
    
    // Leak a static array for column_names (test only)
    let static_column_names: &'static [ColumnName] = Box::leak(Box::new([
        delta_kernel::expressions::column_name!("id"),
    ]));
    
    // Create filter node
    let filter = LogicalPlanNode::Filter(FilterNode {
        child: Box::new(scan),
        filter: filter_impl,
        column_names: static_column_names,
        ordered: false,
    });
    
    Arc::new(filter).into()
}

