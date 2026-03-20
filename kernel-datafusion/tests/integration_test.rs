//! Integration tests using REAL Delta table data files with known expected values
//!
//! Uses rstest for parameterized testing to reduce code duplication.

use std::sync::Arc;
use std::path::PathBuf;
use delta_kernel::plans::{DeclarativePlanNode, ScanNode, FileType, FilterByExpressionNode, SelectNode, FilterByKDF, ParseJsonNode, FirstNonNullNode};
use delta_kernel::schema::{StructType, StructField, DataType};
use delta_kernel::{Expression, FileMeta, Predicate, DeltaResult};
use delta_kernel::scan::ScanMetadata;
use delta_kernel_datafusion::executor::ParallelismConfig;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::snapshot::Snapshot;
use datafusion::prelude::SessionContext;
use datafusion::execution::TaskContext;
use datafusion::assert_batches_eq;
use datafusion::physical_plan::SendableRecordBatchStream;
use arrow::array::{StringArray, Int64Array, Array, RecordBatch, BooleanArray};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;
use rstest::rstest;

// ============================================================================
// Test Helpers
// ============================================================================

/// Collect all batches from a stream into a Vec
async fn collect_batches(stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
    stream.try_collect().await.expect("Failed to collect batches")
}

/// Path to real Delta table test data
fn test_data_path(relative_path: &str) -> PathBuf {
    PathBuf::from(format!("../acceptance/tests/dat/out/reader_tests/generated/{}", relative_path))
}

/// Convert a path to a URL with trailing slash
fn path_to_url(path: &PathBuf) -> url::Url {
    let mut url = url::Url::from_file_path(path.canonicalize().unwrap()).unwrap();
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    url
}

/// Schema for basic_append table: (letter: string, number: long, a_float: double)
fn basic_append_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("letter", DataType::STRING, true),
        StructField::new("number", DataType::LONG, true),
        StructField::new("a_float", DataType::DOUBLE, true),
    ]))
}

/// Get the canonical checkpoint read schema (add action only).
fn checkpoint_add_schema() -> Arc<StructType> {
    use delta_kernel::actions::{get_commit_schema, ADD_NAME};
    get_commit_schema().project(&[ADD_NAME]).expect("add schema projection should succeed")
}

/// Create a ScanNode for the basic_append parquet file
fn basic_append_scan_node() -> ScanNode {
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }.into()],
        schema: basic_append_schema(),
    }
}

/// Execute a plan and return batches
async fn execute_plan(plan: &DeclarativePlanNode) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(plan, &ctx.state(), &ParallelismConfig::default())
        .expect("compile_plan should work");
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    collect_batches(stream).await
}

/// Extract selected rows from ScanMetadata and concatenate into a single batch.
fn extract_selected_rows(metadata_list: &[ScanMetadata]) -> RecordBatch {
    let batches: Vec<RecordBatch> = metadata_list
        .iter()
        .map(|meta| {
            let batch = meta.scan_files.data()
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .expect("Should be ArrowEngineData")
                .record_batch()
                .clone();
            let selection = BooleanArray::from(meta.scan_files.selection_vector().to_vec());
            filter_record_batch(&batch, &selection).expect("filter should succeed")
        })
        .filter(|b| b.num_rows() > 0)
        .collect();

    if batches.is_empty() {
        if let Some(first) = metadata_list.first() {
            let batch = first.scan_files.data()
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .expect("Should be ArrowEngineData")
                .record_batch();
            return RecordBatch::new_empty(batch.schema());
        }
        panic!("No metadata to extract schema from");
    }
    concat_batches(&batches[0].schema(), &batches).expect("concat should succeed")
}

/// Compare ScanMetadata results from DataFusion vs DefaultEngine.
async fn compare_scan_metadata_results(table_url: url::Url) -> DeltaResult<()> {
    use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
    use futures::StreamExt;

    let store = store_from_url(&table_url)?;
    let default_engine: Arc<DefaultEngine<TokioBackgroundExecutor>> = Arc::new(DefaultEngine::builder(store).build());
    let snapshot = Snapshot::builder_for(table_url.clone()).build(default_engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let expected: Vec<ScanMetadata> = scan.scan_metadata(default_engine.as_ref())?
        .collect::<DeltaResult<Vec<_>>>()?;

    let executor = Arc::new(DataFusionExecutor::new().expect("DataFusionExecutor creation should succeed"));
    let df_snapshot = Snapshot::async_builder(table_url).build(&executor).await?;
    let df_scan = Arc::new(df_snapshot).scan_builder().build()?;
    let mut stream = std::pin::pin!(df_scan.scan_metadata_async(executor));
    let mut actual: Vec<ScanMetadata> = Vec::new();
    while let Some(result) = stream.next().await {
        actual.push(result?);
    }

    let expected_batch = extract_selected_rows(&expected);
    let actual_batch = extract_selected_rows(&actual);
    let expected_formatted = pretty_format_batches(&[expected_batch.clone()]).expect("format should succeed").to_string();
    let actual_formatted = pretty_format_batches(&[actual_batch.clone()]).expect("format should succeed").to_string();

    assert_eq!(expected_formatted, actual_formatted, "Scan metadata contents mismatch");
    assert_eq!(expected_batch.num_rows(), actual_batch.num_rows(), "Row count mismatch");
    Ok(())
}

/// Compare ScanMetadata with predicate
async fn compare_scan_metadata_with_predicate(
    table_url: url::Url,
    predicate: delta_kernel::PredicateRef,
) -> DeltaResult<()> {
    use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
    use futures::StreamExt;

    let store = store_from_url(&table_url)?;
    let default_engine: Arc<DefaultEngine<TokioBackgroundExecutor>> = Arc::new(DefaultEngine::builder(store).build());
    let snapshot = Snapshot::builder_for(table_url.clone()).build(default_engine.as_ref())?;
    let scan = snapshot.scan_builder().with_predicate(predicate.clone()).build()?;
    let expected: Vec<ScanMetadata> = scan.scan_metadata(default_engine.as_ref())?.collect::<DeltaResult<Vec<_>>>()?;

    let executor = Arc::new(DataFusionExecutor::new().expect("DataFusionExecutor creation should succeed"));
    let df_snapshot = Snapshot::async_builder(table_url).build(&executor).await?;
    let df_scan = Arc::new(df_snapshot).scan_builder().with_predicate(predicate).build()?;
    let mut stream = std::pin::pin!(df_scan.scan_metadata_async(executor));
    let mut actual: Vec<ScanMetadata> = Vec::new();
    while let Some(result) = stream.next().await {
        actual.push(result?);
    }

    let expected_batch = extract_selected_rows(&expected);
    let actual_batch = extract_selected_rows(&actual);
    assert_eq!(expected_batch.num_rows(), actual_batch.num_rows(), "Row count mismatch with predicate");
    Ok(())
}

// ============================================================================
// Basic Tests
// ============================================================================

#[test]
fn test_schema_conversion() {
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    let kernel_schema = basic_append_schema();
    let arrow_schema: arrow::datatypes::Schema = kernel_schema.as_ref().try_into_arrow().expect("Should convert");
    assert_eq!(arrow_schema.fields().len(), 3);
}

#[test]
fn test_executor_creation() {
    let executor = delta_kernel_datafusion::DataFusionExecutor::new().expect("Should create");
    assert!(executor.session_state().config().batch_size() > 0);
}

// ============================================================================
// Parquet Scan Tests (Parameterized)
// ============================================================================

/// Test: Read REAL Delta table parquet file and verify EXACT data
#[tokio::test]
async fn test_real_parquet_file_exact_data() {
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist: {:?}", file_path);

    let plan = DeclarativePlanNode::Scan(basic_append_scan_node());
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+--------+---------+",
            "| letter | number | a_float |",
            "+--------+--------+---------+",
            "| a      | 1      | 1.1     |",
            "| b      | 2      | 2.2     |",
            "| c      | 3      | 3.3     |",
            "+--------+--------+---------+",
        ],
        &batches
    );
}

/// Test: Filter on REAL Delta data
#[tokio::test]
async fn test_real_parquet_with_filter() {
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(1i64),
        ).into()),
    };

    let plan = DeclarativePlanNode::FilterByExpression {
        child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
        node: filter_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+--------+---------+",
            "| letter | number | a_float |",
            "+--------+--------+---------+",
            "| b      | 2      | 2.2     |",
            "| c      | 3      | 3.3     |",
            "+--------+--------+---------+",
        ],
        &batches
    );
}

/// Test: Projection on REAL Delta data
#[tokio::test]
async fn test_real_parquet_with_projection() {
    let select_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("letter", DataType::STRING, true),
        StructField::new("number", DataType::LONG, true),
    ]));

    let select_node = SelectNode {
        columns: vec![
            Arc::new(Expression::column(["letter"])),
            Arc::new(Expression::column(["number"])),
        ],
        output_schema: select_schema,
    };

    let plan = DeclarativePlanNode::Select {
        child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
        node: select_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+--------+",
            "| letter | number |",
            "+--------+--------+",
            "| a      | 1      |",
            "| b      | 2      |",
            "| c      | 3      |",
            "+--------+--------+",
        ],
        &batches
    );
}

/// Test: Composite plan (Filter + Projection)
#[tokio::test]
async fn test_real_parquet_composite() {
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(1i64),
        ).into()),
    };

    let select_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("letter", DataType::STRING, true),
    ]));

    let select_node = SelectNode {
        columns: vec![Arc::new(Expression::column(["letter"]))],
        output_schema: select_schema,
    };

    let plan = DeclarativePlanNode::Select {
        child: Box::new(DeclarativePlanNode::FilterByExpression {
            child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
            node: filter_node,
        }),
        node: select_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+",
            "| letter |",
            "+--------+",
            "| b      |",
            "| c      |",
            "+--------+",
        ],
        &batches
    );
}

/// Test: Transform expressions (arithmetic)
#[tokio::test]
async fn test_real_parquet_with_transform_expressions() {
    use delta_kernel::expressions::{BinaryExpression, BinaryExpressionOp};

    let number_times_10 = Expression::Binary(BinaryExpression {
        op: BinaryExpressionOp::Multiply,
        left: Box::new(Expression::column(["number"])),
        right: Box::new(Expression::literal(10i64)),
    });

    let float_plus_100 = Expression::Binary(BinaryExpression {
        op: BinaryExpressionOp::Plus,
        left: Box::new(Expression::column(["a_float"])),
        right: Box::new(Expression::literal(100.0f64)),
    });

    let select_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("number_x10", DataType::LONG, true),
        StructField::new("float_plus_100", DataType::DOUBLE, true),
    ]));

    let select_node = SelectNode {
        columns: vec![Arc::new(number_times_10), Arc::new(float_plus_100)],
        output_schema: select_schema,
    };

    let plan = DeclarativePlanNode::Select {
        child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
        node: select_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+------------+----------------+",
            "| number_x10 | float_plus_100 |",
            "+------------+----------------+",
            "| 10         | 101.1          |",
            "| 20         | 102.2          |",
            "| 30         | 103.3          |",
            "+------------+----------------+",
        ],
        &batches
    );
}

/// Test: Multiple REAL Delta files
#[tokio::test]
async fn test_multiple_real_parquet_files() {
    let file1 = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    let file2 = test_data_path("basic_append/delta/part-00000-a9daef62-5a40-43c5-ac63-3ad4a7d749ae-c000.snappy.parquet");

    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![
            FileMeta {
                location: url::Url::parse(&format!("file://{}", file1.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&file1).unwrap().len(),
                last_modified: 0,
            }.into(),
            FileMeta {
                location: url::Url::parse(&format!("file://{}", file2.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&file2).unwrap().len(),
                last_modified: 0,
            }.into(),
        ],
        schema: basic_append_schema(),
    };

    let plan = DeclarativePlanNode::Scan(scan_node);
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+--------+---------+",
            "| letter | number | a_float |",
            "+--------+--------+---------+",
            "| a      | 1      | 1.1     |",
            "| b      | 2      | 2.2     |",
            "| c      | 3      | 3.3     |",
            "| d      | 4      | 4.4     |",
            "| e      | 5      | 5.5     |",
            "+--------+--------+---------+",
        ],
        &batches
    );
}

// ============================================================================
// Expression Transform Tests (Parameterized)
// ============================================================================

#[rstest]
#[case::insert_and_replace(
    |_schema: &Arc<StructType>| {
        use delta_kernel::expressions::Transform;
        Transform::new_top_level()
            .with_replaced_field("letter", Arc::new(Expression::literal("hello")))
            .with_inserted_field(Some("number"), Arc::new(Expression::literal(42i64)))
            .with_dropped_field("a_float")
    },
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("letter", DataType::STRING, true),
        StructField::new("number", DataType::LONG, true),
        StructField::new("inserted_val", DataType::LONG, true),
    ])),
    vec![
        "+--------+--------+--------------+",
        "| letter | number | inserted_val |",
        "+--------+--------+--------------+",
        "| hello  | 1      | 42           |",
        "| hello  | 2      | 42           |",
        "| hello  | 3      | 42           |",
        "+--------+--------+--------------+",
    ]
)]
#[case::with_prepend(
    |_schema: &Arc<StructType>| {
        use delta_kernel::expressions::Transform;
        Transform::new_top_level()
            .with_inserted_field(None::<String>, Arc::new(Expression::literal("FIRST")))
    },
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("prepended_col", DataType::STRING, true),
        StructField::new("letter", DataType::STRING, true),
        StructField::new("number", DataType::LONG, true),
        StructField::new("a_float", DataType::DOUBLE, true),
    ])),
    vec![
        "+---------------+--------+--------+---------+",
        "| prepended_col | letter | number | a_float |",
        "+---------------+--------+--------+---------+",
        "| FIRST         | a      | 1      | 1.1     |",
        "| FIRST         | b      | 2      | 2.2     |",
        "| FIRST         | c      | 3      | 3.3     |",
        "+---------------+--------+--------+---------+",
    ]
)]
#[case::identity(
    |_schema: &Arc<StructType>| {
        use delta_kernel::expressions::Transform;
        Transform::new_top_level()
    },
    basic_append_schema(),
    vec![
        "+--------+--------+---------+",
        "| letter | number | a_float |",
        "+--------+--------+---------+",
        "| a      | 1      | 1.1     |",
        "| b      | 2      | 2.2     |",
        "| c      | 3      | 3.3     |",
        "+--------+--------+---------+",
    ]
)]
#[tokio::test]
async fn test_expression_transform(
    #[case] build_transform: fn(&Arc<StructType>) -> delta_kernel::expressions::Transform,
    #[case] output_schema: Arc<StructType>,
    #[case] expected: Vec<&str>,
) {
    let input_schema = basic_append_schema();
    let transform = build_transform(&input_schema);
    let transform_expr = Expression::Transform(transform);

    let select_node = SelectNode {
        columns: vec![Arc::new(transform_expr)],
        output_schema: output_schema.clone(),
    };

    let plan = DeclarativePlanNode::Select {
        child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
        node: select_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(&expected, &batches);
}

// ============================================================================
// KDF Tests
// ============================================================================

/// Test: CheckpointDedupState KDF
#[tokio::test]
async fn test_checkpoint_dedup_kdf() {
    let checkpoint_file = PathBuf::from("../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_file.exists(), "Real checkpoint file should exist");

    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", checkpoint_file.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&checkpoint_file).unwrap().len(),
            last_modified: 0,
        }.into()],
        schema: checkpoint_add_schema(),
    };

    let (kdf_node, _receiver) = FilterByKDF::checkpoint_dedup();

    let plan = DeclarativePlanNode::FilterByKDF {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: kdf_node,
    };
    let batches = execute_plan(&plan).await;

    // Should have exactly 1 row (the add action)
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "CheckpointDedup should pass through 1 add action");
}

// ============================================================================
// ParseJson Tests
// ============================================================================

/// Test: ParseJson extracts stats from checkpoint
#[tokio::test]
async fn test_parse_json_stats_from_checkpoint() {
    let checkpoint_file = PathBuf::from("../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_file.exists());

    let add_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("path", DataType::STRING),
        StructField::nullable("size", DataType::LONG),
        StructField::nullable("stats", DataType::STRING),
    ]));

    let checkpoint_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("add", DataType::Struct(Box::new((*add_schema).clone()))),
    ]));

    let stats_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("numRecords", DataType::LONG),
    ]));

    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", checkpoint_file.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&checkpoint_file).unwrap().len(),
            last_modified: 0,
        }.into()],
        schema: checkpoint_schema,
    };

    let parse_json_node = ParseJsonNode {
        json_column: "add.stats".to_string(),
        target_schema: stats_schema,
        output_column: String::new(),
    };

    let plan = DeclarativePlanNode::ParseJson {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: parse_json_node,
    };
    let batches = execute_plan(&plan).await;

    // Filter to rows with non-null numRecords
    let mut found_records = false;
    for batch in &batches {
        if let Some(num_records_col) = batch.column_by_name("numRecords") {
            let num_records = num_records_col.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..batch.num_rows() {
                if !num_records.is_null(i) {
                    assert_eq!(num_records.value(i), 5, "numRecords should be 5");
                    found_records = true;
                }
            }
        }
    }
    assert!(found_records, "Should have found numRecords value");
}

// ============================================================================
// FirstNonNull Tests
// ============================================================================

/// Test: FirstNonNull extracts first values
#[tokio::test]
async fn test_first_non_null_extracts_first_values() {
    let first_non_null_node = FirstNonNullNode {
        columns: vec!["letter".to_string(), "number".to_string(), "a_float".to_string()],
    };

    let plan = DeclarativePlanNode::FirstNonNull {
        child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
        node: first_non_null_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+--------+---------+",
            "| letter | number | a_float |",
            "+--------+--------+---------+",
            "| a      | 1      | 1.1     |",
            "+--------+--------+---------+",
        ],
        &batches
    );
}

/// Test: FirstNonNull with filter (ignores filtered rows)
#[tokio::test]
async fn test_first_non_null_with_filter() {
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(1i64),
        ).into()),
    };

    let first_non_null_node = FirstNonNullNode {
        columns: vec!["letter".to_string(), "number".to_string()],
    };

    let plan = DeclarativePlanNode::FirstNonNull {
        child: Box::new(DeclarativePlanNode::FilterByExpression {
            child: Box::new(DeclarativePlanNode::Scan(basic_append_scan_node())),
            node: filter_node,
        }),
        node: first_non_null_node,
    };
    let batches = execute_plan(&plan).await;

    assert_batches_eq!(
        &[
            "+--------+--------+",
            "| letter | number |",
            "+--------+--------+",
            "| b      | 2      |",
            "+--------+--------+",
        ],
        &batches
    );
}

// ============================================================================
// FileListingExec Tests
// ============================================================================

/// Test: FileListingExec lists files from Delta log directory
#[tokio::test]
async fn test_file_listing_exec() {
    use delta_kernel::plans::FileListingNode;

    let log_path = test_data_path("basic_append/delta/_delta_log/");
    assert!(log_path.exists());

    let log_url = url::Url::from_file_path(log_path.canonicalize().unwrap()).unwrap();
    let listing_node = FileListingNode { path: log_url };
    let plan = DeclarativePlanNode::FileListing(listing_node);

    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state(), &ParallelismConfig::default())
        .expect("Should compile");

    // Verify schema
    let schema = exec_plan.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "path");
    assert_eq!(schema.field(1).name(), "size");
    assert_eq!(schema.field(2).name(), "modificationTime");

    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;

    // Should have at least 2 JSON commit files
    let batch = &batches[0];
    let paths = batch.column_by_name("path").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let json_files: Vec<_> = (0..paths.len())
        .filter(|&i| paths.value(i).ends_with(".json"))
        .collect();
    assert!(json_files.len() >= 2, "Should have at least 2 JSON commit files");
}

// ============================================================================
// Scan Metadata Stream Tests (Parameterized)
// ============================================================================

#[rstest]
#[case::basic_append("basic_append/delta")]
#[case::all_primitive_types("all_primitive_types/delta")]
#[case::with_checkpoint("with_checkpoint/delta")]
#[case::basic_partitioned("basic_partitioned/delta")]
#[case::multi_partitioned("multi_partitioned/delta")]
#[case::no_replay("no_replay/delta")]
#[tokio::test]
async fn test_scan_metadata_stream(#[case] table_relative_path: &str) {
    let table_path = test_data_path(table_relative_path);
    if !table_path.exists() {
        println!("Skipping test: {} not found", table_relative_path);
        return;
    }

    let table_url = path_to_url(&table_path);
    compare_scan_metadata_results(table_url).await
        .expect("Scan metadata comparison should succeed");
}

// ============================================================================
// Snapshot State Machine Tests (Parameterized)
// ============================================================================

#[rstest]
#[case::basic_append("basic_append/delta", None, 1)]
#[case::with_checkpoint("with_checkpoint/delta", None, 3)]
#[case::basic_append_v0("basic_append/delta", Some(0), 0)]
#[tokio::test]
async fn test_snapshot_state_machine(
    #[case] table_relative_path: &str,
    #[case] target_version: Option<delta_kernel::Version>,
    #[case] expected_version: delta_kernel::Version,
) {
    use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt};

    let table_path = test_data_path(table_relative_path);
    assert!(table_path.exists(), "Delta table should exist: {:?}", table_path);

    let table_url = path_to_url(&table_path);
    let executor = DataFusionExecutor::new().expect("DataFusionExecutor creation should succeed");

    let mut builder = Snapshot::async_builder(table_url);
    if let Some(v) = target_version {
        builder = builder.with_version(v);
    }

    let snapshot = builder.build(&executor).await.expect("Snapshot construction should succeed");
    assert_eq!(snapshot.version(), expected_version);

    // Verify schema has expected columns
    let schema = snapshot.schema();
    assert!(schema.field("letter").is_some() || schema.field("int").is_some(),
        "Schema should contain expected column");
}

// ============================================================================
// Data Skipping Tests
// ============================================================================

/// Helper to build a predicate for data skipping tests
fn build_gt_predicate(column: &str, threshold: i64) -> delta_kernel::PredicateRef {
    use delta_kernel::expressions::{Expression, Scalar, ColumnName};
    Arc::new(
        Expression::Column(ColumnName::new([column]))
            .gt(Expression::Literal(Scalar::Long(threshold)))
    )
}

#[tokio::test]
async fn test_data_skipping_keep_all() {
    let table_path = test_data_path("basic_partitioned/delta");
    if !table_path.exists() {
        println!("Skipping test: basic_partitioned not found");
        return;
    }
    let table_url = path_to_url(&table_path);
    let predicate = build_gt_predicate("number", -1000);
    compare_scan_metadata_with_predicate(table_url, predicate).await
        .expect("Data skipping comparison should succeed");
}

#[tokio::test]
async fn test_data_skipping_keep_some() {
    let table_path = test_data_path("basic_partitioned/delta");
    if !table_path.exists() {
        println!("Skipping test: basic_partitioned not found");
        return;
    }
    let table_url = path_to_url(&table_path);
    let predicate = build_gt_predicate("number", 3);
    compare_scan_metadata_with_predicate(table_url, predicate).await
        .expect("Data skipping comparison should succeed");
}

#[tokio::test]
async fn test_data_skipping_simple_table() {
    let table_path = test_data_path("table-without-dv-small/delta");
    if !table_path.exists() {
        println!("Skipping test: table-without-dv-small not found");
        return;
    }
    let table_url = path_to_url(&table_path);
    let predicate = build_gt_predicate("value", 5);
    compare_scan_metadata_with_predicate(table_url, predicate).await
        .expect("Data skipping comparison should succeed");
}

// ============================================================================
// SchemaQuery Tests (Parameterized)
// ============================================================================

#[rstest]
#[case::v1_checkpoint(
    "../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet",
    false  // should NOT have sidecar
)]
#[tokio::test]
async fn test_schema_query_checkpoint(
    #[case] checkpoint_path: &str,
    #[case] expect_sidecar: bool,
) {
    use delta_kernel::actions::SIDECAR_NAME;
    use delta_kernel::plans::{SchemaQueryNode, SinkNode, SinkType};
    use delta_kernel::plans::kdf_state::{SchemaReaderState, SchemaStoreState};

    let path = PathBuf::from(checkpoint_path);
    if !path.exists() {
        println!("Skipping test: checkpoint not found at {:?}", path);
        return;
    }

    let file_url = url::Url::parse(&format!("file://{}", path.canonicalize().unwrap().display())).unwrap();

    let schema_store = SchemaStoreState::new();
    let state = SchemaReaderState::SchemaStore(schema_store.clone());

    let schema_query_node = SchemaQueryNode {
        file_path: file_url.to_string(),
        state: state.clone(),
    };

    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::SchemaQuery(schema_query_node)),
        node: SinkNode { sink_type: SinkType::Drop },
    };

    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state(), &ParallelismConfig::default())
        .expect("SchemaQuery should compile");

    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;

    // SchemaQuery produces no data
    assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));

    // Verify schema was stored
    let stored_schema = schema_store.get().expect("Schema should have been stored");

    // Check sidecar column presence
    let has_sidecar = stored_schema.field(SIDECAR_NAME).is_some();
    assert_eq!(has_sidecar, expect_sidecar,
        "Sidecar column presence mismatch: expected {}, got {}", expect_sidecar, has_sidecar);
}

#[tokio::test]
async fn test_schema_query_properties() {
    use delta_kernel::plans::kdf_state::{SchemaReaderState, SchemaStoreState};
    use delta_kernel_datafusion::exec::SchemaQueryExec;
    use datafusion::physical_plan::ExecutionPlan;

    let path = url::Url::parse("file:///tmp/test.parquet").unwrap();
    let state = SchemaReaderState::SchemaStore(SchemaStoreState::new());
    let exec = SchemaQueryExec::new(path.clone(), state);

    assert_eq!(exec.name(), "SchemaQueryExec");
    assert_eq!(exec.file_path(), &path);
    assert!(exec.children().is_empty());
    assert_eq!(exec.schema().fields().len(), 0);
}

// ============================================================================
// JSON Scan Tests
// ============================================================================

/// Test: JSON scan with canonical commit schema
#[tokio::test]
async fn test_json_scan_with_canonical_commit_schema() {
    use delta_kernel::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;

    let commit_schema = get_commit_schema()
        .project(&[ADD_NAME, REMOVE_NAME])
        .expect("schema projection should succeed");

    let commit0 = test_data_path("basic_append/delta/_delta_log/00000000000000000000.json");
    let commit1 = test_data_path("basic_append/delta/_delta_log/00000000000000000001.json");

    let scan_node = ScanNode {
        file_type: FileType::Json,
        files: vec![
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit0.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit0).unwrap().len(),
                last_modified: 0,
            },
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit1.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit1).unwrap().len(),
                last_modified: 0,
            },
        ],
        schema: commit_schema.clone(),
    };

    let plan = DeclarativePlanNode::Scan(scan_node);
    let batches = execute_plan(&plan).await;

    // Verify schema matches
    let expected_arrow_schema: arrow::datatypes::Schema = commit_schema.as_ref().try_into_arrow()
        .expect("schema conversion should succeed");
    assert_eq!(batches[0].schema().as_ref(), &expected_arrow_schema);

    // Should have at least 4 rows (multiple actions per commit)
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 4, "Expected at least 4 rows, got {}", total_rows);
}

// ============================================================================
// ConsumeByKDF Tests
// ============================================================================

/// Test: ConsumeByKDF with MetadataProtocolReader
#[tokio::test]
async fn test_consume_kdf_metadata_protocol() {
    use delta_kernel::actions::{PROTOCOL_NAME, METADATA_NAME, Protocol, Metadata};
    use delta_kernel::schema::{StructField, StructType, ToSchema};
    use delta_kernel::plans::kdf_state::{StateSender, ConsumerKdfState, MetadataProtocolReaderState};
    use delta_kernel::plans::nodes::SinkNode;
    use delta_kernel_datafusion::DataFusionExecutor;
    use futures::TryStreamExt;

    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
    ]));

    let commit0 = test_data_path("basic_append/delta/_delta_log/00000000000000000000.json");
    let commit1 = test_data_path("basic_append/delta/_delta_log/00000000000000000001.json");

    let scan_node = ScanNode {
        file_type: FileType::Json,
        files: vec![
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit0.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit0).unwrap().len(),
                last_modified: 0,
            },
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit1.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit1).unwrap().len(),
                last_modified: 0,
            },
        ],
        schema,
    };

    let (sender, receiver) = StateSender::build(
        ConsumerKdfState::MetadataProtocolReader(MetadataProtocolReaderState::new())
    );

    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(DeclarativePlanNode::Scan(scan_node)),
            node: sender,
        }),
        node: SinkNode::drop(),
    };

    let executor = DataFusionExecutor::new().expect("executor should create");
    let stream = executor.execute_to_stream(plan).await.expect("execute should succeed");
    let _batches: Vec<_> = stream.try_collect().await.expect("collect should succeed");

    let states = receiver.take_all().expect("should collect states");
    assert_eq!(states.len(), 1, "Expected 1 state");

    match &states[0] {
        ConsumerKdfState::MetadataProtocolReader(mp_state) => {
            assert!(mp_state.has_protocol(), "Expected protocol to be extracted");
            assert!(mp_state.has_metadata(), "Expected metadata to be extracted");
            assert!(!mp_state.has_error(), "Expected no errors");
        }
        _ => panic!("Unexpected state variant"),
    }
}

// ============================================================================
// State Machine Driver Tests
// ============================================================================

/// Test: execute_state_machine_async is generic
#[tokio::test]
async fn test_execute_state_machine_async_generic() {
    use delta_kernel::plans::state_machines::SnapshotStateMachine;
    use delta_kernel_datafusion::execute_state_machine_async;

    let table_path = test_data_path("basic_append/delta");
    let table_url = path_to_url(&table_path);

    let executor = delta_kernel_datafusion::DataFusionExecutor::new()
        .expect("DataFusionExecutor creation should succeed");

    let sm = SnapshotStateMachine::new(table_url).expect("SnapshotStateMachine creation should succeed");
    let snapshot = execute_state_machine_async(&executor, sm).await
        .expect("State machine execution should succeed");

    assert_eq!(snapshot.version(), 1);
}

/// Test: FileListingExec -> ConsumeKdfExec (LogSegmentBuilder) pipeline
#[tokio::test]
async fn test_file_listing_to_log_segment_builder() {
    use delta_kernel::plans::{FileListingNode, SinkNode, SinkType};
    use delta_kernel::plans::kdf_state::{ConsumerKdfState, LogSegmentBuilderState, StateSender};
    use futures::TryStreamExt;

    let log_path = test_data_path("basic_append/delta/_delta_log/");
    let log_url = path_to_url(&log_path);

    let (log_segment_sender, _receiver) = StateSender::build(ConsumerKdfState::LogSegmentBuilder(
        LogSegmentBuilderState::new(log_url.clone(), None, None),
    ));

    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(DeclarativePlanNode::FileListing(FileListingNode { path: log_url })),
            node: log_segment_sender,
        }),
        node: SinkNode { sink_type: SinkType::Drop },
    };

    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state(), &ParallelismConfig::default())
        .expect("Plan should compile");

    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let _batches: Vec<_> = stream.try_collect().await.expect("Execution should succeed");

    // Pipeline executed without errors - that's the success condition
    // The Sink may or may not produce empty batches depending on implementation
}

// ============================================================================
// Diagnostic Tests
// ============================================================================

/// Test: Column case preservation
#[tokio::test]
async fn test_column_case_preservation() {
    use delta_kernel::expressions::{column_name, joined_column_expr};
    use delta_kernel_datafusion::expr::lower_column;

    let col = column_name!("nullCount");
    let expr = lower_column(&col);
    let expr_str = format!("{:?}", expr);
    assert!(expr_str.contains("nullCount") || expr_str.contains("Column"));

    let nested_col = joined_column_expr!("nullCount", "number");
    if let delta_kernel::Expression::Column(cn) = nested_col {
        let nested_expr = lower_column(&cn);
        let nested_expr_str = format!("{:?}", nested_expr);
        assert!(nested_expr_str.contains("nullCount"));
    }
}

/// Test: Scan state machine output schema comparison
#[tokio::test]
async fn test_scan_state_machine_output_schema() {
    use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
    use futures::StreamExt;

    let table_path = test_data_path("basic_append/delta");
    let table_url = path_to_url(&table_path);

    // DefaultEngine path
    let store = store_from_url(&table_url).expect("store_from_url should succeed");
    let default_engine: Arc<DefaultEngine<TokioBackgroundExecutor>> = Arc::new(DefaultEngine::builder(store).build());
    let snapshot = Snapshot::builder_for(table_url.clone()).build(default_engine.as_ref()).expect("Snapshot should build");
    let scan = snapshot.scan_builder().build().expect("Scan should build");
    let default_results: Vec<_> = scan.scan_metadata(default_engine.as_ref())
        .expect("scan_metadata should succeed")
        .collect::<DeltaResult<Vec<_>>>()
        .expect("collecting results should succeed");

    // DataFusion path
    let executor = Arc::new(DataFusionExecutor::new().expect("DataFusionExecutor should create"));
    let df_snapshot = Snapshot::async_builder(table_url).build(&executor).await.expect("Snapshot construction should succeed");
    let df_scan = Arc::new(df_snapshot).scan_builder().build().expect("Scan should build");
    let mut stream = std::pin::pin!(df_scan.scan_metadata_async(executor));
    let mut df_results: Vec<ScanMetadata> = Vec::new();
    while let Some(result) = stream.next().await {
        df_results.push(result.expect("Batch should succeed"));
    }

    // Compare
    let default_batch = extract_selected_rows(&default_results);
    let df_batch = extract_selected_rows(&df_results);

    assert_eq!(default_batch.schema(), df_batch.schema());
    assert_eq!(default_batch.num_rows(), df_batch.num_rows());

    let default_formatted = pretty_format_batches(&[default_batch]).expect("format").to_string();
    let df_formatted = pretty_format_batches(&[df_batch]).expect("format").to_string();
    assert_eq!(default_formatted, df_formatted);
}
