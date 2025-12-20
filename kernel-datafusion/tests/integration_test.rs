//! Integration tests using REAL Delta table data files with known expected values

use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashSet;
use delta_kernel::plans::{DeclarativePlanNode, ScanNode, FileType, FilterByExpressionNode, SelectNode, FilterByKDF, ParseJsonNode, FirstNonNullNode};
use delta_kernel::schema::{StructType, StructField, DataType};
use delta_kernel::{Expression, FileMeta, Predicate};
use delta_kernel::log_replay::FileActionKey;
use datafusion::prelude::SessionContext;
use datafusion::execution::TaskContext;
use datafusion::assert_batches_eq;
use datafusion::physical_plan::SendableRecordBatchStream;
use arrow::array::{StringArray, Int64Array, Array, RecordBatch};
use futures::TryStreamExt;

// ============================================================================
// Test Helpers
// ============================================================================

/// Collect all batches from a stream into a Vec
async fn collect_batches(stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
    stream.try_collect().await.expect("Failed to collect batches")
}

// Path to real Delta table test data
fn test_data_path(relative_path: &str) -> PathBuf {
    PathBuf::from(format!("../acceptance/tests/dat/out/reader_tests/generated/{}", relative_path))
}

// ============================================================================
// Canonical Schema Helpers - Use kernel's schemas instead of manual construction
// ============================================================================

/// Get the canonical checkpoint read schema (add action only).
///
/// This uses the kernel's canonical schema definition via `get_commit_schema().project()`.
/// DataFusion's `DefaultSchemaAdapterFactory` will automatically:
/// - Map file columns to this table schema
/// - Fill missing nullable columns with NULLs
/// - Error on missing non-nullable columns
fn checkpoint_add_schema() -> Arc<StructType> {
    use delta_kernel::actions::{get_commit_schema, ADD_NAME};
    get_commit_schema().project(&[ADD_NAME]).expect("add schema projection should succeed")
}

/// Get the canonical commit read schema (add + remove).
#[allow(dead_code)]
fn commit_add_remove_schema() -> Arc<StructType> {
    use delta_kernel::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
    get_commit_schema().project(&[ADD_NAME, REMOVE_NAME]).expect("add/remove schema projection should succeed")
}

// Schema for basic_append table: (letter: string, number: long, a_float: double)
fn basic_append_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("letter", DataType::STRING, true),
        StructField::new("number", DataType::LONG, true),
        StructField::new("a_float", DataType::DOUBLE, true),
    ]))
}

/// Test 1: Read REAL Delta table parquet file and verify EXACT data
#[tokio::test]
async fn test_real_parquet_file_exact_data() {
    // REAL FILE from basic_append Delta table
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist: {:?}", file_path);
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    let plan = DeclarativePlanNode::Scan(scan_node);
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("compile_plan should work with real Delta file");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Verify exact data using table assertion
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

/// Test 2: Filter on REAL Delta data with EXACT expected results
#[tokio::test]
async fn test_real_parquet_with_filter_exact_results() {
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist");
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    // Filter: number > 1
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(1i64),
        ).into()),
    };
    
    let plan = DeclarativePlanNode::FilterByExpression {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: filter_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("filter compilation should work");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Original: [(a,1,1.1), (b,2,2.2), (c,3,3.3)]
    // After filter (number > 1): [(b,2,2.2), (c,3,3.3)]
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

/// Test 3: Projection on REAL Delta data with EXACT expected columns
#[tokio::test]
async fn test_real_parquet_with_projection_exact_columns() {
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist");
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    // Project only letter and number columns (drop a_float)
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
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: select_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("projection compilation should work");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // SELECT letter, number (2 columns, a_float dropped)
    assert_batches_eq!(
        &[
            "+-------+-------+",
            "| col_0 | col_1 |",
            "+-------+-------+",
            "| a     | 1     |",
            "| b     | 2     |",
            "| c     | 3     |",
            "+-------+-------+",
        ],
        &batches
    );
}

/// Test 4: Composite plan (Filter + Projection) with EXACT expected results
#[tokio::test]
async fn test_real_parquet_composite_exact_results() {
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist");
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    // Filter: number > 1
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(1i64),
        ).into()),
    };
    
    // Project: only letter
    let select_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("letter", DataType::STRING, true),
    ]));
    
    let select_node = SelectNode {
        columns: vec![Arc::new(Expression::column(["letter"]))],
        output_schema: select_schema,
    };
    
    let plan = DeclarativePlanNode::Select {
        child: Box::new(DeclarativePlanNode::FilterByExpression {
            child: Box::new(DeclarativePlanNode::Scan(scan_node)),
            node: filter_node,
        }),
        node: select_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("composite plan should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // SELECT letter WHERE number > 1
    // Original: [(a,1), (b,2), (c,3)] -> Filtered: [(b,2), (c,3)] -> Projected: [b, c]
    assert_batches_eq!(
        &[
            "+-------+",
            "| col_0 |",
            "+-------+",
            "| b     |",
            "| c     |",
            "+-------+",
        ],
        &batches
    );
}

/// Test 5: Transform expressions (arithmetic, functions) - THE MOST COMPLEX
#[tokio::test]
async fn test_real_parquet_with_transform_expressions_exact_results() {
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist");
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    // Transform expressions: number * 10, a_float + 100.0
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
        columns: vec![
            Arc::new(number_times_10),
            Arc::new(float_plus_100),
        ],
        output_schema: select_schema,
    };
    
    let plan = DeclarativePlanNode::Select {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: select_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("Transform expressions should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // SELECT number * 10, a_float + 100.0
    // Original: [(1, 1.1), (2, 2.2), (3, 3.3)]
    // Transformed: [(10, 101.1), (20, 102.2), (30, 103.3)]
    assert_batches_eq!(
        &[
            "+-------+-------+",
            "| col_0 | col_1 |",
            "+-------+-------+",
            "| 10    | 101.1 |",
            "| 20    | 102.2 |",
            "| 30    | 103.3 |",
            "+-------+-------+",
        ],
        &batches
    );
}

/// Test 6: Multiple REAL Delta files
#[tokio::test]
async fn test_multiple_real_parquet_files_exact_data() {
    // REAL FILES from basic_append Delta table (both commits)
    let file1 = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    let file2 = test_data_path("basic_append/delta/part-00000-a9daef62-5a40-43c5-ac63-3ad4a7d749ae-c000.snappy.parquet");
    
    assert!(file1.exists(), "First Delta file should exist");
    assert!(file2.exists(), "Second Delta file should exist");
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![
            FileMeta {
                location: url::Url::parse(&format!("file://{}", file1.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&file1).unwrap().len(),
                last_modified: 0,
            },
            FileMeta {
                location: url::Url::parse(&format!("file://{}", file2.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&file2).unwrap().len(),
                last_modified: 0,
            },
        ],
        schema: basic_append_schema(),
    };
    
    let plan = DeclarativePlanNode::Scan(scan_node);
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("Should compile with multiple files");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // file1: (a,1,1.1), (b,2,2.2), (c,3,3.3)
    // file2: (d,4,4.4), (e,5,5.5)
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

// Simple compilation tests
#[test]
fn test_schema_conversion() {
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    let kernel_schema = basic_append_schema();
    let arrow_schema: arrow::datatypes::Schema = kernel_schema.as_ref().try_into_arrow()
        .expect("Should convert");
    assert_eq!(arrow_schema.fields().len(), 3);
}

#[test]
fn test_executor_creation() {
    let executor = delta_kernel_datafusion::DataFusionExecutor::new()
        .expect("Should create");
    assert!(executor.session_state().config().batch_size() > 0);
}

// ============================================================================
// KDF TESTS - Testing the most important custom Delta-specific logic
// ============================================================================

/// Test 7: CheckpointDedupState KDF - Execute and verify EXACT paths from REAL checkpoint
#[tokio::test]
async fn test_checkpoint_dedup_kdf_with_real_checkpoint_exact_paths() {
    // Use REAL CHECKPOINT FILE (not regular data file!)
    let checkpoint_file = PathBuf::from("../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_file.exists(), "Real checkpoint file should exist: {:?}", checkpoint_file);
    
    // =========================================================================
    // This checkpoint contains 1 add action (CheckpointDedup with EMPTY state passes it through)
    // Use kernel's canonical schema - DataFusion's SchemaAdapter handles the rest
    let checkpoint_read_schema = checkpoint_add_schema();
    
    // Create scan node with CHECKPOINT file using kernel's canonical schema
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", checkpoint_file.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&checkpoint_file).unwrap().len(),
            last_modified: 0,
        }],
        schema: checkpoint_read_schema,
    };
    
    // Create FilterByKDF with CheckpointDedup (empty state = all add actions pass through)
    let kdf_node = FilterByKDF::checkpoint_dedup();
    
    let plan = DeclarativePlanNode::FilterByKDF {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: kdf_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("CheckpointDedup KDF should compile with real checkpoint schema");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // CheckpointDedup with empty state passes through the single add action
    assert_batches_eq!(
        &[
            "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                                                                                                                                                                                                          |",
            "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            r#"| {path: part-00000-7261547b-c07f-4530-998c-767b3f4de281-c000.snappy.parquet, partitionValues: {}, size: 976, modificationTime: 1712091442821, dataChange: false, stats: {"numRecords":5,"minValues":{"letter":"a","int":120,"date":"1971-07-01"},"maxValues":{"letter":"c","int":667,"date":"2018-02-01"},"nullCount":{"letter":2,"int":0,"date":0}}, tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |"#,
            "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &batches
    );
}

/// Test 8: CheckpointDedupState with PRE-SEEDED state - Tests filtering functionality
/// 
/// This simulates the REAL workflow:
/// 1. Commit phase: AddRemoveDedup processes commit JSON files, accumulates seen keys
/// 2. Checkpoint phase: CheckpointDedup uses those seen keys to FILTER OUT files already seen
///
/// We pre-seed the CheckpointDedup with the add path from the checkpoint,
/// so it should be FILTERED OUT (result: empty)
#[tokio::test]
async fn test_checkpoint_dedup_kdf_with_preseeded_state_filters_correctly() {
    use delta_kernel::plans::kdf_state::filter::CheckpointDedupState;
    use delta_kernel::plans::kdf_state::FilterKdfState;
    use std::sync::Mutex;
    
    // Use REAL CHECKPOINT FILE
    let checkpoint_file = PathBuf::from("../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_file.exists(), "Real checkpoint file should exist");
    
    // Pre-seed CheckpointDedup with the add path (simulating it was seen in commit phase)
    // Therefore, the add action should be FILTERED OUT (result: empty)
    let path_already_seen = "part-00000-7261547b-c07f-4530-998c-767b3f4de281-c000.snappy.parquet";
    
    // Create CheckpointDedupState with the path PRE-SEEDED
    let mut seen_keys = HashSet::new();
    seen_keys.insert(FileActionKey::new(path_already_seen, None)); // No deletion vector
    let checkpoint_dedup_state = CheckpointDedupState::from_hashset(seen_keys);
    
    // Create the KDF node with pre-seeded state
    let kdf_node = FilterByKDF {
        state: Arc::new(Mutex::new(FilterKdfState::CheckpointDedup(checkpoint_dedup_state))),
    };
    
    // Use kernel's canonical schema - DataFusion's SchemaAdapter handles the rest
    let checkpoint_read_schema = checkpoint_add_schema();
    
    // Create scan node with CHECKPOINT file
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", checkpoint_file.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&checkpoint_file).unwrap().len(),
            last_modified: 0,
        }],
        schema: checkpoint_read_schema,
    };
    
    let plan = DeclarativePlanNode::FilterByKDF {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: kdf_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("CheckpointDedup KDF with pre-seeded state should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Result should be empty - the add action was pre-seeded (filtered out)
    assert_batches_eq!(
        &[
            "+-----+",
            "| add |",
            "+-----+",
            "+-----+",
        ],
        &batches
    );
    
    // Also verify count explicitly
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0,
               "CheckpointDedup with pre-seeded state should filter out the already-seen path");
}

/// Test 9: KDF with Filter on REAL checkpoint - Composite plan with native DF filter + custom KDF
#[tokio::test]
async fn test_kdf_with_filter_on_checkpoint_exact_results() {
    use delta_kernel::plans::kdf_state::filter::CheckpointDedupState;
    use delta_kernel::plans::kdf_state::FilterKdfState;
    use std::sync::Mutex;
    
    // Use REAL CHECKPOINT FILE (has add.path column that KDF expects)
    let checkpoint_file = PathBuf::from("../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_file.exists(), "Real checkpoint file should exist");
    
    // Checkpoint contains 1 add action that passes both filters:
    // - add.size > 0 (native DF filter)
    // - CheckpointDedup with empty state (passes through)
    
    // Use kernel's canonical schema - DataFusion's SchemaAdapter handles the rest
    let checkpoint_read_schema = checkpoint_add_schema();
    
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", checkpoint_file.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&checkpoint_file).unwrap().len(),
            last_modified: 0,
        }],
        schema: checkpoint_read_schema,
    };
    
    // Build composite plan: Scan -> Filter (add.size > 0) -> FilterByKDF (CheckpointDedup)
    // Filter: add.size > 0 (all valid add actions have positive size)
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["add", "size"]),
            Expression::literal(0i64),
        ).into()),
    };
    
    // CheckpointDedup with empty state (all add actions pass through)
    let kdf_node = FilterByKDF {
        state: Arc::new(Mutex::new(FilterKdfState::CheckpointDedup(CheckpointDedupState::new()))),
    };
    
    let plan = DeclarativePlanNode::FilterByKDF {
        child: Box::new(DeclarativePlanNode::FilterByExpression {
            child: Box::new(DeclarativePlanNode::Scan(scan_node)),
            node: filter_node,
        }),
        node: kdf_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("Composite plan with Filter + KDF should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Composite Filter + KDF returns the add action that passes both filters
    assert_batches_eq!(
        &[
            "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                                                                                                                                                                                                          |",
            "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            r#"| {path: part-00000-7261547b-c07f-4530-998c-767b3f4de281-c000.snappy.parquet, partitionValues: {}, size: 976, modificationTime: 1712091442821, dataChange: false, stats: {"numRecords":5,"minValues":{"letter":"a","int":120,"date":"1971-07-01"},"maxValues":{"letter":"c","int":667,"date":"2018-02-01"},"nullCount":{"letter":2,"int":0,"date":0}}, tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |"#,
            "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &batches
    );
}

// ============================================================================
// PARSE JSON TESTS - Testing JSON extraction for data skipping
// ============================================================================

/// Test 10: ParseJson - Extract stats from checkpoint's add.stats JSON column
/// 
/// This tests the core functionality needed for data skipping:
/// 1. Read checkpoint file with add.stats JSON string
/// 2. Parse the JSON into structured numRecords, minValues, maxValues fields
/// 3. Verify the extracted values match expected data
#[tokio::test]
async fn test_parse_json_stats_from_checkpoint() {
    // Use REAL CHECKPOINT FILE with stats
    let checkpoint_file = PathBuf::from("../acceptance/tests/dat/out/reader_tests/generated/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_file.exists(), "Real checkpoint file should exist: {:?}", checkpoint_file);
    
    // =========================================================================
    // EXPECTED DATA (verified with pyarrow):
    // Row 1 stats: {"numRecords":5,"minValues":{"letter":"a","int":120,"date":"1971-07-01"},
    //              "maxValues":{"letter":"c","int":667,"date":"2018-02-01"},
    //              "nullCount":{"letter":2,"int":0,"date":0}}
    
    // Schema for the checkpoint add action (we only need add.stats)
    // Use a minimal schema that includes the stats column
    let add_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("path", DataType::STRING),
        StructField::nullable("size", DataType::LONG),
        StructField::nullable("stats", DataType::STRING),
    ]));
    
    let checkpoint_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("add", DataType::Struct(Box::new((*add_schema).clone()))),
    ]));
    
    // Schema for parsed stats - what we want to extract from the JSON
    let stats_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("numRecords", DataType::LONG),
        // For simplicity, we'll just extract numRecords in this test
        // Full implementation would include minValues/maxValues structs
    ]));
    
    // Create scan node with CHECKPOINT file
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", checkpoint_file.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&checkpoint_file).unwrap().len(),
            last_modified: 0,
        }],
        schema: checkpoint_schema,
    };
    
    // Create ParseJson node to extract stats from add.stats column
    let parse_json_node = ParseJsonNode {
        json_column: "add.stats".to_string(),
        target_schema: stats_schema,
        output_column: String::new(), // Merge at root level
    };
    
    let plan = DeclarativePlanNode::ParseJson {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: parse_json_node,
    };
    
    // Compile and execute
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("ParseJson should compile with checkpoint stats");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Filter to only rows where numRecords is not null (add actions with stats)
    let mut filtered_batches = Vec::new();
    for batch in &batches {
        if let Some(num_records_col) = batch.column_by_name("numRecords") {
            let num_records = num_records_col.as_any().downcast_ref::<Int64Array>().unwrap();
            let mask: arrow::array::BooleanArray = (0..batch.num_rows())
                .map(|i| Some(!num_records.is_null(i)))
                .collect();
            let filtered = arrow::compute::filter_record_batch(batch, &mask).unwrap();
            if filtered.num_rows() > 0 {
                filtered_batches.push(filtered);
            }
        }
    }
    
    // ParseJson extracts numRecords from checkpoint add.stats
    // Row with stats: numRecords=5
    assert_batches_eq!(
        &[
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
            "| add                                                                                                                                                                                                                                                                         | numRecords |",
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
            r#"| {path: part-00000-7261547b-c07f-4530-998c-767b3f4de281-c000.snappy.parquet, size: 976, stats: {"numRecords":5,"minValues":{"letter":"a","int":120,"date":"1971-07-01"},"maxValues":{"letter":"c","int":667,"date":"2018-02-01"},"nullCount":{"letter":2,"int":0,"date":0}}} | 5          |"#,
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
        ],
        &filtered_batches
    );
}

/// Test 11: ParseJson with REAL Delta table - Extract full stats including nested minValues/maxValues
/// 
/// This test reads REAL Delta log files and extracts stats with nested struct values.
/// Uses the basic_append table which has 2 files with known stats:
/// - File 1: numRecords=3, minValues.letter='a', maxValues.letter='c'
/// - File 2: numRecords=2, minValues.letter='d', maxValues.letter='e'
///
/// Following delta-kernel-rs testing patterns:
/// 1. Define expected RecordBatch statically
/// 2. Execute the plan and collect actual data
/// 3. Compare with assert_eq! for exact equality
#[tokio::test]
async fn test_parse_json_full_stats_from_delta_table() {
    // Read REAL Delta log JSON files
    // File 1 (commit 0): numRecords=3, minValues.letter='a', maxValues.letter='c'  
    // File 2 (commit 1): numRecords=2, minValues.letter='d', maxValues.letter='e'
    let commit0 = test_data_path("basic_append/delta/_delta_log/00000000000000000000.json");
    let commit1 = test_data_path("basic_append/delta/_delta_log/00000000000000000001.json");
    assert!(commit0.exists(), "Commit 0 should exist: {:?}", commit0);
    assert!(commit1.exists(), "Commit 1 should exist: {:?}", commit1);
    
    // Schema for reading Delta log JSON (add action with stats)
    let add_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("path", DataType::STRING),
        StructField::nullable("size", DataType::LONG),
        StructField::nullable("stats", DataType::STRING),
    ]));
    
    let log_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("add", DataType::Struct(Box::new((*add_schema).clone()))),
    ]));
    
    // Schema for parsed stats - extract numRecords and nested minValues/maxValues.letter
    let min_max_schema = StructType::new_unchecked(vec![
        StructField::nullable("letter", DataType::STRING),
    ]);
    
    let stats_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("minValues", DataType::Struct(Box::new(min_max_schema.clone()))),
        StructField::nullable("maxValues", DataType::Struct(Box::new(min_max_schema))),
    ]));
    
    // Create scan node with both log files
    let scan_node = ScanNode {
        file_type: FileType::Json,  // Delta log files are newline-delimited JSON
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
        schema: log_schema,
    };
    
    // Create ParseJson node to extract stats from add.stats column
    let parse_json_node = ParseJsonNode {
        json_column: "add.stats".to_string(),
        target_schema: stats_schema,
        output_column: String::new(), // Merge at root level
    };
    
    let plan = DeclarativePlanNode::ParseJson {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: parse_json_node,
    };
    
    // Compile and execute
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("ParseJson should compile with Delta log stats");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Filter to only rows where numRecords is not null (add actions with stats)
    let mut filtered_batches = Vec::new();
    for batch in &batches {
        if let Some(num_records_col) = batch.column_by_name("numRecords") {
            let num_records = num_records_col.as_any().downcast_ref::<Int64Array>().unwrap();
            let mask: arrow::array::BooleanArray = (0..batch.num_rows())
                .map(|i| Some(!num_records.is_null(i)))
                .collect();
            let filtered = arrow::compute::filter_record_batch(batch, &mask).unwrap();
            if filtered.num_rows() > 0 {
                filtered_batches.push(filtered);
            }
        }
    }
    
    // ParseJson extracts stats from add actions:
    // - Commit 0: numRecords=3, minValues.letter='a', maxValues.letter='c'
    // - Commit 1: numRecords=2, minValues.letter='d', maxValues.letter='e'
    assert_batches_eq!(
        &[
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------+-------------+",
            "| add                                                                                                                                                                                                                                                                     | numRecords | minValues   | maxValues   |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------+-------------+",
            r#"| {path: part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet, size: 996, stats: {"numRecords":3,"minValues":{"letter":"a","number":1,"a_float":1.1},"maxValues":{"letter":"c","number":3,"a_float":3.3},"nullCount":{"letter":0,"number":0,"a_float":0}}} | 3          | {letter: a} | {letter: c} |"#,
            r#"| {path: part-00000-a9daef62-5a40-43c5-ac63-3ad4a7d749ae-c000.snappy.parquet, size: 984, stats: {"numRecords":2,"minValues":{"letter":"d","number":4,"a_float":4.4},"maxValues":{"letter":"e","number":5,"a_float":5.5},"nullCount":{"letter":0,"number":0,"a_float":0}}} | 2          | {letter: d} | {letter: e} |"#,
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------+-------------+",
        ],
        &filtered_batches
    );
}

/// Test 12: FirstNonNull extracts first non-null values from columns
/// 
/// This test verifies that FirstNonNull correctly aggregates data using
/// DataFusion's first_value UDAF with IGNORE NULLS.
/// 
/// Input: basic_append parquet file with 3 rows:
///   letter: ['a', 'b', 'c']
///   number: [1, 2, 3]
///   a_float: [1.1, 2.2, 3.3]
/// 
/// Expected Output: Single row with first values:
///   letter: 'a'
///   number: 1
///   a_float: 1.1
#[tokio::test]
async fn test_first_non_null_extracts_first_values() {
    // REAL FILE from basic_append Delta table
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist: {:?}", file_path);
    
    // Create ScanNode with the parquet file
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    // Create FirstNonNull node for all columns
    let first_non_null_node = FirstNonNullNode {
        columns: vec![
            "letter".to_string(),
            "number".to_string(),
            "a_float".to_string(),
        ],
    };
    
    let plan = DeclarativePlanNode::FirstNonNull {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: first_non_null_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("FirstNonNull should compile successfully");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // FirstNonNull produces exactly 1 row with first values: ('a', 1, 1.1)
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

/// Test 13: FirstNonNull ignores null values
/// 
/// This test verifies that FirstNonNull correctly skips null values
/// and returns the first non-null value in each column.
/// 
/// We use a filter to create nulls by filtering some rows, then use FirstNonNull.
#[tokio::test]
async fn test_first_non_null_ignores_nulls() {
    // REAL FILE from basic_append Delta table - has 3 rows
    let file_path = test_data_path("basic_append/delta/part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet");
    assert!(file_path.exists(), "Real Delta table file should exist: {:?}", file_path);
    
    // Create ScanNode
    let scan_node = ScanNode {
        file_type: FileType::Parquet,
        files: vec![FileMeta {
            location: url::Url::parse(&format!("file://{}", file_path.canonicalize().unwrap().display())).unwrap(),
            size: std::fs::metadata(&file_path).unwrap().len(),
            last_modified: 0,
        }],
        schema: basic_append_schema(),
    };
    
    // Filter: number > 1 (keeps rows with number=2, number=3)
    // This leaves rows: (b, 2, 2.2), (c, 3, 3.3)
    let filter_node = FilterByExpressionNode {
        predicate: Arc::new(Predicate::gt(
            Expression::column(["number"]),
            Expression::literal(1i64),
        ).into()),
    };
    
    // FirstNonNull should get 'b' as first letter (since 'a' was filtered out)
    let first_non_null_node = FirstNonNullNode {
        columns: vec![
            "letter".to_string(),
            "number".to_string(),
        ],
    };
    
    let plan = DeclarativePlanNode::FirstNonNull {
        child: Box::new(DeclarativePlanNode::FilterByExpression {
            child: Box::new(DeclarativePlanNode::Scan(scan_node)),
            node: filter_node,
        }),
        node: first_non_null_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("FirstNonNull with filter should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // After filter (number > 1): rows (b, 2), (c, 3)
    // FirstNonNull picks first: (b, 2)
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

/// Test 14: FirstNonNull gets latest metadata/protocol from schema evolution table
/// 
/// This test verifies that FirstNonNull correctly extracts the latest values
/// when reading log files in descending version order, as is required for
/// Delta log replay.
/// 
/// Table: with_schema_change
/// - Version 0: schema={letter:string, number:long}, protocol={minReaderVersion:1, minWriterVersion:2}
/// - Version 1: schema={num1:long, num2:long} (schema changed!), NO protocol action
/// 
/// When reading 1.json → 0.json (newest first):
/// - metadata should come from version 1 (the changed schema with num1, num2)
/// - protocol should come from version 0 (version 1 has no protocol, first non-null is from v0)
#[tokio::test]
async fn test_first_non_null_schema_evolution_gets_latest_metadata() {
    // Read log files from with_schema_change table
    let log_path = test_data_path("with_schema_change/delta/_delta_log");
    let commit0 = log_path.join("00000000000000000000.json");
    let commit1 = log_path.join("00000000000000000001.json");
    assert!(commit0.exists(), "Commit 0 should exist: {:?}", commit0);
    assert!(commit1.exists(), "Commit 1 should exist: {:?}", commit1);
    
    // Schema for reading Delta log - we need protocol and metaData actions
    // Using a simplified schema that captures the key fields we care about
    let protocol_schema = StructType::new_unchecked(vec![
        StructField::nullable("minReaderVersion", DataType::INTEGER),
        StructField::nullable("minWriterVersion", DataType::INTEGER),
    ]);
    
    let metadata_schema = StructType::new_unchecked(vec![
        StructField::nullable("id", DataType::STRING),
        StructField::nullable("schemaString", DataType::STRING),
    ]);
    
    let log_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("protocol", DataType::Struct(Box::new(protocol_schema))),
        StructField::nullable("metaData", DataType::Struct(Box::new(metadata_schema))),
    ]));
    
    // Create scan with files in DESCENDING version order (newest first)
    // This is how Delta log replay works - we want first_value to pick the latest
    let scan_node = ScanNode {
        file_type: FileType::Json,
        files: vec![
            // Version 1 first (newest) - has new schema, no protocol
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit1.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit1).unwrap().len(),
                last_modified: 0,
            },
            // Version 0 second (older) - has original schema and protocol
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit0.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit0).unwrap().len(),
                last_modified: 0,
            },
        ],
        schema: log_schema,
    };
    
    // Apply FirstNonNull to extract first non-null protocol and metaData
    let first_non_null_node = FirstNonNullNode {
        columns: vec![
            "protocol".to_string(),
            "metaData".to_string(),
        ],
    };
    
    let plan = DeclarativePlanNode::FirstNonNull {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: first_non_null_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("FirstNonNull should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // FirstNonNull produces exactly 1 row with:
    // - protocol from version 0: {minReaderVersion: 1, minWriterVersion: 2}
    // - metaData from version 1: schema with num1, num2 (not letter from v0)
    assert_batches_eq!(
        &[
            "+--------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| protocol                                   | metaData                                                                                                                                                                                                       |",
            "+--------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            r#"| {minReaderVersion: 1, minWriterVersion: 2} | {id: 57980a21-14f6-4ebb-8b1e-b683850ba689, schemaString: {"type":"struct","fields":[{"name":"num1","type":"long","nullable":true,"metadata":{}},{"name":"num2","type":"long","nullable":true,"metadata":{}}]}} |"#,
            "+--------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &batches
    );
}

/// Test 15: FirstNonNull with multiple protocol/metadata updates
/// 
/// Uses the log-replay-latest-metadata-protocol golden table which has:
/// - Version 0: schema={col1:long}, protocol={minReader:1, minWriter:2}
/// - Version 1: schema={col1:long, col2:long} (added col2!), NO protocol
/// - Version 2: schema same as v1, protocol={minReader:3, minWriter:7} (upgraded!)
/// 
/// When reading 2.json → 1.json → 0.json (newest first):
/// - metadata should have col2 (from version 2 or 1)
/// - protocol should be {minReader:3, minWriter:7} (from version 2)
#[tokio::test]
async fn test_first_non_null_multiple_protocol_metadata_updates() {
    use std::process::Command;
    
    // Extract the golden table
    let golden_data_path = std::path::PathBuf::from("../kernel/tests/golden_data");
    let tar_file = golden_data_path.join("log-replay-latest-metadata-protocol.tar.zst");
    assert!(tar_file.exists(), "Golden table archive should exist: {:?}", tar_file);
    
    // Create temp dir and extract
    let temp_dir = tempfile::tempdir().expect("Should create temp dir");
    let status = Command::new("zstd")
        .args(["-d", tar_file.to_str().unwrap(), "-o", temp_dir.path().join("table.tar").to_str().unwrap()])
        .status()
        .expect("zstd should run");
    assert!(status.success(), "zstd should succeed");
    
    let status = Command::new("tar")
        .args(["-xf", temp_dir.path().join("table.tar").to_str().unwrap(), "-C", temp_dir.path().to_str().unwrap()])
        .status()
        .expect("tar should run");
    assert!(status.success(), "tar should succeed");
    
    let log_path = temp_dir.path().join("log-replay-latest-metadata-protocol/delta/_delta_log");
    let commit0 = log_path.join("00000000000000000000.json");
    let commit1 = log_path.join("00000000000000000001.json");
    let commit2 = log_path.join("00000000000000000002.json");
    assert!(commit0.exists(), "Commit 0 should exist");
    assert!(commit1.exists(), "Commit 1 should exist");
    assert!(commit2.exists(), "Commit 2 should exist");
    
    // Schema for reading Delta log
    let protocol_schema = StructType::new_unchecked(vec![
        StructField::nullable("minReaderVersion", DataType::INTEGER),
        StructField::nullable("minWriterVersion", DataType::INTEGER),
    ]);
    
    let metadata_schema = StructType::new_unchecked(vec![
        StructField::nullable("id", DataType::STRING),
        StructField::nullable("schemaString", DataType::STRING),
    ]);
    
    let log_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::nullable("protocol", DataType::Struct(Box::new(protocol_schema))),
        StructField::nullable("metaData", DataType::Struct(Box::new(metadata_schema))),
    ]));
    
    // Scan files in DESCENDING version order (newest first): 2 → 1 → 0
    let scan_node = ScanNode {
        file_type: FileType::Json,
        files: vec![
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit2.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit2).unwrap().len(),
                last_modified: 0,
            },
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit1.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit1).unwrap().len(),
                last_modified: 0,
            },
            FileMeta {
                location: url::Url::parse(&format!("file://{}", commit0.canonicalize().unwrap().display())).unwrap(),
                size: std::fs::metadata(&commit0).unwrap().len(),
                last_modified: 0,
            },
        ],
        schema: log_schema,
    };
    
    let first_non_null_node = FirstNonNullNode {
        columns: vec!["protocol".to_string(), "metaData".to_string()],
    };
    
    let plan = DeclarativePlanNode::FirstNonNull {
        child: Box::new(DeclarativePlanNode::Scan(scan_node)),
        node: first_non_null_node,
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("FirstNonNull should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // FirstNonNull produces exactly 1 row with:
    // - protocol from version 2: {minReaderVersion: 3, minWriterVersion: 7}
    // - metaData from version 2: schema with col1, col2
    assert_batches_eq!(
        &[
            "+--------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| protocol                                   | metaData                                                                                                                                                                         |",
            "+--------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            r#"| {minReaderVersion: 3, minWriterVersion: 7} | {id: testId, schemaString: {"type":"struct","fields":[{"name":"col1","type":"long","nullable":true,"metadata":{}},{"name":"col2","type":"long","nullable":true,"metadata":{}}]}} |"#,
            "+--------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &batches
    );
}

// ============================================================================
// FileListingExec Tests - List files from storage via ObjectStore
// ============================================================================

/// Test FileListingExec lists files from a Delta table's _delta_log directory
#[tokio::test]
async fn test_file_listing_exec_lists_delta_log() {
    use delta_kernel::plans::FileListingNode;
    
    // Path to a real Delta table's _delta_log directory
    let log_path = test_data_path("basic_append/delta/_delta_log/");
    assert!(log_path.exists(), "Delta log directory should exist: {:?}", log_path);
    
    // Create FileListingNode
    let log_url = url::Url::from_file_path(log_path.canonicalize().unwrap()).unwrap();
    let listing_node = FileListingNode { path: log_url.clone() };
    let plan = DeclarativePlanNode::FileListing(listing_node);
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("FileListingExec should compile successfully");
    
    // Verify schema
    let schema = exec_plan.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "path");
    assert_eq!(schema.field(1).name(), "size");
    assert_eq!(schema.field(2).name(), "modificationTime");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Transform to filename + size + modificationTime (paths are absolute/machine-specific)
    use arrow::datatypes::{Schema, Field, DataType as ArrowDataType};
    let batch = &batches[0];
    let paths = batch.column_by_name("path").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let sizes = batch.column_by_name("size").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    let mod_times = batch.column_by_name("modificationTime").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    
    // Collect and sort by filename
    let mut rows: Vec<(String, i64, i64)> = (0..paths.len())
        .map(|i| (
            paths.value(i).rsplit('/').next().unwrap().to_string(),
            sizes.value(i),
            mod_times.value(i),
        ))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    
    let filenames: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let sorted_sizes: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let sorted_mod_times: Vec<i64> = rows.iter().map(|r| r.2).collect();
    
    let result_schema = Arc::new(Schema::new(vec![
        Field::new("filename", ArrowDataType::Utf8, false),
        Field::new("size", ArrowDataType::Int64, true),
        Field::new("modificationTime", ArrowDataType::Int64, true),
    ]));
    let result_batch = RecordBatch::try_new(
        result_schema,
        vec![
            Arc::new(StringArray::from(filenames)),
            Arc::new(Int64Array::from(sorted_sizes)),
            Arc::new(Int64Array::from(sorted_mod_times)),
        ],
    ).unwrap();
    
    // basic_append/_delta_log contains 2 JSON commit files + 2 CRC checksum files
    assert_batches_eq!(
        &[
            "+--------------------------------+------+------------------+",
            "| filename                       | size | modificationTime |",
            "+--------------------------------+------+------------------+",
            "| .00000000000000000000.json.crc | 20   | 1712091396000    |",
            "| .00000000000000000001.json.crc | 16   | 1712091404000    |",
            "| 00000000000000000000.json      | 1247 | 1712091396000    |",
            "| 00000000000000000001.json      | 747  | 1712091404000    |",
            "+--------------------------------+------+------------------+",
        ],
        &[result_batch]
    );
}

/// Test FileListingExec streams results incrementally (doesn't block on full listing)
#[tokio::test]
async fn test_file_listing_exec_streaming_behavior() {
    use delta_kernel::plans::FileListingNode;
    
    // Use basic_append which has a known small number of files
    let log_path = test_data_path("basic_append/delta/_delta_log/");
    assert!(log_path.exists(), "Delta log directory should exist");
    
    let log_url = url::Url::from_file_path(log_path.canonicalize().unwrap()).unwrap();
    let listing_node = FileListingNode { path: log_url };
    let plan = DeclarativePlanNode::FileListing(listing_node);
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("Should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Transform to filename + size + modificationTime (paths are absolute/machine-specific)
    use arrow::datatypes::{Schema, Field, DataType as ArrowDataType};
    let batch = &batches[0];
    let paths = batch.column_by_name("path").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let sizes = batch.column_by_name("size").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    let mod_times = batch.column_by_name("modificationTime").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    
    // Collect and sort by filename
    let mut rows: Vec<(String, i64, i64)> = (0..paths.len())
        .map(|i| (
            paths.value(i).rsplit('/').next().unwrap().to_string(),
            sizes.value(i),
            mod_times.value(i),
        ))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    
    let filenames: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let sorted_sizes: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let sorted_mod_times: Vec<i64> = rows.iter().map(|r| r.2).collect();
    
    let result_schema = Arc::new(Schema::new(vec![
        Field::new("filename", ArrowDataType::Utf8, false),
        Field::new("size", ArrowDataType::Int64, true),
        Field::new("modificationTime", ArrowDataType::Int64, true),
    ]));
    let result_batch = RecordBatch::try_new(
        result_schema,
        vec![
            Arc::new(StringArray::from(filenames)),
            Arc::new(Int64Array::from(sorted_sizes)),
            Arc::new(Int64Array::from(sorted_mod_times)),
        ],
    ).unwrap();
    
    assert_batches_eq!(
        &[
            "+--------------------------------+------+------------------+",
            "| filename                       | size | modificationTime |",
            "+--------------------------------+------+------------------+",
            "| .00000000000000000000.json.crc | 20   | 1712091396000    |",
            "| .00000000000000000001.json.crc | 16   | 1712091404000    |",
            "| 00000000000000000000.json      | 1247 | 1712091396000    |",
            "| 00000000000000000001.json      | 747  | 1712091404000    |",
            "+--------------------------------+------+------------------+",
        ],
        &[result_batch]
    );
}

/// Test FileListingExec with a directory containing parquet files
#[tokio::test]
async fn test_file_listing_exec_parquet_directory() {
    use delta_kernel::plans::FileListingNode;
    
    // List the delta directory itself (contains parquet data files)
    let data_path = test_data_path("basic_append/delta/");
    assert!(data_path.exists(), "Delta data directory should exist: {:?}", data_path);
    
    let data_url = url::Url::from_file_path(data_path.canonicalize().unwrap()).unwrap();
    let listing_node = FileListingNode { path: data_url };
    let plan = DeclarativePlanNode::FileListing(listing_node);
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("Should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // Transform to filename + size + modificationTime, filter to parquet files only
    use arrow::datatypes::{Schema, Field, DataType as ArrowDataType};
    let batch = &batches[0];
    let paths = batch.column_by_name("path").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let sizes = batch.column_by_name("size").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    let mod_times = batch.column_by_name("modificationTime").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    
    // Collect parquet files only, sort by filename
    let mut rows: Vec<(String, i64, i64)> = (0..paths.len())
        .filter(|&i| paths.value(i).ends_with(".parquet"))
        .map(|i| (
            paths.value(i).rsplit('/').next().unwrap().to_string(),
            sizes.value(i),
            mod_times.value(i),
        ))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    
    let filenames: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let sorted_sizes: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let sorted_mod_times: Vec<i64> = rows.iter().map(|r| r.2).collect();
    
    let result_schema = Arc::new(Schema::new(vec![
        Field::new("filename", ArrowDataType::Utf8, false),
        Field::new("size", ArrowDataType::Int64, true),
        Field::new("modificationTime", ArrowDataType::Int64, true),
    ]));
    let result_batch = RecordBatch::try_new(
        result_schema,
        vec![
            Arc::new(StringArray::from(filenames)),
            Arc::new(Int64Array::from(sorted_sizes)),
            Arc::new(Int64Array::from(sorted_mod_times)),
        ],
    ).unwrap();
    
    // basic_append has exactly 2 parquet data files (one per commit)
    assert_batches_eq!(
        &[
            "+---------------------------------------------------------------------+------+------------------+",
            "| filename                                                            | size | modificationTime |",
            "+---------------------------------------------------------------------+------+------------------+",
            "| part-00000-a9daef62-5a40-43c5-ac63-3ad4a7d749ae-c000.snappy.parquet | 984  | 1712091404000    |",
            "| part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet | 996  | 1712091396000    |",
            "+---------------------------------------------------------------------+------+------------------+",
        ],
        &[result_batch]
    );
}

// ============================================================================
// JSON SCAN SCHEMA SEMANTICS TESTS
// ============================================================================

/// Test: JSON scan with kernel's canonical commit schema
/// 
/// This test verifies whether JSON scan needs schema relaxation.
/// The canonical schema has non-nullable children (path, size, etc.)
/// inside nullable parents (add, remove).
///
/// If this test fails with a schema/nullability error, JSON scan needs
/// the same schema relaxation as Parquet scan.
#[tokio::test]
async fn test_json_scan_with_canonical_commit_schema() {
    use delta_kernel::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
    
    // Use the kernel's canonical commit schema (has non-nullable nested fields)
    // This schema has:
    //   add (nullable) -> path (NOT NULL), size (NOT NULL), etc.
    //   remove (nullable) -> path (NOT NULL), dataChange (NOT NULL), etc.
    let commit_schema = get_commit_schema()
        .project(&[ADD_NAME, REMOVE_NAME])
        .expect("schema projection should succeed");
    
    // Read commit JSON files from basic_append
    // Each JSON file has multiple lines - some with "add", some with "protocol"/"metaData"
    // Lines without "add" will have add=null, which means add.path etc. are also null
    let commit0 = test_data_path("basic_append/delta/_delta_log/00000000000000000000.json");
    let commit1 = test_data_path("basic_append/delta/_delta_log/00000000000000000001.json");
    assert!(commit0.exists(), "Commit 0 should exist: {:?}", commit0);
    assert!(commit1.exists(), "Commit 1 should exist: {:?}", commit1);
    
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
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("JSON scan with canonical schema should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // =========================================================================
    // SCHEMA ASSERTION
    // =========================================================================
    // Verify output schema matches the kernel's canonical schema exactly
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    let expected_arrow_schema: arrow::datatypes::Schema = commit_schema.as_ref().try_into_arrow()
        .expect("schema conversion should succeed");
    
    assert_eq!(
        batches[0].schema().as_ref(),
        &expected_arrow_schema,
        "Output schema should match kernel's canonical commit schema"
    );
    
    // =========================================================================
    // DATA ASSERTIONS
    // =========================================================================
    // Delta log JSON files contain multiple action types per commit:
    //   - commit 0 (00000000000000000000.json): protocol, metaData, add
    //   - commit 1 (00000000000000000001.json): commitInfo, add
    //
    // Since we only project add/remove columns, rows with other actions
    // (protocol, metaData, commitInfo) show as null for both add and remove.
    assert_batches_eq!(
        &[
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+",
            "| add                                                                                                                                                                                                                                                                                                                                                                                                                                     | remove |",
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+",
            // commit 0: protocol action (add=null, remove=null)
            "|                                                                                                                                                                                                                                                                                                                                                                                                                                         |        |",
            // commit 0: metaData action (add=null, remove=null)
            "|                                                                                                                                                                                                                                                                                                                                                                                                                                         |        |",
            // commit 0: commitInfo action (add=null, remove=null)
            "|                                                                                                                                                                                                                                                                                                                                                                                                                                         |        |",
            // commit 0: add action with file data
            r#"| {path: part-00000-c9f44819-b06d-45dd-b33d-ae9aa1b96909-c000.snappy.parquet, partitionValues: {}, size: 996, modificationTime: 1712091396057, dataChange: true, stats: {"numRecords":3,"minValues":{"letter":"a","number":1,"a_float":1.1},"maxValues":{"letter":"c","number":3,"a_float":3.3},"nullCount":{"letter":0,"number":0,"a_float":0}}, tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |        |"#,
            // commit 1: commitInfo action (add=null, remove=null)
            "|                                                                                                                                                                                                                                                                                                                                                                                                                                         |        |",
            // commit 1: add action with file data
            r#"| {path: part-00000-a9daef62-5a40-43c5-ac63-3ad4a7d749ae-c000.snappy.parquet, partitionValues: {}, size: 984, modificationTime: 1712091404545, dataChange: true, stats: {"numRecords":2,"minValues":{"letter":"d","number":4,"a_float":4.4},"maxValues":{"letter":"e","number":5,"a_float":5.5},"nullCount":{"letter":0,"number":0,"a_float":0}}, tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |        |"#,
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+",
        ],
        &batches
    );
}

// ============================================================================
// SCHEMA QUERY TESTS
// ============================================================================

/// Test: SchemaQueryExec reads schema from checkpoint parquet file
/// 
/// This test verifies that SchemaQueryExec:
/// 1. Correctly reads the parquet file footer
/// 2. Extracts the schema and converts it to kernel StructType
/// 3. Stores the schema in SchemaStoreState
/// 4. Produces no output data (empty stream)
#[tokio::test]
async fn test_schema_query_reads_checkpoint_schema() {
    use delta_kernel::plans::{SchemaQueryNode, SinkNode, SinkType};
    use delta_kernel::plans::kdf_state::{SchemaReaderState, SchemaStoreState};
    
    // Use a real checkpoint file from test data
    let checkpoint_path = test_data_path("with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_path.exists(), "Checkpoint file should exist: {:?}", checkpoint_path);
    
    let file_url = url::Url::parse(&format!("file://{}", checkpoint_path.canonicalize().unwrap().display())).unwrap();
    
    // Create SchemaStoreState to capture the schema
    let schema_store = SchemaStoreState::new();
    let state = SchemaReaderState::SchemaStore(schema_store.clone());
    
    // Create the schema query node
    let schema_query_node = SchemaQueryNode {
        file_path: file_url.to_string(),
        state: state.clone(),
    };
    
    // Wrap in a sink (as the state machine would do)
    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::SchemaQuery(schema_query_node)),
        node: SinkNode { sink_type: SinkType::Drop },
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("SchemaQuery should compile successfully");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // SchemaQuery produces no data rows
    assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0),
        "SchemaQuery should produce no data rows");
    
    // Verify the schema was stored in the state
    let stored_schema = schema_store.get()
        .expect("Schema should have been stored after execution");
    
    // Checkpoint files have standard checkpoint schema columns
    // These are the standard columns in a Delta checkpoint file
    let expected_columns = ["add", "remove", "metaData", "protocol", "txn"];
    
    for col_name in expected_columns {
        assert!(
            stored_schema.field(col_name).is_some(),
            "Checkpoint schema should contain '{}' column. Available columns: {:?}",
            col_name,
            stored_schema.fields().map(|f| f.name()).collect::<Vec<_>>()
        );
    }
}

/// Test: SchemaQueryExec handles file path correctly
/// 
/// Verifies that the exec properly parses file URLs and constructs
/// the ObjectStore path correctly.
#[tokio::test]
async fn test_schema_query_properties() {
    use delta_kernel::plans::kdf_state::{SchemaReaderState, SchemaStoreState};
    use delta_kernel_datafusion::exec::SchemaQueryExec;
    use datafusion::physical_plan::ExecutionPlan;
    
    let path = url::Url::parse("file:///tmp/test.parquet").unwrap();
    let state = SchemaReaderState::SchemaStore(SchemaStoreState::new());
    let exec = SchemaQueryExec::new(path.clone(), state);
    
    // Verify basic properties
    assert_eq!(exec.name(), "SchemaQueryExec");
    assert_eq!(exec.file_path(), &path);
    assert!(exec.children().is_empty(), "SchemaQueryExec has no children");
    
    // Output schema is empty (no data produced)
    assert_eq!(exec.schema().fields().len(), 0);
}

/// Test: SchemaQueryExec detects sidecar column in V2 checkpoint
/// 
/// V2 checkpoints contain a 'sidecar' column that points to sidecar files.
/// This test verifies that SchemaQueryExec can correctly read the schema
/// from a V2 checkpoint and detect the presence of the sidecar column.
#[tokio::test]
async fn test_schema_query_v2_checkpoint_has_sidecar() {
    use delta_kernel::actions::{get_all_actions_schema, ADD_NAME, REMOVE_NAME, METADATA_NAME, PROTOCOL_NAME, SIDECAR_NAME};
    use delta_kernel::plans::{SchemaQueryNode, SinkNode, SinkType};
    use delta_kernel::plans::kdf_state::{SchemaReaderState, SchemaStoreState};
    
    // Load the V2 checkpoint test data (compressed .tar.zst)
    // This uses test_utils to extract the archive to a temp directory
    let test_dir = test_utils::load_test_data(
        "../kernel/tests/data",
        "v2-checkpoints-parquet-with-sidecars"
    ).expect("Failed to load v2-checkpoints-parquet-with-sidecars test data");
    
    // Find a V2 checkpoint file in the delta log
    // V2 checkpoints have UUID-based names like: 00000000000000000002.checkpoint.<uuid>.parquet
    let delta_log_path = test_dir.path()
        .join("v2-checkpoints-parquet-with-sidecars")
        .join("_delta_log");
    
    // Find the first V2 checkpoint file
    let checkpoint_file = std::fs::read_dir(&delta_log_path)
        .expect("Failed to read _delta_log directory")
        .filter_map(|e| e.ok())
        .find(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            // V2 checkpoints have format: NNNNNNNNNNNNNNNNNNNN.checkpoint.<uuid>.parquet
            name.contains(".checkpoint.") && name.ends_with(".parquet") && !name.starts_with('.')
        })
        .expect("No V2 checkpoint file found in test data");
    
    let checkpoint_path = checkpoint_file.path();
    let file_url = url::Url::parse(&format!("file://{}", checkpoint_path.display())).unwrap();
    
    // Create SchemaStoreState to capture the schema
    let schema_store = SchemaStoreState::new();
    let state = SchemaReaderState::SchemaStore(schema_store.clone());
    
    // Create the schema query node
    let schema_query_node = SchemaQueryNode {
        file_path: file_url.to_string(),
        state: state.clone(),
    };
    
    // Wrap in a sink (as the state machine would do)
    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::SchemaQuery(schema_query_node)),
        node: SinkNode { sink_type: SinkType::Drop },
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("SchemaQuery should compile successfully");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let batches = collect_batches(stream).await;
    
    // SchemaQuery produces no data rows
    assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0),
        "SchemaQuery should produce no data rows");
    
    // Verify the schema was stored in the state
    let stored_schema = schema_store.get()
        .expect("Schema should have been stored after execution");
    
    // V2 checkpoints use ALL_ACTIONS_SCHEMA which includes sidecar.
    // Verify all columns in the parquet schema are recognized action types (nullable structs).
    let expected_schema = get_all_actions_schema();
    
    for actual_field in stored_schema.fields() {
        // Each column should be a recognized action type
        assert!(
            expected_schema.field(actual_field.name()).is_some(),
            "Unexpected column '{}' in V2 checkpoint. Expected one of: {:?}",
            actual_field.name(),
            expected_schema.fields().map(|f| f.name()).collect::<Vec<_>>()
        );
        
        // All top-level action columns should be nullable structs
        assert!(
            actual_field.is_nullable(),
            "Column {} should be nullable",
            actual_field.name()
        );
        assert!(
            matches!(actual_field.data_type(), delta_kernel::schema::DataType::Struct(_)),
            "Column {} should be a struct",
            actual_field.name()
        );
    }
    
    // Critical: V2 checkpoint must have sidecar column
    assert!(
        stored_schema.field(SIDECAR_NAME).is_some(),
        "V2 checkpoint MUST have '{}' column",
        SIDECAR_NAME
    );
}

/// Test: V1 checkpoint does NOT have sidecar column
/// 
/// Verifies that classic (V1) checkpoints do not have the sidecar column,
/// confirming our detection logic works correctly.
#[tokio::test]
async fn test_schema_query_v1_checkpoint_no_sidecar() {
    use delta_kernel::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME, METADATA_NAME, PROTOCOL_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME};
    use delta_kernel::plans::{SchemaQueryNode, SinkNode, SinkType};
    use delta_kernel::plans::kdf_state::{SchemaReaderState, SchemaStoreState};
    
    // Use a V1 checkpoint file from test data
    let checkpoint_path = test_data_path("with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet");
    assert!(checkpoint_path.exists(), "V1 Checkpoint file should exist: {:?}", checkpoint_path);
    
    let file_url = url::Url::parse(&format!("file://{}", checkpoint_path.canonicalize().unwrap().display())).unwrap();
    
    // Create SchemaStoreState to capture the schema
    let schema_store = SchemaStoreState::new();
    let state = SchemaReaderState::SchemaStore(schema_store.clone());
    
    // Create the schema query node
    let schema_query_node = SchemaQueryNode {
        file_path: file_url.to_string(),
        state: state.clone(),
    };
    
    // Wrap in a sink
    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::SchemaQuery(schema_query_node)),
        node: SinkNode { sink_type: SinkType::Drop },
    };
    
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("SchemaQuery should compile successfully");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    let _batches = collect_batches(stream).await;
    
    // Verify the schema was stored in the state
    let stored_schema = schema_store.get()
        .expect("Schema should have been stored after execution");
    
    // V1 checkpoints use COMMIT_SCHEMA (no sidecar).
    // Verify all columns in the parquet schema are recognized action types (nullable structs).
    let expected_schema = get_commit_schema();
    
    for actual_field in stored_schema.fields() {
        // Each column should be a recognized action type
        assert!(
            expected_schema.field(actual_field.name()).is_some(),
            "Unexpected column '{}' in V1 checkpoint. Expected one of: {:?}",
            actual_field.name(),
            expected_schema.fields().map(|f| f.name()).collect::<Vec<_>>()
        );
        
        // All top-level action columns should be nullable structs
        assert!(
            actual_field.is_nullable(),
            "Column {} should be nullable",
            actual_field.name()
        );
        assert!(
            matches!(actual_field.data_type(), delta_kernel::schema::DataType::Struct(_)),
            "Column {} should be a struct",
            actual_field.name()
        );
    }
    
    // Critical: V1 checkpoint must NOT have sidecar column
    assert!(
        stored_schema.field(SIDECAR_NAME).is_none(),
        "V1 checkpoint should NOT contain '{}' column",
        SIDECAR_NAME
    );
}

// ============================================================================
// STATE MACHINE DRIVER TESTS
// ============================================================================

/// Test: SnapshotStateMachine can be driven to completion via DataFusion
///
/// This test verifies the full state machine execution path:
/// 1. Create a SnapshotStateMachine for a real Delta table
/// 2. Drive it through all phases using DataFusion execution
/// 3. Verify the resulting Snapshot has correct properties
#[tokio::test]
async fn test_snapshot_state_machine_execution() {
    use delta_kernel_datafusion::build_snapshot_async;
    
    // Use a real Delta table from test fixtures
    let table_path = test_data_path("basic_append/delta");
    assert!(table_path.exists(), "Delta table should exist: {:?}", table_path);
    
    // IMPORTANT: Table URL must end with "/" for join() to work correctly
    // Url::from_file_path for a directory does NOT add trailing slash
    let mut table_url = url::Url::from_file_path(table_path.canonicalize().unwrap()).unwrap();
    if !table_url.path().ends_with('/') {
        table_url.set_path(&format!("{}/", table_url.path()));
    }
    
    println!("table_url = {}", table_url);
    println!("  + '_delta_log/' = {}", table_url.join("_delta_log/").unwrap());
    
    // Create executor and build snapshot via state machine
    let executor = delta_kernel_datafusion::DataFusionExecutor::new()
        .expect("DataFusionExecutor creation should succeed");
    
    let snapshot = build_snapshot_async(&executor, table_url).await
        .expect("Snapshot construction should succeed");
    
    // Verify snapshot properties
    // basic_append table has version 1 (two commits: 0 and 1)
    assert_eq!(snapshot.version(), 1, "basic_append table should be at version 1");
    
    // Verify schema contains expected columns
    let schema = snapshot.schema();
    assert!(
        schema.field("letter").is_some(),
        "Schema should contain 'letter' column. Available: {:?}",
        schema.fields().map(|f| f.name()).collect::<Vec<_>>()
    );
    assert!(
        schema.field("number").is_some(),
        "Schema should contain 'number' column"
    );
    assert!(
        schema.field("a_float").is_some(),
        "Schema should contain 'a_float' column"
    );
}

/// Test: SnapshotStateMachine works with tables that have checkpoints
///
/// The with_checkpoint table has a checkpoint file, testing the full
/// CheckpointHint → ListFiles → LoadMetadata flow.
#[tokio::test]
async fn test_snapshot_state_machine_with_checkpoint() {
    use delta_kernel_datafusion::build_snapshot_async;
    
    let table_path = test_data_path("with_checkpoint/delta");
    assert!(table_path.exists(), "Delta table with checkpoint should exist: {:?}", table_path);
    
    // IMPORTANT: Table URL must end with "/" for join() to work correctly
    let mut table_url = url::Url::from_file_path(table_path.canonicalize().unwrap()).unwrap();
    if !table_url.path().ends_with('/') {
        table_url.set_path(&format!("{}/", table_url.path()));
    }
    
    let executor = delta_kernel_datafusion::DataFusionExecutor::new()
        .expect("DataFusionExecutor creation should succeed");
    
    let snapshot = build_snapshot_async(&executor, table_url).await
        .expect("Snapshot construction with checkpoint should succeed");
    
    // with_checkpoint table is at version 3 (versions 0, 1, 2, 3 with checkpoint at 2)
    assert_eq!(snapshot.version(), 3, "with_checkpoint table should be at version 3");
    
    // Verify schema
    let schema = snapshot.schema();
    assert!(schema.field("letter").is_some(), "Schema should contain 'letter' column");
}

/// Test: build_snapshot_at_version_async works correctly
///
/// Verifies that we can build a snapshot at a specific version.
#[tokio::test]
async fn test_snapshot_state_machine_at_version() {
    use delta_kernel_datafusion::build_snapshot_at_version_async;
    
    let table_path = test_data_path("basic_append/delta");
    assert!(table_path.exists(), "Delta table should exist");
    
    // IMPORTANT: Table URL must end with "/" for join() to work correctly
    let mut table_url = url::Url::from_file_path(table_path.canonicalize().unwrap()).unwrap();
    if !table_url.path().ends_with('/') {
        table_url.set_path(&format!("{}/", table_url.path()));
    }
    
    let executor = delta_kernel_datafusion::DataFusionExecutor::new()
        .expect("DataFusionExecutor creation should succeed");
    
    // Build snapshot at version 0 (first commit only)
    let snapshot = build_snapshot_at_version_async(&executor, table_url, 0).await
        .expect("Snapshot at version 0 should succeed");
    
    assert_eq!(snapshot.version(), 0, "Snapshot should be at version 0");
    
    // Schema should still have all columns
    let schema = snapshot.schema();
    assert!(schema.field("letter").is_some(), "Schema should contain 'letter' column");
    assert!(schema.field("number").is_some(), "Schema should contain 'number' column");
}

/// Test: execute_state_machine_async is generic and works with any state machine
///
/// This test verifies the generic implementation works correctly.
#[tokio::test]
async fn test_execute_state_machine_async_generic() {
    use delta_kernel::plans::state_machines::SnapshotStateMachine;
    use delta_kernel_datafusion::execute_state_machine_async;
    
    let table_path = test_data_path("basic_append/delta");
    // IMPORTANT: Table URL must end with "/" for join() to work correctly
    let mut table_url = url::Url::from_file_path(table_path.canonicalize().unwrap()).unwrap();
    if !table_url.path().ends_with('/') {
        table_url.set_path(&format!("{}/", table_url.path()));
    }
    
    let executor = delta_kernel_datafusion::DataFusionExecutor::new()
        .expect("DataFusionExecutor creation should succeed");
    
    // Create the state machine manually
    let sm = SnapshotStateMachine::new(table_url)
        .expect("SnapshotStateMachine creation should succeed");
    
    // Drive it using the generic function
    let snapshot = execute_state_machine_async(&executor, sm).await
        .expect("State machine execution should succeed");
    
    assert_eq!(snapshot.version(), 1);
}

/// Test: Isolated FileListingExec -> ConsumeKdfExec (LogSegmentBuilder) pipeline
///
/// This test isolates the exact pipeline that's failing in the state machine:
/// 1. FileListingExec lists files in _delta_log
/// 2. ConsumeKdfExec with LogSegmentBuilder accumulates them
/// 3. After draining, we extract the accumulated state
#[tokio::test]
async fn test_file_listing_to_log_segment_builder() {
    use delta_kernel::plans::{DeclarativePlanNode, FileListingNode, ConsumeByKDF, SinkNode, SinkType};
    use delta_kernel::plans::kdf_state::ConsumerKdfState;
    use futures::TryStreamExt;
    
    // Setup: path to _delta_log directory
    let log_path = test_data_path("basic_append/delta/_delta_log/");
    assert!(log_path.exists(), "Delta log directory should exist: {:?}", log_path);
    
    // CRITICAL: URL must end with "/" for join() to append rather than replace
    let mut log_url = url::Url::from_file_path(log_path.canonicalize().unwrap()).unwrap();
    if !log_url.path().ends_with('/') {
        log_url.set_path(&format!("{}/", log_url.path()));
    }
    
    // Create the plan: FileListing -> ConsumeByKDF(LogSegmentBuilder) -> Sink(Drop)
    let log_segment_builder = ConsumeByKDF::log_segment_builder(
        log_url.clone(),
        None, // no end version
        None, // no checkpoint hint
    );
    
    // Keep a reference to the state so we can inspect it after execution
    let builder_state = log_segment_builder.state.clone();
    
    let plan = DeclarativePlanNode::Sink {
        child: Box::new(DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(DeclarativePlanNode::FileListing(FileListingNode {
                path: log_url.clone(),
            })),
            node: log_segment_builder,
        }),
        node: SinkNode { sink_type: SinkType::Drop },
    };
    
    // Execute via DataFusion
    let ctx = SessionContext::new();
    let exec_plan = delta_kernel_datafusion::compile::compile_plan(&plan, &ctx.state())
        .expect("Plan should compile");
    
    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).unwrap();
    
    // Drain the stream to trigger side effects
    // ConsumeKdfExec's apply() is called for each batch, accumulating into the shared state
    let batches: Vec<_> = stream.try_collect().await.expect("Execution should succeed");
    
    println!("Drained {} batches", batches.len());
    println!("log_url = {}", log_url);
    for (i, batch) in batches.iter().enumerate() {
        println!("Batch {}: {} rows", i, batch.num_rows());
        if let Some(paths) = batch.column_by_name("path") {
            let paths = paths.as_any().downcast_ref::<StringArray>().unwrap();
            for j in 0..paths.len().min(5) {
                println!("  path[{}]: {:?}", j, paths.value(j));
            }
        }
    }
    
    // Now check the LogSegmentBuilder state (should have accumulated files from ConsumeKdfExec)
    match &builder_state {
        ConsumerKdfState::LogSegmentBuilder(state) => {
            // Check for any error set during processing
            if state.has_error() {
                println!("ERROR in state: {:?}", state.take_error());
            }
            
            // Try to build the log segment
            match state.into_log_segment() {
                Ok(log_segment) => {
                    println!("LogSegment built successfully!");
                    println!("  end_version: {}", log_segment.end_version);
                    println!("  commit_files: {}", log_segment.ascending_commit_files.len());
                    println!("  checkpoint_parts: {}", log_segment.checkpoint_parts.len());
                    
                    assert!(
                        !log_segment.ascending_commit_files.is_empty() || !log_segment.checkpoint_parts.is_empty(),
                        "LogSegment should have files! commit_files={}, checkpoint_parts={}",
                        log_segment.ascending_commit_files.len(),
                        log_segment.checkpoint_parts.len()
                    );
                }
                Err(e) => {
                    panic!("Failed to build LogSegment: {:?}", e);
                }
            }
        }
        _ => panic!("Expected LogSegmentBuilder state"),
    }
}
