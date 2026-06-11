//! Round-trip integration tests for the `compile_plan` lowering. Each test builds a plan via
//! the [`Context`] builder, wraps it in a [`ResultPlan`], and runs it through
//! [`DataFusionExecutor::result_plan_to_dataframe`] -- exercising the per-`NodeKind` lowerings
//! without requiring a state machine.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use common::SumRowsReducer;
use delta_kernel::arrow::array::{AsArray, RecordBatch};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::expressions::{col, lit, ColumnName, Predicate};
use delta_kernel::plans::ir::nodes::{FileType, LoadColumnInfo, LoadNode, ReduceSink};
use delta_kernel::plans::ir::plan::ResultPlan;
use delta_kernel::plans::state_machines::framework::plan_context::Context;
use delta_kernel::plans::state_machines::framework::state_machine::{
    EngineRequest, EngineResponse,
};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel_datafusion_engine::{testing, DataFusionExecutor};
use test_utils::parquet::write_i64_parquet;
use url::Url;

fn run_to_one_batch(rp: ResultPlan) -> RecordBatch {
    let exec = DataFusionExecutor::try_new().expect("executor");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let batches = runtime
        .block_on(testing::collect_result_plan(&exec, rp))
        .expect("collect");
    assert!(!batches.is_empty(), "expected at least one batch");
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).expect("concat")
}

fn long_field(name: &str) -> StructField {
    StructField::nullable(name, DataType::LONG)
}

fn long_schema(fields: &[&str]) -> SchemaRef {
    Arc::new(StructType::try_new(fields.iter().map(|n| long_field(n))).expect("schema"))
}

fn long_col(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column {name} not found in {:?}", batch.schema()));
    batch
        .column(idx)
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}

/// `Values` rows lower to a `LogicalPlan::Values` whose batches preserve row order.
#[test]
fn values_round_trip_preserves_rows() {
    let ctx = Context::new();
    let builder = ctx
        .values(long_schema(&["a", "b"]), [[1i64, 10], [2, 20], [3, 30]])
        .unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let batch = run_to_one_batch(rp);
    assert_eq!(long_col(&batch, "a"), vec![1, 2, 3]);
    assert_eq!(long_col(&batch, "b"), vec![10, 20, 30]);
}

/// `Filter` keeps rows where the predicate evaluates true.
#[test]
fn filter_drops_rows_where_predicate_is_false() {
    let ctx = Context::new();
    let src = ctx
        .values(long_schema(&["x"]), [[1i64], [2], [3], [4]])
        .unwrap();
    let builder = src.filter(Predicate::gt(col("x"), lit(2i64))).unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let batch = run_to_one_batch(rp);
    let kept = long_col(&batch, "x");
    let kept_set: HashSet<i64> = kept.iter().copied().collect();
    assert_eq!(kept_set, HashSet::from([3, 4]));
}

/// `Project` renames + reorders columns; the output schema honors the named expression list.
#[test]
fn project_renames_columns() {
    let ctx = Context::new();
    let src = ctx.values(long_schema(&["a", "b"]), [[11i64, 22]]).unwrap();
    let builder = src
        .project_with_schema([col("b"), col("a")], long_schema(&["y", "x"]))
        .unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let batch = run_to_one_batch(rp);
    assert_eq!(long_col(&batch, "y"), vec![22]);
    assert_eq!(long_col(&batch, "x"), vec![11]);
}

/// `Union { ordered: true }` concatenates inputs in order. We tag each side with a known
/// constant so the order is observable post-collect.
#[test]
fn ordered_union_preserves_input_order() {
    let ctx = Context::new();
    let left = ctx.values(long_schema(&["v"]), [[1i64], [2]]).unwrap();
    let right = ctx.values(long_schema(&["v"]), [[3i64], [4]]).unwrap();
    let builder = left.union_ordered(&[right]).unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let batch = run_to_one_batch(rp);
    assert_eq!(long_col(&batch, "v"), vec![1, 2, 3, 4]);
}

/// `EquiJoin { kind: LeftAnti }` emits each left row whose key matches no right row.
#[test]
fn left_anti_join_drops_matched_left_rows() {
    let ctx = Context::new();
    let left = ctx.values(long_schema(&["k"]), [[1i64], [2], [3]]).unwrap();
    let right = ctx.values(long_schema(&["k"]), [[2i64]]).unwrap();
    let builder = left.left_anti_join(right, [(col("k"), col("k"))]).unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let batch = run_to_one_batch(rp);
    let kept: HashSet<i64> = long_col(&batch, "k").into_iter().collect();
    assert_eq!(kept, HashSet::from([1, 3]));
}

/// `EngineRequest::Reduce` drains a plan dataflow into a [`KernelReducer`]
/// (`KernelReducer::finish` -> `usize` row count) and the executor returns the finalized
/// handle as `EngineResponse::Reducer`, keyed by the sink's token.
#[tokio::test]
async fn step_reduce_drains_plan_into_reducer_handle() {
    let ctx = Context::new();
    let src = ctx
        .values(long_schema(&["v"]), [[1i64], [2], [3], [4]])
        .unwrap();
    let builder = src.filter(Predicate::gt(col("v"), lit(2i64))).unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();
    let terminal = rp.result;
    let nodes = rp.plan.nodes;

    let sink = ReduceSink::new_reducer(SumRowsReducer::new("plan.reduce_test"));
    let token = sink.token.clone();

    let executor = DataFusionExecutor::try_new().unwrap();
    let payload = executor
        .execute_step(EngineRequest::Reduce {
            nodes,
            terminal,
            sink,
        })
        .await
        .expect("EngineRequest::Reduce execution");

    let handle = match payload {
        EngineResponse::Reducer(h) => h,
        other => panic!("expected EngineResponse::Reducer, got {other:?}"),
    };
    assert_eq!(
        handle.token, token,
        "finished handle carries the sink token"
    );
    let total = *handle
        .erased
        .downcast::<usize>()
        .expect("SumRowsReducer finishes with usize");
    assert_eq!(total, 2, "filter keeps rows with v > 2 (i.e., 3 and 4)");
}

/// `NodeKind::Load` reads each upstream row's path-column file in `file_type`, broadcasts the
/// `metadata_derived_columns` onto every emitted file row, and lifts the `file_schema` columns
/// alongside. Verifies the `NodeKind::Load(LoadNode { ... })` lowering: the engine reads the
/// `LoadNode` payload directly and threads a `LoadTableProvider`/`LoadExec` into the compiled
/// `LogicalPlan`.
#[tokio::test]
async fn load_node_reads_files_and_broadcasts_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("data.parquet");
    write_i64_parquet(&parquet_path, "x", &[10_i64, 20_i64]);
    let rel_path = parquet_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let base_url = Url::from_directory_path(dir.path()).unwrap();

    let upstream_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("tag", DataType::STRING),
        ])
        .unwrap(),
    );
    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());

    let ctx = Context::new();
    let upstream = ctx
        .values(
            upstream_schema,
            [
                [rel_path.clone(), "alpha".into()],
                [rel_path, "beta".into()],
            ],
        )
        .unwrap();
    let builder = upstream
        .load(LoadNode {
            file_schema,
            file_type: FileType::Parquet,
            base_url: Some(base_url),
            metadata_derived_columns: vec![ColumnName::new(["tag"])],
            file_meta: LoadColumnInfo {
                path_column: ColumnName::new(["path"]),
                file_size_column: None,
                num_records_column: None,
            },
            dv_ref: None,
        })
        .unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let exec = DataFusionExecutor::try_new().unwrap();
    let batches = testing::collect_result_plan(&exec, rp).await.unwrap();
    assert!(!batches.is_empty(), "expected at least one batch");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // Two upstream rows, each broadcasting onto two file rows -> 4 emitted rows.
    assert_eq!(total_rows, 4);

    // Every emitted row carries the upstream `tag` value broadcast onto each file row.
    let schema = batches[0].schema();
    assert!(schema.field_with_name("x").is_ok());
    assert!(schema.field_with_name("tag").is_ok());
}

/// `MaxByVersion` keeps the row with the largest `version` per group key, narrowed to the
/// declared `output_schema` fields (group_by exprs are aggregation-internal and do NOT appear
/// in the output).
#[test]
fn max_by_version_keeps_top_row_per_group_and_narrows_to_value_columns() {
    let ctx = Context::new();
    let src = ctx
        .values(
            long_schema(&["k", "version", "payload"]),
            [
                [1i64, 1, 100],
                [1, 3, 300],
                [1, 2, 200],
                [2, 5, 500],
                [2, 7, 700],
            ],
        )
        .unwrap();
    let builder = src
        .max_by_version([col("k")], col("version"), ["payload"])
        .unwrap();
    let rp = ctx.into_result_plan(builder).unwrap();

    let batch = run_to_one_batch(rp);
    // Output is `output_schema` fields only.
    assert_eq!(batch.schema().fields().len(), 1);
    assert_eq!(batch.schema().field(0).name(), "payload");
    let payloads: HashSet<i64> = long_col(&batch, "payload").into_iter().collect();
    assert_eq!(payloads, HashSet::from([300, 700]));
}
