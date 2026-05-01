//! Integration tests for [`delta_kernel::plans::ir::nodes::SinkType::Write`] lowering and artifacts.

use std::sync::Arc;

use delta_kernel::arrow::array::AsArray;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::WriteSink;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

use delta_kernel_datafusion_engine::{DataFusionExecutor, LiftDeltaErr};

fn number_schema() -> delta_kernel::schema::SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "n",
        DataType::LONG,
    )]))
}

#[tokio::test]
async fn write_sink_parquet_round_trips_literal_row_and_reports_row_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.parquet");
    let dest = Url::from_file_path(&path).expect("file url");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let plan = DeclarativePlanNode::literal_row(number_schema(), vec![Scalar::Long(42)])
        .expect("literal")
        .into_write(WriteSink::parquet(dest));

    let batches = ex.execute_plan_collect(plan).await.expect("execute");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let counts = batches[0]
        .column(0)
        .as_primitive::<delta_kernel::arrow::datatypes::UInt64Type>();
    assert_eq!(counts.value(0), 1);

    let file = std::fs::File::open(&path).expect("open parquet");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader");
    let batch = reader.next().expect("some batch").expect("batch ok");
    assert_eq!(batch.num_rows(), 1);
    let n = batch
        .column(0)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    assert_eq!(n.value(0), 42);
}

#[tokio::test]
async fn write_sink_json_lines_writes_ndjson_and_reports_row_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.json");
    let dest = Url::from_file_path(&path).expect("file url");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let plan = DeclarativePlanNode::literal_row(number_schema(), vec![Scalar::Long(7)])
        .expect("literal")
        .into_write(WriteSink::json_lines(dest));

    let batches = ex.execute_plan_collect(plan).await.expect("execute");
    let counts = batches[0]
        .column(0)
        .as_primitive::<delta_kernel::arrow::datatypes::UInt64Type>();
    assert_eq!(counts.value(0), 1);

    let text = std::fs::read_to_string(&path).expect("read json");
    assert!(
        text.trim().contains("\"n\":7") || text.trim().contains("\"n\": 7"),
        "unexpected NDJSON payload: {text:?}"
    );
}

#[tokio::test]
async fn write_sink_empty_stream_still_materializes_single_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.parquet");
    let dest = Url::from_file_path(&path).expect("file url");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let plan = DeclarativePlanNode::literal(number_schema(), Vec::<Vec<Scalar>>::new())
        .expect("empty literal")
        .into_write(WriteSink::parquet(dest));

    let batches = ex.execute_plan_collect(plan).await.expect("execute");
    let counts = batches[0]
        .column(0)
        .as_primitive::<delta_kernel::arrow::datatypes::UInt64Type>();
    assert_eq!(counts.value(0), 0);

    assert!(path.exists(), "parquet path should exist after empty write");
    let file = std::fs::File::open(&path).expect("open parquet");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader");
    assert!(
        reader.next().is_none(),
        "empty logical relation should yield no row batches in parquet"
    );
}

#[tokio::test]
async fn write_dispatch_non_write_sinks_unchanged_results_plan_streams_rows() {
    let ex = DataFusionExecutor::try_new().expect("executor");
    let plan = DeclarativePlanNode::literal_row(number_schema(), vec![Scalar::Long(99)])
        .expect("literal")
        .results();

    let stream = ex.execute_plan_to_stream(plan).await.expect("stream");
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .lift()
        .expect("collect");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}
