//! Phase 4.3 — explicit **kernel/reference vs DataFusion executor** parity for every v1 declarative
//! state machine wired today (`plans::state_machines::{fsr, df}` + framework phase ops).
//!
//! ## Deferred / out of scope (documented here only)
//!
//! - **Multipart V2 checkpoints / sidecars**: kernel checkpoint builder remains classic
//!   single-file; DF multipart emission is not modeled
//!   ([`delta_kernel::plans::state_machines::df::checkpoint_write`]).
//! - **Raw parquet byte identity**: writers may choose different row-group splits or compression;
//!   parity asserts decoded Arrow batches (and row-count telemetry), not file hashes.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::nodes::{RelationHandle, WriteSink};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::kdf::{ConsumerKdf, KdfOutput};
use delta_kernel::plans::state_machines::df::{
    checkpoint_classic_parquet_write_plan, commit_action_emit_sm, commit_action_envelopes_literal,
    prepare_classic_checkpoint_parquet_materialization, CommitEnvelopeCollector,
};
use delta_kernel::plans::state_machines::framework::phase_operation::{
    PhaseOperation, SchemaQueryNode,
};
use delta_kernel::plans::state_machines::fsr::{
    try_build_fsr_footer_schema_sm, try_build_fsr_strip_then_fanout_sm, FsrFooterSchemaOutcome,
    FsrStripThenFanoutOutcome,
};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Engine as KernelEngine, EngineData, EvaluationHandler, Snapshot};
use delta_kernel_datafusion_engine::{DataFusionExecutor, DriveOpts};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use tempfile::tempdir;
use url::Url;

fn concat_or_clone(batches: &[RecordBatch]) -> RecordBatch {
    match batches.len() {
        0 => panic!("empty batches"),
        1 => batches[0].clone(),
        _ => concat_batches(&batches[0].schema(), batches).expect("concat_batches"),
    }
}

fn assert_batch_column_data_equal(
    kernel_schema: &Arc<StructType>,
    expected: &RecordBatch,
    actual: &RecordBatch,
) {
    assert_eq!(expected.num_rows(), actual.num_rows(), "row count");
    assert_eq!(expected.num_columns(), actual.num_columns(), "column count");
    let canonical: Arc<delta_kernel::arrow::datatypes::Schema> = Arc::new(
        kernel_schema
            .as_ref()
            .try_into_arrow()
            .expect("kernel→arrow schema"),
    );
    let exp = RecordBatch::try_new(canonical.clone(), expected.columns().to_vec())
        .expect("canonical exp");
    let act = RecordBatch::try_new(canonical, actual.columns().to_vec()).expect("canonical act");
    assert_eq!(exp, act);
}

fn write_commit_json(log: &Path, version: u64, lines: &[serde_json::Value]) {
    let mut buf = String::new();
    for v in lines {
        buf.push_str(&serde_json::to_string(v).expect("json"));
        buf.push('\n');
    }
    let name = format!("{:020}.json", version);
    fs::write(log.join(name), buf).expect("commit write");
}

fn setup_v2_checkpoint_fixture(table_root: &Path) {
    let log = table_root.join("_delta_log");
    fs::create_dir_all(&log).expect("mkdir log");

    write_commit_json(
        &log,
        0,
        &[
            json!({
                "add": {
                    "path": "fake_path_2",
                    "partitionValues": {},
                    "size": 50_i64,
                    "modificationTime": 1_i64,
                    "dataChange": true,
                    "stats": r#"{"numRecords":50,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":5}}"#
                }
            }),
            json!({
                "remove": {
                    "path": "fake_path_1",
                    "deletionTimestamp": 9223372036854775807_i64,
                    "dataChange": true,
                }
            }),
        ],
    );

    write_commit_json(
        &log,
        1,
        &[
            json!({
                "metaData": {
                    "id": "388876aa-094f-49fb-aabd-be833707970b",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0_i64,
                }
            }),
            json!({
                "protocol": {
                    "minReaderVersion": 3_i32,
                    "minWriterVersion": 7_i32,
                    "readerFeatures": ["v2Checkpoint"],
                    "writerFeatures": ["v2Checkpoint"],
                }
            }),
        ],
    );
}

fn read_all_parquet_batches(path: &Path) -> Vec<RecordBatch> {
    let file = fs::File::open(path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader");
    reader.map(|b| b.expect("batch")).collect()
}

fn number_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "n",
        DataType::LONG,
    )]))
}

fn long_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "x",
        DataType::LONG,
    )]))
}

fn kernel_literal_record_batch(schema: SchemaRef, rows: &[Vec<Scalar>]) -> RecordBatch {
    let handler = ArrowEvaluationHandler;
    let row_refs: Vec<&[Scalar]> = rows.iter().map(|r| r.as_slice()).collect();
    handler
        .create_many(schema, &row_refs)
        .expect("create_many")
        .try_into_record_batch()
        .expect("record batch")
}

fn action_json_batch(lines: &[String]) -> RecordBatch {
    use delta_kernel::arrow::array::StringArray;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDT, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new(
        "action_json",
        ArrowDT::Utf8,
        true,
    )]));
    let arr = StringArray::from_iter(lines.iter().map(|s| Some(s.as_str())));
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("batch")
}

#[tokio::test]
async fn parity_fsr_strip_fanout_df_matches_literal_row_count_reference() {
    let strip_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());
    let strip_rows = vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]];
    let fanout_schema = Arc::clone(&strip_schema);
    let fanout_rows = vec![
        vec![Scalar::Long(10)],
        vec![Scalar::Long(11)],
        vec![Scalar::Long(12)],
    ];

    let kernel_reference = FsrStripThenFanoutOutcome {
        strip_rows: strip_rows.len(),
        fanout_rows: fanout_rows.len(),
    };

    let sm =
        try_build_fsr_strip_then_fanout_sm(strip_schema, strip_rows, fanout_schema, fanout_rows)
            .expect("build SM");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let df_outcome = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive");

    assert_eq!(
        df_outcome, kernel_reference,
        "FSR RowCounter totals match deterministic literal row counts (kernel semantics reference)"
    );
}

#[tokio::test]
async fn parity_fsr_footer_schema_coroutine_matches_direct_kernel_footer_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("probe.parquet");
    use delta_kernel::arrow::array::Int64Array;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    let arrow_schema = ArrowSchema::new(vec![Field::new("z", ArrowDataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![9_i64]))],
    )
    .unwrap();
    let file = fs::File::create(&path).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, Arc::new(arrow_schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let url = Url::from_file_path(&path).unwrap();

    let sm = try_build_fsr_footer_schema_sm(url.to_string()).expect("build SM");
    let ex = DataFusionExecutor::try_new().expect("executor");
    let df_out = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive");

    let meta = ex
        .engine()
        .storage_handler()
        .head(&url)
        .expect("head parquet");
    let kernel_footer_schema = ex.read_parquet_footer_schema(&meta).expect("footer schema");

    assert_eq!(
        df_out,
        FsrFooterSchemaOutcome {
            column_count: kernel_footer_schema.fields().len(),
        },
        "FSR schema-query SM outcome aligns with kernel ParquetHandler footer schema column count"
    );
}

#[tokio::test]
async fn parity_phase_schema_query_matches_read_parquet_footer_schema_helper() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("chunk.parquet");
    use delta_kernel::arrow::array::Int64Array;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    let arrow_schema = ArrowSchema::new(vec![Field::new("id", ArrowDataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let file = fs::File::create(&path).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, Arc::new(arrow_schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let url = Url::from_file_path(&path).unwrap();
    let executor = DataFusionExecutor::try_new().unwrap();

    let meta = executor.engine().storage_handler().head(&url).unwrap();
    let kernel_direct = executor.read_parquet_footer_schema(&meta).unwrap();

    let node = SchemaQueryNode::new(url.as_str());
    let state = executor
        .execute_phase_operation(PhaseOperation::SchemaQuery(node))
        .await
        .expect("schema query phase");

    let phase_schema = state.take_schema().expect("footer schema submitted");

    assert_eq!(
        phase_schema.as_ref(),
        kernel_direct.as_ref(),
        "PhaseOperation::SchemaQuery (DF executor driver) matches direct kernel footer schema read"
    );
}

#[tokio::test]
async fn parity_phase_plans_relation_pipe_matches_kernel_literal_materialization() {
    let schema = long_schema();
    let rows = vec![vec![Scalar::Long(100)], vec![Scalar::Long(200)]];
    let kernel_reference_batch = kernel_literal_record_batch(Arc::clone(&schema), &rows);

    let handle = RelationHandle::fresh("parity_pipe", Arc::clone(&schema));
    let producer = DeclarativePlanNode::literal(Arc::clone(&schema), rows.clone())
        .unwrap()
        .into_relation(handle.clone());
    let consumer = DeclarativePlanNode::relation(handle.clone()).results();

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_phase_operation(PhaseOperation::Plans(vec![producer, consumer]))
        .await
        .expect("phase Plans");

    let read_plan = DeclarativePlanNode::relation(handle).results();
    let df_batches = executor
        .execute_plan_collect(read_plan)
        .await
        .expect("collect");
    let df_concat = concat_or_clone(&df_batches);

    assert_batch_column_data_equal(&schema, &kernel_reference_batch, &df_concat);
}

#[tokio::test]
async fn parity_checkpoint_classic_kernel_parquet_write_matches_df_relation_write_sink() {
    let dir = tempdir().expect("table dir");
    setup_v2_checkpoint_fixture(dir.path());

    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let table_root = Url::from_directory_path(dir.path()).expect("table url");
    let snapshot = Arc::new(
        Snapshot::builder_for(table_root)
            .build(engine.as_ref())
            .expect("snapshot"),
    );
    let writer = Snapshot::create_checkpoint_writer(Arc::clone(&snapshot)).expect("writer");

    let (handle, batches, dest_checkpoint_url, _state) =
        prepare_classic_checkpoint_parquet_materialization(engine.as_ref(), &writer)
            .expect("materialize checkpoint rows");

    let kernel_reference_path = dir.path().join("kernel_reference.checkpoint.parquet");
    let kernel_url = Url::from_file_path(&kernel_reference_path).expect("kernel ref url");
    let batches_for_kernel = batches.clone();
    let kernel_iter = batches_for_kernel
        .into_iter()
        .map(|rb| Ok(Box::new(ArrowEngineData::new(rb)) as Box<dyn EngineData>));
    engine
        .parquet_handler()
        .write_parquet_file(kernel_url, Box::new(kernel_iter))
        .expect("kernel checkpoint parquet write");

    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    ex.relation_batch_registry()
        .register(handle.id, batches.clone());
    let plan = checkpoint_classic_parquet_write_plan(handle, dest_checkpoint_url.clone());
    ex.drive_checkpoint_classic_parquet_write_sm(plan)
        .await
        .expect("DF checkpoint parquet SM");

    let cp_df_path = dest_checkpoint_url.to_file_path().expect("checkpoint path");
    let batches_k = read_all_parquet_batches(&kernel_reference_path);
    let batches_d = read_all_parquet_batches(&cp_df_path);

    assert_eq!(
        concat_or_clone(&batches_k),
        concat_or_clone(&batches_d),
        "classic V2 checkpoint rows: kernel ParquetHandler stream write vs DF Relation→Write sink"
    );
}

#[tokio::test]
async fn parity_insert_kernel_parquet_handler_write_matches_df_insert_sm_rows() {
    let dir = tempdir().unwrap();
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());

    let schema = number_schema();
    let rows = vec![
        vec![Scalar::Long(11)],
        vec![Scalar::Long(22)],
        vec![Scalar::Long(33)],
    ];
    let rb = kernel_literal_record_batch(Arc::clone(&schema), &rows);

    let kernel_path = dir.path().join("kernel.parquet");
    let kernel_url = Url::from_file_path(&kernel_path).unwrap();

    let batch_iter = std::iter::once(Ok(
        Box::new(ArrowEngineData::new(rb.clone())) as Box<dyn EngineData>
    ));
    engine
        .parquet_handler()
        .write_parquet_file(kernel_url, Box::new(batch_iter))
        .expect("kernel parquet write");

    let df_path = dir.path().join("df.parquet");
    let df_url = Url::from_file_path(&df_path).unwrap();
    let plan = DeclarativePlanNode::literal(Arc::clone(&schema), rows.clone())
        .unwrap()
        .into_write(WriteSink::parquet(df_url));

    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).unwrap();
    let written = ex.drive_insert_write_sm(plan).await.unwrap();
    assert_eq!(written, rows.len() as u64);

    let kernel_batches = read_all_parquet_batches(&kernel_path);
    let df_batches = read_all_parquet_batches(&df_path);

    assert_batch_column_data_equal(
        &schema,
        &concat_or_clone(&kernel_batches),
        &concat_or_clone(&df_batches),
    );
}

#[tokio::test]
async fn parity_commit_emit_df_sm_matches_direct_commit_envelope_collector() {
    let lines: Vec<String> = vec![
        r#"{"commitInfo":{"operation":"WRITE","engineInfo":"kernel-test"}}"#.into(),
        r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":42}}"#.into(),
    ];

    let batch = action_json_batch(&lines);
    let mut collector = CommitEnvelopeCollector::default();
    collector
        .apply(&ArrowEngineData::new(batch))
        .expect("collector apply");
    let kernel_reference =
        CommitEnvelopeCollector::into_output(vec![collector]).expect("into_output");

    let (plan, extractor) =
        commit_action_envelopes_literal(lines.clone()).expect("literal envelopes");
    let sm = commit_action_emit_sm(plan, extractor).expect("SM");
    let df_lines = DataFusionExecutor::try_new()
        .expect("executor")
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive");

    assert_eq!(
        df_lines, kernel_reference,
        "commit_action_emit SM (DF Plans phase) matches CommitEnvelopeCollector applied in-process \
         (kernel reference)"
    );

    assert!(serde_json::from_str::<serde_json::Value>(&df_lines[0])
        .expect("json")
        .get("commitInfo")
        .is_some());
    assert!(serde_json::from_str::<serde_json::Value>(&df_lines[1])
        .expect("json")
        .get("add")
        .is_some());
}
