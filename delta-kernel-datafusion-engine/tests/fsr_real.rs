//! End-to-end FSR assertions with explicit expected rows.
//!
//! We assert:
//! 1) terminal `Snapshot::full_state` output rows (live add-file paths) against exact expected
//!    pretty-table rows; and
//! 2) intermediate commit-dedup action kinds contain protocol / metadata / domain metadata rows on
//!    a synthetic log fixture; and
//! 3) full `ToJson` payloads on real fixtures:
//!    - `table-with-dv-small`: protocol, metadata, full add (with DV), full remove.
//!    - `app-txn-no-checkpoint`: protocol, metadata, txn (two apps), multiple adds across commits.
//!
//!    No JSON commit fixture under `kernel/tests/data` contains a top-level `domainMetadata`
//!    action today (`crc-full` only lists `domainMetadata` in protocol writerFeatures); synthetic
//!    rows still cover domain metadata payloads.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::{Expression, UnaryExpressionOp};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::{Plan, RelationHandle};
use delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation;
use delta_kernel::plans::state_machines::fsr::full_state::FSR_COMMIT_DEDUP;
use delta_kernel::plans::state_machines::fsr::{
    build_fsr_plans, checkpoint_shape_from_last_checkpoint,
};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::{Engine as KernelEngine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use url::Url;

fn commit_dedup_sink_handle(plans: &[Plan]) -> RelationHandle {
    plans
        .iter()
        .find_map(|p| match &p.sink.sink_type {
            SinkType::Relation(h) if h.name == FSR_COMMIT_DEDUP => Some(h.clone()),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "FSR plans must include a Relation sink named {FSR_COMMIT_DEDUP:?}; got plan sinks: {:?}",
                plans
                    .iter()
                    .map(|p| &p.sink.sink_type)
                    .collect::<Vec<_>>()
            )
        })
}

fn fixture_table(name: &str) -> Url {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data")
        .join(name);
    Url::from_directory_path(root.canonicalize().expect("fixture path")).expect("table url")
}

fn default_engine() -> Arc<dyn KernelEngine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

fn extract_sorted_non_null_paths_from_results(batches: &[RecordBatch]) -> Vec<String> {
    let merged = if batches.len() == 1 {
        batches[0].clone()
    } else {
        concat_batches(&batches[0].schema(), batches).expect("concat results batches")
    };
    let path_col = merged.column(0).as_string::<i32>();
    let mut out: Vec<String> = (0..path_col.len())
        .filter_map(|i| path_col.is_valid(i).then(|| path_col.value(i).to_string()))
        .collect();
    out.sort();
    out
}

async fn run_full_state_results(table: &str) -> Vec<RecordBatch> {
    let table_root = fixture_table(table);
    let engine = default_engine();
    let snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .expect("snapshot");
    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    let sm = snapshot.full_state().expect("full_state SM");
    let ((), fsr_batches) = ex
        .drive_coroutine_sm_collecting_results(sm)
        .await
        .expect("drive full_state");
    fsr_batches
}

fn write_json_commit(path: &Path) {
    let rows = [
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
        r#"{"domainMetadata":{"domain":"d1","configuration":"c1","removed":false}}"#,
        r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":123}}"#,
        r#"{"add":{"path":"x.parquet","partitionValues":{"p":"1"},"size":11,"modificationTime":22,"dataChange":true,"stats":"{\"numRecords\":1}","tags":{"t1":"v1","t2":null},"deletionVector":{"storageType":"u","pathOrInlineDv":"ab", "offset":3,"sizeInBytes":4,"cardinality":5},"baseRowId":6,"defaultRowCommitVersion":7,"clusteringProvider":"cp"}}"#,
        r#"{"remove":{"path":"y.parquet","deletionTimestamp":33,"dataChange":false,"extendedFileMetadata":true,"partitionValues":{"p":"2"},"size":44,"stats":"{\"numRecords\":1}","tags":{"rt":"rv"},"deletionVector":{"storageType":"u","pathOrInlineDv":"cd","offset":8,"sizeInBytes":9,"cardinality":10},"baseRowId":11,"defaultRowCommitVersion":12}}"#,
    ];
    fs::write(path, format!("{}\n", rows.join("\n"))).expect("write commit json");
}

fn build_synthetic_protocol_metadata_domain_table() -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir = dir.path().join("_delta_log");
    fs::create_dir_all(&log_dir).expect("mkdir _delta_log");
    write_json_commit(&log_dir.join("00000000000000000000.json"));
    dir
}

async fn commit_dedup_batches_for_snapshot(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn KernelEngine>,
) -> Vec<RecordBatch> {
    let shape = checkpoint_shape_from_last_checkpoint(&snapshot).expect("shape");
    let plans = build_fsr_plans(&snapshot, shape).expect("build fsr plans");
    let commit_dedup_handle = commit_dedup_sink_handle(&plans);
    let ex = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    ex.execute_phase_operation(PhaseOperation::Plans(plans))
        .await
        .expect("run fsr plans");
    ex
        .relation_batch_registry()
        .get_cloned(commit_dedup_handle.id)
        .expect("commit_dedup relation batches")
}

async fn dedup_action_kinds_for_synthetic_table() -> Vec<String> {
    let dir = build_synthetic_protocol_metadata_domain_table();
    let table_url =
        Url::from_directory_path(dir.path().canonicalize().expect("canon tmp table")).expect("url");
    let engine = default_engine();
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot");
    let batches = commit_dedup_batches_for_snapshot(snapshot, engine).await;

    let mut kinds = Vec::new();
    for b in batches {
        let schema = b.schema();
        let idx_add = schema.index_of("add").expect("add idx");
        let idx_remove = schema.index_of("remove").expect("remove idx");
        let idx_protocol = schema.index_of("protocol").expect("protocol idx");
        let idx_meta = schema.index_of("metaData").expect("metaData idx");
        let idx_domain = schema.index_of("domainMetadata").expect("domain idx");
        let idx_txn = schema.index_of("txn").expect("txn idx");
        let add = b.column(idx_add).as_struct_opt().expect("add struct");
        let remove = b.column(idx_remove).as_struct_opt().expect("remove struct");
        let protocol = b
            .column(idx_protocol)
            .as_struct_opt()
            .expect("protocol struct");
        let meta = b.column(idx_meta).as_struct_opt().expect("meta struct");
        let domain = b.column(idx_domain).as_struct_opt().expect("domain struct");
        let txn = b.column(idx_txn).as_struct_opt().expect("txn struct");
        for i in 0..b.num_rows() {
            if add.is_valid(i) {
                kinds.push("add".to_string());
            } else if remove.is_valid(i) {
                kinds.push("remove".to_string());
            } else if protocol.is_valid(i) {
                kinds.push("protocol".to_string());
            } else if meta.is_valid(i) {
                kinds.push("metaData".to_string());
            } else if domain.is_valid(i) {
                kinds.push("domainMetadata".to_string());
            } else if txn.is_valid(i) {
                kinds.push("txn".to_string());
            } else {
                kinds.push("none".to_string());
            }
        }
    }
    kinds.sort();
    kinds
}

async fn dedup_full_rows_for_synthetic_table() -> Vec<RecordBatch> {
    let dir = build_synthetic_protocol_metadata_domain_table();
    let table_url =
        Url::from_directory_path(dir.path().canonicalize().expect("canon tmp table")).expect("url");
    let engine = default_engine();
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot");
    commit_dedup_batches_for_snapshot(snapshot, engine).await
}

async fn commit_dedup_merged_for_real_fixture(table_name: &str) -> RecordBatch {
    let table_root = fixture_table(table_name);
    let engine = default_engine();
    let snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .expect("snapshot");
    let dedup_batches = commit_dedup_batches_for_snapshot(snapshot, engine).await;
    if dedup_batches.len() == 1 {
        dedup_batches.into_iter().next().expect("one batch")
    } else {
        concat_batches(&dedup_batches[0].schema(), &dedup_batches).expect("concat commit-dedup")
    }
}

fn payload_stable_sort_key(kind: &str, v: &JsonValue) -> (String, String) {
    let tie = match kind {
        "add" | "remove" => v
            .get("path")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        "metaData" => v
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        "domainMetadata" => format!(
            "{}|{}",
            v.get("domain").and_then(JsonValue::as_str).unwrap_or(""),
            v.get("configuration")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
        ),
        "txn" => v
            .get("appId")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    };
    (kind.to_string(), tie)
}

fn evaluate_to_json_column(batch: &RecordBatch, col: &'static str) -> StringArray {
    let arr = evaluate_expression(
        &Expression::unary(UnaryExpressionOp::ToJson, Expression::column([col])),
        batch,
        Some(&KernelDataType::STRING),
    )
    .expect("evaluate to_json");
    arr.as_string::<i32>().clone()
}

fn full_action_payload_rows(batch: &RecordBatch) -> Vec<(String, JsonValue)> {
    let add_json = evaluate_to_json_column(batch, "add");
    let remove_json = evaluate_to_json_column(batch, "remove");
    let protocol_json = evaluate_to_json_column(batch, "protocol");
    let metadata_json = evaluate_to_json_column(batch, "metaData");
    let domain_json = evaluate_to_json_column(batch, "domainMetadata");
    let txn_json = evaluate_to_json_column(batch, "txn");
    let mut rows = Vec::new();
    for i in 0..batch.num_rows() {
        let push = |kind: &str, s: &str, out: &mut Vec<(String, JsonValue)>| {
            out.push((
                kind.to_string(),
                serde_json::from_str::<JsonValue>(s).expect("json payload"),
            ));
        };
        if add_json.is_valid(i) {
            push("add", add_json.value(i), &mut rows);
        } else if remove_json.is_valid(i) {
            push("remove", remove_json.value(i), &mut rows);
        } else if protocol_json.is_valid(i) {
            push("protocol", protocol_json.value(i), &mut rows);
        } else if metadata_json.is_valid(i) {
            push("metaData", metadata_json.value(i), &mut rows);
        } else if domain_json.is_valid(i) {
            push("domainMetadata", domain_json.value(i), &mut rows);
        } else if txn_json.is_valid(i) {
            push("txn", txn_json.value(i), &mut rows);
        }
    }
    rows.sort_by(|(k1, v1), (k2, v2)| {
        payload_stable_sort_key(k1, v1).cmp(&payload_stable_sort_key(k2, v2))
    });
    rows
}

#[tokio::test]
async fn fsr_results_no_checkpoint_expected_rows() {
    let batches = run_full_state_results("app-txn-no-checkpoint").await;
    let actual = extract_sorted_non_null_paths_from_results(&batches);
    let expected = vec![
        "modified=2021-02-01/part-00001-80996595-a345-43b7-b213-e247d6f091f7-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-01/part-00001-8ebcaf8b-0f48-4213-98c9-5c2156d20a7e-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-9a16b9f6-c12a-4609-a9c4-828eacb9526a-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-bfac5c74-426e-410f-ab74-21a64e518e9c-c000.snappy.parquet"
            .to_string(),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_results_v1_checkpoint_expected_rows() {
    let batches = run_full_state_results("app-txn-checkpoint").await;
    let actual = extract_sorted_non_null_paths_from_results(&batches);
    let expected = vec![
        "modified=2021-02-01/part-00001-3b6e7f26-8140-4067-8504-47540a363758-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-01/part-00001-7e32952f-35ad-423c-8926-dbd3d264b1ee-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-5113d412-8632-458b-a49a-f34db1069081-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-f968feb7-7f54-40c5-8aea-6a3b7a406d9c-c000.snappy.parquet"
            .to_string(),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_commit_dedup_includes_protocol_metadata_and_domain_metadata_rows() {
    let actual_kinds = dedup_action_kinds_for_synthetic_table().await;
    let expected_kinds = vec![
        "add".to_string(),
        "domainMetadata".to_string(),
        "metaData".to_string(),
        "protocol".to_string(),
        "remove".to_string(),
        "txn".to_string(),
    ];
    assert_eq!(actual_kinds, expected_kinds);
}

#[tokio::test]
async fn fsr_commit_dedup_full_rows_expected_table_includes_protocol_metadata_domain() {
    let dedup_batches = dedup_full_rows_for_synthetic_table().await;
    let merged = if dedup_batches.len() == 1 {
        dedup_batches[0].clone()
    } else {
        concat_batches(&dedup_batches[0].schema(), &dedup_batches).expect("concat commit-dedup")
    };
    let actual = full_action_payload_rows(&merged);
    let expected = vec![
        (
            "add".to_string(),
            serde_json::json!({
                "path":"x.parquet",
                "partitionValues":{"p":"1"},
                "size":11,
                "modificationTime":22,
                "dataChange":true,
                "stats":"{\"numRecords\":1}",
                "tags":{"t1":"v1"},
                "deletionVector":{"storageType":"u","pathOrInlineDv":"ab","offset":3,"sizeInBytes":4,"cardinality":5},
                "baseRowId":6,
                "defaultRowCommitVersion":7,
                "clusteringProvider":"cp"
            }),
        ),
        (
            "domainMetadata".to_string(),
            serde_json::json!({"domain":"d1","configuration":"c1","removed":false}),
        ),
        (
            "metaData".to_string(),
            serde_json::json!({
                "id":"mid-1",
                "format":{"provider":"parquet","options":{}},
                "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns":[],
                "configuration":{}
            }),
        ),
        (
            "protocol".to_string(),
            serde_json::json!({"minReaderVersion":1,"minWriterVersion":2}),
        ),
        (
            "remove".to_string(),
            serde_json::json!({
                "path":"y.parquet",
                "deletionTimestamp":33,
                "dataChange":false,
                "extendedFileMetadata":true,
                "partitionValues":{"p":"2"},
                "size":44,
                "stats":"{\"numRecords\":1}",
                "tags":{"rt":"rv"},
                "deletionVector":{"storageType":"u","pathOrInlineDv":"cd","offset":8,"sizeInBytes":9,"cardinality":10},
                "baseRowId":11,
                "defaultRowCommitVersion":12
            }),
        ),
        (
            "txn".to_string(),
            serde_json::json!({"appId":"app-1","version":7,"lastUpdated":123}),
        ),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_commit_dedup_real_table_with_dv_small_full_to_json_payloads() {
    let merged = commit_dedup_merged_for_real_fixture("table-with-dv-small").await;
    let actual = full_action_payload_rows(&merged);

    let expected: Vec<(String, JsonValue)> = vec![
        (
            "add".to_string(),
            serde_json::json!({
                "path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet",
                "partitionValues":{},
                "size":635,
                "modificationTime":1677811178336_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}",
                "tags":{
                    "INSERTION_TIME":"1677811178336000",
                    "MIN_INSERTION_TIME":"1677811178336000",
                    "MAX_INSERTION_TIME":"1677811178336000",
                    "OPTIMIZE_TARGET_SIZE":"268435456"
                },
                "deletionVector":{
                    "storageType":"u",
                    "pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA",
                    "offset":1,
                    "sizeInBytes":36,
                    "cardinality":2
                }
            }),
        ),
        (
            "metaData".to_string(),
            serde_json::json!({
                "id":"testId",
                "format":{"provider":"parquet","options":{}},
                "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns":[],
                "configuration":{
                    "delta.enableDeletionVectors":"true",
                    "delta.columnMapping.mode":"none"
                },
                "createdTime":1677811175819_i64
            }),
        ),
        (
            "protocol".to_string(),
            serde_json::json!({
                "minReaderVersion":3,
                "minWriterVersion":7,
                "readerFeatures":["deletionVectors"],
                "writerFeatures":["deletionVectors"]
            }),
        ),
        (
            "remove".to_string(),
            serde_json::json!({
                "path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet",
                "deletionTimestamp":1677811194426_i64,
                "dataChange":true,
                "extendedFileMetadata":true,
                "partitionValues":{},
                "size":635,
                "tags":{
                    "INSERTION_TIME":"1677811178336000",
                    "MIN_INSERTION_TIME":"1677811178336000",
                    "MAX_INSERTION_TIME":"1677811178336000",
                    "OPTIMIZE_TARGET_SIZE":"268435456"
                }
            }),
        ),
    ];

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_commit_dedup_real_app_txn_no_checkpoint_full_to_json_payloads() {
    let merged = commit_dedup_merged_for_real_fixture("app-txn-no-checkpoint").await;
    let actual = full_action_payload_rows(&merged);

    let expected: Vec<(String, JsonValue)> = vec![
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-01/part-00001-80996595-a345-43b7-b213-e247d6f091f7-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-01"},
                "size":810,
                "modificationTime":1713400714557_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":8,\"minValues\":{\"value\":4,\"id\":\"A\"},\"maxValues\":{\"id\":\"B\",\"value\":11},\"nullCount\":{\"id\":0,\"value\":0}}"
            }),
        ),
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-01/part-00001-8ebcaf8b-0f48-4213-98c9-5c2156d20a7e-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-01"},
                "size":810,
                "modificationTime":1713400714564_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":8,\"minValues\":{\"value\":4,\"id\":\"A\"},\"maxValues\":{\"value\":11,\"id\":\"B\"},\"nullCount\":{\"id\":0,\"value\":0}}"
            }),
        ),
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-02/part-00001-9a16b9f6-c12a-4609-a9c4-828eacb9526a-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-02"},
                "size":789,
                "modificationTime":1713400714564_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":3,\"minValues\":{\"value\":1,\"id\":\"A\"},\"maxValues\":{\"id\":\"B\",\"value\":3},\"nullCount\":{\"id\":0,\"value\":0}}"
            }),
        ),
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-02/part-00001-bfac5c74-426e-410f-ab74-21a64e518e9c-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-02"},
                "size":789,
                "modificationTime":1713400714557_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":3,\"minValues\":{\"id\":\"A\",\"value\":1},\"maxValues\":{\"value\":3,\"id\":\"B\"},\"nullCount\":{\"value\":0,\"id\":0}}"
            }),
        ),
        (
            "metaData".to_string(),
            serde_json::json!({
                "id":"dc67687f-4462-4bc9-9070-b53a58e4780e",
                "format":{"provider":"parquet","options":{}},
                "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"modified\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns":["modified"],
                "configuration":{},
                "createdTime":1713400714555_i64
            }),
        ),
        (
            "protocol".to_string(),
            serde_json::json!({"minReaderVersion":1,"minWriterVersion":2}),
        ),
        (
            "txn".to_string(),
            serde_json::json!({"appId":"my-app","version":3}),
        ),
        (
            "txn".to_string(),
            serde_json::json!({"appId":"my-app2","version":2}),
        ),
    ];

    assert_eq!(actual, expected);
}
