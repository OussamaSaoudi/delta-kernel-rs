//! Phase 3.3 — classic V2 checkpoint parquet via DF SM driving (`checkpoint_parquet_write` phase).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::{Action, Add, Metadata, Protocol, Remove};
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use delta_kernel_datafusion_engine::DataFusionExecutor;
use tempfile::tempdir;
use url::Url;

fn write_commit_disk(log: &std::path::Path, version: u64, actions: &[Action]) {
    let mut buf = String::new();
    for a in actions {
        serde_json::to_string(a)
            .map(|s| {
                buf.push_str(&s);
                buf.push('\n');
            })
            .expect("action json");
    }
    let name = format!("{:020}.json", version);
    std::fs::write(log.join(name), buf).expect("commit write");
}

fn protocol_v2() -> Action {
    Action::Protocol(
        Protocol::try_new_modern(vec!["v2Checkpoint"], vec!["v2Checkpoint"]).unwrap(),
    )
}

fn metadata_action() -> Action {
    Action::Metadata(
        Metadata::try_new(
            Some("test-table".into()),
            None,
            Arc::new(StructType::try_new([StructField::nullable(
                "value",
                DataType::INTEGER,
            )]).unwrap()),
            vec![],
            0,
            HashMap::new(),
        )
        .unwrap(),
    )
}

fn add_stats(path: &str, num_records: i64) -> Action {
    let stats = format!(
        r#"{{"numRecords":{num_records},"minValues":{{"id":1,"name":"alice"}},"maxValues":{{"id":100,"name":"zoe"}},"nullCount":{{"id":0,"name":5}}}}"#
    );
    Action::Add(Add {
        path: path.into(),
        data_change: true,
        stats: Some(stats),
        ..Default::default()
    })
}

fn remove_action(path: &str) -> Action {
    Action::Remove(Remove {
        path: path.into(),
        data_change: true,
        deletion_timestamp: Some(i64::MAX),
        ..Default::default()
    })
}

#[tokio::test]
async fn checkpoint_v2_classic_parquet_df_sm_happy_path() {
    let dir = tempdir().expect("tempdir");
    let log = dir.path().join("_delta_log");
    std::fs::create_dir_all(&log).expect("mkdir log");

    write_commit_disk(
        &log,
        0,
        &[add_stats("fake_path_2", 50), remove_action("fake_path_1")],
    );
    write_commit_disk(&log, 1, &[metadata_action(), protocol_v2()]);

    let table_root = Url::from_directory_path(dir.path()).expect("table url");
    let engine = Arc::new(
        DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build(),
    );
    let snapshot = Arc::new(
        Snapshot::builder_for(table_root)
            .build(engine.as_ref())
            .expect("snapshot"),
    );
    let writer = snapshot.create_checkpoint_writer().expect("checkpoint writer");

    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    ex.checkpoint_write_classic_parquet_and_finalize(writer)
        .await
        .expect("checkpoint via DF SM");

    let cp_path = log.join("00000000000000000001.checkpoint.parquet");
    assert!(
        cp_path.exists(),
        "expected classic parquet checkpoint at {}",
        cp_path.display()
    );

    let lc_path = log.join("_last_checkpoint");
    assert!(lc_path.exists());
    let lc: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&lc_path).expect("open hint")).expect("parse");
    assert_eq!(lc["version"], 1);
    assert_eq!(lc["numOfAddFiles"], 1);

    let file = std::fs::File::open(&cp_path).expect("open parquet");
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
    let arrow_schema = builder.schema();
    assert!(
        arrow_schema
            .fields()
            .iter()
            .any(|f| f.name() == "checkpointMetadata"),
        "v2 checkpoint should materialize checkpointMetadata column"
    );
}
