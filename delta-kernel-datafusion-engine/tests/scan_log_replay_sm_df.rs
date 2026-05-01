//! [`scan_log_replay_sm`] driven end-to-end through [`DataFusionExecutor`] (Phase 4.x DF scaffold).

use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, AsArray};
use delta_kernel::arrow::datatypes::{DataType as ArrowPhysicalType, Schema as ArrowSchema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::state_machines::df::scan_log_replay_sm;
use delta_kernel::plans::state_machines::framework::state_machine::{AdvanceResult, StateMachine};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::Snapshot;
use delta_kernel_datafusion_engine::{DataFusionExecutor, DriveOpts};
use url::Url;

#[tokio::test]
async fn scan_log_replay_sm_df_no_checkpoint_scan_rows_match_fixture_paths() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let table_root = manifest_dir.join("../kernel/tests/data/app-txn-no-checkpoint");
    let table_root = std::fs::canonicalize(table_root).expect("canonicalize kernel fixture path");

    let url = Url::from_directory_path(&table_root).expect("table URL");
    let engine = Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(url.as_str())
        .build(engine.as_ref())
        .expect("snapshot");

    let mut sm = scan_log_replay_sm(snapshot).expect("build SM");
    let exec = DataFusionExecutor::try_new_with_engine(engine).expect("executor");

    let mut last_results_batches = Vec::<delta_kernel::arrow::array::RecordBatch>::new();

    loop {
        let op = sm.get_operation().expect("phase op");
        let (accum, batches_opt) = exec
            .execute_phase_operation_with_results_batches(op, &DriveOpts::default())
            .await
            .expect("phase drain");

        if let Some(bs) = batches_opt {
            last_results_batches = bs;
        }

        match sm.advance(Ok(accum)).expect("advance") {
            AdvanceResult::Continue => {}
            AdvanceResult::Done(()) => break,
        }
    }

    assert!(
        !last_results_batches.is_empty(),
        "expected at least one batch from Results sink"
    );
    let batch = last_results_batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("non-empty scan rows");

    let scan_schema = scan_row_schema();
    assert_eq!(
        batch.num_columns(),
        scan_schema.fields().len(),
        "scan-row projection column count must match scan_row_schema"
    );

    let expected_arrow: Arc<ArrowSchema> = Arc::new(
        scan_schema
            .as_ref()
            .try_into_arrow()
            .expect("kernel→arrow schema"),
    );
    assert_eq!(
        batch.schema().fields().len(),
        expected_arrow.fields().len(),
        "Arrow projection schema width mismatch"
    );

    let path_idx = batch
        .schema()
        .index_of("path")
        .expect("'path' column from scan_row_schema");
    let paths = batch.column(path_idx).as_string::<i32>();
    let mut collected: Vec<String> = Vec::new();
    for i in 0..paths.len() {
        collected.push(paths.value(i).to_string());
    }
    assert!(
        collected.iter().any(|p| p.contains("modified=2021-02-01")),
        "expected fixture add path partition 2021-02-01 in {:?}",
        collected
    );
    assert!(
        collected.iter().any(|p| p.contains("modified=2021-02-02")),
        "expected fixture add path partition 2021-02-02 in {:?}",
        collected
    );

    let fcv_idx = batch
        .schema()
        .index_of("fileConstantValues")
        .expect("nested fileConstantValues column");
    assert!(
        matches!(
            batch.column(fcv_idx).data_type(),
            ArrowPhysicalType::Struct(_)
        ),
        "fileConstantValues must be a struct column in the DataFusion batch"
    );
}
