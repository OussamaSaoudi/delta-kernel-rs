//! End-to-end driver integration: [`Snapshot::full_state`] yields a real, multi-plan FSR
//! [`CoroutineSM`] (window-on-commits + anti-join-on-checkpoint); the DataFusion executor
//! drives it and the returned action-row batches must agree with classic
//! [`Snapshot::scan`] over the same fixtures.
//!
//! These tests exist mainly to lock in the *Snapshot wiring* — that
//! `Snapshot::full_state` actually runs the canonical FSR algorithm and surfaces a
//! non-empty, action-schema row stream through the executor's `Results`
//! consumer. Rich content / parity assertions live in `fsr_real.rs`.

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, AsArray, RecordBatch};
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::{Engine as KernelEngine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use url::Url;

fn fixture_table(name: &str) -> Url {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data")
        .join(name);
    Url::from_directory_path(root.canonicalize().expect("fixture path")).expect("table url")
}

fn collect_add_paths(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let add_idx = b.schema().index_of("add").expect("add idx");
        let add_col = b.column(add_idx).as_struct_opt().expect("add struct");
        let path_col = add_col.column_by_name("path").expect("add.path");
        for i in 0..path_col.len() {
            if add_col.is_valid(i) && path_col.is_valid(i) {
                out.push(path_col.as_string::<i32>().value(i).to_string());
            }
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn snapshot_full_state_no_checkpoint_emits_action_row_batches() {
    let table_root = fixture_table("table-with-dv-small");
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .expect("snapshot");

    let sm = snapshot.full_state().expect("full_state SM");
    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    let ((), batches) = ex
        .drive_coroutine_sm_collecting_results(sm)
        .await
        .expect("drive FSR SM + capture results");

    assert!(
        !batches.is_empty(),
        "FSR `Results` sink must emit at least one batch (the table is non-empty)"
    );
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let unique_paths = collect_add_paths(&batches);
    assert!(
        total_rows >= unique_paths.len(),
        "full_state returns a mixed action stream; add rows are a subset"
    );
    assert!(
        !unique_paths.is_empty(),
        "non-empty table must materialize at least one live add row"
    );
}

#[tokio::test]
async fn snapshot_full_state_v1_checkpoint_emits_action_row_batches() {
    let table_root = fixture_table("app-txn-checkpoint");
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .expect("snapshot");

    let sm = snapshot.full_state().expect("full_state SM");
    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    let ((), batches) = ex
        .drive_coroutine_sm_collecting_results(sm)
        .await
        .expect("drive FSR SM + capture results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let paths = collect_add_paths(&batches);
    assert!(
        total_rows >= paths.len(),
        "add rows are a subset of action rows"
    );
    assert!(
        !paths.is_empty(),
        "V1-checkpoint table must materialize live add rows from the checkpoint side"
    );
}
