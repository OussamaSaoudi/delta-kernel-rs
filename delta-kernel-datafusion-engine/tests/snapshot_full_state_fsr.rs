//! Phase 4.1 — [`Snapshot::full_state`] builds an FSR-shaped [`CoroutineSM`] tied to log listing;
//! DataFusion executor drives it end-to-end.

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::state_machines::fsr::FsrStripThenFanoutOutcome;
use delta_kernel::{Engine as KernelEngine, Snapshot};
use delta_kernel_datafusion_engine::{DataFusionExecutor, DriveOpts};
use url::Url;

fn fixture_table(name: &str) -> Url {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data")
        .join(name);
    Url::from_directory_path(root.canonicalize().expect("fixture path")).expect("table url")
}

#[tokio::test]
async fn snapshot_full_state_no_checkpoint_two_tail_commits() {
    let table_root = fixture_table("table-with-dv-small");
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .expect("snapshot");

    let sm = snapshot.full_state().expect("full_state SM");
    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    let out = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive FSR SM");

    assert_eq!(
        out,
        FsrStripThenFanoutOutcome {
            strip_rows: 1,
            fanout_rows: 2,
        },
        "no checkpoint → one sentinel strip row; two json commits in segment",
    );
}

#[tokio::test]
async fn snapshot_full_state_checkpoint_with_empty_tail_commits() {
    let table_root = fixture_table("app-txn-checkpoint");
    let engine: Arc<dyn KernelEngine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_root)
        .build(engine.as_ref())
        .expect("snapshot");

    let sm = snapshot.full_state().expect("full_state SM");
    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    let out = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive FSR SM");

    assert_eq!(
        out,
        FsrStripThenFanoutOutcome {
            strip_rows: 1,
            fanout_rows: 1,
        },
        "single-part checkpoint + commits folded into checkpoint ⇒ one strip row; empty tail ⇒ sentinel fanout row",
    );
}
