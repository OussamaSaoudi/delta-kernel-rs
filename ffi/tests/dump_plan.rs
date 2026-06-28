//! Dev helper: print the kernel scan-metadata `ResultPlan` (with full expressions) so the
//! Rust->SQL lowering can be written precisely. Run with:
//! `cargo test -p delta_kernel_ffi --features duckdb --test dump_plan -- --nocapture`.
#![cfg(feature = "duckdb")]

use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use url::Url;

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let p = e.path();
        let t = dst.join(e.file_name());
        if p.is_dir() {
            copy_dir(&p, &t);
        } else {
            std::fs::copy(&p, &t).unwrap();
        }
    }
}

#[test]
fn dump_scan_metadata_plan() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data/table-without-dv-small");
    let tmp = tempfile::tempdir().unwrap();
    let table = tmp.path().join("t");
    copy_dir(&src, &table);

    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let url = Url::from_directory_path(table.canonicalize().unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let rp = rt
        .block_on(executor.drive_to_completion(scan.scan_metadata_state_machine().unwrap()))
        .unwrap();
    println!("=== ResultPlan (result = {:?}) ===", rp.result);
    for (i, node) in rp.plan.nodes.iter().enumerate() {
        println!("\n----- node {i} (output={:?}, inputs={:?}) -----", node.output, node.inputs);
        println!("{:#?}", node.kind);
    }
}
