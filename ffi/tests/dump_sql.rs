//! Dev helper: lower the (data-stage, peeled-to-scan_file_row) plan to DuckDB SQL and print it.
//! `DUMP_TABLE=/abs/path cargo test -p delta_kernel_ffi --features duckdb --test dump_sql -- --nocapture`
#![cfg(feature = "duckdb")]

use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::nodes::NodeKind;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use delta_kernel_ffi::duckdb::plan_to_sql::result_plan_to_sql_until;
use url::Url;

#[test]
fn dump_scan_sql() {
    let table = std::env::var("DUMP_TABLE")
        .unwrap_or_else(|_| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../kernel/tests/data/table-without-dv-small")
                .display()
                .to_string()
        });
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let url = Url::from_directory_path(std::path::Path::new(&table).canonicalize().unwrap()).unwrap();
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build().unwrap();
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let rp = rt
        .block_on(executor.drive_to_completion(scan.scan_state_machine().unwrap()))
        .unwrap();

    // Peel: terminal Project <- data Load <- scan_file_row.
    let mut plan = rp.plan;
    let term = &plan.nodes[rp.result.0 as usize];
    println!("terminal node kind: {}", term.kind);
    let load_ref = term.inputs[0];
    let load = &plan.nodes[load_ref.0 as usize];
    println!("load node kind: {}", load.kind);
    let sfr = load.inputs[0];
    println!("scan_file_row ref: {sfr:?}");
    assert!(matches!(term.kind, NodeKind::Project(_)));
    assert!(matches!(load.kind, NodeKind::Load(_)));

    let result_ref = rp.result;
    delta_kernel_ffi::duckdb::materialize_runtime_loads(&mut plan, &executor, &rt, sfr).unwrap();

    // Full plan (no peel): the data Load lowers to delta_load(<scan_file_row cte>, ...).
    match result_plan_to_sql_until(&plan, result_ref) {
        Ok(sql) => println!("=== BEGIN SQL ===\n{sql}\n=== END SQL ==="),
        Err(e) => println!("=== SQL LOWERING ERROR ===\n{e}"),
    }
}
