//! Smoke tests for [`DataFusionExecutor::scan_metadata`] / [`DataFusionExecutor::scan_data`]
//! routed through the scan state machines.
//!
//! Cross-checks the scan path's `scan_metadata` row count against the kernel default-engine
//! reference (`Scan::scan_metadata` add-path set, the same source of truth used by the FSR
//! golden tests). `scan_data` is asserted to drive without error and produce some rows; the
//! per-row data correctness is covered by kernel parity in the metadata test above.

mod common;

use std::sync::Arc;

use common::open_snapshot_for_fixture;
use delta_kernel::scan::Scan;
use delta_kernel::Engine as KernelEngine;
use delta_kernel_datafusion_engine::DataFusionExecutor;
use rstest::rstest;

fn open_scan(table: &str) -> (Arc<dyn KernelEngine>, Scan) {
    let (engine, snapshot) = open_snapshot_for_fixture(table);
    let scan = snapshot.scan_builder().build().expect("scan builder build");
    (engine, scan)
}

/// Reference live-file count via the kernel default-engine scan_metadata path.
fn kernel_reference_live_file_count(scan: &Scan, engine: &dyn KernelEngine) -> usize {
    let mut total = 0usize;
    for batch in scan.scan_metadata(engine).expect("scan_metadata") {
        let metadata = batch.expect("scan_metadata batch");
        for selected in metadata.scan_files.selection_vector() {
            if *selected {
                total += 1;
            }
        }
    }
    total
}

/// `scan_metadata` row count matches the kernel default-engine reference live-file count
/// across the FSR golden fixtures. Mirrors the fixture set used by `fsr_real.rs` so any
/// disagreement points at the scan-specific terminal (action_pair -> flat scan_file_row),
/// not the shared reconciliation.
#[rstest]
#[case::commit_only("app-txn-no-checkpoint")]
#[case::v1_checkpoint("app-txn-checkpoint")]
#[case::dv_small("table-with-dv-small")]
#[case::no_dv_small("table-without-dv-small")]
#[case::v2_classic_parquet("v2-classic-parquet-struct-stats-only")]
#[case::v2_json_sidecars("v2-json-sidecars-struct-stats-only")]
#[case::v2_parquet_sidecars("v2-parquet-sidecars-struct-stats-only")]
#[tokio::test]
async fn scan_metadata_row_count_matches_kernel_reference(#[case] fixture: &str) {
    let (engine, scan) = open_scan(fixture);
    let expected = kernel_reference_live_file_count(&scan, engine.as_ref());

    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    let df = executor.scan_metadata(&scan).await.expect("scan_metadata");
    let batches = df.collect().await.expect("collect scan_metadata");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, expected,
        "scan_metadata row count vs kernel reference for {fixture}"
    );
}

/// `scan_data` drives end-to-end without error and produces at least one row. Detailed
/// per-row data correctness is covered by kernel parity in the metadata test above; this case
/// asserts the full data-stage pipeline (Load + logical projection on top of reconciliation)
/// wires up.
///
/// Fixtures with deletion vectors are intentionally excluded: `LoadExec::new` rejects DV
/// plans up front because the `_row_number` virtual column path needs DataFusion 54.
/// Those fixtures rejoin `scan_data` coverage in the DF54 follow-up.
#[rstest]
#[case::commit_only("app-txn-no-checkpoint")]
#[case::v1_checkpoint("app-txn-checkpoint")]
#[case::no_dv_small("table-without-dv-small")]
#[tokio::test]
async fn scan_data_drives_without_error(#[case] fixture: &str) {
    let (engine, scan) = open_scan(fixture);
    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    let df = executor.scan_data(&scan).await.expect("scan_data");
    let batches = df.collect().await.expect("collect scan_data");
    let row_total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(row_total > 0, "scan_data produced zero rows for {fixture}");
}

/// Counterpart to [`scan_data_drives_without_error`]: DV fixtures should produce a typed
/// `plan_compilation` error from `LoadExec::new`'s DF54 guard rather than silently
/// reading the wrong data. Pin the rejection contract so the DF54 follow-up can flip
/// these cases over without losing coverage.
#[rstest]
#[case::dv_small("table-with-dv-small")]
#[case::short_dv("with-short-dv")]
#[tokio::test]
async fn scan_data_rejects_dv_fixtures_until_df54(#[case] fixture: &str) {
    let (engine, scan) = open_scan(fixture);
    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    // The rejection may surface at `scan_data` (during plan compile) or at `collect`
    // (during physical planning), depending on which DataFusion pass first tries to
    // materialize the Load node. Either is acceptable; assert by message.
    let msg = match executor.scan_data(&scan).await {
        Err(e) => e.to_string(),
        Ok(df) => df
            .collect()
            .await
            .expect_err("DV scan_data must fail on DF53")
            .to_string(),
    };
    assert!(
        msg.contains("deletion vectors") || msg.contains("DataFusion 54"),
        "expected DF54 / DV rejection, got: {msg}"
    );
}
