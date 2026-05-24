//! DataFusion-specific acceptance workload harness.
//!
//! Mirrors `acceptance_workloads_reader`'s structure so the two suites are
//! directly comparable. The expected-failure set for DataFusion is defined as
//! a *transformation* on the kernel default engine's set:
//!
//! ```text
//! EXPECTED_DATAFUSION_FAILURES =
//!     (EXPECTED_KERNEL_FAILURES ∪ ADDITIONAL_DF_FAILURES) ∖ FIXED_IN_DATAFUSION
//! ```
//!
//! `ADDITIONAL_DF_FAILURES` lists divergences DataFusion introduces (workloads the
//! kernel default engine passes but DataFusion fails). `FIXED_IN_DATAFUSION` lists
//! improvements DataFusion makes (kernel-failure workloads DataFusion handles
//! correctly). Both are explicit so divergences from the reference are always
//! visible at a glance.
//!
//! In addition to the spec-path lists, the harness also classifies the snapshot at
//! runtime via [`common::datafusion_executor_unsupported_feature`] and treats CM /
//! DV tables as expected DataFusion failures regardless of spec-path entries --
//! the DataFusion executor rejects those plans at `LoadExec::new` until DataFusion
//! 54 ships the `_row_number` virtual column and field-id-aware adapters.

mod common;

use std::path::Path;
use std::sync::Arc;

use acceptance::acceptance_workloads::validation::{validate_read_result, validate_snapshot};
use acceptance::acceptance_workloads::workload::{build_snapshot, SnapshotResult};
use acceptance::acceptance_workloads::TestCase;
use delta_kernel::{DeltaResult, Engine, Error};
use delta_kernel_benchmarks::models::{SnapshotConstructionSpec, Spec};
use delta_kernel_benchmarks::workload::{build_scan_for_spec, execute_read_via_datafusion};
use delta_kernel_datafusion_engine::DataFusionExecutor;

use crate::common::{
    datafusion_executor_unsupported_feature, should_skip_test, EXPECTED_KERNEL_FAILURES,
};

/// Workloads where DataFusion fails but the kernel default engine passes. Each entry is
/// `(reason, &[spec_path_infix, ...])` mirroring [`EXPECTED_KERNEL_FAILURES`]. CM and DV
/// tables are detected at runtime by [`datafusion_executor_unsupported_feature`] and
/// don't need explicit entries here.
const ADDITIONAL_DF_FAILURES: &[(&str, &[&str])] = &[];

/// Workloads where DataFusion succeeds but the kernel default engine fails. Each entry
/// is a spec-path infix. The workload runs through DataFusion and is expected to pass;
/// the listing exists only to override the corresponding reader-side
/// [`EXPECTED_KERNEL_FAILURES`] entry so the harness's "expected-fail-but-succeeded"
/// check doesn't false-positive on improvements the DataFusion path makes over the
/// default engine.
const FIXED_IN_DATAFUSION: &[&str] = &[
    // Reading corrupt/invalid commit or checkpoint: default engine accepts these
    // (no validation), DataFusion's FSR replay rejects them at the strict
    // arrow-json read boundary.
    "corrupt_truncated_commit_json/specs/corrupt_truncated_commit_json_error",
    "cp_err_missing_protocol/specs/cp_err_missing_protocol_error",
    "ct_invalid_json/specs/ct_invalid_json_error",
    "dsReadCorruptJson/specs/dsReadCorruptJson_error",
    "dsReadCorruptCheckpoint/specs/dsReadCorruptCheckpoint_error",
    "dsReadModifyCheckpoint/specs/dsReadModifyCheckpoint_error",
    // Schema-integrity validation: same as above; DataFusion rejects an invalid
    // / empty schemaString at the strict JSON-read boundary.
    "err_schema_invalid_json/specs/err_schema_invalid_json_error",
    "err_schema_empty/specs/err_schema_empty_error",
    // Spark's protocol-downgrade encoding writes a partial `metaData` action
    // with no `schemaString` in commit 1; the default engine short-circuits
    // protocol/metadata via the CRC at end_version, but DataFusion's full_state
    // SM replays every commit and rejects the partial action. The reader test
    // is in EXPECTED_KERNEL_FAILURES because spec is `error`-typed and the
    // default engine doesn't error there; DataFusion erroring matches `error`.
    "pv_protocol_downgrade/specs/pv_protocol_downgrade_snapshot",
    // Type widening: DataFusion's parquet field-id-aware adapter handles list-
    // element / map-key widening and nested-field metadata correctly, where the
    // kernel default engine's apply_schema currently bails.
    "tw_array_element/specs/tw_array_element_read_all",
    "tw_map_key_value_widening/specs/tw_map_key_value_widening_read_all",
    "tw_nested_field/specs/tw_nested_field_read_all",
    "tw_nested_field/specs/tw_nested_field_read_large_count",
    // CDC schema evolution + column-mapping id-swap: the kernel default engine's
    // apply_schema currently rejects these but the DataFusion field-id-aware
    // PhysicalExprAdapter handles them end-to-end (id-keyed root rebinding +
    // nested struct cast).
    "cdc_schema_evolution/specs/cdc_schema_evolution_read_all",
    "cdf_with_schema_evolution/specs/cdf_with_schema_evolution_read_all",
    "cm_id_matching_swapped/specs/cm_id_matching_swapped_select_a_reads_e",
];

/// Locate the expected-failure entry for `spec_path_str`, applying the DataFusion delta
/// on top of [`EXPECTED_KERNEL_FAILURES`].
fn datafusion_expected_failure(
    spec_path_str: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    if FIXED_IN_DATAFUSION
        .iter()
        .any(|p| spec_path_str.contains(p))
    {
        return None;
    }
    EXPECTED_KERNEL_FAILURES
        .iter()
        .chain(ADDITIONAL_DF_FAILURES.iter())
        .copied()
        .find(|(_, patterns)| patterns.iter().any(|p| spec_path_str.contains(p)))
}

async fn execute_snapshot_workload_datafusion(
    engine: Arc<dyn Engine>,
    table_root: &url::Url,
    snapshot_spec: &SnapshotConstructionSpec,
) -> DeltaResult<SnapshotResult> {
    let snapshot = build_snapshot(
        engine.as_ref(),
        table_root,
        snapshot_spec.time_travel.as_ref(),
    )?;
    if let Some(reason) = datafusion_executor_unsupported_feature(&snapshot) {
        return Err(Error::generic(format!(
            "datafusion executor skip: {reason}"
        )));
    }
    let executor = DataFusionExecutor::try_new_with_engine(engine)
        .map_err(|e| Error::generic(format!("create DataFusionExecutor: {e}")))?;
    let fsr = snapshot
        .full_state_builder()
        .build()
        .map_err(|e| Error::generic(format!("build full_state plan: {e}")))?;
    executor
        .full_state(&fsr)
        .await
        .map_err(|e| Error::generic(format!("drive full_state via DataFusionExecutor: {e}")))?
        .collect()
        .await
        .map_err(|e| Error::generic(format!("collect full_state DataFrame: {e}")))?;
    let table_configuration = snapshot.table_configuration();
    Ok(SnapshotResult {
        version: snapshot.version(),
        protocol: table_configuration.protocol().clone(),
        metadata: table_configuration.metadata().clone(),
    })
}

/// Run a workload through DataFusion and return the validation outcome.
fn run_datafusion_workload(test_case: &TestCase) -> Result<(), String> {
    let table_root = test_case
        .table_root()
        .map_err(|e| format!("Failed to get table URL: {e}"))?;
    let engine: Arc<dyn Engine> = test_utils::create_default_engine(&table_root)
        .map_err(|e| format!("Failed to create engine: {e}"))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to build runtime: {e}"))?;

    match &test_case.spec {
        Spec::Read(read_spec) => {
            let Some(expected) = read_spec.expected.as_ref() else {
                return Ok(());
            };
            let result = (|| -> DeltaResult<_> {
                let snapshot =
                    build_snapshot(engine.as_ref(), &table_root, read_spec.time_travel.as_ref())?;
                if let Some(reason) = datafusion_executor_unsupported_feature(&snapshot) {
                    return Err(Error::generic(format!(
                        "datafusion executor skip: {reason}"
                    )));
                }
                let (scan, predicate) = build_scan_for_spec(snapshot, read_spec)?;
                let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
                    .map_err(|e| Error::generic(format!("create DataFusionExecutor: {e}")))?;
                runtime.block_on(execute_read_via_datafusion(
                    &executor,
                    &scan,
                    predicate.as_deref(),
                ))
            })();
            validate_read_result(result, &test_case.expected_dir(), expected)
                .map_err(|e| e.to_string())
        }
        Spec::SnapshotConstruction(snapshot_spec) => {
            let Some(expected) = snapshot_spec.expected.as_ref() else {
                return Ok(());
            };
            let result = runtime.block_on(execute_snapshot_workload_datafusion(
                Arc::clone(&engine),
                &table_root,
                snapshot_spec.as_ref(),
            ));
            validate_snapshot(result, expected).map_err(|e| e.to_string())
        }
    }
}

fn acceptance_workloads_datafusion_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();
    #[cfg(windows)]
    let spec_path_str = spec_path_str.replace('\\', "/");

    // Match reader's ordering: expected-failure check first (those workloads must
    // actually run to assert they still fail), then SKIP_LIST short-circuit.
    let expected_failure = datafusion_expected_failure(&spec_path_str);
    if expected_failure.is_none() && should_skip_test(&spec_path_str).is_some() {
        return Ok(());
    }

    let test_case = TestCase::from_spec_path(&spec_path_abs);
    let result = run_datafusion_workload(&test_case);

    // Treat runtime CM/DV gating identically to an explicit ADDITIONAL_DF_FAILURES
    // entry: the kernel default engine handles these, DataFusion does not yet, and
    // the workload should be counted as expected-fail in either case.
    let is_runtime_skip = matches!(&result, Err(e) if e.contains("datafusion executor skip:"));

    match (result, expected_failure, is_runtime_skip) {
        (Err(_), _, true) => {}    // Runtime CM/DV gate fired -- treat as expected fail
        (Err(_), Some(_), _) => {} // Listed expected fail, did fail
        (Ok(_), None, _) => {}     // Listed expected pass, did pass
        (Ok(_), Some((reason, _)), _) => {
            // Validation succeeded but the workload was in `EXPECTED_KERNEL_FAILURES`.
            // Two legitimate paths to Ok here:
            //   1. success-typed spec: DataFusion fixed a real default-engine bug; the
            //      spec belongs in `FIXED_IN_DATAFUSION` so future runs assert it.
            //   2. error-typed spec: the default engine wrongly accepts the corrupt
            //      input (kernel divergence), DataFusion correctly errors, and
            //      `validate_read_result` returned Ok because the error code matched
            //      the spec's expected error. This is also a "DataFusion is correct"
            //      outcome but adding it to `FIXED_IN_DATAFUSION` only matters when
            //      we want to start asserting DataFusion's behavior.
            // Emit a non-fatal warning rather than panicking so the suite doesn't
            // false-fail on path (2) without first being run end-to-end.
            eprintln!(
                "[datafusion] workload '{}' was in EXPECTED_KERNEL_FAILURES ({reason}) \
                 but DataFusion validated Ok. Consider adding to FIXED_IN_DATAFUSION.",
                test_case.workload_name,
            );
        }
        (Err(e), None, _) => panic!(
            "DataFusion workload '{}' failed: {}",
            test_case.workload_name, e
        ),
    }
    Ok(())
}

datatest_stable::harness! {
    {
        test = acceptance_workloads_datafusion_test,
        root = "workloads/",
        pattern = r"specs/.*\.json$"
    },
}
