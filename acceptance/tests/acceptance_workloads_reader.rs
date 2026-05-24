//! Test harness for acceptance workloads.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! acceptance_workloads directory. Each spec file becomes its own test.
//!
//! Shared `SKIP_LIST` and `EXPECTED_KERNEL_FAILURES` live in `tests/common/mod.rs`
//! so the DataFusion-side harness (`acceptance_workloads_datafusion.rs`) and this
//! reader-side harness consume one canonical list rather than duplicating it.

mod common;

use std::path::Path;

use acceptance::acceptance_workloads::workload::execute_and_validate_workload;
use acceptance::acceptance_workloads::TestCase;

use crate::common::{should_skip_test, EXPECTED_KERNEL_FAILURES};

fn acceptance_workloads_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();
    // Normalize Windows backslashes to forward slashes for pattern matching
    #[cfg(windows)]
    let spec_path_str = spec_path_str.replace('\\', "/");

    // Check expected kernel failures FIRST (path matching only - these need to
    // actually run to assert kernel still fails). Skip list checked second.
    let expected_failure = EXPECTED_KERNEL_FAILURES
        .iter()
        .find(|(_, patterns)| patterns.iter().any(|p| spec_path_str.contains(p)));

    if expected_failure.is_none() && should_skip_test(&spec_path_str).is_some() {
        return Ok(());
    }

    // Load and execute test case
    let test_case = TestCase::from_spec_path(&spec_path_abs);
    let table_root = test_case.table_root().expect("Failed to get table URL");
    let engine = test_utils::create_default_engine(&table_root).expect("Failed to create engine");
    let result = execute_and_validate_workload(
        engine,
        &table_root,
        &test_case.spec,
        &test_case.expected_dir(),
    );

    match (result, expected_failure) {
        (Err(_), Some(_)) => {} // Expected to fail, did fail
        (Ok(_), None) => {}     // Expected to pass, did pass
        (Ok(_), Some((reason, _))) => panic!(
            "Workload '{}' was expected to fail but succeeded! \
             Reason: {reason}. Remove from EXPECTED_KERNEL_FAILURES!",
            test_case.workload_name
        ),
        (Err(e), None) => panic!("Workload '{}' failed: {}", test_case.workload_name, e),
    }
    Ok(())
}

datatest_stable::harness! {
    {
        test = acceptance_workloads_test,
        root = "workloads/",
        pattern = r"specs/.*\.json$"
    },
}
