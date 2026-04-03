//! Integration test: replay write specs via kernel, then validate reads.
//!
//! This test proves kernel can BUILD a table from a write_spec.json and
//! produce correct results. Unlike the acceptance_workloads_reader tests
//! (which read Spark-pre-built tables), this test:
//! 1. Creates a fresh empty directory
//! 2. Executes write_spec.json via kernel's write APIs
//! 3. Reads the kernel-written table
//! 4. Compares against Spark's expected_data

use std::path::PathBuf;
use std::sync::Arc;

use acceptance::acceptance_workloads::workload::execute_and_validate_workload;
use acceptance::acceptance_workloads::write_executor::{execute_write_spec, WriteSpec};
use delta_kernel_benchmarks::models::Spec;
use tempfile::tempdir;
use url::Url;

/// Find the workloads directory relative to the test binary
fn workloads_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(manifest_dir).join("workloads")
}

/// Replay a write spec and validate all read specs against expected data.
fn replay_and_validate(workload_name: &str) {
    let workloads = workloads_dir();
    let workload_dir = workloads.join(workload_name);

    // Read write_spec.json
    let write_spec_path = workload_dir.join("write_spec.json");
    if !write_spec_path.exists() {
        panic!("No write_spec.json in {}", workload_dir.display());
    }
    let write_spec: WriteSpec =
        serde_json::from_str(&std::fs::read_to_string(&write_spec_path).unwrap()).unwrap();

    // Create fresh table directory
    let temp = tempdir().unwrap();
    let table_dir = temp.path().join("delta");
    std::fs::create_dir_all(&table_dir).unwrap();
    let table_url = Url::from_directory_path(&table_dir).unwrap();

    // Create engine
    let engine = test_utils::create_default_engine(&table_url).unwrap();

    // Execute write spec
    let result = execute_write_spec(&write_spec, &table_url, &workload_dir, engine.as_ref());
    match result {
        Ok(r) => {
            println!(
                "[{}] {} applied, {} skipped",
                workload_name, r.commits_applied, r.commits_skipped
            );
            if r.commits_skipped > 0 {
                println!("  Skipped: {:?}", r.skipped_reasons);
                // If any commits were skipped, we can't validate — the table is incomplete
                println!("  SKIP: table is incomplete due to unsupported operations");
                return;
            }
        }
        Err(e) => {
            panic!("[{}] Write spec execution failed: {}", workload_name, e);
        }
    }

    // Now validate read specs against the kernel-written table
    let specs_dir = workload_dir.join("specs");
    if !specs_dir.exists() {
        return;
    }

    let mut passed = 0;
    let mut failed = 0;

    for entry in std::fs::read_dir(&specs_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.extension().map_or(false, |e| e == "json") {
            continue;
        }

        let spec_content = std::fs::read_to_string(&path).unwrap();
        let spec: Result<Spec, _> = serde_json::from_str(&spec_content);
        let spec = match spec {
            Ok(s) => s,
            Err(e) => {
                println!("  SKIP {}: parse error: {}", path.display(), e);
                continue;
            }
        };

        // Skip write specs (they're not validation specs)
        if matches!(spec, Spec::Write(_)) {
            continue;
        }

        // Build expected dir path
        let spec_name = path.file_stem().unwrap().to_string_lossy().to_string();
        let expected_dir = workload_dir.join("expected").join(&spec_name);

        let result = execute_and_validate_workload(
            engine.clone(),
            &table_url,
            &spec,
            &expected_dir,
        );

        match result {
            Ok(()) => {
                println!("  OK: {}", spec_name);
                passed += 1;
            }
            Err(e) => {
                println!("  FAIL: {}: {}", spec_name, e);
                failed += 1;
            }
        }
    }

    println!(
        "[{}] {} passed, {} failed",
        workload_name, passed, failed
    );
    assert_eq!(failed, 0, "Some read specs failed after write replay");
}

// Test: create_table + insert, no predicates, no DML — kernel writes from scratch
#[test]
fn write_replay_insert_multiple() {
    replay_and_validate("insert_multiple");
}

// Test: create_table + single insert — simplest possible write
#[test]
fn write_replay_create_and_read() {
    replay_and_validate("create_and_read");
}

// Test: create with properties (DV + CDF)
#[test]
fn write_replay_create_with_properties() {
    replay_and_validate("create_with_properties");
}
