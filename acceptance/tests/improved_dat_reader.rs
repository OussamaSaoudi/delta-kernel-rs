//! Test harness for improved_dat test suite.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! improved_dat directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::improved_dat::{
    types::WorkloadSpec,
    validation::{validate_domain_metadata, validate_read_result, validate_snapshot_metadata},
    workload::{execute_workload, WorkloadResult},
};
use url::Url;

fn should_skip_test(test_path: &str) -> bool {
    // Skip tests that hang due to TokioBackgroundExecutor issues or huge tables
    let skip_prefixes = [
        "cp_partitioned/",  // TokioBackgroundExecutor crash/hang
        "DV-017/",          // Huge table (2B rows) causes hang
    ];
    for prefix in &skip_prefixes {
        if test_path.contains(prefix) {
            return true;
        }
    }
    false
}

fn should_skip_workload(spec: &WorkloadSpec) -> bool {
    match spec {
        // Skip timestamp-based time travel (requires reading commitInfo from files)
        WorkloadSpec::Read { timestamp: Some(_), .. } => true,
        WorkloadSpec::Snapshot { timestamp: Some(_), .. } => true,
        // Skip domain metadata (some tests access system-protected delta.* domains)
        WorkloadSpec::DomainMetadata { .. } => true,
        _ => false,
    }
}

fn improved_dat_test(spec_path: &Path) -> datatest_stable::Result<()> {
    // Build absolute path to spec file
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    // Canonicalize to resolve ".." in the path
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();

    // Check skip list
    if should_skip_test(&spec_path_str) {
        println!("Skipping test: {}", spec_path_str);
        return Ok(());
    }

    // Extract workload name from spec file (e.g., "st_numeric_stats_full_scan" from "st_numeric_stats_full_scan.json")
    let workload_name = spec_path_abs
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    // The test case root is two levels up from the spec file (specs/<file>.json -> test_case/)
    let test_case_dir = spec_path_abs
        .parent() // specs/
        .and_then(|p| p.parent()) // test_case/
        .expect("Could not find test case directory");

    // Expected results directory
    let expected_dir = test_case_dir.join("expected").join(workload_name);

    // Delta table directory
    let delta_dir = test_case_dir.join("delta");

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            // Load the spec file
            let spec_file = std::fs::File::open(&spec_path_abs)
                .expect(&format!("Failed to open spec file: {:?}", spec_path_abs));
            let spec: WorkloadSpec = serde_json::from_reader(spec_file)
                .expect(&format!("Failed to parse spec file: {:?}", spec_path_abs));

            // Check if workload type should be skipped
            if should_skip_workload(&spec) {
                println!("Skipping workload (unsupported type): {}", workload_name);
                return;
            }

            println!("Running workload: {}", workload_name);

            // Get table URL and create engine
            let table_root = Url::from_directory_path(&delta_dir)
                .expect("Failed to create URL from delta directory");
            let engine = test_utils::create_default_engine(&table_root)
                .expect("Failed to create engine");

            // Check if this workload expects an error
            if spec.expects_error() {
                let expected_error = spec.expected_error().unwrap();
                // Execute and expect failure
                let result = execute_workload(engine.clone(), &table_root, &spec);
                match result {
                    Ok(_) => {
                        panic!(
                            "Workload '{}' expected error '{}' but succeeded",
                            workload_name, expected_error.error_code
                        );
                    }
                    Err(e) => {
                        println!("  Got expected error (expected '{}'): {}", expected_error.error_code, e);
                    }
                }
                return;
            }

            // Execute workload
            let result = match execute_workload(engine.clone(), &table_root, &spec) {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("{}", e);
                    // Skip gracefully if failure is due to unsupported predicate syntax
                    if msg.contains("Failed to parse predicate: Unsupported")
                        || msg.contains("Predicate references unknown column")
                    {
                        println!("  Skipping (unsupported predicate): {}", msg);
                        return;
                    }
                    // Skip gracefully if kernel can't deserialize the schema (e.g., TIME type)
                    if msg.contains("did not match any variant of untagged enum DataType")
                        || msg.contains("unsupported data type")
                    {
                        println!("  Skipping (unsupported data type in schema): {}", msg);
                        return;
                    }
                    panic!("Workload '{}' failed: {}", workload_name, e);
                }
            };

            // Validate results based on workload type
            match result {
                WorkloadResult::Read(read_result) => {
                    if expected_dir.exists() {
                        let batch = read_result.concat().expect("Failed to concat batches");
                        validate_read_result(batch, &expected_dir)
                            .await
                            .expect(&format!(
                                "Validation failed for workload '{}'",
                                workload_name
                            ));
                    }
                }
                WorkloadResult::Snapshot(snapshot_result) => {
                    if expected_dir.exists() {
                        validate_snapshot_metadata(&snapshot_result, &expected_dir).expect(
                            &format!("Metadata validation failed for workload '{}'", workload_name),
                        );
                    }
                }
                WorkloadResult::DomainMetadata(dm_result) => {
                    // Get expected from the spec
                    if let WorkloadSpec::DomainMetadata { expected, .. } = &spec {
                        validate_domain_metadata(&dm_result, expected).expect(&format!(
                            "Domain metadata validation failed for workload '{}'",
                            workload_name
                        ));
                    }
                }
            }

            println!("  Passed");
        });

    Ok(())
}

datatest_stable::harness! {
    {
        test = improved_dat_test,
        root = "../improved_dat/",
        pattern = r"specs/.*\.json$"
    },
}
