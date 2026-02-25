//! Workload-based benchmarks using Criterion.
//!
//! This benchmark dynamically loads workload specifications from JSON files and
//! executes them using Criterion for performance measurement.
//!
//! You can run this benchmark with `cargo bench --bench workload_bench`.
//!
//! By default, it uses local workload specs in the benches/workload_specs directory.
//! To use a different directory, set the WORKLOAD_SPECS_DIR environment variable:
//! ```bash
//! WORKLOAD_SPECS_DIR=/path/to/specs cargo bench --bench workload_bench
//! ```
//!
//! To produce a unified benchmark report (compatible with Java's WorkloadOutputFormat),
//! set the BENCHMARK_REPORT_PATH environment variable:
//! ```bash
//! WORKLOAD_SPECS_DIR=/path/to/specs BENCHMARK_REPORT_PATH=/tmp/report.json \
//!   cargo bench --bench workload_bench
//! ```
//!
//! To compare your changes vs. latest main:
//! ```bash
//! # checkout baseline branch and save as baseline
//! git checkout main
//! cargo bench --bench workload_bench -- --save-baseline main
//!
//! # switch back to your changes, and compare against baseline
//! git checkout your-branch
//! cargo bench --bench workload_bench -- --baseline main
//! ```

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use delta_kernel::benchmarks::report::{BenchmarkTimings, build_report, write_report};
use delta_kernel::benchmarks::{
    create_runner, load_all_workloads, ReadOperationType, WorkloadSpecType, WorkloadSpecVariant,
};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

/// Collected result for a single benchmark after Criterion completes.
struct CollectedResult {
    name: String,
    spec_json: serde_json::Value,
    durations: Vec<Duration>,
}

// Thread-local collector for benchmark timing data.
// Criterion doesn't expose per-iteration timings, so we record them ourselves.
thread_local! {
    static COLLECTOR: RefCell<Vec<CollectedResult>> = RefCell::new(Vec::new());
}

/// Get the path to the workload specifications directory.
///
/// Checks in order:
/// 1. WORKLOAD_SPECS_DIR environment variable
/// 2. Local workload specs in benches/workload_specs (default)
fn get_workload_specs_path() -> PathBuf {
    // Check environment variable first
    if let Ok(custom_path) = std::env::var("WORKLOAD_SPECS_DIR") {
        println!("Using custom workload specs: {}", custom_path);
        return PathBuf::from(custom_path);
    }

    // Use local workload specs as default
    let local_specs = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("workload_specs");

    println!("Using local workload specs: {}", local_specs.display());
    local_specs
}

/// Set up the default engine for benchmarks, using the table URL to configure
/// the correct object store (local filesystem, S3, etc.).
fn setup_engine(table_url: &str) -> Arc<DefaultEngine<TokioBackgroundExecutor>> {
    use delta_kernel::engine::default::storage::store_from_url;
    use delta_kernel::try_parse_uri;

    let url = try_parse_uri(table_url).expect("Failed to parse table URL");
    let store = store_from_url(&url).expect("Failed to create store");
    Arc::new(DefaultEngine::new(store))
}

/// Serialize a WorkloadSpecVariant to a JSON value for the report.
fn spec_to_json(spec: &WorkloadSpecVariant) -> serde_json::Value {
    serde_json::json!({
        "table": spec.table_info.name,
        "case": spec.case_name,
        "type": match &spec.spec_type {
            WorkloadSpecType::Read(_) => "read",
            WorkloadSpecType::SnapshotConstruction(_) => "snapshot_construction",
        },
        "operation_type": spec.operation_type.map(|op| op.as_str()),
    })
}

/// Helper function to run a single benchmark
fn run_benchmark(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    spec: WorkloadSpecVariant,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
) {
    let workload_name = spec.full_name();
    println!("Setting up benchmark: {}", workload_name);

    let spec_json = spec_to_json(&spec);

    let Ok(mut runner) = create_runner(spec, engine) else {
        eprintln!("Failed to create runner for {}", workload_name);
        return;
    };

    // Verify setup works at least once before benchmarking
    if let Err(e) = runner.setup() {
        eprintln!("Failed to setup runner for {}: {}", workload_name, e);
        return;
    }
    runner.cleanup().ok(); // Clean up after verification

    // Collect per-invocation durations alongside Criterion's measurement
    let collected_durations: RefCell<Vec<Duration>> = RefCell::new(Vec::new());
    let bench_name = workload_name.clone();

    group.bench_function(BenchmarkId::from_parameter(&workload_name), |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;

            for _ in 0..iters {
                // Setup (not timed)
                runner.setup().expect("Setup failed during benchmark");

                // Execute (timed)
                let start = std::time::Instant::now();
                black_box(runner.execute().expect("Benchmark execution failed"));
                let elapsed = start.elapsed();
                total_duration += elapsed;

                // Record individual duration for the report
                collected_durations.borrow_mut().push(elapsed);

                // Cleanup (not timed)
                runner.cleanup().expect("Cleanup failed during benchmark");
            }

            total_duration
        });
    });

    // Store collected results for report generation
    let durations = collected_durations.into_inner();
    COLLECTOR.with(|c| {
        c.borrow_mut().push(CollectedResult {
            name: bench_name,
            spec_json,
            durations,
        });
    });
}

/// Main benchmark function that loads and runs all workloads
fn workload_benchmarks(c: &mut Criterion) {
    let specs_path = get_workload_specs_path();

    // Load all workload specifications
    let Ok(specs) = load_all_workloads(&specs_path) else {
        eprintln!("Failed to load workload specifications from {:?}", specs_path);
        eprintln!("Make sure workload specs are set up in kernel/benches/workload_specs/");
        return;
    };

    if specs.is_empty() {
        eprintln!("No workload specifications found in {:?}", specs_path);
        eprintln!("Please add workload specs to kernel/benches/workload_specs/");
        return;
    }

    println!("Loaded {} workload specification(s)", specs.len());

    // Determine the engine URL from the first spec's table root.
    // All specs in a single run typically share the same storage backend.
    let engine_url = specs
        .first()
        .map(|s| s.table_info.resolved_table_root())
        .unwrap_or_else(|| ".".to_string());
    println!("Creating engine for URL: {}", engine_url);
    let engine = setup_engine(&engine_url);

    // Create a benchmark group for each workload type
    let mut group = c.benchmark_group("workloads");

    for spec in specs {
        // For Read specs, create benchmarks for each operation type
        match &spec.spec_type {
            WorkloadSpecType::Read(_) => {
                // Currently only support read_metadata
                for operation_type in [ReadOperationType::ReadMetadata] {
                    let mut spec_with_op = spec.clone();
                    spec_with_op.operation_type = Some(operation_type);
                    run_benchmark(&mut group, spec_with_op, engine.clone());
                }
            }
            WorkloadSpecType::SnapshotConstruction(_) => {
                run_benchmark(&mut group, spec, engine.clone());
            }
        }
    }

    group.finish();

    // Write benchmark report if BENCHMARK_REPORT_PATH is set
    write_benchmark_report();
}

/// Write the collected benchmark data as a unified report.
fn write_benchmark_report() {
    let report_path = match std::env::var("BENCHMARK_REPORT_PATH") {
        Ok(path) => PathBuf::from(path),
        Err(_) => return, // No report path set, skip report generation
    };

    println!("Writing benchmark report to: {}", report_path.display());

    let timings: Vec<BenchmarkTimings> = COLLECTOR.with(|c| {
        c.borrow()
            .iter()
            .map(|result| {
                let mut t = BenchmarkTimings::new(
                    result.name.clone(),
                    result.spec_json.clone(),
                );
                for d in &result.durations {
                    t.record(*d);
                }
                t
            })
            .collect()
    });

    let report = build_report("Delta Kernel Benchmarks", timings);
    match write_report(&report, &report_path) {
        Ok(()) => println!(
            "Benchmark report written: {} benchmark(s)",
            report.benchmarks.len()
        ),
        Err(e) => eprintln!("Failed to write benchmark report: {}", e),
    }
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);
