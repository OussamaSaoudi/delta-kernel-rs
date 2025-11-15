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

use std::path::PathBuf;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use delta_kernel::benchmarks::{create_runner, load_all_workloads};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

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

/// Set up the default engine for benchmarks
fn setup_engine() -> Arc<DefaultEngine<TokioBackgroundExecutor>> {
    use delta_kernel::engine::default::storage::store_from_url;
    use delta_kernel::try_parse_uri;

    // Use a dummy URL just to create the store - individual benchmarks will
    // use their own table paths
    let dummy_url = try_parse_uri(".").expect("Failed to parse current directory");
    let store = store_from_url(&dummy_url).expect("Failed to create store");
    Arc::new(DefaultEngine::new(store))
}

/// Helper function to run a single benchmark
fn run_benchmark(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    spec: delta_kernel::benchmarks::WorkloadSpecVariant,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
) {
    let workload_name = spec.full_name();
    println!("Setting up benchmark: {}", workload_name);
    
    let mut runner = match create_runner(spec, engine) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to create runner for {}: {}", workload_name, e);
            return;
        }
    };
    
    if let Err(e) = runner.setup() {
        eprintln!("Failed to setup runner for {}: {}", workload_name, e);
        return;
    }
    
    group.bench_function(BenchmarkId::from_parameter(&workload_name), |b| {
        b.iter(|| {
            runner.execute().expect("Benchmark execution failed");
        });
    });
    
    if let Err(e) = runner.cleanup() {
        eprintln!("Failed to cleanup runner for {}: {}", workload_name, e);
    }
}

/// Main benchmark function that loads and runs all workloads
fn workload_benchmarks(c: &mut Criterion) {
    let specs_path = get_workload_specs_path();
    
    // Load all workload specifications
    let specs = match load_all_workloads(&specs_path) {
        Ok(specs) => specs,
        Err(e) => {
            eprintln!("Failed to load workload specifications from {:?}: {}", specs_path, e);
            eprintln!("Make sure workload specs are set up in kernel/benches/workload_specs/");
            return;
        }
    };

    if specs.is_empty() {
        eprintln!("No workload specifications found in {:?}", specs_path);
        eprintln!("Please add workload specs to kernel/benches/workload_specs/");
        return;
    }

    println!("Loaded {} workload specification(s)", specs.len());

    let engine = setup_engine();

    // Create a benchmark group for each workload type
    let mut group = c.benchmark_group("workloads");
    
    for spec in specs {
        // For Read specs, create benchmarks for each operation type
        match &spec.spec_type {
            delta_kernel::benchmarks::WorkloadSpecType::Read(read_spec) => {
                // Currently only support read_metadata
                for operation_type in [delta_kernel::benchmarks::ReadOperationType::ReadMetadata] {
                    let spec_with_op = delta_kernel::benchmarks::WorkloadSpecVariant {
                        table_info: spec.table_info.clone(),
                        case_name: spec.case_name.clone(),
                        spec_type: delta_kernel::benchmarks::WorkloadSpecType::Read(
                            read_spec.clone().with_operation_type(operation_type)
                        ),
                    };
                    run_benchmark(&mut group, spec_with_op, engine.clone());
                }
            }
            delta_kernel::benchmarks::WorkloadSpecType::SnapshotConstruction(_) => {
                run_benchmark(&mut group, spec, engine.clone());
            }
        }
    }

    group.finish();
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);

