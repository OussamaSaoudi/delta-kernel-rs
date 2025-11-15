//! Workload-based benchmarks using Criterion.
//!
//! This benchmark dynamically loads workload specifications from JSON files and
//! executes them using Criterion for performance measurement.
//!
//! You can run this benchmark with `cargo bench --bench workload_bench`.
//!
//! By default, it uses the Java kernel's workload specs for compatibility testing.
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
/// 2. Java kernel's workload specs (for compatibility testing)
/// 3. Rust kernel's local workload specs (fallback)
fn get_workload_specs_path() -> PathBuf {
    // Check environment variable first
    if let Ok(custom_path) = std::env::var("WORKLOAD_SPECS_DIR") {
        return PathBuf::from(custom_path);
    }
    
    // Try Java kernel's workload specs (relative to workspace root)
    // This assumes the Java kernel is checked out alongside delta-kernel-rs
    let workspace_root = env!("CARGO_MANIFEST_DIR");
    let java_specs = PathBuf::from(workspace_root)
        .parent()  // kernel -> delta-kernel-rs
        .and_then(|p| p.parent())  // delta-kernel-rs -> projects/code
        .map(|p| p.join("delta/kernel/kernel-benchmarks/src/test/resources/workload_specs"));
    
    if let Some(ref path) = java_specs {
        if path.exists() {
            println!("Using Java kernel workload specs: {}", path.display());
            return path.clone();
        }
    }
    
    // Fallback to local workload specs
    let local_specs = PathBuf::from(workspace_root)
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
        let workload_name = spec.full_name();
        println!("Setting up benchmark: {}", workload_name);
        
        // Create the runner for this workload
        let mut runner = match create_runner(spec, engine.clone()) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to create runner for {}: {}", workload_name, e);
                continue;
            }
        };

        // Set up the runner (this happens once, not timed)
        if let Err(e) = runner.setup() {
            eprintln!("Failed to setup runner for {}: {}", workload_name, e);
            continue;
        }

        // Run the benchmark
        group.bench_function(BenchmarkId::from_parameter(&workload_name), |b| {
            b.iter(|| {
                runner.execute().expect("Benchmark execution failed");
            });
        });

        // Clean up after this workload
        if let Err(e) = runner.cleanup() {
            eprintln!("Failed to cleanup runner for {}: {}", workload_name, e);
        }
    }

    group.finish();
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);

