use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use delta_kernel_benchmarks::models::{
    default_read_configs, ParallelScan, ReadConfig, ReadEngine, ReadOperation, Spec, TableInfo,
};
use delta_kernel_benchmarks::runners::{
    create_read_runner, SnapshotConstructionRunner, WorkloadRunner,
};
use delta_kernel_benchmarks::utils::load_all_workloads;
use test_utils::CountingReporter;

// Loads all workloads and sets up a shared runtime, then registers each as a top-level benchmark.
// For each workload, builds a runner that encapsulates the state (table info, engine, config, etc.)
// and execution logic. After each Criterion timing pass, runs one IO-profiling iteration and
// prints per-call storage and log-replay counts.
fn workload_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

    let reporter = Arc::new(CountingReporter::new());
    let runtime = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"));

    for workload in &workloads {
        match &workload.spec {
            Spec::Read(read_spec) => {
                for operation in [ReadOperation::ReadMetadata, ReadOperation::ReadData] {
                    for config in build_read_configs(&workload.table_info, operation) {
                        let runner = create_read_runner(
                            &workload.table_info,
                            &workload.case_name,
                            read_spec,
                            operation,
                            config,
                            runtime.clone(),
                        )
                        .expect("Failed to create read runner");
                        run_benchmark(c, runner.as_ref(), &reporter);
                    }
                }
            }
            // Snapshot construction has no DataFusion executor variant -- snapshot loading
            // flows through the kernel engine for both runners -- so we register the
            // default-engine `SnapshotConstructionRunner` only.
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                let runner = SnapshotConstructionRunner::setup(
                    &workload.table_info,
                    &workload.case_name,
                    snapshot_construction_spec,
                    runtime.clone(),
                )
                .expect("Failed to create snapshot construction runner");
                run_benchmark(c, &runner, &reporter);
            }
        }
    }
}

// Registers a workload with Criterion and benchmarks its `execute()` function.
// After timing completes, runs one IO-profiling iteration and prints per-call storage and
// log-replay counts. The IO profile is skipped entirely when Criterion filters out the benchmark,
// since Criterion never calls the closure for filtered benchmarks.
fn run_benchmark(c: &mut Criterion, runner: &dyn WorkloadRunner, reporter: &CountingReporter) {
    let bench_ran = AtomicBool::new(false);
    c.bench_function(runner.name(), |b| {
        bench_ran.store(true, Ordering::Relaxed);
        b.iter(|| runner.execute().expect("Benchmark execution failed"))
    });
    if bench_ran.load(Ordering::Relaxed) {
        reporter.reset();
        runner.execute().expect("IO profiling iteration failed");
        reporter.print_summary(runner.name());
    }
}

fn build_read_configs(table_info: &TableInfo, operation: ReadOperation) -> Vec<ReadConfig> {
    match operation {
        ReadOperation::ReadMetadata => {
            // Metadata benchmark comparison modes:
            // 1) default-engine serial
            // 2) default-engine parallel
            let mut configs = vec![ReadConfig {
                name: "default_engine_serial".into(),
                read_engine: ReadEngine::DefaultEngine,
                parallel_scan: ParallelScan::Disabled,
            }];
            let num_threads = if table_info.name.contains("V2Chkpt") {
                2
            } else {
                4
            };
            configs.push(ReadConfig {
                name: format!("default_engine_parallel{num_threads}"),
                read_engine: ReadEngine::DefaultEngine,
                parallel_scan: ParallelScan::Enabled { num_threads },
            });
            configs
        }
        ReadOperation::ReadData => {
            // Skip the DataFusion `ReadData` config for tables with column mapping or
            // deletion vectors enabled: `LoadExec::new` rejects those plans on
            // DataFusion 53 and the bench would panic instead of producing a number.
            // The DataFusion variant rejoins once DF54 ships the `_row_number` virtual
            // column + field-id-aware adapters.
            default_read_configs()
                .into_iter()
                .filter(|cfg| {
                    !matches!(cfg.read_engine, ReadEngine::Datafusion)
                        || datafusion_executor_supports(table_info)
                })
                .collect()
        }
    }
}

/// Quick property-based check: the DataFusion read-data runner cannot drive tables that
/// require column mapping or deletion vectors on DataFusion 53. Used to filter unsupported
/// configurations out of the bench matrix before runner setup so they don't panic at
/// `LoadExec::new`.
///
/// Parallels `acceptance/tests/common/mod.rs::datafusion_executor_unsupported_feature` --
/// that one inspects a fully-loaded `Snapshot`, this one inspects the static `TableInfo`
/// JSON before any IO. Keep the two predicates in lockstep when DF54 gates land
/// (row-tracking, etc.); a shared helper is the right home once `Snapshot` exposes
/// the protocol feature list publicly.
fn datafusion_executor_supports(table_info: &TableInfo) -> bool {
    const COLUMN_MAPPING_MODE: &str = "delta.columnMapping.mode";
    const ENABLE_DELETION_VECTORS: &str = "delta.enableDeletionVectors";
    let cm_enabled = table_info
        .properties
        .get(COLUMN_MAPPING_MODE)
        .is_some_and(|m| m != "none");
    let dv_enabled = table_info
        .properties
        .get(ENABLE_DELETION_VECTORS)
        .is_some_and(|v| v == "true");
    !cm_enabled && !dv_enabled
}

criterion_group!(benches, workload_benchmarks);
criterion_main!(benches);
