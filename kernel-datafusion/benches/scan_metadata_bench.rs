//! Benchmark comparing DataFusion executor vs DefaultEngine for scan metadata performance.
//!
//! This benchmark measures the time to produce scan metadata (file lists with selection vectors)
//! using both the synchronous DefaultEngine and the async DataFusion executor.
//!
//! Test scenarios:
//! - No predicate: Full log replay without filtering
//! - Partition filter: Prunes based on partition column values
//! - Data skipping filter: Prunes based on file statistics (min/max values)
//!
//! Run with: `cargo bench --bench scan_metadata_bench`
//!
//! To compare against a baseline:
//! ```bash
//! git checkout main
//! cargo bench --bench scan_metadata_bench -- --save-baseline main
//! git checkout your-branch
//! cargo bench --bench scan_metadata_bench -- --baseline main
//! ```

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use futures::StreamExt;
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::{column_expr, Expression, Scalar};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::try_parse_uri;
use delta_kernel::PredicateRef;

use delta_kernel_datafusion::{DataFusionExecutor, ScanAsyncExt, SnapshotAsyncBuilderExt};

/// Path to the test Delta table with ~1849 commits
/// Schema: commit_hash (string), repository (string), commit_message (string), commit_date (timestamp)
/// Partitioned by: repository
const TABLE_PATH: &str = "/Users/oussama.saoudi/pyspark_playground/test_2000_large_commits";

// Reduce sample size since these benchmarks are expensive
const SAMPLE_SIZE: usize = 10;

/// Get the table URL using delta_kernel's URI parser
fn table_url() -> Url {
    try_parse_uri(TABLE_PATH).expect("valid table path")
}

/// Setup the DefaultEngine for the test table
fn setup_default_engine() -> (Url, Arc<DefaultEngine<TokioBackgroundExecutor>>) {
    let url = table_url();
    let store = store_from_url(&url).expect("Failed to create store");
    let engine = Arc::new(DefaultEngine::new(store));
    (url, engine)
}

/// Setup the DataFusion executor with row group parallelism and parallel dedup
fn setup_datafusion_executor() -> Arc<DataFusionExecutor> {
    let mut config = ConfigOptions::default();
    config.optimizer.repartition_file_scans = true;
    config.execution.target_partitions = num_cpus::get();
    config.optimizer.enable_round_robin_repartition = false; // ← Add this!
                                                             // config.execution.batch_size = 512; // Try reducing to 1024 or 512

    // Create memory pool
    let memory_pool = Arc::new(GreedyMemoryPool::new(
        8 * 1024 * 1024 * 1024, // 1GB - adjust based on your needs
    ));

    // Actually USE the memory pool in RuntimeEnv
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .unwrap(),
    );

    let session_state = SessionStateBuilder::new()
        .with_config(config.into())
        .with_runtime_env(runtime)
        .build();

    Arc::new(DataFusionExecutor::with_session_state(session_state).with_parallel_dedup(true))
}

/// Create a partition filter predicate: repository = 'backend-api'
fn partition_predicate() -> PredicateRef {
    Arc::new(column_expr!("repository").eq(Expression::literal("backend-api")))
}

/// Create a data skipping predicate: commit_date > '2025-01-01 00:00:00 UTC'
/// Timestamp is in microseconds since epoch
fn data_skipping_predicate() -> PredicateRef {
    // 2025-01-01 00:00:00 UTC in microseconds since Unix epoch
    // = 1735689600 seconds * 1_000_000 = 1735689600000000 microseconds
    let cutoff_micros: i64 = 1735689600 * 1_000_000;
    Arc::new(column_expr!("commit_date").gt(Expression::Literal(Scalar::Timestamp(cutoff_micros))))
}

/// Benchmark scan metadata with DefaultEngine (no predicate)
fn bench_default_engine_no_predicate(c: &mut Criterion) {
    let (url, engine) = setup_default_engine();

    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

    let mut group = c.benchmark_group("scan_metadata_no_predicate");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function(BenchmarkId::new("engine", "DefaultEngine"), |b| {
        b.iter(|| {
            let scan = snapshot
                .clone()
                .scan_builder()
                .build()
                .expect("Failed to build scan");
            let metadata_iter = scan
                .scan_metadata(engine.as_ref())
                .expect("Failed to get scan metadata");
            // Consume the iterator to do the actual work
            for result in metadata_iter {
                black_box(result.expect("Failed to process scan metadata"));
            }
        })
    });

    group.finish();
}

/// Benchmark scan metadata with DataFusion (no predicate)
fn bench_datafusion_no_predicate(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get()) // Match your DataFusion partition count
        .thread_name("datafusion-worker")
        .enable_all()
        .build()
        .expect("Failed to create runtime");
    let url = table_url();
    let executor = setup_datafusion_executor();

    // Build snapshot once outside the benchmark loop
    let snapshot = rt.block_on(async {
        Snapshot::async_builder(url.clone())
            .build(&executor)
            .await
            .expect("Failed to create snapshot")
    });
    let snapshot = Arc::new(snapshot);

    let mut group = c.benchmark_group("scan_metadata_no_predicate");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function(BenchmarkId::new("engine", "DataFusion"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let scan = snapshot
                    .clone()
                    .scan_builder()
                    .build()
                    .expect("Failed to build scan");
                let mut stream = std::pin::pin!(scan.scan_metadata_async(executor.clone()));
                while let Some(result) = stream.next().await {
                    black_box(result.expect("Failed to process scan metadata"));
                }
            })
        })
    });

    group.finish();
}

/// Benchmark scan metadata with DefaultEngine (partition filter)
fn bench_default_engine_partition_filter(c: &mut Criterion) {
    let (url, engine) = setup_default_engine();
    let predicate = partition_predicate();

    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

    let mut group = c.benchmark_group("scan_metadata_partition_filter");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function(BenchmarkId::new("engine", "DefaultEngine"), |b| {
        b.iter(|| {
            let scan = snapshot
                .clone()
                .scan_builder()
                .with_predicate(predicate.clone())
                .build()
                .expect("Failed to build scan");
            let metadata_iter = scan
                .scan_metadata(engine.as_ref())
                .expect("Failed to get scan metadata");
            for result in metadata_iter {
                black_box(result.expect("Failed to process scan metadata"));
            }
        })
    });

    group.finish();
}

/// Benchmark scan metadata with DataFusion (partition filter)
fn bench_datafusion_partition_filter(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let url = table_url();
    let executor = setup_datafusion_executor();
    let predicate = partition_predicate();

    let snapshot = rt.block_on(async {
        Snapshot::async_builder(url.clone())
            .build(&executor)
            .await
            .expect("Failed to create snapshot")
    });
    let snapshot = Arc::new(snapshot);

    let mut group = c.benchmark_group("scan_metadata_partition_filter");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function(BenchmarkId::new("engine", "DataFusion"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let scan = snapshot
                    .clone()
                    .scan_builder()
                    .with_predicate(predicate.clone())
                    .build()
                    .expect("Failed to build scan");
                let mut stream = std::pin::pin!(scan.scan_metadata_async(executor.clone()));
                while let Some(result) = stream.next().await {
                    black_box(result.expect("Failed to process scan metadata"));
                }
            })
        })
    });

    group.finish();
}

/// Benchmark scan metadata with DefaultEngine (data skipping filter)
fn bench_default_engine_data_skipping(c: &mut Criterion) {
    let (url, engine) = setup_default_engine();
    let predicate = data_skipping_predicate();

    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

    let mut group = c.benchmark_group("scan_metadata_data_skipping");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function(BenchmarkId::new("engine", "DefaultEngine"), |b| {
        b.iter(|| {
            let scan = snapshot
                .clone()
                .scan_builder()
                .with_predicate(predicate.clone())
                .build()
                .expect("Failed to build scan");
            let metadata_iter = scan
                .scan_metadata(engine.as_ref())
                .expect("Failed to get scan metadata");
            for result in metadata_iter {
                black_box(result.expect("Failed to process scan metadata"));
            }
        })
    });

    group.finish();
}

/// Benchmark scan metadata with DataFusion (data skipping filter)
fn bench_datafusion_data_skipping(c: &mut Criterion) {
    // Initialize tracing once
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("datafusion=debug")),
        )
        .with_test_writer() // Prevents interference with benchmark output
        .try_init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get()) // Match your DataFusion partition count
        .thread_name("datafusion-worker")
        .enable_all()
        .build()
        .expect("Failed to create runtime");
    let url = table_url();
    let executor = setup_datafusion_executor();
    let predicate = data_skipping_predicate();

    let snapshot = rt.block_on(async {
        Snapshot::async_builder(url.clone())
            .build(&executor)
            .await
            .expect("Failed to create snapshot")
    });
    let snapshot = Arc::new(snapshot);

    let mut group = c.benchmark_group("scan_metadata_data_skipping");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function(BenchmarkId::new("engine", "DataFusion"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let scan = snapshot
                    .clone()
                    .scan_builder()
                    .with_predicate(predicate.clone())
                    .build()
                    .expect("Failed to build scan");
                let mut stream = std::pin::pin!(scan.scan_metadata_async(executor.clone()));
                while let Some(result) = stream.next().await {
                    black_box(result.expect("Failed to process scan metadata"));
                }
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_default_engine_no_predicate,
    bench_datafusion_no_predicate,
    bench_default_engine_partition_filter,
    bench_datafusion_partition_filter,
    bench_default_engine_data_skipping,
    bench_datafusion_data_skipping,
);

criterion_main!(benches);
