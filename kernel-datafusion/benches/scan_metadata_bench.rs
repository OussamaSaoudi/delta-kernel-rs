//! Benchmark: DataFusion vs DefaultEngine scan metadata performance.
//! Set KERNEL_BENCH_TABLE_PATH to override default table path.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
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

const DEFAULT_TABLE_PATH: &str = "/Users/oussama.saoudi/pyspark_playground/test_2000_large_commits";
const SAMPLE_SIZE: usize = 10;

fn get_table_path() -> Option<String> {
    let path = std::env::var("KERNEL_BENCH_TABLE_PATH").unwrap_or_else(|_| DEFAULT_TABLE_PATH.to_string());
    if std::path::Path::new(&path).exists() {
        Some(path)
    } else {
        eprintln!("Benchmark table not found at '{}'. Set KERNEL_BENCH_TABLE_PATH. Skipping.", path);
        None
    }
}

fn table_url() -> Option<Url> {
    get_table_path().map(|path| try_parse_uri(&path).expect("valid table path"))
}

fn setup_default_engine() -> Option<(Url, Arc<DefaultEngine<TokioBackgroundExecutor>>)> {
    let url = table_url()?;
    let store = store_from_url(&url).expect("Failed to create store");
    let engine = Arc::new(DefaultEngine::builder(store).build());
    Some((url, engine))
}

fn setup_datafusion_executor() -> Arc<DataFusionExecutor> {
    let mut config = ConfigOptions::default();
    config.optimizer.repartition_file_scans = true;
    config.execution.target_partitions = num_cpus::get();
    config.optimizer.enable_round_robin_repartition = false;

    let memory_pool = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024 * 1024));
    let runtime = Arc::new(RuntimeEnvBuilder::new().with_memory_pool(memory_pool).build().unwrap());

    let session_state = SessionStateBuilder::new()
        .with_config(config.into())
        .with_runtime_env(runtime)
        .build();

    Arc::new(DataFusionExecutor::with_session_state(session_state).with_parallel_dedup(true))
}

fn partition_predicate() -> PredicateRef {
    Arc::new(column_expr!("repository").eq(Expression::literal("backend-api")))
}

fn data_skipping_predicate() -> PredicateRef {
    let cutoff_micros: i64 = 1735689600 * 1_000_000; // 2025-01-01 00:00:00 UTC
    Arc::new(column_expr!("commit_date").gt(Expression::Literal(Scalar::Timestamp(cutoff_micros))))
}

fn bench_default_engine_no_predicate(c: &mut Criterion) {
    let Some((url, engine)) = setup_default_engine() else {
        return;
    };

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
            for result in metadata_iter {
                black_box(result.expect("Failed to process scan metadata"));
            }
        })
    });

    group.finish();
}

fn bench_datafusion_no_predicate(c: &mut Criterion) {
    let Some(url) = table_url() else {
        return;
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("datafusion-worker")
        .enable_all()
        .build()
        .expect("Failed to create runtime");
    let executor = setup_datafusion_executor();

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

fn bench_default_engine_partition_filter(c: &mut Criterion) {
    let Some((url, engine)) = setup_default_engine() else {
        return;
    };
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

fn bench_datafusion_partition_filter(c: &mut Criterion) {
    let Some(url) = table_url() else {
        return;
    };

    let rt = Runtime::new().expect("Failed to create runtime");
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

fn bench_default_engine_data_skipping(c: &mut Criterion) {
    let Some((url, engine)) = setup_default_engine() else {
        return;
    };
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

fn bench_datafusion_data_skipping(c: &mut Criterion) {
    let Some(url) = table_url() else {
        return;
    };

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("datafusion=debug")))
        .with_test_writer()
        .try_init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("datafusion-worker")
        .enable_all()
        .build()
        .expect("Failed to create runtime");
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
