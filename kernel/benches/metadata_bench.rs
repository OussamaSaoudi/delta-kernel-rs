//! Regression test for metadata (snapshot and scan) performance. This currently reads a table with
//! only JSON commits (checkpoints TODO) and measures (1) time to create a snapshot and (2) time to
//! create scan metadata.
//!
//! You can run this regression test with `cargo bench`.
//!
//! To compare your changes vs. latest main, you can:
//! ```bash
//! # checkout baseline branch (upstream/main) and save as baseline
//! git checkout main # or upstream/main, another branch, etc.
//! cargo bench --bench metadata_bench -- --save-baseline main
//!
//! # switch back to your changes, and compare against baseline
//! git checkout your-branch
//! cargo bench --bench metadata_bench -- --baseline main
//! ```
//!
//! Follow-ups: <https://github.com/delta-io/delta-kernel-rs/issues/1185>

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::{try_parse_uri, Engine};
use tempfile::TempDir;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::load_test_data;
use url::Url;

// force scan metadata bench to use smaller sample size so test runs faster (100 -> 20)
const SCAN_METADATA_BENCH_SAMPLE_SIZE: usize = 20;

struct TrackingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static BASELINE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                record(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

fn record(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn reset_peak() {
    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    BASELINE_BYTES.store(baseline, Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
}

fn peak_delta() -> usize {
    PEAK_BYTES
        .load(Ordering::Relaxed)
        .saturating_sub(BASELINE_BYTES.load(Ordering::Relaxed))
}

fn setup() -> (TempDir, Url, Arc<DefaultEngine<TokioBackgroundExecutor>>) {
    // note this table _only_ has a _delta_log, no data files (can only do metadata reads)
    let table = "300k-add-files-100-col-partitioned";
    let tempdir = load_test_data("./tests/data", table).unwrap();
    let table_path = tempdir.path().join(table);
    let url = try_parse_uri(table_path.to_str().unwrap()).expect("Failed to parse table path");
    // TODO: use multi-threaded executor
    use test_utils::delta_kernel_default_engine::storage::store_from_url;
    let store = store_from_url(&url).expect("Failed to create store");
    let engine = DefaultEngineBuilder::new(store).build();

    (tempdir, url, Arc::new(engine))
}

fn create_snapshot_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup();

    c.bench_function("create_snapshot", |b| {
        b.iter(|| {
            Snapshot::builder_for(url.clone())
                .build(engine.as_ref())
                .expect("Failed to create snapshot")
        })
    });
}

fn scan_metadata_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup();

    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .expect("Failed to create snapshot");

    let mut group = c.benchmark_group("scan_metadata");
    group.sample_size(SCAN_METADATA_BENCH_SAMPLE_SIZE);
    // Benchmark both the default path (kernel builds a per-file transform expression for this
    // partitioned table) and the `without_row_transforms` opt-out (which skips that build and the
    // per-row partition-value parse that feeds it).
    for without_row_transforms in [false, true] {
        let id = if without_row_transforms {
            "scan_metadata_without_row_transforms"
        } else {
            "scan_metadata"
        };
        group.bench_function(id, |b| {
            b.iter(|| {
                let mut builder = snapshot.clone().scan_builder();
                if without_row_transforms {
                    builder = builder.without_row_transforms();
                }
                let scan = builder.build().expect("Failed to build scan");
                let metadata_iter = scan
                    .scan_metadata(engine.as_ref())
                    .expect("Failed to get scan metadata");
                // kernel scans are lazy, we must consume iterator to do the work we want to test
                for result in metadata_iter {
                    result.expect("Failed to process scan metadata");
                }
            })
        });
    }
    group.finish();
}

fn scan_metadata_declarative_sync_benchmark(c: &mut Criterion) {
    let (_tempdir, url, store) = {
        let table = "300k-add-files-100-col-partitioned";
        let tempdir = load_test_data("./tests/data", table).unwrap();
        let table_path = tempdir.path().join(table);
        let url = try_parse_uri(table_path.to_str().unwrap()).expect("Failed to parse table path");
        use test_utils::delta_kernel_default_engine::storage::store_from_url;
        let store = store_from_url(&url).expect("Failed to create store");
        (tempdir, url, store)
    };

    let fallback = Arc::new(SyncEngine::new_with_store(store));
    let engine: Arc<dyn Engine> = Arc::new(PlanBasedEngine::new(
        fallback.clone(),
        fallback.plan_executor(),
    ));
    let snapshot = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .expect("Failed to build snapshot");

    fn execute_scan(snapshot: SnapshotRef, engine: Arc<dyn Engine>) {
        let scan = snapshot
            .scan_builder()
            .build()
            .expect("Failed to build scan");
        for result in scan
            .scan_metadata(engine.as_ref())
            .expect("Failed to get scan metadata")
        {
            result.expect("Failed to process scan metadata");
        }
    }

    if std::env::var_os("ALLOC_SWEEP_ONCE").is_some() {
        for query_count in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
            reset_peak();
            std::thread::scope(|scope| {
                let handles = (0..query_count)
                    .map(|_| {
                        let snapshot = snapshot.clone();
                        let engine = engine.clone();
                        scope.spawn(|| execute_scan(snapshot, engine))
                    })
                    .collect::<Vec<_>>();
                for handle in handles {
                    handle.join().expect("Declarative scan panicked");
                }
            });
            println!(
                "[alloc-sweep] queries={query_count} peak={} KiB",
                peak_delta() / 1024
            );
        }
        return;
    }

    let last_peak = AtomicUsize::new(0);
    c.bench_function("scan_metadata_declarative_sync", |b| {
        b.iter(|| {
            reset_peak();
            execute_scan(snapshot.clone(), engine.clone());
            last_peak.store(peak_delta(), Ordering::Relaxed);
        })
    });
    println!(
        "[alloc] scan_metadata_declarative_sync: peak +{} KiB",
        last_peak.load(Ordering::Relaxed) / 1024
    );

    for query_count in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
        let name = format!("scan_metadata_declarative_sync/concurrent{query_count}");
        let last_peak = AtomicUsize::new(0);
        c.bench_function(&name, |b| {
            b.iter(|| {
                reset_peak();
                std::thread::scope(|scope| {
                    let handles = (0..query_count)
                        .map(|_| {
                            let snapshot = snapshot.clone();
                            let engine = engine.clone();
                            scope.spawn(|| execute_scan(snapshot, engine))
                        })
                        .collect::<Vec<_>>();
                    for handle in handles {
                        handle.join().expect("Declarative scan panicked");
                    }
                });
                last_peak.store(peak_delta(), Ordering::Relaxed);
            })
        });
        println!(
            "[alloc] {name}: peak +{} KiB",
            last_peak.load(Ordering::Relaxed) / 1024
        );
    }
}

criterion_group!(
    benches,
    create_snapshot_benchmark,
    scan_metadata_benchmark,
    scan_metadata_declarative_sync_benchmark
);
criterion_main!(benches);
