//! Shared helpers for `delta-kernel-datafusion-engine` integration tests.
//!
//! Each `tests/*.rs` file compiles as a separate binary, so this module lives at
//! `tests/common/mod.rs` (rather than `tests/common.rs`) to keep it from being treated as a
//! standalone test binary. Test files include it via `mod common;`.

#![allow(dead_code)]

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::plan::ResultPlan;
use delta_kernel::plans::kernel_reducers::{KdfControl, KernelReducer, KernelReducerKind};
use delta_kernel::{DeltaResult, Engine as KernelEngine, EngineData, Snapshot};
use delta_kernel_datafusion_engine::{testing, DataFusionExecutor};
use tempfile::TempDir;
use url::Url;

/// Reducer KDF that accumulates the total number of rows seen across all batches and finishes
/// with the count as a `usize`. Used by tests that verify KDF wiring end-to-end.
///
/// Token identity is by-UUID, so the [`KernelReducerKind`] tag is incidental for test wiring; we
/// reuse [`KernelReducerKind::CheckpointHint`] as a stable placeholder.
#[derive(Debug, Clone, Default)]
pub struct SumRowsReducer {
    pub total: usize,
}

impl SumRowsReducer {
    pub fn new(_kind_label: &'static str) -> Self {
        Self::default()
    }
}

impl KernelReducer for SumRowsReducer {
    fn kind(&self) -> KernelReducerKind {
        KernelReducerKind::CheckpointHint
    }

    fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
        Box::new(self.total)
    }

    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let arrow = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| delta_kernel::Error::generic("expected ArrowEngineData"))?;
        self.total += arrow.record_batch().num_rows();
        Ok(KdfControl::Continue)
    }
}

/// Compile `rp` through a fresh [`DataFusionExecutor`] on a current-thread tokio runtime
/// and concat the resulting batches into a single [`RecordBatch`]. Sync wrapper around
/// [`testing::collect_result_plan`] so synchronous tests don't have to manage their own
/// runtime.
pub fn run_to_one_batch(rp: ResultPlan) -> RecordBatch {
    let exec = DataFusionExecutor::try_new().expect("executor");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let batches = runtime
        .block_on(testing::collect_result_plan(&exec, rp))
        .expect("collect");
    assert!(!batches.is_empty(), "expected at least one batch");
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).expect("concat")
}

/// Bundle of a fixture-table URL plus an optional `TempDir` keeping any tar.zst-extracted
/// data alive for the test's duration.
pub struct FixtureTable {
    _tmp: Option<TempDir>,
    pub url: Url,
}

/// Resolve a kernel test fixture by name. Prefers the on-disk
/// `../kernel/tests/data/{name}` directory; falls back to extracting
/// `../kernel/tests/data/{name}.tar.zst` into a temporary directory.
pub fn fixture_table(name: &str) -> FixtureTable {
    let data_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../kernel/tests/data");
    let direct = data_root.join(name);
    if direct.is_dir() {
        return FixtureTable {
            _tmp: None,
            url: Url::from_directory_path(direct.canonicalize().expect("fixture path"))
                .expect("table url"),
        };
    }
    let tmp = test_utils::load_test_data("../kernel/tests/data", name)
        .unwrap_or_else(|e| panic!("load archived fixture {name}: {e}"));
    let extracted = tmp.path().join(name);
    assert!(
        extracted.is_dir(),
        "archived fixture extraction missing directory: {}",
        extracted.display()
    );
    FixtureTable {
        _tmp: Some(tmp),
        url: Url::from_directory_path(extracted.canonicalize().expect("extracted fixture path"))
            .expect("table url"),
    }
}

/// Build the kernel `DefaultEngine` backed by `LocalFileSystem`. Tests that need the
/// engine for both kernel-reference replay and the `DataFusionExecutor` can construct
/// once and clone the `Arc`.
pub fn default_local_engine() -> Arc<dyn KernelEngine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

/// Open a [`Snapshot`] for the named fixture using [`default_local_engine`].
pub fn open_snapshot_for_fixture(table: &str) -> (Arc<dyn KernelEngine>, Arc<Snapshot>) {
    let engine = default_local_engine();
    let snapshot = Snapshot::builder_for(fixture_table(table).url)
        .build(engine.as_ref())
        .expect("snapshot");
    (engine, snapshot)
}
