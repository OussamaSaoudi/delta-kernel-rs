//! Shared helpers for `delta-kernel-datafusion-engine` integration tests.
//!
//! Each `tests/*.rs` file compiles as a separate binary, so this module lives at
//! `tests/common/mod.rs` (rather than `tests/common.rs`) to keep it from being treated as a
//! standalone test binary. Test files include it via `mod common;`.

#![allow(dead_code)]

use std::any::Any;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::kernel_reducers::{KdfControl, KernelReducer, KernelReducerKind};
use delta_kernel::{DeltaResult, EngineData};

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
