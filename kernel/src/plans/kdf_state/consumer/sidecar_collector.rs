//! SidecarCollector Consumer KDF - collects sidecar file paths from V2 checkpoint manifest.

use url::Url;

use crate::{DeltaResult, EngineData, FileMeta};

/// State for SidecarCollector consumer KDF - collects sidecar file paths from V2 checkpoint manifest.
///
/// Already uses the visitor pattern (SidecarVisitor) - no downcasting needed!
/// Direct mutation (no interior mutability) - owned state is mutated and passed back.
#[derive(Debug, Clone)]
pub struct SidecarCollectorState {
    /// Log directory root URL (used to construct full sidecar file paths)
    log_root: Url,
    /// Collected sidecar files
    sidecar_files: Vec<FileMeta>,
    /// Stored error to surface during advance()
    error: Option<String>,
}

impl SidecarCollectorState {
    /// Create new state for collecting sidecar files.
    pub fn new(log_root: Url) -> Self {
        Self {
            log_root,
            sidecar_files: Vec::new(),
            error: None,
        }
    }

    /// Apply consumer to a batch of manifest data.
    #[inline]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::actions::visitors::SidecarVisitor;
        use crate::engine_data::RowVisitor as _;

        // If we already have an error, stop processing
        if self.error.is_some() {
            return Ok(false);
        }

        // Reuse the canonical sidecar extractor used elsewhere in the kernel.
        let mut visitor = SidecarVisitor::default();
        visitor.visit_rows_of(batch)?;

        for sidecar in visitor.sidecars {
            self.sidecar_files.push(sidecar.to_filemeta(&self.log_root)?);
        }

        Ok(true) // Continue processing
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&mut self) {
        // Nothing special needed for finalization
    }

    /// Check if any sidecar files were collected.
    pub fn has_sidecars(&self) -> bool {
        !self.sidecar_files.is_empty()
    }

    /// Get the number of collected sidecar files.
    pub fn sidecar_count(&self) -> usize {
        self.sidecar_files.len()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }

    /// Take the collected sidecar files.
    pub fn take_sidecar_files(&mut self) -> Vec<FileMeta> {
        std::mem::take(&mut self.sidecar_files)
    }

    /// Get a clone of the collected sidecar files.
    pub fn get_sidecar_files(&self) -> Vec<FileMeta> {
        self.sidecar_files.clone()
    }
}
