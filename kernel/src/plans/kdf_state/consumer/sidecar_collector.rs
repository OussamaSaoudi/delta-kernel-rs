//! SidecarCollector Consumer KDF - collects sidecar file paths from V2 checkpoint manifest.

use std::sync::{Arc, Mutex};

use url::Url;

use crate::{DeltaResult, EngineData, FileMeta};

/// Inner mutable state for SidecarCollector.
#[derive(Debug)]
struct SidecarCollectorInner {
    /// Log directory root URL (used to construct full sidecar file paths)
    log_root: Url,
    /// Collected sidecar files
    sidecar_files: Vec<FileMeta>,
    /// Stored error to surface during advance()
    error: Option<String>,
}

/// State for SidecarCollector consumer KDF - collects sidecar file paths from V2 checkpoint manifest.
///
/// Already uses the visitor pattern (SidecarVisitor) - no downcasting needed!
#[derive(Debug, Clone)]
pub struct SidecarCollectorState {
    inner: Arc<Mutex<SidecarCollectorInner>>,
}

impl SidecarCollectorState {
    /// Create new state for collecting sidecar files.
    pub fn new(log_root: Url) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SidecarCollectorInner {
                log_root,
                sidecar_files: Vec::new(),
                error: None,
            })),
        }
    }

    /// Apply consumer to a batch of manifest data.
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::actions::visitors::SidecarVisitor;
        use crate::engine_data::RowVisitor as _;

        let mut inner = self.inner.lock().unwrap();

        // If we already have an error, stop processing
        if inner.error.is_some() {
            return Ok(false);
        }

        // Reuse the canonical sidecar extractor used elsewhere in the kernel.
        let mut visitor = SidecarVisitor::default();
        visitor.visit_rows_of(batch)?;

        // Clone log_root before the loop to avoid borrow conflicts
        let log_root = inner.log_root.clone();
        for sidecar in visitor.sidecars {
            inner.sidecar_files.push(sidecar.to_filemeta(&log_root)?);
        }

        Ok(true) // Continue processing
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&self) {
        // Nothing special needed for finalization
    }

    /// Check if any sidecar files were collected.
    pub fn has_sidecars(&self) -> bool {
        !self.inner.lock().unwrap().sidecar_files.is_empty()
    }

    /// Get the number of collected sidecar files.
    pub fn sidecar_count(&self) -> usize {
        self.inner.lock().unwrap().sidecar_files.len()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Take the collected sidecar files.
    pub fn take_sidecar_files(&self) -> Vec<FileMeta> {
        std::mem::take(&mut self.inner.lock().unwrap().sidecar_files)
    }

    /// Get a clone of the collected sidecar files.
    pub fn get_sidecar_files(&self) -> Vec<FileMeta> {
        self.inner.lock().unwrap().sidecar_files.clone()
    }
}


