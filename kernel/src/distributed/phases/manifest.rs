//! Manifest phase for log replay - processes single-part checkpoint manifest files.

use std::sync::Arc;

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::log_replay::{ActionsBatch, LogReplayProcessor};
use crate::scan::MANIFEST_READ_SCHEMA;
use crate::{DeltaResult, Engine, Error, FileMeta, RowVisitor};

/// Phase that processes single-part checkpoint manifest files.
///
/// Extracts sidecar references while processing the manifest.
pub(crate) struct ManifestPhase<P: LogReplayProcessor> {
    processor: P,
    actions: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>,
    sidecar_visitor: SidecarVisitor,
    log_root: Url,
}

/// Possible transitions after ManifestPhase completes.
pub(crate) enum AfterManifest<P: LogReplayProcessor> {
    /// Has sidecars → return processor + sidecar files
    Sidecars {
        processor: P,
        sidecars: Vec<FileMeta>,
    },
    /// No sidecars
    Done(P),
}

impl<P: LogReplayProcessor> ManifestPhase<P> {
    /// Create a new manifest phase for a single-part checkpoint.
    ///
    /// Processes the manifest file using `MANIFEST_READ_SCHEMA` and accumulates
    /// sidecar references via `SidecarVisitor`.
    ///
    /// # Parameters
    /// - `processor`: The log replay processor
    /// - `manifest_file`: The checkpoint manifest file to process
    /// - `log_root`: Root URL for resolving sidecar paths
    /// - `engine`: Engine for reading files
    pub fn new(
        processor: P,
        manifest_file: FileMeta,
        log_root: Url,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Self> {
        let files = vec![manifest_file.clone()];

        // Determine file type from extension
        let extension = manifest_file
            .location
            .path()
            .rsplit('.')
            .next()
            .unwrap_or("");

        let actions = match extension {
            "json" => engine
                .json_handler()
                .read_json_files(&files, MANIFEST_READ_SCHEMA.clone(), None)?,
            "parquet" => engine
                .parquet_handler()
                .read_parquet_files(&files, MANIFEST_READ_SCHEMA.clone(), None)?,
            ext => {
                return Err(Error::generic(format!(
                    "Unsupported checkpoint extension: {}",
                    ext
                )))
            }
        };

        let actions = actions.map(|batch| batch.map(|b| ActionsBatch::new(b, false)));

        Ok(Self {
            processor,
            actions: Box::new(actions),
            sidecar_visitor: SidecarVisitor::default(),
            log_root,
        })
    }

    /// Transition to the next phase.
    ///
    /// Returns an enum indicating what comes next:
    /// - `Sidecars`: Extracted sidecar files and processor
    /// - `Done`: No sidecars found
    pub fn into_next(self) -> DeltaResult<AfterManifest<P>> {
        let sidecars = self
            .sidecar_visitor
            .sidecars
            .into_iter()
            .map(|s| s.to_filemeta(&self.log_root))
            .collect::<Result<Vec<_>, _>>()?;

        if sidecars.is_empty() {
            Ok(AfterManifest::Done(self.processor))
        } else {
            Ok(AfterManifest::Sidecars {
                processor: self.processor,
                sidecars,
            })
        }
    }
}

impl<P: LogReplayProcessor> Iterator for ManifestPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        self.actions.next().map(|batch_result| {
            batch_result.and_then(|batch| {
                // Extract sidecar references from the batch
                self.sidecar_visitor.visit_rows_of(batch.actions())?;
                // Process the batch through the processor
                self.processor.process_actions_batch(batch)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests will be added with test fixtures
}

