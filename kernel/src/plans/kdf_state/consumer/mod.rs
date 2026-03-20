//! Consumer KDF states.
//!
//! Consumer KDFs process batches and return a boolean:
//! - `true` = Continue (keep feeding data)
//! - `false` = Break (stop iteration)

mod log_segment_builder;
mod checkpoint_hint;
mod metadata_protocol;
mod sidecar_collector;

pub use log_segment_builder::LogSegmentBuilderState;
pub use checkpoint_hint::CheckpointHintReaderState;
pub use metadata_protocol::MetadataProtocolReaderState;
pub use sidecar_collector::SidecarCollectorState;

use crate::{DeltaResult, EngineData, Error};

/// Consumer KDF state - the enum variant IS the function identity.
#[derive(Debug, Clone)]
pub enum ConsumerKdfState {
    /// Builds a LogSegment from file listing results
    LogSegmentBuilder(LogSegmentBuilderState),
    /// Extracts checkpoint hint from _last_checkpoint scan
    CheckpointHintReader(CheckpointHintReaderState),
    /// Extracts metadata and protocol from log files
    MetadataProtocolReader(MetadataProtocolReaderState),
    /// Collects sidecar file paths from V2 checkpoint manifest
    SidecarCollector(SidecarCollectorState),
}

impl ConsumerKdfState {
    /// Apply consumer to a batch.
    ///
    /// Returns:
    /// - `Ok(true)` = Continue (keep feeding data)
    /// - `Ok(false)` = Break (stop iteration)
    ///
    /// Uses direct mutation - takes `&mut self` for efficient state updates.
    #[inline]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<bool> {
        match self {
            Self::LogSegmentBuilder(state) => state.apply(batch),
            Self::CheckpointHintReader(state) => state.apply(batch),
            Self::MetadataProtocolReader(state) => state.apply(batch),
            Self::SidecarCollector(state) => state.apply(batch),
        }
    }

    /// Finalize the consumer state after iteration completes.
    pub fn finalize(&mut self) {
        match self {
            Self::LogSegmentBuilder(state) => state.finalize(),
            Self::CheckpointHintReader(state) => state.finalize(),
            Self::MetadataProtocolReader(state) => state.finalize(),
            Self::SidecarCollector(state) => state.finalize(),
        }
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        match self {
            Self::LogSegmentBuilder(state) => state.has_error(),
            Self::CheckpointHintReader(state) => state.has_error(),
            Self::MetadataProtocolReader(state) => state.has_error(),
            Self::SidecarCollector(state) => state.has_error(),
        }
    }

    /// Take the error as a DeltaResult, if any.
    pub fn take_error(&mut self) -> Option<Error> {
        match self {
            Self::LogSegmentBuilder(state) => {
                state.take_error().map(Error::generic)
            }
            Self::CheckpointHintReader(state) => {
                state.take_error().map(Error::generic)
            }
            Self::MetadataProtocolReader(state) => {
                state.take_error().map(Error::generic)
            }
            Self::SidecarCollector(state) => {
                state.take_error().map(Error::generic)
            }
        }
    }
}

// Implement FFI conversion using the macro
crate::impl_ffi_convertible!(ConsumerKdfState);
