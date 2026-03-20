//! MetadataProtocolReader Consumer KDF - extracts metadata and protocol from log files.

use crate::actions::{Metadata, Protocol};
use crate::{DeltaResult, EngineData};

/// State for MetadataProtocolReader consumer KDF - extracts metadata and protocol actions.
///
/// Uses the existing `Protocol::try_new_from_data` and `Metadata::try_new_from_data`
/// visitor methods which guarantee atomic extraction (either we get a complete action or nothing).
/// Direct mutation (no interior mutability) - owned state is mutated and passed back.
#[derive(Debug, Clone, Default)]
pub struct MetadataProtocolReaderState {
    /// The extracted protocol action (complete or None)
    protocol: Option<Protocol>,
    /// The extracted metadata action (complete or None)
    metadata: Option<Metadata>,
    /// Stored error to surface during advance()
    error: Option<String>,
}

impl MetadataProtocolReaderState {
    /// Create new state for reading metadata and protocol.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply consumer to a batch of log file data.
    #[inline]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<bool> {
        // If we already have both, stop
        if self.protocol.is_some() && self.metadata.is_some() {
            return Ok(false);
        }

        // If we have an error, stop processing
        if self.error.is_some() {
            return Ok(false);
        }

        // Try to extract protocol if not found yet
        if self.protocol.is_none() {
            match Protocol::try_new_from_data(batch) {
                Ok(Some(protocol)) => {
                    self.protocol = Some(protocol);
                }
                Ok(None) => {
                    // No protocol in this batch, continue
                }
                Err(e) => {
                    self.error = Some(format!("Failed to extract protocol: {}", e));
                    return Ok(false);
                }
            }
        }

        // Try to extract metadata if not found yet
        if self.metadata.is_none() {
            match Metadata::try_new_from_data(batch) {
                Ok(Some(metadata)) => {
                    self.metadata = Some(metadata);
                }
                Ok(None) => {
                    // No metadata in this batch, continue
                }
                Err(e) => {
                    self.error = Some(format!("Failed to extract metadata: {}", e));
                    return Ok(false);
                }
            }
        }

        // Return false (Break) if we found both, otherwise continue
        Ok(!(self.protocol.is_some() && self.metadata.is_some()))
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&mut self) {
        // Nothing special needed for finalization
    }

    /// Check if both protocol and metadata were successfully extracted.
    pub fn is_complete(&self) -> bool {
        self.protocol.is_some() && self.metadata.is_some()
    }

    /// Check if protocol was found.
    pub fn has_protocol(&self) -> bool {
        self.protocol.is_some()
    }

    /// Check if metadata was found.
    pub fn has_metadata(&self) -> bool {
        self.metadata.is_some()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }

    /// Take the extracted protocol, if any.
    pub fn take_protocol(&mut self) -> Option<Protocol> {
        self.protocol.take()
    }

    /// Take the extracted metadata, if any.
    pub fn take_metadata(&mut self) -> Option<Metadata> {
        self.metadata.take()
    }

    /// Get a clone of the extracted protocol, if any.
    pub fn get_protocol(&self) -> Option<Protocol> {
        self.protocol.clone()
    }

    /// Get a clone of the extracted metadata, if any.
    pub fn get_metadata(&self) -> Option<Metadata> {
        self.metadata.clone()
    }

    // Test helper accessors
    #[cfg(test)]
    pub(crate) fn protocol_ref(&self) -> Option<&Protocol> {
        self.protocol.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn metadata_ref(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn error_ref(&self) -> Option<&String> {
        self.error.as_ref()
    }
}
