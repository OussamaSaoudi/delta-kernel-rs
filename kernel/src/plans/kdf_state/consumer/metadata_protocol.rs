//! MetadataProtocolReader Consumer KDF - extracts metadata and protocol from log files.

use std::sync::{Arc, Mutex};

use crate::actions::{Metadata, Protocol};
use crate::{DeltaResult, EngineData};

/// Inner mutable state for MetadataProtocolReader.
#[derive(Debug, Default)]
struct MetadataProtocolReaderInner {
    /// The extracted protocol action (complete or None)
    protocol: Option<Protocol>,
    /// The extracted metadata action (complete or None)
    metadata: Option<Metadata>,
    /// Stored error to surface during advance()
    error: Option<String>,
}

/// State for MetadataProtocolReader consumer KDF - extracts metadata and protocol actions.
///
/// Uses the existing `Protocol::try_new_from_data` and `Metadata::try_new_from_data`
/// visitor methods which guarantee atomic extraction (either we get a complete action or nothing).
#[derive(Debug, Clone)]
pub struct MetadataProtocolReaderState {
    inner: Arc<Mutex<MetadataProtocolReaderInner>>,
}

impl Default for MetadataProtocolReaderState {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataProtocolReaderState {
    /// Create new state for reading metadata and protocol.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetadataProtocolReaderInner::default())),
        }
    }

    /// Apply consumer to a batch of log file data.
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        let mut inner = self.inner.lock().unwrap();

        // If we already have both, stop
        if inner.protocol.is_some() && inner.metadata.is_some() {
            return Ok(false);
        }

        // If we have an error, stop processing
        if inner.error.is_some() {
            return Ok(false);
        }

        // Try to extract protocol if not found yet
        if inner.protocol.is_none() {
            match Protocol::try_new_from_data(batch) {
                Ok(Some(protocol)) => {
                    inner.protocol = Some(protocol);
                }
                Ok(None) => {
                    // No protocol in this batch, continue
                }
                Err(e) => {
                    inner.error = Some(format!("Failed to extract protocol: {}", e));
                    return Ok(false);
                }
            }
        }

        // Try to extract metadata if not found yet
        if inner.metadata.is_none() {
            match Metadata::try_new_from_data(batch) {
                Ok(Some(metadata)) => {
                    inner.metadata = Some(metadata);
                }
                Ok(None) => {
                    // No metadata in this batch, continue
                }
                Err(e) => {
                    inner.error = Some(format!("Failed to extract metadata: {}", e));
                    return Ok(false);
                }
            }
        }

        // Return false (Break) if we found both, otherwise continue
        Ok(!(inner.protocol.is_some() && inner.metadata.is_some()))
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&self) {
        // Nothing special needed for finalization
    }

    /// Check if both protocol and metadata were successfully extracted.
    pub fn is_complete(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.protocol.is_some() && inner.metadata.is_some()
    }

    /// Check if protocol was found.
    pub fn has_protocol(&self) -> bool {
        self.inner.lock().unwrap().protocol.is_some()
    }

    /// Check if metadata was found.
    pub fn has_metadata(&self) -> bool {
        self.inner.lock().unwrap().metadata.is_some()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Take the extracted protocol, if any.
    pub fn take_protocol(&self) -> Option<Protocol> {
        self.inner.lock().unwrap().protocol.take()
    }

    /// Take the extracted metadata, if any.
    pub fn take_metadata(&self) -> Option<Metadata> {
        self.inner.lock().unwrap().metadata.take()
    }

    /// Get a clone of the extracted protocol, if any.
    pub fn get_protocol(&self) -> Option<Protocol> {
        self.inner.lock().unwrap().protocol.clone()
    }

    /// Get a clone of the extracted metadata, if any.
    pub fn get_metadata(&self) -> Option<Metadata> {
        self.inner.lock().unwrap().metadata.clone()
    }

    // Single test helper instead of 3 separate accessor methods
    #[cfg(test)]
    pub fn inner(&self) -> std::sync::MutexGuard<MetadataProtocolReaderInner> {
        self.inner.lock().unwrap()
    }
}

// Make inner public for testing
#[cfg(test)]
impl MetadataProtocolReaderInner {
    pub fn protocol(&self) -> Option<&Protocol> {
        self.protocol.as_ref()
    }
    
    pub fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }
    
    pub fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }
}


