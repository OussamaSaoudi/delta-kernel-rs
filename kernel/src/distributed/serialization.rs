//! Serialization support for distributed log replay.
//!
//! This module provides serialization/deserialization for processor state
//! to enable distribution across executors.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::log_replay::FileActionKey;
use crate::scan::log_replay::ScanLogReplayProcessor;
use crate::scan::state_info::StateInfo;
use crate::{DeltaResult, Engine, Error};

/// Serializable representation of FileActionKey.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SerializableFileActionKey {
    pub path: String,
    pub dv_unique_id: Option<String>,
}

impl From<&FileActionKey> for SerializableFileActionKey {
    fn from(key: &FileActionKey) -> Self {
        Self {
            path: key.path.clone(),
            dv_unique_id: key.dv_unique_id.clone(),
        }
    }
}

impl From<SerializableFileActionKey> for FileActionKey {
    fn from(key: SerializableFileActionKey) -> Self {
        FileActionKey {
            path: key.path,
            dv_unique_id: key.dv_unique_id,
        }
    }
}

/// Serializable state for ScanLogReplayProcessor.
///
/// Contains the minimum information needed to reconstruct a processor
/// on an executor node.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SerializableScanProcessorState {
    /// File action keys seen so far (for deduplication)
    pub seen_file_keys: Vec<SerializableFileActionKey>,
    /// State info (will need custom serialization)
    /// For now, we skip serialization and require it to be provided during deserialization
    /// TODO: Implement proper StateInfo serialization
    #[serde(skip)]
    pub state_info: Option<Arc<StateInfo>>,
}

impl ScanLogReplayProcessor {
    /// Serialize the processor state for distribution to executors.
    ///
    /// # Returns
    /// A byte vector containing the serialized state.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub(crate) fn serialize(&self) -> DeltaResult<Vec<u8>> {
        let state = SerializableScanProcessorState {
            seen_file_keys: self.seen_file_keys().iter().map(|k| k.into()).collect(),
            state_info: Some(self.state_info().clone()),
        };

        // Use serde_json for serialization
        serde_json::to_vec(&state).map_err(|e| {
            Error::generic(format!("Failed to serialize processor state: {}", e))
        })
    }

    /// Deserialize a processor state from bytes.
    ///
    /// # Parameters
    /// - `data`: Serialized processor state
    /// - `engine`: Engine for initializing the processor
    /// - `state_info`: StateInfo to use (since we can't serialize it yet)
    ///
    /// # Returns
    /// A reconstructed `ScanLogReplayProcessor`.
    ///
    /// # Errors
    /// Returns an error if deserialization or reconstruction fails.
    pub(crate) fn deserialize(
        data: &[u8],
        engine: &dyn Engine,
        state_info: Arc<StateInfo>,
    ) -> DeltaResult<Self> {
        let state: SerializableScanProcessorState =
            serde_json::from_slice(data).map_err(|e| {
                Error::generic(format!("Failed to deserialize processor state: {}", e))
            })?;

        // Reconstruct the processor
        let mut processor = Self::new(engine, state_info)?;

        // Restore seen file keys
        processor.set_seen_file_keys(
            state
                .seen_file_keys
                .into_iter()
                .map(|k| k.into())
                .collect()
        );

        Ok(processor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests will be added with test fixtures
}

