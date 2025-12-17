//! CheckpointHintReader Consumer KDF - extracts checkpoint hint from _last_checkpoint scan.

use std::sync::{Arc, Mutex};

use crate::{DeltaResult, EngineData, Version};

/// Inner mutable state for CheckpointHintReader.
#[derive(Debug, Default)]
struct CheckpointHintReaderInner {
    /// The extracted checkpoint version
    version: Option<Version>,
    /// The number of actions stored in the checkpoint
    size: Option<i64>,
    /// The number of fragments if the checkpoint was written in multiple parts
    parts: Option<i32>,
    /// The number of bytes of the checkpoint
    size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint
    num_of_add_files: Option<i64>,
    /// Whether we've processed the hint (only expect one row)
    processed: bool,
    /// Stored error to surface during advance()
    error: Option<String>,
}

/// State for CheckpointHintReader consumer KDF - extracts checkpoint hint from scan results.
///
/// Uses RowVisitor pattern to eliminate downcasting and column extraction boilerplate.
#[derive(Debug, Clone)]
pub struct CheckpointHintReaderState {
    inner: Arc<Mutex<CheckpointHintReaderInner>>,
}

impl Default for CheckpointHintReaderState {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointHintReaderState {
    /// Create new state for reading checkpoint hint.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CheckpointHintReaderInner::default())),
        }
    }

    /// Apply consumer to a batch of checkpoint hint data using RowVisitor pattern.
    #[inline]
    pub fn apply(&self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::schema::{ColumnName, DataType};

        let mut inner = self.inner.lock().unwrap();

        // If we already processed a hint, stop
        if inner.processed {
            return Ok(false);
        }

        // If we have an error, stop processing
        if inner.error.is_some() {
            return Ok(false);
        }

        struct HintVisitor<'a> {
            inner: &'a mut CheckpointHintReaderInner,
        }

        impl crate::engine_data::RowVisitor for HintVisitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                const LONG: DataType = DataType::LONG;
                const INTEGER: DataType = DataType::INTEGER;
                crate::column_names_and_types![
                    LONG => crate::schema::column_name!("version"),
                    LONG => crate::schema::column_name!("size"),
                    INTEGER => crate::schema::column_name!("parts"),
                    LONG => crate::schema::column_name!("sizeInBytes"),
                    LONG => crate::schema::column_name!("numOfAddFiles"),
                ]
            }

            fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
                if row_count == 0 {
                    return Ok(());
                }

                // We only expect one row in the _last_checkpoint file
                self.inner.version = getters[0].get_long(0, "version")?.map(|v| v as Version);
                self.inner.size = getters[1].get_long(0, "size")?;
                self.inner.parts = getters[2].get_int(0, "parts")?;
                self.inner.size_in_bytes = getters[3].get_long(0, "sizeInBytes")?;
                self.inner.num_of_add_files = getters[4].get_long(0, "numOfAddFiles")?;
                self.inner.processed = true;

                Ok(())
            }
        }

        let mut visitor = HintVisitor { inner: &mut inner };
        visitor.visit_rows_of(batch)?;

        // Return false to indicate we're done (Break)
        Ok(false)
    }

    /// Finalize the state after iteration completes.
    pub fn finalize(&self) {
        // Nothing special needed for finalization
    }

    /// Check if a hint was successfully extracted.
    pub fn has_hint(&self) -> bool {
        self.inner.lock().unwrap().version.is_some()
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.inner.lock().unwrap().error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.take()
    }

    /// Get the extracted version, if any.
    pub fn get_version(&self) -> Option<Version> {
        self.inner.lock().unwrap().version
    }

    /// Get the extracted parts count, if any.
    pub fn get_parts(&self) -> Option<i32> {
        self.inner.lock().unwrap().parts
    }

    // Single test helper instead of 6 separate accessor methods
    #[cfg(test)]
    pub fn inner(&self) -> std::sync::MutexGuard<CheckpointHintReaderInner> {
        self.inner.lock().unwrap()
    }
}

// Make inner public for testing
#[cfg(test)]
impl CheckpointHintReaderInner {
    pub fn version(&self) -> Option<Version> {
        self.version
    }
    
    pub fn parts(&self) -> Option<i32> {
        self.parts
    }
    
    pub fn size(&self) -> Option<i64> {
        self.size
    }
    
    pub fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }
    
    pub fn size_in_bytes(&self) -> Option<i64> {
        self.size_in_bytes
    }
    
    pub fn num_of_add_files(&self) -> Option<i64> {
        self.num_of_add_files
    }
}

