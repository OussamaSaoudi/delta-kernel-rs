//! SchemaStore Schema Reader KDF - stores a schema reference for later retrieval.

use std::sync::Arc;

use crate::schema::SchemaRef;

/// State for SchemaStore - stores a schema reference for later retrieval.
///
/// Used for schema query operations where we need to capture schema from
/// parquet file footers.
#[derive(Debug, Clone)]
pub struct SchemaStoreState {
    /// Uses Arc<OnceLock> for thread-safe, one-time initialization during execution.
    /// Arc ensures that when the state is cloned (for execution), both the original
    /// and clone share the same underlying storage.
    schema: Arc<std::sync::OnceLock<SchemaRef>>,
}

impl Default for SchemaStoreState {
    fn default() -> Self {
        Self {
            schema: Arc::new(std::sync::OnceLock::new()),
        }
    }
}

impl SchemaStoreState {
    /// Create new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a schema. Can only be called once; subsequent calls are ignored.
    pub fn store(&self, schema: SchemaRef) {
        let _ = self.schema.set(schema);
    }

    /// Get the stored schema, if any.
    pub fn get(&self) -> Option<&SchemaRef> {
        self.schema.get()
    }
}


