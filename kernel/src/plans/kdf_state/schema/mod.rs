//! Schema Reader KDF states.
//!
//! Schema Reader KDFs extract schema information from parquet file footers.

mod schema_store;

pub use schema_store::SchemaStoreState;

/// Schema reader KDF state - the enum variant IS the function identity.
#[derive(Debug, Clone)]
pub enum SchemaReaderState {
    /// Stores a schema reference for later retrieval
    SchemaStore(SchemaStoreState),
}

// Implement FFI conversion using the macro
crate::impl_ffi_convertible!(SchemaReaderState);

