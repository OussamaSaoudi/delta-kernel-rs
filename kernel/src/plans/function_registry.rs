//! Function registries for Kernel-Defined Functions (KDFs).
//!
//! This module provides helper functions for Schema Reader KDFs which use
//! the registry pattern. Filter KDFs have been migrated to use typed state
//! (see `kdf_state.rs`) and no longer use registry-based dispatch.
//!
//! # KDF Categories
//!
//! - **Filter KDFs**: Now use `FilterKdfState` enum (strongly typed)
//!   - State enum variant IS the function identity
//!   - Direct dispatch via `state.apply()` - no registry lookup
//!   - See `kdf_state.rs` for implementation
//!
//! - **Schema Reader KDFs**: `(state_ptr, schema) -> ()`
//!   - Used for receiving and storing schema information
//!   - Simpler lifecycle: just store_schema (local use only)
//!   - Still uses registry pattern for now

use std::collections::HashMap;
use std::sync::LazyLock;

use super::nodes::SchemaReaderFunctionId;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error};

// =============================================================================
// Schema Reader KDF Types and Registry
// =============================================================================

/// Function pointer type for storing a schema in a Schema Reader KDF.
///
/// # Arguments
/// - `state_ptr`: Raw pointer to the concrete state type
/// - `schema`: The schema to store
pub type SchemaReaderStoreFn = fn(state_ptr: u64, schema: SchemaRef) -> DeltaResult<()>;

/// Entry in the schema reader registry.
///
/// Schema Reader KDFs are simpler than Filter KDFs - they're for local use only,
/// so no serialize/deserialize/free functions are needed.
#[derive(Clone, Copy)]
pub struct SchemaReaderKdfEntry {
    /// Store a schema in the state
    pub store_schema: SchemaReaderStoreFn,
}

/// Static function registry mapping SchemaReaderFunctionId to function pointers.
pub static SCHEMA_READER_REGISTRY: LazyLock<HashMap<SchemaReaderFunctionId, SchemaReaderKdfEntry>> =
    LazyLock::new(|| {
        use super::kdf_implementations::*;

        HashMap::from([(
            SchemaReaderFunctionId::SchemaStore,
            SchemaReaderKdfEntry {
                store_schema: schema_store_store,
            },
        )])
    });

// =============================================================================
// Schema Reader KDF Helper Functions
// =============================================================================

/// Create initial state for a Schema Reader KDF.
///
/// # Arguments
/// - `function_id`: Which Schema Reader KDF to create state for
///
/// # Returns
/// state_ptr pointing to newly allocated state
pub fn schema_reader_create_state(function_id: SchemaReaderFunctionId) -> DeltaResult<u64> {
    use super::kdf_implementations::*;

    match function_id {
        SchemaReaderFunctionId::SchemaStore => schema_store_create(),
    }
}

/// Store a schema in a Schema Reader KDF via registry lookup.
///
/// # Arguments
/// - `function_id`: Which Schema Reader KDF to use
/// - `state_ptr`: Raw pointer to the state
/// - `schema`: The schema to store
pub fn schema_reader_store(
    function_id: SchemaReaderFunctionId,
    state_ptr: u64,
    schema: SchemaRef,
) -> DeltaResult<()> {
    let entry = SCHEMA_READER_REGISTRY.get(&function_id).ok_or_else(|| {
        Error::generic(format!(
            "Unknown schema reader function_id: {:?}",
            function_id
        ))
    })?;
    (entry.store_schema)(state_ptr, schema)
}

/// Get the stored schema from a Schema Reader KDF.
///
/// # Arguments
/// - `function_id`: Which Schema Reader KDF to query
/// - `state_ptr`: Raw pointer to the state
///
/// # Returns
/// The stored schema, if any
pub fn schema_reader_get(
    function_id: SchemaReaderFunctionId,
    state_ptr: u64,
) -> DeltaResult<Option<SchemaRef>> {
    use super::kdf_implementations::*;

    match function_id {
        SchemaReaderFunctionId::SchemaStore => schema_store_get(state_ptr),
    }
}
