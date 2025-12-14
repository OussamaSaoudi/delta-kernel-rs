//! Function pointer registries for Kernel-Defined Functions (KDFs).
//!
//! This module provides static registries that map KDF function IDs to function pointers.
//! KDFs are categorized by their input/output signatures:
//!
//! - **Filter KDFs**: `(state_ptr, engineData, selection) -> BooleanArray`
//!   - Used for filtering data batches (e.g., deduplication)
//!   - Requires full lifecycle: apply, serialize, deserialize, free
//!
//! - **Schema Reader KDFs**: `(state_ptr, schema) -> ()`
//!   - Used for receiving and storing schema information
//!   - Simpler lifecycle: just store_schema (local use only)

use std::collections::HashMap;
use std::sync::LazyLock;

use super::nodes::{FilterKernelFunctionId, SchemaReaderFunctionId};
use crate::arrow::array::BooleanArray;
use crate::schema::SchemaRef;
use crate::{DeltaResult, EngineData, Error};

// =============================================================================
// Filter KDF Types and Registry
// =============================================================================

/// Function pointer type for applying a Filter KDF.
///
/// # Arguments
/// - `state_ptr`: Raw pointer to the concrete state type
/// - `batch`: The data batch to process
/// - `selection`: Arrow BooleanArray selection vector - function ANDs its result with existing selection
///
/// # Returns
/// The mutated BooleanArray with filter results ANDed in
pub type FilterApplyFn = fn(
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray>;

/// Function pointer type for serializing Filter KDF state.
///
/// # Arguments
/// - `state_ptr`: Raw pointer to the concrete state type
///
/// # Returns
/// Serialized bytes that can be sent to executors
pub type FilterSerializeFn = fn(state_ptr: u64) -> DeltaResult<Vec<u8>>;

/// Function pointer type for deserializing Filter KDF state.
///
/// # Arguments
/// - `bytes`: Serialized state bytes
///
/// # Returns
/// New state_ptr pointing to reconstructed state
pub type FilterDeserializeFn = fn(bytes: &[u8]) -> DeltaResult<u64>;

/// Function pointer type for freeing Filter KDF state.
///
/// # Arguments
/// - `state_ptr`: Raw pointer to the state to free
pub type FilterFreeFn = fn(state_ptr: u64);

/// Entry in the filter registry containing all function pointers for a Filter KDF.
#[derive(Clone, Copy)]
pub struct FilterKdfEntry {
    /// Apply the filter to a batch, mutating the selection vector
    pub apply: FilterApplyFn,
    /// Serialize state for distribution
    pub serialize: FilterSerializeFn,
    /// Deserialize state on executor
    pub deserialize: FilterDeserializeFn,
    /// Free the state when done
    pub free: FilterFreeFn,
}

/// Static function registry mapping FilterKernelFunctionId to function pointers.
pub static FILTER_REGISTRY: LazyLock<HashMap<FilterKernelFunctionId, FilterKdfEntry>> =
    LazyLock::new(|| {
        use super::kdf_implementations::*;

        HashMap::from([
            (
                FilterKernelFunctionId::AddRemoveDedup,
                FilterKdfEntry {
                    apply: add_remove_dedup_apply,
                    serialize: add_remove_dedup_serialize,
                    deserialize: add_remove_dedup_deserialize,
                    free: add_remove_dedup_free,
                },
            ),
            (
                FilterKernelFunctionId::CheckpointDedup,
                FilterKdfEntry {
                    apply: checkpoint_dedup_apply,
                    serialize: checkpoint_dedup_serialize,
                    deserialize: checkpoint_dedup_deserialize,
                    free: checkpoint_dedup_free,
                },
            ),
        ])
    });

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
// Filter KDF Helper Functions
// =============================================================================

/// Apply a Filter KDF via registry lookup.
///
/// # Arguments
/// - `function_id`: Which Filter KDF to apply
/// - `state_ptr`: Raw pointer to the state (type determined by function_id)
/// - `batch`: The data batch to process
/// - `selection`: Arrow BooleanArray selection vector - function ANDs its result with existing selection
///
/// # Returns
/// The mutated BooleanArray with filter results ANDed in
pub fn filter_kdf_apply(
    function_id: FilterKernelFunctionId,
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray> {
    let entry = FILTER_REGISTRY
        .get(&function_id)
        .ok_or_else(|| Error::generic(format!("Unknown filter function_id: {:?}", function_id)))?;
    (entry.apply)(state_ptr, batch, selection)
}

/// Serialize Filter KDF state via registry lookup.
///
/// # Arguments
/// - `function_id`: Which Filter KDF's state to serialize
/// - `state_ptr`: Raw pointer to the state
///
/// # Returns
/// Serialized bytes that can be sent to executors
pub fn filter_kdf_serialize(
    function_id: FilterKernelFunctionId,
    state_ptr: u64,
) -> DeltaResult<Vec<u8>> {
    let entry = FILTER_REGISTRY
        .get(&function_id)
        .ok_or_else(|| Error::generic(format!("Unknown filter function_id: {:?}", function_id)))?;
    (entry.serialize)(state_ptr)
}

/// Deserialize Filter KDF state via registry lookup.
///
/// # Arguments
/// - `function_id`: Which Filter KDF's state to deserialize
/// - `bytes`: Serialized state bytes
///
/// # Returns
/// New state_ptr pointing to reconstructed state
pub fn filter_kdf_deserialize(
    function_id: FilterKernelFunctionId,
    bytes: &[u8],
) -> DeltaResult<u64> {
    let entry = FILTER_REGISTRY
        .get(&function_id)
        .ok_or_else(|| Error::generic(format!("Unknown filter function_id: {:?}", function_id)))?;
    (entry.deserialize)(bytes)
}

/// Free Filter KDF state via registry lookup.
///
/// # Arguments
/// - `function_id`: Which Filter KDF's state to free
/// - `state_ptr`: Raw pointer to the state to free
pub fn filter_kdf_free(function_id: FilterKernelFunctionId, state_ptr: u64) {
    if let Some(entry) = FILTER_REGISTRY.get(&function_id) {
        (entry.free)(state_ptr);
    }
}

/// Create initial state for a Filter KDF.
///
/// # Arguments
/// - `function_id`: Which Filter KDF to create state for
///
/// # Returns
/// state_ptr pointing to newly allocated state
pub fn filter_kdf_create_state(function_id: FilterKernelFunctionId) -> DeltaResult<u64> {
    use super::kdf_implementations::*;

    match function_id {
        FilterKernelFunctionId::AddRemoveDedup => add_remove_dedup_create(),
        FilterKernelFunctionId::CheckpointDedup => checkpoint_dedup_create(),
    }
}

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

// =============================================================================
// Legacy Compatibility (deprecated, will be removed)
// =============================================================================

// Keep old names as type aliases for backward compatibility during migration
#[deprecated(note = "Use FilterApplyFn instead")]
pub type KdfApplyFn = FilterApplyFn;

#[deprecated(note = "Use FilterSerializeFn instead")]
pub type KdfSerializeFn = FilterSerializeFn;

#[deprecated(note = "Use FilterDeserializeFn instead")]
pub type KdfDeserializeFn = FilterDeserializeFn;

#[deprecated(note = "Use FilterFreeFn instead")]
pub type KdfFreeFn = FilterFreeFn;

#[deprecated(note = "Use FilterKdfEntry instead")]
pub type KdfEntry = FilterKdfEntry;
