//! Function pointer registry for Kernel-Defined Functions (KDFs).
//!
//! This module provides a static registry that maps `KernelFunctionId` to function pointers
//! for apply, serialize, deserialize, and free operations. This allows engines to call
//! kernel-defined functions without needing trait objects.
//!
//! # Design
//!
//! Each KDF has:
//! - An `apply` function that mutates a selection vector based on batch data
//! - A `serialize` function to convert state to bytes for distribution
//! - A `deserialize` function to reconstruct state from bytes on executors
//! - A `free` function to clean up state when done
//!
//! The `state_ptr` in `FilterByKDF` is a raw pointer to the concrete state type,
//! which is determined by the `function_id`.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::arrow::array::BooleanArray;
use crate::{DeltaResult, EngineData, Error};
use super::nodes::KernelFunctionId;

/// Function pointer type for applying a KDF filter.
/// 
/// # Arguments
/// - `state_ptr`: Raw pointer to the concrete state type
/// - `batch`: The data batch to process  
/// - `selection`: Arrow BooleanArray selection vector - function ANDs its result with existing selection
/// 
/// # Returns
/// The mutated BooleanArray with filter results ANDed in
pub type KdfApplyFn = fn(
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray>;

/// Function pointer type for serializing KDF state.
/// 
/// # Arguments
/// - `state_ptr`: Raw pointer to the concrete state type
/// 
/// # Returns
/// Serialized bytes that can be sent to executors
pub type KdfSerializeFn = fn(state_ptr: u64) -> DeltaResult<Vec<u8>>;

/// Function pointer type for deserializing KDF state.
/// 
/// # Arguments
/// - `bytes`: Serialized state bytes
/// 
/// # Returns
/// New state_ptr pointing to reconstructed state
pub type KdfDeserializeFn = fn(bytes: &[u8]) -> DeltaResult<u64>;

/// Function pointer type for freeing KDF state.
/// 
/// # Arguments
/// - `state_ptr`: Raw pointer to the state to free
pub type KdfFreeFn = fn(state_ptr: u64);

/// Entry in the function registry containing all function pointers for a KDF.
#[derive(Clone, Copy)]
pub struct KdfEntry {
    /// Apply the filter to a batch, mutating the selection vector
    pub apply: KdfApplyFn,
    /// Serialize state for distribution
    pub serialize: KdfSerializeFn,
    /// Deserialize state on executor
    pub deserialize: KdfDeserializeFn,
    /// Free the state when done
    pub free: KdfFreeFn,
}

/// Static function registry mapping KernelFunctionId to function pointers.
pub static FUNCTION_REGISTRY: LazyLock<HashMap<KernelFunctionId, KdfEntry>> = LazyLock::new(|| {
    use super::kdf_implementations::*;
    
    HashMap::from([
        (KernelFunctionId::AddRemoveDedup, KdfEntry {
            apply: add_remove_dedup_apply,
            serialize: add_remove_dedup_serialize,
            deserialize: add_remove_dedup_deserialize,
            free: add_remove_dedup_free,
        }),
        (KernelFunctionId::CheckpointDedup, KdfEntry {
            apply: checkpoint_dedup_apply,
            serialize: checkpoint_dedup_serialize,
            deserialize: checkpoint_dedup_deserialize,
            free: checkpoint_dedup_free,
        }),
    ])
});

/// Apply a KDF filter via registry lookup.
///
/// # Arguments
/// - `function_id`: Which KDF to apply
/// - `state_ptr`: Raw pointer to the state (type determined by function_id)
/// - `batch`: The data batch to process
/// - `selection`: Arrow BooleanArray selection vector - function ANDs its result with existing selection
///
/// # Returns
/// The mutated BooleanArray with filter results ANDed in
pub fn kdf_apply(
    function_id: KernelFunctionId,
    state_ptr: u64,
    batch: &dyn EngineData,
    selection: BooleanArray,
) -> DeltaResult<BooleanArray> {
    let entry = FUNCTION_REGISTRY.get(&function_id)
        .ok_or_else(|| Error::generic(format!("Unknown function_id: {:?}", function_id)))?;
    (entry.apply)(state_ptr, batch, selection)
}

/// Serialize KDF state via registry lookup.
///
/// # Arguments
/// - `function_id`: Which KDF's state to serialize
/// - `state_ptr`: Raw pointer to the state
///
/// # Returns
/// Serialized bytes that can be sent to executors
pub fn kdf_serialize(
    function_id: KernelFunctionId,
    state_ptr: u64,
) -> DeltaResult<Vec<u8>> {
    let entry = FUNCTION_REGISTRY.get(&function_id)
        .ok_or_else(|| Error::generic(format!("Unknown function_id: {:?}", function_id)))?;
    (entry.serialize)(state_ptr)
}

/// Deserialize KDF state via registry lookup.
///
/// # Arguments
/// - `function_id`: Which KDF's state to deserialize
/// - `bytes`: Serialized state bytes
///
/// # Returns
/// New state_ptr pointing to reconstructed state
pub fn kdf_deserialize(
    function_id: KernelFunctionId,
    bytes: &[u8],
) -> DeltaResult<u64> {
    let entry = FUNCTION_REGISTRY.get(&function_id)
        .ok_or_else(|| Error::generic(format!("Unknown function_id: {:?}", function_id)))?;
    (entry.deserialize)(bytes)
}

/// Free KDF state via registry lookup.
///
/// # Arguments
/// - `function_id`: Which KDF's state to free
/// - `state_ptr`: Raw pointer to the state to free
pub fn kdf_free(
    function_id: KernelFunctionId,
    state_ptr: u64,
) {
    if let Some(entry) = FUNCTION_REGISTRY.get(&function_id) {
        (entry.free)(state_ptr);
    }
}

/// Create initial state for a KDF.
///
/// # Arguments
/// - `function_id`: Which KDF to create state for
///
/// # Returns
/// state_ptr pointing to newly allocated state
pub fn kdf_create_state(function_id: KernelFunctionId) -> DeltaResult<u64> {
    use super::kdf_implementations::*;
    
    match function_id {
        KernelFunctionId::AddRemoveDedup => add_remove_dedup_create(),
        KernelFunctionId::CheckpointDedup => checkpoint_dedup_create(),
    }
}

