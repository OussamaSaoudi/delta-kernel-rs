//! Concrete implementations of Kernel-Defined Functions (KDFs).
//!
//! # Filter KDFs
//!
//! Filter KDFs have been migrated to use strongly typed state in `kdf_state.rs`.
//! The enum variant IS the function identity - no separate function_id needed.
//! See `FilterKdfState`, `AddRemoveDedupState`, and `CheckpointDedupState`.
//!
//! # Schema Reader KDFs (Legacy)
//!
//! Schema Reader KDFs still use the raw pointer approach:
//! Functions: `*_create`, `*_store`, `*_get`
//! - SchemaStore: Stores a schema reference for later retrieval

use crate::schema::SchemaRef;
use crate::DeltaResult;

// =============================================================================
// SchemaStore - Stores a schema reference for later retrieval
// =============================================================================

/// State type for SchemaStore: Optional schema reference
type SchemaStoreState = Option<SchemaRef>;

/// Create initial empty state for SchemaStore.
pub fn schema_store_create() -> DeltaResult<u64> {
    let state = Box::new(SchemaStoreState::None);
    Ok(Box::into_raw(state) as u64)
}

/// Store a schema in the SchemaStore state.
///
/// # Arguments
/// - `state_ptr`: Raw pointer to the SchemaStore state
/// - `schema`: The schema to store
pub fn schema_store_store(state_ptr: u64, schema: SchemaRef) -> DeltaResult<()> {
    let state = unsafe { &mut *(state_ptr as *mut SchemaStoreState) };
    *state = Some(schema);
    Ok(())
}

/// Get the stored schema from SchemaStore state.
///
/// # Arguments
/// - `state_ptr`: Raw pointer to the SchemaStore state
///
/// # Returns
/// The stored schema, if any
pub fn schema_store_get(state_ptr: u64) -> DeltaResult<Option<SchemaRef>> {
    let state = unsafe { &*(state_ptr as *const SchemaStoreState) };
    Ok(state.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // =========================================================================
    // SchemaStore Tests
    // =========================================================================

    #[test]
    fn test_schema_store_create() {
        let state_ptr = schema_store_create().unwrap();
        assert_ne!(state_ptr, 0);
        
        // Initially should be None
        let schema = schema_store_get(state_ptr).unwrap();
        assert!(schema.is_none());
    }

    #[test]
    fn test_schema_store_store_and_get() {
        use crate::schema::{StructType, StructField, DataType};
        
        let state_ptr = schema_store_create().unwrap();
        
        // Create a test schema
        let test_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]));
        
        // Store the schema
        schema_store_store(state_ptr, test_schema.clone()).unwrap();
        
        // Retrieve and verify
        let retrieved = schema_store_get(state_ptr).unwrap();
        assert!(retrieved.is_some());
        
        let retrieved_schema = retrieved.unwrap();
        assert_eq!(retrieved_schema.fields().len(), 2);
        assert_eq!(retrieved_schema.field("id").unwrap().name(), "id");
        assert_eq!(retrieved_schema.field("name").unwrap().name(), "name");
    }

    #[test]
    fn test_schema_store_overwrite() {
        use crate::schema::{StructType, StructField, DataType};
        
        let state_ptr = schema_store_create().unwrap();
        
        // Store first schema
        let schema1 = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("a", DataType::LONG),
        ]));
        schema_store_store(state_ptr, schema1).unwrap();
        
        // Verify first schema
        let retrieved1 = schema_store_get(state_ptr).unwrap().unwrap();
        assert_eq!(retrieved1.fields().len(), 1);
        
        // Store second schema (overwrites)
        let schema2 = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("b", DataType::STRING),
            StructField::nullable("c", DataType::BOOLEAN),
        ]));
        schema_store_store(state_ptr, schema2).unwrap();
        
        // Verify second schema
        let retrieved2 = schema_store_get(state_ptr).unwrap().unwrap();
        assert_eq!(retrieved2.fields().len(), 2);
        assert_eq!(retrieved2.field("b").unwrap().name(), "b");
    }
}
