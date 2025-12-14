//! Individual plan node types.
//!
//! These are the basic building blocks for constructing plans.
//! Each node represents a single operation in a query plan.

use std::sync::Arc;

use crate::expressions::Expression;
use crate::schema::SchemaRef;
use crate::FileMeta;

use super::kdf_state::{AddRemoveDedupState, CheckpointDedupState, FilterKdfState, SchemaReaderState, SchemaStoreState};

// =============================================================================
// Kernel-Defined Function (KDF) Type System
// =============================================================================
//
// KDFs are categorized by their input/output signatures:
// - Filters: Use typed state in FilterKdfState enum (see kdf_state.rs)
// - Schema Readers: (state_ptr, schema) -> ()
// - (Future: Sinks/Consumers: (state_ptr, engineData) -> ())


/// Schema Reader KDFs: receive and store schema information.
///
/// These are used to capture schema from parquet file footers or other sources.
/// Simpler than Filter KDFs - local use only, no distribution/serialization needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaReaderFunctionId {
    /// Stores a schema reference for later retrieval.
    SchemaStore,
}

// =============================================================================
// Leaf Nodes (no children)
// =============================================================================

/// File types that can be read by a scan node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Scan files from storage.
///
/// Reads Parquet or JSON files and produces record batches.
#[derive(Debug, Clone)]
pub struct ScanNode {
    /// Type of files to read
    pub file_type: FileType,
    /// Files to scan
    pub files: Vec<FileMeta>,
    /// Schema to read
    pub schema: SchemaRef,
}

/// List files from a storage path.
///
/// Calls `StorageHandler::list_from` and produces file metadata.
#[derive(Debug, Clone)]
pub struct FileListingNode {
    /// Directory URL or file path to start listing after
    pub path: url::Url,
}

// =============================================================================
// Unary Nodes (one child, not stored here - stored in tree wrapper)
// =============================================================================

/// Filter rows using a kernel-defined function (KDF).
///
/// KDFs are filters implemented in kernel-rs that engines must use because they
/// contain Delta-specific logic (e.g., deduplication, stats skipping).
/// The function produces a selection vector; engines call the kernel FFI to apply it.
#[derive(Debug, Clone)]
pub struct FilterByKDF {
    /// Typed state - the variant encodes which function to apply
    pub state: FilterKdfState,
}

impl FilterByKDF {
    /// Create a new AddRemoveDedup filter.
    pub fn add_remove_dedup() -> Self {
        Self {
            state: FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new()),
        }
    }

    /// Create a new CheckpointDedup filter.
    pub fn checkpoint_dedup() -> Self {
        Self {
            state: FilterKdfState::CheckpointDedup(CheckpointDedupState::new()),
        }
    }

    /// Create from existing state.
    pub fn with_state(state: FilterKdfState) -> Self {
        Self { state }
    }
}

/// Query parquet file schema (footer read only).
///
/// Used to determine checkpoint type by examining the schema.
#[derive(Debug, Clone)]
pub struct SchemaQueryNode {
    /// Path to the parquet file to query
    pub file_path: String,
    /// Typed state - the variant encodes which function to apply
    pub state: SchemaReaderState,
}

impl SchemaQueryNode {
    /// Create a new schema query with SchemaStore state.
    pub fn schema_store(file_path: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
            state: SchemaReaderState::SchemaStore(SchemaStoreState::new()),
        }
    }
}

/// Filter rows by evaluating a predicate expression.
///
/// Unlike `FilterByKDF`, this uses an arbitrary predicate expression
/// that engines evaluate themselves.
#[derive(Debug, Clone)]
pub struct FilterByExpressionNode {
    /// Predicate expression to evaluate
    pub predicate: Arc<Expression>,
}

/// Project/transform columns using expressions.
///
/// Evaluates expressions to produce new columns.
#[derive(Debug, Clone)]
pub struct SelectNode {
    /// Expressions to evaluate for each output column
    pub columns: Vec<Arc<Expression>>,
    /// Schema of the output
    pub output_schema: SchemaRef,
}

/// Parse a JSON column into structured data.
///
/// Used for parsing Delta stats JSON into typed columns.
#[derive(Debug, Clone)]
pub struct ParseJsonNode {
    /// Column containing JSON string (e.g., "add.stats")
    pub json_column: String,
    /// Schema to parse the JSON into
    pub target_schema: SchemaRef,
    /// Name for the output column
    pub output_column: String,
}

/// Extract first non-null value for specified columns.
///
/// Used to extract metadata like protocol/metadata from log files.
#[derive(Debug, Clone)]
pub struct FirstNonNullNode {
    /// Column names to extract first non-null for
    pub columns: Vec<String>,
}

// WRITE EXTENSION:
//
// Need to introduce the following notions:
// * Sink nodes. These can sink to the following:
//     - Context with name.
//     - File
//     - Result (should be streamed to the user)
//     - KDF: A KDF that returns nothing
//     - Void?: throws away the result
//
// * Source nodes: These have two types, and two sources
//    Sources:
//        - User: user provided data. ex: Data files written
//        - Context with name. Contains two types:
//            Types:
//                - Consume: consumes the data from the context
//                - Read
//
//
//  * Union: unions tuples from two streams
//
//
// FULLY EXTERNALIZED READS EXTENSION:
// Suboperator dialect:
//
// CreateMap: creates a map with key and value type
//
// Lookup:
//   Args:
//     - Map type
//     - Lookup key names [ColumnName]
//   Result:
//     - List of type ref
//
// ScanList:
//   Args:
//     - List type
//   Result: tuple stream
//
//
// Gather:
//   Args:
//     - Column name, where column type is Ref
//   Result:
//     - Value at ref. For Map<K,V> this has type V
//
// Scatter:
//   Args:
//     - Column name, where column is a ref
//     - Value: new value
//   Result: None
//
