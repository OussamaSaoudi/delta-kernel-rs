//! Individual plan node types.
//!
//! These are the basic building blocks for constructing plans.
//! Each node represents a single operation in a query plan.

use std::sync::Arc;

use crate::expressions::Expression;
use crate::schema::SchemaRef;
use crate::FileMeta;

// =============================================================================
// Kernel-Defined Function (KDF) Type System
// =============================================================================
//
// KDFs are categorized by their input/output signatures:
// - Filters: (state_ptr, engineData, selection) -> BooleanArray
// - Schema Readers: (state_ptr, schema) -> ()
// - (Future: Sinks/Consumers: (state_ptr, engineData) -> ())

/// Filter KDFs: take engine data and return a boolean selection vector.
///
/// These are filters implemented in kernel-rs that engines must use because they
/// contain Delta-specific logic (e.g., deduplication, stats skipping).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterKernelFunctionId {
    /// Deduplicates add/remove file actions during commit log replay.
    /// Tracks seen file keys and filters out duplicates.
    AddRemoveDedup,
    /// Deduplicates file actions when reading checkpoint files.
    /// Uses tombstone set built from commit files.
    CheckpointDedup,
}

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
    /// Which kernel filter function to apply
    pub function_id: FilterKernelFunctionId,
    /// Pointer to the concrete state type (determined by function_id).
    /// For local execution, this is a raw pointer to the state.
    /// Engine passes this through without interpreting it.
    pub state_ptr: u64,
    /// Optional serialized state for distributed execution.
    /// Engine explicitly calls kdf_serialize() to populate this when distributing.
    pub serialized_state: Option<Vec<u8>>,
}

/// Query parquet file schema (footer read only).
///
/// Used to determine checkpoint type by examining the schema.
#[derive(Debug, Clone)]
pub struct SchemaQueryNode {
    /// Path to the parquet file to query
    pub file_path: String,
    /// Which schema reader function to use for storing the result
    pub function_id: SchemaReaderFunctionId,
    /// Pointer to the schema reader state
    pub state_ptr: u64,
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
