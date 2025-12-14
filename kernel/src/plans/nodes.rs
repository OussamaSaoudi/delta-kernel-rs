//! Individual plan node types.
//!
//! These are the basic building blocks for constructing plans.
//! Each node represents a single operation in a query plan.

use std::sync::Arc;

use crate::expressions::Expression;
use crate::schema::SchemaRef;
use crate::FileMeta;

/// Identifier for kernel-defined functions.
///
/// These are filters/transforms implemented in kernel-rs that engines
/// must use (they contain Delta-specific logic).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KernelFunctionId {
    /// Deduplicates add/remove file actions during commit log replay.
    /// Tracks seen file keys and filters out duplicates.
    AddRemoveDedup,
    /// Deduplicates file actions when reading checkpoint files.
    /// Uses tombstone set built from commit files.
    CheckpointDedup,
    /// Filters files based on min/max statistics.
    /// Requires parsed stats JSON.
    StatsSkipping,
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
    /// Which kernel function to apply
    pub function_id: KernelFunctionId,
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
