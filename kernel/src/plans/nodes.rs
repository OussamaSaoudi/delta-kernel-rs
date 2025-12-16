//! Individual plan node types.
//!
//! These are the basic building blocks for constructing plans.
//! Each node represents a single operation in a query plan.

use std::sync::{Arc, Mutex};

use crate::expressions::Expression;
use crate::schema::SchemaRef;
use crate::FileMeta;

use crate::Version;

use super::kdf_state::{AddRemoveDedupState, CheckpointDedupState, CheckpointHintReaderState, ConsumerKdfState, FilterKdfState, LogSegmentBuilderState, MetadataProtocolReaderState, SchemaReaderState, SchemaStoreState, SidecarCollectorState};

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
    pub state: Arc<Mutex<FilterKdfState>>,
}

impl FilterByKDF {
    /// Create a new AddRemoveDedup filter.
    pub fn add_remove_dedup() -> Self {
        Self {
            state: Arc::new(Mutex::new(FilterKdfState::AddRemoveDedup(
                AddRemoveDedupState::new(),
            ))),
        }
    }

    /// Create a new CheckpointDedup filter.
    pub fn checkpoint_dedup() -> Self {
        Self {
            state: Arc::new(Mutex::new(FilterKdfState::CheckpointDedup(
                CheckpointDedupState::new(),
            ))),
        }
    }

    /// Create from existing state.
    pub fn with_state(state: FilterKdfState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }
}

/// Consume rows using a kernel-defined function (KDF).
///
/// Unlike `FilterByKDF` which returns a per-row selection vector, `ConsumeByKDF`
/// processes the entire batch and returns a single boolean:
/// - `true` = Continue (keep feeding data, equivalent to `ControlFlow::Continue`)
/// - `false` = Break (stop iteration, equivalent to `ControlFlow::Break`)
///
/// Consumer KDFs are used for operations that accumulate state across batches,
/// such as building a LogSegment from file listing results.
#[derive(Debug, Clone)]
pub struct ConsumeByKDF {
    /// Typed state - the variant encodes which function to apply
    pub state: ConsumerKdfState,
}

impl ConsumeByKDF {
    /// Create a new LogSegmentBuilder consumer.
    ///
    /// This consumer builds a LogSegment from file listing results by accumulating
    /// commit files, checkpoint parts, and compaction files.
    ///
    /// # Arguments
    /// * `log_root` - The log directory root URL
    /// * `end_version` - Optional end version to stop at
    /// * `checkpoint_hint_version` - Optional checkpoint hint version from `_last_checkpoint`
    pub fn log_segment_builder(
        log_root: url::Url,
        end_version: Option<Version>,
        checkpoint_hint_version: Option<Version>,
    ) -> Self {
        Self {
            state: ConsumerKdfState::LogSegmentBuilder(LogSegmentBuilderState::new(
                log_root,
                end_version,
                checkpoint_hint_version,
            )),
        }
    }

    /// Create a new CheckpointHintReader consumer.
    ///
    /// This consumer extracts checkpoint hint information from the scan results
    /// of the `_last_checkpoint` file.
    pub fn checkpoint_hint_reader() -> Self {
        Self {
            state: ConsumerKdfState::CheckpointHintReader(CheckpointHintReaderState::new()),
        }
    }

    /// Create a new MetadataProtocolReader consumer.
    ///
    /// This consumer extracts the first non-null metadata and protocol actions
    /// from the scan results of log files (JSON commits or Parquet checkpoints).
    /// Used during snapshot construction to get table configuration.
    pub fn metadata_protocol_reader() -> Self {
        Self {
            state: ConsumerKdfState::MetadataProtocolReader(MetadataProtocolReaderState::new()),
        }
    }

    /// Create a new SidecarCollector consumer.
    ///
    /// This consumer collects sidecar file paths from V2 checkpoint manifest scan results.
    /// Used during log replay to gather sidecar files for V2 checkpoints.
    ///
    /// # Arguments
    /// * `log_root` - The log directory root URL (used to construct full sidecar paths)
    pub fn sidecar_collector(log_root: url::Url) -> Self {
        Self {
            state: ConsumerKdfState::SidecarCollector(SidecarCollectorState::new(log_root)),
        }
    }

    /// Create from existing state.
    pub fn with_state(state: ConsumerKdfState) -> Self {
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
    /// Name for the output column. If empty, outputs at root level (replaces batch).
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

// =============================================================================
// Sink Nodes (terminal nodes that consume data)
// =============================================================================

/// Sink type determines what happens to data flowing into the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    /// Discard all incoming data (consume without output)
    Drop,
    /// Stream results back to the user
    Results,
}

/// Terminal node that consumes data flow.
///
/// All complete plans must end with a sink. Sinks determine the fate of data:
/// - `Drop`: Data is consumed and discarded (useful for side-effect-only operations)
/// - `Results`: Data is streamed back to the user
#[derive(Debug, Clone)]
pub struct SinkNode {
    /// The type of sink determining data fate
    pub sink_type: SinkType,
}

impl SinkNode {
    /// Create a Drop sink that discards all data.
    pub fn drop() -> Self {
        Self {
            sink_type: SinkType::Drop,
        }
    }

    /// Create a Results sink that streams data to the user.
    pub fn results() -> Self {
        Self {
            sink_type: SinkType::Results,
        }
    }
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
