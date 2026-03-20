//! Individual plan node types.
//!
//! These are the basic building blocks for constructing plans.
//! Each node represents a single operation in a query plan.

use std::sync::Arc;

use crate::expressions::{Expression, VariadicExpressionOp};
use crate::schema::{ColumnName, SchemaRef};
use crate::FileMeta;

use crate::Version;

use super::kdf_state::{
    AddRemoveDedupState, CheckpointDedupState, CheckpointHintReaderState, ConsumerKdfState,
    ConsumerStateSender, FilterKdfState, FilterStateReceiver, FilterStateSender,
    LogSegmentBuilderState, MetadataProtocolReaderState, SchemaReaderState, SchemaStoreState,
    SidecarCollectorState, StateSender,
};

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
///
/// # Schema Semantics
///
/// The `schema` field specifies the desired output schema with nullability semantics
/// that match the kernel's `ParquetHandler::read_parquet_files` contract:
///
/// - **Missing nullable columns**: Filled with NULL values
/// - **Missing non-nullable columns**: Returns an error
/// - **Type differences**: Cast to the specified type
/// - **Column reordering**: Columns are reordered to match the schema
///
/// When compiled to DataFusion, the `DefaultSchemaAdapterFactory` automatically
/// enforces these semantics by adapting file-level record batches to the table schema.
///
/// # Adding Computed Columns
///
/// To add computed columns (e.g., version from filename), wrap the scan in a Select
/// node with a Transform expression. For multiple files with different transforms,
/// use Union to combine separate Scan→Select pipelines.
#[derive(Debug, Clone)]
pub struct ScanNode {
    /// Type of files to read
    pub file_type: FileType,
    /// Files to scan
    pub files: Vec<FileMeta>,
    /// Schema specifying the desired output structure and nullability constraints.
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

// =============================================================================
// Ordering and Partitioning Specifications for KDFs
// =============================================================================

/// Specifies ordering requirements for a KDF.
///
/// Used to declare what ordering the KDF needs for correct execution.
/// This information allows executors to automatically insert sort operators.
#[derive(Debug, Clone)]
pub struct OrderingSpec {
    /// Column to order by
    pub column: ColumnName,
    /// True for descending order, false for ascending
    pub descending: bool,
}

impl OrderingSpec {
    /// Create a new ordering specification.
    pub fn new(column: ColumnName, descending: bool) -> Self {
        Self { column, descending }
    }

    /// Create a descending ordering specification.
    pub fn desc(column: ColumnName) -> Self {
        Self::new(column, true)
    }

    /// Create an ascending ordering specification.
    pub fn asc(column: ColumnName) -> Self {
        Self::new(column, false)
    }
}

/// Filter rows using a kernel-defined function (KDF).
///
/// KDFs are filters implemented in kernel-rs that engines must use because they
/// contain Delta-specific logic (e.g., deduplication, stats skipping).
/// The function produces a selection vector; engines call the kernel FFI to apply it.
///
/// # Parallelism Support
///
/// `FilterByKDF` declares its parallelism capabilities:
/// - `partitionable_by`: If `Some`, the executor MAY hash-partition input by this expression.
///   The kernel guarantees correctness when partitioned this way.
/// - `requires_ordering`: If `Some`, the executor MUST ensure this ordering within each partition.
///
/// # State Management
///
/// The `sender` field is a `StateSender<FilterKdfState>` that creates `OwnedState` for each
/// partition during execution. Plans hold the sender, while phases hold the corresponding
/// `StateReceiver` to collect results after execution.
///
/// # Creating FilterByKDF
///
/// Use the provided constructors:
///
/// ```ignore
/// use delta_kernel::plans::nodes::FilterByKDF;
///
/// // Add/Remove deduplication - can partition by path, requires version DESC
/// let (filter, receiver) = FilterByKDF::add_remove_dedup();
///
/// // Checkpoint deduplication - stateless, any distribution works
/// let (filter, receiver) = FilterByKDF::checkpoint_dedup();
/// ```
#[derive(Debug, Clone)]
pub struct FilterByKDF {
    /// State sender - creates owned states for each partition.
    /// The sender is cloned into each partition's stream.
    pub sender: FilterStateSender,

    /// Expression to partition by for parallel execution.
    /// If `Some`, executor MAY hash-partition input by this expression.
    /// Kernel guarantees correctness when partitioned this way.
    pub partitionable_by: Option<Arc<Expression>>,

    /// Required ordering within each partition (or globally if single partition).
    /// If `Some`, executor MUST ensure this ordering.
    pub requires_ordering: Option<OrderingSpec>,
}

impl FilterByKDF {
    /// Create a new FilterByKDF with the given sender and metadata.
    pub fn new(
        sender: FilterStateSender,
        partitionable_by: Option<Arc<Expression>>,
        requires_ordering: Option<OrderingSpec>,
    ) -> Self {
        Self {
            sender,
            partitionable_by,
            requires_ordering,
        }
    }

    /// Create an AddRemoveDedup filter with its parallelism capabilities.
    ///
    /// AddRemoveDedup:
    /// - CAN partition by `coalesce(add.path, remove.path)` - deduplication is path-specific
    /// - REQUIRES version DESC ordering within each partition
    ///
    /// Returns both the filter (goes into the plan) and receiver (goes into the phase).
    pub fn add_remove_dedup() -> (Self, FilterStateReceiver) {
        let (sender, receiver) = StateSender::build(FilterKdfState::AddRemoveDedup(
            AddRemoveDedupState::new(),
        ));

        // Partitionable by coalesce(add.path, remove.path)
        // All rows for a given path will be routed to the same partition
        let partitionable_by = Some(Arc::new(Expression::variadic(
            VariadicExpressionOp::Coalesce,
            vec![
                Expression::column(["add", "path"]),
                Expression::column(["remove", "path"]),
            ],
        )));

        // Requires version DESC ordering within each partition
        let requires_ordering = Some(OrderingSpec::desc(ColumnName::new(["version"])));

        (
            Self {
                sender,
                partitionable_by,
                requires_ordering,
            },
            receiver,
        )
    }

    /// Create a CheckpointDedup filter.
    ///
    /// CheckpointDedup:
    /// - NO partitioning requirement (stateless - any distribution works)
    /// - NO ordering requirement
    ///
    /// Returns both the filter (goes into the plan) and receiver (goes into the phase).
    pub fn checkpoint_dedup() -> (Self, FilterStateReceiver) {
        let (sender, receiver) = StateSender::build(FilterKdfState::CheckpointDedup(
            CheckpointDedupState::new(),
        ));

        (
            Self {
                sender,
                partitionable_by: None,
                requires_ordering: None,
            },
            receiver,
        )
    }

    /// Create a FilterByKDF from an existing sender (for compatibility).
    ///
    /// This infers parallelism capabilities from the sender's template state:
    /// - AddRemoveDedup: partitionable by path, requires version DESC
    /// - CheckpointDedup: no requirements
    /// - PartitionPrune: no requirements
    pub fn from_sender(sender: FilterStateSender) -> Self {
        match sender.template() {
            FilterKdfState::AddRemoveDedup(_) => {
                let partitionable_by = Some(Arc::new(Expression::variadic(
                    VariadicExpressionOp::Coalesce,
                    vec![
                        Expression::column(["add", "path"]),
                        Expression::column(["remove", "path"]),
                    ],
                )));
                let requires_ordering = Some(OrderingSpec::desc(ColumnName::new(["version"])));
                Self {
                    sender,
                    partitionable_by,
                    requires_ordering,
                }
            }
            FilterKdfState::CheckpointDedup(_) | FilterKdfState::PartitionPrune(_) => Self {
                sender,
                partitionable_by: None,
                requires_ordering: None,
            },
        }
    }

    /// Get a reference to the underlying state sender.
    pub fn sender(&self) -> &FilterStateSender {
        &self.sender
    }

    /// Get access to the template state (for inspection).
    pub fn template(&self) -> &FilterKdfState {
        self.sender.template()
    }

    /// Create an owned state for a partition.
    ///
    /// Delegates to the underlying sender.
    pub fn create_owned(&self) -> super::kdf_state::OwnedFilterState {
        self.sender.create_owned()
    }

    /// Check if this KDF can be partitioned (has a partitionable_by expression).
    pub fn can_partition(&self) -> bool {
        self.partitionable_by.is_some()
    }

    /// Check if this KDF requires ordering.
    pub fn requires_order(&self) -> bool {
        self.requires_ordering.is_some()
    }
}

use super::kdf_state::ConsumerStateReceiver;

/// Consumer KDF with optional ordering requirement.
///
/// Similar to `FilterByKDF`, this struct holds a state sender and optional
/// ordering requirement. Plans hold the sender (which creates `OwnedState`
/// for each partition), while phases hold the corresponding `StateReceiver`
/// to collect results after execution.
///
/// # Creating ConsumerByKDF
///
/// Use the provided constructors:
///
/// ```ignore
/// use delta_kernel::plans::nodes::ConsumerByKDF;
///
/// // MetadataProtocolReader with version DESC ordering
/// let (consumer, receiver) = ConsumerByKDF::metadata_protocol_reader();
///
/// // LogSegmentBuilder without ordering
/// let (consumer, receiver) = ConsumerByKDF::log_segment_builder(log_root, None, None);
/// ```
#[derive(Debug, Clone)]
pub struct ConsumerByKDF {
    /// State sender - creates owned states for each partition.
    /// The sender is cloned into each partition's stream.
    pub sender: ConsumerStateSender,

    /// Required ordering within each partition (or globally if single partition).
    /// If `Some`, executor MUST ensure this ordering.
    pub requires_ordering: Option<OrderingSpec>,
}

impl ConsumerByKDF {
    /// Create a new ConsumerByKDF with the given sender and optional ordering.
    pub fn new(sender: ConsumerStateSender, requires_ordering: Option<OrderingSpec>) -> Self {
        Self {
            sender,
            requires_ordering,
        }
    }

    /// Create a MetadataProtocolReader consumer with version DESC ordering.
    ///
    /// This consumer extracts the first non-null metadata and protocol actions
    /// from log files. With `version DESC` ordering, the first P&M found is
    /// guaranteed to be from the highest (latest) version.
    pub fn metadata_protocol_reader() -> (Self, ConsumerStateReceiver) {
        let (sender, receiver) = StateSender::build(ConsumerKdfState::MetadataProtocolReader(
            MetadataProtocolReaderState::new(),
        ));
        (
            Self {
                sender,
                requires_ordering: Some(OrderingSpec::desc(ColumnName::new(["version"]))),
            },
            receiver,
        )
    }

    /// Create a LogSegmentBuilder consumer (no ordering required).
    ///
    /// This consumer builds a LogSegment from file listing results by accumulating
    /// commit files, checkpoint parts, and compaction files.
    pub fn log_segment_builder(
        log_root: url::Url,
        end_version: Option<Version>,
        checkpoint_hint_version: Option<Version>,
    ) -> (Self, ConsumerStateReceiver) {
        let (sender, receiver) = StateSender::build(ConsumerKdfState::LogSegmentBuilder(
            LogSegmentBuilderState::new(log_root, end_version, checkpoint_hint_version),
        ));
        (
            Self {
                sender,
                requires_ordering: None,
            },
            receiver,
        )
    }

    /// Create a CheckpointHintReader consumer (no ordering required).
    ///
    /// This consumer extracts checkpoint hint information from the scan results
    /// of the `_last_checkpoint` file.
    pub fn checkpoint_hint_reader() -> (Self, ConsumerStateReceiver) {
        let (sender, receiver) = StateSender::build(ConsumerKdfState::CheckpointHintReader(
            CheckpointHintReaderState::new(),
        ));
        (
            Self {
                sender,
                requires_ordering: None,
            },
            receiver,
        )
    }

    /// Create a SidecarCollector consumer (no ordering required).
    ///
    /// This consumer collects sidecar file paths from V2 checkpoint manifest scan results.
    pub fn sidecar_collector(log_root: url::Url) -> (Self, ConsumerStateReceiver) {
        let (sender, receiver) = StateSender::build(ConsumerKdfState::SidecarCollector(
            SidecarCollectorState::new(log_root),
        ));
        (
            Self {
                sender,
                requires_ordering: None,
            },
            receiver,
        )
    }

    /// Create a ConsumerByKDF from a raw sender (for backwards compatibility).
    ///
    /// Use this when you already have a `ConsumerStateSender` and don't need ordering.
    pub fn from_sender(sender: ConsumerStateSender) -> Self {
        Self {
            sender,
            requires_ordering: None,
        }
    }

    /// Check if this consumer requires ordering.
    pub fn requires_order(&self) -> bool {
        self.requires_ordering.is_some()
    }

    /// Get a reference to the underlying state sender.
    pub fn sender(&self) -> &ConsumerStateSender {
        &self.sender
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
