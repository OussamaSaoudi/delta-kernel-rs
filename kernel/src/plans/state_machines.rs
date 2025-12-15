//! State machine framework for Delta operations.
//!
//! ALL Delta operations are represented as state machines:
//! - Snapshot building
//! - Scan execution
//! - Log replay
//! - (Future: Write, Compaction, etc.)
//!
//! # Two API Styles
//!
//! ## 1. Compile-time Typed (Type-safe, operation-specific)
//! ```ignore
//! let sm: SnapshotStateMachine = snapshot_builder.build();
//! while !sm.is_terminal() {
//!     let plan = sm.get_plan();
//!     let result = engine.execute_plan(plan)?;
//!     sm.advance(Ok(result))?;
//! }
//! let snapshot: Snapshot = sm.into_result().unwrap();
//! ```
//!
//! ## 2. Generic Reusable (One driver for all operations)
//! ```ignore
//! let sm: AnyStateMachine = snapshot_builder.build().into();
//! let result = engine.execute(sm);  // Works for ANY state machine type
//! ```
//!
//! # Builder Pattern
//!
//! State machines originate from builder APIs, not constructed directly:
//! ```ignore
//! // Snapshot
//! let snapshot = engine.execute(
//!     snapshot_builder.for_version(10).build()
//! );
//!
//! // Scan
//! let results = engine.execute(
//!     snapshot.scan().with_predicate(pred).build()
//! );
//! ```

use std::sync::Arc;

use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::log_segment::LogSegment;
use crate::proto_generated as proto;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::FileMeta;
use crate::{DeltaResult, Error};

use super::composite::*;
use super::declarative::DeclarativePlanNode;
use super::kdf_state::{AddRemoveDedupState, CheckpointDedupState, CheckpointHintReaderState, ConsumerKdfState, FilterKdfState, LogSegmentBuilderState, MetadataProtocolReaderState};
use super::nodes::{ConsumeByKDF, FileListingNode, FileType, FilterByKDF, FirstNonNullNode, ScanNode, SchemaQueryNode, SelectNode, SinkNode};
use super::AsQueryPlan;

// =============================================================================
// StateMachine Trait (Generic Interface)
// =============================================================================

/// Result of advancing a state machine.
#[derive(Debug)]
pub enum AdvanceResult<R> {
    /// State machine advanced to next phase, continue execution.
    Continue,
    /// State machine completed, here's the result.
    Done(R),
}

impl<R> AdvanceResult<R> {
    /// Check if this is the terminal result.
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    /// Extract the result, panicking if not done.
    pub fn unwrap(self) -> R {
        match self {
            Self::Done(r) => r,
            Self::Continue => panic!("Called unwrap on AdvanceResult::Continue"),
        }
    }

    /// Extract the result if done, or None if continuing.
    pub fn into_result(self) -> Option<R> {
        match self {
            Self::Done(r) => Some(r),
            Self::Continue => None,
        }
    }
}

/// Trait for all Delta state machines.
///
/// This trait defines the common interface that ALL operations implement.
/// Engines can write one generic driver that works with any state machine.
///
/// # Example
/// ```ignore
/// let mut sm = create_state_machine();
/// loop {
///     let plan = sm.get_plan().expect("should have plan if not done");
///     let executed = engine.execute(plan)?;
///     match sm.advance(Ok(executed))? {
///         AdvanceResult::Continue => continue,
///         AdvanceResult::Done(result) => return Ok(result),
///     }
/// }
/// ```
pub trait StateMachine {
    /// The result type when the state machine completes.
    type Result;

    /// Get the current plan to execute.
    ///
    /// Returns an error if called on a terminal state machine (after `advance()` returned `Done`).
    fn get_plan(&self) -> DeltaResult<DeclarativePlanNode>;

    /// Advance with the result of plan execution.
    ///
    /// The executed plan contains mutated KDF state (via typed state in FilterByKDF).
    /// The state machine extracts this state and transitions to the next phase.
    ///
    /// # Returns
    /// - `Ok(Continue)` - Advanced to next phase, call `get_plan()` for next plan
    /// - `Ok(Done(result))` - State machine completed, here's the result
    /// - `Err(_)` - Error during advancement
    fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<Self::Result>>;

    /// Get the operation type for this state machine.
    fn operation_type(&self) -> OperationType;

    /// Get the current phase name for debugging.
    fn phase_name(&self) -> &'static str;
}

// =============================================================================
// AnyStateMachine (Type-erased wrapper for generic drivers)
// =============================================================================

/// Type-erased state machine for generic engine drivers.
///
/// Wraps any concrete state machine type, allowing engines to write
/// one driver that works with all operations.
///
/// # Example
/// ```ignore
/// fn execute_any(engine: &Engine, mut sm: AnyStateMachine) -> DeltaResult<AnyResult> {
///     loop {
///         let plan = sm.get_plan().expect("should have plan");
///         let executed = engine.execute_plan(plan)?;
///         match sm.advance(Ok(executed))? {
///             AdvanceResult::Continue => continue,
///             AdvanceResult::Done(result) => return Ok(result),
///         }
///     }
/// }
/// ```
pub enum AnyStateMachine {
    Snapshot(SnapshotStateMachine),
    Scan(ScanStateMachine),
    // Future:
    // Write(WriteStateMachine),
}

/// Type-erased result from any state machine.
pub enum AnyResult {
    Snapshot(Snapshot),
    Scan(ScanMetadataResult),
    // Future:
    // Write(WriteResult),
}

impl AnyStateMachine {
    /// Get the current plan to execute.
    /// Returns an error if called on a terminal state machine.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        match self {
            Self::Snapshot(sm) => sm.get_plan(),
            Self::Scan(sm) => sm.get_plan(),
        }
    }

    /// Check if the state machine has reached a terminal state.
    ///
    /// Convenience method for FFI callers to check state before calling get_plan().
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Snapshot(sm) => sm.is_terminal(),
            Self::Scan(sm) => sm.is_terminal(),
        }
    }

    /// Advance with the result of plan execution.
    pub fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<AnyResult>> {
        match self {
            Self::Snapshot(sm) => {
                let advance_result = sm.advance(result)?;
                Ok(match advance_result {
                    AdvanceResult::Continue => AdvanceResult::Continue,
                    AdvanceResult::Done(r) => AdvanceResult::Done(AnyResult::Snapshot(r)),
                })
            }
            Self::Scan(sm) => {
                let advance_result = sm.advance(result)?;
                Ok(match advance_result {
                    AdvanceResult::Continue => AdvanceResult::Continue,
                    AdvanceResult::Done(r) => AdvanceResult::Done(AnyResult::Scan(r)),
                })
            }
        }
    }

    /// Get the operation type.
    pub fn operation_type(&self) -> OperationType {
        match self {
            Self::Snapshot(sm) => sm.operation_type(),
            Self::Scan(sm) => sm.operation_type(),
        }
    }

    /// Get the phase name for debugging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::Snapshot(sm) => sm.phase_name(),
            Self::Scan(sm) => sm.phase_name(),
        }
    }
}

// =============================================================================
// Opaque State Machine (Handle-based API)
// =============================================================================

/// Checkpoint type discovered during log replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointType {
    /// Classic single-file checkpoint
    Classic,
    /// Multi-part checkpoint (v1)
    MultiPart,
    /// V2 checkpoint with manifest and sidecars
    V2,
}


// =============================================================================
// Snapshot State Machine
// =============================================================================

/// Phase of the snapshot state machine with embedded typed plans.
///
/// Each variant contains the compile-time typed plan struct for that phase.
/// Use `as_query_plan()` to convert to runtime `DeclarativePlanNode`.
#[derive(Debug, Clone)]
pub enum SnapshotPhase {
    /// Read _last_checkpoint hint file
    CheckpointHint(CheckpointHintPlan),
    /// List files in _delta_log directory
    ListFiles(FileListingPhasePlan),
    /// Read protocol and metadata from log files
    LoadMetadata(MetadataLoadPlan),
    /// Snapshot is ready
    Complete,
}

impl SnapshotPhase {
    /// Get the query plan for the current phase (if not complete).
    pub fn as_query_plan(&self) -> Option<DeclarativePlanNode> {
        match self {
            Self::CheckpointHint(p) => Some(p.as_query_plan()),
            Self::ListFiles(p) => Some(p.as_query_plan()),
            Self::LoadMetadata(p) => Some(p.as_query_plan()),
            Self::Complete => None,
        }
    }

    /// Check if this is a terminal state.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Get the phase name for debugging/logging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::CheckpointHint(_) => "CheckpointHint",
            Self::ListFiles(_) => "ListFiles",
            Self::LoadMetadata(_) => "LoadMetadata",
            Self::Complete => "Complete",
        }
    }
}

impl AsDeclarativePhase for SnapshotPhase {
    fn as_declarative_phase(&self) -> DeclarativePhase {
        match self {
            Self::CheckpointHint(plan) => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::CheckpointHint,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::ListFiles(plan) => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::ListFiles,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::LoadMetadata(plan) => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::LoadMetadata,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::Complete => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::Complete,
                query_plan: None,
                terminal_data: None,
            },
        }
    }
}

/// Internal state accumulated during snapshot building.
#[derive(Debug)]
pub struct SnapshotBuildState {
    /// Table root URL
    pub table_root: Url,
    /// Log directory URL
    pub log_root: Url,
    /// Target version (None = latest)
    pub version: Option<i64>,
    /// Discovered checkpoint hint version
    pub checkpoint_hint_version: Option<i64>,

    // LogSegment built during ListFiles phase
    /// The constructed log segment
    pub log_segment: Option<LogSegment>,

    // TableConfiguration data accumulated during LoadMetadata phase
    /// Protocol discovered from log
    pub protocol: Option<Protocol>,
    /// Metadata discovered from log
    pub metadata: Option<Metadata>,
}

impl SnapshotBuildState {
    fn new(table_root: Url) -> DeltaResult<Self> {
        let log_root = table_root.join("_delta_log/")?;
        Ok(Self {
            table_root,
            log_root,
            version: None,
            checkpoint_hint_version: None,
            log_segment: None,
            protocol: None,
            metadata: None,
        })
    }
}

/// State machine for building a table snapshot.
///
/// Phases:
/// 1. `CheckpointHint` - Read _last_checkpoint file for optimization
/// 2. `ListFiles` - List log files to find version and checkpoint
/// 3. `LoadMetadata` - Read protocol and metadata from log files
/// 4. `Complete` - Snapshot is ready
///
/// # Example
///
/// ```ignore
/// let sm = SnapshotStateMachine::new(table_root)?;
/// let snapshot = executor.execute_state_machine(sm)?;
/// // snapshot is a full Snapshot with LogSegment and TableConfiguration
/// ```
#[derive(Debug)]
pub struct SnapshotStateMachine {
    /// Current phase 
    phase: SnapshotPhase,
    /// Accumulated state
    state: SnapshotBuildState,
}

impl SnapshotStateMachine {
    /// Create a new snapshot state machine for a table.
    pub fn new(table_root: Url) -> DeltaResult<Self> {
        let state = SnapshotBuildState::new(table_root)?;
        let initial_plan = Self::create_checkpoint_hint_plan(&state)?;
        Ok(Self {
            phase: SnapshotPhase::CheckpointHint(initial_plan),
            state,
        })
    }

    /// Create with a specific target version.
    pub fn with_version(table_root: Url, version: i64) -> DeltaResult<Self> {
        let mut sm = Self::new(table_root)?;
        sm.state.version = Some(version);
        Ok(sm)
    }

    /// Get the current phase.
    pub fn phase(&self) -> &SnapshotPhase {
        &self.phase
    }

    /// Check if terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase, SnapshotPhase::Complete)
    }

    /// Get the plan for the current phase.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        self.phase
            .as_query_plan()
            .ok_or_else(|| Error::generic("Cannot get plan from Complete state"))
    }

    /// Get the declarative phase (for runtime inspection).
    pub fn as_declarative_phase(&self) -> DeclarativePhase {
        self.phase.as_declarative_phase()
    }

    /// Advance the state machine with the result of plan execution.
    pub fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<Snapshot>> {
        match &self.phase {
            SnapshotPhase::CheckpointHint(_) => {
                // For CheckpointHint phase, FileNotFound is OK - the file is optional
                match result {
                    Ok(executed_plan) => {
                        // Extract checkpoint hint state from the executed plan
                        self.extract_consumer_kdf_state(executed_plan)?;
                    }
                    Err(Error::FileNotFound(_)) => {
                        // _last_checkpoint file not found is OK - continue without hint
                        // checkpoint_hint_version remains None
                    }
                    Err(Error::IOError(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::NotFound =>
                    {
                        // IOError with NotFound kind is also OK - file doesn't exist
                        // checkpoint_hint_version remains None
                    }
                    Err(e) => {
                        // Other errors should be propagated
                        return Err(e);
                    }
                }
                
                // Move to ListFiles phase with its plan
                let list_files_plan = Self::create_list_files_plan(&self.state)?;
                self.phase = SnapshotPhase::ListFiles(list_files_plan);
                Ok(AdvanceResult::Continue)
            }
            SnapshotPhase::ListFiles(_) => {
                // Propagate execution errors for this phase
                let executed_plan = result?;
                
                // Extract consumer KDF state from the executed plan (builds LogSegment)
                self.extract_consumer_kdf_state(executed_plan)?;
                
                // Move to LoadMetadata phase with its plan
                let load_metadata_plan = Self::create_load_metadata_plan(&self.state)?;
                self.phase = SnapshotPhase::LoadMetadata(load_metadata_plan);
                Ok(AdvanceResult::Continue)
            }
            SnapshotPhase::LoadMetadata(_) => {
                // Propagate execution errors for this phase
                let executed_plan = result?;
                
                // Extract protocol and metadata from the executed plan's consumer KDF state
                self.extract_consumer_kdf_state(executed_plan)?;
                
                // Fail if protocol or metadata were not found - this is required for snapshot construction
                let protocol = self
                    .state
                    .protocol
                    .take()
                    .ok_or_else(|| Error::MissingProtocol)?;
                let metadata = self
                    .state
                    .metadata
                    .take()
                    .ok_or_else(|| Error::MissingMetadata)?;
                
                // Complete - build the full Snapshot
                self.phase = SnapshotPhase::Complete;

                // Get the LogSegment built during ListFiles phase
                let log_segment = self
                    .state
                    .log_segment
                    .take()
                    .ok_or_else(|| Error::generic("LogSegment not built during ListFiles phase"))?;

                let table_configuration = TableConfiguration::try_new(
                    metadata,
                    protocol,
                    self.state.table_root.clone(),
                    log_segment.end_version,
                )?;

                // Construct the final Snapshot
                let snapshot = Snapshot::new(log_segment, table_configuration);

                Ok(AdvanceResult::Done(snapshot))
            }
            SnapshotPhase::Complete => Err(Error::generic("Cannot advance from Complete state")),
        }
    }

    // =========================================================================
    // Plan Creation Helpers
    // =========================================================================

    fn create_checkpoint_hint_plan(state: &SnapshotBuildState) -> DeltaResult<CheckpointHintPlan> {
        use crate::schema::{DataType, StructField, StructType};

        let checkpoint_path = state.log_root.join("_last_checkpoint")?;
        let file_meta = FileMeta {
            location: checkpoint_path,
            last_modified: 0,
            size: 0,
        };

        // Schema for _last_checkpoint JSON file
        let checkpoint_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("version", DataType::LONG),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("parts", DataType::INTEGER),
            StructField::nullable("sizeInBytes", DataType::LONG),
            StructField::nullable("numOfAddFiles", DataType::LONG),
        ]));

        Ok(CheckpointHintPlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![file_meta],
                schema: checkpoint_schema,
            },
            hint_reader: ConsumeByKDF::checkpoint_hint_reader(),
            sink: SinkNode::drop(), // Drop sink - no results, just side effects
        })
    }

    fn create_list_files_plan(state: &SnapshotBuildState) -> DeltaResult<FileListingPhasePlan> {
        // List files in the _delta_log directory with LogSegmentBuilder consumer
        // Pass the checkpoint hint version from the previous phase to optimize file listing
        Ok(FileListingPhasePlan {
            listing: FileListingNode {
                path: state.log_root.clone(),
            },
            log_segment_builder: ConsumeByKDF::log_segment_builder(
                state.log_root.clone(),
                state.version.map(|v| v as u64),
                state.checkpoint_hint_version.map(|v| v as u64),
            ),
            sink: SinkNode::drop(), // Drop sink - no results, just side effects
        })
    }

    fn create_load_metadata_plan(state: &SnapshotBuildState) -> DeltaResult<MetadataLoadPlan> {
        use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
        use crate::schema::{StructField, StructType, ToSchema};

        // Get the log segment built during ListFiles phase
        let log_segment = state
            .log_segment
            .as_ref()
            .ok_or_else(|| Error::generic("LogSegment not available for LoadMetadata phase"))?;

        // Use the canonical schemas from Protocol and Metadata types
        // This ensures field order matches what the visitors expect
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(PROTOCOL_NAME, crate::actions::Protocol::to_schema()),
            StructField::nullable(METADATA_NAME, crate::actions::Metadata::to_schema()),
        ]));

        // Build list of files to scan from checkpoint_parts and commit_files
        let mut files = Vec::new();
        
        // Add checkpoint files first (they contain complete P&M)
        for checkpoint in &log_segment.checkpoint_parts {
            // ParsedLogPath<FileMeta>.location is already a FileMeta
            files.push(checkpoint.location.clone());
        }
        
        // Add commit files (in reverse order - newest first to find P&M faster)
        for commit in log_segment.ascending_commit_files.iter().rev() {
            // ParsedLogPath<FileMeta>.location is already a FileMeta
            files.push(commit.location.clone());
        }

        // Determine file type based on what we're scanning
        let file_type = if !log_segment.checkpoint_parts.is_empty() {
            FileType::Parquet
        } else {
            FileType::Json
        };

        Ok(MetadataLoadPlan {
            scan: ScanNode {
                file_type,
                files,
                schema,
            },
            metadata_reader: ConsumeByKDF::metadata_protocol_reader(),
            sink: SinkNode::drop(), // Drop sink - no results, just side effects
        })
    }

    /// Extract consumer KDF state from the executed plan and update snapshot build state.
    ///
    /// Walks the plan tree to find ConsumeByKDF nodes and extracts the results.
    fn extract_consumer_kdf_state(&mut self, plan: DeclarativePlanNode) -> DeltaResult<()> {
        match plan {
            DeclarativePlanNode::ConsumeByKDF { child: _, node } => {
                // Extract state based on consumer type
                match node.state {
                    ConsumerKdfState::LogSegmentBuilder(builder_state) => {
                        // Build LogSegment directly from the builder state
                        self.state.log_segment = Some(builder_state.into_log_segment()?);
                    }
                    ConsumerKdfState::CheckpointHintReader(hint_state) => {
                        // Check for errors first
                        if let Some(error) = hint_state.take_error() {
                            return Err(Error::generic(error));
                        }

                        // Extract checkpoint hint version if present
                        if let Some(version) = hint_state.get_version() {
                            self.state.checkpoint_hint_version = Some(version as i64);
                        }
                    }
                    ConsumerKdfState::MetadataProtocolReader(mp_state) => {
                        // Check for errors first
                        if let Some(error) = mp_state.take_error() {
                            return Err(Error::generic(error));
                        }

                        // Extract protocol if present (already a complete Protocol or None)
                        if let Some(protocol) = mp_state.take_protocol() {
                            self.state.protocol = Some(protocol);
                        }

                        // Extract metadata if present (already a complete Metadata or None)
                        if let Some(metadata) = mp_state.take_metadata() {
                            self.state.metadata = Some(metadata);
                        }
                    }
                    ConsumerKdfState::SidecarCollector(_) => {
                        // SidecarCollector is not used in snapshot building
                    }
                }
                Ok(())
            }
            // Recursively check children
            DeclarativePlanNode::FilterByKDF { child, .. }
            | DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. }
            | DeclarativePlanNode::Sink { child, .. } => {
                self.extract_consumer_kdf_state(*child)
            }
            // Leaf nodes have no consumer state
            DeclarativePlanNode::Scan(_)
            | DeclarativePlanNode::FileListing(_)
            | DeclarativePlanNode::SchemaQuery(_) => Ok(()),
        }
    }
}

impl StateMachine for SnapshotStateMachine {
    type Result = Snapshot;

    fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        SnapshotStateMachine::get_plan(self)
    }

    fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<Self::Result>> {
        SnapshotStateMachine::advance(self, result)
    }

    fn operation_type(&self) -> OperationType {
        OperationType::SnapshotBuild
    }

    fn phase_name(&self) -> &'static str {
        self.phase.phase_name()
    }
}

impl From<SnapshotStateMachine> for AnyStateMachine {
    fn from(sm: SnapshotStateMachine) -> Self {
        Self::Snapshot(sm)
    }
}

// =============================================================================
// Scan State Machine
// =============================================================================

/// Phase of the scan state machine with embedded typed plans.
///
/// Each variant contains the compile-time typed plan struct for that phase.
/// Use `as_query_plan()` to convert to runtime `DeclarativePlanNode`.
#[derive(Debug, Clone)]
pub enum ScanPhase {
    /// Process commit files (JSON log) - Result sink
    Commit(CommitPhasePlan),
    /// Query checkpoint schema to detect V2 checkpoints - no sink (schema only)
    SchemaQuery(SchemaQueryPhasePlan),
    /// Read checkpoint manifest (v2 checkpoints) - Drop sink
    CheckpointManifest(CheckpointManifestPlan),
    /// Read checkpoint leaf/sidecar files - Result sink
    CheckpointLeaf(CheckpointLeafPlan),
    /// Scan is complete
    Complete,
}

impl ScanPhase {
    /// Get the phase name for debugging/logging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::Commit(_) => "Commit",
            Self::SchemaQuery(_) => "SchemaQuery",
            Self::CheckpointManifest(_) => "CheckpointManifest",
            Self::CheckpointLeaf(_) => "CheckpointLeaf",
            Self::Complete => "Complete",
        }
    }

    /// Get the query plan for the current phase (if not complete).
    pub fn as_query_plan(&self) -> Option<DeclarativePlanNode> {
        match self {
            Self::Commit(p) => Some(p.as_query_plan()),
            Self::SchemaQuery(p) => Some(p.as_query_plan()),
            Self::CheckpointManifest(p) => Some(p.as_query_plan()),
            Self::CheckpointLeaf(p) => Some(p.as_query_plan()),
            Self::Complete => None,
        }
    }

    /// Check if this is a terminal state.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Internal state accumulated during scan execution.
#[derive(Debug)]
pub struct ScanBuildState {
    /// Table root URL
    pub table_root: Url,
    /// Log directory URL
    pub log_root: Url,
    /// Physical schema for reading data files
    pub physical_schema: SchemaRef,
    /// Logical schema (output schema)
    pub logical_schema: SchemaRef,
    /// Typed dedup state for AddRemoveDedup KDF
    pub dedup_state: FilterKdfState,
    /// Files discovered during commit phase
    pub commit_files: Vec<FileMeta>,
    /// Checkpoint files to read
    pub checkpoint_files: Vec<FileMeta>,
    /// Sidecar files for v2 checkpoints
    pub sidecar_files: Vec<FileMeta>,
    /// Checkpoint type (determined from schema query)
    pub checkpoint_type: Option<CheckpointType>,
}

impl ScanBuildState {
    fn new(
        table_root: Url,
        physical_schema: SchemaRef,
        logical_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let log_root = table_root.join("_delta_log/")?;

        Ok(Self {
            table_root,
            log_root,
            physical_schema,
            logical_schema,
            dedup_state: FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new()),
            commit_files: Vec::new(),
            checkpoint_files: Vec::new(),
            sidecar_files: Vec::new(),
            checkpoint_type: None,
        })
    }
}

/// Result of scan metadata generation.
///
/// Contains metadata about files to be scanned, produced by the scan state machine.
#[derive(Debug, Clone)]
pub struct ScanMetadataResult {
    /// Table root URL
    pub table_root: Url,
    /// Files to scan (with selection vectors and transforms applied)
    pub files: Vec<FileMeta>,
    /// Physical schema for reading files
    pub physical_schema: SchemaRef,
    /// Logical schema (output schema)
    pub logical_schema: SchemaRef,
}

/// State machine for executing a scan (generating scan metadata).
///
/// Phases:
/// 1. `Commit` - Process commit files with AddRemoveDedup
/// 2. `CheckpointManifest` - Read v2 checkpoint manifest (if applicable)
/// 3. `CheckpointLeaf` - Read checkpoint leaf/sidecar files
/// 4. `Complete` - Scan metadata is ready
///
/// # Example
///
/// ```ignore
/// let sm = ScanStateMachine::new(table_root, physical_schema, logical_schema)?;
/// let result = executor.execute_state_machine(sm)?;
/// // result.files contains the files to scan
/// ```
#[derive(Debug)]
pub struct ScanStateMachine {
    /// Current phase
    phase: ScanPhase,
    /// Accumulated state
    state: ScanBuildState,
}

impl ScanStateMachine {
    /// Create a new scan state machine.
    pub fn new(
        table_root: Url,
        physical_schema: SchemaRef,
        logical_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        let state = ScanBuildState::new(table_root, physical_schema, logical_schema)?;
        let initial_plan = Self::create_commit_plan_static(&state)?;
        Ok(Self {
            phase: ScanPhase::Commit(initial_plan),
            state,
        })
    }

    /// Create from scan configuration (used by Scan::begin()).
    pub fn from_scan_config(
        table_root: Url,
        log_root: Url,
        physical_schema: SchemaRef,
        logical_schema: SchemaRef,
        commit_files: Vec<FileMeta>,
        checkpoint_files: Vec<FileMeta>,
    ) -> DeltaResult<Self> {
        let state = ScanBuildState {
            table_root,
            log_root,
            physical_schema,
            logical_schema,
            dedup_state: FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new()),
            commit_files,
            checkpoint_files,
            sidecar_files: Vec::new(),
            checkpoint_type: None,
        };
        let initial_plan = Self::create_commit_plan_static(&state)?;
        Ok(Self {
            phase: ScanPhase::Commit(initial_plan),
            state,
        })
    }

    /// Get the current phase.
    pub fn phase(&self) -> &ScanPhase {
        &self.phase
    }

    /// Check if terminal.
    pub fn is_terminal(&self) -> bool {
        self.phase.is_complete()
    }

    /// Get the plan for the current phase.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        self.phase
            .as_query_plan()
            .ok_or_else(|| Error::generic("Cannot get plan from Complete state"))
    }

    /// Advance the state machine with the result of plan execution.
    pub fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<ScanMetadataResult>> {
        // Propagate execution errors
        let executed_plan = result?;

        // Extract any KDF state updates from the executed plan
        self.extract_kdf_state(&executed_plan)?;

        match &self.phase {
            ScanPhase::Commit(_) => {
                // Check if we have checkpoint files
                if self.state.checkpoint_files.is_empty() {
                    // No checkpoint - we're done
                    self.phase = ScanPhase::Complete;
                    return Ok(AdvanceResult::Done(self.build_result()));
                }

                // Determine checkpoint type based on the number of checkpoint files
                match self.state.checkpoint_type {
                    Some(CheckpointType::V2) => {
                        // V2 checkpoint (already known) - read manifest first
                        let manifest_plan = self.create_checkpoint_manifest_plan()?;
                        self.phase = ScanPhase::CheckpointManifest(manifest_plan);
                    }
                    Some(CheckpointType::MultiPart) => {
                        // Multi-part checkpoint - go directly to leaf phase
                        let leaf_plan = self.create_checkpoint_leaf_plan()?;
                        self.phase = ScanPhase::CheckpointLeaf(leaf_plan);
                    }
                    Some(CheckpointType::Classic) | None => {
                        // Single-part checkpoint - query schema to detect V2
                        let schema_plan = self.create_schema_query_plan()?;
                        self.phase = ScanPhase::SchemaQuery(schema_plan);
                    }
                }
                Ok(AdvanceResult::Continue)
            }
            ScanPhase::SchemaQuery(_) => {
                // After schema query, check if schema has 'sidecar' column
                let has_sidecar = self.check_schema_has_sidecar(&executed_plan);
                if has_sidecar {
                    // V2 checkpoint with sidecars - read manifest first
                    self.state.checkpoint_type = Some(CheckpointType::V2);
                    let manifest_plan = self.create_checkpoint_manifest_plan()?;
                    self.phase = ScanPhase::CheckpointManifest(manifest_plan);
                } else {
                    // Classic single-part checkpoint - read directly
                    self.state.checkpoint_type = Some(CheckpointType::Classic);
                    let leaf_plan = self.create_checkpoint_leaf_plan()?;
                    self.phase = ScanPhase::CheckpointLeaf(leaf_plan);
                }
                Ok(AdvanceResult::Continue)
            }
            ScanPhase::CheckpointManifest(_) => {
                // After manifest, extract sidecar files and read the checkpoint + sidecars
                self.extract_sidecar_files(&executed_plan)?;
                let leaf_plan = self.create_checkpoint_leaf_plan()?;
                self.phase = ScanPhase::CheckpointLeaf(leaf_plan);
                Ok(AdvanceResult::Continue)
            }
            ScanPhase::CheckpointLeaf(_) => {
                // Complete
                self.phase = ScanPhase::Complete;
                Ok(AdvanceResult::Done(self.build_result()))
            }
            ScanPhase::Complete => Err(Error::generic("Cannot advance from Complete state")),
        }
    }

    fn build_result(&self) -> ScanMetadataResult {
        ScanMetadataResult {
            table_root: self.state.table_root.clone(),
            files: Vec::new(), // Would be populated from accumulated state
            physical_schema: self.state.physical_schema.clone(),
            logical_schema: self.state.logical_schema.clone(),
        }
    }

    fn extract_kdf_state(&mut self, plan: &DeclarativePlanNode) -> DeltaResult<()> {
        // Walk plan tree to extract KDF state updates
        self.extract_kdf_state_recursive(plan)
    }

    fn extract_kdf_state_recursive(&mut self, plan: &DeclarativePlanNode) -> DeltaResult<()> {
        match plan {
            DeclarativePlanNode::FilterByKDF { child, node } => {
                // State is typed - variant encodes the function identity
                match &node.state {
                    FilterKdfState::AddRemoveDedup(_) => {
                        // Update our stored dedup state with the mutated state from execution
                        self.state.dedup_state = node.state.clone();
                    }
                    FilterKdfState::CheckpointDedup(_) => {
                        // Checkpoint dedup state handled separately
                    }
                }
                self.extract_kdf_state_recursive(child)
            }
            // ConsumeByKDF state is handled separately (it's a consumer, not a filter)
            DeclarativePlanNode::ConsumeByKDF { child, .. } => {
                self.extract_kdf_state_recursive(child)
            }
            DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. }
            | DeclarativePlanNode::Sink { child, .. } => {
                self.extract_kdf_state_recursive(child)
            }
            DeclarativePlanNode::Scan(_)
            | DeclarativePlanNode::FileListing(_)
            | DeclarativePlanNode::SchemaQuery(_) => Ok(()),
        }
    }

    // =========================================================================
    // Plan Creation Helpers
    // =========================================================================

    /// Static version of create_commit_plan for use in constructors.
    fn create_commit_plan_static(state: &ScanBuildState) -> DeltaResult<CommitPhasePlan> {
        use crate::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
        use crate::scan::log_replay::{get_add_transform_expr, SCAN_ROW_SCHEMA};

        // Schema for reading add/remove actions from commit files
        // Use the standard schema from actions module which includes all required fields
        let schema = get_commit_schema().project(&[ADD_NAME, REMOVE_NAME])?;

        // Use the standard scan row schema and transform expression from log_replay
        let output_schema = SCAN_ROW_SCHEMA.clone();
        let transform_expr = get_add_transform_expr();

        Ok(CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: state.commit_files.clone(),
                schema,
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::with_state(state.dedup_state.clone()),
            project: SelectNode {
                columns: vec![transform_expr],
                output_schema,
            },
            sink: SinkNode::results(), // Result sink - streams file actions
        })
    }

    fn create_commit_plan(&self) -> DeltaResult<CommitPhasePlan> {
        Self::create_commit_plan_static(&self.state)
    }

    fn create_checkpoint_manifest_plan(&self) -> DeltaResult<CheckpointManifestPlan> {
        use crate::schema::{DataType, StructField, StructType};

        // Schema for v2 checkpoint manifest
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::not_null(
            "sidecar",
            DataType::struct_type_unchecked(vec![
                StructField::not_null("path", DataType::STRING),
                StructField::not_null("sizeInBytes", DataType::LONG),
            ]),
        )]));

        // Output schema for sidecar file paths
        let output_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("sizeInBytes", DataType::LONG),
        ]));

        Ok(CheckpointManifestPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: vec![], // Would be populated with manifest file
                schema,
            },
            project: SelectNode {
                columns: vec![],
                output_schema,
            },
            sink: SinkNode::drop(), // Drop sink - side effect only (collects sidecar paths)
        })
    }

    fn create_checkpoint_leaf_plan(&self) -> DeltaResult<CheckpointLeafPlan> {
        use crate::schema::{DataType, StructField, StructType};

        // Schema for reading add actions from checkpoint files
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "add",
            DataType::struct_type_unchecked(vec![
                StructField::not_null("path", DataType::STRING),
                StructField::not_null("size", DataType::LONG),
            ]),
        )]));

        // Project to output schema
        let output_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
        ]));

        Ok(CheckpointLeafPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: self.state.checkpoint_files.clone(),
                schema,
            },
            dedup_filter: FilterByKDF::checkpoint_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema,
            },
            sink: SinkNode::results(), // Result sink - streams file actions
        })
    }

    fn create_schema_query_plan(&self) -> DeltaResult<SchemaQueryPhasePlan> {
        // Get the first checkpoint file path for schema query
        let file_path = self
            .state
            .checkpoint_files
            .first()
            .map(|f| f.location.to_string())
            .unwrap_or_default();

        Ok(SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store(file_path),
            sink: SinkNode::drop(), // Drop sink - schema query produces no user-facing data
        })
    }

    /// Check if the executed schema query plan's schema contains a 'sidecar' column.
    fn check_schema_has_sidecar(&self, plan: &DeclarativePlanNode) -> bool {
        // Walk the plan to find the SchemaQueryNode and check its state
        match plan {
            DeclarativePlanNode::SchemaQuery(node) => {
                // Check if the schema stored in SchemaStoreState has a 'sidecar' column
                let super::kdf_state::SchemaReaderState::SchemaStore(store) = &node.state;
                if let Some(schema) = store.get() {
                    return schema.field("sidecar").is_some();
                }
                false
            }
            // Recurse into children for other node types
            DeclarativePlanNode::FilterByKDF { child, .. }
            | DeclarativePlanNode::ConsumeByKDF { child, .. }
            | DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. }
            | DeclarativePlanNode::Sink { child, .. } => self.check_schema_has_sidecar(child),
            // Leaf nodes that aren't SchemaQuery
            DeclarativePlanNode::Scan(_) | DeclarativePlanNode::FileListing(_) => false,
        }
    }

    /// Extract sidecar files from the manifest phase's consumer KDF state.
    fn extract_sidecar_files(&mut self, plan: &DeclarativePlanNode) -> DeltaResult<()> {
        match plan {
            DeclarativePlanNode::ConsumeByKDF { node, .. } => {
                if let ConsumerKdfState::SidecarCollector(collector) = &node.state {
                    self.state.sidecar_files = collector.get_sidecar_files().to_vec();
                }
                Ok(())
            }
            // Recurse into children
            DeclarativePlanNode::FilterByKDF { child, .. }
            | DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. }
            | DeclarativePlanNode::Sink { child, .. } => self.extract_sidecar_files(child),
            // Leaf nodes
            DeclarativePlanNode::Scan(_)
            | DeclarativePlanNode::FileListing(_)
            | DeclarativePlanNode::SchemaQuery(_) => Ok(()),
        }
    }
}

impl StateMachine for ScanStateMachine {
    type Result = ScanMetadataResult;

    fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        ScanStateMachine::get_plan(self)
    }

    fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<Self::Result>> {
        ScanStateMachine::advance(self, result)
    }

    fn operation_type(&self) -> OperationType {
        OperationType::Scan
    }

    fn phase_name(&self) -> &'static str {
        self.phase.phase_name()
    }
}

impl From<ScanStateMachine> for AnyStateMachine {
    fn from(sm: ScanStateMachine) -> Self {
        Self::Scan(sm)
    }
}

// =============================================================================
// Declarative State Machine (Abstract/Runtime Representation)
// =============================================================================

/// Operation type for state machines.
///
/// Identifies which high-level operation this state machine is executing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Building a table snapshot
    SnapshotBuild,
    /// Executing a scan operation
    Scan,
}

/// Phase type within an operation.
///
/// Identifies what stage of the operation we're in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseType {
    /// Processing commit files (JSON log)
    Commit,
    /// Querying checkpoint schema to detect V2 checkpoints
    SchemaQuery,
    /// Reading checkpoint manifest (v2 checkpoints)
    CheckpointManifest,
    /// Reading checkpoint leaf/sidecar files
    CheckpointLeaf,
    /// Listing log files
    ListFiles,
    /// Loading protocol and metadata
    LoadMetadata,
    /// Operation complete - terminal state
    Complete,
    /// Snapshot/Scan ready - terminal state with data
    Ready,
    /// Reading checkpoint hint file (_last_checkpoint)
    CheckpointHint,
}

/// Declarative state machine phase - abstract representation for runtime interpretation.
///
/// This enum allows engines to interpret state machines at runtime without
/// knowing the specific typed state machine. Engines can use pattern matching
/// on `operation` and `phase_type` to determine behavior.
///
/// # Example
///
/// ```ignore
/// fn drive_state_machine(phase: DeclarativePhase) -> Result<()> {
///     loop {
///         if phase.is_terminal() {
///             return Ok(());
///         }
///
///         let plan = phase.query_plan().expect("Non-terminal has plan");
///         let result = execute_plan(plan);
///         phase = phase.advance(result)?;
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct DeclarativePhase {
    /// Which operation this state machine is for
    pub operation: OperationType,
    /// Current phase within the operation
    pub phase_type: PhaseType,
    /// Query plan for this phase (None if terminal)
    pub query_plan: Option<DeclarativePlanNode>,
    /// Terminal data if this is a terminal state (e.g., snapshot data)
    pub terminal_data: Option<TerminalData>,
}

/// Data returned when a state machine reaches a terminal state.
#[derive(Debug, Clone)]
pub enum TerminalData {
    /// Snapshot is ready
    SnapshotReady {
        version: i64,
        table_schema: SchemaRef,
    },
    /// Scan completed
    ScanComplete,
}

impl DeclarativePhase {
    /// Check if this is a terminal state (Complete or Ready).
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase_type, PhaseType::Complete | PhaseType::Ready)
    }

    /// Get the phase name for debugging/logging.
    pub fn phase_name(&self) -> &'static str {
        match self.phase_type {
            PhaseType::Commit => "Commit",
            PhaseType::SchemaQuery => "SchemaQuery",
            PhaseType::CheckpointManifest => "CheckpointManifest",
            PhaseType::CheckpointLeaf => "CheckpointLeaf",
            PhaseType::ListFiles => "ListFiles",
            PhaseType::LoadMetadata => "LoadMetadata",
            PhaseType::Complete => "Complete",
            PhaseType::Ready => "Ready",
            PhaseType::CheckpointHint => "CheckpointHint",
        }
    }

    /// Get the operation name for debugging/logging.
    pub fn operation_name(&self) -> &'static str {
        match self.operation {
            OperationType::SnapshotBuild => "SnapshotBuild",
            OperationType::Scan => "Scan",
        }
    }
}

/// Trait for converting typed state machines to abstract representation.
///
/// This allows engines to choose between:
/// - Using typed state machines directly (compile-time known structure)
/// - Converting to `DeclarativePhase` for generic state machine execution
pub trait AsDeclarativePhase {
    /// Convert this state machine to a `DeclarativePhase`.
    fn as_declarative_phase(&self) -> DeclarativePhase;
}

// =============================================================================
// Snapshot Build State Machine (Typed/Compile-time)
// =============================================================================

/// State machine for building a table snapshot.
///
/// Phases:
/// 1. `ListFiles` - List log files to find latest version
/// 2. `LoadMetadata` - Load protocol and metadata
/// 3. `Ready` - Snapshot ready for use
#[derive(Debug, Clone)]
pub enum SnapshotBuildPhase {
    /// Listing log files to find versions
    ListFiles(FileListingPhasePlan),
    /// Loading protocol and table metadata
    LoadMetadata(MetadataLoadPlan),
    /// Snapshot ready
    Ready(SnapshotData),
}

/// Data for a ready snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotData {
    /// Table version
    pub version: i64,
    /// Table schema
    pub table_schema: SchemaRef,
    // Additional metadata as needed
}

impl SnapshotBuildPhase {
    /// Get the query plan for the current phase (if not ready).
    pub fn as_query_plan(&self) -> Option<DeclarativePlanNode> {
        match self {
            Self::ListFiles(p) => Some(p.as_query_plan()),
            Self::LoadMetadata(p) => Some(p.as_query_plan()),
            Self::Ready(_) => None,
        }
    }

    /// Check if this is a terminal state.
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    /// Get the phase name for debugging/logging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::ListFiles(_) => "ListFiles",
            Self::LoadMetadata(_) => "LoadMetadata",
            Self::Ready(_) => "Ready",
        }
    }
}

impl AsDeclarativePhase for SnapshotBuildPhase {
    fn as_declarative_phase(&self) -> DeclarativePhase {
        match self {
            Self::ListFiles(plan) => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::ListFiles,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::LoadMetadata(plan) => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::LoadMetadata,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::Ready(data) => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::Ready,
                query_plan: None,
                terminal_data: Some(TerminalData::SnapshotReady {
                    version: data.version,
                    table_schema: data.table_schema.clone(),
                }),
            },
        }
    }
}

// =============================================================================
// Phase Data (Dual Representation for FFI)
// =============================================================================

/// Wrapper providing both typed plan and tree representation.
///
/// This is what gets serialized to protobuf for cross-language use.
/// Engines can use either the typed plan or the tree.
#[derive(Debug, Clone)]
pub struct PhaseData<P: AsQueryPlan + Clone> {
    /// Typed plan (compile-time structure)
    pub plan: P,
    /// Tree representation (runtime interpretation)
    query_plan: Option<DeclarativePlanNode>,
}

impl<P: AsQueryPlan + Clone> PhaseData<P> {
    /// Create phase data from a typed plan.
    pub fn new(plan: P) -> Self {
        Self {
            plan,
            query_plan: None,
        }
    }

    /// Get the query plan (lazily computed).
    pub fn query_plan(&self) -> DeclarativePlanNode {
        self.plan.as_query_plan()
    }
}

// =============================================================================
// Proto Conversion for Declarative Phase
// =============================================================================

impl From<OperationType> for i32 {
    fn from(op: OperationType) -> i32 {
        match op {
            OperationType::SnapshotBuild => proto::OperationType::SnapshotBuild as i32,
            OperationType::Scan => proto::OperationType::Scan as i32,
        }
    }
}

impl From<PhaseType> for i32 {
    fn from(pt: PhaseType) -> i32 {
        match pt {
            PhaseType::Commit => proto::PhaseType::Commit as i32,
            PhaseType::SchemaQuery => proto::PhaseType::SchemaQuery as i32,
            PhaseType::CheckpointManifest => proto::PhaseType::CheckpointManifest as i32,
            PhaseType::CheckpointLeaf => proto::PhaseType::CheckpointLeaf as i32,
            PhaseType::ListFiles => proto::PhaseType::ListFiles as i32,
            PhaseType::LoadMetadata => proto::PhaseType::LoadMetadata as i32,
            PhaseType::Complete => proto::PhaseType::Complete as i32,
            PhaseType::Ready => proto::PhaseType::Ready as i32,
            PhaseType::CheckpointHint => proto::PhaseType::CheckpointHint as i32,
        }
    }
}

impl From<&DeclarativePhase> for proto::DeclarativePhase {
    fn from(phase: &DeclarativePhase) -> Self {
        let terminal_data = phase.terminal_data.as_ref().map(|td| {
            use proto::terminal_data::Data;
            let data = match td {
                TerminalData::SnapshotReady {
                    version,
                    table_schema: _,
                } => {
                    Data::SnapshotReady(proto::SnapshotReady {
                        version: *version,
                        table_schema: None, // TODO: implement schema conversion
                    })
                }
                TerminalData::ScanComplete => {
                    // Use LogReplayComplete proto message for ScanComplete (for backwards compatibility)
                    Data::LogReplayComplete(proto::LogReplayComplete {})
                }
            };
            proto::TerminalData { data: Some(data) }
        });

        proto::DeclarativePhase {
            operation: phase.operation.into(),
            phase_type: phase.phase_type.into(),
            query_plan: phase.query_plan.as_ref().map(|p| p.into()),
            terminal_data,
        }
    }
}
