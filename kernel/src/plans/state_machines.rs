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

use std::collections::HashSet;
use std::sync::Arc;

use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::listed_log_files::ListedLogFiles;
use crate::log_replay::FileActionKey;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::proto_generated as proto;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::FileMeta;
use crate::{DeltaResult, Error};

use super::composite::*;
use super::declarative::DeclarativePlanNode;
use super::nodes::{FileType, FilterByKDF, FilterFilterKernelFunctionId, ScanNode, SelectNode};
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
    /// The executed plan contains mutated KDF state (via state_ptr in FilterByKDF).
    /// The state machine extracts this state and transitions to the next phase.
    ///
    /// # Returns
    /// - `Ok(Continue)` - Advanced to next phase, call `get_plan()` for next plan
    /// - `Ok(Done(result))` - State machine completed, here's the result
    /// - `Err(_)` - Error during advancement
    fn advance(&mut self, result: DeltaResult<DeclarativePlanNode>) -> DeltaResult<AdvanceResult<Self::Result>>;

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
    LogReplay(LogReplayStateMachine),
    Snapshot(SnapshotStateMachine),
    Scan(ScanStateMachine),
    // Future:
    // Write(WriteStateMachine),
}

/// Type-erased result from any state machine.
pub enum AnyResult {
    LogReplay(LogReplayResult),
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
            Self::LogReplay(sm) => sm.get_plan(),
            Self::Snapshot(sm) => sm.get_plan(),
            Self::Scan(sm) => sm.get_plan(),
        }
    }

    /// Check if the state machine has reached a terminal state.
    ///
    /// Convenience method for FFI callers to check state before calling get_plan().
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::LogReplay(sm) => sm.is_terminal(),
            Self::Snapshot(sm) => sm.is_terminal(),
            Self::Scan(sm) => sm.is_terminal(),
        }
    }

    /// Advance with the result of plan execution.
    pub fn advance(&mut self, result: DeltaResult<DeclarativePlanNode>) -> DeltaResult<AdvanceResult<AnyResult>> {
        match self {
            Self::LogReplay(sm) => {
                let advance_result = sm.advance(result)?;
                Ok(match advance_result {
                    AdvanceResult::Continue => AdvanceResult::Continue,
                    AdvanceResult::Done(r) => AdvanceResult::Done(AnyResult::LogReplay(r)),
                })
            }
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
            Self::LogReplay(sm) => sm.operation_type(),
            Self::Snapshot(sm) => sm.operation_type(),
            Self::Scan(sm) => sm.operation_type(),
        }
    }

    /// Get the phase name for debugging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::LogReplay(sm) => sm.phase_name(),
            Self::Snapshot(sm) => sm.phase_name(),
            Self::Scan(sm) => sm.phase_name(),
        }
    }
}

impl From<LogReplayStateMachine> for AnyStateMachine {
    fn from(sm: LogReplayStateMachine) -> Self {
        Self::LogReplay(sm)
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

/// Internal state accumulated across phases during log replay.
///
/// This is opaque to the engine - they only see the state machine handle.
#[derive(Debug)]
pub struct LogReplayState {
    /// Dedup set built during commit phase - tracks seen file keys
    pub seen_file_keys: HashSet<FileActionKey>,
    /// Checkpoint files discovered during listing
    pub checkpoint_files: Vec<FileMeta>,
    /// Sidecar files for v2 checkpoints
    pub sidecar_files: Vec<FileMeta>,
    /// Checkpoint type (determined from schema query)
    pub checkpoint_type: Option<CheckpointType>,
    /// Table path
    pub table_path: Url,
    /// Target version (None = latest)
    pub target_version: Option<i64>,
}

impl LogReplayState {
    /// Create new state for a table.
    pub fn new(table_path: Url) -> Self {
        Self {
            seen_file_keys: HashSet::new(),
            checkpoint_files: Vec::new(),
            sidecar_files: Vec::new(),
            checkpoint_type: None,
            table_path,
            target_version: None,
        }
    }
}

/// Result of log replay - the file actions to scan.
#[derive(Debug, Clone)]
pub struct LogReplayResult {
    /// Files to read for the scan
    pub files: Vec<FileMeta>,
    /// Table version
    pub version: i64,
}

/// Opaque state machine for log replay.
///
/// This is the handle-based API that engines use. Internal state is hidden.
/// Engines drive a simple loop: get_plan → execute → advance → repeat until terminal.
#[derive(Debug)]
pub struct LogReplayStateMachine {
    /// Current phase (typed enum)
    phase: LogReplayPhase,
    /// Accumulated internal state
    state: LogReplayState,
    /// Result when complete
    result: Option<LogReplayResult>,
}

impl LogReplayStateMachine {
    /// Create a new log replay state machine for a table.
    pub fn new(table_path: Url) -> Self {
        let state = LogReplayState::new(table_path.clone());
        
        // Start with commit phase - create initial plan
        let initial_plan = Self::create_commit_plan(&state);
        
        Self {
            phase: LogReplayPhase::Commit(initial_plan),
            state,
            result: None,
        }
    }

    /// Get the current plan to execute.
    /// Returns an error if called on a terminal state machine.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        self.phase.as_query_plan().ok_or_else(|| {
            Error::generic("Cannot get plan from terminal state machine")
        })
    }

    /// Check if the state machine has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        self.phase.is_complete()
    }

    /// Get the current phase name for debugging.
    pub fn phase_name(&self) -> &'static str {
        self.phase.phase_name()
    }

    /// Advance the state machine with the result of plan execution.
    ///
    /// The engine calls this after executing the plan returned by `get_plan()`.
    /// The executed plan is passed back containing mutated KDF state (via state_ptr in FilterByKDF).
    /// The kernel extracts the updated state and transitions to the next phase.
    ///
    /// # Arguments
    /// - `result`: The result of executing the plan. On success, contains the plan with
    ///   mutated KDF state. On error, the error propagates and the state machine remains
    ///   in its current phase.
    ///
    /// # Example
    /// ```ignore
    /// let plan = state_machine.get_plan()?;
    /// let executed_plan = engine.execute(plan)?;
    /// match state_machine.advance(Ok(executed_plan))? {
    ///     AdvanceResult::Continue => { /* keep going */ }
    ///     AdvanceResult::Done(result) => { /* done! */ }
    /// }
    /// ```
    pub fn advance(&mut self, result: DeltaResult<DeclarativePlanNode>) -> DeltaResult<AdvanceResult<LogReplayResult>> {
        // Propagate execution errors
        let executed_plan = result?;
        
        // Extract KDF state from the executed plan
        self.extract_and_update_kdf_state(&executed_plan)?;
        
        // Transition to next phase
        match &self.phase {
            LogReplayPhase::Commit(_) => {
                // After commit phase, check if we have checkpoint files
                if self.state.checkpoint_files.is_empty() {
                    // No checkpoint - we're done
                    self.phase = LogReplayPhase::Complete;
                    let result = LogReplayResult {
                        files: vec![], // TODO: collect files from state
                        version: self.state.target_version.unwrap_or(0),
                    };
                    return Ok(AdvanceResult::Done(result));
                } else {
                    // Have checkpoint - determine type and proceed
                    match self.state.checkpoint_type {
                        Some(CheckpointType::V2) => {
                            // V2 checkpoint - read manifest first
                            let manifest_plan = Self::create_checkpoint_manifest_plan(&self.state);
                            self.phase = LogReplayPhase::CheckpointManifest(manifest_plan);
                        }
                        _ => {
                            // Classic/MultiPart - read checkpoint directly
                            let leaf_plan = Self::create_checkpoint_leaf_plan(&self.state);
                            self.phase = LogReplayPhase::CheckpointLeaf(leaf_plan);
                        }
                    }
                }
            }
            LogReplayPhase::CheckpointManifest(_) => {
                // After manifest, read the sidecars
                let leaf_plan = Self::create_checkpoint_leaf_plan(&self.state);
                self.phase = LogReplayPhase::CheckpointLeaf(leaf_plan);
            }
            LogReplayPhase::CheckpointLeaf(_) => {
                // After checkpoint leaf, we're done
                self.phase = LogReplayPhase::Complete;
                let result = LogReplayResult {
                    files: vec![], // TODO: collect files from state
                    version: self.state.target_version.unwrap_or(0),
                };
                return Ok(AdvanceResult::Done(result));
            }
            LogReplayPhase::Complete => {
                return Err(Error::generic("Cannot advance from Complete state"));
            }
        }
        Ok(AdvanceResult::Continue)
    }

    /// Extract KDF state from the executed plan and update internal state.
    ///
    /// Walks the plan tree to find FilterByKDF nodes with KDF state and extracts
    /// the mutated state back into the state machine's internal state.
    fn extract_and_update_kdf_state(&mut self, plan: &DeclarativePlanNode) -> DeltaResult<()> {
        // Walk the plan tree to find Filter nodes with state
        self.extract_kdf_state_recursive(plan)
    }

    fn extract_kdf_state_recursive(&mut self, plan: &DeclarativePlanNode) -> DeltaResult<()> {
        match plan {
            DeclarativePlanNode::FilterByKDF { child, node } => {
                // Extract KDF state based on function_id
                if node.state_ptr != 0 {
                    match node.function_id {
                        FilterKernelFunctionId::AddRemoveDedup => {
                            // The dedup state (HashSet) is still owned by the state_ptr
                            // We can access it via the KDF registry if needed
                            // For now, the state persists in the pointer
                        }
                        FilterKernelFunctionId::CheckpointDedup => {
                            // Similar handling for checkpoint dedup
                        }
                    }
                }
                // Recurse into child
                self.extract_kdf_state_recursive(child)
            }
            // Recursively check children
            DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. } => {
                self.extract_kdf_state_recursive(child)
            }
            // Leaf nodes have no KDF state
            DeclarativePlanNode::Scan(_) | DeclarativePlanNode::FileListing(_) | DeclarativePlanNode::SchemaQuery(_) => Ok(()),
        }
    }

    /// Get the result (only valid when terminal).
    pub fn get_result(&self) -> Option<&LogReplayResult> {
        self.result.as_ref()
    }

    /// Get the typed phase (for compile-time access).
    pub fn as_phase(&self) -> &LogReplayPhase {
        &self.phase
    }

    /// Get the declarative phase (for runtime inspection).
    pub fn as_declarative_phase(&self) -> DeclarativePhase {
        self.phase.as_declarative_phase()
    }

    /// Get mutable access to internal state (for KDF callbacks).
    pub fn state_mut(&mut self) -> &mut LogReplayState {
        &mut self.state
    }

    // =========================================================================
    // Plan Creation Helpers
    // =========================================================================

    fn create_commit_plan(state: &LogReplayState) -> CommitPhasePlan {
        // Create empty schema - will be populated from actual files
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        
        // Create dedup state
        let dedup_state_ptr = super::filter_kdf_create_state(FilterKernelFunctionId::AddRemoveDedup)
            .expect("Failed to create AddRemoveDedup state");

        CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![], // Engine will populate from listing
                schema: schema.clone(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: FilterKernelFunctionId::AddRemoveDedup,
                state_ptr: dedup_state_ptr,
                serialized_state: None,
            },
            project: SelectNode {
                columns: vec![],
                output_schema: schema,
            },
        }
    }

    fn create_checkpoint_manifest_plan(_state: &LogReplayState) -> CheckpointManifestPlan {
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        
        CheckpointManifestPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: vec![],
                schema: schema.clone(),
            },
            project: SelectNode {
                columns: vec![],
                output_schema: schema,
            },
        }
    }

    fn create_checkpoint_leaf_plan(state: &LogReplayState) -> CheckpointLeafPlan {
        let schema = Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        
        // Create dedup state for checkpoint
        let dedup_state_ptr = super::filter_kdf_create_state(FilterKernelFunctionId::CheckpointDedup)
            .expect("Failed to create CheckpointDedup state");

        CheckpointLeafPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: state.checkpoint_files.clone(),
                schema: schema.clone(),
            },
            dedup_filter: Some(FilterByKDF {
                function_id: FilterKernelFunctionId::CheckpointDedup,
                state_ptr: dedup_state_ptr,
                serialized_state: None,
            }),
            project: SelectNode {
                columns: vec![],
                output_schema: schema,
            },
        }
    }
}

// Implement the StateMachine trait for LogReplayStateMachine
impl StateMachine for LogReplayStateMachine {
    type Result = LogReplayResult;

    fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        LogReplayStateMachine::get_plan(self)
    }

    fn advance(&mut self, result: DeltaResult<DeclarativePlanNode>) -> DeltaResult<AdvanceResult<Self::Result>> {
        // Call the inherent method
        LogReplayStateMachine::advance(self, result)
    }

    fn operation_type(&self) -> OperationType {
        OperationType::LogReplay
    }

    fn phase_name(&self) -> &'static str {
        self.phase.phase_name()
    }
}

// =============================================================================
// Snapshot State Machine
// =============================================================================

/// Phase of the snapshot state machine.
#[derive(Debug, Clone)]
pub enum SnapshotPhase {
    /// Read _last_checkpoint hint file
    CheckpointHint,
    /// List files in _delta_log directory  
    ListFiles,
    /// Read protocol and metadata from log files
    LoadMetadata,
    /// Snapshot is ready
    Complete,
}

impl SnapshotPhase {
    fn phase_name(&self) -> &'static str {
        match self {
            Self::CheckpointHint => "CheckpointHint",
            Self::ListFiles => "ListFiles",
            Self::LoadMetadata => "LoadMetadata",
            Self::Complete => "Complete",
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
    /// Final version discovered
    pub final_version: Option<i64>,
    
    // LogSegment data accumulated during ListFiles phase
    /// Sorted commit files in the log segment (ascending)
    pub ascending_commit_files: Vec<ParsedLogPath>,
    /// Sorted compaction files in the log segment (ascending)
    pub ascending_compaction_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment
    pub checkpoint_parts: Vec<ParsedLogPath>,
    /// Latest CRC (checksum) file
    pub latest_crc_file: Option<ParsedLogPath>,
    /// Latest commit file (may not be in contiguous segment)
    pub latest_commit_file: Option<ParsedLogPath>,
    
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
            final_version: None,
            ascending_commit_files: Vec::new(),
            ascending_compaction_files: Vec::new(),
            checkpoint_parts: Vec::new(),
            latest_crc_file: None,
            latest_commit_file: None,
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
        Ok(Self {
            phase: SnapshotPhase::CheckpointHint,
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
        match &self.phase {
            SnapshotPhase::CheckpointHint => self.create_checkpoint_hint_plan(),
            SnapshotPhase::ListFiles => self.create_list_files_plan(),
            SnapshotPhase::LoadMetadata => self.create_load_metadata_plan(),
            SnapshotPhase::Complete => {
                Err(Error::generic("Cannot get plan from Complete state"))
            }
        }
    }

    /// Advance the state machine with the result of plan execution.
    pub fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<Snapshot>> {
        // Propagate execution errors
        let _executed_plan = result?;

        match &self.phase {
            SnapshotPhase::CheckpointHint => {
                // Move to ListFiles phase
                self.phase = SnapshotPhase::ListFiles;
                Ok(AdvanceResult::Continue)
            }
            SnapshotPhase::ListFiles => {
                // Move to LoadMetadata phase
                self.phase = SnapshotPhase::LoadMetadata;
                Ok(AdvanceResult::Continue)
            }
            SnapshotPhase::LoadMetadata => {
                // Complete - build the full Snapshot
                self.phase = SnapshotPhase::Complete;

                // Build LogSegment from accumulated state
                let listed_files = ListedLogFiles {
                    ascending_commit_files: std::mem::take(&mut self.state.ascending_commit_files),
                    ascending_compaction_files: std::mem::take(&mut self.state.ascending_compaction_files),
                    checkpoint_parts: std::mem::take(&mut self.state.checkpoint_parts),
                    latest_crc_file: self.state.latest_crc_file.take(),
                    latest_commit_file: self.state.latest_commit_file.take(),
                };

                let end_version = self.state.final_version.map(|v| v as u64);
                let log_segment = LogSegment::try_new(
                    listed_files,
                    self.state.log_root.clone(),
                    end_version,
                )?;

                // Build TableConfiguration from protocol/metadata
                let metadata = self.state.metadata.take().ok_or_else(|| {
                    Error::MissingMetadata
                })?;
                let protocol = self.state.protocol.take().ok_or_else(|| {
                    Error::MissingProtocol
                })?;
                
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
            SnapshotPhase::Complete => {
                Err(Error::generic("Cannot advance from Complete state"))
            }
        }
    }

    // =========================================================================
    // Plan Creation Helpers
    // =========================================================================

    fn create_checkpoint_hint_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        use crate::schema::{DataType, StructField, StructType};

        let checkpoint_path = self.state.log_root.join("_last_checkpoint")?;
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

        Ok(DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![file_meta],
            schema: checkpoint_schema,
        }))
    }

    fn create_list_files_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        use super::nodes::FileListingNode;

        // List files in the _delta_log directory
        Ok(DeclarativePlanNode::FileListing(FileListingNode {
            path: self.state.log_root.clone(),
        }))
    }

    fn create_load_metadata_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        use crate::schema::{DataType, StructField, StructType};

        // For now, create an empty scan - the actual implementation
        // would scan the appropriate commit/checkpoint files
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("protocol", DataType::struct_type_unchecked(vec![
                StructField::not_null("minReaderVersion", DataType::INTEGER),
                StructField::not_null("minWriterVersion", DataType::INTEGER),
            ])),
            StructField::nullable("metaData", DataType::struct_type_unchecked(vec![
                StructField::not_null("id", DataType::STRING),
                StructField::nullable("schemaString", DataType::STRING),
            ])),
        ]));

        Ok(DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![], // Would be populated with actual log files
            schema,
        }))
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

/// Phase of the scan state machine.
#[derive(Debug, Clone)]
pub enum ScanPhase {
    /// Process commit files (JSON log)
    Commit,
    /// Read checkpoint manifest (v2 checkpoints)
    CheckpointManifest,
    /// Read checkpoint leaf/sidecar files
    CheckpointLeaf,
    /// Scan is complete
    Complete,
}

impl ScanPhase {
    fn phase_name(&self) -> &'static str {
        match self {
            Self::Commit => "Commit",
            Self::CheckpointManifest => "CheckpointManifest",
            Self::CheckpointLeaf => "CheckpointLeaf",
            Self::Complete => "Complete",
        }
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
    /// Dedup state pointer for AddRemoveDedup KDF
    pub dedup_state_ptr: u64,
    /// Files discovered during commit phase
    pub commit_files: Vec<FileMeta>,
    /// Checkpoint files to read
    pub checkpoint_files: Vec<FileMeta>,
    /// Whether we have v2 checkpoints
    pub has_v2_checkpoint: bool,
}

impl ScanBuildState {
    fn new(table_root: Url, physical_schema: SchemaRef, logical_schema: SchemaRef) -> DeltaResult<Self> {
        let log_root = table_root.join("_delta_log/")?;
        
        // Create dedup state for AddRemoveDedup filter
        let dedup_state_ptr = super::function_registry::filter_kdf_create_state(FilterKernelFunctionId::AddRemoveDedup)?;
        
        Ok(Self {
            table_root,
            log_root,
            physical_schema,
            logical_schema,
            dedup_state_ptr,
            commit_files: Vec::new(),
            checkpoint_files: Vec::new(),
            has_v2_checkpoint: false,
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
        Ok(Self {
            phase: ScanPhase::Commit,
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
        let dedup_state_ptr = super::function_registry::filter_kdf_create_state(FilterKernelFunctionId::AddRemoveDedup)?;
        
        Ok(Self {
            phase: ScanPhase::Commit,
            state: ScanBuildState {
                table_root,
                log_root,
                physical_schema,
                logical_schema,
                dedup_state_ptr,
                commit_files,
                checkpoint_files,
                has_v2_checkpoint: false,
            },
        })
    }

    /// Get the current phase.
    pub fn phase(&self) -> &ScanPhase {
        &self.phase
    }

    /// Check if terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase, ScanPhase::Complete)
    }

    /// Get the plan for the current phase.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        match &self.phase {
            ScanPhase::Commit => self.create_commit_plan(),
            ScanPhase::CheckpointManifest => self.create_checkpoint_manifest_plan(),
            ScanPhase::CheckpointLeaf => self.create_checkpoint_leaf_plan(),
            ScanPhase::Complete => {
                Err(Error::generic("Cannot get plan from Complete state"))
            }
        }
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
            ScanPhase::Commit => {
                // Check if we have checkpoint files
                if self.state.checkpoint_files.is_empty() {
                    // No checkpoint - we're done
                    self.phase = ScanPhase::Complete;
                    return Ok(AdvanceResult::Done(self.build_result()));
                }

                // Check for v2 checkpoints
                if self.state.has_v2_checkpoint {
                    self.phase = ScanPhase::CheckpointManifest;
                } else {
                    self.phase = ScanPhase::CheckpointLeaf;
                }
                Ok(AdvanceResult::Continue)
            }
            ScanPhase::CheckpointManifest => {
                // After manifest, read leaf files
                self.phase = ScanPhase::CheckpointLeaf;
                Ok(AdvanceResult::Continue)
            }
            ScanPhase::CheckpointLeaf => {
                // Complete
                self.phase = ScanPhase::Complete;
                Ok(AdvanceResult::Done(self.build_result()))
            }
            ScanPhase::Complete => {
                Err(Error::generic("Cannot advance from Complete state"))
            }
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
                if node.state_ptr != 0 && node.function_id == FilterKernelFunctionId::AddRemoveDedup {
                    // Update our stored dedup state pointer
                    self.state.dedup_state_ptr = node.state_ptr;
                }
                self.extract_kdf_state_recursive(child)
            }
            DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. } => {
                self.extract_kdf_state_recursive(child)
            }
            DeclarativePlanNode::Scan(_) | DeclarativePlanNode::FileListing(_) | DeclarativePlanNode::SchemaQuery(_) => Ok(()),
        }
    }

    // =========================================================================
    // Plan Creation Helpers
    // =========================================================================

    fn create_commit_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        use crate::schema::{DataType, MapType, StructField, StructType};

        // Schema for reading add/remove actions from commit files
        let partition_values_type: DataType = MapType::new(DataType::STRING, DataType::STRING, true).into();
        let add_schema = DataType::struct_type_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("partitionValues", partition_values_type),
            StructField::not_null("size", DataType::LONG),
            StructField::nullable("modificationTime", DataType::LONG),
            StructField::nullable("dataChange", DataType::BOOLEAN),
            StructField::nullable("stats", DataType::STRING),
        ]);

        let remove_schema = DataType::struct_type_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("deletionTimestamp", DataType::LONG),
            StructField::nullable("dataChange", DataType::BOOLEAN),
        ]);

        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("add", add_schema),
            StructField::nullable("remove", remove_schema),
        ]));

        // Create scan node
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: self.state.commit_files.clone(),
            schema: schema.clone(),
        });

        // Add dedup KDF filter
        let filter = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF {
                function_id: FilterKernelFunctionId::AddRemoveDedup,
                state_ptr: self.state.dedup_state_ptr,
                serialized_state: None,
            },
        };

        // Project to output schema
        let output_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
        ]));

        Ok(DeclarativePlanNode::Select {
            child: Box::new(filter),
            node: SelectNode {
                columns: vec![],
                output_schema,
            },
        })
    }

    fn create_checkpoint_manifest_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        use crate::schema::{DataType, StructField, StructType};

        // Schema for v2 checkpoint manifest
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("sidecar", DataType::struct_type_unchecked(vec![
                StructField::not_null("path", DataType::STRING),
                StructField::not_null("sizeInBytes", DataType::LONG),
            ])),
        ]));

        Ok(DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![], // Would be populated with manifest file
            schema,
        }))
    }

    fn create_checkpoint_leaf_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        use crate::schema::{DataType, StructField, StructType};

        // Schema for reading add actions from checkpoint files
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("add", DataType::struct_type_unchecked(vec![
                StructField::not_null("path", DataType::STRING),
                StructField::not_null("size", DataType::LONG),
            ])),
        ]));

        // Create dedup state for checkpoint
        let checkpoint_dedup_state = super::function_registry::filter_kdf_create_state(
            FilterKernelFunctionId::CheckpointDedup
        )?;

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: self.state.checkpoint_files.clone(),
            schema: schema.clone(),
        });

        // Add checkpoint dedup KDF filter
        let filter = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF {
                function_id: FilterKernelFunctionId::CheckpointDedup,
                state_ptr: checkpoint_dedup_state,
                serialized_state: None,
            },
        };

        // Project
        let output_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
        ]));

        Ok(DeclarativePlanNode::Select {
            child: Box::new(filter),
            node: SelectNode {
                columns: vec![],
                output_schema,
            },
        })
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
    /// Replaying transaction log to build file list
    LogReplay,
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
    /// Log replay completed - no data needed
    LogReplayComplete,
    /// Snapshot is ready
    SnapshotReady {
        version: i64,
        table_schema: SchemaRef,
    },
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
            PhaseType::CheckpointManifest => "CheckpointManifest",
            PhaseType::CheckpointLeaf => "CheckpointLeaf",
            PhaseType::ListFiles => "ListFiles",
            PhaseType::LoadMetadata => "LoadMetadata",
            PhaseType::Complete => "Complete",
            PhaseType::Ready => "Ready",
        }
    }

    /// Get the operation name for debugging/logging.
    pub fn operation_name(&self) -> &'static str {
        match self.operation {
            OperationType::LogReplay => "LogReplay",
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
// Log Replay State Machine (Typed/Compile-time)
// =============================================================================

/// State machine for replaying the transaction log.
///
/// Phases:
/// 1. `Commit` - Process commit files (JSON log)
/// 2. `CheckpointManifest` - Read v2 checkpoint manifest (if present)
/// 3. `CheckpointLeaf` - Read checkpoint parquet files
/// 4. `Complete` - Log replay finished
#[derive(Debug, Clone)]
pub enum LogReplayPhase {
    /// Processing commit files (JSON log files)
    Commit(CommitPhasePlan),
    /// Reading checkpoint manifest (v2 checkpoints)
    CheckpointManifest(CheckpointManifestPlan),
    /// Reading checkpoint leaf/sidecar files
    CheckpointLeaf(CheckpointLeafPlan),
    /// Log replay complete
    Complete,
}

impl LogReplayPhase {
    /// Get the query plan for the current phase (if not complete).
    pub fn as_query_plan(&self) -> Option<DeclarativePlanNode> {
        match self {
            Self::Commit(p) => Some(p.as_query_plan()),
            Self::CheckpointManifest(p) => Some(p.as_query_plan()),
            Self::CheckpointLeaf(p) => Some(p.as_query_plan()),
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
            Self::Commit(_) => "Commit",
            Self::CheckpointManifest(_) => "CheckpointManifest",
            Self::CheckpointLeaf(_) => "CheckpointLeaf",
            Self::Complete => "Complete",
        }
    }

    /// Advance the state machine with the result of plan execution.
    ///
    /// Takes `DeltaResult<DeclarativePlanNode>` where the plan contains updated state
    /// (e.g., dedup state in FilterByKDF.state_ptr). On success, extracts state from
    /// the plan and generates the next phase. On error, propagates the error.
    ///
    /// # Arguments
    /// - `result`: The result of executing the current phase's plan
    ///
    /// # Returns
    /// The next phase of the state machine, or an error
    pub fn advance(self, result: DeltaResult<DeclarativePlanNode>) -> DeltaResult<Self> {
        let plan = result?; // Propagate error
        
        // Extract updated state from the plan
        let updated_state = self.extract_kdf_state(&plan)?;
        
        // Generate next phase based on current phase and updated state
        self.generate_next_phase(updated_state)
    }

    /// Extract KDF state from the executed plan.
    ///
    /// Walks the plan tree to find FilterByKDF with state_ptr and extracts it.
    fn extract_kdf_state(&self, plan: &DeclarativePlanNode) -> DeltaResult<Option<(FilterKernelFunctionId, u64)>> {
        // Walk the plan tree to find FilterByKDF nodes with state
        match plan {
            DeclarativePlanNode::FilterByKDF { child: _, node } => {
                if node.state_ptr != 0 {
                    Ok(Some((node.function_id, node.state_ptr)))
                } else {
                    Ok(None)
                }
            }
            // Recursively check children
            DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. } => {
                self.extract_kdf_state(child)
            }
            // Leaf nodes have no KDF state
            DeclarativePlanNode::Scan(_) | DeclarativePlanNode::FileListing(_) | DeclarativePlanNode::SchemaQuery(_) => Ok(None),
        }
    }

    /// Generate the next phase based on current state and extracted KDF state.
    fn generate_next_phase(self, _updated_state: Option<(FilterKernelFunctionId, u64)>) -> DeltaResult<Self> {
        match self {
            Self::Commit(_) => {
                // After commit phase, move to checkpoint manifest or leaf
                // TODO: Implement proper transition logic based on checkpoint type
                // For now, go directly to Complete
                Ok(Self::Complete)
            }
            Self::CheckpointManifest(_) => {
                // After manifest, move to checkpoint leaf
                // TODO: Implement proper transition with checkpoint files
                Ok(Self::Complete)
            }
            Self::CheckpointLeaf(_) => {
                // After checkpoint leaf, we're done
                Ok(Self::Complete)
            }
            Self::Complete => {
                Err(Error::generic("Cannot advance from Complete state"))
            }
        }
    }
}

impl AsDeclarativePhase for LogReplayPhase {
    fn as_declarative_phase(&self) -> DeclarativePhase {
        match self {
            Self::Commit(plan) => DeclarativePhase {
                operation: OperationType::LogReplay,
                phase_type: PhaseType::Commit,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::CheckpointManifest(plan) => DeclarativePhase {
                operation: OperationType::LogReplay,
                phase_type: PhaseType::CheckpointManifest,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::CheckpointLeaf(plan) => DeclarativePhase {
                operation: OperationType::LogReplay,
                phase_type: PhaseType::CheckpointLeaf,
                query_plan: Some(plan.as_query_plan()),
                terminal_data: None,
            },
            Self::Complete => DeclarativePhase {
                operation: OperationType::LogReplay,
                phase_type: PhaseType::Complete,
                query_plan: None,
                terminal_data: Some(TerminalData::LogReplayComplete),
            },
        }
    }
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
// Proto Conversion for State Machines
// =============================================================================

impl From<&LogReplayPhase> for proto::LogReplayPhase {
    fn from(phase: &LogReplayPhase) -> Self {
        use proto::log_replay_phase::Phase;

        let phase_variant = match phase {
            LogReplayPhase::Commit(p) => {
                Phase::Commit(proto::CommitPhaseData {
                    plan: Some(p.into()),
                    query_plan: Some((&p.as_query_plan()).into()),
                })
            }
            LogReplayPhase::CheckpointManifest(p) => {
                Phase::CheckpointManifest(proto::CheckpointManifestData {
                    plan: Some(p.into()),
                    query_plan: Some((&p.as_query_plan()).into()),
                })
            }
            LogReplayPhase::CheckpointLeaf(p) => {
                Phase::CheckpointLeaf(proto::CheckpointLeafData {
                    plan: Some(p.into()),
                    query_plan: Some((&p.as_query_plan()).into()),
                })
            }
            LogReplayPhase::Complete => {
                Phase::Complete(proto::LogReplayComplete {})
            }
        };

        proto::LogReplayPhase {
            phase: Some(phase_variant),
        }
    }
}

// =============================================================================
// Proto Conversion for Declarative Phase
// =============================================================================

impl From<OperationType> for i32 {
    fn from(op: OperationType) -> i32 {
        match op {
            OperationType::LogReplay => proto::OperationType::LogReplay as i32,
            OperationType::SnapshotBuild => proto::OperationType::SnapshotBuild as i32,
            OperationType::Scan => proto::OperationType::Scan as i32,
        }
    }
}

impl From<PhaseType> for i32 {
    fn from(pt: PhaseType) -> i32 {
        match pt {
            PhaseType::Commit => proto::PhaseType::Commit as i32,
            PhaseType::CheckpointManifest => proto::PhaseType::CheckpointManifest as i32,
            PhaseType::CheckpointLeaf => proto::PhaseType::CheckpointLeaf as i32,
            PhaseType::ListFiles => proto::PhaseType::ListFiles as i32,
            PhaseType::LoadMetadata => proto::PhaseType::LoadMetadata as i32,
            PhaseType::Complete => proto::PhaseType::Complete as i32,
            PhaseType::Ready => proto::PhaseType::Ready as i32,
        }
    }
}

impl From<&DeclarativePhase> for proto::DeclarativePhase {
    fn from(phase: &DeclarativePhase) -> Self {
        let terminal_data = phase.terminal_data.as_ref().map(|td| {
            use proto::terminal_data::Data;
            let data = match td {
                TerminalData::LogReplayComplete => {
                    Data::LogReplayComplete(proto::LogReplayComplete {})
                }
                TerminalData::SnapshotReady { version, table_schema: _ } => {
                    Data::SnapshotReady(proto::SnapshotReady {
                        version: *version,
                        table_schema: None, // TODO: implement schema conversion
                    })
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

