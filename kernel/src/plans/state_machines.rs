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
use crate::expressions::PredicateRef;
use crate::log_segment::LogSegment;
use crate::plans::PartitionPruneState;
use crate::proto_generated as proto;
use crate::schema::SchemaRef;
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::{DeltaResult, Error, FileMeta, SnapshotRef};

use super::composite::*;
use super::declarative::DeclarativePlanNode;
use super::kdf_state::{
    AddRemoveDedupState, CheckpointDedupState, CheckpointHintReaderState, ConsumerKdfState,
    ConsumerStateReceiver, FilterKdfState, FilterStateReceiver, LogSegmentBuilderState,
    MetadataProtocolReaderState, StateSender,
};
use super::nodes::{
    ConsumerByKDF, FileListingNode, FileType, FilterByExpressionNode, FilterByKDF, FirstNonNullNode,
    ParseJsonNode, ScanNode, SchemaQueryNode, SelectNode, SinkNode,
};
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

/// Phase of the snapshot state machine - holds receivers for state collection.
///
/// Phases hold `ConsumerStateReceiver` to collect consumer states after execution completes.
/// The corresponding plan (which holds the sender) is stored separately in
/// `SnapshotStateMachine::current_plan`.
#[derive(Debug)]
pub enum SnapshotPhase {
    /// Read _last_checkpoint hint file - holds receiver for CheckpointHintReader state
    CheckpointHint { consumer_receiver: ConsumerStateReceiver },
    /// List files in _delta_log directory - holds receiver for LogSegmentBuilder state
    ListFiles { consumer_receiver: ConsumerStateReceiver },
    /// Read protocol and metadata from log files - holds receiver for MetadataProtocolReader state
    LoadMetadata { consumer_receiver: ConsumerStateReceiver },
    /// Snapshot is ready
    Complete,
}

impl SnapshotPhase {
    /// Check if this is a terminal state.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Get the phase name for debugging/logging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::CheckpointHint { .. } => "CheckpointHint",
            Self::ListFiles { .. } => "ListFiles",
            Self::LoadMetadata { .. } => "LoadMetadata",
            Self::Complete => "Complete",
        }
    }
}

/// Plan enum for snapshot state machine - holds plan structs with senders.
///
/// Plans contain `ConsumerByKDF` (which is `StateSender<ConsumerKdfState>`) that
/// creates `OwnedState` for each partition during execution. The sender
/// is cloned into each partition's stream.
#[derive(Debug, Clone)]
pub enum SnapshotPlan {
    /// Read _last_checkpoint hint file
    CheckpointHint(CheckpointHintPlan),
    /// List files in _delta_log directory
    ListFiles(FileListingPhasePlan),
    /// Read protocol and metadata from log files
    LoadMetadata(MetadataLoadPlan),
}

impl SnapshotPlan {
    /// Get the query plan for this plan variant.
    pub fn as_query_plan(&self) -> DeclarativePlanNode {
        match self {
            Self::CheckpointHint(p) => p.as_query_plan(),
            Self::ListFiles(p) => p.as_query_plan(),
            Self::LoadMetadata(p) => p.as_query_plan(),
        }
    }
}

impl AsDeclarativePhase for SnapshotPhase {
    fn as_declarative_phase(&self) -> DeclarativePhase {
        match self {
            Self::CheckpointHint { .. } => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::CheckpointHint,
                query_plan: None, // Plan is stored separately
                terminal_data: None,
            },
            Self::ListFiles { .. } => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::ListFiles,
                query_plan: None, // Plan is stored separately
                terminal_data: None,
            },
            Self::LoadMetadata { .. } => DeclarativePhase {
                operation: OperationType::SnapshotBuild,
                phase_type: PhaseType::LoadMetadata,
                query_plan: None, // Plan is stored separately
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
/// # Architecture
///
/// The state machine separates **phases** from **plans**:
/// - **Phases** (`phase`) hold receivers for state collection
/// - **Plans** (`current_plan`) hold senders that go to the executor
///
/// When creating a phase, `StateSender::build()` creates a sender/receiver pair.
/// The sender goes into the plan, the receiver goes into the phase. After execution,
/// states are collected via the receiver and processed.
///
/// # Example
///
/// ```ignore
/// let sm = SnapshotStateMachine::new(table_root)?;
///
/// loop {
///     let plan = sm.take_plan()?;
///     let result = executor.execute_plan(plan)?;
///     match sm.advance(result)? {
///         AdvanceResult::Continue => continue,
///         AdvanceResult::Done(snapshot) => return Ok(snapshot),
///     }
/// }
/// ```
#[derive(Debug)]
pub struct SnapshotStateMachine {
    /// Current phase - holds receivers for state collection
    phase: SnapshotPhase,
    /// Current plan - holds senders, taken once per phase
    current_plan: Option<SnapshotPlan>,
    /// Accumulated state across phases
    state: SnapshotBuildState,
}

impl SnapshotStateMachine {
    /// Create a new snapshot state machine for a table.
    pub fn new(table_root: Url) -> DeltaResult<Self> {
        let state = SnapshotBuildState::new(table_root)?;
        let (phase, plan) = Self::create_checkpoint_hint_phase_and_plan(&state)?;
        Ok(Self {
            phase,
            current_plan: Some(plan),
            state,
        })
    }

    /// Create with a specific target version.
    pub fn with_version(table_root: Url, version: i64) -> DeltaResult<Self> {
        let mut sm = Self::new(table_root)?;
        sm.state.version = Some(version);
        Ok(sm)
    }

    /// Get the current phase (for testing/debugging).
    pub fn phase(&self) -> &SnapshotPhase {
        &self.phase
    }

    /// Check if terminal.
    pub fn is_terminal(&self) -> bool {
        self.phase.is_complete()
    }

    /// Take the current plan for execution.
    ///
    /// This method takes ownership of the plan, which should only be called once per phase.
    /// After execution, call `advance()` to transition to the next phase.
    ///
    /// # Errors
    ///
    /// Returns an error if called when no plan is available (already taken or complete state).
    pub fn take_plan(&mut self) -> DeltaResult<SnapshotPlan> {
        self.current_plan
            .take()
            .ok_or_else(|| Error::generic("Plan already taken or state machine is complete"))
    }

    /// Get the plan for the current phase (without taking ownership).
    ///
    /// This returns the `DeclarativePlanNode` representation suitable for execution.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        self.current_plan
            .as_ref()
            .map(|p| p.as_query_plan())
            .ok_or_else(|| Error::generic("No plan available - already taken or complete state"))
    }

    /// Get the declarative phase (for runtime inspection).
    pub fn as_declarative_phase(&self) -> DeclarativePhase {
        self.phase.as_declarative_phase()
    }

    /// Advance the state machine with the result of plan execution.
    ///
    /// This method:
    /// 1. Takes ownership of the current phase (which contains the receiver)
    /// 2. Collects states from the receiver
    /// 3. Processes states and updates accumulated state
    /// 4. Creates the next phase/plan pair
    /// 5. Transitions to the next phase
    pub fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<Snapshot>> {
        // Take ownership of the current phase to extract the receiver
        let phase = std::mem::replace(&mut self.phase, SnapshotPhase::Complete);

        match phase {
            SnapshotPhase::CheckpointHint { consumer_receiver } => {
                // For CheckpointHint phase, FileNotFound is OK - the file is optional
                match result {
                    Ok(_) => {
                        // Collect states from receiver
                        let states = consumer_receiver.take_all()?;
                        self.extract_checkpoint_hint_from_states(states)?;
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

                // Create next phase/plan
                let (phase, plan) = self.create_list_files_phase_and_plan()?;
                self.phase = phase;
                self.current_plan = Some(plan);
                Ok(AdvanceResult::Continue)
            }
            SnapshotPhase::ListFiles { consumer_receiver } => {
                // Propagate execution errors for this phase
                result?;

                // Collect states from receiver
                let states = consumer_receiver.take_all()?;
                self.extract_log_segment_from_states(states)?;

                // Create next phase/plan
                let (phase, plan) = self.create_load_metadata_phase_and_plan()?;
                self.phase = phase;
                self.current_plan = Some(plan);
                Ok(AdvanceResult::Continue)
            }
            SnapshotPhase::LoadMetadata { consumer_receiver } => {
                // Propagate execution errors for this phase
                result?;

                // Collect states from receiver
                let states = consumer_receiver.take_all()?;
                self.extract_protocol_metadata_from_states(states)?;

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
                self.current_plan = None;

                // Get the LogSegment built during ListFiles phase
                let log_segment =
                    self.state.log_segment.take().ok_or_else(|| {
                        Error::generic("LogSegment not built during ListFiles phase")
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
            SnapshotPhase::Complete => Err(Error::generic("Cannot advance from Complete state")),
        }
    }

    // =========================================================================
    // Phase/Plan Creation Helpers
    // =========================================================================

    fn create_checkpoint_hint_phase_and_plan(
        state: &SnapshotBuildState,
    ) -> DeltaResult<(SnapshotPhase, SnapshotPlan)> {
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

        // Build sender/receiver pair for checkpoint hint reader
        let (sender, receiver) = StateSender::build(ConsumerKdfState::CheckpointHintReader(
            CheckpointHintReaderState::new(),
        ));

        let plan = CheckpointHintPlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![file_meta],
                schema: checkpoint_schema,
            },
            hint_reader: sender,
            sink: SinkNode::drop(),
        };

        Ok((
            SnapshotPhase::CheckpointHint {
                consumer_receiver: receiver,
            },
            SnapshotPlan::CheckpointHint(plan),
        ))
    }

    fn create_list_files_phase_and_plan(&self) -> DeltaResult<(SnapshotPhase, SnapshotPlan)> {
        // Build sender/receiver pair for log segment builder
        let (sender, receiver) = StateSender::build(ConsumerKdfState::LogSegmentBuilder(
            LogSegmentBuilderState::new(
                self.state.log_root.clone(),
                self.state.version.map(|v| v as u64),
                self.state.checkpoint_hint_version.map(|v| v as u64),
            ),
        ));

        let plan = FileListingPhasePlan {
            listing: FileListingNode {
                path: self.state.log_root.clone(),
            },
            log_segment_builder: sender,
            sink: SinkNode::drop(),
        };

        Ok((
            SnapshotPhase::ListFiles {
                consumer_receiver: receiver,
            },
            SnapshotPlan::ListFiles(plan),
        ))
    }

    fn create_load_metadata_phase_and_plan(&self) -> DeltaResult<(SnapshotPhase, SnapshotPlan)> {
        use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
        use crate::schema::{StructField, StructType, ToSchema};

        // Get the log segment built during ListFiles phase
        let log_segment = self
            .state
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
            files.push(checkpoint.location.clone());
        }

        // Add commit files (in reverse order - newest first to find P&M faster)
        for commit in log_segment.ascending_commit_files.iter().rev() {
            files.push(commit.location.clone());
        }

        // Determine file type based on what we're scanning
        let file_type = if !log_segment.checkpoint_parts.is_empty() {
            FileType::Parquet
        } else {
            FileType::Json
        };

        // Build sender/receiver pair for metadata protocol reader
        let (sender, receiver) = StateSender::build(ConsumerKdfState::MetadataProtocolReader(
            MetadataProtocolReaderState::new(),
        ));

        let plan = MetadataLoadPlan {
            scan: ScanNode {
                file_type,
                files,
                schema,
            },
            metadata_reader: sender,
            sink: SinkNode::drop(),
        };

        Ok((
            SnapshotPhase::LoadMetadata {
                consumer_receiver: receiver,
            },
            SnapshotPlan::LoadMetadata(plan),
        ))
    }

    // =========================================================================
    // State Extraction Helpers
    // =========================================================================

    /// Extract checkpoint hint from collected consumer states.
    fn extract_checkpoint_hint_from_states(
        &mut self,
        states: Vec<ConsumerKdfState>,
    ) -> DeltaResult<()> {
        for state in states {
            if let ConsumerKdfState::CheckpointHintReader(hint_state) = state {
                // Check for errors first
                if let Some(error) = hint_state.take_error() {
                    return Err(Error::generic(error));
                }

                // Extract checkpoint hint version if present
                if let Some(version) = hint_state.get_version() {
                    self.state.checkpoint_hint_version = Some(version as i64);
                }
            }
        }
        Ok(())
    }

    /// Extract log segment from collected consumer states.
    fn extract_log_segment_from_states(
        &mut self,
        states: Vec<ConsumerKdfState>,
    ) -> DeltaResult<()> {
        for state in states {
            if let ConsumerKdfState::LogSegmentBuilder(builder_state) = state {
                // Build LogSegment directly from the builder state
                self.state.log_segment = Some(builder_state.into_log_segment()?);
                return Ok(());
            }
        }
        Err(Error::generic("No LogSegmentBuilder state found"))
    }

    /// Extract protocol and metadata from collected consumer states.
    fn extract_protocol_metadata_from_states(
        &mut self,
        states: Vec<ConsumerKdfState>,
    ) -> DeltaResult<()> {
        for state in states {
            if let ConsumerKdfState::MetadataProtocolReader(mp_state) = state {
                // Check for errors first
                if let Some(error) = mp_state.take_error() {
                    return Err(Error::generic(error));
                }

                // Extract protocol if present
                if let Some(protocol) = mp_state.take_protocol() {
                    self.state.protocol = Some(protocol);
                }

                // Extract metadata if present
                if let Some(metadata) = mp_state.take_metadata() {
                    self.state.metadata = Some(metadata);
                }
            }
        }
        Ok(())
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

/// Phase of the scan state machine - holds receivers for state collection.
///
/// Phases hold `StateReceiver` to collect filter states after execution completes.
/// The corresponding plan (which holds the sender) is stored separately in
/// `ScanStateMachine::current_plan`.
#[derive(Debug)]
pub enum ScanStateMachinePhase {
    /// Process commit files (JSON log) - holds receiver for AddRemoveDedup state
    CommitPhase {
        filter_receiver: FilterStateReceiver,
    },
    /// Query checkpoint schema to detect V2 checkpoints - no state needed
    SchemaQuery,
    /// Read JSON checkpoint - holds receivers for dedup state and sidecar collector
    JsonCheckpoint {
        filter_receiver: FilterStateReceiver,
        consumer_receiver: ConsumerStateReceiver,
    },
    /// Read checkpoint manifest (v2 checkpoints) - holds receiver for sidecar collector
    CheckpointManifest {
        consumer_receiver: ConsumerStateReceiver,
    },
    /// Read checkpoint leaf/sidecar files - holds receiver for CheckpointDedup state
    CheckpointLeaf {
        filter_receiver: FilterStateReceiver,
    },
    /// Scan is complete
    Complete,
}

impl ScanStateMachinePhase {
    /// Get the phase name for debugging/logging.
    pub fn phase_name(&self) -> &'static str {
        match self {
            Self::CommitPhase { .. } => "Commit",
            Self::SchemaQuery => "SchemaQuery",
            Self::JsonCheckpoint { .. } => "JsonCheckpoint",
            Self::CheckpointManifest { .. } => "CheckpointManifest",
            Self::CheckpointLeaf { .. } => "CheckpointLeaf",
            Self::Complete => "Complete",
        }
    }

    /// Check if this is a terminal state.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Plan enum for scan state machine - holds plan structs with senders.
///
/// Plans contain `FilterByKDF` (which is `StateSender<FilterKdfState>`) that
/// creates `OwnedState` for each partition during execution. The sender
/// is cloned into each partition's stream.
#[derive(Debug, Clone)]
pub enum ScanStateMachinePlan {
    /// Process commit files (JSON log)
    Commit(CommitPhasePlan),
    /// Query checkpoint schema to detect V2 checkpoints
    SchemaQuery(SchemaQueryPhasePlan),
    /// Read JSON checkpoint - collects sidecars and streams add actions
    JsonCheckpoint(JsonCheckpointPhasePlan),
    /// Read checkpoint manifest (v2 checkpoints)
    CheckpointManifest(CheckpointManifestPlan),
    /// Read checkpoint leaf/sidecar files
    CheckpointLeaf(CheckpointLeafPlan),
}

impl ScanStateMachinePlan {
    /// Get the query plan for this plan variant.
    pub fn as_query_plan(&self) -> DeclarativePlanNode {
        match self {
            Self::Commit(p) => p.as_query_plan(),
            Self::SchemaQuery(p) => p.as_query_plan(),
            Self::JsonCheckpoint(p) => p.as_query_plan(),
            Self::CheckpointManifest(p) => p.as_query_plan(),
            Self::CheckpointLeaf(p) => p.as_query_plan(),
        }
    }
}

// Keep ScanPhase as an alias for backwards compatibility during transition
#[doc(hidden)]
#[deprecated(note = "Use ScanStateMachinePhase instead")]
pub type ScanPhase = ScanStateMachinePhase;

/// Internal state accumulated during scan execution.
#[derive(Debug)]
pub struct ScanBuildState {
    /// Reference to the snapshot being scanned
    snapshot: SnapshotRef,
    /// Reference to the scan state info (contains schemas, predicate, column mapping, etc.)
    state_info: Arc<crate::scan::state_info::StateInfo>,
    /// Accumulated dedup state from commit phase (used to seed checkpoint dedup)
    pub commit_dedup_state: Option<AddRemoveDedupState>,
    /// Optional partition pruning state (built from predicate + transform spec)
    pub partition_prune_state: Option<FilterKdfState>,
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
    /// Get the log root URL from the snapshot
    fn log_root(&self) -> &Url {
        &self.snapshot.log_segment().log_root
    }

    /// Get the table root URL from the snapshot
    fn table_root(&self) -> &Url {
        self.snapshot.table_root()
    }

    /// Get the physical schema from state_info
    fn physical_schema(&self) -> &SchemaRef {
        &self.state_info.physical_schema
    }

    /// Get the logical schema from state_info
    fn logical_schema(&self) -> &SchemaRef {
        &self.state_info.logical_schema
    }

    /// Get the column mapping mode from state_info
    fn column_mapping_mode(&self) -> crate::table_features::ColumnMappingMode {
        self.state_info.column_mapping_mode
    }

    /// Get the predicate from state_info (if present)
    fn predicate(&self) -> Option<(PredicateRef, SchemaRef)> {
        use crate::scan::PhysicalPredicate;
        match &self.state_info.physical_predicate {
            PhysicalPredicate::Some(pred, schema) => Some((pred.clone(), schema.clone())),
            PhysicalPredicate::None | PhysicalPredicate::StaticSkipAll => None,
        }
    }

    /// Compute stats schema from predicate's referenced schema (delegates to DataSkippingFilter).
    /// This is computed on-demand to avoid duplication.
    fn stats_schema(&self) -> Option<SchemaRef> {
        use crate::scan::data_skipping::DataSkippingFilter;
        use crate::scan::PhysicalPredicate;

        let referenced_schema = match &self.state_info.physical_predicate {
            PhysicalPredicate::Some(_, schema) => schema,
            PhysicalPredicate::None | PhysicalPredicate::StaticSkipAll => return None,
        };

        DataSkippingFilter::compute_stats_schema(referenced_schema)
    }

    /// Create a partition prune filter sender.
    ///
    /// Returns a (sender, receiver) pair for the partition prune filter.
    /// The receiver can be discarded since partition pruning is stateless.
    #[inline]
    fn create_partition_prune_filter(&self) -> Option<FilterByKDF> {
        self.partition_prune_state.as_ref().map(|s| {
            let (sender, _receiver) = StateSender::build(s.clone());
            sender
        })
    }

    // FIXME: Remove this constructor. We should only construct from Scan
    fn new(
        snapshot: SnapshotRef,
        state_info: Arc<crate::scan::state_info::StateInfo>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            snapshot,
            state_info,
            commit_dedup_state: None,
            partition_prune_state: None,
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
/// # Architecture
///
/// The state machine separates **phases** from **plans**:
/// - **Phases** (`current_phase`) hold receivers for state collection
/// - **Plans** (`current_plan`) hold senders that go to the executor
///
/// When creating a phase, `StateSender::build()` creates a sender/receiver pair.
/// The sender goes into the plan, the receiver goes into the phase. After execution,
/// states are collected via the receiver and merged.
///
/// # Example
///
/// ```ignore
/// let scan = ScanBuilder::new(snapshot)
///     .with_schema(schema)
///     .build()?;
/// let sm = ScanStateMachine::from_scan(&scan)?;
///
/// loop {
///     let plan = sm.take_plan()?;
///     let result = executor.execute_plan(plan)?;
///     match sm.advance(result)? {
///         AdvanceResult::Continue => continue,
///         AdvanceResult::Done(result) => return Ok(result),
///     }
/// }
/// ```
#[derive(Debug)]
pub struct ScanStateMachine {
    /// Current phase - holds receivers for state collection
    current_phase: ScanStateMachinePhase,
    /// Current plan - holds senders, taken once per phase
    current_plan: Option<ScanStateMachinePlan>,
    /// Accumulated state across phases
    state: ScanBuildState,
}

impl ScanStateMachine {
    /// Construct a scan state machine from a `Scan` (the only intended construction path).
    ///
    /// This avoids re-threading schemas/predicate/column-mapping that already live in `StateInfo`.
    pub(crate) fn from_scan(scan: &crate::scan::Scan) -> DeltaResult<Self> {
        use crate::scan::PhysicalPredicate;

        if let PhysicalPredicate::StaticSkipAll = scan.state_info().physical_predicate {
            // Caller typically checks this already; return a trivially-complete state machine.
            return Ok(Self {
                current_phase: ScanStateMachinePhase::Complete,
                current_plan: None,
                state: ScanBuildState {
                    snapshot: scan.snapshot().clone(),
                    state_info: scan.state_info(),
                    commit_dedup_state: None,
                    partition_prune_state: None,
                    commit_files: vec![],
                    checkpoint_files: vec![],
                    sidecar_files: vec![],
                    checkpoint_type: None,
                },
            });
        }

        // Extract log segment files.
        let log_segment = scan.snapshot().log_segment();
        let commit_files: Vec<FileMeta> = log_segment.find_commit_cover();
        let checkpoint_files: Vec<FileMeta> = log_segment
            .checkpoint_parts
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Compute partition pruning state from scan's predicate and transform spec
        let partition_prune_state = match (
            &scan.state_info().physical_predicate,
            &scan.state_info().transform_spec,
        ) {
            (PhysicalPredicate::Some(pred, _), Some(ts)) => {
                Some(FilterKdfState::PartitionPrune(PartitionPruneState {
                    predicate: pred.clone(),
                    transform_spec: ts.clone(),
                    logical_schema: scan.state_info().logical_schema.clone(),
                    column_mapping_mode: scan.state_info().column_mapping_mode,
                }))
            }
            _ => None,
        };

        let mut state = ScanBuildState::new(scan.snapshot().clone(), scan.state_info())?;
        state.commit_files = commit_files;
        state.checkpoint_files = checkpoint_files;
        state.partition_prune_state = partition_prune_state;

        // Create initial phase and plan using sender/receiver pattern
        let (phase, plan) = Self::create_commit_phase_and_plan(&state)?;
        Ok(Self {
            current_phase: phase,
            current_plan: Some(plan),
            state,
        })
    }

    /// Get the current phase (for testing/debugging).
    #[cfg(test)]
    pub fn phase(&self) -> &ScanStateMachinePhase {
        &self.current_phase
    }

    /// Check if terminal.
    pub fn is_terminal(&self) -> bool {
        self.current_phase.is_complete()
    }

    /// Take the current plan for execution.
    ///
    /// This method takes ownership of the plan, which should only be called once per phase.
    /// After execution, call `advance()` to transition to the next phase.
    ///
    /// # Errors
    ///
    /// Returns an error if called when no plan is available (already taken or complete state).
    pub fn take_plan(&mut self) -> DeltaResult<ScanStateMachinePlan> {
        self.current_plan
            .take()
            .ok_or_else(|| Error::generic("Plan already taken or state machine is complete"))
    }

    /// Get the plan for the current phase (without taking ownership).
    ///
    /// This returns the `DeclarativePlanNode` representation suitable for execution.
    pub fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
        self.current_plan
            .as_ref()
            .map(|p| p.as_query_plan())
            .ok_or_else(|| Error::generic("No plan available - already taken or complete state"))
    }

    /// Advance the state machine with the result of plan execution.
    ///
    /// This method:
    /// 1. Takes ownership of the current phase (which contains the receiver)
    /// 2. Collects states from the receiver
    /// 3. Merges states and updates accumulated state
    /// 4. Creates the next phase/plan pair
    /// 5. Transitions to the next phase
    pub fn advance(
        &mut self,
        result: DeltaResult<DeclarativePlanNode>,
    ) -> DeltaResult<AdvanceResult<ScanMetadataResult>> {
        // Propagate execution errors
        let executed_plan = result?;

        // Take ownership of the current phase to extract the receiver
        let phase = std::mem::replace(&mut self.current_phase, ScanStateMachinePhase::Complete);

        match phase {
            ScanStateMachinePhase::CommitPhase { filter_receiver } => {
                // Collect states from the receiver
                let states = filter_receiver.take_all()?;
                self.state.commit_dedup_state = Some(Self::merge_dedup_states(states)?);

                // Check if we have checkpoint files
                if self.state.checkpoint_files.is_empty() {
                    // No checkpoint - we're done
                    self.current_phase = ScanStateMachinePhase::Complete;
                    self.current_plan = None;
                    return Ok(AdvanceResult::Done(self.build_result()));
                }

                // Determine checkpoint type and create next phase/plan
                match self.state.checkpoint_type {
                    Some(CheckpointType::V2) => {
                        // V2 checkpoint (already known) - read manifest first
                        let (phase, plan) = self.create_checkpoint_manifest_phase_and_plan()?;
                        self.current_phase = phase;
                        self.current_plan = Some(plan);
                    }
                    Some(CheckpointType::MultiPart) => {
                        // Multi-part checkpoint - go directly to leaf phase
                        let (phase, plan) = self.create_checkpoint_leaf_phase_and_plan()?;
                        self.current_phase = phase;
                        self.current_plan = Some(plan);
                    }
                    Some(CheckpointType::Classic) | None => {
                        // Check if the checkpoint file is JSON (can't do parquet schema query)
                        let is_json_checkpoint = self
                            .state
                            .checkpoint_files
                            .first()
                            .map(|f| f.location.path().ends_with(".json"))
                            .unwrap_or(false);

                        if is_json_checkpoint {
                            // JSON checkpoints can't be queried for parquet schema.
                            let (phase, plan) = self.create_json_checkpoint_phase_and_plan()?;
                            self.current_phase = phase;
                            self.current_plan = Some(plan);
                        } else {
                            // Parquet checkpoint - query schema to detect V2 with sidecars
                            let plan = self.create_schema_query_plan()?;
                            self.current_phase = ScanStateMachinePhase::SchemaQuery;
                            self.current_plan = Some(ScanStateMachinePlan::SchemaQuery(plan));
                        }
                    }
                }
                Ok(AdvanceResult::Continue)
            }
            ScanStateMachinePhase::SchemaQuery => {
                // After schema query, check if schema has 'sidecar' column
                let has_sidecar = self.check_schema_has_sidecar(&executed_plan);
                if has_sidecar {
                    // V2 checkpoint with sidecars - read manifest first
                    self.state.checkpoint_type = Some(CheckpointType::V2);
                    let (phase, plan) = self.create_checkpoint_manifest_phase_and_plan()?;
                    self.current_phase = phase;
                    self.current_plan = Some(plan);
                } else {
                    // Classic single-part checkpoint - read directly
                    self.state.checkpoint_type = Some(CheckpointType::Classic);
                    let (phase, plan) = self.create_checkpoint_leaf_phase_and_plan()?;
                    self.current_phase = phase;
                    self.current_plan = Some(plan);
                }
                Ok(AdvanceResult::Continue)
            }
            ScanStateMachinePhase::JsonCheckpoint {
                filter_receiver,
                consumer_receiver,
            } => {
                // Collect filter states (for future use if needed)
                let _filter_states = filter_receiver.take_all()?;

                // Collect sidecar files from consumer receiver
                let consumer_states = consumer_receiver.take_all()?;
                self.extract_sidecar_files_from_states(consumer_states)?;

                if self.state.sidecar_files.is_empty() {
                    // No sidecars - all add actions were in the checkpoint, we're done
                    self.current_phase = ScanStateMachinePhase::Complete;
                    self.current_plan = None;
                    Ok(AdvanceResult::Done(self.build_result()))
                } else {
                    // Sidecars found - need to read them for more add actions
                    self.state.checkpoint_type = Some(CheckpointType::V2);
                    let (phase, plan) = self.create_checkpoint_leaf_phase_and_plan()?;
                    self.current_phase = phase;
                    self.current_plan = Some(plan);
                    Ok(AdvanceResult::Continue)
                }
            }
            ScanStateMachinePhase::CheckpointManifest { consumer_receiver } => {
                // Collect sidecar files from consumer receiver
                let consumer_states = consumer_receiver.take_all()?;
                self.extract_sidecar_files_from_states(consumer_states)?;

                let (phase, plan) = self.create_checkpoint_leaf_phase_and_plan()?;
                self.current_phase = phase;
                self.current_plan = Some(plan);
                Ok(AdvanceResult::Continue)
            }
            ScanStateMachinePhase::CheckpointLeaf { filter_receiver } => {
                // Collect states (for completeness)
                let _states = filter_receiver.take_all()?;

                // Complete
                self.current_phase = ScanStateMachinePhase::Complete;
                self.current_plan = None;
                Ok(AdvanceResult::Done(self.build_result()))
            }
            ScanStateMachinePhase::Complete => {
                Err(Error::generic("Cannot advance from Complete state"))
            }
        }
    }

    /// Merge multiple AddRemoveDedupState instances into one.
    fn merge_dedup_states(states: Vec<FilterKdfState>) -> DeltaResult<AddRemoveDedupState> {
        let mut merged = AddRemoveDedupState::new();
        for state in states {
            if let FilterKdfState::AddRemoveDedup(dedup_state) = state {
                // Merge the seen keys from this partition
                for key in dedup_state.seen_keys() {
                    merged.insert(key);
                }
            }
        }
        Ok(merged)
    }

    fn build_result(&self) -> ScanMetadataResult {
        ScanMetadataResult {
            table_root: self.state.table_root().clone(),
            files: Vec::new(), // Would be populated from accumulated state
            physical_schema: self.state.physical_schema().clone(),
            logical_schema: self.state.logical_schema().clone(),
        }
    }

    // =========================================================================
    // Phase/Plan Creation Helpers
    // =========================================================================

    /// Create commit phase and plan using sender/receiver pattern.
    fn create_commit_phase_and_plan(
        state: &ScanBuildState,
    ) -> DeltaResult<(ScanStateMachinePhase, ScanStateMachinePlan)> {
        use crate::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
        use crate::scan::data_skipping::as_sql_data_skipping_predicate;
        use crate::scan::log_replay::{get_add_transform_expr, SCAN_ROW_SCHEMA};

        // Schema for reading add/remove actions from commit files
        let schema = get_commit_schema().project(&[ADD_NAME, REMOVE_NAME])?;

        // Use the standard scan row schema and transform expression from log_replay
        let output_schema = SCAN_ROW_SCHEMA.clone();
        let transform_expr = get_add_transform_expr();

        // Build data skipping plan if predicate is present
        let data_skipping = match (state.predicate(), state.stats_schema().as_ref()) {
            (Some((predicate, _)), Some(stats_schema)) => {
                let skipping_pred = as_sql_data_skipping_predicate(&predicate);
                skipping_pred.map(|pred| {
                    DataSkippingPlan {
                        parse_json: ParseJsonNode {
                            json_column: "add.stats".to_string(),
                            target_schema: stats_schema.clone(),
                            output_column: String::new(),
                        },
                        filter: FilterByExpressionNode {
                            predicate: Arc::new(pred.into()),
                        },
                    }
                })
            }
            _ => None,
        };

        // Create sender/receiver pair for AddRemoveDedup filter
        let (dedup_sender, dedup_receiver) = StateSender::build(
            FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new()),
        );

        let plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: state.commit_files.clone(),
                schema,
            },
            data_skipping,
            partition_prune_filter: state.create_partition_prune_filter(),
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![transform_expr],
                output_schema,
            },
            sink: SinkNode::results(),
        };

        Ok((
            ScanStateMachinePhase::CommitPhase {
                filter_receiver: dedup_receiver,
            },
            ScanStateMachinePlan::Commit(plan),
        ))
    }

    /// Create checkpoint manifest phase and plan using sender/receiver pattern.
    fn create_checkpoint_manifest_phase_and_plan(
        &self,
    ) -> DeltaResult<(ScanStateMachinePhase, ScanStateMachinePlan)> {
        use super::kdf_state::SidecarCollectorState;
        use crate::schema::{DataType, StructField, StructType};
        use crate::Expression;

        // Schema for v2 checkpoint manifest - we need the sidecar struct
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "sidecar",
            DataType::struct_type_unchecked(vec![
                StructField::nullable("path", DataType::STRING),
                StructField::nullable("sizeInBytes", DataType::LONG),
                StructField::nullable("modificationTime", DataType::LONG),
                StructField::nullable(
                    "tags",
                    DataType::Map(Box::new(crate::schema::MapType::new(
                        DataType::STRING,
                        DataType::STRING,
                        true,
                    ))),
                ),
            ]),
        )]));

        // Pass through the `sidecar` struct so SidecarCollector can reuse SidecarVisitor
        // (avoid bespoke Arrow column-walking logic and keep a single canonical extractor).
        let output_schema = schema.clone();
        let columns = vec![Expression::column(["sidecar"]).into()];

        // Create sender/receiver pair for sidecar collector
        let (sender, receiver) = StateSender::build(ConsumerKdfState::SidecarCollector(
            SidecarCollectorState::new(self.state.log_root().clone()),
        ));

        let plan = CheckpointManifestPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: self.state.checkpoint_files.clone(),
                schema,
            },
            project: SelectNode {
                columns,
                output_schema,
            },
            sidecar_collector: sender,
            sink: SinkNode::drop(), // Drop sink - side effect only (collects sidecar paths)
        };

        Ok((
            ScanStateMachinePhase::CheckpointManifest {
                consumer_receiver: receiver,
            },
            ScanStateMachinePlan::CheckpointManifest(plan),
        ))
    }

    /// Create checkpoint leaf phase and plan using sender/receiver pattern.
    fn create_checkpoint_leaf_phase_and_plan(
        &self,
    ) -> DeltaResult<(ScanStateMachinePhase, ScanStateMachinePlan)> {
        use crate::actions::{get_commit_schema, ADD_NAME};
        use crate::scan::log_replay::{get_add_transform_expr, SCAN_ROW_SCHEMA};

        let schema = get_commit_schema().project(&[ADD_NAME])?;
        let output_schema = SCAN_ROW_SCHEMA.clone();
        let transform_expr = get_add_transform_expr();

        // Create checkpoint dedup state with keys from commit phase
        let checkpoint_dedup_state = match &self.state.commit_dedup_state {
            Some(add_remove_state) => add_remove_state.to_checkpoint_dedup_state(),
            None => CheckpointDedupState::new(),
        };

        // For V2 checkpoints with sidecars, read from sidecar files
        let files = if !self.state.sidecar_files.is_empty() {
            self.state.sidecar_files.clone()
        } else {
            self.state.checkpoint_files.clone()
        };

        // Determine file type based on the file extension
        let file_type = if !self.state.sidecar_files.is_empty() {
            FileType::Parquet
        } else {
            files
                .first()
                .map(|f| {
                    if f.location.path().ends_with(".json") {
                        FileType::Json
                    } else {
                        FileType::Parquet
                    }
                })
                .unwrap_or(FileType::Parquet)
        };

        // Create sender/receiver pair for CheckpointDedup filter
        let (dedup_sender, dedup_receiver) = StateSender::build(
            FilterKdfState::CheckpointDedup(checkpoint_dedup_state),
        );

        let plan = CheckpointLeafPlan {
            scan: ScanNode {
                file_type,
                files,
                schema,
            },
            partition_prune_filter: self.state.create_partition_prune_filter(),
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![transform_expr],
                output_schema,
            },
            sink: SinkNode::results(),
        };

        Ok((
            ScanStateMachinePhase::CheckpointLeaf {
                filter_receiver: dedup_receiver,
            },
            ScanStateMachinePlan::CheckpointLeaf(plan),
        ))
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

    /// Create JSON checkpoint phase and plan using sender/receiver pattern.
    ///
    /// This plan:
    /// 1. Scans the JSON checkpoint file
    /// 2. Collects sidecar file paths via ConsumeByKDF (sees all rows before filtering)
    /// 3. Filters add/remove actions via FilterByKDF
    /// 4. Projects add actions to SCAN_ROW_SCHEMA
    /// 5. Streams results to caller
    ///
    /// After execution, if sidecars were collected, we transition to CheckpointLeaf phase.
    fn create_json_checkpoint_phase_and_plan(
        &self,
    ) -> DeltaResult<(ScanStateMachinePhase, ScanStateMachinePlan)> {
        use super::kdf_state::SidecarCollectorState;
        use crate::actions::get_all_actions_schema;
        use crate::scan::log_replay::{get_add_transform_expr, SCAN_ROW_SCHEMA};

        let schema = get_all_actions_schema();

        // For JSON checkpoint phase, create dedup state seeded from commit phase
        let dedup_state = match &self.state.commit_dedup_state {
            Some(commit_state) => FilterKdfState::AddRemoveDedup(commit_state.clone()),
            None => FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new()),
        };

        // Create sender/receiver pair for the dedup filter
        let (dedup_sender, dedup_receiver) = StateSender::build(dedup_state);

        // Create sender/receiver pair for sidecar collector
        let (sidecar_sender, sidecar_receiver) = StateSender::build(
            ConsumerKdfState::SidecarCollector(SidecarCollectorState::new(
                self.state.log_root().clone(),
            )),
        );

        let plan = JsonCheckpointPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: self.state.checkpoint_files.clone(),
                schema: schema.clone(),
            },
            sidecar_collector: sidecar_sender,
            partition_prune_filter: self.state.create_partition_prune_filter(),
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![get_add_transform_expr()],
                output_schema: SCAN_ROW_SCHEMA.clone(),
            },
            sink: SinkNode::results(),
        };

        Ok((
            ScanStateMachinePhase::JsonCheckpoint {
                filter_receiver: dedup_receiver,
                consumer_receiver: sidecar_receiver,
            },
            ScanStateMachinePlan::JsonCheckpoint(plan),
        ))
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

    /// Extract sidecar files from collected consumer states.
    fn extract_sidecar_files_from_states(
        &mut self,
        states: Vec<ConsumerKdfState>,
    ) -> DeltaResult<()> {
        for state in states {
            if let ConsumerKdfState::SidecarCollector(collector) = state {
                // Collect sidecar files from this state
                self.state.sidecar_files.extend(collector.get_sidecar_files().to_vec());
            }
        }
        Ok(())
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
        self.current_phase.phase_name()
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
