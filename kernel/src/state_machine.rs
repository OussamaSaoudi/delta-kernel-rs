//! State machine architecture for multi-phase Delta kernel operations.

use crate::kernel_df::LogicalPlanNode;
use crate::DeltaResult;

/// Result of advancing a state machine phase.
pub enum StateMachinePhase<T> {
    /// Terminal state - operation complete, contains final result
    Terminus(T),
    /// Execute plan and yield partial query results (can be streamed)
    PartialResult(Box<dyn PartialResultPhase<Output = T>>),
    /// Execute plan and consume result as configuration (internal use)
    Consume(Box<dyn ConsumePhase<Output = T>>),
}

/// Phase that yields partial query results.
pub trait PartialResultPhase: Send + Sync {
    type Output;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode>;
    fn next(self: Box<Self>) -> DeltaResult<StateMachinePhase<Self::Output>>;
    fn is_parallelizable(&self) -> bool { false }
}

/// Phase that consumes configuration data.
pub trait ConsumePhase: Send + Sync {
    type Output;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode>;
    fn next(
        self: Box<Self>,
        data: Box<dyn Iterator<Item = DeltaResult<crate::arrow::array::RecordBatch>> + Send>
    ) -> DeltaResult<StateMachinePhase<Self::Output>>;
}

