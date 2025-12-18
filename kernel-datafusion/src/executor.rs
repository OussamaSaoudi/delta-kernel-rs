//! DataFusion executor implementation.

use std::sync::Arc;
use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext, runtime_env::RuntimeEnv};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_common::config::ConfigOptions;

use crate::error::{DfResult, DfError};
use crate::compile::compile_plan;
use delta_kernel::plans::DeclarativePlanNode;

/// DataFusion-based executor for Delta Kernel plans.
///
/// This executor compiles `DeclarativePlanNode` trees into DataFusion physical plans
/// and executes them asynchronously.
pub struct DataFusionExecutor {
    session_state: SessionState,
}

impl DataFusionExecutor {
    /// Create a new DataFusion executor with default configuration.
    pub fn new() -> DfResult<Self> {
        let config = ConfigOptions::default();
        let runtime = Arc::new(RuntimeEnv::default());
        let session_state = SessionStateBuilder::new()
            .with_config(config.into())
            .with_runtime_env(runtime)
            .build();
        
        Ok(Self { session_state })
    }
    
    /// Create a new executor with custom session state.
    pub fn with_session_state(session_state: SessionState) -> Self {
        Self { session_state }
    }
    
    /// Compile a declarative plan into a DataFusion physical plan.
    pub fn compile(&self, plan: &DeclarativePlanNode) -> DfResult<Arc<dyn ExecutionPlan>> {
        compile_plan(plan, &self.session_state)
    }
    
    /// Execute a declarative plan and return a stream of RecordBatches.
    pub async fn execute_to_stream(
        &self,
        plan: DeclarativePlanNode,
    ) -> DfResult<SendableRecordBatchStream> {
        let physical_plan = self.compile(&plan)?;
        let task_ctx = Arc::new(TaskContext::from(&self.session_state));
        
        // Execute partition 0 (we'll handle partitioning at compile time)
        let stream = physical_plan.execute(0, task_ctx)?;
        Ok(stream)
    }
    
    /// Get a reference to the session state.
    pub fn session_state(&self) -> &SessionState {
        &self.session_state
    }
}

impl Default for DataFusionExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create default DataFusion executor")
    }
}

