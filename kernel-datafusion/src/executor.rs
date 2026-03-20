//! DataFusion executor implementation.

use datafusion::execution::{
    runtime_env::RuntimeEnv, SessionState, SessionStateBuilder, TaskContext,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use datafusion_common::config::ConfigOptions;
use std::sync::Arc;
use tracing::debug;

use crate::compile::compile_plan;
use crate::error::DfResult;
use delta_kernel::plans::DeclarativePlanNode;

/// Configuration for controlling parallelism strategies in Delta operations.
#[derive(Clone, Debug)]
pub struct ParallelismConfig {
    /// Enable hash-partitioned parallel dedup.
    pub enable_parallel_dedup: bool,
    /// Minimum file count to use parallel dedup (when enabled).
    pub min_files_for_parallel: usize,
    /// Target partition count for parallel dedup.
    pub target_partitions: usize,
}

impl Default for ParallelismConfig {
    fn default() -> Self {
        Self {
            enable_parallel_dedup: false, // Safe default: sequential
            min_files_for_parallel: 10,   // Only parallelize for larger tables
            target_partitions: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        }
    }
}

impl ParallelismConfig {
    pub fn new() -> Self { Self::default() }
    pub fn with_parallel_dedup(mut self, enabled: bool) -> Self { self.enable_parallel_dedup = enabled; self }
    pub fn with_min_files_for_parallel(mut self, min_files: usize) -> Self { self.min_files_for_parallel = min_files; self }
    pub fn with_target_partitions(mut self, partitions: usize) -> Self { self.target_partitions = partitions; self }
}

/// DataFusion-based executor for Delta Kernel plans.
pub struct DataFusionExecutor {
    session_state: SessionState,
    /// Configuration for parallelism strategies
    parallelism_config: ParallelismConfig,
}

impl DataFusionExecutor {
    pub fn new() -> DfResult<Self> {
        let config = ConfigOptions::default();
        let runtime = Arc::new(RuntimeEnv::default());
        let session_state = SessionStateBuilder::new()
            .with_config(config.into())
            .with_runtime_env(runtime)
            .build();

        Ok(Self {
            session_state,
            parallelism_config: ParallelismConfig::default(),
        })
    }

    pub fn with_session_state(session_state: SessionState) -> Self {
        Self {
            session_state,
            parallelism_config: ParallelismConfig::default(),
        }
    }

    pub fn with_parallel_dedup(mut self, enabled: bool) -> Self {
        self.parallelism_config.enable_parallel_dedup = enabled;
        self
    }

    pub fn with_min_files_for_parallel(mut self, min_files: usize) -> Self {
        self.parallelism_config.min_files_for_parallel = min_files;
        self
    }

    pub fn with_target_partitions(mut self, partitions: usize) -> Self {
        self.parallelism_config.target_partitions = partitions;
        self
    }

    pub fn with_parallelism_config(mut self, config: ParallelismConfig) -> Self { self.parallelism_config = config; self }
    pub fn parallelism_config(&self) -> &ParallelismConfig { &self.parallelism_config }

    /// Compile a declarative plan into a DataFusion physical plan.
    pub fn compile(&self, plan: &DeclarativePlanNode) -> DfResult<Arc<dyn ExecutionPlan>> {
        compile_plan(plan, &self.session_state, &self.parallelism_config)
    }

    /// Run DataFusion's physical optimizer on a plan.
    pub fn optimize(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let config = self.session_state.config_options();
        let mut result = plan;

        // Use physical_optimizers() from SessionState, NOT PhysicalOptimizer::default()
        for rule in self.session_state.physical_optimizers() {
            result = rule.optimize(result, config)?;
        }

        Ok(result)
    }

    /// Execute a declarative plan and return a stream of RecordBatches.
    pub async fn execute_to_stream(
        &self,
        plan: DeclarativePlanNode,
    ) -> DfResult<SendableRecordBatchStream> {
        // Step 1: Log the kernel plan
        debug!("=== KERNEL DECLARATIVE PLAN ===");
        debug!("{:#?}", plan);

        // Step 2: Compile to DataFusion physical plan
        let physical_plan = self.compile(&plan)?;
        debug!("=== DATAFUSION PLAN (before optimization) ===");
        debug!("\n{}", displayable(physical_plan.as_ref()).indent(true));

        // Step 3: Run the physical optimizer
        let optimized_plan = self.optimize(physical_plan)?;
        debug!("=== DATAFUSION PLAN (after optimization) ===");
        debug!("\n{}", displayable(optimized_plan.as_ref()).indent(true));

        // Step 4: Handle multiple partitions by coalescing
        let output_partitions = optimized_plan.output_partitioning().partition_count();
        debug!("Output partitioning: {} partition(s)", output_partitions);

        let final_plan: Arc<dyn ExecutionPlan> = if output_partitions == 1 {
            optimized_plan
        } else {
            // Wrap with CoalescePartitionsExec to merge all partitions.
            // This ensures parallel execution: DataFusion spawns a task for each
            // child partition and merges results into a single stream.
            debug!(
                "Coalescing {} partitions for parallel execution",
                output_partitions
            );
            Arc::new(CoalescePartitionsExec::new(optimized_plan))
        };

        // Step 5: Execute
        let task_ctx = Arc::new(TaskContext::from(&self.session_state));
        let stream = final_plan.execute(0, task_ctx)?;
        Ok(stream)
    }

    pub fn session_state(&self) -> &SessionState {
        &self.session_state
    }
}

impl Default for DataFusionExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create default DataFusion executor")
    }
}
