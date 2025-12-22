//! DataFusion executor implementation.

use datafusion::execution::{
    runtime_env::RuntimeEnv, SessionState, SessionStateBuilder, TaskContext,
};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream};
use datafusion_common::config::ConfigOptions;
use std::sync::Arc;
use tracing::{debug, info};

use crate::compile::compile_plan;
use crate::error::DfResult;
use delta_kernel::plans::DeclarativePlanNode;

// =============================================================================
// Parallelism Configuration
// =============================================================================

/// Configuration for controlling parallelism strategies in Delta operations.
///
/// This configuration determines how the executor handles stateful operations
/// like `AddRemoveDedup` during log replay.
///
/// # Strategies
///
/// **Sequential (Merge-Based)** - Default, `enable_parallel_dedup = false`:
/// - Files are read in parallel across multiple partitions
/// - `SortPreservingMergeExec` merges into a single ordered stream
/// - Single-threaded deduplication with shared state
///
/// **Parallel (Hash-Partitioned)** - `enable_parallel_dedup = true`:
/// - Data is hash-partitioned by file path
/// - Each partition is sorted independently (version DESC)
/// - Parallel deduplication with per-partition state
/// - Results are combined via `CoalescePartitionsExec`
///
/// # Example
///
/// ```ignore
/// let executor = DataFusionExecutor::new()?
///     .with_parallel_dedup(true)
///     .with_min_files_for_parallel(20);
/// ```
#[derive(Clone, Debug)]
pub struct ParallelismConfig {
    /// Enable hash-partitioned parallel dedup.
    ///
    /// When `true`, `AddRemoveDedup` uses `Distribution::HashPartitioned(path)`
    /// to process different paths in parallel.
    ///
    /// When `false` (default), uses `Distribution::SinglePartition` with
    /// `SortPreservingMergeExec` to merge all data into one stream.
    pub enable_parallel_dedup: bool,

    /// Minimum file count to use parallel dedup (when enabled).
    ///
    /// Below this threshold, sequential execution is used even if
    /// `enable_parallel_dedup` is true. This avoids parallelism overhead
    /// for small tables.
    pub min_files_for_parallel: usize,

    /// Target partition count for parallel dedup.
    ///
    /// When parallel dedup is enabled, this controls how many partitions
    /// the data is split into. More partitions = more parallelism but
    /// also more overhead.
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
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable parallel dedup.
    pub fn with_parallel_dedup(mut self, enabled: bool) -> Self {
        self.enable_parallel_dedup = enabled;
        self
    }

    /// Set minimum file count for parallel dedup.
    pub fn with_min_files_for_parallel(mut self, min_files: usize) -> Self {
        self.min_files_for_parallel = min_files;
        self
    }

    /// Set target partition count.
    pub fn with_target_partitions(mut self, partitions: usize) -> Self {
        self.target_partitions = partitions;
        self
    }
}

// =============================================================================
// DataFusion Executor
// =============================================================================

/// DataFusion-based executor for Delta Kernel plans.
///
/// This executor compiles `DeclarativePlanNode` trees into DataFusion physical plans
/// and executes them asynchronously.
///
/// # Parallelism Control
///
/// The executor's `parallelism_config` controls how stateful operations are handled:
///
/// ```ignore
/// // Default: sequential dedup (safe, predictable)
/// let executor = DataFusionExecutor::new()?;
///
/// // Opt-in: parallel dedup (higher throughput for large tables)
/// let executor = DataFusionExecutor::new()?
///     .with_parallel_dedup(true);
/// ```
pub struct DataFusionExecutor {
    session_state: SessionState,
    /// Configuration for parallelism strategies
    parallelism_config: ParallelismConfig,
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

        Ok(Self {
            session_state,
            parallelism_config: ParallelismConfig::default(),
        })
    }

    /// Create a new executor with custom session state.
    pub fn with_session_state(session_state: SessionState) -> Self {
        Self {
            session_state,
            parallelism_config: ParallelismConfig::default(),
        }
    }

    /// Enable or disable parallel dedup for stateful KDFs.
    ///
    /// When enabled, `AddRemoveDedup` will use hash-partitioning by path
    /// to process different files in parallel. Each partition maintains
    /// its own state and sorts independently.
    ///
    /// Default: `false` (sequential dedup with merge-based ordering)
    pub fn with_parallel_dedup(mut self, enabled: bool) -> Self {
        self.parallelism_config.enable_parallel_dedup = enabled;
        self
    }

    /// Set the minimum file count for parallel dedup.
    ///
    /// When `enable_parallel_dedup` is true, this threshold determines
    /// whether to actually use parallel execution. Tables with fewer
    /// files than this threshold will use sequential execution.
    ///
    /// Default: 10
    pub fn with_min_files_for_parallel(mut self, min_files: usize) -> Self {
        self.parallelism_config.min_files_for_parallel = min_files;
        self
    }

    /// Set the target partition count for parallel operations.
    ///
    /// Default: number of CPU cores
    pub fn with_target_partitions(mut self, partitions: usize) -> Self {
        self.parallelism_config.target_partitions = partitions;
        self
    }

    /// Set the parallelism configuration.
    pub fn with_parallelism_config(mut self, config: ParallelismConfig) -> Self {
        self.parallelism_config = config;
        self
    }

    /// Get the parallelism configuration.
    pub fn parallelism_config(&self) -> &ParallelismConfig {
        &self.parallelism_config
    }

    /// Compile a declarative plan into a DataFusion physical plan.
    pub fn compile(&self, plan: &DeclarativePlanNode) -> DfResult<Arc<dyn ExecutionPlan>> {
        compile_plan(plan, &self.session_state, &self.parallelism_config)
    }

    /// Run DataFusion's physical optimizer on a plan.
    ///
    /// This applies optimizer rules like `EnforceDistribution` and `EnforceSorting`
    /// which insert `RepartitionExec`, `SortExec`, and `SortPreservingMergeExec`
    /// based on the declared requirements of each operator.
    pub fn optimize(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let optimizer = PhysicalOptimizer::default();
        let config = self.session_state.config_options();
        let mut result = plan;
        for rule in optimizer.rules.iter() {
            result = rule.optimize(result, config)?;
        }
        Ok(result)
    }

    /// Execute a declarative plan and return a stream of RecordBatches.
    ///
    /// This method:
    /// 1. Compiles the kernel's DeclarativePlanNode into a DataFusion ExecutionPlan
    /// 2. Runs DataFusion's physical optimizer (inserts Sort/Repartition operators)
    /// 3. Coalesces multiple partitions if needed (ensures parallel execution)
    /// 4. Returns a single stream of results
    pub async fn execute_to_stream(
        &self,
        plan: DeclarativePlanNode,
    ) -> DfResult<SendableRecordBatchStream> {
        // Step 1: Log the kernel plan
        debug!("=== KERNEL DECLARATIVE PLAN ===");
        debug!("{:#?}", plan);

        // Step 2: Compile to DataFusion physical plan
        let physical_plan = self.compile(&plan)?;
        info!("=== DATAFUSION PLAN (before optimization) ===");
        info!("\n{}", displayable(physical_plan.as_ref()).indent(true));

        // Step 3: Run the physical optimizer
        let optimized_plan = self.optimize(physical_plan)?;
        info!("=== DATAFUSION PLAN (after optimization) ===");
        info!("\n{}", displayable(optimized_plan.as_ref()).indent(true));

        // Step 4: Handle multiple partitions by coalescing
        let output_partitions = optimized_plan.output_partitioning().partition_count();
        debug!(
            "Output partitioning: {} partition(s)",
            output_partitions
        );

        let final_plan: Arc<dyn ExecutionPlan> = if output_partitions == 1 {
            optimized_plan
        } else {
            // Wrap with CoalescePartitionsExec to merge all partitions.
            // This ensures parallel execution: DataFusion spawns a task for each
            // child partition and merges results into a single stream.
            info!(
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
