//! KdfFilterExec - applies FilterByKDF to filter rows with declarative parallelism.
//!
//! # Parallelism Strategies
//!
//! This executor supports two parallelism strategies, determined by `ParallelismConfig`:
//!
//! ## Strategy 1: Merge-Based (Sequential Dedup) - Default
//!
//! - DataFusion optimizer inserts `SortPreservingMergeExec` to merge parallel streams
//! - Single-threaded deduplication with shared state
//! - Lower parallelism overhead, good for smaller tables
//!
//! ## Strategy 2: Hash-Partitioned (Parallel Dedup)
//!
//! - DataFusion optimizer inserts `RepartitionExec(Hash(path))` + `SortExec` per partition
//! - Parallel deduplication with per-partition state
//! - Higher throughput for large tables with many files
//!
//! # Declarative Requirements
//!
//! This exec declares its requirements via:
//! - `required_input_distribution()`: SinglePartition vs HashPartitioned(path)
//! - `required_input_ordering()`: version DESC within each partition
//!
//! DataFusion's optimizer automatically inserts the necessary operators.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, BooleanArray, PrimitiveArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{Int64Type, SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::Partitioning;
use futures::{Stream, StreamExt};

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::kdf_state::{
    AddRemoveDedupState, FilterKdfState, OwnedFilterState, StateSender,
};
use delta_kernel::plans::FilterByKDF;

use crate::executor::ParallelismConfig;
use crate::expr::lower_expression;

/// ExecutionPlan that applies a FilterByKDF to its child stream.
///
/// # Parallelism
///
/// This exec uses DataFusion's declarative parallelism model:
/// - Declares `required_input_distribution()` and `required_input_ordering()`
/// - Optimizer automatically inserts merge/repartition/sort operators
///
/// # State Management
///
/// - **Sequential mode**: Uses shared state from the KDF
/// - **Parallel mode**: Creates independent state per partition
#[derive(Debug)]
pub struct KdfFilterExec {
    child: Arc<dyn ExecutionPlan>,
    kdf: FilterByKDF,
    /// Parallelism configuration from the executor
    parallelism_config: ParallelismConfig,
    /// Cached decision: use partitioned strategy?
    use_partitioned: bool,
    /// Properties for this execution plan
    properties: PlanProperties,
}

impl KdfFilterExec {
    /// Create a new KdfFilterExec with the given parallelism configuration.
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        kdf: FilterByKDF,
        parallelism_config: ParallelismConfig,
    ) -> Self {
        // Determine strategy based on config and KDF capabilities
        let use_partitioned = Self::should_use_partitioned(&kdf, &parallelism_config);

        // Determine output partitioning based on strategy
        let output_partitioning = if use_partitioned {
            // Parallel mode: output has same partitioning as input (after repartition)
            // The optimizer will insert RepartitionExec before us
            Partitioning::UnknownPartitioning(parallelism_config.target_partitions)
        } else if Self::is_parallel_safe(&kdf) {
            // Stateless KDF: preserve input partitioning
            child.properties().output_partitioning().clone()
        } else {
            // Sequential mode: single partition output
            Partitioning::UnknownPartitioning(1)
        };

        // Preserve child's equivalence properties (including ordering) since
        // filter operations maintain input order (see maintains_input_order()).
        // This is critical for SortPreservingMergeExec to work correctly.
        let properties = PlanProperties::new(
            child.properties().equivalence_properties().clone(),
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            child,
            kdf,
            parallelism_config,
            use_partitioned,
            properties,
        }
    }

    /// Determine if we should use the partitioned strategy.
    fn should_use_partitioned(kdf: &FilterByKDF, config: &ParallelismConfig) -> bool {
        // Check if kernel says we CAN partition
        let can_partition = kdf.partitionable_by.is_some();

        // Check if config says we SHOULD partition
        let should_partition = config.enable_parallel_dedup;

        // Note: We can't check file count here since we don't have that info at plan time
        // The min_files_for_parallel check would need to happen at a higher level

        can_partition && should_partition
    }

    /// Determine if a KDF is parallel-safe (stateless).
    fn is_parallel_safe(kdf: &FilterByKDF) -> bool {
        match kdf.template() {
            FilterKdfState::AddRemoveDedup(_) => false, // Stateful
            FilterKdfState::CheckpointDedup(_) => true, // Stateless
            FilterKdfState::PartitionPrune(_) => true,  // Stateless
        }
    }

    /// Build the hash partition expression for DataFusion.
    ///
    /// Converts the kernel's `partitionable_by` expression to a DataFusion PhysicalExpr.
    fn build_partition_expr(
        &self,
    ) -> DfResult<Option<Arc<dyn datafusion_physical_expr::PhysicalExpr>>> {
        let Some(kernel_expr) = &self.kdf.partitionable_by else {
            return Ok(None);
        };

        // Lower the kernel expression to DataFusion logical expr
        let df_expr = lower_expression(kernel_expr, None, None).map_err(|e| {
            DataFusionError::Internal(format!("Failed to lower partition expression: {}", e))
        })?;

        // Convert to physical expr using child's schema
        let schema = self.child.schema();
        let df_schema = datafusion_common::DFSchema::try_from_qualified_schema("", &schema)?;

        // We need execution props - create default ones
        let execution_props = datafusion::execution::context::ExecutionProps::new();

        let physical_expr = datafusion_physical_expr::create_physical_expr(
            &df_expr,
            &df_schema,
            &execution_props,
        )?;

        Ok(Some(physical_expr))
    }

    /// Build the ordering requirement for DataFusion.
    ///
    /// Converts the kernel's `requires_ordering` to DataFusion's OrderingRequirements.
    fn build_ordering_requirement(
        &self,
    ) -> DfResult<Option<datafusion_physical_expr::OrderingRequirements>> {
        use datafusion_physical_expr::LexRequirement;
        use datafusion_physical_expr::OrderingRequirements;
        use datafusion_physical_expr::PhysicalSortRequirement;

        let Some(ordering) = &self.kdf.requires_ordering else {
            return Ok(None);
        };

        // Find the column in the schema
        let schema = self.child.schema();
        let col_name = ordering.column.to_string();

        let col_idx = schema.index_of(&col_name).map_err(|_| {
            DataFusionError::Internal(format!(
                "Ordering column '{}' not found in schema",
                col_name
            ))
        })?;

        // Create physical sort requirement
        let sort_req = PhysicalSortRequirement {
            expr: Arc::new(datafusion_physical_expr::expressions::Column::new(
                &col_name, col_idx,
            )),
            options: Some(arrow::compute::SortOptions {
                descending: ordering.descending,
                nulls_first: false,
            }),
        };

        // Wrap in LexRequirement first, then OrderingRequirements
        // LexRequirement::new returns None for empty vectors, but we always have one element
        let lex_req = LexRequirement::new(vec![sort_req])
            .expect("LexRequirement should be non-empty with one sort requirement");
        Ok(Some(OrderingRequirements::new(lex_req)))
    }
}

impl DisplayAs for KdfFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let strategy = if self.use_partitioned {
            "partitioned"
        } else if Self::is_parallel_safe(&self.kdf) {
            "parallel-safe"
        } else {
            "sequential"
        };
        write!(f, "KdfFilterExec[strategy={}]", strategy)
    }
}

impl ExecutionPlan for KdfFilterExec {
    fn name(&self) -> &str {
        "KdfFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.child.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(KdfFilterExec::new(
            Arc::clone(&children[0]),
            self.kdf.clone(),
            self.parallelism_config.clone(),
        )))
    }

    /// Declare the required input distribution.
    ///
    /// - **Partitioned mode**: HashPartitioned by path expression
    /// - **Sequential mode**: SinglePartition (merge all streams)
    /// - **Parallel-safe**: UnspecifiedDistribution (any distribution ok)
    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        use datafusion_physical_expr::Distribution;

        if self.use_partitioned {
            // Hash partition by path - optimizer will insert RepartitionExec
            if let Ok(Some(partition_expr)) = self.build_partition_expr() {
                return vec![Distribution::HashPartitioned(vec![partition_expr])];
            }
            // Fallback to single partition if expression building fails
            vec![Distribution::SinglePartition]
        } else if Self::is_parallel_safe(&self.kdf) {
            // Stateless KDF: any distribution is fine
            vec![Distribution::UnspecifiedDistribution]
        } else {
            // Stateful KDF in sequential mode: need single partition
            // Optimizer will insert SortPreservingMergeExec
            vec![Distribution::SinglePartition]
        }
    }

    /// Declare the required input ordering.
    ///
    /// If the KDF requires ordering (e.g., version DESC for AddRemoveDedup),
    /// this tells the optimizer to ensure that ordering.
    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion_physical_expr::OrderingRequirements>> {
        if let Ok(Some(requirements)) = self.build_ordering_requirement() {
            vec![Some(requirements)]
        } else {
            vec![None]
        }
    }

    /// Declare whether we maintain input order.
    ///
    /// Filter operations preserve ordering.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Validate partition number for non-partitioned mode
        if !self.use_partitioned && !Self::is_parallel_safe(&self.kdf) && partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "KdfFilterExec in sequential mode can only execute on partition 0, got {}",
                partition
            )));
        }

        let child_stream = self.child.execute(partition, context)?;
        let schema = child_stream.schema();

        // Create state based on strategy
        let state = if self.use_partitioned {
            // Parallel mode: create independent state per partition
            // Each partition gets a fresh AddRemoveDedupState
            match self.kdf.template() {
                FilterKdfState::AddRemoveDedup(_) => {
                    // Create a new sender just for this partition's state
                    let (sender, _receiver) =
                        StateSender::build(FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new()));
                    sender.create_owned()
                }
                _ => {
                    // For other KDF types, use the shared sender
                    self.kdf.create_owned()
                }
            }
        } else {
            // Sequential mode: use shared state from the KDF
            self.kdf.create_owned()
        };

        Ok(Box::pin(KdfFilterStream {
            schema,
            input: child_stream,
            state,
            selection_vector: None,
            last_seen_version: None,
        }))
    }
}

/// Stream that applies FilterKDF to batches with ordering validation.
///
/// Uses `OwnedState<FilterKdfState>` for zero-lock access during batch processing.
/// When the stream is dropped (on completion or error), the state is automatically
/// sent back to the `StateReceiver` for collection.
struct KdfFilterStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    /// Owned state - zero-lock access, sent back on drop
    state: OwnedFilterState,
    /// Reusable selection vector to minimize allocations
    selection_vector: Option<BooleanArray>,
    /// Track last seen version for ordering validation
    last_seen_version: Option<i64>,
}

impl Stream for KdfFilterStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // 1. VALIDATE ORDERING (if version column present)
                if let Some(version_col) = batch.column_by_name("version") {
                    if let Some(versions) =
                        version_col.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()
                    {
                        if !versions.is_empty() && !versions.is_null(0) {
                            let first_version = versions.value(0);
                            if let Some(last) = self.last_seen_version {
                                if first_version >= last {
                                    return Poll::Ready(Some(Err(DataFusionError::Internal(
                                        format!(
                                            "KDF ordering violation: expected version < {}, got {}. \
                                             Batches must be ordered by version DESC (newest first).",
                                            last, first_version
                                        ),
                                    ))));
                                }
                            }
                            self.last_seen_version = Some(first_version);
                        }
                    }
                }

                // 2. Initialize selection vector
                let batch_len = batch.num_rows();
                let selection = BooleanArray::from(vec![true; batch_len]);

                // 3. APPLY KDF FILTER - ZERO LOCKS via OwnedState
                let engine_data = ArrowEngineData::new(batch.clone());
                let filtered_selection = match self.state.apply(&engine_data, selection) {
                    Ok(sel) => sel,
                    Err(e) => {
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
                    }
                };

                // Store for potential reuse (future optimization)
                self.selection_vector = Some(filtered_selection.clone());

                // 4. FILTER BATCH using selection vector
                match filter_record_batch(&batch, &filtered_selection) {
                    Ok(filtered_batch) => Poll::Ready(Some(Ok(filtered_batch))),
                    Err(e) => Poll::Ready(Some(Err(DataFusionError::ArrowError(Box::new(e), None)))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for KdfFilterStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test that the strategy selection logic works correctly
    #[test]
    fn test_strategy_selection() {
        // AddRemoveDedup with parallel enabled should use partitioned
        let (kdf, _) = FilterByKDF::add_remove_dedup();
        let config = ParallelismConfig::default().with_parallel_dedup(true);
        assert!(KdfFilterExec::should_use_partitioned(&kdf, &config));

        // AddRemoveDedup with parallel disabled should not use partitioned
        let config_seq = ParallelismConfig::default().with_parallel_dedup(false);
        assert!(!KdfFilterExec::should_use_partitioned(&kdf, &config_seq));

        // CheckpointDedup should never use partitioned (no partitionable_by)
        let (kdf_cp, _) = FilterByKDF::checkpoint_dedup();
        assert!(!KdfFilterExec::should_use_partitioned(&kdf_cp, &config));
    }

    #[test]
    fn test_parallel_safe_detection() {
        let (kdf_add, _) = FilterByKDF::add_remove_dedup();
        assert!(!KdfFilterExec::is_parallel_safe(&kdf_add));

        let (kdf_cp, _) = FilterByKDF::checkpoint_dedup();
        assert!(KdfFilterExec::is_parallel_safe(&kdf_cp));
    }
}

/// Example code showing how to print before/after optimization of a kernel plan.
///
/// Run with: `cargo test --package delta_kernel_datafusion --lib -- print_plan_demo --nocapture`
#[cfg(test)]
mod optimization_demo {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::physical_plan::displayable;
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;

    /// Creates a mock scan with multiple partitions (simulating parallel file reads)
    fn create_mock_scan(num_partitions: usize) -> Arc<dyn ExecutionPlan> {
        // Schema matching Delta log replay: add.path, remove.path, version
        let add_fields: Fields = vec![Field::new("path", DataType::Utf8, true)].into();
        let remove_fields: Fields = vec![Field::new("path", DataType::Utf8, true)].into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields), true),
            Field::new("remove", DataType::Struct(remove_fields), true),
            Field::new("version", DataType::Int64, false),
        ]));

        // PlaceholderRowExec is used to create a simple mock scan
        Arc::new(PlaceholderRowExec::new(schema).with_partitions(num_partitions))
    }

    /// Prints a plan tree with nice formatting
    fn format_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}", displayable(plan.as_ref()).indent(true))
    }

    /// Single test that demonstrates both strategies to avoid interleaved output
    #[test]
    fn print_plan_demo() {
        let mut output = String::new();

        // =====================================================================
        // SEQUENTIAL STRATEGY
        // =====================================================================
        output.push_str("\n\n");
        output.push_str("╔══════════════════════════════════════════════════════════════════════╗\n");
        output.push_str("║   DEMO: Sequential Dedup (Merge-Based) - Default Strategy            ║\n");
        output.push_str("╚══════════════════════════════════════════════════════════════════════╝\n");

        let scan = create_mock_scan(4);
        let (kdf, _receiver) = FilterByKDF::add_remove_dedup();
        let parallelism_config = ParallelismConfig::default();
        let kdf_exec: Arc<dyn ExecutionPlan> =
            Arc::new(KdfFilterExec::new(scan, kdf, parallelism_config));

        output.push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("Unoptimized Plan\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str(&format_plan(&kdf_exec));

        output.push_str("\n┌─────────────────────────────────────────────────────────────────────┐\n");
        output.push_str("│ DECLARED REQUIREMENTS (what KdfFilterExec tells the optimizer)      │\n");
        output.push_str("├─────────────────────────────────────────────────────────────────────┤\n");
        output.push_str(&format!(
            "│ required_input_distribution: {:?}\n",
            kdf_exec.required_input_distribution()
        ));
        output.push_str(&format!(
            "│ required_input_ordering:     {:?}\n",
            kdf_exec.required_input_ordering()
        ));
        output.push_str(&format!(
            "│ maintains_input_order:       {:?}\n",
            kdf_exec.maintains_input_order()
        ));
        output.push_str("└─────────────────────────────────────────────────────────────────────┘\n");

        output.push_str("\nWhat DataFusion optimizer will do:\n");
        output.push_str("  1. See required_input_distribution = SinglePartition\n");
        output.push_str("     → Insert CoalescePartitionsExec or SortPreservingMergeExec\n");
        output.push_str("  2. See required_input_ordering = [version DESC]\n");
        output.push_str("     → Insert SortExec if not already sorted\n\n");
        output.push_str("Expected optimized plan structure:\n");
        output.push_str("  KdfFilterExec[strategy=sequential]\n");
        output.push_str("    └─ SortPreservingMergeExec[version DESC]  ← merges parallel streams\n");
        output.push_str("         └─ ScanExec[4 partitions]             ← parallel file reads\n");

        // =====================================================================
        // PARALLEL STRATEGY
        // =====================================================================
        output.push_str("\n\n");
        output.push_str("╔══════════════════════════════════════════════════════════════════════╗\n");
        output.push_str("║   DEMO: Parallel Dedup (Hash-Partitioned) - Opt-in Strategy          ║\n");
        output.push_str("╚══════════════════════════════════════════════════════════════════════╝\n");

        let scan = create_mock_scan(4);
        let (kdf, _receiver) = FilterByKDF::add_remove_dedup();
        let parallelism_config = ParallelismConfig::default()
            .with_parallel_dedup(true)
            .with_target_partitions(4);
        let kdf_exec: Arc<dyn ExecutionPlan> =
            Arc::new(KdfFilterExec::new(scan, kdf, parallelism_config));

        output.push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("Unoptimized Plan\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str(&format_plan(&kdf_exec));

        output.push_str("\n┌─────────────────────────────────────────────────────────────────────┐\n");
        output.push_str("│ DECLARED REQUIREMENTS (what KdfFilterExec tells the optimizer)      │\n");
        output.push_str("├─────────────────────────────────────────────────────────────────────┤\n");
        output.push_str(&format!(
            "│ required_input_distribution: HashPartitioned(coalesce(add.path, remove.path))\n"
        ));
        output.push_str(&format!(
            "│ required_input_ordering:     [version DESC]\n"
        ));
        output.push_str(&format!(
            "│ maintains_input_order:       [true]\n"
        ));
        output.push_str("└─────────────────────────────────────────────────────────────────────┘\n");

        output.push_str("\nWhat DataFusion optimizer will do:\n");
        output.push_str("  1. See required_input_distribution = HashPartitioned(coalesce(add.path, remove.path))\n");
        output.push_str("     → Insert RepartitionExec to redistribute by path hash\n");
        output.push_str("  2. See required_input_ordering = [version DESC]\n");
        output.push_str("     → Insert SortExec per partition\n\n");
        output.push_str("Expected optimized plan structure:\n");
        output.push_str("  KdfFilterExec[strategy=partitioned]\n");
        output.push_str("    └─ SortExec[version DESC]              ← sort per partition\n");
        output.push_str("         └─ RepartitionExec[Hash(path)]     ← redistribute by path\n");
        output.push_str("              └─ ScanExec[4 partitions]     ← parallel file reads\n");

        // Print all at once to avoid interleaving
        println!("{}", output);
    }

    /// Demo showing a full CommitPhasePlan with DataSkipping and Dedup
    /// This simulates: Scan → DataSkipping → AddRemoveDedup → Project
    /// 
    /// Run with: `cargo test --package delta_kernel_datafusion --lib -- print_full_commit --nocapture`
    #[test]
    fn print_full_commit_plan_demo() {
        use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
        use datafusion::physical_plan::filter::FilterExec;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion_common::config::ConfigOptions;
        use datafusion_common::ScalarValue;
        use datafusion_physical_expr::expressions::{Column, Literal};
        use datafusion_physical_expr::PhysicalExpr;

        let mut output = String::new();

        // Config for running optimizer rules
        let config = ConfigOptions::default();

        /// Helper to run DataFusion's physical optimizer on a plan
        fn optimize_plan(
            plan: Arc<dyn ExecutionPlan>,
            config: &ConfigOptions,
        ) -> Arc<dyn ExecutionPlan> {
            let optimizer = PhysicalOptimizer::default();
            let mut result = plan;
            for rule in optimizer.rules.iter() {
                result = rule.optimize(result, config).expect("optimization failed");
            }
            result
        }

        // =====================================================================
        // FULL COMMIT PLAN - SEQUENTIAL (DEFAULT)
        // =====================================================================
        output.push_str("\n\n");
        output.push_str("╔══════════════════════════════════════════════════════════════════════╗\n");
        output.push_str("║   FULL COMMIT PLAN: Scan → DataSkipping → Dedup → Project            ║\n");
        output.push_str("║                     (Sequential Strategy - Default)                  ║\n");
        output.push_str("╚══════════════════════════════════════════════════════════════════════╝\n");

        // Build the plan tree bottom-up:
        // Scan(4 partitions) → DataSkipping(parallel-safe) → Dedup(stateful) → Project
        let scan = create_mock_scan(4);

        // DataSkipping is just a regular filter - parallel-safe, no ordering requirements
        let dataskipping_filter = Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>;
        let after_dataskipping: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(dataskipping_filter, scan).unwrap());

        // AddRemoveDedup - stateful, requires version DESC ordering
        let (kdf, _receiver) = FilterByKDF::add_remove_dedup();
        let parallelism_config = ParallelismConfig::default(); // sequential
        let after_dedup: Arc<dyn ExecutionPlan> =
            Arc::new(KdfFilterExec::new(after_dataskipping.clone(), kdf, parallelism_config));

        // Project (just pass through for demo)
        let project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("add", 0)), "add".to_string()),
            (Arc::new(Column::new("remove", 1)), "remove".to_string()),
        ];
        let unoptimized_plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(project_exprs.clone(), after_dedup).unwrap());

        output.push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("BEFORE Optimization\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str(&format_plan(&unoptimized_plan));

        // Run the optimizer
        let optimized_plan = optimize_plan(unoptimized_plan, &config);

        output.push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("AFTER Optimization (DataFusion inserted Sort/Merge operators)\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str(&format_plan(&optimized_plan));

        // =====================================================================
        // FULL COMMIT PLAN - PARALLEL
        // =====================================================================
        output.push_str("\n\n");
        output.push_str("╔══════════════════════════════════════════════════════════════════════╗\n");
        output.push_str("║   FULL COMMIT PLAN: Scan → DataSkipping → Dedup → Project            ║\n");
        output.push_str("║                     (Parallel Strategy - Opt-in)                     ║\n");
        output.push_str("╚══════════════════════════════════════════════════════════════════════╝\n");

        // Same structure but with parallel config
        let scan = create_mock_scan(4);
        let dataskipping_filter = Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>;
        let after_dataskipping: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(dataskipping_filter, scan).unwrap());

        let (kdf, _receiver) = FilterByKDF::add_remove_dedup();
        let parallelism_config = ParallelismConfig::default()
            .with_parallel_dedup(true)
            .with_target_partitions(4);
        let after_dedup: Arc<dyn ExecutionPlan> =
            Arc::new(KdfFilterExec::new(after_dataskipping.clone(), kdf, parallelism_config));

        let unoptimized_plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(project_exprs, after_dedup).unwrap());

        output.push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("BEFORE Optimization\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str(&format_plan(&unoptimized_plan));

        // Run the optimizer
        let optimized_plan = optimize_plan(unoptimized_plan, &config);

        output.push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("AFTER Optimization (DataFusion inserted Repartition/Sort operators)\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str(&format_plan(&optimized_plan));

        println!("{}", output);
    }
}
