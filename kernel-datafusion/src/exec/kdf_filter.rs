//! KdfFilterExec - applies FilterByKDF to filter rows with zero-lock streaming.
//!
//! ORDERING REQUIREMENT: This exec requires input batches to be ordered by
//! version in DESCENDING order (newest first). Violating this will cause
//! incorrect deduplication results.
//!
//! Enforcement: Single partition + runtime validation.
//!
//! ARCHITECTURE: Uses `OwnedState<FilterKdfState>` for zero-lock access during
//! batch processing. Each partition gets its own cloned state via `create_owned()`.
//! When the stream completes (dropped), states are automatically sent back to the
//! `StateReceiver` held by the state machine for collection and merging.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::{SchemaRef as ArrowSchemaRef, Int64Type};
use arrow::record_batch::RecordBatch;
use arrow::array::{BooleanArray, Array, PrimitiveArray};
use arrow::compute::filter_record_batch;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_physical_plan::execution_plan::{EmissionType, Boundedness};
use datafusion_physical_plan::Partitioning;
use datafusion_common::{Result as DfResult, DataFusionError};
use datafusion_physical_expr::EquivalenceProperties;
use futures::{Stream, StreamExt};

use delta_kernel::plans::FilterByKDF;
use delta_kernel::plans::kdf_state::{FilterKdfState, OwnedFilterState};
use delta_kernel::engine::arrow_data::ArrowEngineData;

/// ExecutionPlan that applies a FilterByKDF to its child stream.
///
/// FilterByKDF produces a selection vector per batch, keeping only the selected rows.
/// Concurrency: Some KDFs (AddRemoveDedup) are stateful and require single-threaded execution.
/// Others (CheckpointDedup) are parallel-safe.
#[derive(Debug)]
pub struct KdfFilterExec {
    child: Arc<dyn ExecutionPlan>,
    kdf: FilterByKDF,
    /// Whether this KDF can be parallelized across partitions
    is_parallel_safe: bool,
    properties: PlanProperties,
}

impl KdfFilterExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, kdf: FilterByKDF) -> Self {
        // Determine if this KDF can be parallelized
        let is_parallel_safe = is_filter_kdf_parallel_safe(&kdf);
        
        let properties = PlanProperties::new(
            EquivalenceProperties::new(child.schema()),
            // Single partition if not parallel-safe
            if is_parallel_safe {
                child.properties().output_partitioning().clone()
            } else {
                Partitioning::UnknownPartitioning(1)
            },
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        
        Self {
            child,
            kdf,
            is_parallel_safe,
            properties,
        }
    }
}

impl DisplayAs for KdfFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KdfFilterExec[parallel={}]", self.is_parallel_safe)
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
        )))
    }
    
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if !self.is_parallel_safe && partition != 0 {
            return Err(DataFusionError::Internal(
                "KdfFilterExec with non-parallel-safe KDF can only execute on partition 0".to_string()
            ));
        }
        
        let child_stream = self.child.execute(partition, context)?;
        let schema = child_stream.schema();
        
        // Create owned state from the sender - zero-lock access during streaming
        // State is cloned from template and will be sent back to receiver on drop
        let state = self.kdf.create_owned();
        
        Ok(Box::pin(KdfFilterStream {
            schema,
            input: child_stream,
            state,
            selection_vector: None, // Allocated on first batch
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
                    if let Some(versions) = version_col.as_any().downcast_ref::<PrimitiveArray<Int64Type>>() {
                        if versions.len() > 0 && !versions.is_null(0) {
                            let first_version = versions.value(0);
                            if let Some(last) = self.last_seen_version {
                                if first_version >= last {
                                    return Poll::Ready(Some(Err(DataFusionError::Internal(
                                        format!(
                                            "KDF ordering violation: expected version < {}, got {}. \
                                             Batches must be ordered by version DESC (newest first).",
                                            last, first_version
                                        )
                                    ))));
                                }
                            }
                            self.last_seen_version = Some(first_version);
                        }
                    }
                }
                
                // 2. Initialize or reuse selection vector
                let batch_len = batch.num_rows();
                let selection = BooleanArray::from(vec![true; batch_len]);
                
                // 3. APPLY KDF FILTER - ZERO LOCKS via OwnedState
                let engine_data = ArrowEngineData::new(batch.clone());
                let filtered_selection = match self.state.apply(&engine_data, selection) {
                    Ok(sel) => sel,
                    Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
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

/// Determine if a FilterKDF is parallel-safe.
fn is_filter_kdf_parallel_safe(kdf: &FilterByKDF) -> bool {
    // Access the template state from the sender to check the variant
    match kdf.template() {
        FilterKdfState::AddRemoveDedup(_) => false, // Stateful, needs serialization
        FilterKdfState::CheckpointDedup(_) => true, // Stateless, can parallelize
        FilterKdfState::PartitionPrune(_) => true,  // Stateless, can parallelize
    }
}

