//! KdfFilterExec - applies FilterByKDF to filter rows.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_common::{Result as DfResult, DataFusionError};
use datafusion_physical_expr::EquivalenceProperties;
use futures::{Stream, StreamExt};

use delta_kernel::plans::FilterByKDF;
use delta_kernel::plans::kdf_state::FilterKdfState;

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
                child.output_partitioning().clone()
            } else {
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
            },
            ExecutionMode::Bounded,
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
        let kdf_state = Arc::clone(&self.kdf.state);
        
        Ok(Box::pin(KdfFilterStream {
            schema,
            input: child_stream,
            kdf_state,
        }))
    }
}

/// Stream that applies FilterKDF to batches.
struct KdfFilterStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    kdf_state: Arc<std::sync::Mutex<FilterKdfState>>,
}

impl Stream for KdfFilterStream {
    type Item = DfResult<RecordBatch>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Apply KDF filter
                let mut state = self.kdf_state.lock().unwrap();
                match state.apply(&batch) {
                    Ok(filtered_batch) => Poll::Ready(Some(Ok(filtered_batch))),
                    Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
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
    let state = kdf.state.lock().unwrap();
    match &*state {
        FilterKdfState::AddRemoveDedup(_) => false, // Stateful, needs serialization
        FilterKdfState::CheckpointDedup(_) => true, // Stateless, can parallelize
        FilterKdfState::PartitionPrune(_) => true,  // Stateless, can parallelize
    }
}


