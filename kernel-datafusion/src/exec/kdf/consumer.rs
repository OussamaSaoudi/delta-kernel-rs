//! ConsumeKdfExec - applies ConsumeByKDF for side effects and early termination.

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
use tokio::sync::Mutex as TokioMutex;

use delta_kernel::plans::ConsumeByKDF;
use delta_kernel::plans::kdf_state::ConsumerKdfState;

/// ExecutionPlan that applies a ConsumeByKDF to its child stream.
///
/// ConsumeByKDF processes batches for side effects and returns a boolean:
/// - true: continue streaming (pass batch through)
/// - false: break (stop iteration, no more batches)
///
/// This is used for e.g. LogSegmentBuilder, which accumulates file metadata.
#[derive(Debug)]
pub struct ConsumeKdfExec {
    child: Arc<dyn ExecutionPlan>,
    kdf: ConsumeByKDF,
    properties: PlanProperties,
}

impl ConsumeKdfExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, kdf: ConsumeByKDF) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(child.schema()),
            child.output_partitioning().clone(),
            ExecutionMode::Bounded,
        );
        
        Self {
            child,
            kdf,
            properties,
        }
    }
}

impl DisplayAs for ConsumeKdfExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConsumeKdfExec")
    }
}

impl ExecutionPlan for ConsumeKdfExec {
    fn name(&self) -> &str {
        "ConsumeKdfExec"
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
        Ok(Arc::new(ConsumeKdfExec::new(
            Arc::clone(&children[0]),
            self.kdf.clone(),
        )))
    }
    
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let schema = child_stream.schema();
        let kdf_state = Arc::new(TokioMutex::new(self.kdf.state.clone()));
        
        Ok(Box::pin(ConsumeKdfStream {
            schema,
            input: child_stream,
            kdf_state,
            should_continue: true,
        }))
    }
}

/// Stream that applies ConsumeByKDF to batches.
struct ConsumeKdfStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    kdf_state: Arc<TokioMutex<ConsumerKdfState>>,
    should_continue: bool,
}

impl Stream for ConsumeKdfStream {
    type Item = DfResult<RecordBatch>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.should_continue {
            return Poll::Ready(None);
        }
        
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Apply consumer KDF - use try_lock to avoid blocking
                let kdf_state = Arc::clone(&self.kdf_state);
                
                match kdf_state.try_lock() {
                    Ok(mut state) => {
                        match state.apply(&batch) {
                            Ok(should_continue) => {
                                self.should_continue = should_continue;
                                Poll::Ready(Some(Ok(batch)))
                            }
                            Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
                        }
                    }
                    Err(_) => {
                        // Lock contention - wake and try again
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ConsumeKdfStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}


