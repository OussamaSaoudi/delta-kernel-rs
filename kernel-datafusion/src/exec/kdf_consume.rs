//! ConsumeKdfExec - applies ConsumeByKDF for side effects and early termination.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::{Stream, StreamExt};

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::kdf_state::{ConsumerKdfState, ConsumerStateSender, OwnedState};

/// ExecutionPlan that applies a ConsumerByKDF to its child stream.
///
/// ConsumerByKDF processes batches for side effects and returns a boolean:
/// - true: continue streaming (pass batch through)
/// - false: break (stop iteration, no more batches)
///
/// This is used for e.g. LogSegmentBuilder, which accumulates file metadata.
#[derive(Debug)]
pub struct ConsumeKdfExec {
    child: Arc<dyn ExecutionPlan>,
    kdf: ConsumerStateSender,
    properties: PlanProperties,
}

impl ConsumeKdfExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, kdf: ConsumerStateSender) -> Self {
        // Consumer KDFs accumulate state and must execute as a single partition.
        // This ensures:
        // 1. Only one OwnedState is created (via create_owned())
        // 2. All data flows through that single state
        // 3. The filled state is sent to the receiver when the stream drops
        //
        // If the child has multiple partitions, DataFusion's optimizer will
        // insert a CoalescePartitionsExec before us to merge data.
        let properties = PlanProperties::new(
            EquivalenceProperties::new(child.schema()),
            Partitioning::UnknownPartitioning(1), // Single partition output
            EmissionType::Incremental,
            Boundedness::Bounded,
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

    /// Declare the required input distribution.
    ///
    /// Consumer KDFs accumulate state and need all input data in a single partition.
    /// This tells the optimizer to insert CoalescePartitionsExec (not RepartitionExec)
    /// before this node when the child has multiple partitions.
    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        vec![datafusion_physical_expr::Distribution::SinglePartition]
    }

    /// Declare the required input ordering.
    ///
    /// Consumer KDFs no longer track ordering requirements directly.
    /// Ordering is enforced via Union structure in the plan itself.
    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion_physical_expr::OrderingRequirements>> {
        vec![None]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let schema = child_stream.schema();
        // Create owned state from the sender - state is cloned from template
        // and will be sent back to the receiver when dropped (enabling state collection)
        let owned_state = self.kdf.create_owned();

        Ok(Box::pin(ConsumeKdfStream {
            schema,
            input: child_stream,
            owned_state,
            should_continue: true,
        }))
    }
}

/// Stream that applies ConsumeByKDF to batches.
///
/// Holds an `OwnedState<ConsumerKdfState>` which:
/// - Is created via `StateSender::create_owned()` (increments counter, clones template)
/// - Provides zero-lock, direct mutable access during streaming (no interior mutability)
/// - Automatically sends the mutated state to the receiver when dropped
struct ConsumeKdfStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    owned_state: OwnedState<ConsumerKdfState>,
    should_continue: bool,
}

// ConsumeKdfStream is Unpin because all its fields are Unpin:
// - ArrowSchemaRef (Arc<Schema>) is Unpin
// - SendableRecordBatchStream (Box<dyn ...>) is Unpin
// - OwnedState<ConsumerKdfState> is Unpin (ManuallyDrop + mpsc::Sender are Unpin)
// - bool is Unpin
impl Unpin for ConsumeKdfStream {}

impl Stream for ConsumeKdfStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safe to use get_mut() because ConsumeKdfStream is Unpin
        let this = self.get_mut();
        
        if !this.should_continue {
            return Poll::Ready(None);
        }

        match this.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Apply consumer KDF - wrap RecordBatch in ArrowEngineData
                let engine_data = ArrowEngineData::new(batch.clone());

                // Apply the KDF to the batch - direct mutable access, no locking needed
                let should_continue = match this.owned_state.state_mut().apply(&engine_data) {
                    Ok(cont) => cont,
                    Err(e) => {
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
                    }
                };

                // Update continuation flag
                this.should_continue = should_continue;
                Poll::Ready(Some(Ok(batch)))
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
