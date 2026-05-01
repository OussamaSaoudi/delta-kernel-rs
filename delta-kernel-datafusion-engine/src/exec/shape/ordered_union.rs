use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::RecordBatch;
use futures::Stream;

/// Executes children in declaration order, draining child N fully before N+1.
pub struct OrderedUnionExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl OrderedUnionExec {
    pub fn try_new(children: Vec<Arc<dyn ExecutionPlan>>) -> DfResult<Self> {
        let Some(first) = children.first() else {
            return Err(DataFusionError::Plan(
                "OrderedUnionExec requires at least one child".into(),
            ));
        };
        let schema = first.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            children,
            schema,
            properties,
        })
    }
}

impl fmt::Debug for OrderedUnionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedUnionExec")
            .field("children", &self.children.len())
            .finish()
    }
}

impl DisplayAs for OrderedUnionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrderedUnionExec(children={})", self.children.len())
    }
}

impl ExecutionPlan for OrderedUnionExec {
    fn name(&self) -> &str {
        "OrderedUnionExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(children)?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "OrderedUnionExec supports only partition 0, got {partition}"
            )));
        }
        Ok(Box::pin(OrderedUnionStream {
            children: self.children.clone(),
            context,
            current_child_idx: 0,
            current_stream: None,
            schema: self.schema.clone(),
        }))
    }
}

struct OrderedUnionStream {
    children: Vec<Arc<dyn ExecutionPlan>>,
    context: Arc<TaskContext>,
    current_child_idx: usize,
    current_stream: Option<SendableRecordBatchStream>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
}

impl Stream for OrderedUnionStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.current_stream.is_none() {
                if self.current_child_idx >= self.children.len() {
                    return Poll::Ready(None);
                }
                let next_stream = match self.children[self.current_child_idx]
                    .execute(0, Arc::clone(&self.context))
                {
                    Ok(stream) => stream,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                self.current_stream = Some(next_stream);
            }

            if let Some(stream) = self.current_stream.as_mut() {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                    Poll::Ready(None) => {
                        self.current_stream = None;
                        self.current_child_idx += 1;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

impl RecordBatchStream for OrderedUnionStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
