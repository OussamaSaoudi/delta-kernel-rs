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
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{ArrayRef, RecordBatch};
use delta_kernel::arrow::compute::cast;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::schema::StructType;
use futures::{Stream, StreamExt};

#[derive(Debug)]
pub struct ApplySchemaExec {
    child: Arc<dyn ExecutionPlan>,
    output_schema: Arc<StructType>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl ApplySchemaExec {
    pub fn try_new(child: Arc<dyn ExecutionPlan>, output_schema: Arc<StructType>) -> DfResult<Self> {
        let arrow_schema: delta_kernel::arrow::datatypes::Schema = output_schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let arrow_schema = Arc::new(arrow_schema);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            child,
            output_schema,
            arrow_schema,
            properties,
        })
    }
}

impl DisplayAs for ApplySchemaExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApplySchemaExec(fields={})", self.output_schema.num_fields())
    }
}

impl ExecutionPlan for ApplySchemaExec {
    fn name(&self) -> &str {
        "ApplySchemaExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.arrow_schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "ApplySchemaExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.output_schema.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        Ok(Box::pin(ApplySchemaStream {
            input,
            output_schema: self.output_schema.clone(),
            arrow_schema: self.arrow_schema.clone(),
        }))
    }
}

struct ApplySchemaStream {
    input: SendableRecordBatchStream,
    output_schema: Arc<StructType>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
}

impl Stream for ApplySchemaStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let out = coerce_batch_to_kernel_schema(
                    &batch,
                    self.output_schema.as_ref(),
                    self.arrow_schema.clone(),
                )?;
                Poll::Ready(Some(Ok(out)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ApplySchemaStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.arrow_schema.clone()
    }
}

fn coerce_batch_to_kernel_schema(
    batch: &RecordBatch,
    kernel_schema: &StructType,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
) -> DfResult<RecordBatch> {
    if batch.num_columns() != kernel_schema.num_fields() {
        return Err(DataFusionError::Internal(format!(
            "ApplySchemaExec: column count mismatch: {} vs {}",
            batch.num_columns(),
            kernel_schema.num_fields()
        )));
    }
    let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (idx, kernel_field) in kernel_schema.fields().enumerate() {
        let src = batch.column(idx);
        let _target_kernel_type = &kernel_field.data_type;
        let arrow_target = arrow_schema.field(idx).data_type();
        let coerced = cast(src.as_ref(), arrow_target)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        new_cols.push(coerced);
    }
    RecordBatch::try_new(arrow_schema, new_cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}
