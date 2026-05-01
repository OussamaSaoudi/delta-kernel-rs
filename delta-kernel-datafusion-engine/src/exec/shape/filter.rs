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
use delta_kernel::arrow::array::{Array, AsArray, BooleanArray, RecordBatch};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::Expression;
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{EvaluationHandler, ExpressionEvaluator};
use futures::{Stream, StreamExt};

pub struct KernelFilterExec {
    child: Arc<dyn ExecutionPlan>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    evaluator: Arc<dyn ExpressionEvaluator>,
    properties: Arc<PlanProperties>,
}

impl KernelFilterExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        predicate: Arc<Expression>,
    ) -> Result<Self, delta_kernel::plans::errors::DeltaError> {
        let evaluator = ArrowEvaluationHandler.new_expression_evaluator(
            input_schema,
            predicate,
            DataType::BOOLEAN,
        ).map_err(|e| crate::error::internal_error(format!("filter evaluator init: {e}")))?;
        let schema = child.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            child,
            schema,
            evaluator,
            properties,
        })
    }
}

impl fmt::Debug for KernelFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelFilterExec").finish_non_exhaustive()
    }
}

impl DisplayAs for KernelFilterExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KernelFilterExec")
    }
}

impl ExecutionPlan for KernelFilterExec {
    fn name(&self) -> &str {
        "KernelFilterExec"
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
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "KernelFilterExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
            schema: self.schema.clone(),
            evaluator: Arc::clone(&self.evaluator),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                children[0].properties().output_partitioning().clone(),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        Ok(Box::pin(KernelFilterStream {
            input,
            schema: self.schema.clone(),
            evaluator: Arc::clone(&self.evaluator),
        }))
    }
}

struct KernelFilterStream {
    input: SendableRecordBatchStream,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    evaluator: Arc<dyn ExpressionEvaluator>,
}

impl Stream for KernelFilterStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch_for_eval = ArrowEngineData::new(batch.clone());
                let pred_data = match self.evaluator.evaluate(&batch_for_eval) {
                    Ok(v) => v,
                    Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
                };
                let pred_batch = match pred_data.try_into_record_batch() {
                    Ok(rb) => rb,
                    Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
                };
                let predicate = pred_batch.column(0).as_boolean();
                // Keep nulls (NULL -> true), matching kernel filter semantics.
                let mask = BooleanArray::from_iter((0..predicate.len()).map(|i| {
                    Some(if predicate.is_null(i) { true } else { predicate.value(i) })
                }));
                let filtered = filter_record_batch(&batch, &mask)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                Poll::Ready(Some(filtered))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for KernelFilterStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
