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
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::Expression;
use delta_kernel::schema::SchemaRef;
use delta_kernel::{EvaluationHandler, ExpressionEvaluator};
use futures::{Stream, StreamExt};

pub struct KernelProjectExec {
    child: Arc<dyn ExecutionPlan>,
    evaluators: Vec<Arc<dyn ExpressionEvaluator>>,
    output_schema: SchemaRef,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl KernelProjectExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        columns: &[Arc<Expression>],
        output_schema: SchemaRef,
    ) -> Result<Self, delta_kernel::plans::errors::DeltaError> {
        let output_fields: Vec<_> = output_schema.fields().collect();
        if output_fields.len() != columns.len() {
            return Err(crate::error::plan_compilation(format!(
                "Project columns ({}) != output fields ({})",
                columns.len(),
                output_fields.len()
            )));
        }
        let mut evaluators = Vec::with_capacity(columns.len());
        for (idx, expr) in columns.iter().enumerate() {
            let eval = ArrowEvaluationHandler.new_expression_evaluator(
                input_schema.clone(),
                Arc::clone(expr),
                output_fields[idx].data_type.clone(),
            )
            .map_err(|e| crate::error::internal_error(format!("project evaluator init: {e}")))?;
            evaluators.push(eval);
        }
        let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef = Arc::new(
            output_schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e| crate::error::internal_error(format!("project schema conversion: {e}")))?,
        );
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            child,
            evaluators,
            output_schema,
            arrow_schema,
            properties,
        })
    }
}

impl fmt::Debug for KernelProjectExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelProjectExec")
            .field("exprs", &self.evaluators.len())
            .finish()
    }
}

impl DisplayAs for KernelProjectExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KernelProjectExec(exprs={})", self.evaluators.len())
    }
}

impl ExecutionPlan for KernelProjectExec {
    fn name(&self) -> &str {
        "KernelProjectExec"
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
                "KernelProjectExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
            evaluators: self.evaluators.clone(),
            output_schema: self.output_schema.clone(),
            arrow_schema: self.arrow_schema.clone(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.arrow_schema.clone()),
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
        Ok(Box::pin(KernelProjectStream {
            input,
            evaluators: self.evaluators.clone(),
            arrow_schema: self.arrow_schema.clone(),
        }))
    }
}

struct KernelProjectStream {
    input: SendableRecordBatchStream,
    evaluators: Vec<Arc<dyn ExpressionEvaluator>>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
}

impl Stream for KernelProjectStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch_for_eval = ArrowEngineData::new(batch);
                let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.evaluators.len());
                for eval in &self.evaluators {
                    let evaluated = match eval.evaluate(&batch_for_eval) {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
                    };
                    let rb = match evaluated.try_into_record_batch() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
                    };
                    if rb.num_columns() != 1 {
                        return Poll::Ready(Some(Err(DataFusionError::Execution(
                            "KernelProjectExec expects one-column evaluator output".into(),
                        ))));
                    }
                    cols.push(rb.column(0).clone());
                }
                let out = RecordBatch::try_new(self.arrow_schema.clone(), cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                Poll::Ready(Some(out))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for KernelProjectStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.arrow_schema.clone()
    }
}
