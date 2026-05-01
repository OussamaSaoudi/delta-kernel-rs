//! Streaming `row_number()` window evaluation aligned with kernel [`WindowNode`] semantics:
//! empty `order_by` uses upstream batch row order (see IR docs on [`WindowNode`]).

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
use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use delta_kernel::engine::arrow_conversion::scalar::extract_primitive_scalar;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::{Expression, Scalar};
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{EvaluationHandler, ExpressionEvaluator};
use futures::{Stream, StreamExt};

/// Physical operator: append `row_number` columns over `PARTITION BY` keys using stream order.
pub struct KernelRowNumberWindowExec {
    child: Arc<dyn ExecutionPlan>,
    partition_evaluators: Vec<Arc<dyn ExpressionEvaluator>>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
    num_window_cols: usize,
    properties: Arc<PlanProperties>,
}

impl KernelRowNumberWindowExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        output_kernel_schema: SchemaRef,
        partition_by: &[Arc<Expression>],
        num_window_cols: usize,
    ) -> Result<Self, delta_kernel::plans::errors::DeltaError> {
        if num_window_cols == 0 {
            return Err(crate::error::plan_compilation(
                "KernelRowNumberWindowExec requires at least one window output column",
            ));
        }

        let mut partition_evaluators = Vec::with_capacity(partition_by.len());
        let schema_struct = input_schema.as_ref();
        for expr in partition_by {
            let out_ty = infer_partition_expr_output_type(schema_struct, expr.as_ref())?;
            let eval = ArrowEvaluationHandler
                .new_expression_evaluator(input_schema.clone(), Arc::clone(expr), out_ty)
                .map_err(|e| {
                    crate::error::internal_error(format!("window partition evaluator init: {e}"))
                })?;
            partition_evaluators.push(eval);
        }

        let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef = Arc::new(
            output_kernel_schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e| {
                    crate::error::internal_error(format!("window output schema conversion: {e}"))
                })?,
        );

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            child,
            partition_evaluators,
            arrow_schema,
            num_window_cols,
            properties,
        })
    }
}

fn infer_partition_expr_output_type(
    schema: &delta_kernel::schema::StructType,
    expr: &Expression,
) -> Result<DataType, delta_kernel::plans::errors::DeltaError> {
    match expr {
        Expression::Column(col) => {
            let path = col.path();
            if path.is_empty() {
                return Err(crate::error::plan_compilation(
                    "empty column path in PARTITION BY",
                ));
            }
            let mut current = schema;
            for (i, segment) in path.iter().enumerate() {
                let field = current.field(segment.as_str()).ok_or_else(|| {
                    crate::error::plan_compilation(format!(
                        "PARTITION BY column `{col}` not found in input schema"
                    ))
                })?;
                if i + 1 == path.len() {
                    return Ok(field.data_type().clone());
                }
                match field.data_type() {
                    DataType::Struct(inner) => current = inner.as_ref(),
                    other => {
                        return Err(crate::error::plan_compilation(format!(
                            "PARTITION BY path `{col}`: intermediate field is not a struct ({other:?})"
                        )));
                    }
                }
            }
            Err(crate::error::internal_error(
                "unreachable PARTITION BY path walk",
            ))
        }
        Expression::Literal(s) => Ok(s.data_type()),
        _ => Err(crate::error::unsupported(
            "PARTITION BY for Window must use column references or literals in this engine",
        )),
    }
}

impl fmt::Debug for KernelRowNumberWindowExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelRowNumberWindowExec")
            .field("partition_exprs", &self.partition_evaluators.len())
            .field("num_window_cols", &self.num_window_cols)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for KernelRowNumberWindowExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KernelRowNumberWindowExec(partitions={}, outs={})",
            self.partition_evaluators.len(),
            self.num_window_cols
        )
    }
}

impl ExecutionPlan for KernelRowNumberWindowExec {
    fn name(&self) -> &str {
        "KernelRowNumberWindowExec"
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
                "KernelRowNumberWindowExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
            partition_evaluators: self.partition_evaluators.clone(),
            arrow_schema: self.arrow_schema.clone(),
            num_window_cols: self.num_window_cols,
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
        Ok(Box::pin(RowNumberWindowStream {
            input,
            partition_evaluators: self.partition_evaluators.clone(),
            arrow_schema: self.arrow_schema.clone(),
            num_window_cols: self.num_window_cols,
            prev_keys: None,
            current_rn: 0,
        }))
    }
}

struct RowNumberWindowStream {
    input: SendableRecordBatchStream,
    partition_evaluators: Vec<Arc<dyn ExpressionEvaluator>>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
    num_window_cols: usize,
    prev_keys: Option<Vec<Scalar>>,
    current_rn: i64,
}

impl RowNumberWindowStream {
    fn extend_batch(&self, batch: RecordBatch, rn: Int64Array) -> DfResult<RecordBatch> {
        let rn_arr: ArrayRef = Arc::new(rn);
        let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
        for _ in 0..self.num_window_cols {
            cols.push(Arc::clone(&rn_arr));
        }
        RecordBatch::try_new(self.arrow_schema.clone(), cols)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for RowNumberWindowStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(Some(Ok(batch)));
                }

                let batch_for_eval = ArrowEngineData::new(batch.clone());
                let mut part_cols: Vec<ArrayRef> =
                    Vec::with_capacity(self.partition_evaluators.len());
                for eval in &self.partition_evaluators {
                    let evaluated = match eval.evaluate(&batch_for_eval) {
                        Ok(v) => v,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                    };
                    let rb = match evaluated.try_into_record_batch() {
                        Ok(v) => v,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                    };
                    if rb.num_columns() != 1 {
                        return Poll::Ready(Some(Err(DataFusionError::Execution(
                            "partition evaluator expects single-column output".into(),
                        ))));
                    }
                    part_cols.push(rb.column(0).clone());
                }

                let num_rows = batch.num_rows();
                let mut rn_vals = Vec::with_capacity(num_rows);
                for row in 0..num_rows {
                    let keys: Vec<Scalar> = match part_cols
                        .iter()
                        .map(|a| extract_primitive_scalar(a.as_ref(), row))
                        .collect::<delta_kernel::DeltaResult<Vec<_>>>()
                    {
                        Ok(k) => k,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                    };

                    let same_partition = self.prev_keys.as_ref().is_some_and(|prev| prev == &keys);
                    if same_partition {
                        self.current_rn += 1;
                    } else {
                        self.current_rn = 1;
                        self.prev_keys = Some(keys);
                    }
                    rn_vals.push(self.current_rn);
                }

                let rn_array = Int64Array::from(rn_vals);
                let extended = match self.extend_batch(batch, rn_array) {
                    Ok(out) => Ok(out),
                    Err(e) => Err(e),
                };
                Poll::Ready(Some(extended))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RowNumberWindowStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.arrow_schema.clone()
    }
}
