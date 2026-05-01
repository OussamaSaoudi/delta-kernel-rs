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
use delta_kernel::arrow::array::{Array, ArrayRef, NullBufferBuilder, RecordBatch, StructArray};
use delta_kernel::arrow::datatypes::Field as ArrowField;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::expressions::Expression;
use delta_kernel::schema::{DataType, SchemaRef, StructType};
use delta_kernel::{EvaluationHandler, ExpressionEvaluator};
use futures::{Stream, StreamExt};

/// How one projected expression maps onto Arrow columns in the output batch.
#[derive(Clone, Debug)]
enum ProjectionEmit {
    /// Single Arrow column (`evaluate` yields a one-column batch).
    SingleColumn,
    /// Kernel struct evaluator yields `num_columns` sibling arrays matching consecutive IR fields.
    SpreadFlat { num_columns: usize },
    /// Kernel struct evaluator yields child arrays that must be nested into one struct column.
    PackedStruct { kernel_struct: StructType },
}

#[derive(Clone)]
struct ProjectionStep {
    evaluator: Arc<dyn ExpressionEvaluator>,
    emit: ProjectionEmit,
}

fn packed_struct_column(
    kernel_struct: &StructType,
    columns: Vec<ArrayRef>,
) -> Result<ArrayRef, delta_kernel::plans::errors::DeltaError> {
    if kernel_struct.num_fields() != columns.len() {
        return Err(crate::error::internal_error(format!(
            "packed struct column mismatch: schema expects {} fields, got {} arrays",
            kernel_struct.num_fields(),
            columns.len()
        )));
    }
    let arrow_fields: Vec<Arc<ArrowField>> = kernel_struct
        .fields()
        .map(|f| {
            let af = f.try_into_arrow().map_err(|e| {
                crate::error::internal_error(format!("packed struct Arrow field: {e}"))
            })?;
            Ok(Arc::new(af))
        })
        .collect::<Result<Vec<_>, delta_kernel::plans::errors::DeltaError>>()?;

    let fields: delta_kernel::arrow::datatypes::Fields = arrow_fields.into();

    // Expression evaluators return flattened child columns, which can lose the original parent
    // struct validity mask. If a NOT NULL child carries nulls, reconstruct a parent validity mask
    // so those rows become NULL at the struct level instead of violating Arrow invariants.
    let nulls = if fields
        .iter()
        .zip(columns.iter())
        .any(|(f, c)| !f.is_nullable() && c.null_count() > 0)
    {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        let mut nulls = NullBufferBuilder::new(num_rows);
        for row in 0..num_rows {
            let has_invalid_non_nullable_child = fields
                .iter()
                .zip(columns.iter())
                .any(|(f, c)| !f.is_nullable() && c.is_null(row));
            if has_invalid_non_nullable_child {
                nulls.append_null();
            } else {
                nulls.append_non_null();
            }
        }
        nulls.finish()
    } else {
        None
    };
    let struct_arr = StructArray::new(fields, columns, nulls);

    Ok(Arc::new(struct_arr) as ArrayRef)
}

/// Align projected expressions with [`KernelProjectExec`] output fields.
///
/// - [`Expression::Struct`] expressions whose outer IR field is itself a [`DataType::Struct`] with
///   matching arity use [`ProjectionEmit::PackedStruct`] (one Arrow struct column).
/// - Otherwise the struct expression consumes consecutive **top-level** IR fields whose names and
///   types define the evaluator struct ([`ProjectionEmit::SpreadFlat`]).
/// - Scalar expressions consume one IR field each.
fn resolve_projection_steps(
    input_schema: SchemaRef,
    columns: &[Arc<Expression>],
    output_schema: &StructType,
) -> Result<Vec<ProjectionStep>, delta_kernel::plans::errors::DeltaError> {
    let fields: Vec<_> = output_schema.fields().cloned().collect();
    let mut field_cursor = 0usize;
    let mut steps = Vec::with_capacity(columns.len());

    for expr in columns {
        match expr.as_ref() {
            Expression::Struct(inner, _) => {
                let n = inner.len();
                if n == 0 {
                    return Err(crate::error::plan_compilation(
                        "Project struct expression must have at least one field expression",
                    ));
                }
                if field_cursor >= fields.len() {
                    return Err(crate::error::plan_compilation(
                        "Project output_schema exhausted before struct expression",
                    ));
                }

                if let DataType::Struct(st) = &fields[field_cursor].data_type {
                    if st.num_fields() == n {
                        let eval = ArrowEvaluationHandler
                            .new_expression_evaluator(
                                input_schema.clone(),
                                Arc::clone(expr),
                                fields[field_cursor].data_type.clone(),
                            )
                            .map_err(|e| {
                                crate::error::internal_error(format!(
                                    "project struct-packed evaluator init: {e}"
                                ))
                            })?;
                        steps.push(ProjectionStep {
                            evaluator: eval,
                            emit: ProjectionEmit::PackedStruct {
                                kernel_struct: (*st.as_ref()).clone(),
                            },
                        });
                        field_cursor += 1;
                        continue;
                    }
                    return Err(crate::error::plan_compilation(format!(
                        "struct expression has {} fields but output schema struct `{}` has {}",
                        n,
                        fields[field_cursor].name(),
                        st.num_fields(),
                    )));
                }

                if field_cursor + n > fields.len() {
                    return Err(crate::error::plan_compilation(format!(
                        "struct spread needs {n} trailing output fields starting at index \
                         {field_cursor}, but schema has only {} fields",
                        fields.len()
                    )));
                }
                let slice = &fields[field_cursor..field_cursor + n];
                let synth = StructType::try_new(slice.iter().cloned()).map_err(|e| {
                    crate::error::plan_compilation(format!(
                        "Project struct spread slice does not form a valid StructType: {e}"
                    ))
                })?;
                let eval_type = DataType::Struct(Box::new(synth));
                let eval = ArrowEvaluationHandler
                    .new_expression_evaluator(input_schema.clone(), Arc::clone(expr), eval_type)
                    .map_err(|e| {
                        crate::error::internal_error(format!(
                            "project struct-spread evaluator init: {e}"
                        ))
                    })?;
                steps.push(ProjectionStep {
                    evaluator: eval,
                    emit: ProjectionEmit::SpreadFlat { num_columns: n },
                });
                field_cursor += n;
            }
            _ => {
                if field_cursor >= fields.len() {
                    return Err(crate::error::plan_compilation(
                        "Project output_schema exhausted before scalar expression",
                    ));
                }
                let eval_type = fields[field_cursor].data_type.clone();
                let emit = match &eval_type {
                    DataType::Struct(st) => ProjectionEmit::PackedStruct {
                        kernel_struct: (**st).clone(),
                    },
                    _ => ProjectionEmit::SingleColumn,
                };
                let eval = ArrowEvaluationHandler
                    .new_expression_evaluator(input_schema.clone(), Arc::clone(expr), eval_type)
                    .map_err(|e| {
                        crate::error::internal_error(format!("project evaluator init: {e}"))
                    })?;
                steps.push(ProjectionStep {
                    evaluator: eval,
                    emit,
                });
                field_cursor += 1;
            }
        }
    }

    if field_cursor != fields.len() {
        return Err(crate::error::plan_compilation(format!(
            "Project expressions consumed {field_cursor} output fields but output_schema has {}",
            fields.len()
        )));
    }

    Ok(steps)
}

fn append_evaluated_columns(
    cols: &mut Vec<ArrayRef>,
    evaluated: &RecordBatch,
    emit: &ProjectionEmit,
) -> Result<(), DataFusionError> {
    match emit {
        ProjectionEmit::SingleColumn => {
            if evaluated.num_columns() != 1 {
                return Err(DataFusionError::Execution(format!(
                    "KernelProjectExec expects one-column evaluator output, got {}",
                    evaluated.num_columns()
                )));
            }
            cols.push(evaluated.column(0).clone());
        }
        ProjectionEmit::SpreadFlat { num_columns } => {
            if evaluated.num_columns() != *num_columns {
                return Err(DataFusionError::Execution(format!(
                    "KernelProjectExec spread expects {} evaluator columns, got {}",
                    num_columns,
                    evaluated.num_columns()
                )));
            }
            for i in 0..*num_columns {
                cols.push(evaluated.column(i).clone());
            }
        }
        ProjectionEmit::PackedStruct { kernel_struct } => {
            let n = kernel_struct.num_fields();
            if evaluated.num_columns() != n {
                return Err(DataFusionError::Execution(format!(
                    "KernelProjectExec packed struct expects {} evaluator columns, got {}",
                    n,
                    evaluated.num_columns()
                )));
            }
            let inner_cols: Vec<ArrayRef> = (0..n).map(|i| evaluated.column(i).clone()).collect();
            let packed = packed_struct_column(kernel_struct, inner_cols)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            cols.push(packed);
        }
    }
    Ok(())
}

pub struct KernelProjectExec {
    child: Arc<dyn ExecutionPlan>,
    projection_steps: Vec<ProjectionStep>,
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
        let projection_steps =
            resolve_projection_steps(input_schema, columns, output_schema.as_ref())?;
        let arrow_schema: delta_kernel::arrow::datatypes::SchemaRef =
            Arc::new(output_schema.as_ref().try_into_arrow().map_err(|e| {
                crate::error::internal_error(format!("project schema conversion: {e}"))
            })?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            child,
            projection_steps,
            output_schema,
            arrow_schema,
            properties,
        })
    }
}

impl fmt::Debug for KernelProjectExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelProjectExec")
            .field("steps", &self.projection_steps.len())
            .finish()
    }
}

impl DisplayAs for KernelProjectExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KernelProjectExec(steps={})",
            self.projection_steps.len()
        )
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
            projection_steps: self.projection_steps.clone(),
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
            projection_steps: self.projection_steps.clone(),
            arrow_schema: self.arrow_schema.clone(),
        }))
    }
}

struct KernelProjectStream {
    input: SendableRecordBatchStream,
    projection_steps: Vec<ProjectionStep>,
    arrow_schema: delta_kernel::arrow::datatypes::SchemaRef,
}

impl Stream for KernelProjectStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch_for_eval = ArrowEngineData::new(batch);
                let mut cols: Vec<ArrayRef> = Vec::new();
                for step in &self.projection_steps {
                    let evaluated = match step.evaluator.evaluate(&batch_for_eval) {
                        Ok(v) => v,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
                        }
                    };
                    let rb = match evaluated.try_into_record_batch() {
                        Ok(v) => v,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
                        }
                    };
                    if let Err(e) = append_evaluated_columns(&mut cols, &rb, &step.emit) {
                        return Poll::Ready(Some(Err(e)));
                    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{AsArray, Int64Array, StringArray};
    use delta_kernel::arrow::datatypes::DataType as ArrowPhysicalType;
    use delta_kernel::expressions::{column_expr, Expression, Scalar};
    use delta_kernel::plans::ir::DeclarativePlanNode;
    use delta_kernel::schema::{DataType, StructField, StructType};

    use crate::DataFusionExecutor;

    #[tokio::test]
    async fn project_struct_spread_to_top_level_columns() {
        let input_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::STRING),
        ]));
        let out_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::STRING),
        ]));
        let rows = vec![
            vec![Scalar::Long(1), Scalar::String("x".into())],
            vec![Scalar::Long(2), Scalar::String("y".into())],
        ];
        let plan = DeclarativePlanNode::values(input_schema.clone(), rows)
            .expect("literal")
            .project(
                vec![Arc::new(Expression::struct_from([
                    Expression::column(["a"]),
                    Expression::column(["b"]),
                ]))],
                out_schema.clone(),
            )
            .into_results();

        let exec = DataFusionExecutor::try_new().expect("executor");
        let batches = exec.execute_plan_collect(plan).await.expect("run");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![1_i64, 2])
        );
        assert_eq!(
            batch.column(1).as_string::<i32>(),
            &StringArray::from(vec!["x", "y"])
        );
    }

    #[tokio::test]
    async fn project_struct_packed_single_column() {
        let inner = StructType::new_unchecked([
            StructField::not_null("u", DataType::LONG),
            StructField::not_null("v", DataType::STRING),
        ]);
        let input_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("x", DataType::LONG),
            StructField::not_null("y", DataType::STRING),
        ]));
        let out_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "nested",
            DataType::Struct(Box::new(inner.clone())),
        )]));
        let rows = vec![vec![Scalar::Long(7), Scalar::String("hi".into())]];
        let plan = DeclarativePlanNode::values(input_schema.clone(), rows)
            .expect("literal")
            .project(
                vec![Arc::new(Expression::struct_from([
                    Expression::column(["x"]),
                    Expression::column(["y"]),
                ]))],
                out_schema.clone(),
            )
            .into_results();

        let exec = DataFusionExecutor::try_new().expect("executor");
        let batches = exec.execute_plan_collect(plan).await.expect("run");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 1);
        assert!(
            matches!(batch.column(0).data_type(), ArrowPhysicalType::Struct(_)),
            "expected struct column"
        );
        let sa = batch.column(0).as_struct_opt().expect("struct column");
        assert_eq!(sa.num_columns(), 2);
        assert_eq!(
            sa.column(0)
                .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![7_i64])
        );
        assert_eq!(
            sa.column(1).as_string::<i32>(),
            &StringArray::from(vec!["hi"])
        );
    }

    #[tokio::test]
    async fn project_mixed_scalar_and_packed_struct() {
        let inner = StructType::new_unchecked([
            StructField::nullable("u", DataType::LONG),
            StructField::nullable("v", DataType::STRING),
        ]);
        let input_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("x", DataType::LONG),
            StructField::not_null("y", DataType::STRING),
        ]));
        let out_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("nested", DataType::Struct(Box::new(inner.clone()))),
        ]));
        let rows = vec![
            vec![
                Scalar::Long(100),
                Scalar::Long(1),
                Scalar::String("a".into()),
            ],
            vec![
                Scalar::Long(200),
                Scalar::Long(2),
                Scalar::String("b".into()),
            ],
        ];
        let plan = DeclarativePlanNode::values(input_schema.clone(), rows)
            .expect("literal")
            .project(
                vec![
                    Arc::new(column_expr!("id")),
                    Arc::new(Expression::struct_from([
                        Expression::column(["x"]),
                        Expression::column(["y"]),
                    ])),
                ],
                out_schema.clone(),
            )
            .into_results();

        let exec = DataFusionExecutor::try_new().expect("executor");
        let batches = exec.execute_plan_collect(plan).await.expect("run");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![100_i64, 200])
        );
        let nested = batch.column(1).as_struct_opt().expect("nested");
        assert_eq!(
            nested
                .column(0)
                .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![1_i64, 2])
        );
        assert_eq!(
            nested.column(1).as_string::<i32>(),
            &StringArray::from(vec!["a", "b"])
        );
    }
}
