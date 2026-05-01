//! Literal source: emits in-memory batches from kernel [`Scalar`] rows (see [`ValuesNode`]).
//!
//! [`ValuesNode`]: delta_kernel::plans::ir::nodes::ValuesNode

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use delta_kernel::arrow::array::{Array, ArrayRef, RecordBatch};
use delta_kernel::arrow::compute::concat;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt, KernelErrAsDelta};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;

/// Execution operator that materializes [`ValuesNode`] rows as Arrow batches.
pub struct LiteralExec {
    kernel_schema: KernelSchemaRef,
    arrow_schema: ArrowSchemaRef,
    rows: Vec<Vec<Scalar>>,
    properties: Arc<PlanProperties>,
}

impl LiteralExec {
    /// Build a literal execution node from validated kernel literal rows.
    pub fn try_new(
        kernel_schema: KernelSchemaRef,
        rows: Vec<Vec<Scalar>>,
    ) -> Result<Self, DeltaError> {
        let arrow_schema: ArrowSchemaRef =
            Arc::new(kernel_schema.as_ref().try_into_arrow().map_err(|e| {
                crate::error::internal_error(format!("literal schema conversion: {e}"))
            })?);

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(Self {
            kernel_schema,
            arrow_schema,
            rows,
            properties,
        })
    }

    fn column_arrays(&self) -> Result<Vec<ArrayRef>, DeltaError> {
        if self.rows.is_empty() {
            return Ok(Vec::new());
        }
        let width = self.rows[0].len();
        let mut cols = Vec::with_capacity(width);
        for col_idx in 0..width {
            let mut pieces: Vec<ArrayRef> = Vec::with_capacity(self.rows.len());
            for row in &self.rows {
                pieces.push(
                    row[col_idx]
                        .to_array(1)
                        .map_err(|e| e.into_delta_default())?,
                );
            }
            let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
            cols.push(concat(&refs).or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?);
        }
        Ok(cols)
    }
}

impl Debug for LiteralExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiteralExec")
            .field("rows", &self.rows.len())
            .field("schema_fields", &self.kernel_schema.fields().count())
            .finish()
    }
}

impl DisplayAs for LiteralExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "LiteralExec(rows={}, fields={})",
                    self.rows.len(),
                    self.kernel_schema.fields().count()
                )
            }
        }
    }
}

impl ExecutionPlan for LiteralExec {
    fn name(&self) -> &str {
        "LiteralExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "LiteralExec cannot have children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<datafusion_physical_plan::SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LiteralExec supports partition 0 only, got {partition}",
            )));
        }

        if self.rows.is_empty() {
            let batch = RecordBatch::new_empty(self.arrow_schema.clone());
            return Ok(Box::pin(MemoryStream::try_new(
                vec![batch],
                self.arrow_schema.clone(),
                None,
            )?));
        }

        let arrays = self
            .column_arrays()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(DataFusionError::from)?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.arrow_schema.clone(),
            None,
        )?))
    }
}
