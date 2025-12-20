//! NullabilityValidationExec - validates kernel nullability semantics at runtime.
//!
//! When reading parquet files with a relaxed schema (all nullable), this exec
//! validates that non-nullable nested fields don't contain nulls when their
//! parent struct is present.
//!
//! Semantics: For each (parent, child) validation pair:
//!   - If parent struct IS NOT NULL, then child field must be NOT NULL
//!   - Error if this constraint is violated

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, StructArray};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_physical_plan::execution_plan::{EmissionType, Boundedness};
use datafusion_common::{Result as DfResult, DataFusionError};
use datafusion_physical_expr::EquivalenceProperties;
use futures::{Stream, StreamExt};

/// Validation rule: (parent_column_name, child_field_name)
/// Semantics: If parent IS NOT NULL, child must be NOT NULL
pub type ValidationRule = (String, String);

/// ExecutionPlan that validates nullability constraints on batches.
///
/// For each batch, checks that non-nullable nested fields (under nullable parents)
/// don't contain nulls when the parent is present.
#[derive(Debug)]
pub struct NullabilityValidationExec {
    child: Arc<dyn ExecutionPlan>,
    /// Validation rules: (parent_column, child_field) pairs
    validations: Vec<ValidationRule>,
    properties: PlanProperties,
}

impl NullabilityValidationExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, validations: Vec<ValidationRule>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(child.schema()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            child,
            validations,
            properties,
        }
    }
}

impl DisplayAs for NullabilityValidationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NullabilityValidationExec[validations={}]",
            self.validations.len()
        )
    }
}

impl ExecutionPlan for NullabilityValidationExec {
    fn name(&self) -> &str {
        "NullabilityValidationExec"
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
        Ok(Arc::new(NullabilityValidationExec::new(
            Arc::clone(&children[0]),
            self.validations.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let schema = child_stream.schema();
        let validations = self.validations.clone();

        Ok(Box::pin(NullabilityValidationStream {
            schema,
            input: child_stream,
            validations,
        }))
    }
}

/// Stream that validates nullability constraints on each batch.
struct NullabilityValidationStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    validations: Vec<ValidationRule>,
}

impl Stream for NullabilityValidationStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Validate nullability constraints
                if let Err(e) = validate_batch(&batch, &self.validations) {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for NullabilityValidationStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Validate that non-nullable children under nullable parents don't have nulls
/// when the parent is present.
fn validate_batch(batch: &RecordBatch, validations: &[ValidationRule]) -> DfResult<()> {
    for (parent_name, child_name) in validations {
        // Find the parent column
        let parent_col = batch.column_by_name(parent_name).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Nullability validation: parent column '{}' not found",
                parent_name
            ))
        })?;

        // Parent must be a struct
        let struct_arr = parent_col
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Nullability validation: parent column '{}' is not a struct",
                    parent_name
                ))
            })?;

        // Find the child field
        let child_col = struct_arr.column_by_name(child_name).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Nullability validation: child field '{}' not found in struct '{}'",
                child_name, parent_name
            ))
        })?;

        // Check: for rows where parent IS NOT NULL, child must be NOT NULL
        // parent_valid & child_null should be all zeros
        let parent_nulls = struct_arr.nulls();
        let child_nulls = child_col.nulls();

        // If parent has no null buffer, all parents are valid
        // If child has no null buffer, all children are valid (no violation possible)
        if child_nulls.is_none() {
            continue; // Child has no nulls, constraint satisfied
        }

        let child_null_buffer = child_nulls.unwrap();

        // Check each row
        for row_idx in 0..struct_arr.len() {
            let parent_is_valid = parent_nulls
                .map(|nulls| nulls.is_valid(row_idx))
                .unwrap_or(true);
            let child_is_null = !child_null_buffer.is_valid(row_idx);

            if parent_is_valid && child_is_null {
                return Err(DataFusionError::Internal(format!(
                    "Nullability constraint violation at row {}: \
                     '{}.{}' is null but parent '{}' is present. \
                     Non-nullable fields must not be null when parent is present.",
                    row_idx, parent_name, child_name, parent_name
                )));
            }
        }
    }

    Ok(())
}


