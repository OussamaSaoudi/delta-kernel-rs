//! NullabilityValidationExec - validates kernel nullability semantics at runtime.
//!
//! When reading parquet files with a relaxed schema (all nullable), this exec:
//! 1. Validates that non-nullable nested fields don't contain nulls when their
//!    parent struct is present
//! 2. Recursively casts arrays to the target (tighter) schema for Union compatibility
//!
//! Semantics: For each (parent, child) validation pair:
//!   - If parent struct IS NOT NULL, then child field must be NOT NULL
//!   - Error if this constraint is violated

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::{SchemaRef as ArrowSchemaRef, Field, DataType, Fields};
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef, StructArray, ListArray, MapArray};
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

/// ExecutionPlan that validates nullability constraints and casts to target schema.
///
/// For each batch:
/// 1. Checks that non-nullable nested fields (under nullable parents) don't contain nulls
/// 2. Outputs the batch with the target schema (tighter nullability constraints)
#[derive(Debug)]
pub struct NullabilityValidationExec {
    child: Arc<dyn ExecutionPlan>,
    /// Validation rules: (parent_column, child_field) pairs
    validations: Vec<ValidationRule>,
    /// Target output schema (original kernel schema with tighter nullability)
    target_schema: ArrowSchemaRef,
    properties: PlanProperties,
}

impl NullabilityValidationExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        validations: Vec<ValidationRule>,
        target_schema: ArrowSchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            child,
            validations,
            target_schema,
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
        self.target_schema.clone()
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
            self.target_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let validations = self.validations.clone();
        let target_schema = self.target_schema.clone();

        Ok(Box::pin(NullabilityValidationStream {
            target_schema,
            input: child_stream,
            validations,
        }))
    }
}

/// Stream that validates nullability constraints and outputs with target schema.
struct NullabilityValidationStream {
    target_schema: ArrowSchemaRef,
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
                
                // Recursively cast columns to target schema
                // This rewraps StructArrays with the correct field definitions
                let new_columns: Vec<ArrayRef> = self.target_schema.fields().iter()
                    .zip(batch.columns())
                    .map(|(target_field, col)| cast_array_to_field(col, target_field.as_ref()))
                    .collect();
                
                let new_batch = RecordBatch::try_new(self.target_schema.clone(), new_columns)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                
                Poll::Ready(Some(Ok(new_batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Recursively cast an array to match the target field's schema.
///
/// This handles the case where StructArrays carry their own field definitions.
/// We need to rewrap them with the correct (tighter) field definitions from
/// the target schema.
fn cast_array_to_field(array: &ArrayRef, target_field: &Field) -> ArrayRef {
    match target_field.data_type() {
        DataType::Struct(target_fields) => {
            cast_struct_array(array, target_fields)
        }
        DataType::List(target_inner) => {
            cast_list_array(array, target_inner.as_ref())
        }
        DataType::LargeList(target_inner) => {
            cast_large_list_array(array, target_inner.as_ref())
        }
        DataType::Map(target_entries, _) => {
            cast_map_array(array, target_entries.as_ref())
        }
        // Primitive types don't carry nullability info in the array itself
        _ => array.clone(),
    }
}

/// Cast a StructArray to match target field definitions.
fn cast_struct_array(array: &ArrayRef, target_fields: &Fields) -> ArrayRef {
    let struct_arr = array.as_any().downcast_ref::<StructArray>()
        .expect("Expected StructArray");
    
    // Recursively cast each child column
    let new_columns: Vec<ArrayRef> = target_fields.iter()
        .zip(struct_arr.columns())
        .map(|(target_field, col)| cast_array_to_field(col, target_field.as_ref()))
        .collect();
    
    // Create new StructArray with target field definitions
    Arc::new(StructArray::new(
        target_fields.clone(),
        new_columns,
        struct_arr.nulls().cloned(),
    ))
}

/// Cast a ListArray to match target element field.
fn cast_list_array(array: &ArrayRef, target_element_field: &Field) -> ArrayRef {
    let list_arr = array.as_any().downcast_ref::<ListArray>()
        .expect("Expected ListArray");
    
    // Cast the values array
    let new_values = cast_array_to_field(list_arr.values(), target_element_field);
    
    // Create new ListArray with cast values
    let new_field = Arc::new(target_element_field.clone());
    Arc::new(ListArray::new(
        new_field,
        list_arr.offsets().clone(),
        new_values,
        list_arr.nulls().cloned(),
    ))
}

/// Cast a LargeListArray to match target element field.
fn cast_large_list_array(array: &ArrayRef, target_element_field: &Field) -> ArrayRef {
    use arrow::array::LargeListArray;
    
    let list_arr = array.as_any().downcast_ref::<LargeListArray>()
        .expect("Expected LargeListArray");
    
    // Cast the values array
    let new_values = cast_array_to_field(list_arr.values(), target_element_field);
    
    // Create new LargeListArray with cast values
    let new_field = Arc::new(target_element_field.clone());
    Arc::new(LargeListArray::new(
        new_field,
        list_arr.offsets().clone(),
        new_values,
        list_arr.nulls().cloned(),
    ))
}

/// Cast a MapArray to match target entries field.
fn cast_map_array(array: &ArrayRef, target_entries_field: &Field) -> ArrayRef {
    let map_arr = array.as_any().downcast_ref::<MapArray>()
        .expect("Expected MapArray");
    
    // Cast the entries struct array
    if let DataType::Struct(target_entry_fields) = target_entries_field.data_type() {
        // map_arr.entries() returns &StructArray, convert to ArrayRef for our function
        let entries_ref: ArrayRef = Arc::new(map_arr.entries().clone());
        let new_entries = cast_struct_array(&entries_ref, target_entry_fields);
        let entries_struct = new_entries.as_any().downcast_ref::<StructArray>()
            .expect("Cast should produce StructArray");
        
        let new_field = Arc::new(target_entries_field.clone());
        Arc::new(MapArray::new(
            new_field,
            map_arr.offsets().clone(),
            entries_struct.clone(),
            map_arr.nulls().cloned(),
            false, // keys_sorted
        ))
    } else {
        // Fallback: return original if structure doesn't match
        array.clone()
    }
}

impl RecordBatchStream for NullabilityValidationStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.target_schema)
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
