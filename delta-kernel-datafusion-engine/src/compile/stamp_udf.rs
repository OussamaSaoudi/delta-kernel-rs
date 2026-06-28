//! [`ScalarUDF`] that stamps a kernel logical [`ArrowField`] onto a single input column.
//!
//! At plan time the UDF declares the target [`FieldRef`] (logical name, Delta metadata, and
//! nested struct/list/map names) via [`ScalarUDFImpl::return_field_from_args`]. At runtime it
//! casts the input array to that declared type with `arrow::compute::cast`, which renames nested
//! fields by position rather than by name.
//!
//! Used instead of `Expr::Cast` (no metadata; rejects column-mapping struct renames) or
//! `Expr::Alias` (name only, no metadata).

use std::sync::Arc;

use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef, Fields as ArrowFields,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// Declares a target [`FieldRef`] on its single argument without changing array values.
///
/// `Hash`/`Eq` satisfy [`ScalarUDFImpl`] for optimizer UDF dedup.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StampFieldUdf {
    /// Output [`FieldRef`] returned from [`Self::return_field_from_args`].
    target_field: FieldRef,
    signature: Signature,
}

impl StampFieldUdf {
    /// Build a UDF that stamps `target_field` onto its single argument.
    pub fn new(target_field: FieldRef) -> Self {
        Self {
            target_field,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Wrap `arg` in this UDF as a `ScalarFunction` expression.
    pub fn call(self, arg: Expr) -> Expr {
        let udf = Arc::new(ScalarUDF::from(self));
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![arg]))
    }
}

impl ScalarUDFImpl for StampFieldUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "kernel_stamp_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DfResult<ArrowDataType> {
        Ok(self.target_field.data_type().clone())
    }

    /// Returns a merged [`FieldRef`]: target names and metadata with input nullability at every
    /// level so the declared schema matches what [`Self::invoke_with_args`] can materialize.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
        let input = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{}: expected one argument field; got {}",
                self.name(),
                args.arg_fields.len()
            ))
        })?;
        Ok(merge_field(input, &self.target_field))
    }

    /// Casts the input array to `args.return_field`'s data type; passes scalars through unchanged.
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arg = args.args.swap_remove(0);
        let target_dt = args.return_field.data_type();
        match arg {
            ColumnarValue::Array(arr) if arr.data_type() == target_dt => {
                Ok(ColumnarValue::Array(arr))
            }
            ColumnarValue::Array(arr) => {
                let casted = cast(arr.as_ref(), target_dt)?;
                Ok(ColumnarValue::Array(casted))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s)),
        }
    }
}

/// Merge `target`'s logical name and metadata onto `input`'s runtime nullability, recursively
/// for nested struct / list / map types.
fn merge_field(input: &FieldRef, target: &FieldRef) -> FieldRef {
    let merged_dt = merge_data_type(input.data_type(), target.data_type());
    let mut field = ArrowField::new(target.name(), merged_dt, input.is_nullable());
    if !target.metadata().is_empty() {
        field = field.with_metadata(target.metadata().clone());
    }
    Arc::new(field)
}

fn merge_data_type(input_dt: &ArrowDataType, target_dt: &ArrowDataType) -> ArrowDataType {
    match (input_dt, target_dt) {
        (ArrowDataType::Struct(input_fields), ArrowDataType::Struct(target_fields))
            if input_fields.len() == target_fields.len() =>
        {
            ArrowDataType::Struct(merge_fields(input_fields, target_fields))
        }
        (ArrowDataType::List(input_elem), ArrowDataType::List(target_elem)) => {
            ArrowDataType::List(merge_field(input_elem, target_elem))
        }
        (ArrowDataType::LargeList(input_elem), ArrowDataType::LargeList(target_elem)) => {
            ArrowDataType::LargeList(merge_field(input_elem, target_elem))
        }
        (
            ArrowDataType::FixedSizeList(input_elem, input_n),
            ArrowDataType::FixedSizeList(target_elem, _),
        ) => ArrowDataType::FixedSizeList(merge_field(input_elem, target_elem), *input_n),
        (ArrowDataType::Map(input_kv, input_sorted), ArrowDataType::Map(target_kv, _)) => {
            ArrowDataType::Map(merge_field(input_kv, target_kv), *input_sorted)
        }
        // Mismatched shapes keep the input runtime type; parent metadata still flows through.
        _ => input_dt.clone(),
    }
}

fn merge_fields(input_fields: &ArrowFields, target_fields: &ArrowFields) -> ArrowFields {
    input_fields
        .iter()
        .zip(target_fields.iter())
        .map(|(i, t)| merge_field(i, t))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::execution::context::SessionContext;
    use datafusion_common::arrow::array::{Int64Array, RecordBatch};
    use datafusion_common::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion_expr::col;

    use super::*;

    fn stamped_field(id: &str) -> FieldRef {
        Arc::new(
            ArrowField::new("x", ArrowDataType::Int64, true).with_metadata(HashMap::from([(
                "delta.columnMapping.id".to_string(),
                id.to_string(),
            )])),
        )
    }

    #[tokio::test]
    async fn stamp_udf_carries_field_metadata_through_projection() {
        let ctx = SessionContext::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            ArrowDataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();

        let df = ctx
            .read_batch(batch)
            .expect("read batch")
            .select(vec![StampFieldUdf::new(stamped_field("7"))
                .call(col("x"))
                .alias("x")])
            .expect("select");

        let logical_meta = df.schema().field(0).metadata().clone();
        assert_eq!(
            logical_meta.get("delta.columnMapping.id"),
            Some(&"7".to_string()),
            "stamp UDF must surface the declared FieldRef metadata into the \
             projection's output schema"
        );

        let collected = df.collect().await.expect("collect");
        let runtime_meta = collected[0].schema().field(0).metadata().clone();
        assert_eq!(
            runtime_meta.get("delta.columnMapping.id"),
            Some(&"7".to_string()),
            "stamped metadata must also flow through to the materialized batch schema"
        );
    }
}
