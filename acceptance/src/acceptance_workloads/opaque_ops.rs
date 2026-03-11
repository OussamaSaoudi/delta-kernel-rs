//! Opaque expression operations for SQL functions used in test predicates.
//!
//! Implements `ArrowOpaqueExpressionOp` for functions like `year()`, `month()`,
//! `trunc()`, `date_trunc()`, `date_add()`, `datediff()`, `length()`, `size()`,
//! and modulo (`%`).

use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, Date32Array, GenericListArray, Int32Array, Int64Array, OffsetSizeTrait,
    RecordBatch, TimestampMicrosecondArray,
};
use delta_kernel::arrow::compute::kernels::cast::cast;
use delta_kernel::arrow::compute::kernels::numeric::rem;
use delta_kernel::arrow::compute::kernels::temporal::{date_part, DatePart};
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaqueExpression, ArrowOpaqueExpressionOp,
};
use delta_kernel::expressions::{Expression, Scalar, ScalarExpressionEvaluator};
use delta_kernel::schema::DataType;
use delta_kernel::DeltaResult;

// ---------------------------------------------------------------------------
// year()
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct YearOp;

impl ArrowOpaqueExpressionOp for YearOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 1, "year() takes exactly one argument");
        let arr = evaluate_expression(&args[0], batch, None)?;
        let result = date_part(arr.as_ref(), DatePart::Year)?;
        // Cast Int32 -> Int64 to match default Long literals
        Ok(cast(&result, &ArrowDataType::Int64)?)
    }

    fn name(&self) -> &str {
        "year"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let scalar = eval_scalar(eval, &exprs[0])?;
        let year = extract_year_from_scalar(&scalar)?;
        Ok(Scalar::Long(year as i64))
    }
}

// ---------------------------------------------------------------------------
// month()
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct MonthOp;

impl ArrowOpaqueExpressionOp for MonthOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 1, "month() takes exactly one argument");
        let arr = evaluate_expression(&args[0], batch, None)?;
        let result = date_part(arr.as_ref(), DatePart::Month)?;
        // Cast Int32 -> Int64 to match default Long literals
        Ok(cast(&result, &ArrowDataType::Int64)?)
    }

    fn name(&self) -> &str {
        "month"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let scalar = eval_scalar(eval, &exprs[0])?;
        let month = extract_month_from_scalar(&scalar)?;
        Ok(Scalar::Long(month as i64))
    }
}

// ---------------------------------------------------------------------------
// trunc(date_col, 'YEAR'|'MONTH')  — Spark's trunc() for dates
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct TruncOp;

impl ArrowOpaqueExpressionOp for TruncOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        // trunc(date_col, 'YEAR') or trunc(date_col, 'MONTH')
        assert!(args.len() == 2, "trunc() takes exactly two arguments");

        let date_arr = evaluate_expression(&args[0], batch, None)?;
        // Second arg is a string literal for the precision
        let precision = extract_string_literal(&args[1])?;

        truncate_date_array(date_arr.as_ref(), &precision)
    }

    fn name(&self) -> &str {
        "trunc"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let date_scalar = eval_scalar(eval, &exprs[0])?;
        let precision = extract_string_literal(&exprs[1])?;
        truncate_date_scalar(&date_scalar, &precision)
    }
}

// ---------------------------------------------------------------------------
// date_trunc('MONTH', timestamp_col)  — Spark's date_trunc() for timestamps
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DateTruncOp;

impl ArrowOpaqueExpressionOp for DateTruncOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        // date_trunc('MONTH', ts_col)
        assert!(args.len() == 2, "date_trunc() takes exactly two arguments");

        let precision = extract_string_literal(&args[0])?;
        let ts_arr = evaluate_expression(&args[1], batch, None)?;

        truncate_timestamp_array(ts_arr.as_ref(), &precision)
    }

    fn name(&self) -> &str {
        "date_trunc"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let precision = extract_string_literal(&exprs[0])?;
        let ts_scalar = eval_scalar(eval, &exprs[1])?;
        truncate_timestamp_scalar(&ts_scalar, &precision)
    }
}

// ---------------------------------------------------------------------------
// date_add(date_col, days_int)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DateAddOp;

impl ArrowOpaqueExpressionOp for DateAddOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 2, "date_add() takes exactly two arguments");

        let date_arr = evaluate_expression(&args[0], batch, None)?;
        let days_arr = evaluate_expression(&args[1], batch, None)?;

        // Date32 + days: manually add since arrow doesn't support Date32 + Int32 directly
        let date_arr = date_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| delta_kernel::Error::generic("date_add: expected Date32 array"))?;
        // Cast days to Int32
        let days_i32 = cast(days_arr.as_ref(), &ArrowDataType::Int32)?;
        let days_arr = days_i32
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| delta_kernel::Error::generic("date_add: expected Int32 days"))?;

        let mut builder = Date32Array::builder(date_arr.len());
        for i in 0..date_arr.len() {
            if date_arr.is_null(i) {
                builder.append_null();
            } else {
                let days_to_add = if days_arr.len() == 1 {
                    // Scalar
                    days_arr.value(0)
                } else {
                    if days_arr.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    days_arr.value(i)
                };
                builder.append_value(date_arr.value(i) + days_to_add);
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn name(&self) -> &str {
        "date_add"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let date_scalar = eval_scalar(eval, &exprs[0])?;
        let days_scalar = eval_scalar(eval, &exprs[1])?;

        let days = match date_scalar {
            Scalar::Date(d) => d,
            Scalar::Null(_) => return Ok(Scalar::Null(DataType::DATE)),
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "date_add: expected Date scalar, got {:?}",
                    date_scalar
                )))
            }
        };

        let days_to_add = scalar_to_i32(&days_scalar)?;
        Ok(Scalar::Date(days + days_to_add))
    }
}

// ---------------------------------------------------------------------------
// datediff(end_date, start_date) -> integer (number of days)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DateDiffOp;

impl ArrowOpaqueExpressionOp for DateDiffOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 2, "datediff() takes exactly two arguments");

        let end_arr = evaluate_expression(&args[0], batch, None)?;
        let start_arr = evaluate_expression(&args[1], batch, None)?;

        // For Date32 arrays: subtract gives Duration(s), convert to days as Int64
        let end_date = end_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| delta_kernel::Error::generic("datediff: expected Date32 arrays"))?;
        let start_date = start_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| delta_kernel::Error::generic("datediff: expected Date32 arrays"))?;

        // Date32 values are already days since epoch, so just subtract
        let mut builder = Int64Array::builder(end_date.len());
        for i in 0..end_date.len() {
            if end_date.is_null(i) || start_date.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value((end_date.value(i) - start_date.value(i)) as i64);
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn name(&self) -> &str {
        "datediff"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let end_scalar = eval_scalar(eval, &exprs[0])?;
        let start_scalar = eval_scalar(eval, &exprs[1])?;

        let end_days = match end_scalar {
            Scalar::Date(d) => d,
            Scalar::Null(_) => return Ok(Scalar::Null(DataType::LONG)),
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "datediff: expected Date scalar, got {:?}",
                    end_scalar
                )))
            }
        };

        let start_days = match start_scalar {
            Scalar::Date(d) => d,
            Scalar::Null(_) => return Ok(Scalar::Null(DataType::LONG)),
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "datediff: expected Date scalar, got {:?}",
                    start_scalar
                )))
            }
        };

        Ok(Scalar::Long((end_days - start_days) as i64))
    }
}

// ---------------------------------------------------------------------------
// length(string_col) -> integer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct LengthOp;

impl ArrowOpaqueExpressionOp for LengthOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 1, "length() takes exactly one argument");
        let arr = evaluate_expression(&args[0], batch, None)?;
        let result = delta_kernel::arrow::compute::kernels::length::length(arr.as_ref())?;
        // Cast Int32 -> Int64 to match default Long literals
        Ok(cast(&result, &ArrowDataType::Int64)?)
    }

    fn name(&self) -> &str {
        "length"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let scalar = eval_scalar(eval, &exprs[0])?;
        match scalar {
            Scalar::String(s) => Ok(Scalar::Long(s.chars().count() as i64)),
            Scalar::Null(_) => Ok(Scalar::Null(DataType::LONG)),
            _ => Err(delta_kernel::Error::generic(format!(
                "length: expected String scalar, got {:?}",
                scalar
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// size(array_col) -> integer (number of elements in list/array)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct SizeOp;

impl ArrowOpaqueExpressionOp for SizeOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 1, "size() takes exactly one argument");
        let arr = evaluate_expression(&args[0], batch, None)?;

        // Try as large list first, then regular list
        if let Some(list) = arr.as_any().downcast_ref::<GenericListArray<i64>>() {
            let result = list_lengths(list);
            Ok(cast(&result, &ArrowDataType::Int64)?)
        } else if let Some(list) = arr.as_any().downcast_ref::<GenericListArray<i32>>() {
            let result = list_lengths(list);
            Ok(cast(&result, &ArrowDataType::Int64)?)
        } else {
            Err(delta_kernel::Error::generic(format!(
                "size(): expected list/array column, got {:?}",
                arr.data_type()
            )))
        }
    }

    fn name(&self) -> &str {
        "size"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let scalar = eval_scalar(eval, &exprs[0])?;
        match scalar {
            Scalar::Array(arr) => Ok(Scalar::Long(arr.array_elements().len() as i64)),
            Scalar::Null(_) => Ok(Scalar::Long(-1)), // Spark returns -1 for null arrays
            _ => Err(delta_kernel::Error::generic(format!(
                "size: expected Array scalar, got {:?}",
                scalar
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Modulo (a % b)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ModuloOp;

impl ArrowOpaqueExpressionOp for ModuloOp {
    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        assert!(args.len() == 2, "modulo takes exactly two arguments");
        let lhs = evaluate_expression(&args[0], batch, None)?;
        let rhs = evaluate_expression(&args[1], batch, None)?;
        Ok(rem(&lhs, &rhs)?)
    }

    fn name(&self) -> &str {
        "modulo"
    }

    fn eval_expr_scalar(
        &self,
        eval: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        let lhs = eval_scalar(eval, &exprs[0])?;
        let rhs = eval_scalar(eval, &exprs[1])?;
        scalar_modulo(&lhs, &rhs)
    }
}

// ===========================================================================
// Helper functions
// ===========================================================================

/// Extract a string literal from a kernel Expression.
fn extract_string_literal(expr: &Expression) -> DeltaResult<String> {
    match expr {
        Expression::Literal(Scalar::String(s)) => Ok(s.clone()),
        _ => Err(delta_kernel::Error::generic(format!(
            "Expected string literal, got: {:?}",
            expr
        ))),
    }
}

/// Evaluate an expression using the scalar evaluator, returning an error if None.
fn eval_scalar(eval: &ScalarExpressionEvaluator<'_>, expr: &Expression) -> DeltaResult<Scalar> {
    eval(expr).ok_or_else(|| {
        delta_kernel::Error::generic(format!("Failed to evaluate expression: {:?}", expr))
    })
}

/// Convert microseconds since epoch to NaiveDateTime.
fn micros_to_naive_datetime(micros: i64) -> DeltaResult<chrono::NaiveDateTime> {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| delta_kernel::Error::generic("Invalid timestamp microseconds"))
}

/// Convert a Date or Timestamp scalar to a chrono NaiveDate.
fn scalar_to_naive_date(scalar: &Scalar) -> DeltaResult<chrono::NaiveDate> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    match scalar {
        Scalar::Date(days) => Ok(epoch + chrono::Duration::days(*days as i64)),
        Scalar::Timestamp(micros) | Scalar::TimestampNtz(micros) => {
            let dt = micros_to_naive_datetime(*micros)?;
            Ok(dt.date())
        }
        _ => Err(delta_kernel::Error::generic(format!(
            "Expected Date or Timestamp scalar, got {:?}",
            scalar
        ))),
    }
}

/// Convert a Timestamp scalar to a chrono NaiveDateTime.
fn scalar_to_naive_datetime(scalar: &Scalar) -> DeltaResult<chrono::NaiveDateTime> {
    match scalar {
        Scalar::Timestamp(micros) | Scalar::TimestampNtz(micros) => {
            micros_to_naive_datetime(*micros)
        }
        Scalar::Date(days) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch + chrono::Duration::days(*days as i64);
            Ok(date.and_hms_opt(0, 0, 0).unwrap())
        }
        _ => Err(delta_kernel::Error::generic(format!(
            "Expected Timestamp scalar, got {:?}",
            scalar
        ))),
    }
}

/// Extract year from a Date or Timestamp scalar.
fn extract_year_from_scalar(scalar: &Scalar) -> DeltaResult<i32> {
    if matches!(scalar, Scalar::Null(_)) {
        return Err(delta_kernel::Error::generic(
            "Cannot extract year from null",
        ));
    }
    let date = scalar_to_naive_date(scalar)?;
    Ok(date.year())
}

/// Extract month from a Date or Timestamp scalar.
fn extract_month_from_scalar(scalar: &Scalar) -> DeltaResult<u32> {
    if matches!(scalar, Scalar::Null(_)) {
        return Err(delta_kernel::Error::generic(
            "Cannot extract month from null",
        ));
    }
    let date = scalar_to_naive_date(scalar)?;
    Ok(date.month())
}

/// Truncate a Date scalar to the given precision.
fn truncate_date_scalar(scalar: &Scalar, precision: &str) -> DeltaResult<Scalar> {
    if matches!(scalar, Scalar::Null(_)) {
        return Ok(Scalar::Null(DataType::DATE));
    }

    let date = scalar_to_naive_date(scalar)?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    let truncated = match precision.to_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => chrono::NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap(),
        "MONTH" | "MON" | "MM" => {
            chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap()
        }
        _ => {
            return Err(delta_kernel::Error::generic(format!(
                "trunc: unsupported precision: {}",
                precision
            )))
        }
    };

    let days = (truncated - epoch).num_days() as i32;
    Ok(Scalar::Date(days))
}

/// Truncate a Timestamp scalar to the given precision.
fn truncate_timestamp_scalar(scalar: &Scalar, precision: &str) -> DeltaResult<Scalar> {
    let is_ntz = matches!(scalar, Scalar::TimestampNtz(_));

    if matches!(scalar, Scalar::Null(_)) {
        return Ok(Scalar::Null(if is_ntz {
            DataType::TIMESTAMP_NTZ
        } else {
            DataType::TIMESTAMP
        }));
    }

    let dt = scalar_to_naive_datetime(scalar)?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let truncated = match precision.to_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        "MONTH" | "MON" | "MM" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        "DAY" | "DD" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        "HOUR" | "HH" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .unwrap()
            .and_hms_opt(dt.hour(), 0, 0)
            .unwrap(),
        _ => {
            return Err(delta_kernel::Error::generic(format!(
                "date_trunc: unsupported precision: {}",
                precision
            )))
        }
    };

    let micros = truncated
        .signed_duration_since(epoch)
        .num_microseconds()
        .unwrap();

    if is_ntz {
        Ok(Scalar::TimestampNtz(micros))
    } else {
        Ok(Scalar::Timestamp(micros))
    }
}

/// Convert a numeric scalar to i32.
fn scalar_to_i32(scalar: &Scalar) -> DeltaResult<i32> {
    match scalar {
        Scalar::Byte(v) => Ok(*v as i32),
        Scalar::Short(v) => Ok(*v as i32),
        Scalar::Integer(v) => Ok(*v),
        Scalar::Long(v) => Ok(*v as i32),
        Scalar::Null(_) => Err(delta_kernel::Error::generic("Cannot convert null to i32")),
        _ => Err(delta_kernel::Error::generic(format!(
            "Expected numeric scalar, got {:?}",
            scalar
        ))),
    }
}

/// Compute modulo of two numeric scalars.
fn scalar_modulo(lhs: &Scalar, rhs: &Scalar) -> DeltaResult<Scalar> {
    match (lhs, rhs) {
        (Scalar::Null(dt), _) | (_, Scalar::Null(dt)) => Ok(Scalar::Null(dt.clone())),
        (Scalar::Byte(a), Scalar::Byte(b)) => Ok(Scalar::Byte(a % b)),
        (Scalar::Short(a), Scalar::Short(b)) => Ok(Scalar::Short(a % b)),
        (Scalar::Integer(a), Scalar::Integer(b)) => Ok(Scalar::Integer(a % b)),
        (Scalar::Long(a), Scalar::Long(b)) => Ok(Scalar::Long(a % b)),
        // Mixed integer types: promote to Long
        (Scalar::Long(a), b) => {
            let b_val = scalar_to_i64(b)?;
            Ok(Scalar::Long(a % b_val))
        }
        (a, Scalar::Long(b)) => {
            let a_val = scalar_to_i64(a)?;
            Ok(Scalar::Long(a_val % b))
        }
        (Scalar::Integer(a), b) => {
            let b_val = scalar_to_i32(b)?;
            Ok(Scalar::Integer(a % b_val))
        }
        (a, Scalar::Integer(b)) => {
            let a_val = scalar_to_i32(a)?;
            Ok(Scalar::Integer(a_val % b))
        }
        (Scalar::Float(a), Scalar::Float(b)) => Ok(Scalar::Float(a % b)),
        (Scalar::Double(a), Scalar::Double(b)) => Ok(Scalar::Double(a % b)),
        _ => Err(delta_kernel::Error::generic(format!(
            "modulo: unsupported types {:?} % {:?}",
            lhs, rhs
        ))),
    }
}

/// Convert a numeric scalar to i64.
fn scalar_to_i64(scalar: &Scalar) -> DeltaResult<i64> {
    match scalar {
        Scalar::Byte(v) => Ok(*v as i64),
        Scalar::Short(v) => Ok(*v as i64),
        Scalar::Integer(v) => Ok(*v as i64),
        Scalar::Long(v) => Ok(*v),
        Scalar::Null(_) => Err(delta_kernel::Error::generic("Cannot convert null to i64")),
        _ => Err(delta_kernel::Error::generic(format!(
            "Expected numeric scalar, got {:?}",
            scalar
        ))),
    }
}

/// Compute list lengths for a GenericListArray.
fn list_lengths<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> ArrayRef {
    let mut builder = Int32Array::builder(list.len());
    for i in 0..list.len() {
        if list.is_null(i) {
            // Spark's size() returns -1 for null arrays, but we match the test expectations
            builder.append_value(-1);
        } else {
            builder.append_value(list.value_length(i).as_usize() as i32);
        }
    }
    Arc::new(builder.finish())
}

/// Truncate a Date32 array to the given precision (YEAR or MONTH).
fn truncate_date_array(arr: &dyn Array, precision: &str) -> DeltaResult<ArrayRef> {
    let date_arr = arr
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| delta_kernel::Error::generic("trunc: expected Date32 array"))?;

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut builder = Date32Array::builder(date_arr.len());

    for i in 0..date_arr.len() {
        if date_arr.is_null(i) {
            builder.append_null();
        } else {
            let days = date_arr.value(i);
            let date = epoch + chrono::Duration::days(days as i64);
            let truncated = match precision.to_uppercase().as_str() {
                "YEAR" | "YYYY" | "YY" => {
                    chrono::NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap()
                }
                "MONTH" | "MON" | "MM" => {
                    chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap()
                }
                _ => {
                    return Err(delta_kernel::Error::generic(format!(
                        "trunc: unsupported precision: {}",
                        precision
                    )));
                }
            };
            let result_days = (truncated - epoch).num_days() as i32;
            builder.append_value(result_days);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Truncate a Timestamp (microsecond) array to the given precision.
fn truncate_timestamp_array(arr: &dyn Array, precision: &str) -> DeltaResult<ArrayRef> {
    // Extract microsecond values regardless of timezone
    let (micros, original_type) = match arr.data_type() {
        ArrowDataType::Timestamp(delta_kernel::arrow::datatypes::TimeUnit::Microsecond, _tz) => {
            let ts_arr = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    delta_kernel::Error::generic("date_trunc: failed to downcast timestamp array")
                })?;
            let vals: Vec<Option<i64>> = (0..ts_arr.len())
                .map(|i| {
                    if ts_arr.is_null(i) {
                        None
                    } else {
                        Some(ts_arr.value(i))
                    }
                })
                .collect();
            (vals, arr.data_type().clone())
        }
        _ => {
            return Err(delta_kernel::Error::generic(format!(
                "date_trunc: expected TimestampMicrosecond array, got {:?}",
                arr.data_type()
            )));
        }
    };

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut builder = TimestampMicrosecondArray::builder(micros.len());
    for m in &micros {
        match m {
            None => builder.append_null(),
            Some(us) => {
                let dt = epoch + chrono::Duration::microseconds(*us);
                let truncated = match precision.to_uppercase().as_str() {
                    "YEAR" | "YYYY" | "YY" => chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                    "MONTH" | "MON" | "MM" => {
                        chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                    }
                    "DAY" | "DD" => {
                        chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                    }
                    "HOUR" | "HH" => {
                        chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                            .unwrap()
                            .and_hms_opt(dt.hour(), 0, 0)
                            .unwrap()
                    }
                    _ => {
                        return Err(delta_kernel::Error::generic(format!(
                            "date_trunc: unsupported precision: {}",
                            precision
                        )));
                    }
                };
                let result_us = truncated
                    .signed_duration_since(epoch)
                    .num_microseconds()
                    .unwrap();
                builder.append_value(result_us);
            }
        }
    }

    let result = builder.finish();
    // Cast to preserve original timezone if present
    Ok(cast(&result, &original_type)?)
}

// Bring chrono traits into scope
use chrono::{Datelike, Timelike};

/// Helper: create an opaque expression for a function call.
pub fn make_function_opaque(name: &str, args: Vec<Expression>) -> Result<Expression, String> {
    match name.to_lowercase().as_str() {
        "year" => Ok(Expression::arrow_opaque(YearOp, args)),
        "month" => Ok(Expression::arrow_opaque(MonthOp, args)),
        "trunc" => Ok(Expression::arrow_opaque(TruncOp, args)),
        "date_trunc" => Ok(Expression::arrow_opaque(DateTruncOp, args)),
        "date_add" => Ok(Expression::arrow_opaque(DateAddOp, args)),
        "datediff" => Ok(Expression::arrow_opaque(DateDiffOp, args)),
        "length" => Ok(Expression::arrow_opaque(LengthOp, args)),
        "size" => Ok(Expression::arrow_opaque(SizeOp, args)),
        _ => Err(format!("Unsupported function: {}", name)),
    }
}

/// Create a modulo opaque expression.
pub fn make_modulo_opaque(left: Expression, right: Expression) -> Expression {
    Expression::arrow_opaque(ModuloOp, [left, right])
}
