//! Kernel [`Expression`] / [`Predicate`] -> DataFusion [`Expr`] translation.
//!
//! Single entry point: [`kernel_expr_to_df`] takes a kernel expression and a
//! [`TranslationContext`] (optional output-field target + optional input schema).
//!
//! Cast policy: primitive targets are wrapped in `cast(..)`; nested-container targets
//! (Struct/Array/Map) trust the producing arm to emit the right shape, because DataFusion's
//! `cast` on these is name-based and silently breaks column-mapping reshape.

use std::sync::Arc;

use datafusion_common::arrow::array::StructArray;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Case, InList};
use datafusion_expr::expr_fn::cast;
use datafusion_expr::{lit, Expr, Operator};
use datafusion_functions::core::expr_fn::{
    coalesce, get_field, named_struct, r#struct as make_struct,
};
use datafusion_functions_nested::expr_fn::make_array;
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
    Expression, JunctionPredicate, JunctionPredicateOp, MapToStructExpression, ParseJsonExpression,
    Predicate, Scalar, StructData, UnaryPredicate, UnaryPredicateOp, VariadicExpression,
    VariadicExpressionOp,
};
use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};

use crate::compile::json_parse;
use crate::error::{internal_error, plan_compilation, unsupported};

/// Optional target context for [`kernel_expr_to_df`]. See module docs for the contract.
#[derive(Default, Clone, Copy)]
pub struct TranslationContext<'a> {
    /// Output field whose declared kernel type drives target shaping and primitive casts.
    pub output_field: Option<&'a StructField>,
    /// Required by identity-`Transform` to resolve source struct physical field names.
    pub input_schema: Option<&'a ArrowSchema>,
}

impl<'a> TranslationContext<'a> {
    /// Context with no output target or input schema; natural-type translation only.
    pub fn untyped() -> Self {
        Self::default()
    }

    /// Context for a projection arm: `output_field` drives casts/shaping and `input_schema`
    /// resolves physical column names for identity transforms.
    pub fn typed(output_field: &'a StructField, input_schema: &'a ArrowSchema) -> Self {
        Self {
            output_field: Some(output_field),
            input_schema: Some(input_schema),
        }
    }

    /// Apply this context's target type to `raw`: wrap primitives in `cast(..)`; pass nested
    /// container targets through unchanged because DataFusion's cast is name-based there.
    fn apply_target_cast(&self, raw: Expr) -> Result<Expr, DataFusionError> {
        let Some(field) = self.output_field else {
            return Ok(raw);
        };
        if matches!(
            field.data_type(),
            DataType::Struct(_) | DataType::Array(_) | DataType::Map(_)
        ) {
            return Ok(raw);
        }
        let arrow_ty: ArrowDataType = field.data_type().try_into_arrow().map_err(|e| {
            plan_compilation(format!(
                "kernel_expr_to_df: target type conversion failed for `{}`: {e}",
                field.name()
            ))
        })?;
        Ok(cast(raw, arrow_ty))
    }
}

/// Translate a slice of kernel `Arc<Expression>`s with the same context.
pub fn kernel_exprs_to_df(
    exprs: &[Arc<Expression>],
    cx: &TranslationContext<'_>,
) -> Result<Vec<Expr>, DataFusionError> {
    exprs.iter().map(|e| kernel_expr_to_df(e, cx)).collect()
}

/// Shorthand for translating a kernel `Expression` without a target context. The vast majority
/// of internal call sites do not have an output field on hand (only the outer projection arm
/// in `compile_project_node` does); collapsing the `&TranslationContext::untyped()` boilerplate
/// into one name keeps the natural-type arms readable.
pub fn kernel_expr_to_df_untyped(expr: &Expression) -> Result<Expr, DataFusionError> {
    kernel_expr_to_df(expr, &TranslationContext::untyped())
}

/// Slice form of [`kernel_expr_to_df_untyped`].
pub fn kernel_exprs_to_df_untyped(exprs: &[Arc<Expression>]) -> Result<Vec<Expr>, DataFusionError> {
    kernel_exprs_to_df(exprs, &TranslationContext::untyped())
}

/// Translate a kernel [`Expression`] to a DataFusion [`Expr`].
pub fn kernel_expr_to_df(
    expr: &Expression,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    match expr {
        Expression::ParseJson(parse_json) => return parse_json_to_df(parse_json),
        Expression::MapToStruct(map_to_struct) => return map_to_struct_to_df(map_to_struct, cx),
        Expression::StructPatch(patch) => {
            let input = cx
                .input_schema
                .ok_or_else(|| plan_compilation("StructPatch requires a typed input schema"))?;
            let input = StructType::try_from_arrow(input).map_err(|e| {
                plan_compilation(format!("StructPatch input schema conversion failed: {e}"))
            })?;
            let children = crate::compile::logical::expand_patch(patch, &input)?;
            return struct_to_df(&children, None, cx);
        }
        Expression::Struct(children, nullability_predicate) => {
            return struct_to_df(children.as_slice(), nullability_predicate.as_ref(), cx);
        }
        _ => {}
    }
    let raw = match expr {
        Expression::Literal(scalar) => Expr::Literal(scalar_value_to_df(scalar)?, None),
        Expression::Column(name) => column_to_df(name)?,
        Expression::Predicate(pred) => kernel_pred_to_df(pred)?,
        Expression::Binary(BinaryExpression { op, left, right }) => {
            binary_expr_to_df(*op, left, right)?
        }
        Expression::Variadic(VariadicExpression { op, exprs }) => variadic_to_df(*op, exprs, cx)?,
        Expression::Cast(cast_expr) => {
            let target: ArrowDataType = (&cast_expr.target)
                .try_into_arrow()
                .map_err(|e| plan_compilation(format!("Cast target conversion failed: {e}")))?;
            cast(kernel_expr_to_df_untyped(cast_expr.expr.as_ref())?, target)
        }
        Expression::Unary(_) => {
            return Err(unsupported(
                "expr_translator: Unary(ToJson) is not supported",
            ))
        }
        Expression::Opaque(_) => {
            return Err(unsupported(
                "expr_translator: Opaque expressions are not supported (engine-defined ops \
                 do not round-trip through DataFusion logical plans)",
            ))
        }
        Expression::Unknown(name) => {
            return Err(unsupported(format!(
                "expr_translator: Unknown expression {name:?} cannot be translated"
            )))
        }
        // Already handled above and returned early.
        Expression::ParseJson(_)
        | Expression::MapToStruct(_)
        | Expression::StructPatch(_)
        | Expression::Struct(_, _) => {
            return Err(internal_error(
                "expr_translator: target-shaping arm fell through; should have returned early",
            ));
        }
    };
    cx.apply_target_cast(raw)
}

/// Translate a kernel [`Predicate`] to a boolean-typed DataFusion [`Expr`].
pub fn kernel_pred_to_df(pred: &Predicate) -> Result<Expr, DataFusionError> {
    match pred {
        Predicate::BooleanExpression(expr) => kernel_expr_to_df_untyped(expr),
        Predicate::Not(inner) => Ok(Expr::Not(Box::new(kernel_pred_to_df(inner)?))),
        Predicate::Unary(UnaryPredicate { op, expr }) => unary_pred_to_df(*op, expr),
        Predicate::Binary(BinaryPredicate { op, left, right }) => {
            binary_pred_to_df(*op, left, right)
        }
        Predicate::Junction(JunctionPredicate { op, preds }) => junction_to_df(*op, preds),
        Predicate::Opaque(_) => Err(unsupported(
            "expr_translator: Opaque predicates are not supported",
        )),
        Predicate::Unknown(name) => Err(unsupported(format!(
            "expr_translator: Unknown predicate {name:?} cannot be translated"
        ))),
    }
}

// === Target-shaping arms (each emits cx.output_field's declared shape) ===

/// `ParseJson` carries its own `output_schema`; works in any context (typed or untyped).
fn parse_json_to_df(parse_json: &ParseJsonExpression) -> Result<Expr, DataFusionError> {
    let json_expr = kernel_expr_to_df_untyped(parse_json.json_expr.as_ref())?;
    let extracted = json_parse::generate_schema_extractions(&json_expr, &parse_json.output_schema)?;
    let mut args = Vec::with_capacity(extracted.len() * 2);
    for (field_expr, field_name) in extracted {
        args.push(lit(field_name));
        args.push(field_expr);
    }
    Ok(named_struct(args))
}

/// `MapToStruct` requires a Struct target in `cx.output_field` (target field list drives
/// extraction). Errors if the context is untyped or the target isn't a Struct.
fn map_to_struct_to_df(
    map_to_struct: &MapToStructExpression,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    let target_field = cx.output_field.ok_or_else(|| {
        unsupported(
            "MapToStruct requires a typed projection context (Struct target); reached \
             without one (the enclosing Project/Coalesce/If didn't propagate target type).",
        )
    })?;
    let DataType::Struct(target_struct) = target_field.data_type() else {
        return Err(plan_compilation(format!(
            "MapToStruct projection requires Struct output type, got {:?}",
            target_field.data_type()
        )));
    };
    let map_expr = kernel_expr_to_df_untyped(map_to_struct.map_expr.as_ref())?;
    let mut args = Vec::with_capacity(target_struct.fields().count() * 2);
    for child in target_struct.fields() {
        let arrow_ty: ArrowDataType = child.data_type().try_into_arrow().map_err(|e| {
            plan_compilation(format!(
                "MapToStruct target field `{}` type conversion failed: {e}",
                child.name()
            ))
        })?;
        let raw_value = get_field(map_expr.clone(), child.name().to_string());
        args.push(lit(child.name().to_string()));
        args.push(cast(raw_value, arrow_ty));
    }
    Ok(named_struct(args))
}

/// Typed Struct target with matching arity: `named_struct(...)` using target field names
/// (column-mapping renames flow through). Otherwise positional `make_struct(...)`. An
/// optional nullability predicate wraps the result in `CASE WHEN p THEN s ELSE NULL END`.
fn struct_to_df(
    children: &[Arc<Expression>],
    nullability_predicate: Option<&Arc<Expression>>,
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    let body = match cx.output_field.map(|f| f.data_type()) {
        Some(DataType::Struct(target_struct))
            if target_struct.fields().count() == children.len() =>
        {
            let mut args = Vec::with_capacity(children.len() * 2);
            for (child_expr, child_field) in children.iter().zip(target_struct.fields()) {
                let child_cx = TranslationContext {
                    output_field: Some(child_field),
                    input_schema: cx.input_schema,
                };
                args.push(lit(child_field.name().to_string()));
                args.push(kernel_expr_to_df(child_expr.as_ref(), &child_cx)?);
            }
            named_struct(args)
        }
        _ => {
            let positional = children
                .iter()
                .map(|e| kernel_expr_to_df_untyped(e.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            make_struct(positional)
        }
    };
    match nullability_predicate {
        None => Ok(body),
        // Kernel semantics: predicate false/null -> struct null. Same as DataFusion CASE.
        Some(predicate) => {
            let pred_expr = kernel_expr_to_df_untyped(predicate.as_ref())?;
            Ok(Expr::Case(Case::new(
                None,
                vec![(Box::new(pred_expr), Box::new(body))],
                Some(Box::new(Expr::Literal(ScalarValue::Null, None))),
            )))
        }
    }
}

// === Natural-type arms + cx-propagating compound arms ===

fn column_to_df(name: &ColumnName) -> Result<Expr, DataFusionError> {
    let mut parts = name.iter();
    let root = parts
        .next()
        .ok_or_else(|| unsupported("expr_translator: empty column path cannot be translated"))?;
    Ok(parts.fold(
        Expr::Column(Column::new_unqualified(root)),
        |expr, field| get_field(expr, field.as_str()),
    ))
}

fn binary_expr_to_df(
    op: BinaryExpressionOp,
    left: &Expression,
    right: &Expression,
) -> Result<Expr, DataFusionError> {
    let df_op = match op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    Ok(binary(
        kernel_expr_to_df_untyped(left)?,
        df_op,
        kernel_expr_to_df_untyped(right)?,
    ))
}

fn unary_pred_to_df(op: UnaryPredicateOp, expr: &Expression) -> Result<Expr, DataFusionError> {
    let inner = kernel_expr_to_df_untyped(expr)?;
    match op {
        UnaryPredicateOp::IsNull => Ok(Expr::IsNull(Box::new(inner))),
    }
}

fn binary_pred_to_df(
    op: BinaryPredicateOp,
    left: &Expression,
    right: &Expression,
) -> Result<Expr, DataFusionError> {
    // `In` is special: kernel models it as `Binary(In, value, array_literal)` where the right
    // side is a constant `Scalar::Array`. DataFusion's `Expr::InList` carries the list as a
    // Vec<Expr>.
    let df_op = match op {
        BinaryPredicateOp::In => return in_pred_to_df(left, right),
        BinaryPredicateOp::Equal => Operator::Eq,
        BinaryPredicateOp::LessThan => Operator::Lt,
        BinaryPredicateOp::GreaterThan => Operator::Gt,
        BinaryPredicateOp::Distinct => Operator::IsDistinctFrom,
    };
    Ok(binary(
        kernel_expr_to_df_untyped(left)?,
        df_op,
        kernel_expr_to_df_untyped(right)?,
    ))
}

fn in_pred_to_df(value: &Expression, list: &Expression) -> Result<Expr, DataFusionError> {
    let Expression::Literal(Scalar::Array(arr)) = list else {
        return Err(unsupported(format!(
            "expr_translator: IN predicate requires a literal array on the right; got {list:?}"
        )));
    };
    let elements = arr
        .array_elements()
        .iter()
        .map(|s| scalar_value_to_df(s).map(|v| Expr::Literal(v, None)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::InList(InList::new(
        Box::new(kernel_expr_to_df_untyped(value)?),
        elements,
        false,
    )))
}

fn junction_to_df(op: JunctionPredicateOp, preds: &[Predicate]) -> Result<Expr, DataFusionError> {
    let df_op = match op {
        JunctionPredicateOp::And => Operator::And,
        JunctionPredicateOp::Or => Operator::Or,
    };
    let mut iter = preds.iter().map(kernel_pred_to_df);
    let first = iter.next().ok_or_else(|| {
        unsupported("expr_translator: empty Junction (And/Or) cannot be lowered")
    })??;
    iter.try_fold(first, |acc, next| Ok(binary(acc, df_op, next?)))
}

/// `Coalesce` propagates `cx` to every branch (all branches must produce the target type).
/// `Array` translates elements untyped (kernel doesn't carry element type in `output_field`).
fn variadic_to_df(
    op: VariadicExpressionOp,
    exprs: &[Expression],
    cx: &TranslationContext<'_>,
) -> Result<Expr, DataFusionError> {
    match op {
        VariadicExpressionOp::Coalesce if exprs.is_empty() => Err(unsupported(
            "expr_translator: COALESCE() requires at least one argument",
        )),
        VariadicExpressionOp::Coalesce => {
            let args: Vec<Expr> = exprs
                .iter()
                .map(|e| kernel_expr_to_df(e, cx))
                .collect::<Result<_, _>>()?;
            Ok(coalesce(args))
        }
        VariadicExpressionOp::Array => {
            let args: Vec<Expr> = exprs
                .iter()
                .map(kernel_expr_to_df_untyped)
                .collect::<Result<_, _>>()?;
            Ok(make_array(args))
        }
    }
}

// === Scalar / NULL conversion helpers ===

pub(crate) fn scalar_value_to_df(scalar: &Scalar) -> Result<ScalarValue, DataFusionError> {
    Ok(match scalar {
        Scalar::Integer(v) => ScalarValue::Int32(Some(*v)),
        Scalar::Long(v) => ScalarValue::Int64(Some(*v)),
        Scalar::Short(v) => ScalarValue::Int16(Some(*v)),
        Scalar::Byte(v) => ScalarValue::Int8(Some(*v)),
        Scalar::Float(v) => ScalarValue::Float32(Some(*v)),
        Scalar::Double(v) => ScalarValue::Float64(Some(*v)),
        Scalar::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Scalar::Boolean(v) => ScalarValue::Boolean(Some(*v)),
        Scalar::Date(v) => ScalarValue::Date32(Some(*v)),
        Scalar::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(*v), Some(Arc::from("UTC"))),
        Scalar::TimestampNtz(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        Scalar::IntervalYearMonth(v) => ScalarValue::Int32(Some(*v)),
        Scalar::IntervalDayTime(v) => ScalarValue::Int64(Some(*v)),
        Scalar::Binary(v) => ScalarValue::Binary(Some(v.clone())),
        Scalar::Decimal(d) => {
            ScalarValue::Decimal128(Some(d.bits()), d.ty().precision(), d.ty().scale() as i8)
        }
        Scalar::Null(dt) => typed_null_to_df(dt)?,
        Scalar::Struct(v) => scalar_struct_value_to_df(v)?,
        Scalar::Array(_) | Scalar::Map(_) => {
            return Err(unsupported(format!(
                "expr_translator: complex literal scalar {:?} is not supported",
                scalar.data_type()
            )))
        }
    })
}

fn scalar_struct_value_to_df(struct_data: &StructData) -> Result<ScalarValue, DataFusionError> {
    let fields: Vec<ArrowField> = struct_data
        .fields()
        .iter()
        .map(|f| {
            f.try_into_arrow().map_err(|e| {
                unsupported(format!(
                    "expr_translator: struct literal field `{}` conversion failed: {e}",
                    f.name()
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let values = struct_data
        .values()
        .iter()
        .map(|v| scalar_value_to_df(v)?.to_array())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
        fields.into(),
        values,
        None,
    ))))
}

fn typed_null_to_df(data_type: &DataType) -> Result<ScalarValue, DataFusionError> {
    if let DataType::Primitive(p) = data_type {
        return Ok(match p {
            PrimitiveType::Integer => ScalarValue::Int32(None),
            PrimitiveType::Long => ScalarValue::Int64(None),
            PrimitiveType::Short => ScalarValue::Int16(None),
            PrimitiveType::Byte => ScalarValue::Int8(None),
            PrimitiveType::Float => ScalarValue::Float32(None),
            PrimitiveType::Double => ScalarValue::Float64(None),
            PrimitiveType::String => ScalarValue::Utf8(None),
            PrimitiveType::Boolean => ScalarValue::Boolean(None),
            PrimitiveType::Date => ScalarValue::Date32(None),
            PrimitiveType::Timestamp => {
                ScalarValue::TimestampMicrosecond(None, Some(Arc::from("UTC")))
            }
            PrimitiveType::TimestampNtz => ScalarValue::TimestampMicrosecond(None, None),
            PrimitiveType::Void => ScalarValue::Null,
            PrimitiveType::IntervalYearMonth => ScalarValue::Int32(None),
            PrimitiveType::IntervalDayTime => ScalarValue::Int64(None),
            PrimitiveType::Binary => ScalarValue::Binary(None),
            PrimitiveType::Decimal(d) => {
                ScalarValue::Decimal128(None, d.precision(), d.scale() as i8)
            }
        });
    }

    let arrow_dt: ArrowDataType = data_type.try_into_arrow().map_err(|e| {
        unsupported(format!(
            "expr_translator: typed NULL conversion failed for {data_type:?}: {e}"
        ))
    })?;
    ScalarValue::try_from(&arrow_dt).map_err(|e| {
        unsupported(format!(
            "expr_translator: typed NULL for {data_type:?} is not supported by DataFusion: {e}"
        ))
    })
}

/// Rebuild a struct expression with target field names while preserving the parent null
/// bitmap. The naive `named_struct(get_field(col, "a"), ...)` produces all-present rows; we
/// wrap in `CASE WHEN col IS NOT NULL THEN rebuilt ELSE NULL END` to keep parent-NULL rows.
fn rebuild_struct_with_target_names(
    input_expr: Expr,
    source_fields: &ArrowFields,
    target_struct: &StructType,
) -> Result<Expr, DataFusionError> {
    let target_count = target_struct.fields().count();
    if source_fields.len() != target_count {
        return Err(plan_compilation(format!(
            "Identity Transform field count mismatch: source struct has {} fields, target \
             projection schema has {target_count} fields",
            source_fields.len(),
        )));
    }
    let mut args: Vec<Expr> = Vec::with_capacity(target_count * 2);
    for (source_field, target_field) in source_fields.iter().zip(target_struct.fields()) {
        let child_expr = get_field(input_expr.clone(), source_field.name().to_string());
        let renamed = rebuild_field_for_target(
            child_expr,
            source_field.data_type(),
            target_field.data_type(),
        )?;
        args.push(lit(target_field.name().to_string()));
        args.push(renamed);
    }
    let rebuilt = named_struct(args);
    Ok(Expr::Case(Case::new(
        None,
        vec![(
            Box::new(Expr::IsNotNull(Box::new(input_expr))),
            Box::new(rebuilt),
        )],
        Some(Box::new(Expr::Literal(ScalarValue::Null, None))),
    )))
}

/// Reshape `base_expr` to match `target_dt`'s logical field names without changing values.
///
/// Returns `base_expr` unchanged when outer kinds differ or no rename is needed. Struct and
/// array targets recurse into children; map reshape is unsupported and passes through.
fn rebuild_field_for_target(
    base_expr: Expr,
    source_dt: &ArrowDataType,
    target_dt: &DataType,
) -> Result<Expr, DataFusionError> {
    match (source_dt, target_dt) {
        (ArrowDataType::Struct(src_fields), DataType::Struct(tgt_struct)) => {
            rebuild_struct_with_target_names(base_expr, src_fields, tgt_struct.as_ref())
        }
        (
            ArrowDataType::List(src_elem)
            | ArrowDataType::LargeList(src_elem)
            | ArrowDataType::FixedSizeList(src_elem, _),
            DataType::Array(tgt_array),
        ) => rebuild_list_with_target_element(base_expr, src_elem, &tgt_array.element_type),
        _ => Ok(base_expr),
    }
}

/// Reshape every element of `list_expr` from `src_elem`'s physical names to
/// `tgt_elem_dt`'s logical names using `array_transform(list, x -> reshape(x))`.
///
/// Short-circuits to a bare `list_expr` when the element doesn't actually need renaming
/// (primitive elements, or nested types whose names already match) so we don't pay for an
/// extra higher-order-function eval on the hot path.
fn rebuild_list_with_target_element(
    list_expr: Expr,
    src_elem: &Arc<ArrowField>,
    tgt_elem_dt: &DataType,
) -> Result<Expr, DataFusionError> {
    if !field_needs_rename(src_elem.data_type(), tgt_elem_dt) {
        return Ok(list_expr);
    }
    // DataFusion's logical expression API does not expose the lambda needed to reshape every
    // nested list element. Struct and primitive list elements that need no rename return above.
    Err(DataFusionError::NotImplemented(
        "nested-list column-mapping reshape is unsupported".to_string(),
    ))
}

/// True when reshaping `source_dt` to `target_dt` would change a struct field name
/// somewhere inside the type tree. Walks the same shape as [`rebuild_field_for_target`]
/// so the answer matches what that helper would actually emit.
fn field_needs_rename(source_dt: &ArrowDataType, target_dt: &DataType) -> bool {
    match (source_dt, target_dt) {
        (ArrowDataType::Struct(src), DataType::Struct(tgt)) => {
            if src.len() != tgt.fields().count() {
                // Mismatch: surface at recurse-time so the caller's error message wins.
                return true;
            }
            src.iter().zip(tgt.fields()).any(|(s, t)| {
                s.name() != t.name() || field_needs_rename(s.data_type(), t.data_type())
            })
        }
        (
            ArrowDataType::List(src)
            | ArrowDataType::LargeList(src)
            | ArrowDataType::FixedSizeList(src, _),
            DataType::Array(tgt),
        ) => field_needs_rename(src.data_type(), &tgt.element_type),
        _ => false,
    }
}

/// Build a per-top-level-field projection that maps the underlying provider's raw arrow
/// schema (physical column-id names on column-mapping tables) into the kernel's logical
/// schema. Each output expression is rooted at `col(source.name)` and is reshaped
/// recursively via `rebuild_field_for_target` when the field has nested structs or
/// lists of structs whose names differ between source and target.
///
/// Returns one [`Expr`] per top-level kernel field, in order. Callers wrap each result
/// in an alias (and, if they care about Arrow field metadata, the stamp UDF) before
/// handing it to [`DataFrame::select`](datafusion::dataframe::DataFrame::select).
pub fn build_logical_projection(
    source: &ArrowSchema,
    target: &StructType,
) -> Result<Vec<Expr>, DataFusionError> {
    let source_count = source.fields().len();
    let target_count = target.fields().count();
    if source_count != target_count {
        return Err(plan_compilation(format!(
            "build_logical_projection: source schema has {source_count} top-level field(s); \
             target schema has {target_count}"
        )));
    }
    source
        .fields()
        .iter()
        .zip(target.fields())
        .map(|(source_field, target_field)| {
            let base = Expr::Column(Column::new_unqualified(source_field.name()));
            rebuild_field_for_target(base, source_field.data_type(), target_field.data_type())
        })
        .collect()
}

fn binary(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::expressions::{
        column_expr, null_lit, ArrayData, BinaryPredicateOp, Expression as Expr_,
        Predicate as Pred, Scalar,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField, StructType};
    use rstest::rstest;

    use super::{kernel_expr_to_df_untyped, kernel_pred_to_df};

    fn lower_expr(e: Expr_) -> String {
        format!("{}", kernel_expr_to_df_untyped(&e).unwrap())
    }
    fn lower_pred(p: Pred) -> String {
        format!("{}", kernel_pred_to_df(&p).unwrap())
    }

    #[rstest]
    #[case::depth_2(Expr_::column(["add", "path"]), "get_field(add, Utf8(\"path\"))")]
    #[case::depth_3(
        Expr_::column(["a", "b", "c"]),
        "get_field(get_field(a, Utf8(\"b\")), Utf8(\"c\"))"
    )]
    fn nested_column_lowers_to_get_field_chain(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[rstest]
    #[case::i32(Expr_::literal(7i32), "Int32(7)")]
    #[case::i64(Expr_::literal(42i64), "Int64(42)")]
    #[case::string(Expr_::literal("abc"), "Utf8(\"abc\")")]
    #[case::bool(Expr_::literal(true), "Boolean(true)")]
    #[case::null_long(null_lit(DataType::LONG), "Int64(NULL)")]
    fn translates_primitive_literals(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[rstest]
    #[case::eq(column_expr!("x").eq(Expr_::literal(1i64)), "x = Int64(1)")]
    #[case::lt(column_expr!("x").lt(Expr_::literal(1i64)), "x < Int64(1)")]
    #[case::gt(column_expr!("x").gt(Expr_::literal(1i64)), "x > Int64(1)")]
    #[case::distinct(
        column_expr!("x").distinct(Expr_::literal(1i64)),
        "x IS DISTINCT FROM Int64(1)"
    )]
    fn translates_binary_predicates(#[case] kernel: Pred, #[case] expected: &str) {
        assert_eq!(lower_pred(kernel), expected);
    }

    #[rstest]
    #[case::not(Pred::not(column_expr!("x").is_null()), "NOT x IS NULL")]
    #[case::and_and(
        Pred::and(
            column_expr!("a").is_null(),
            column_expr!("b").gt(Expr_::literal(5i64)),
        ),
        "a IS NULL AND b > Int64(5)"
    )]
    #[case::or_chain(
        Pred::or_from([
            column_expr!("a").is_null(),
            column_expr!("b").is_null(),
            column_expr!("c").is_null(),
        ]),
        "a IS NULL OR b IS NULL OR c IS NULL"
    )]
    #[case::in_list({
        let arr = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)],
        )
        .unwrap();
        Pred::binary(
            BinaryPredicateOp::In,
            column_expr!("x"),
            Expr_::literal(Scalar::Array(arr)),
        )
    }, "x IN ([Int64(1), Int64(2), Int64(3)])")]
    #[case::arith_then_gt_and_not_isnull(
        Pred::and(
            (column_expr!("a") + Expr_::literal(1i64)).gt(Expr_::literal(5i64)),
            Pred::not(column_expr!("b").is_null()),
        ),
        "a + Int64(1) > Int64(5) AND NOT b IS NULL"
    )]
    fn translates_compound_predicates(#[case] pred: Pred, #[case] expected: &str) {
        assert_eq!(lower_pred(pred), expected);
    }

    #[rstest]
    #[case::arith_add(column_expr!("a") + Expr_::literal(5i64), "a + Int64(5)")]
    #[case::coalesce(
        Expr_::coalesce([column_expr!("a"), Expr_::literal(5i64)]),
        "coalesce(a, Int64(5))"
    )]
    fn translates_expression_shapes(#[case] kernel: Expr_, #[case] expected: &str) {
        assert_eq!(lower_expr(kernel), expected);
    }

    #[test]
    fn parse_json_translates_to_json_get_and_named_struct() {
        let output_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
        );
        let lowered = lower_expr(Expr_::parse_json(column_expr!("stats"), output_schema));
        assert!(lowered.contains("named_struct"), "{lowered}");
        assert!(lowered.contains("json_get_int"), "{lowered}");
    }

    #[test]
    fn opaque_predicate_returns_unsupported() {
        let err = kernel_pred_to_df(&Pred::unknown("mystery")).unwrap_err();
        assert!(format!("{err}").contains("Unknown"));
    }
}
