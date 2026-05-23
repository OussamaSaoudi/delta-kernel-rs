//! Bidirectional expression type checker for builder schema derivation.
//!
//! Derives a [`DataType`] for each named expression in a `Project` from the input row
//! schema so callers can build output schemas without spelling out every column type.
//! Operates in two modes:
//!
//! * **Synthesis** (`expected = None`): infer the output type from the expression alone. Supported
//!   variants: `Literal`, `Column`, `Predicate`, `Variadic::Array`, `Variadic::Coalesce`, `If`,
//!   `Unary::ToJson`, `ParseJson`.
//!
//! * **Checking** (`expected = Some(t)`): verify the expression's output type is `t`. In addition
//!   to all synthesis variants (which are inferred and compared to `t`), checking-mode admits
//!   variants that synthesis alone cannot handle:
//!   - `MapToStruct(m)`: confirms `m` is `Map<String, String>`. The expected struct type supplies
//!     the output schema (`MapToStruct` carries no schema of its own).
//!   - `Struct(fields)`: recurses to check each `fields[i]` against the matching field of the
//!     expected struct, enforcing arity and per-field type agreement.
//!   - `Transform`, `Opaque`, `Unknown`: fully opaque -- validates column references against the
//!     input schema and accepts `t` as the output type (no type rule available).
//!
//! Does not validate expression well-formedness beyond column references and type
//! agreement; the kernel evaluator enforces semantics at runtime.

use std::borrow::Cow;

use itertools::Itertools;

use crate::delta_error;
use crate::expressions::{
    ColumnName, Expression, ExpressionRef, UnaryExpressionOp, VariadicExpressionOp,
};
use crate::plans::errors::{DeltaError, DeltaErrorCode, DeltaResultExt};
use crate::schema::{ArrayType, DataType, StructType};
use crate::transforms::ExpressionTransform;

/// Bidirectional type check for `expr` against `input_schema`.
///
/// See module docs for synthesis (`expected = None`) vs. checking (`expected = Some(t)`)
/// semantics. Returns the verified output [`DataType`] on success; errors on column-ref
/// misses, type mismatches, or variants that require an expected type but were given
/// `None`.
pub(crate) fn check_expression(
    expr: &Expression,
    input_schema: &StructType,
    expected: Option<&DataType>,
) -> Result<DataType, DeltaError> {
    if let Some(expected_ty) = expected {
        match expr {
            Expression::MapToStruct(m) => {
                return check_map_to_struct(&m.map_expr, input_schema, expected_ty)
            }
            Expression::Struct(fields, _) => {
                return check_struct(fields, input_schema, expected_ty)
            }
            Expression::Transform(_) | Expression::Opaque(_) | Expression::Unknown(_) => {
                check_column_refs(expr, input_schema)?;
                return Ok(expected_ty.clone());
            }
            _ => {}
        }
    }
    let inferred = synthesize(expr, input_schema)?;
    match expected {
        Some(want) if &inferred != want => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "expression type mismatch: inferred {inferred:?}, expected {want:?}",
        )),
        _ => Ok(inferred),
    }
}

/// Bidirectional rule for `MapToStruct`: requires `expected` to be a struct and the
/// input map expression to produce `Map<String, String>`.
fn check_map_to_struct(
    map_expr: &Expression,
    input_schema: &StructType,
    expected: &DataType,
) -> Result<DataType, DeltaError> {
    if !matches!(expected, DataType::Struct(_)) {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "map_to_struct: expected output type must be Struct, got {expected:?}",
        ));
    }
    let input_ty = synthesize(map_expr, input_schema)?;
    match &input_ty {
        DataType::Map(m) if m.key_type == DataType::STRING && m.value_type == DataType::STRING => {
            Ok(expected.clone())
        }
        _ => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "map_to_struct: input must produce Map<String, String>, got {input_ty:?}",
        )),
    }
}

/// Bidirectional rule for `Struct`: requires `expected` to be a struct with arity
/// matching `fields`. Recurses to check each field expression against the matching
/// expected field type.
fn check_struct(
    fields: &[ExpressionRef],
    input_schema: &StructType,
    expected: &DataType,
) -> Result<DataType, DeltaError> {
    let DataType::Struct(target) = expected else {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "struct: expected output type must be Struct, got {expected:?}",
        ));
    };
    let target_fields: Vec<_> = target.fields().collect();
    if fields.len() != target_fields.len() {
        return Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "struct: arity mismatch -- {got} expressions, {want} expected fields",
            got = fields.len(),
            want = target_fields.len(),
        ));
    }
    for (expr, field) in fields.iter().zip(target_fields) {
        check_expression(expr.as_ref(), input_schema, Some(field.data_type()))?;
    }
    Ok(expected.clone())
}

/// Walk `expr` via [`ExpressionTransform`] and verify every column reference resolves
/// in `schema`. Pure validation -- no type rule, no rewrite.
///
/// Used as the opaque-variant fallback in [`check_expression`] when no type rule is
/// available, and directly by callers (e.g. `project_with_schema`) that intentionally
/// rename fields and therefore cannot rely on strict type equality but still want to
/// confirm referenced columns exist.
pub(crate) fn check_column_refs(expr: &Expression, schema: &StructType) -> Result<(), DeltaError> {
    struct Validator<'a> {
        schema: &'a StructType,
        err: Option<DeltaError>,
    }
    impl<'a> ExpressionTransform<'a> for Validator<'a> {
        fn transform_expr_column(&mut self, name: &'a ColumnName) -> Option<Cow<'a, ColumnName>> {
            if self.err.is_none() {
                if let Err(e) = self.schema.walk_column_fields(name) {
                    self.err = Some(delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        source = e,
                        "unresolved column reference {name}",
                    ));
                }
            }
            Some(Cow::Borrowed(name))
        }
    }
    let mut v = Validator { schema, err: None };
    let _ = v.transform_expr(expr);
    v.err.map_or(Ok(()), Err)
}

/// Synthesis path: infer `expr`'s output [`DataType`] from the expression alone.
/// Errors on bidirectional-only variants (`Struct`, `Transform`, `Binary`,
/// `MapToStruct`, `Opaque`, `Unknown`) -- callers should supply an expected type and
/// route through [`check_expression`].
fn synthesize(expr: &Expression, input_schema: &StructType) -> Result<DataType, DeltaError> {
    match expr {
        Expression::Literal(scalar) => Ok(scalar.data_type()),

        Expression::Column(col) => {
            let fields = input_schema
                .walk_column_fields(col)
                .or_delta(DeltaErrorCode::DeltaCommandInvariantViolation)?;
            // walk_column_fields guarantees a non-empty result on success.
            let leaf = fields.last().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "walk_column_fields returned empty path for non-empty column",
                )
            })?;
            Ok(leaf.data_type().clone())
        }

        Expression::Predicate(_) => Ok(DataType::BOOLEAN),

        Expression::Unary(u) => match u.op {
            UnaryExpressionOp::ToJson => Ok(DataType::STRING),
        },

        Expression::Variadic(v) => match v.op {
            VariadicExpressionOp::Coalesce => unify_arms(
                v.exprs.iter().map(|e| synthesize(e, input_schema)),
                "coalesce",
            ),
            VariadicExpressionOp::Array => {
                let elem =
                    unify_arms(v.exprs.iter().map(|e| synthesize(e, input_schema)), "array")?;
                Ok(ArrayType::new(elem, true).into())
            }
        },

        Expression::If(if_expr) => unify_arms(
            [
                synthesize(&if_expr.then_expr, input_schema),
                synthesize(&if_expr.else_expr, input_schema),
            ]
            .into_iter(),
            "if",
        ),

        Expression::ParseJson(p) => Ok((*p.output_schema).clone().into()),

        // Bidirectional-only -- caller must supply an expected type.
        Expression::Struct(..)
        | Expression::Transform(_)
        | Expression::Binary(_)
        | Expression::MapToStruct(_)
        | Expression::Opaque(_)
        | Expression::Unknown(_) => Err(delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            "expression variant cannot be synthesized without an expected type; supply \
             an output schema and route through check_expression(.., Some(expected))",
        )),
    }
}

/// Unify the inferred types of N expression arms into a single output type.
///
/// All arms must share the same [`DataType`] (no implicit promotion). Empty input
/// is rejected -- the operator guarantees at least one arm at construction.
fn unify_arms(
    types: impl Iterator<Item = Result<DataType, DeltaError>>,
    op: &str,
) -> Result<DataType, DeltaError> {
    itertools::process_results(types, |mut it| it.all_equal_value())?.map_err(|disagree| {
        match disagree {
            None => delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "{op}: cannot infer type with zero arms",
            ),
            Some((first, ty)) => delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "{op}: arm types disagree -- got {first:?} and {ty:?}",
            ),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{col, Expression, Predicate};
    use crate::schema::{MapType, StructField, StructType};

    fn input_schema() -> StructType {
        StructType::try_new(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("ts", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
            StructField::nullable(
                "tags",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
            ),
        ])
        .unwrap()
    }

    /// `Struct<a: LONG, b: LONG>` used as an expected type for `Struct` / `MapToStruct`
    /// checks.
    fn long_long_struct() -> DataType {
        DataType::Struct(Box::new(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::LONG),
                StructField::nullable("b", DataType::LONG),
            ])
            .unwrap(),
        ))
    }

    // === Synthesis-mode happy paths ===
    #[rstest]
    #[case::literal_long(Expression::literal(42i64), DataType::LONG)]
    #[case::literal_null_string(Expression::null_literal(DataType::STRING), DataType::STRING)]
    #[case::column_string(col("id"), DataType::STRING)]
    #[case::column_long(col("ts"), DataType::LONG)]
    #[case::predicate_boolean(Expression::from_pred(Predicate::column(["flag"])), DataType::BOOLEAN)]
    #[case::to_json_string(col("id").to_json(), DataType::STRING)]
    #[case::coalesce_match(
        Expression::coalesce([col("id"), Expression::literal("default")]),
        DataType::STRING
    )]
    #[case::if_unify_then_else(
        Expression::if_then_else(Predicate::column(["flag"]), col("ts"), Expression::literal(0i64)),
        DataType::LONG
    )]
    fn synthesis_happy_path(#[case] expr: Expression, #[case] expected: DataType) {
        let s = input_schema();
        assert_eq!(check_expression(&expr, &s, None).unwrap(), expected);
    }

    // === Synthesis-mode errors ===
    #[rstest]
    #[case::unknown_column(col("missing"), "missing")]
    #[case::coalesce_disagree(Expression::coalesce([col("id"), col("ts")]), "disagree")]
    #[case::hard_variant(Expression::unknown("opaque"), "expected type")]
    fn synthesis_errors(#[case] expr: Expression, #[case] needle: &str) {
        let s = input_schema();
        let err = check_expression(&expr, &s, None).unwrap_err();
        assert!(
            err.to_string().contains(needle),
            "expected error containing {needle:?}, got: {err}",
        );
    }

    #[test]
    fn array_wraps_unified_element_type() {
        let s = input_schema();
        let expr = Expression::array([col("id"), Expression::literal("x")]);
        let DataType::Array(arr) = check_expression(&expr, &s, None).unwrap() else {
            panic!("expected Array");
        };
        assert_eq!(arr.element_type, DataType::STRING);
        assert!(arr.contains_null);
    }

    // === Checking-mode: easy variants ===
    #[test]
    fn check_mode_accepts_matching_inferred_type() {
        let s = input_schema();
        assert_eq!(
            check_expression(&col("ts"), &s, Some(&DataType::LONG)).unwrap(),
            DataType::LONG,
        );
    }

    #[test]
    fn check_mode_rejects_type_mismatch() {
        let s = input_schema();
        let err = check_expression(&col("ts"), &s, Some(&DataType::STRING)).unwrap_err();
        assert!(err.to_string().contains("expression type mismatch"));
    }

    // === Bidirectional MapToStruct ===
    #[test]
    fn map_to_struct_with_string_map_input_accepts_expected_struct() {
        let s = input_schema();
        let expr = Expression::map_to_struct(col("tags"));
        let expected = long_long_struct();
        assert_eq!(
            check_expression(&expr, &s, Some(&expected)).unwrap(),
            expected,
        );
    }

    #[test]
    fn map_to_struct_rejects_non_map_input() {
        let s = input_schema();
        let expr = Expression::map_to_struct(col("id"));
        let err = check_expression(&expr, &s, Some(&long_long_struct())).unwrap_err();
        assert!(err.to_string().contains("Map<String, String>"));
    }

    #[test]
    fn map_to_struct_rejects_non_struct_expected() {
        let s = input_schema();
        let expr = Expression::map_to_struct(col("tags"));
        let err = check_expression(&expr, &s, Some(&DataType::LONG)).unwrap_err();
        assert!(err.to_string().contains("must be Struct"));
    }

    // === Bidirectional Struct ===
    #[test]
    fn struct_arity_mismatch_rejected() {
        let s = input_schema();
        let expr = Expression::struct_from([Arc::new(col("ts"))]);
        let err = check_expression(&expr, &s, Some(&long_long_struct())).unwrap_err();
        assert!(err.to_string().contains("arity mismatch"));
    }

    #[test]
    fn struct_per_field_check_succeeds() {
        let s = input_schema();
        let expr = Expression::struct_from([Arc::new(col("ts")), Arc::new(col("ts"))]);
        let expected = long_long_struct();
        assert_eq!(
            check_expression(&expr, &s, Some(&expected)).unwrap(),
            expected,
        );
    }

    #[test]
    fn struct_per_field_type_mismatch_rejected() {
        let s = input_schema();
        let expr = Expression::struct_from([Arc::new(col("ts")), Arc::new(col("id"))]);
        let err = check_expression(&expr, &s, Some(&long_long_struct())).unwrap_err();
        assert!(err.to_string().contains("expression type mismatch"));
    }

    // === Opaque/Unknown column-ref fallback ===
    #[test]
    fn unknown_with_expected_accepts_any_type() {
        let s = input_schema();
        let expr = Expression::unknown("opaque");
        assert_eq!(
            check_expression(&expr, &s, Some(&DataType::LONG)).unwrap(),
            DataType::LONG,
        );
    }
}
