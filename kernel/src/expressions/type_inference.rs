//! Type inference for kernel expressions.
//!
//! Provides type synthesis (bottom-up inference) to determine the output type of an expression
//! given an input schema.

use crate::expressions::{
    BinaryExpression, BinaryExpressionOp, ColumnName, Expression, ExpressionRef, UnaryExpression,
    UnaryExpressionOp, VariadicExpression, VariadicExpressionOp,
};
use crate::schema::{ArrayType, DataType, StructField, StructType};

// ================================================================================================
// Shared utilities
// ================================================================================================

/// Checks if `from` type can be widened/coerced to `to` type.
pub fn can_coerce(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Primitive(f), DataType::Primitive(t)) => f == t || f.can_widen_to(t),
        _ => from == to,
    }
}

/// Finds the widest common type between two types, if one exists.
pub fn find_common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        Some(left.clone())
    } else if can_coerce(left, right) {
        Some(right.clone())
    } else if can_coerce(right, left) {
        Some(left.clone())
    } else {
        None
    }
}

// ================================================================================================
// Type Synthesis (bottom-up inference)
// ================================================================================================

/// Synthesizes (infers) the output type of an expression given an input schema.
///
/// Returns `Some(DataType)` if the type can be determined, `None` if the expression
/// type cannot be inferred (e.g., unknown expressions, struct patches without context).
pub fn synthesize_type(expr: &Expression, schema: &StructType) -> Option<DataType> {
    match expr {
        Expression::Literal(scalar) => Some(scalar.data_type()),

        Expression::Column(name) => synthesize_column_type(name, schema),

        Expression::Predicate(_) => Some(DataType::BOOLEAN),

        Expression::Struct(fields, _) => synthesize_struct_type(fields, schema),

        Expression::StructPatch(_) => {
            // StructPatch modifies an existing schema - needs separate handling
            // via SchemaPatch::into_struct_type()
            None
        }

        Expression::Unary(UnaryExpression { op, expr }) => synthesize_unary_type(*op, expr, schema),

        Expression::Binary(BinaryExpression { op, left, right }) => {
            synthesize_binary_type(*op, left, right, schema)
        }

        Expression::Variadic(VariadicExpression { op, exprs }) => {
            synthesize_variadic_type(*op, exprs, schema)
        }

        Expression::ParseJson(parse_json) => Some(DataType::Struct(Box::new(
            parse_json.output_schema.as_ref().clone(),
        ))),

        // MapToStruct output schema depends on the map's keys, not available statically
        Expression::MapToStruct(_) => None,

        // Opaque and Unknown expressions cannot have their types inferred
        Expression::Opaque(_) | Expression::Unknown(_) => None,
    }
}

/// Synthesizes the type of a column reference by looking it up in the schema.
fn synthesize_column_type(name: &ColumnName, schema: &StructType) -> Option<DataType> {
    schema
        .walk_column_fields(name)
        .ok()?
        .last()
        .map(|f| f.data_type().clone())
}

/// Synthesizes the type of a struct expression.
///
/// Returns `None` because struct expressions don't carry field names - those come from
/// the expected output schema. Use [`check_type`] with an expected `StructType` instead.
fn synthesize_struct_type(_fields: &[ExpressionRef], _schema: &StructType) -> Option<DataType> {
    None
}

/// Synthesizes the type of a unary expression.
fn synthesize_unary_type(
    op: UnaryExpressionOp,
    _expr: &Expression,
    _schema: &StructType,
) -> Option<DataType> {
    match op {
        UnaryExpressionOp::ToJson => Some(DataType::STRING),
    }
}

/// Synthesizes the type of a binary expression.
fn synthesize_binary_type(
    op: BinaryExpressionOp,
    left: &Expression,
    right: &Expression,
    schema: &StructType,
) -> Option<DataType> {
    let left_type = synthesize_type(left, schema)?;
    let right_type = synthesize_type(right, schema)?;

    match op {
        // Arithmetic operations return the common type of operands
        BinaryExpressionOp::Plus
        | BinaryExpressionOp::Minus
        | BinaryExpressionOp::Multiply
        | BinaryExpressionOp::Divide => find_common_type(&left_type, &right_type),
    }
}

/// Synthesizes the type of a variadic expression.
fn synthesize_variadic_type(
    op: VariadicExpressionOp,
    exprs: &[Expression],
    schema: &StructType,
) -> Option<DataType> {
    // Variadic expressions require at least one element
    let first = exprs.first()?;
    let first_type = synthesize_type(first, schema)?;

    // Find common type across all elements
    let common_type = exprs[1..].iter().try_fold(first_type, |acc, e| {
        let elem_type = synthesize_type(e, schema)?;
        find_common_type(&acc, &elem_type)
    })?;

    match op {
        VariadicExpressionOp::Coalesce => Some(common_type),
        VariadicExpressionOp::Array => {
            Some(DataType::Array(Box::new(ArrayType::new(common_type, true))))
        }
    }
}

// ================================================================================================
// Type Checking (top-down verification)
// ================================================================================================

/// Checks that an expression can produce the expected type.
///
/// Returns `true` if the expression is well-typed for the expected type, `false` otherwise.
/// This is the "check" direction of bidirectional type checking - given an expected type,
/// verify the expression conforms to it.
///
/// This is particularly useful for:
/// - `Expression::Struct`: field expressions can be checked against expected field types
/// - Literals that could have multiple types (e.g., numeric literals)
pub fn check_type(expr: &Expression, expected: &DataType, schema: &StructType) -> bool {
    match expr {
        Expression::Struct(fields, _) => check_struct_type(fields, expected, schema),

        // For most expressions, synthesize and check compatibility
        _ => {
            if let Some(actual) = synthesize_type(expr, schema) {
                can_coerce(&actual, expected)
            } else {
                false
            }
        }
    }
}

/// Checks a struct expression against an expected struct type.
fn check_struct_type(fields: &[ExpressionRef], expected: &DataType, schema: &StructType) -> bool {
    let DataType::Struct(expected_struct) = expected else {
        return false;
    };

    let expected_fields: Vec<&StructField> = expected_struct.fields().collect();

    // Field count must match
    if fields.len() != expected_fields.len() {
        return false;
    }

    // Each field expression must check against its expected type
    fields
        .iter()
        .zip(expected_fields.iter())
        .all(|(expr, expected_field)| check_type(expr, &expected_field.data_type, schema))
}

// ================================================================================================
// Extension methods
// ================================================================================================

impl Expression {
    /// Synthesizes the output type of this expression given an input schema.
    ///
    /// Returns `Some(DataType)` if the type can be determined, `None` otherwise.
    pub fn synthesize_type(&self, schema: &StructType) -> Option<DataType> {
        synthesize_type(self, schema)
    }

    /// Checks that this expression can produce the expected type.
    ///
    /// Returns `true` if the expression is well-typed for the expected type.
    pub fn check_type(&self, expected: &DataType, schema: &StructType) -> bool {
        check_type(self, expected, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Expression;
    use crate::schema::{DataType, StructField, StructType};

    fn test_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("int_col", DataType::INTEGER),
            StructField::nullable("long_col", DataType::LONG),
            StructField::nullable("str_col", DataType::STRING),
            StructField::nullable("bool_col", DataType::BOOLEAN),
            StructField::nullable("double_col", DataType::DOUBLE),
            StructField::nullable(
                "nested",
                DataType::Struct(Box::new(
                    StructType::try_new([StructField::nullable("inner_int", DataType::INTEGER)])
                        .unwrap(),
                )),
            ),
        ])
        .unwrap()
    }

    #[test]
    fn synthesize_literal_types() {
        let schema = test_schema();

        assert_eq!(
            Expression::literal(42i32).synthesize_type(&schema),
            Some(DataType::INTEGER)
        );
        assert_eq!(
            Expression::literal(42i64).synthesize_type(&schema),
            Some(DataType::LONG)
        );
        assert_eq!(
            Expression::literal("hello").synthesize_type(&schema),
            Some(DataType::STRING)
        );
        assert_eq!(
            Expression::literal(true).synthesize_type(&schema),
            Some(DataType::BOOLEAN)
        );
    }

    #[test]
    fn synthesize_column_types() {
        let schema = test_schema();

        assert_eq!(
            Expression::column(["int_col"]).synthesize_type(&schema),
            Some(DataType::INTEGER)
        );
        assert_eq!(
            Expression::column(["nested", "inner_int"]).synthesize_type(&schema),
            Some(DataType::INTEGER)
        );
        assert_eq!(
            Expression::column(["unknown_col"]).synthesize_type(&schema),
            None
        );
    }

    #[test]
    fn synthesize_unary_types() {
        let schema = test_schema();

        assert_eq!(
            Expression::unary(UnaryExpressionOp::ToJson, Expression::column(["int_col"]))
                .synthesize_type(&schema),
            Some(DataType::STRING)
        );
    }

    #[test]
    fn synthesize_binary_arithmetic_widens() {
        let schema = test_schema();

        // INTEGER + LONG -> LONG
        assert_eq!(
            Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::column(["int_col"]),
                Expression::column(["long_col"])
            )
            .synthesize_type(&schema),
            Some(DataType::LONG)
        );
    }

    #[test]
    fn synthesize_variadic_array() {
        let schema = test_schema();

        assert_eq!(
            Expression::variadic(
                VariadicExpressionOp::Array,
                [Expression::literal(1i32), Expression::literal(2i32),]
            )
            .synthesize_type(&schema),
            Some(DataType::Array(Box::new(ArrayType::new(
                DataType::INTEGER,
                true
            ))))
        );
    }

    #[test]
    fn can_coerce_numeric_widening() {
        assert!(can_coerce(&DataType::INTEGER, &DataType::LONG));
        assert!(can_coerce(&DataType::FLOAT, &DataType::DOUBLE));
        assert!(!can_coerce(&DataType::LONG, &DataType::INTEGER));
        assert!(!can_coerce(&DataType::STRING, &DataType::INTEGER));
    }

    #[test]
    fn find_common_type_picks_wider() {
        assert_eq!(
            find_common_type(&DataType::INTEGER, &DataType::LONG),
            Some(DataType::LONG)
        );
        assert_eq!(
            find_common_type(&DataType::LONG, &DataType::INTEGER),
            Some(DataType::LONG)
        );
        assert_eq!(
            find_common_type(&DataType::STRING, &DataType::INTEGER),
            None
        );
    }

    // ==================== Check type tests ====================

    #[test]
    fn check_struct_expression_against_expected_type() {
        let schema = test_schema();

        // Expected output struct type
        let expected = DataType::Struct(Box::new(
            StructType::try_new([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ])
            .unwrap(),
        ));

        // Struct expression with matching field types
        let expr =
            Expression::struct_from([Expression::literal(42i32), Expression::literal("hello")]);

        assert!(expr.check_type(&expected, &schema));
    }

    #[test]
    fn check_struct_expression_with_coercion() {
        let schema = test_schema();

        // Expected LONG field
        let expected = DataType::Struct(Box::new(
            StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap(),
        ));

        // Provide INTEGER literal - should coerce to LONG
        let expr = Expression::struct_from([Expression::literal(42i32)]);

        assert!(expr.check_type(&expected, &schema));
    }

    #[test]
    fn check_struct_expression_wrong_field_count() {
        let schema = test_schema();

        let expected = DataType::Struct(Box::new(
            StructType::try_new([
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ])
            .unwrap(),
        ));

        // Only one field when two expected
        let expr = Expression::struct_from([Expression::literal(42i32)]);

        assert!(!expr.check_type(&expected, &schema));
    }

    #[test]
    fn check_struct_expression_wrong_field_type() {
        let schema = test_schema();

        let expected = DataType::Struct(Box::new(
            StructType::try_new([StructField::nullable("a", DataType::INTEGER)]).unwrap(),
        ));

        // String literal cannot coerce to INTEGER
        let expr = Expression::struct_from([Expression::literal("not an int")]);

        assert!(!expr.check_type(&expected, &schema));
    }

    #[test]
    fn check_literal_coerces_to_wider_type() {
        let schema = test_schema();

        // INTEGER literal can check against LONG
        assert!(Expression::literal(42i32).check_type(&DataType::LONG, &schema));

        // LONG literal cannot check against INTEGER (narrowing)
        assert!(!Expression::literal(42i64).check_type(&DataType::INTEGER, &schema));
    }
}
