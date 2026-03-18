//! Parses SQL WHERE clause expressions into kernel [`Predicate`] types.
//!
//! Supports a subset of SQL sufficient for benchmark predicates:
//! - Binary comparisons: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
//! - Null-safe equals: `<=>`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - `IS NULL`, `IS NOT NULL`
//! - `IN (...)`, `NOT IN (...)`
//! - `BETWEEN ... AND ...`
//! - Column references and literal values (integers, floats, strings, booleans)
//!
//! Unsupported (returns error): `LIKE`, function calls (`HEX`, `size`, `length`),
//! arithmetic expressions (`a % 100`), typed literals (`TIME '...'`).

use delta_kernel::expressions::{ArrayData, ColumnName, Expression, Predicate, Scalar};
use delta_kernel::schema::{ArrayType, DataType, PrimitiveType, Schema};

use sqlparser::ast::{self, Expr, UnaryOperator, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Context for type checking that provides access to the table schema.
struct TypeContext<'a> {
    schema: &'a Schema,
}

/// Parses a SQL WHERE clause expression string into a kernel [`Predicate`] with type checking.
///
/// Performs bidirectional type checking using the provided schema:
/// - Column types are looked up in the schema
/// - Literals are checked against expected types from columns
/// - Type mismatches produce detailed error messages
///
/// # Example
/// ```ignore
/// let schema = Schema::try_new(vec![
///     StructField::new("id", DataType::LONG, false),
///     StructField::new("name", DataType::STRING, false),
/// ])?;
/// let pred = parse_predicate("id < 500 AND name = 'alice'", &schema).unwrap();
/// ```
pub fn parse_predicate(
    sql: &str,
    schema: &Schema,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    let dialect = GenericDialect {};
    let expr = Parser::new(&dialect)
        .try_with_sql(sql)?
        .parse_expr()
        .map_err(|e| format!("Failed to parse predicate: {e}"))?;
    let ctx = TypeContext { schema };
    convert_expr_to_predicate(&ctx, &expr)
}

////////////////////////////////////////////////////////////////////////
// Typed (bidirectional) conversion functions
////////////////////////////////////////////////////////////////////////

/// Navigates through nested struct fields to find the type of a compound identifier.
///
/// For example, given schema with a struct field "user" containing "age",
/// this can look up ["user", "age"] to get the type of "user.age".
fn find_nested_field_type(schema: &Schema, field_path: &[&str]) -> Option<DataType> {
    if field_path.is_empty() {
        return None;
    }

    // Start with the root schema
    let mut current_type: &DataType = schema.field(field_path[0])?.data_type();

    // Navigate through remaining path parts
    for (i, &field_name) in field_path.iter().enumerate().skip(1) {
        // Extract the struct type from current_type
        let current_struct = match current_type {
            DataType::Struct(s) => s.as_ref(),
            _ => return None, // Can't navigate into non-struct
        };

        // Find the field in this struct
        let field = current_struct.fields().find(|f| f.name() == field_name)?;

        // If this is the last field in the path, return its type
        if i == field_path.len() - 1 {
            return Some(field.data_type().clone());
        }

        // Otherwise, continue navigating
        current_type = field.data_type();
    }

    // If we only had one field in the path, return its type
    if field_path.len() == 1 {
        return Some(current_type.clone());
    }

    None
}

/// Tries to synthesize a type for an expression.
///
/// Returns `Some(DataType)` if the expression has a definitive type without context:
/// - Column references: look up type in schema (supports nested structs)
/// - String literals: always String
/// - Boolean literals: always Boolean
///
/// Returns `None` if the expression needs context to determine its type:
/// - Numeric literals: ambiguous (could be Long, Integer, Short, Byte, Float, Double)
/// - NULL: could be any type
fn try_synthesize_expr(ctx: &TypeContext, expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Identifier(ident) => {
            // Look up column in schema (simple field name)
            ctx.schema
                .field(ident.value.as_str())
                .map(|f| f.data_type().clone())
        }
        Expr::CompoundIdentifier(parts) => {
            // Navigate through nested structs for compound identifiers like "user.age"
            let field_names: Vec<&str> = parts.iter().map(|p| p.value.as_str()).collect();
            find_nested_field_type(ctx.schema, &field_names)
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            // CompoundFieldAccess is used for:
            // 1. Dot-style field access: user.age (sqlparser quirk - also uses this instead of CompoundIdentifier)
            // 2. Subscript operations: a[1], a['field'] (not supported in kernel predicates)
            // 3. Mixed: a[1].field (not supported in kernel predicates)
            //
            // We only support pure dot-style access (case 1)

            // Check if this is pure dot-style access (no subscripts)
            let has_subscripts = access_chain
                .iter()
                .any(|access| !matches!(access, ast::AccessExpr::Dot(_)));
            if has_subscripts {
                return None; // Subscripts not supported
            }

            // Extract field path from root and access chain
            let mut parts: Vec<&str> = Vec::new();
            if let Expr::Function(func) = root.as_ref() {
                // sqlparser parses "user.age" with root as a Function node
                for part in &func.name.0 {
                    if let Some(ident) = part.as_ident() {
                        parts.push(&ident.value);
                    }
                }
            } else if let Expr::Identifier(ident) = root.as_ref() {
                parts.push(&ident.value);
            }

            // Extract remaining parts from access chain
            for access in access_chain {
                if let ast::AccessExpr::Dot(Expr::Identifier(ident)) = access {
                    parts.push(&ident.value);
                }
            }

            find_nested_field_type(ctx.schema, &parts)
        }
        Expr::Value(value_with_span) => match &value_with_span.value {
            // String literals are polymorphic - they can be coerced to dates, timestamps, etc.
            // So don't synthesize a type - let them be checked bidirectionally
            Value::SingleQuotedString(_) | Value::DoubleQuotedString(_) => None,
            // Boolean literals synthesize to BOOLEAN
            Value::Boolean(_) => Some(DataType::BOOLEAN),
            // Numeric literals are ambiguous - could be any numeric type
            Value::Number(_, _) => None,
            // NULL is ambiguous - could be any type
            Value::Null => None,
            _ => None,
        },
        Expr::Nested(inner) => try_synthesize_expr(ctx, inner),
        Expr::UnaryOp { op, expr } => {
            if matches!(op, ast::UnaryOperator::Minus | ast::UnaryOperator::Plus) {
                // Unary +/- preserves the type of the inner expression
                try_synthesize_expr(ctx, expr)
            } else {
                None
            }
        }
        Expr::BinaryOp { left, op, right } => {
            use ast::BinaryOperator::*;
            match op {
                // Arithmetic operators - return wider of the two numeric types
                Plus | Minus | Multiply | Divide | Modulo => {
                    let left_ty = try_synthesize_expr(ctx, left)?;
                    let right_ty = try_synthesize_expr(ctx, right)?;
                    synthesize_arithmetic_result_type(&left_ty, &right_ty)
                }
                // Comparison and logical operators return boolean, but they're handled as predicates
                _ => None,
            }
        }
        _ => None,
    }
}

/// Get the numeric rank of a primitive type for widening purposes.
/// Higher rank = wider type. Returns None for non-numeric types.
fn numeric_rank(ty: &PrimitiveType) -> Option<u8> {
    use PrimitiveType::*;
    match ty {
        Byte => Some(1),
        Short => Some(2),
        Integer => Some(3),
        Long => Some(4),
        Float => Some(5),
        Double => Some(6),
        _ => None,
    }
}

/// Convert a numeric rank back to a DataType.
fn rank_to_type(rank: u8) -> DataType {
    match rank {
        1 => DataType::BYTE,
        2 => DataType::SHORT,
        3 => DataType::INTEGER,
        4 => DataType::LONG,
        5 => DataType::FLOAT,
        6 => DataType::DOUBLE,
        _ => unreachable!("Invalid numeric rank"),
    }
}

/// Synthesize the result type of an arithmetic operation.
/// Returns the wider of the two numeric types following Delta's type widening rules:
/// - Integer widening: byte → short → int → long
/// - Float widening: float → double
/// - Mixed integer/float: result is the float type
fn synthesize_arithmetic_result_type(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::Primitive(l), DataType::Primitive(r)) => {
            let left_rank = numeric_rank(l)?;
            let right_rank = numeric_rank(r)?;
            Some(rank_to_type(left_rank.max(right_rank)))
        }
        _ => None,
    }
}

/// Checks that an expression can be coerced to the expected type.
///
/// For literals, attempts to parse them as the expected type.
/// For other expressions, synthesizes their type and verifies compatibility.
fn check_expr(
    ctx: &TypeContext,
    expected_ty: &DataType,
    expr: &Expr,
) -> Result<Expression, Box<dyn std::error::Error>> {
    match expr {
        Expr::Value(value_with_span) => check_literal(expected_ty, &value_with_span.value, false),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(value_with_span) = inner.as_ref() {
                check_literal(expected_ty, &value_with_span.value, true)
            } else {
                Err(format!("Unsupported unary minus on: {expr}").into())
            }
        }
        Expr::Nested(inner) => check_expr(ctx, expected_ty, inner),
        _ => {
            // For non-literals, try to synthesize the type and verify compatibility
            if let Some(actual_ty) = try_synthesize_expr(ctx, expr) {
                if can_coerce(&actual_ty, expected_ty) {
                    convert_expr_to_expression(ctx, expr)
                } else {
                    Err(format!(
                        "Type mismatch: expected {:?}, got {:?}",
                        expected_ty, actual_ty
                    )
                    .into())
                }
            } else {
                // Cannot synthesize type - this shouldn't happen in well-formed predicates
                Err(format!("Cannot determine type for expression: {expr}").into())
            }
        }
    }
}

/// Parse a date string in YYYY-MM-DD format to days since Unix epoch.
fn parse_date_string(s: &str) -> Result<i32, Box<dyn std::error::Error>> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("Invalid date format: {}", s).into());
    }

    let year: i32 = parts[0]
        .parse()
        .map_err(|_| format!("Invalid year: {}", parts[0]))?;
    let month: u32 = parts[1]
        .parse()
        .map_err(|_| format!("Invalid month: {}", parts[1]))?;
    let day: u32 = parts[2]
        .parse()
        .map_err(|_| format!("Invalid day: {}", parts[2]))?;

    // Simple date to days calculation
    // Days since 1970-01-01
    let days_per_year = 365;
    let days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let mut days = (year - 1970) * days_per_year;

    // Add leap days
    for y in 1970..year {
        if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            days += 1;
        }
    }

    // Add days for months
    for m in 1..month {
        days += days_per_month[(m - 1) as usize];
    }

    // Add leap day for current year if needed
    if month > 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        days += 1;
    }

    // Add days in month
    days += day as i32 - 1;

    Ok(days)
}

/// Parse a time string in HH:MM:SS format to microseconds.
fn parse_time_string(s: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return Err(format!("Invalid time format: {}", s).into());
    }

    let hours: i64 = parts[0]
        .parse()
        .map_err(|_| format!("Invalid hours: {}", parts[0]))?;
    let minutes: i64 = parts[1]
        .parse()
        .map_err(|_| format!("Invalid minutes: {}", parts[1]))?;
    let seconds: i64 = parts[2]
        .parse()
        .map_err(|_| format!("Invalid seconds: {}", parts[2]))?;

    Ok((hours * 3600 + minutes * 60 + seconds) * 1_000_000)
}

/// Parse a timestamp string (YYYY-MM-DD HH:MM:SS or just HH:MM:SS) to microseconds since Unix epoch.
fn parse_timestamp_string(s: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if s.contains(' ') {
        // Full timestamp: YYYY-MM-DD HH:MM:SS
        let parts: Vec<&str> = s.split(' ').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid timestamp format: {}", s).into());
        }

        let days = parse_date_string(parts[0])?;
        let time_micros = parse_time_string(parts[1])?;

        Ok(days as i64 * 24 * 3600 * 1_000_000 + time_micros)
    } else {
        // Just time: HH:MM:SS (assume day 0)
        parse_time_string(s)
    }
}

/// Checks a literal value against an expected type and converts it to that type.
/// Handles both positive and negative numeric literals.
fn check_literal(
    expected_ty: &DataType,
    value: &Value,
    is_negative: bool,
) -> Result<Expression, Box<dyn std::error::Error>> {
    match (expected_ty, value) {
        // Numeric types
        (DataType::Primitive(PrimitiveType::Long), Value::Number(n, _)) => {
            let val = n
                .parse::<i64>()
                .map_err(|_| format!("Cannot parse '{}' as Long (i64)", n))?;
            Ok(Scalar::Long(if is_negative { -val } else { val }).into())
        }
        (DataType::Primitive(PrimitiveType::Integer), Value::Number(n, _)) => {
            let val = n
                .parse::<i32>()
                .map_err(|_| format!("Cannot parse '{}' as Integer (i32)", n))?;
            Ok(Scalar::Integer(if is_negative { -val } else { val }).into())
        }
        (DataType::Primitive(PrimitiveType::Short), Value::Number(n, _)) => {
            let val = n
                .parse::<i16>()
                .map_err(|_| format!("Cannot parse '{}' as Short (i16)", n))?;
            Ok(Scalar::Short(if is_negative { -val } else { val }).into())
        }
        (DataType::Primitive(PrimitiveType::Byte), Value::Number(n, _)) => {
            let val = n
                .parse::<i8>()
                .map_err(|_| format!("Cannot parse '{}' as Byte (i8)", n))?;
            Ok(Scalar::Byte(if is_negative { -val } else { val }).into())
        }
        (DataType::Primitive(PrimitiveType::Float), Value::Number(n, _)) => {
            let val = n
                .parse::<f32>()
                .map_err(|_| format!("Cannot parse '{}' as Float (f32)", n))?;
            Ok(Scalar::Float(if is_negative { -val } else { val }).into())
        }
        (DataType::Primitive(PrimitiveType::Double), Value::Number(n, _)) => {
            let val = n
                .parse::<f64>()
                .map_err(|_| format!("Cannot parse '{}' as Double (f64)", n))?;
            Ok(Scalar::Double(if is_negative { -val } else { val }).into())
        }

        // String type
        (DataType::Primitive(PrimitiveType::String), Value::SingleQuotedString(s))
        | (DataType::Primitive(PrimitiveType::String), Value::DoubleQuotedString(s)) => {
            Ok(Scalar::from(s.clone()).into())
        }

        // Boolean type
        (DataType::Primitive(PrimitiveType::Boolean), Value::Boolean(b)) => {
            Ok(Scalar::Boolean(*b).into())
        }

        // Date/Time types from string literals
        (DataType::Primitive(PrimitiveType::Date), Value::SingleQuotedString(s))
        | (DataType::Primitive(PrimitiveType::Date), Value::DoubleQuotedString(s)) => {
            // Parse date string (YYYY-MM-DD format)
            let days = parse_date_string(s)?;
            Ok(Scalar::Date(days).into())
        }
        (DataType::Primitive(PrimitiveType::Timestamp), Value::SingleQuotedString(s))
        | (DataType::Primitive(PrimitiveType::Timestamp), Value::DoubleQuotedString(s)) => {
            // Parse timestamp string (time portion can be HH:MM:SS format for just times)
            let micros = parse_timestamp_string(s)?;
            Ok(Scalar::Timestamp(micros).into())
        }
        (DataType::Primitive(PrimitiveType::TimestampNtz), Value::SingleQuotedString(s))
        | (DataType::Primitive(PrimitiveType::TimestampNtz), Value::DoubleQuotedString(s)) => {
            let micros = parse_timestamp_string(s)?;
            Ok(Scalar::TimestampNtz(micros).into())
        }

        // NULL can be any type
        (ty, Value::Null) => Ok(Scalar::Null(ty.clone()).into()),

        // Type mismatches
        (expected, actual) => Err(format!(
            "Type mismatch: cannot use {:?} for column of type {:?}",
            actual, expected
        )
        .into()),
    }
}

/// Checks if one type can be coerced to another.
///
/// Supports primitive type widening:
/// - Integer widening: byte → short → int → long
/// - Float widening: float → double
/// - Timestamp equivalence: Timestamp ↔ TimestampNtz
fn can_coerce(from: &DataType, to: &DataType) -> bool {
    use PrimitiveType::*;
    match (from, to) {
        (DataType::Primitive(f), DataType::Primitive(t)) => {
            f == t
                || matches!(
                    (f, t),
                    (Byte, Short | Integer | Long)
                        | (Short, Integer | Long)
                        | (Integer, Long)
                        | (Float, Double)
                        | (Timestamp, TimestampNtz)
                        | (TimestampNtz, Timestamp)
                )
        }
        _ => from == to,
    }
}

/// Converts a sqlparser AST expression into a kernel [`Predicate`] with type checking.
fn convert_expr_to_predicate(
    ctx: &TypeContext,
    expr: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    match expr {
        Expr::BinaryOp { left, op, right } => convert_binary_op(ctx, left, op, right),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let inner = convert_expr_to_predicate(ctx, expr)?;
            Ok(Predicate::not(inner))
        }
        Expr::IsNull(expr) => {
            let inner = convert_expr_to_expression(ctx, expr)?;
            Ok(Predicate::is_null(inner))
        }
        Expr::IsNotNull(expr) => {
            let inner = convert_expr_to_expression(ctx, expr)?;
            Ok(Predicate::is_not_null(inner))
        }
        Expr::Nested(inner) => convert_expr_to_predicate(ctx, inner),
        Expr::Value(value) => match &value.value {
            Value::Boolean(b) => Ok(Predicate::literal(*b)),
            _ => Err(format!("Unsupported literal in predicate position: {value}").into()),
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let col_expr = convert_expr_to_expression(ctx, expr)?;
            Ok(Predicate::from_expr(col_expr))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => convert_in_list(ctx, expr, list, *negated),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => convert_between(ctx, expr, *negated, low, high),
        _ => Err(format!("Unsupported expression type: {expr}").into()),
    }
}

/// Converts a binary operation into a kernel [`Predicate`] with type checking.
///
/// Uses bidirectional type checking:
/// - If left side has a type (e.g., column), check right against it
/// - If right side has a type (e.g., column), check left against it
/// - If both have types, verify they're compatible
/// - If neither has a type, fall back to inference
fn convert_binary_op(
    ctx: &TypeContext,
    left: &Expr,
    op: &ast::BinaryOperator,
    right: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    match op {
        ast::BinaryOperator::Eq
        | ast::BinaryOperator::NotEq
        | ast::BinaryOperator::Lt
        | ast::BinaryOperator::LtEq
        | ast::BinaryOperator::Gt
        | ast::BinaryOperator::GtEq
        | ast::BinaryOperator::Spaceship => {
            let left_synth = try_synthesize_expr(ctx, left);
            let right_synth = try_synthesize_expr(ctx, right);

            let (l, r) = match (left_synth, right_synth) {
                // Both sides have types - verify compatibility
                (Some(left_ty), Some(right_ty)) => {
                    if !can_coerce(&left_ty, &right_ty) && !can_coerce(&right_ty, &left_ty) {
                        return Err(format!(
                            "Type mismatch: cannot compare {:?} with {:?}",
                            left_ty, right_ty
                        )
                        .into());
                    }
                    (
                        convert_expr_to_expression(ctx, left)?,
                        convert_expr_to_expression(ctx, right)?,
                    )
                }
                // Left has type, check right against it
                (Some(left_ty), None) => {
                    let l = convert_expr_to_expression(ctx, left)?;
                    let r = check_expr(ctx, &left_ty, right)?;
                    (l, r)
                }
                // Right has type, check left against it
                (None, Some(right_ty)) => {
                    let l = check_expr(ctx, &right_ty, left)?;
                    let r = convert_expr_to_expression(ctx, right)?;
                    (l, r)
                }
                // Neither has type - fall back to inference
                (None, None) => (
                    convert_expr_to_expression(ctx, left)?,
                    convert_expr_to_expression(ctx, right)?,
                ),
            };

            match op {
                ast::BinaryOperator::Eq => Ok(Predicate::eq(l, r)),
                ast::BinaryOperator::NotEq => Ok(Predicate::ne(l, r)),
                ast::BinaryOperator::Lt => Ok(Predicate::lt(l, r)),
                ast::BinaryOperator::LtEq => Ok(Predicate::le(l, r)),
                ast::BinaryOperator::Gt => Ok(Predicate::gt(l, r)),
                ast::BinaryOperator::GtEq => Ok(Predicate::ge(l, r)),
                ast::BinaryOperator::Spaceship => Ok(Predicate::not(Predicate::distinct(l, r))),
                _ => unreachable!(),
            }
        }
        ast::BinaryOperator::And => {
            let l = convert_expr_to_predicate(ctx, left)?;
            let r = convert_expr_to_predicate(ctx, right)?;
            Ok(Predicate::and(l, r))
        }
        ast::BinaryOperator::Or => {
            let l = convert_expr_to_predicate(ctx, left)?;
            let r = convert_expr_to_predicate(ctx, right)?;
            Ok(Predicate::or(l, r))
        }
        _ => Err(format!("Unsupported binary operator: {op}").into()),
    }
}

/// Converts an IN/NOT IN list into a kernel [`Predicate`] with type checking.
fn convert_in_list(
    ctx: &TypeContext,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    // Synthesize the column type
    let col_ty = try_synthesize_expr(ctx, expr).ok_or_else(|| {
        format!(
            "IN expression requires a column reference on the left side, got: {}",
            expr
        )
    })?;

    // Check each list element against the column type
    let scalars: Vec<Scalar> = list
        .iter()
        .map(|e| {
            let expr = check_expr(ctx, &col_ty, e)?;
            match expr {
                Expression::Literal(s) => Ok(s),
                _ => Err(format!("IN list elements must be literals, got: {e}").into()),
            }
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;

    let array_data = ArrayData::try_new(ArrayType::new(col_ty, false), scalars)?;
    let array_expr = Expression::literal(Scalar::Array(array_data));
    let col_expr = convert_expr_to_expression(ctx, expr)?;

    let pred = Predicate::binary(
        delta_kernel::expressions::BinaryPredicateOp::In,
        col_expr,
        array_expr,
    );
    if negated {
        Ok(Predicate::not(pred))
    } else {
        Ok(pred)
    }
}

/// Converts BETWEEN into `col >= low AND col <= high` with type checking.
fn convert_between(
    ctx: &TypeContext,
    expr: &Expr,
    negated: bool,
    low: &Expr,
    high: &Expr,
) -> Result<Predicate, Box<dyn std::error::Error>> {
    // Synthesize the column type
    let col_ty = try_synthesize_expr(ctx, expr).ok_or_else(|| {
        format!(
            "BETWEEN expression requires a column reference, got: {}",
            expr
        )
    })?;

    let col = convert_expr_to_expression(ctx, expr)?;
    let low_expr = check_expr(ctx, &col_ty, low)?;
    let high_expr = check_expr(ctx, &col_ty, high)?;

    let pred = Predicate::and(
        Predicate::ge(col.clone(), low_expr),
        Predicate::le(col, high_expr),
    );
    if negated {
        Ok(Predicate::not(pred))
    } else {
        Ok(pred)
    }
}

/// Converts a sqlparser AST expression into a kernel [`Expression`] with type checking.
fn convert_expr_to_expression(
    _ctx: &TypeContext,
    expr: &Expr,
) -> Result<Expression, Box<dyn std::error::Error>> {
    // Type checking already happened at this point, so we just convert to Expression.
    match expr {
        Expr::Identifier(ident) => {
            let name = ColumnName::new([ident.value.clone()]);
            Ok(name.into())
        }
        Expr::CompoundIdentifier(parts) => {
            let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
            Ok(Expression::column(names))
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            // Handle compound field access like "user.age"
            let mut parts = Vec::new();
            if let Expr::Function(func) = root.as_ref() {
                for part in &func.name.0 {
                    if let Some(ident) = part.as_ident() {
                        parts.push(ident.value.clone());
                    }
                }
            } else if let Expr::Identifier(ident) = root.as_ref() {
                parts.push(ident.value.clone());
            }

            for access in access_chain {
                if let ast::AccessExpr::Dot(Expr::Identifier(ident)) = access {
                    parts.push(ident.value.clone());
                }
            }

            Ok(Expression::column(parts))
        }
        Expr::Value(value) => {
            // Convert value to expression (untyped - for compatibility)
            match &value.value {
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Scalar::Long(i).into())
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Scalar::Double(f).into())
                    } else {
                        Err(format!("Cannot parse number: {n}").into())
                    }
                }
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    Ok(Scalar::from(s.clone()).into())
                }
                Value::Boolean(b) => Ok(Scalar::Boolean(*b).into()),
                Value::Null => Ok(Scalar::Null(DataType::LONG).into()),
                _ => Err(format!("Unsupported value: {value}").into()),
            }
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(value) = inner.as_ref() {
                // Handle negative numbers
                match &value.value {
                    Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(Scalar::Long(-i).into())
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(Scalar::Double(-f).into())
                        } else {
                            Err(format!("Cannot parse negative number: {n}").into())
                        }
                    }
                    _ => Err("Unsupported negative value".to_string().into()),
                }
            } else {
                Err(format!("Unsupported unary minus on: {expr}").into())
            }
        }
        Expr::Nested(inner) => convert_expr_to_expression(_ctx, inner),
        _ => Err(format!("Unsupported expression: {expr}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::expressions::{column_name as col, Predicate as Pred, Scalar::*};
    use delta_kernel::schema::{MapType, StructField, StructType};
    use rstest::rstest;

    fn test_schema() -> StructType {
        StructType::new_unchecked(vec![
            // Primitive types
            StructField::new("byte_col", DataType::BYTE, true),
            StructField::new("short_col", DataType::SHORT, true),
            StructField::new("int_col", DataType::INTEGER, true),
            StructField::new("long_col", DataType::LONG, true),
            StructField::new("float_col", DataType::FLOAT, true),
            StructField::new("double_col", DataType::DOUBLE, true),
            StructField::new("str_col", DataType::STRING, true),
            StructField::new("bool_col", DataType::BOOLEAN, true),
            StructField::new("date_col", DataType::DATE, true),
            StructField::new("ts_col", DataType::TIMESTAMP, true),
            // Struct type
            StructField::new(
                "struct_col",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("inner_int", DataType::INTEGER, true),
                    StructField::new("inner_str", DataType::STRING, true),
                ]))),
                true,
            ),
            // Array type
            StructField::new(
                "array_col",
                DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                true,
            ),
            // Map type
            StructField::new(
                "map_col",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::LONG,
                    true,
                ))),
                true,
            ),
        ])
    }

    // Comparisons with typed scalars
    #[rstest]
    #[case("long_col < 500", Pred::lt(col!("long_col"), Long(500)))]
    #[case("500 > long_col", Pred::gt(Long(500), col!("long_col")))]
    #[case("long_col = 999", Pred::eq(col!("long_col"), Long(999)))]
    #[case("int_col > 100", Pred::gt(col!("int_col"), Integer(100)))]
    #[case("short_col < 50", Pred::lt(col!("short_col"), Short(50)))]
    #[case("byte_col <= 127", Pred::le(col!("byte_col"), Byte(127)))]
    #[case("double_col >= 3.5", Pred::ge(col!("double_col"), Double(3.5)))]
    #[case("float_col < 1.5", Pred::lt(col!("float_col"), Float(1.5)))]
    #[case("str_col = 'hello'", Pred::eq(col!("str_col"), String("hello".to_string())))]
    #[case("bool_col = true", Pred::eq(col!("bool_col"), Boolean(true)))]
    #[case("bool_col = false", Pred::eq(col!("bool_col"), Boolean(false)))]
    #[case("long_col > -10", Pred::gt(col!("long_col"), Long(-10)))]
    #[case("long_col IS NULL", Pred::is_null(col!("long_col")))]
    #[case("long_col IS NOT NULL", Pred::is_not_null(col!("long_col")))]
    #[case("str_col IS NOT NULL", Pred::not(Pred::is_null(col!("str_col"))))]
    fn parse_comparison(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Logical operators
    #[rstest]
    #[case(
        "long_col < 500 AND int_col > 10",
        Pred::and(
            Pred::lt(col!("long_col"), Long(500)),
            Pred::gt(col!("int_col"), Integer(10))
        )
    )]
    #[case(
        "long_col = 1 OR long_col = 2",
        Pred::or(
            Pred::eq(col!("long_col"), Long(1)),
            Pred::eq(col!("long_col"), Long(2))
        )
    )]
    #[case(
        "NOT long_col = 1",
        Pred::not(Pred::eq(col!("long_col"), Long(1)))
    )]
    #[case(
        "(long_col < 500) AND (int_col > 10)",
        Pred::and(
            Pred::lt(col!("long_col"), Long(500)),
            Pred::gt(col!("int_col"), Integer(10))
        )
    )]
    fn parse_logical(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // BETWEEN
    #[rstest]
    #[case(
        "long_col BETWEEN 10 AND 20",
        Pred::and(
            Pred::ge(col!("long_col"), Long(10)),
            Pred::le(col!("long_col"), Long(20))
        )
    )]
    #[case(
        "short_col BETWEEN 10 AND 20",
        Pred::and(
            Pred::ge(col!("short_col"), Short(10)),
            Pred::le(col!("short_col"), Short(20))
        )
    )]
    #[case(
        "int_col NOT BETWEEN 0 AND 10",
        Pred::not(Pred::and(
            Pred::ge(col!("int_col"), Integer(0)),
            Pred::le(col!("int_col"), Integer(10))
        ))
    )]
    fn parse_between(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // IN list
    #[test]
    fn parse_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("long_col IN (1, 2, 3)", &schema).unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Long(1), Long(2), Long(3)],
        )
        .unwrap();
        let expected = Pred::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            col!("long_col"),
            Expression::literal(Array(array)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_not_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("long_col NOT IN (1, 2)", &schema).unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Long(1), Long(2)],
        )
        .unwrap();
        let expected = Pred::not(Pred::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            col!("long_col"),
            Expression::literal(Array(array)),
        ));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_in_list_typed() {
        let schema = test_schema();
        let pred = parse_predicate("int_col IN (10, 20, 30)", &schema).unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, false),
            vec![Integer(10), Integer(20), Integer(30)],
        )
        .unwrap();
        let expected = Pred::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            col!("int_col"),
            Expression::literal(Array(array)),
        );
        assert_eq!(pred, expected);
    }

    // Null-safe equals
    #[rstest]
    #[case(
        "long_col <=> 1",
        Pred::not(Pred::distinct(col!("long_col"), Long(1)))
    )]
    #[case(
        "long_col <=> NULL",
        Pred::not(Pred::distinct(col!("long_col"), Null(DataType::LONG)))
    )]
    fn parse_null_safe_equals(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Type errors
    #[rstest]
    #[case("long_col > 'CD'", "Type mismatch")]
    #[case("long_col < 'AB'", "Type mismatch")]
    #[case("long_col = false", "Type mismatch")]
    #[case("long_col <= false", "Type mismatch")]
    #[case("false = long_col", "Type mismatch")]
    #[case("str_col = 123", "Type mismatch")]
    #[case("short_col < 100000", "Cannot parse")]
    fn type_error_rejected(#[case] sql: &str, #[case] error_contains: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(result.is_err(), "Expected error for: {}", sql);
        assert!(
            result.unwrap_err().to_string().contains(error_contains),
            "Error should contain '{}'",
            error_contains
        );
    }

    // Unknown column falls back to untyped parsing
    #[test]
    fn unknown_column_falls_back_to_untyped() {
        let schema = test_schema();
        let pred = parse_predicate("unknown_col < 500", &schema).unwrap();
        assert!(format!("{:?}", pred).contains("unknown_col"));
    }

    // Nested struct tests
    #[rstest]
    #[case("struct_col.inner_int > 25", Pred::gt(col!("struct_col.inner_int"), Integer(25)))]
    #[case("struct_col.inner_str = 'alice'", Pred::eq(col!("struct_col.inner_str"), String("alice".to_string())))]
    #[case("struct_col.inner_int IS NULL", Pred::is_null(col!("struct_col.inner_int")))]
    fn parse_nested_struct(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_nested_struct_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("struct_col.inner_int IN (18, 21, 25)", &schema).unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, false),
            vec![Integer(18), Integer(21), Integer(25)],
        )
        .unwrap();
        let expected = Pred::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            col!("struct_col.inner_int"),
            Expression::literal(Array(array)),
        );
        assert_eq!(pred, expected);
    }

    #[rstest]
    #[case(
        "struct_col.inner_int BETWEEN 18 AND 65",
        Pred::and(
            Pred::ge(col!("struct_col.inner_int"), Integer(18)),
            Pred::le(col!("struct_col.inner_int"), Integer(65))
        )
    )]
    fn parse_nested_struct_between(#[case] sql: &str, #[case] expected: Predicate) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // LIKE is not supported
    #[rstest]
    #[case("str_col LIKE 'b%'")]
    #[case("str_col like '%test%'")]
    fn unsupported_like(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(result.is_err(), "LIKE should not be supported: {}", sql);
    }

    // Function calls are not supported
    #[rstest]
    #[case("str_col = HEX('1111')")]
    #[case("size(array_col) > 2")]
    #[case("length(str_col) < 4")]
    fn unsupported_function_calls(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Function calls should not be supported: {}",
            sql
        );
    }

    // Arithmetic expressions are not supported
    #[rstest]
    #[case("long_col % 100 < 10")]
    #[case("long_col + int_col > 10")]
    #[case("long_col * 2 = 10")]
    fn unsupported_arithmetic(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Arithmetic should not be supported: {}",
            sql
        );
    }

    // Typed literals (TIME keyword) are not supported
    #[rstest]
    #[case("ts_col >= TIME '00:00:00'")]
    #[case("ts_col > TIME '12:00:00'")]
    fn unsupported_typed_literals(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Typed literals should not be supported: {}",
            sql
        );
    }

    // IS NULL on complex expressions (not a column) is not supported
    #[rstest]
    #[case("(long_col < 0) IS NULL")]
    #[case("(long_col > 0 AND int_col > 1) IS NULL")]
    fn unsupported_is_null_on_expr(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "IS NULL on expressions should not be supported: {}",
            sql
        );
    }
}
