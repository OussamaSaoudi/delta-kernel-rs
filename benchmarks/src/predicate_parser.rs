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
            let has_subscripts = access_chain.iter().any(|access| !matches!(access, ast::AccessExpr::Dot(_)));
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

    let year: i32 = parts[0].parse().map_err(|_| format!("Invalid year: {}", parts[0]))?;
    let month: u32 = parts[1].parse().map_err(|_| format!("Invalid month: {}", parts[1]))?;
    let day: u32 = parts[2].parse().map_err(|_| format!("Invalid day: {}", parts[2]))?;

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

    let hours: i64 = parts[0].parse().map_err(|_| format!("Invalid hours: {}", parts[0]))?;
    let minutes: i64 = parts[1].parse().map_err(|_| format!("Invalid minutes: {}", parts[1]))?;
    let seconds: i64 = parts[2].parse().map_err(|_| format!("Invalid seconds: {}", parts[2]))?;

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
    use delta_kernel::expressions::column_name;
    use delta_kernel::schema::{StructField, StructType};

    fn test_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::LONG, true),
            StructField::new("value", DataType::LONG, true),
            StructField::new("version_tag", DataType::STRING, true),
            StructField::new("a", DataType::LONG, true),
            StructField::new("b", DataType::LONG, true),
            StructField::new("c", DataType::LONG, true),
            StructField::new("full_name", DataType::STRING, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("age", DataType::LONG, true),
            StructField::new("score", DataType::LONG, true),
            StructField::new("status", DataType::STRING, true),
            StructField::new("price", DataType::DOUBLE, true),
            StructField::new("amount", DataType::DOUBLE, true),
            StructField::new("distance", DataType::DOUBLE, true),
            StructField::new("flag", DataType::BOOLEAN, true),
            StructField::new("active", DataType::BOOLEAN, true),
            StructField::new("dt", DataType::DATE, true),
            StructField::new("date_col", DataType::DATE, true),
            StructField::new("event_date", DataType::DATE, true),
            StructField::new("start_date", DataType::DATE, true),
            StructField::new("time_col", DataType::TIMESTAMP, true),
            StructField::new("event_time", DataType::TIMESTAMP, true),
            StructField::new("start_time", DataType::TIMESTAMP, true),
            StructField::new("byte_col", DataType::BYTE, true),
            StructField::new("short_col", DataType::SHORT, true),
            StructField::new("int_col", DataType::INTEGER, true),
            StructField::new("long_col", DataType::LONG, true),
            StructField::new("float_col", DataType::FLOAT, true),
            StructField::new("double_col", DataType::DOUBLE, true),
            StructField::new("str_col", DataType::STRING, true),
            StructField::new("c1", DataType::LONG, true),      // Used with integers in tests
            StructField::new("c2", DataType::STRING, true),
            StructField::new("c3", DataType::DOUBLE, true),    // Used with floats in tests
            StructField::new("c4", DataType::DOUBLE, true),    // Used with floats in tests
            StructField::new("c5", DataType::STRING, true),
            StructField::new("c6", DataType::STRING, true),
            StructField::new("c7", DataType::STRING, true),
            StructField::new("c8", DataType::STRING, true),
            StructField::new("c9", DataType::BOOLEAN, true),
            StructField::new("c10", DataType::DOUBLE, true),   // Used with floats in tests
            StructField::new("cc1", DataType::LONG, true),     // Used with integers in tests
            StructField::new("cc2", DataType::STRING, true),
            StructField::new("cc3", DataType::DOUBLE, true),   // Used with floats in tests
            StructField::new("cc4", DataType::DOUBLE, true),   // Used with floats in tests
            StructField::new("cc5", DataType::STRING, true),
            StructField::new("cc6", DataType::STRING, true),
            StructField::new("cc7", DataType::STRING, true),
            StructField::new("cc8", DataType::STRING, true),
            StructField::new("cc9", DataType::BOOLEAN, true),
            StructField::new("cc10", DataType::DOUBLE, true),  // Used with floats in tests
            StructField::new("part", DataType::LONG, true),  // Changed to LONG for partition columns
            StructField::new("category", DataType::STRING, true),
            StructField::new("date", DataType::DATE, true),
            StructField::new("fruit", DataType::STRING, true),
            StructField::new("code", DataType::STRING, true),
            StructField::new("domain", DataType::STRING, true),
            StructField::new("date_part", DataType::DATE, true),
            StructField::new("wrapper", DataType::Struct(Box::new(StructType::new_unchecked(vec![
                StructField::new("label", DataType::STRING, true),
            ]))), true),
            // Changed c1-c4 and cc1-cc4 to match the data types the tests expect
            // Some appear to be numeric based on the test predicates
        ])
    }

    #[test]
    fn parse_simple_comparison() {
        let schema = test_schema();
        let pred = parse_predicate("id < 500", &schema).unwrap();
        let expected = Predicate::lt(column_name!("id"), Scalar::Long(500));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_and_predicate() {
        let schema = test_schema();
        let pred = parse_predicate("id < 500 AND value > 10", &schema).unwrap();
        let expected = Predicate::and(
            Predicate::lt(column_name!("id"), Scalar::Long(500)),
            Predicate::gt(column_name!("value"), Scalar::Long(10)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_or_predicate() {
        let schema = test_schema();
        let pred = parse_predicate("id = 1 OR id = 2", &schema).unwrap();
        let expected = Predicate::or(
            Predicate::eq(column_name!("id"), Scalar::Long(1)),
            Predicate::eq(column_name!("id"), Scalar::Long(2)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_string_comparison() {
        let schema = test_schema();
        let pred = parse_predicate("version_tag = 'v0'", &schema).unwrap();
        let expected = Predicate::eq(column_name!("version_tag"), Scalar::from("v0".to_string()));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_not_predicate() {
        let schema = test_schema();
        let pred = parse_predicate("NOT id = 500", &schema).unwrap();
        let expected = Predicate::not(Predicate::eq(column_name!("id"), Scalar::Long(500)));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_is_null() {
        let schema = test_schema();
        let pred = parse_predicate("id IS NULL", &schema).unwrap();
        let expected = Predicate::is_null(column_name!("id"));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_is_not_null() {
        let schema = test_schema();
        let pred = parse_predicate("id IS NOT NULL", &schema).unwrap();
        let expected = Predicate::is_not_null(column_name!("id"));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_nested_expression() {
        let schema = test_schema();
        let pred = parse_predicate("(id < 500) AND (value > 10)", &schema).unwrap();
        let expected = Predicate::and(
            Predicate::lt(column_name!("id"), Scalar::Long(500)),
            Predicate::gt(column_name!("value"), Scalar::Long(10)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_negative_number() {
        let schema = test_schema();
        let pred = parse_predicate("id > -100", &schema).unwrap();
        let expected = Predicate::gt(column_name!("id"), Scalar::Long(-100));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_complex_predicate() {
        let schema = test_schema();
        let pred = parse_predicate("id >= 0 AND id < 1000 AND version_tag = 'v0'", &schema).unwrap();
        let expected = Predicate::and(
            Predicate::and(
                Predicate::ge(column_name!("id"), Scalar::Long(0)),
                Predicate::lt(column_name!("id"), Scalar::Long(1000)),
            ),
            Predicate::eq(column_name!("version_tag"), Scalar::from("v0".to_string())),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("a in (1, 2, 3)", &schema).unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(3)],
        )
        .unwrap();
        let expected = Predicate::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            column_name!("a"),
            Expression::literal(Scalar::Array(array)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_not_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("a NOT IN (1, 2)", &schema).unwrap();
        let array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, false),
            vec![Scalar::Long(1), Scalar::Long(2)],
        )
        .unwrap();
        let expected = Predicate::not(Predicate::binary(
            delta_kernel::expressions::BinaryPredicateOp::In,
            column_name!("a"),
            Expression::literal(Scalar::Array(array)),
        ));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_between() {
        let schema = test_schema();
        let pred = parse_predicate("id BETWEEN 10 AND 20", &schema).unwrap();
        let expected = Predicate::and(
            Predicate::ge(column_name!("id"), Scalar::Long(10)),
            Predicate::le(column_name!("id"), Scalar::Long(20)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_null_safe_equals() {
        let schema = test_schema();
        let pred = parse_predicate("a <=> 1", &schema).unwrap();
        let expected = Predicate::not(Predicate::distinct(column_name!("a"), Scalar::Long(1)));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_null_safe_equals_null() {
        let schema = test_schema();
        let pred = parse_predicate("a <=> NULL", &schema).unwrap();
        let expected = Predicate::not(Predicate::distinct(
            column_name!("a"),
            Scalar::Null(DataType::LONG),
        ));
        assert_eq!(pred, expected);
    }

    /// Bulk test: predicates that the typed parser should handle successfully.
    /// Column names indicate their types (e.g., long_col, str_col, double_col).
    #[test]
    fn parse_supported_predicates_with_types() {
        let schema = test_schema();
        let supported = [
            // LONG column comparisons
            "id < 5",
            "id = 999",
            "id >= 50 AND id < 80",
            "id BETWEEN 10 AND 20",
            "id BETWEEN -10 AND -1",
            "value >= 100",
            "long_col >= 900 AND long_col < 950",
            "long_col <=> 1",
            "long_col <> 1",
            "long_col <= 1 AND long_col > -1",
            "long_col > 0 AND long_col < 3",
            "long_col IS NULL",
            "long_col IS NOT NULL",
            "NOT long_col = 1",
            "NOT long_col <=> NULL",
            "long_col = 1 OR long_col = 2",
            "long_col >= 2 OR long_col < -1",
            "long_col IN (1, 2, 3)",
            "long_col NOT IN (1, 2)",
            "value IN (100, 200, 300)",
            "long_col <=> NULL",
            "NOT (long_col <=> NULL)",
            "NOT(long_col < 1 OR value > 20)",
            "NOT(long_col >= 10 AND value <= 20)",
            "part = 1",
            "part = 0",
            // INTEGER column comparisons
            "int_col = 50",
            "int_col IS NULL",
            "int_col IS NOT NULL AND str_col IS NOT NULL",
            // STRING column comparisons
            "str_col = 'hello'",
            "name = 'alice'",
            "status = 'active'",
            "category = 'A'",
            "fruit = 'apple'",
            "code = 'ABCDE'",
            "domain = 'example.com'",
            "name IS NOT NULL",
            // DOUBLE column comparisons
            "price > 100.0",
            "amount > 250.0",
            "distance = 5.0",
            "double_col > 1400.0",
            // BOOLEAN column comparisons
            "flag = true",
            "active = false",
            // DATE column comparisons (accept string date literals)
            "dt >= '2024-01-15'",
            "date_col = '2024-01-15'",
            "event_date = '2024-01-15'",
            "start_date >= '2024-02-01'",
            "date = '2024-01-01'",
            "date_part = '2024-01-01'",
            // TIMESTAMP column comparisons (accept string time literals)
            "time_col >= '2024-01-01 10:00:00'",
            "event_time >= '12:00:00'",
            "start_time < '10:00:00'",
            // Boolean literals
            "TRUE",
            "FALSE",
            // Nested struct field
            "wrapper.label = 'first'",
        ];

        let mut failures = Vec::new();
        for sql in &supported {
            if let Err(e) = parse_predicate(sql, &schema) {
                failures.push(format!("  FAIL: {sql:?} -> {e}"));
            }
        }
        assert!(
            failures.is_empty(),
            "Expected all predicates to parse successfully, but {} failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    /// Tests that type errors are correctly detected and rejected.
    /// Column names clearly indicate the type to make failures obvious.
    #[test]
    fn type_errors_are_rejected() {
        let schema = test_schema();
        let type_errors = [
            // LONG column with non-date string literal
            ("long_col > 'CD'", "comparing LONG column with non-date string"),
            ("long_col < 'AB'", "comparing LONG column with non-date string"),
            ("value = 'hello'", "comparing LONG column with string"),
            // LONG column with boolean
            ("long_col = false", "comparing LONG column with boolean"),
            ("long_col <= false", "comparing LONG column with boolean"),
            // LONG column with float (cannot parse as long)
            ("value > 1.5", "comparing LONG column with float literal"),
            // Boolean compared with LONG
            ("false = long_col", "comparing boolean with LONG column"),
            // LONG column (part) with string
            ("part = '1'", "comparing LONG column with string"),
        ];

        for (sql, description) in &type_errors {
            let result = parse_predicate(sql, &schema);
            assert!(
                result.is_err(),
                "Expected type error for {}: {} - but it parsed successfully as {:?}",
                description, sql, result.unwrap()
            );
        }
    }

    /// Tests for the typed predicate parser with bidirectional type checking.
    mod typed_parser_tests {
        use super::*;
        use delta_kernel::schema::{StructField, StructType};
        use rstest::rstest;

        fn test_schema() -> StructType {
            StructType::try_new(vec![
                StructField::new("id", DataType::LONG, false),
                StructField::new("age", DataType::INTEGER, false),
                StructField::new("score", DataType::SHORT, false),
                StructField::new("rating", DataType::DOUBLE, false),
                StructField::new("name", DataType::STRING, false),
                StructField::new("active", DataType::BOOLEAN, false),
            ])
            .unwrap()
        }

        #[rstest]
        #[case("id < 500", Predicate::lt(column_name!("id"), Scalar::Long(500)))]
        #[case("500 > id", Predicate::gt(Scalar::Long(500), column_name!("id")))]
        #[case("age > 100", Predicate::gt(column_name!("age"), Scalar::Integer(100)))]
        #[case("score < 50", Predicate::lt(column_name!("score"), Scalar::Short(50)))]
        #[case("rating >= 3.5", Predicate::ge(column_name!("rating"), Scalar::Double(3.5)))]
        #[case("name = 'alice'", Predicate::eq(column_name!("name"), Scalar::from("alice".to_string())))]
        #[case("active = true", Predicate::eq(column_name!("active"), Scalar::Boolean(true)))]
        #[case("age > -10", Predicate::gt(column_name!("age"), Scalar::Integer(-10)))]
        fn typed_parse_success(#[case] sql: &str, #[case] expected: Predicate) {
            let schema = test_schema();
            let pred = parse_predicate(sql, &schema).unwrap();
            assert_eq!(pred, expected);
        }

        #[test]
        fn typed_parse_in_list() {
            let schema = test_schema();
            let pred = parse_predicate("age IN (10, 20, 30)", &schema).unwrap();
            let array = ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                vec![
                    Scalar::Integer(10),
                    Scalar::Integer(20),
                    Scalar::Integer(30),
                ],
            )
            .unwrap();
            let expected = Predicate::binary(
                delta_kernel::expressions::BinaryPredicateOp::In,
                column_name!("age"),
                Expression::literal(Scalar::Array(array)),
            );
            assert_eq!(pred, expected);
        }

        #[test]
        fn typed_parse_between() {
            let schema = test_schema();
            let pred = parse_predicate("score BETWEEN 10 AND 20", &schema).unwrap();
            let expected = Predicate::and(
                Predicate::ge(column_name!("score"), Scalar::Short(10)),
                Predicate::le(column_name!("score"), Scalar::Short(20)),
            );
            assert_eq!(pred, expected);
        }

        #[test]
        fn typed_parse_complex_predicate() {
            let schema = test_schema();
            let pred = parse_predicate("age >= 18 AND age < 65 AND name = 'alice'", &schema)
                .unwrap();
            let expected = Predicate::and(
                Predicate::and(
                    Predicate::ge(column_name!("age"), Scalar::Integer(18)),
                    Predicate::lt(column_name!("age"), Scalar::Integer(65)),
                ),
                Predicate::eq(column_name!("name"), Scalar::from("alice".to_string())),
            );
            assert_eq!(pred, expected);
        }

        #[rstest]
        #[case("id < 'abc'", "Type mismatch")]
        #[case("name = 123", "Type mismatch")]
        #[case("score < 100000", "Cannot parse")]
        fn typed_parse_errors(#[case] sql: &str, #[case] error_contains: &str) {
            let schema = test_schema();
            let result = parse_predicate(sql, &schema);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(error_contains));
        }

        #[test]
        fn typed_unknown_column_falls_back() {
            let schema = test_schema();
            // Unknown columns fall back to untyped parsing
            // The column reference is created, but without type information
            // It will only error at evaluation time, not parse time
            let result = parse_predicate("unknown_col < 500", &schema);
            assert!(result.is_ok());
        }

        // Complex validation tests
        mod complex_validation {
            use super::*;

            fn nested_schema() -> StructType {
                StructType::try_new(vec![
                    StructField::new("id", DataType::LONG, false),
                    StructField::new(
                        "user",
                        DataType::try_struct_type(vec![
                            StructField::new("age", DataType::INTEGER, false),
                            StructField::new("name", DataType::STRING, false),
                        ])
                        .unwrap(),
                        false,
                    ),
                ])
                .unwrap()
            }

            #[test]
            fn complex_nested_struct_navigation() {
                let schema = nested_schema();

                // First test: simple top-level column
                let pred = parse_predicate("id > 1000", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Long(1000)"));

                // Test nested column reference: user.age
                let pred = parse_predicate("user.age > 25", &schema).unwrap();
                // Should have typed the literal as Integer (the type of user.age)
                assert!(format!("{:?}", pred).contains("Integer(25)"));

                // Test nested column reference: user.name
                let pred = parse_predicate("user.name = 'alice'", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("alice"));

                // Test mixing top-level and nested columns
                let pred = parse_predicate("id > 1000 AND user.age < 50", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Long(1000)"));
                assert!(format!("{:?}", pred).contains("Integer(50)"));
            }

            #[test]
            fn complex_nested_struct_in_operations() {
        let schema = test_schema();
                let schema = nested_schema();

                // IN list with nested column
                let pred = parse_predicate("user.age IN (18, 21, 25, 30)", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Integer(18)"));

                // BETWEEN with nested column
                let pred = parse_predicate("user.age BETWEEN 18 AND 65", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Integer(18)"));
                assert!(format!("{:?}", pred).contains("Integer(65)"));
            }

            #[rstest]
            #[case("id IS NULL", "simple column")]
            #[case("user.age IS NULL", "nested struct field")]
            #[case("user.name IS NOT NULL", "nested struct IS NOT NULL")]
            fn complex_is_null_operations(#[case] sql: &str, #[case] _desc: &str) {
                let schema = nested_schema();
                // IS NULL and IS NOT NULL work on any expression including nested fields
                // This demonstrates that kernel's Predicate::is_null accepts any Expression
                let result = parse_predicate(sql, &schema);
                assert!(result.is_ok(), "Failed: {}", sql);
            }

            #[rstest]
            #[case("(id > 100 AND id < 200) OR id = 50", "complex OR with nested AND")]
            #[case("(id < 50 OR id > 150) AND age > 25", "mixed AND/OR")]
            fn complex_logical_predicates(#[case] sql: &str, #[case] _desc: &str) {
                let schema = test_schema();
                let result = parse_predicate(sql, &schema);
                assert!(result.is_ok(), "Failed to parse: {}", sql);
            }

            #[rstest]
            #[case("id BETWEEN 10 AND 20 AND age BETWEEN 30 AND 40", "multiple BETWEEN")]
            #[case(
                "(id > 10 AND age < 50) AND (name = 'alice' OR name = 'bob')",
                "deeply nested"
            )]
            fn complex_mixed_operators(#[case] sql: &str, #[case] _desc: &str) {
                let schema = test_schema();
                let result = parse_predicate(sql, &schema);
                assert!(result.is_ok(), "Failed to parse: {}", sql);
            }

            #[test]
            fn complex_multiple_columns_different_types() {
                let schema = test_schema();
                // Mix different types in same predicate
                let pred = parse_predicate(
                    "id > 1000 AND age < 50 AND score >= 80 AND rating > 3.5 AND name = 'test' AND active = true",
                    &schema,
                ).unwrap();

                // Verify all type coercions happened correctly by checking the predicate structure
                assert!(format!("{:?}", pred).contains("1000"));
                assert!(format!("{:?}", pred).contains("50"));
                assert!(format!("{:?}", pred).contains("80"));
                assert!(format!("{:?}", pred).contains("3.5"));
                assert!(format!("{:?}", pred).contains("test"));
                assert!(format!("{:?}", pred).contains("true"));
            }

            #[test]
            fn complex_type_widening_in_comparisons() {
        let schema = test_schema();
                let schema = StructType::try_new(vec![
                    StructField::new("byte_col", DataType::BYTE, false),
                    StructField::new("short_col", DataType::SHORT, false),
                    StructField::new("int_col", DataType::INTEGER, false),
                    StructField::new("long_col", DataType::LONG, false),
                ])
                .unwrap();

                // All these should work due to type widening
                let cases = vec![
                    ("byte_col < 100", Scalar::Byte(100)),
                    ("short_col < 30000", Scalar::Short(30000)),
                    ("int_col < 2000000", Scalar::Integer(2000000)),
                    ("long_col < 9000000000", Scalar::Long(9000000000)),
                ];

                for (sql, expected_scalar) in cases {
                    let pred = parse_predicate(sql, &schema).unwrap();
                    // Verify scalar type is correct
                    let pred_str = format!("{:?}", pred);
                    let scalar_str = format!("{:?}", expected_scalar);
                    assert!(
                        pred_str.contains(&scalar_str),
                        "Expected {} in predicate for '{}'",
                        scalar_str,
                        sql
                    );
                }
            }

            #[rstest]
            #[case("age > -50", Scalar::Integer(-50), "negative Integer")]
            #[case("rating > -99.5", Scalar::Double(-99.5), "negative Double")]
            fn complex_negative_numbers(
                #[case] sql: &str,
                #[case] expected: Scalar,
                #[case] _desc: &str,
            ) {
                let schema = test_schema();
                let pred = parse_predicate(sql, &schema).unwrap();
                let pred_str = format!("{:?}", pred);
                let expected_str = format!("{:?}", expected);
                assert!(pred_str.contains(&expected_str));
            }

            #[test]
            fn complex_in_list_with_different_sizes() {
                let schema = test_schema();

                // Small list
                let pred = parse_predicate("age IN (1, 2)", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Integer(1)"));

                // Medium list
                let pred = parse_predicate("age IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)", &schema)
                    .unwrap();
                assert!(format!("{:?}", pred).contains("Integer(10)"));
            }

            #[test]
            fn complex_between_with_negation() {
                let schema = test_schema();

                // Normal BETWEEN
                let pred = parse_predicate("age BETWEEN 18 AND 65", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Integer(18)"));
                assert!(format!("{:?}", pred).contains("Integer(65)"));

                // NOT BETWEEN
                let pred = parse_predicate("age NOT BETWEEN 0 AND 10", &schema).unwrap();
                assert!(format!("{:?}", pred).contains("Integer(0)"));
                assert!(format!("{:?}", pred).contains("Integer(10)"));
            }

            #[rstest]
            #[case("id = age", "comparing two Integer columns")]
            fn complex_column_to_column_comparisons(#[case] sql: &str, #[case] _desc: &str) {
                let schema = test_schema();
                let result = parse_predicate(sql, &schema);
                // Should succeed - columns can be compared
                assert!(result.is_ok(), "Failed: {}", sql);
            }

            #[test]
            fn complex_string_comparisons() {
                let schema = test_schema();

                let cases = vec!["name = 'alice'", "name >= 'a' AND name < 'n'"];

                for sql in cases {
                    let result = parse_predicate(sql, &schema);
                    assert!(result.is_ok(), "Failed: {}", sql);
                }
            }

            #[test]
            fn complex_boolean_logic() {
                let schema = test_schema();

                let cases = vec!["active = true", "active = true AND age > 18"];

                for sql in cases {
                    let result = parse_predicate(sql, &schema);
                    assert!(result.is_ok(), "Failed: {}", sql);
                }
            }

            #[test]
            fn complex_deeply_nested_predicates() {
                let schema = test_schema();

                let sql = "((id > 10 AND age < 50) OR (name = 'test' AND active = true)) AND \
                          (score BETWEEN 70 AND 90 OR rating > 4.0)";
                let result = parse_predicate(sql, &schema);
                assert!(result.is_ok());
            }
        }
    }

    /// Predicates that use SQL features not representable in kernel expressions.
    /// These should fail gracefully with an error (not panic).
    #[test]
    fn unsupported_predicates_fail_gracefully() {
        let schema = test_schema();
        let unsupported = [
            // LIKE (no kernel support)
            "a like 'C%'",
            "a like 'A%'",
            "a.b like '%'",
            "a.b like 'mic%'",
            "fruit like 'b%'",
            "fruit like 'a%'",
            "fruit like 'z%'",
            "fruit like '%'",
            "name LIKE 'b%'",
            "a > 0 AND b like '2016-%'",
            "a >= 2 AND b like '2017-08-%'",
            "a >= 2 or b like '2016-08-%'",
            "a < 2 or b like '2017-08-%'",
            "a < 0 or b like '2016-%'",
            // Function calls
            "cc8 = HEX('1111')",
            "cc8 = HEX('3333')",
            "c8 = HEX('3333')",
            "c8 = HEX('1111')",
            "size(items) > 2",
            "size(tags) > 2",
            "length(s) < 4",
            // Arithmetic expressions
            "a % 100 < 10 OR b > 20",
            "a % 100 < 10 AND b > 20",
            "a < 10 OR b % 100 > 20",
            "a % 100 < 10 AND b % 100 > 20",
            "a < 10 AND b % 100 > 20",
            // Typed literals (TIME keyword)
            "time_col >= TIME '00:00:00'",
            "time_col >= TIME '10:00:00' AND time_col < TIME '12:00:00'",
            "time_col > TIME '12:00:00'",
            "time_col < TIME '12:00:00'",
            // IS NULL on complex expressions (not a column)
            "(a < 0) IS NULL",
            "(a > 0) IS NULL",
            "(a > 1) IS NULL",
            "NOT ((a > 0) IS NULL)",
            "NOT ((a > 1) IS NULL)",
            "(a > 0 OR b > 1) IS NULL",
            "(a > 0 AND b > 1) IS NULL",
            "(a > 1 AND a > 0) IS NULL",
            "(b > 1 AND a > 0) IS NULL",
            "(b > 1 OR a < 0) IS NULL",
            "(a > 1 OR a < 0) IS NULL",
            "(b > 0) IS NULL",
            "(b < 0) IS NULL",
        ];

        for sql in &unsupported {
            let result = parse_predicate(sql, &schema);
            assert!(
                result.is_err(),
                "Expected {sql:?} to fail, but it parsed as: {:?}",
                result.unwrap()
            );
        }
    }
}
