//! SQL predicate parser for improved_dat test predicates.
//!
//! Uses sqlparser crate to parse SQL WHERE clause expressions and converts them
//! to kernel [`Predicate`] types with automatic type coercion based on schema.

use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, Schema, StructType};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Error type for predicate parsing
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    SqlParser(String),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpr(String),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),
    #[error("Invalid literal: {0}")]
    InvalidLiteral(String),
}

impl From<sqlparser::parser::ParserError> for ParseError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        ParseError::SqlParser(e.to_string())
    }
}

/// Schema-aware predicate parser
pub struct SchemaAwareParser<'a> {
    schema: Option<&'a Schema>,
}

impl<'a> SchemaAwareParser<'a> {
    /// Create a new parser with optional schema for type coercion
    pub fn new(schema: Option<&'a Schema>) -> Self {
        Self { schema }
    }

    /// Find the data type of a column in the schema
    fn find_column_type(&self, column_name: &ColumnName) -> Option<&DataType> {
        let schema = self.schema?;
        let parts = column_name.path();

        // Navigate through nested structs
        let mut current_struct: &StructType = schema;
        let mut result_type: Option<&DataType> = None;

        for (i, part) in parts.iter().enumerate() {
            let field = current_struct.fields().find(|f| f.name() == part)?;
            if i == parts.len() - 1 {
                result_type = Some(field.data_type());
            } else {
                // Navigate into struct
                if let DataType::Struct(inner) = field.data_type() {
                    current_struct = inner;
                } else {
                    return None;
                }
            }
        }
        result_type
    }

    /// Check if data type is INTEGER
    fn is_integer(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Integer))
    }

    /// Check if data type is SHORT
    fn is_short(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Short))
    }

    /// Check if data type is BYTE
    fn is_byte(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Byte))
    }

    /// Check if data type is LONG
    fn is_long(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Long))
    }

    /// Check if data type is FLOAT
    fn is_float(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Float))
    }

    /// Check if data type is DOUBLE
    fn is_double(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Double))
    }

    /// Check if data type is DATE
    fn is_date(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Date))
    }

    /// Check if data type is TIMESTAMP
    fn is_timestamp(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::Timestamp))
    }

    /// Check if data type is TIMESTAMP_NTZ
    fn is_timestamp_ntz(dt: &DataType) -> bool {
        matches!(dt, DataType::Primitive(PrimitiveType::TimestampNtz))
    }

    /// Get decimal type if this is a decimal
    fn get_decimal_type(dt: &DataType) -> Option<&DecimalType> {
        if let DataType::Primitive(PrimitiveType::Decimal(dtype)) = dt {
            Some(dtype)
        } else {
            None
        }
    }

    /// Convert a SQL literal value to a kernel Scalar, with optional type coercion
    fn value_to_scalar(
        &self,
        value: &Value,
        target_type: Option<&DataType>,
    ) -> Result<Scalar, ParseError> {
        match value {
            Value::Number(n, _) => {
                // Parse as the target type if known, otherwise default to Long
                if let Some(dt) = target_type {
                    if Self::is_integer(dt) {
                        let i: i32 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as INTEGER", n))
                        })?;
                        return Ok(Scalar::Integer(i));
                    }
                    if Self::is_short(dt) {
                        let s: i16 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as SHORT", n))
                        })?;
                        return Ok(Scalar::Short(s));
                    }
                    if Self::is_byte(dt) {
                        let b: i8 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as BYTE", n))
                        })?;
                        return Ok(Scalar::Byte(b));
                    }
                    if Self::is_long(dt) {
                        let l: i64 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as LONG", n))
                        })?;
                        return Ok(Scalar::Long(l));
                    }
                    if Self::is_float(dt) {
                        let f: f32 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as FLOAT", n))
                        })?;
                        return Ok(Scalar::Float(f));
                    }
                    if Self::is_double(dt) {
                        let d: f64 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as DOUBLE", n))
                        })?;
                        return Ok(Scalar::Double(d));
                    }
                    if let Some(decimal_type) = Self::get_decimal_type(dt) {
                        // Parse decimal - for now just parse as f64 and scale
                        let d: f64 = n.parse().map_err(|_| {
                            ParseError::InvalidLiteral(format!("Cannot parse '{}' as DECIMAL", n))
                        })?;
                        let scale_factor = 10_i128.pow(decimal_type.scale() as u32);
                        let bits = (d * scale_factor as f64).round() as i128;
                        return Scalar::decimal(bits, decimal_type.precision(), decimal_type.scale())
                            .map_err(|e| ParseError::InvalidLiteral(format!("Invalid decimal: {}", e)));
                    }
                }
                // Default: try to parse as i64, fall back to f64
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Scalar::Long(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Scalar::Double(f))
                } else {
                    Err(ParseError::InvalidLiteral(format!(
                        "Cannot parse number: {}",
                        n
                    )))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                // Handle date/timestamp strings if target type requires
                if let Some(dt) = target_type {
                    if Self::is_date(dt) {
                        // Parse date string like "2024-01-15"
                        let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(
                            |_| ParseError::InvalidLiteral(format!("Cannot parse '{}' as DATE", s)),
                        )?;
                        let days = date
                            .signed_duration_since(
                                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                            )
                            .num_days() as i32;
                        return Ok(Scalar::Date(days));
                    }
                    if Self::is_timestamp(dt) || Self::is_timestamp_ntz(dt) {
                        // Parse timestamp string
                        let ts =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                .or_else(|_| {
                                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                })
                                .or_else(|_| {
                                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                                })
                                .or_else(|_| {
                                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                                })
                                .map_err(|_| {
                                    ParseError::InvalidLiteral(format!(
                                        "Cannot parse '{}' as TIMESTAMP",
                                        s
                                    ))
                                })?;
                        let micros = ts.and_utc().timestamp_micros();
                        if Self::is_timestamp_ntz(dt) {
                            return Ok(Scalar::TimestampNtz(micros));
                        }
                        return Ok(Scalar::Timestamp(micros));
                    }
                }
                Ok(Scalar::String(s.clone()))
            }
            Value::Boolean(b) => Ok(Scalar::Boolean(*b)),
            Value::Null => {
                // Use target type for null if known
                let dt = target_type.cloned().unwrap_or(DataType::STRING);
                Ok(Scalar::Null(dt))
            }
            _ => Err(ParseError::UnsupportedExpr(format!(
                "Unsupported literal: {:?}",
                value
            ))),
        }
    }

    /// Recursively extract all column references from a SQL expression.
    /// Used to convert `(expr) IS NULL` into `col1 IS NULL OR col2 IS NULL ...`
    /// since a comparison is null iff any of its column operands is null.
    fn extract_column_refs(expr: &Expr) -> Vec<ColumnName> {
        match expr {
            Expr::Identifier(ident) => vec![ColumnName::new([ident.value.clone()])],
            Expr::CompoundIdentifier(parts) => {
                let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
                vec![ColumnName::new(names)]
            }
            Expr::BinaryOp { left, right, .. } => {
                let mut cols = Self::extract_column_refs(left);
                cols.extend(Self::extract_column_refs(right));
                cols
            }
            Expr::UnaryOp { expr, .. } => Self::extract_column_refs(expr),
            Expr::Nested(inner) => Self::extract_column_refs(inner),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => Self::extract_column_refs(inner),
            _ => vec![], // Literals, functions, etc. — no column refs
        }
    }

    /// Extract column name from an expression (for type lookup)
    fn extract_column_name(&self, expr: &Expr) -> Option<ColumnName> {
        match expr {
            Expr::Identifier(ident) => Some(ColumnName::new([ident.value.clone()])),
            Expr::CompoundIdentifier(parts) => {
                let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
                Some(ColumnName::new(names))
            }
            _ => None,
        }
    }

    /// Convert a SQL expression to a kernel Expression
    fn expr_to_expression(
        &self,
        expr: &Expr,
        type_hint: Option<&DataType>,
    ) -> Result<Expression, ParseError> {
        match expr {
            Expr::Identifier(ident) => {
                Ok(Expression::column(ColumnName::new([ident.value.clone()])))
            }
            Expr::CompoundIdentifier(parts) => {
                let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
                Ok(Expression::column(ColumnName::new(names)))
            }
            Expr::Value(value) => {
                let scalar = self.value_to_scalar(value, type_hint)?;
                Ok(Expression::literal(scalar))
            }
            Expr::Nested(inner) => self.expr_to_expression(inner, type_hint),
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Minus => {
                    // Handle negative numbers
                    if let Expr::Value(Value::Number(n, long)) = expr.as_ref() {
                        let neg_n = format!("-{}", n);
                        let neg_value = Value::Number(neg_n, *long);
                        let scalar = self.value_to_scalar(&neg_value, type_hint)?;
                        Ok(Expression::literal(scalar))
                    } else {
                        Err(ParseError::UnsupportedExpr(
                            "Unary minus on non-literal".to_string(),
                        ))
                    }
                }
                _ => Err(ParseError::UnsupportedOperator(format!("{:?}", op))),
            },
            _ => Err(ParseError::UnsupportedExpr(format!("{:?}", expr))),
        }
    }

    /// Convert a SQL expression to a kernel Predicate
    fn expr_to_predicate(&self, expr: &Expr) -> Result<Predicate, ParseError> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // For comparisons, try to infer type from column side
                let left_col = self.extract_column_name(left);
                let right_col = self.extract_column_name(right);

                let type_hint = left_col
                    .as_ref()
                    .and_then(|c| self.find_column_type(c))
                    .or_else(|| right_col.as_ref().and_then(|c| self.find_column_type(c)));

                match op {
                    BinaryOperator::Eq => {
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::eq(l, r))
                    }
                    BinaryOperator::NotEq => {
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::ne(l, r))
                    }
                    BinaryOperator::Lt => {
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::lt(l, r))
                    }
                    BinaryOperator::LtEq => {
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::le(l, r))
                    }
                    BinaryOperator::Gt => {
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::gt(l, r))
                    }
                    BinaryOperator::GtEq => {
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::ge(l, r))
                    }
                    BinaryOperator::And => {
                        let l = self.expr_to_predicate(left)?;
                        let r = self.expr_to_predicate(right)?;
                        Ok(Predicate::and(l, r))
                    }
                    BinaryOperator::Or => {
                        let l = self.expr_to_predicate(left)?;
                        let r = self.expr_to_predicate(right)?;
                        Ok(Predicate::or(l, r))
                    }
                    BinaryOperator::Spaceship => {
                        // <=> null-safe equality
                        let l = self.expr_to_expression(left, type_hint)?;
                        let r = self.expr_to_expression(right, type_hint)?;
                        Ok(Predicate::not(Predicate::distinct(l, r)))
                    }
                    _ => Err(ParseError::UnsupportedOperator(format!("{:?}", op))),
                }
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let inner = self.expr_to_predicate(expr)?;
                    Ok(Predicate::not(inner))
                }
                _ => Err(ParseError::UnsupportedOperator(format!("{:?}", op))),
            },
            Expr::IsNull(inner) => {
                // Try as expression first; if that fails (e.g., for `(a > 0) IS NULL`),
                // extract column references and convert to IS NULL on those columns.
                match self.expr_to_expression(inner, None) {
                    Ok(e) => Ok(Predicate::is_null(e)),
                    Err(_) => {
                        // Inner is a predicate-level expression like `a > 0`.
                        // `(a > 0) IS NULL` is true when the comparison result is null,
                        // which happens iff any column operand is null (literals are never
                        // null). So `(a > 0) IS NULL` ≡ `a IS NULL`, and for compound
                        // predicates, OR together IS NULL for each referenced column.
                        let cols = Self::extract_column_refs(inner);
                        if cols.is_empty() {
                            return Err(ParseError::UnsupportedExpr(
                                "IS NULL on predicate with no column references".to_string(),
                            ));
                        }
                        let mut pred = Predicate::is_null(Expression::column(cols[0].clone()));
                        for col in &cols[1..] {
                            pred = Predicate::or(
                                pred,
                                Predicate::is_null(Expression::column(col.clone())),
                            );
                        }
                        Ok(pred)
                    }
                }
            }
            Expr::IsNotNull(inner) => {
                match self.expr_to_expression(inner, None) {
                    Ok(e) => Ok(Predicate::is_not_null(e)),
                    Err(_) => {
                        // `(a > 0) IS NOT NULL` ≡ `a IS NOT NULL` (for single-column
                        // comparisons against literals). For compound predicates, AND
                        // together IS NOT NULL for each referenced column.
                        let cols = Self::extract_column_refs(inner);
                        if cols.is_empty() {
                            return Err(ParseError::UnsupportedExpr(
                                "IS NOT NULL on predicate with no column references".to_string(),
                            ));
                        }
                        let mut pred =
                            Predicate::is_not_null(Expression::column(cols[0].clone()));
                        for col in &cols[1..] {
                            pred = Predicate::and(
                                pred,
                                Predicate::is_not_null(Expression::column(col.clone())),
                            );
                        }
                        Ok(pred)
                    }
                }
            }
            Expr::Nested(inner) => self.expr_to_predicate(inner),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                // Convert BETWEEN to: expr >= low AND expr <= high (or NOT of that)
                let col = self.extract_column_name(expr);
                let type_hint = col.as_ref().and_then(|c| self.find_column_type(c));

                let e = self.expr_to_expression(expr, type_hint)?;
                let l = self.expr_to_expression(low, type_hint)?;
                let h = self.expr_to_expression(high, type_hint)?;

                let between_pred =
                    Predicate::and(Predicate::ge(e.clone(), l), Predicate::le(e, h));

                if *negated {
                    Ok(Predicate::not(between_pred))
                } else {
                    Ok(between_pred)
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                // Convert IN to: expr = v1 OR expr = v2 OR ...
                let col = self.extract_column_name(expr);
                let type_hint = col.as_ref().and_then(|c| self.find_column_type(c));

                let e = self.expr_to_expression(expr, type_hint)?;

                if list.is_empty() {
                    // Empty IN list - always false (or true if negated)
                    return Ok(if *negated {
                        Predicate::from_expr(Expression::literal(Scalar::Boolean(true)))
                    } else {
                        Predicate::from_expr(Expression::literal(Scalar::Boolean(false)))
                    });
                }

                let mut predicates: Vec<Predicate> = Vec::new();
                for item in list {
                    let v = self.expr_to_expression(item, type_hint)?;
                    predicates.push(Predicate::eq(e.clone(), v));
                }

                // Combine with OR
                let mut result = predicates.remove(0);
                for p in predicates {
                    result = Predicate::or(result, p);
                }

                if *negated {
                    Ok(Predicate::not(result))
                } else {
                    Ok(result)
                }
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                // Handle LIKE with 'prefix%' patterns by converting to range comparison
                if let Expr::Value(Value::SingleQuotedString(pat)) = pattern.as_ref() {
                    if pat == "%" {
                        // col LIKE '%' matches everything non-null → IS NOT NULL
                        let e = self.expr_to_expression(expr, None)?;
                        let pred = Predicate::is_not_null(e);
                        return Ok(if *negated { Predicate::not(pred) } else { pred });
                    }
                    if pat.ends_with('%') && !pat[..pat.len()-1].contains('%') {
                        // Simple prefix pattern like 'abc%'
                        let prefix = &pat[..pat.len()-1];
                        let col = self.extract_column_name(expr);
                        let type_hint = col.as_ref().and_then(|c| self.find_column_type(c));
                        let e = self.expr_to_expression(expr, type_hint)?;

                        // Convert to: col >= 'prefix' AND col < 'prefiy'
                        // where 'prefiy' is prefix with last char incremented
                        let lower = Expression::literal(Scalar::String(prefix.to_string()));
                        let mut upper_chars: Vec<char> = prefix.chars().collect();
                        if let Some(last) = upper_chars.last_mut() {
                            *last = char::from_u32(*last as u32 + 1).unwrap_or(*last);
                        }
                        let upper_str: String = upper_chars.into_iter().collect();
                        let upper = Expression::literal(Scalar::String(upper_str));

                        let pred = Predicate::and(
                            Predicate::ge(e.clone(), lower),
                            Predicate::lt(e, upper),
                        );
                        return Ok(if *negated { Predicate::not(pred) } else { pred });
                    }
                }
                Err(ParseError::UnsupportedExpr(format!(
                    "LIKE operator not supported by kernel: {} LIKE {:?}",
                    expr, pattern
                )))
            }
            // Handle boolean literal values as predicates
            Expr::Value(Value::Boolean(b)) => {
                Ok(Predicate::from_expr(Expression::literal(Scalar::Boolean(*b))))
            }
            // Handle simple column reference as boolean
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let e = self.expr_to_expression(expr, Some(&DataType::BOOLEAN))?;
                Ok(Predicate::from_expr(e))
            }
            _ => Err(ParseError::UnsupportedExpr(format!("{:?}", expr))),
        }
    }

    /// Parse a predicate string
    pub fn parse(&self, input: &str) -> Result<Predicate, ParseError> {
        let dialect = GenericDialect {};
        // Wrap in SELECT WHERE to make it a valid SQL statement
        let sql = format!("SELECT * FROM t WHERE {}", input);
        let statements = Parser::parse_sql(&dialect, &sql)?;

        if statements.len() != 1 {
            return Err(ParseError::SqlParser("Expected single statement".to_string()));
        }

        // Extract the WHERE clause
        match &statements[0] {
            sqlparser::ast::Statement::Query(query) => {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(selection) = &select.selection {
                        return self.expr_to_predicate(selection);
                    }
                }
                Err(ParseError::SqlParser("No WHERE clause found".to_string()))
            }
            _ => Err(ParseError::SqlParser(
                "Expected SELECT statement".to_string(),
            )),
        }
    }
}

/// Parse a SQL-like predicate string into a kernel Predicate (without schema)
pub fn parse_predicate(input: &str) -> Result<Predicate, ParseError> {
    let parser = SchemaAwareParser::new(None);
    parser.parse(input)
}

/// Parse a SQL-like predicate string with schema for type coercion
pub fn parse_predicate_with_schema(input: &str, schema: &Schema) -> Result<Predicate, ParseError> {
    let parser = SchemaAwareParser::new(Some(schema));
    parser.parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::StructField;

    #[test]
    fn test_simple_equality() {
        let pred = parse_predicate("id = 2").unwrap();
        assert!(matches!(pred, Predicate::Binary(_)));
    }

    #[test]
    fn test_compound_predicate() {
        let pred = parse_predicate("long_col >= 900 AND long_col < 950").unwrap();
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_is_null() {
        let pred = parse_predicate("col IS NULL").unwrap();
        assert!(matches!(pred, Predicate::Unary(_)));
    }

    #[test]
    fn test_is_not_null() {
        let pred = parse_predicate("col IS NOT NULL").unwrap();
        assert!(matches!(pred, Predicate::Not(_)));
    }

    #[test]
    fn test_string_literal() {
        let pred = parse_predicate("name = 'alice'").unwrap();
        assert!(matches!(pred, Predicate::Binary(_)));
    }

    #[test]
    fn test_nested_column() {
        let pred = parse_predicate("data.category = 'test'").unwrap();
        assert!(matches!(pred, Predicate::Binary(_)));
    }

    #[test]
    fn test_or_predicate() {
        let pred = parse_predicate("a = 1 OR b = 2").unwrap();
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_parentheses() {
        let pred = parse_predicate("(a = 1 OR b = 2) AND c = 3").unwrap();
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_not_predicate() {
        let pred = parse_predicate("NOT a = 1").unwrap();
        assert!(matches!(pred, Predicate::Not(_)));
    }

    #[test]
    fn test_between() {
        let pred = parse_predicate("x BETWEEN 0 AND 100").unwrap();
        // BETWEEN becomes AND of two comparisons
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_in_list() {
        let pred = parse_predicate("x IN (1, 2, 3)").unwrap();
        // IN becomes OR of equalities
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_schema_aware_int32() {
        let schema = StructType::try_new(vec![
            StructField::new("int_col", DataType::INTEGER, true)
        ]).unwrap();

        let pred = parse_predicate_with_schema("int_col = 42", &schema).unwrap();
        // Should create Int32 literal, not Int64
        if let Predicate::Binary(bin) = pred {
            if let Expression::Literal(scalar) = bin.right.as_ref() {
                assert!(matches!(scalar, Scalar::Integer(42)));
            } else {
                panic!("Expected literal on right side");
            }
        } else {
            panic!("Expected binary predicate");
        }
    }
}
