//! SQL predicate parser for acceptance workload test predicates.
//!
//! Uses sqlparser crate to parse SQL WHERE clause expressions and converts them
//! to kernel [`Predicate`] types with automatic type coercion based on schema.

use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};
use delta_kernel::schema::{DataType, PrimitiveType, Schema, StructType};
use sqlparser::ast::{
    BinaryOperator, CastKind, DataType as SqlDataType, ExactNumberInfo, Expr, FunctionArg,
    FunctionArgExpr, FunctionArguments, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::opaque_ops;

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

    /// Convert a SQL data type to a kernel DataType
    fn sql_type_to_kernel_type(sql_type: &SqlDataType) -> Option<DataType> {
        match sql_type {
            SqlDataType::TinyInt(_) => Some(DataType::Primitive(PrimitiveType::Byte)),
            SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => {
                Some(DataType::Primitive(PrimitiveType::Short))
            }
            SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => {
                Some(DataType::Primitive(PrimitiveType::Integer))
            }
            SqlDataType::BigInt(_) | SqlDataType::Int8(_) => {
                Some(DataType::Primitive(PrimitiveType::Long))
            }
            SqlDataType::Float(_) | SqlDataType::Real => {
                Some(DataType::Primitive(PrimitiveType::Float))
            }
            SqlDataType::Double | SqlDataType::DoublePrecision => {
                Some(DataType::Primitive(PrimitiveType::Double))
            }
            SqlDataType::Boolean => Some(DataType::Primitive(PrimitiveType::Boolean)),
            SqlDataType::Date => Some(DataType::Primitive(PrimitiveType::Date)),
            SqlDataType::Timestamp(_, tz) => {
                // WITH TIME ZONE -> Timestamp, WITHOUT -> TimestampNtz
                match tz {
                    sqlparser::ast::TimezoneInfo::None
                    | sqlparser::ast::TimezoneInfo::WithoutTimeZone => {
                        Some(DataType::Primitive(PrimitiveType::TimestampNtz))
                    }
                    _ => Some(DataType::Primitive(PrimitiveType::Timestamp)),
                }
            }
            SqlDataType::String(_) | SqlDataType::Varchar(_) | SqlDataType::Text => {
                Some(DataType::Primitive(PrimitiveType::String))
            }
            SqlDataType::Binary(_) | SqlDataType::Varbinary(_) | SqlDataType::Blob(_) => {
                Some(DataType::Primitive(PrimitiveType::Binary))
            }
            SqlDataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, s)) => {
                Some(DataType::decimal(*p as u8, *s as u8).ok()?)
            }
            SqlDataType::Decimal(ExactNumberInfo::Precision(p)) => {
                Some(DataType::decimal(*p as u8, 0).ok()?)
            }
            SqlDataType::Decimal(ExactNumberInfo::None) => Some(DataType::decimal(10, 0).ok()?),
            _ => None,
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
                if let Some(DataType::Primitive(prim)) = target_type {
                    match prim {
                        PrimitiveType::Integer => {
                            let i: i32 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!(
                                    "Cannot parse '{}' as INTEGER",
                                    n
                                ))
                            })?;
                            return Ok(Scalar::Integer(i));
                        }
                        PrimitiveType::Short => {
                            let s: i16 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!("Cannot parse '{}' as SHORT", n))
                            })?;
                            return Ok(Scalar::Short(s));
                        }
                        PrimitiveType::Byte => {
                            let b: i8 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!("Cannot parse '{}' as BYTE", n))
                            })?;
                            return Ok(Scalar::Byte(b));
                        }
                        PrimitiveType::Long => {
                            let l: i64 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!("Cannot parse '{}' as LONG", n))
                            })?;
                            return Ok(Scalar::Long(l));
                        }
                        PrimitiveType::Float => {
                            let f: f32 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!("Cannot parse '{}' as FLOAT", n))
                            })?;
                            return Ok(Scalar::Float(f));
                        }
                        PrimitiveType::Double => {
                            let d: f64 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!(
                                    "Cannot parse '{}' as DOUBLE",
                                    n
                                ))
                            })?;
                            return Ok(Scalar::Double(d));
                        }
                        PrimitiveType::Decimal(decimal_type) => {
                            // Parse decimal - for now just parse as f64 and scale
                            let d: f64 = n.parse().map_err(|_| {
                                ParseError::InvalidLiteral(format!(
                                    "Cannot parse '{}' as DECIMAL",
                                    n
                                ))
                            })?;
                            let scale_factor = 10_i128.pow(decimal_type.scale() as u32);
                            let bits = (d * scale_factor as f64).round() as i128;
                            return Scalar::decimal(
                                bits,
                                decimal_type.precision(),
                                decimal_type.scale(),
                            )
                            .map_err(|e| {
                                ParseError::InvalidLiteral(format!("Invalid decimal: {}", e))
                            });
                        }
                        _ => {}
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
                if let Some(DataType::Primitive(prim)) = target_type {
                    match prim {
                        PrimitiveType::Date => {
                            // Parse date string like "2024-01-15"
                            let date =
                                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                                    ParseError::InvalidLiteral(format!(
                                        "Cannot parse '{}' as DATE",
                                        s
                                    ))
                                })?;
                            let days = date
                                .signed_duration_since(
                                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                                )
                                .num_days() as i32;
                            return Ok(Scalar::Date(days));
                        }
                        PrimitiveType::Timestamp | PrimitiveType::TimestampNtz => {
                            // Parse timestamp string
                            let ts =
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                    .or_else(|_| {
                                        chrono::NaiveDateTime::parse_from_str(
                                            s,
                                            "%Y-%m-%d %H:%M:%S",
                                        )
                                    })
                                    .or_else(|_| {
                                        chrono::NaiveDateTime::parse_from_str(
                                            s,
                                            "%Y-%m-%dT%H:%M:%S%.f",
                                        )
                                    })
                                    .or_else(|_| {
                                        chrono::NaiveDateTime::parse_from_str(
                                            s,
                                            "%Y-%m-%dT%H:%M:%S",
                                        )
                                    })
                                    .or_else(|_| {
                                        // Try date-only format (e.g. '2024-06-01' -> midnight)
                                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                                    })
                                    .map_err(|_| {
                                        ParseError::InvalidLiteral(format!(
                                            "Cannot parse '{}' as TIMESTAMP",
                                            s
                                        ))
                                    })?;
                            let micros = ts.and_utc().timestamp_micros();
                            if matches!(prim, PrimitiveType::TimestampNtz) {
                                return Ok(Scalar::TimestampNtz(micros));
                            }
                            return Ok(Scalar::Timestamp(micros));
                        }
                        _ => {}
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

    /// Extract column name from an expression (for type lookup).
    /// For function calls and casts, looks inside to find the first column reference.
    fn extract_column_name(expr: &Expr) -> Option<ColumnName> {
        match expr {
            Expr::Identifier(ident) => Some(ColumnName::new([ident.value.clone()])),
            Expr::CompoundIdentifier(parts) => {
                let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
                Some(ColumnName::new(names))
            }
            Expr::Nested(inner) => Self::extract_column_name(inner),
            Expr::Cast { expr: inner, .. } => Self::extract_column_name(inner),
            Expr::Function(func) => {
                // Look inside function args for the first column reference
                if let FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            if let Some(col) = Self::extract_column_name(e) {
                                return Some(col);
                            }
                        }
                    }
                }
                None
            }
            Expr::BinaryOp { left, right, .. } => {
                // For modulo etc., look in both sides
                Self::extract_column_name(left).or_else(|| Self::extract_column_name(right))
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
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Modulo,
                right,
            } => {
                let l = self.expr_to_expression(left, type_hint)?;
                let r = self.expr_to_expression(right, type_hint)?;
                Ok(opaque_ops::make_modulo_opaque(l, r))
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();

                // HEX('literal') — precompute to a literal
                if func_name == "hex" {
                    let args = extract_function_args(func)?;
                    if args.len() == 1 {
                        if let Expr::Value(Value::SingleQuotedString(s)) = &args[0] {
                            // HEX('1111') produces string "31313131" in Spark
                            let hex_str = hex::encode(s.as_bytes()).to_uppercase();
                            // If comparing against a binary column, produce binary
                            // literal (the UTF-8 bytes of the hex string) so
                            // Arrow can do Binary == Binary comparison
                            if let Some(dt) = type_hint {
                                if matches!(dt, DataType::Primitive(PrimitiveType::Binary)) {
                                    return Ok(Expression::literal(Scalar::Binary(
                                        hex_str.into_bytes(),
                                    )));
                                }
                            }
                            return Ok(Expression::literal(Scalar::String(hex_str)));
                        }
                    }
                    return Err(ParseError::UnsupportedExpr(
                        "HEX() with non-literal argument".to_string(),
                    ));
                }

                // All other supported functions → opaque expressions
                let raw_args = extract_function_args(func)?;
                let mut kernel_args = Vec::with_capacity(raw_args.len());
                for arg in &raw_args {
                    kernel_args.push(self.expr_to_expression(arg, None)?);
                }

                opaque_ops::make_function_opaque(&func_name, kernel_args)
                    .map_err(ParseError::UnsupportedExpr)
            }
            Expr::Cast {
                expr: inner,
                data_type: sql_type,
                kind,
                ..
            } => {
                // Only support standard CAST, not TRY_CAST or SAFE_CAST
                if !matches!(kind, CastKind::Cast) {
                    return Err(ParseError::UnsupportedExpr(format!(
                        "Unsupported cast kind: {:?}",
                        kind
                    )));
                }

                // Convert SQL type to kernel DataType
                let sql_target_type = Self::sql_type_to_kernel_type(sql_type).ok_or_else(|| {
                    ParseError::UnsupportedExpr(format!(
                        "Unsupported cast target type: {}",
                        sql_type
                    ))
                })?;

                // Prefer type_hint (from schema column) over SQL cast type.
                // This handles cases like comparing partition column Timestamp(UTC)
                // with CAST(literal AS TIMESTAMP) - we want to use the column's type.
                let effective_type = type_hint.unwrap_or(&sql_target_type);

                // If the inner expression is a literal, evaluate the cast at parse time
                if let Expr::Value(value) = inner.as_ref() {
                    let scalar = self.value_to_scalar(value, Some(effective_type))?;
                    return Ok(Expression::literal(scalar));
                }

                // If casting a column, just return the column expression.
                // The schema-aware type coercion will handle comparing against
                // the correct type when the comparison is built.
                self.expr_to_expression(inner, Some(effective_type))
            }
            _ => Err(ParseError::UnsupportedExpr(format!("{:?}", expr))),
        }
    }

    /// Convert a SQL expression to a kernel Predicate
    fn expr_to_predicate(&self, expr: &Expr) -> Result<Predicate, ParseError> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // For comparisons, try to infer type from column side
                let left_col = Self::extract_column_name(left);
                let right_col = Self::extract_column_name(right);

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
                        let mut pred = Predicate::is_not_null(Expression::column(cols[0].clone()));
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
                let col = Self::extract_column_name(expr);
                let type_hint = col.as_ref().and_then(|c| self.find_column_type(c));

                let e = self.expr_to_expression(expr, type_hint)?;
                let l = self.expr_to_expression(low, type_hint)?;
                let h = self.expr_to_expression(high, type_hint)?;

                let between_pred = Predicate::and(Predicate::ge(e.clone(), l), Predicate::le(e, h));

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
                let col = Self::extract_column_name(expr);
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
                    if pat.ends_with('%') && !pat[..pat.len() - 1].contains('%') {
                        // Simple prefix pattern like 'abc%'
                        let prefix = &pat[..pat.len() - 1];
                        let col = Self::extract_column_name(expr);
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
            Expr::Value(Value::Boolean(b)) => Ok(Predicate::from_expr(Expression::literal(
                Scalar::Boolean(*b),
            ))),
            // Handle simple column reference as boolean
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let e = self.expr_to_expression(expr, Some(&DataType::BOOLEAN))?;
                Ok(Predicate::from_expr(e))
            }
            _ => Err(ParseError::UnsupportedExpr(format!("{:?}", expr))),
        }
    }

    /// Preprocess predicate string to handle Spark-specific syntax that sqlparser can't parse.
    /// Converts `DATE'...'`, `TIMESTAMP_NTZ'...'` and `TIMESTAMP'...'` cast syntax to plain
    /// quoted strings, relying on schema-aware type coercion to produce the correct scalar type.
    fn preprocess(input: &str) -> String {
        let mut result = input.to_string();
        // Replace TIMESTAMP_NTZ'...' with just '...'
        while let Some(pos) = result.find("TIMESTAMP_NTZ'") {
            let after = pos + "TIMESTAMP_NTZ".len();
            result = format!("{}{}", &result[..pos], &result[after..]);
        }
        // Replace TIMESTAMP'...' with just '...' (but not TIMESTAMP_NTZ which was already handled)
        while let Some(pos) = result.find("TIMESTAMP'") {
            let after = pos + "TIMESTAMP".len();
            result = format!("{}{}", &result[..pos], &result[after..]);
        }
        // Replace DATE'...' with just '...'
        while let Some(pos) = result.find("DATE'") {
            let after = pos + "DATE".len();
            result = format!("{}{}", &result[..pos], &result[after..]);
        }
        result
    }

    /// Parse a predicate string
    pub fn parse(&self, input: &str) -> Result<Predicate, ParseError> {
        let dialect = GenericDialect {};
        // Preprocess to handle Spark-specific cast syntax
        let preprocessed = Self::preprocess(input);
        // Wrap in SELECT WHERE to make it a valid SQL statement
        let sql = format!("SELECT * FROM t WHERE {}", preprocessed);
        let statements = Parser::parse_sql(&dialect, &sql)?;

        if statements.len() != 1 {
            return Err(ParseError::SqlParser(
                "Expected single statement".to_string(),
            ));
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

/// Extract plain `Expr` arguments from a sqlparser `Function`.
fn extract_function_args(func: &sqlparser::ast::Function) -> Result<Vec<Expr>, ParseError> {
    match &func.args {
        FunctionArguments::List(arg_list) => {
            let mut exprs = Vec::new();
            for arg in &arg_list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => exprs.push(e.clone()),
                    _ => {
                        return Err(ParseError::UnsupportedExpr(format!(
                            "Unsupported function argument: {:?}",
                            arg
                        )));
                    }
                }
            }
            Ok(exprs)
        }
        FunctionArguments::None => Ok(Vec::new()),
        _ => Err(ParseError::UnsupportedExpr(
            "Unsupported function arguments form".to_string(),
        )),
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
        let schema =
            StructType::try_new(vec![StructField::new("int_col", DataType::INTEGER, true)])
                .unwrap();

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
