//! Expression lowering: Kernel Expression/Predicate -> DataFusion Expr.

use datafusion_expr::{Expr, Operator, lit, col};
use datafusion_common::ScalarValue;

use delta_kernel::expressions::{
    Expression, Predicate, Scalar,
    UnaryPredicateOp, BinaryPredicateOp, JunctionPredicateOp,
    UnaryExpressionOp, BinaryExpressionOp, VariadicExpressionOp,
    ColumnName,
};

use crate::error::{DfResult, DfError};

/// Lower a kernel Expression to a DataFusion Expr.
pub fn lower_expression(expr: &Expression) -> DfResult<Expr> {
    match expr {
        Expression::Literal(scalar) => lower_scalar(scalar),
        Expression::Column(col_name) => Ok(lower_column(col_name)),
        Expression::Predicate(pred) => {
            // Predicate treated as boolean expression
            lower_predicate(pred)
        }
        Expression::Struct(fields) => {
            // Struct expression - not commonly used in filters/projections at this level
            // For now, we'll treat as unsupported
            Err(DfError::Unsupported(
                "Struct expression lowering not yet implemented".to_string()
            ))
        }
        Expression::Transform(_) => {
            Err(DfError::Unsupported(
                "Transform expression lowering not yet implemented".to_string()
            ))
        }
        Expression::Unary(unary) => {
            lower_unary_expression(unary.op, &unary.expr)
        }
        Expression::Binary(binary) => {
            lower_binary_expression(binary.op, &binary.left, &binary.right)
        }
        Expression::Variadic(variadic) => {
            lower_variadic_expression(variadic.op, &variadic.exprs)
        }
        Expression::Opaque(opaque) => {
            // Opaque ops are DataFusion-known operations - lower as function calls
            let fn_name = opaque.op.name();
            // For now, error out - need UDF registry integration
            Err(DfError::ExpressionLowering(
                format!("Opaque expression '{}' not yet supported - need UDF registry", fn_name)
            ))
        }
        Expression::Unknown(name) => {
            Err(DfError::ExpressionLowering(
                format!("Unknown expression '{}' cannot be lowered", name)
            ))
        }
    }
}

/// Lower a kernel Predicate to a DataFusion boolean Expr.
pub fn lower_predicate(pred: &Predicate) -> DfResult<Expr> {
    match pred {
        Predicate::BooleanExpression(expr) => {
            lower_expression(expr)
        }
        Predicate::Not(inner) => {
            let inner_expr = lower_predicate(inner)?;
            Ok(Expr::Not(Box::new(inner_expr)))
        }
        Predicate::Unary(unary) => {
            lower_unary_predicate(unary.op, &unary.expr)
        }
        Predicate::Binary(binary) => {
            lower_binary_predicate(binary.op, &binary.left, &binary.right)
        }
        Predicate::Junction(junction) => {
            lower_junction_predicate(junction.op, &junction.preds)
        }
        Predicate::Opaque(opaque) => {
            // Similar to opaque expressions
            let fn_name = opaque.op.name();
            Err(DfError::ExpressionLowering(
                format!("Opaque predicate '{}' not yet supported - need UDF registry", fn_name)
            ))
        }
        Predicate::Unknown(name) => {
            Err(DfError::ExpressionLowering(
                format!("Unknown predicate '{}' cannot be lowered", name)
            ))
        }
    }
}

// Helper: lower kernel Scalar to DataFusion ScalarValue + lit()
fn lower_scalar(scalar: &Scalar) -> DfResult<Expr> {
    let value = match scalar {
        Scalar::Null(dt) => {
            // Convert kernel DataType to Arrow DataType
            let arrow_type = kernel_type_to_arrow(dt)?;
            ScalarValue::try_from(&arrow_type)?
        }
        Scalar::Boolean(b) => ScalarValue::Boolean(Some(*b)),
        Scalar::Byte(i) => ScalarValue::Int8(Some(*i)),
        Scalar::Short(i) => ScalarValue::Int16(Some(*i)),
        Scalar::Integer(i) => ScalarValue::Int32(Some(*i)),
        Scalar::Long(i) => ScalarValue::Int64(Some(*i)),
        Scalar::Float(f) => ScalarValue::Float32(Some(*f)),
        Scalar::Double(f) => ScalarValue::Float64(Some(*f)),
        Scalar::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Scalar::Binary(b) => ScalarValue::Binary(Some(b.clone())),
        Scalar::Decimal(d) => {
            ScalarValue::Decimal128(
                Some(d.bits()),
                d.precision(),
                d.scale() as i8,
            )
        }
        Scalar::Date(days) => {
            ScalarValue::Date32(Some(*days))
        }
        Scalar::Timestamp(micros) => {
            ScalarValue::TimestampMicrosecond(Some(*micros), None)
        }
        Scalar::TimestampNtz(micros) => {
            ScalarValue::TimestampMicrosecond(Some(*micros), None)
        }
        Scalar::Struct(_) | Scalar::Array(_) | Scalar::Map(_) => {
            return Err(DfError::ExpressionLowering(
                "Complex scalar types (struct/array/map) not yet supported".to_string()
            ));
        }
    };
    Ok(lit(value))
}

// Helper: lower kernel ColumnName to DataFusion column reference
fn lower_column(col_name: &ColumnName) -> Expr {
    // ColumnName can be nested (e.g. "add.path")
    let path = col_name.as_ref();
    if path.len() == 1 {
        col(&path[0])
    } else {
        // Nested column access: use compound identifier
        Expr::Column(datafusion_common::Column::from_qualified_name(
            path.join(".")
        ))
    }
}

// Helper: convert kernel DataType to Arrow DataType
fn kernel_type_to_arrow(dt: &delta_kernel::schema::DataType) -> DfResult<arrow::datatypes::DataType> {
    use delta_kernel::schema::{DataType as KernelType, PrimitiveType};
    use arrow::datatypes::DataType as ArrowType;
    
    Ok(match dt {
        KernelType::Primitive(prim) => match prim {
            PrimitiveType::Boolean => ArrowType::Boolean,
            PrimitiveType::Byte => ArrowType::Int8,
            PrimitiveType::Short => ArrowType::Int16,
            PrimitiveType::Integer => ArrowType::Int32,
            PrimitiveType::Long => ArrowType::Int64,
            PrimitiveType::Float => ArrowType::Float32,
            PrimitiveType::Double => ArrowType::Float64,
            PrimitiveType::String => ArrowType::Utf8,
            PrimitiveType::Binary => ArrowType::Binary,
            PrimitiveType::Date => ArrowType::Date32,
            PrimitiveType::Timestamp => ArrowType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            PrimitiveType::TimestampNtz => ArrowType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            PrimitiveType::Decimal(dec_type) => {
                ArrowType::Decimal128(dec_type.precision(), dec_type.scale() as i8)
            }
        },
        KernelType::Struct(_) | KernelType::Array(_) | KernelType::Map(_) | KernelType::Variant(_) => {
            return Err(DfError::ExpressionLowering(
                format!("Complex type {:?} not yet supported in expression lowering", dt)
            ));
        }
    })
}

// Unary expression lowering
fn lower_unary_expression(op: UnaryExpressionOp, expr: &Expression) -> DfResult<Expr> {
    let inner = lower_expression(expr)?;
    match op {
        UnaryExpressionOp::ToJson => {
            // Map to DataFusion's to_json function if available
            Err(DfError::Unsupported("ToJson unary op not yet implemented".to_string()))
        }
    }
}

// Binary expression lowering
fn lower_binary_expression(
    op: BinaryExpressionOp,
    left: &Expression,
    right: &Expression,
) -> DfResult<Expr> {
    let left_expr = lower_expression(left)?;
    let right_expr = lower_expression(right)?;
    
    let df_op = match op {
        BinaryExpressionOp::Plus => Operator::Plus,
        BinaryExpressionOp::Minus => Operator::Minus,
        BinaryExpressionOp::Multiply => Operator::Multiply,
        BinaryExpressionOp::Divide => Operator::Divide,
    };
    
    Ok(Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
        Box::new(left_expr),
        df_op,
        Box::new(right_expr),
    )))
}

// Variadic expression lowering
fn lower_variadic_expression(
    op: VariadicExpressionOp,
    exprs: &[Expression],
) -> DfResult<Expr> {
    match op {
        VariadicExpressionOp::Coalesce => {
            let args: Result<Vec<_>, _> = exprs.iter().map(lower_expression).collect();
            // DataFusion doesn't have Expr::coalesce, use case expression
            // COALESCE(a, b, c) = CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ELSE c END
            // For simplicity, error out for now - will implement proper lowering later
            Err(DfError::Unsupported("COALESCE lowering not yet implemented".to_string()))
        }
    }
}

// Unary predicate lowering
fn lower_unary_predicate(op: UnaryPredicateOp, expr: &Expression) -> DfResult<Expr> {
    let inner = lower_expression(expr)?;
    match op {
        UnaryPredicateOp::IsNull => Ok(Expr::IsNull(Box::new(inner))),
    }
}

// Binary predicate lowering
fn lower_binary_predicate(
    op: BinaryPredicateOp,
    left: &Expression,
    right: &Expression,
) -> DfResult<Expr> {
    let left_expr = lower_expression(left)?;
    let right_expr = lower_expression(right)?;
    
    match op {
        BinaryPredicateOp::Equal => {
            Ok(Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
                Box::new(left_expr),
                Operator::Eq,
                Box::new(right_expr),
            )))
        }
        BinaryPredicateOp::LessThan => {
            Ok(Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
                Box::new(left_expr),
                Operator::Lt,
                Box::new(right_expr),
            )))
        }
        BinaryPredicateOp::GreaterThan => {
            Ok(Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
                Box::new(left_expr),
                Operator::Gt,
                Box::new(right_expr),
            )))
        }
        BinaryPredicateOp::Distinct => {
            // DISTINCT(a, b) in Delta means "a IS DISTINCT FROM b"
            // In DataFusion this is: (a != b) OR (a IS NULL != b IS NULL)
            // Or we can use IsDistinctFrom operator if available
            Ok(Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
                Box::new(left_expr),
                Operator::IsDistinctFrom,
                Box::new(right_expr),
            )))
        }
        BinaryPredicateOp::In => {
            // IN operator: left IN (right)
            // Right should be a list/array - for now assume it's an array literal
            // This is tricky; DataFusion InList expects Vec<Expr>
            // For simplicity, error out for now
            Err(DfError::ExpressionLowering(
                "IN predicate lowering not yet fully implemented".to_string()
            ))
        }
    }
}

// Junction predicate lowering (AND/OR)
fn lower_junction_predicate(
    op: JunctionPredicateOp,
    preds: &[Predicate],
) -> DfResult<Expr> {
    if preds.is_empty() {
        return Err(DfError::ExpressionLowering(
            "Junction with no predicates".to_string()
        ));
    }
    
    let mut exprs: Vec<Expr> = preds.iter()
        .map(lower_predicate)
        .collect::<Result<Vec<_>, _>>()?;
    
    if exprs.len() == 1 {
        return Ok(exprs.pop().unwrap());
    }
    
    // Build binary tree of AND/OR
    let df_op = match op {
        JunctionPredicateOp::And => Operator::And,
        JunctionPredicateOp::Or => Operator::Or,
    };
    
    let mut result = exprs.pop().unwrap();
    while let Some(expr) = exprs.pop() {
        result = Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
            Box::new(expr),
            df_op,
            Box::new(result),
        ));
    }
    
    Ok(result)
}
