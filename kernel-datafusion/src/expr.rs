//! Expression lowering: Kernel Expression/Predicate -> DataFusion Expr.

use datafusion_expr::{Expr, Operator, lit};
use datafusion_common::{ScalarValue, Column};

use delta_kernel::expressions::{
    Expression, Predicate, Scalar,
    UnaryPredicateOp, BinaryPredicateOp, JunctionPredicateOp,
    UnaryExpressionOp, BinaryExpressionOp, VariadicExpressionOp,
    ColumnName, Transform, ExpressionRef,
};
use delta_kernel::schema::{DataType, StructType};

use crate::error::{DfResult, DfError};

/// Lower a kernel Expression to a DataFusion Expr.
///
/// Parameters:
/// - `expr`: The kernel expression to lower
/// - `output_type`: Optional output type for expressions that need schema context (Struct, Transform)
/// - `input_schema`: Optional input schema for Transform expressions to iterate over input fields
///
/// For simple expressions like literals, columns, and predicates, both optional params can be `None`.
pub fn lower_expression(
    expr: &Expression,
    output_type: Option<&DataType>,
    input_schema: Option<&StructType>,
) -> DfResult<Expr> {
    match expr {
        Expression::Literal(scalar) => lower_scalar(scalar),
        Expression::Column(col_name) => Ok(lower_column(col_name)),
        Expression::Predicate(pred) => {
            // Predicate treated as boolean expression
            lower_predicate(pred)
        }
        Expression::Struct(fields) => {
            lower_struct_expression(fields, output_type, input_schema)
        }
        Expression::Transform(transform) => {
            lower_transform_expression(transform, output_type, input_schema)
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
            lower_expression(expr, None, None)
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

/// Lower a Struct expression to a DataFusion named_struct call.
///
/// If an output schema is provided, field names and types come from it.
/// Otherwise, we generate default field names (c0, c1, ...).
fn lower_struct_expression(
    fields: &[ExpressionRef],
    output_type: Option<&DataType>,
    input_schema: Option<&StructType>,
) -> DfResult<Expr> {
    // Build named_struct arguments: alternating [name, value, name, value, ...]
    let mut struct_args = Vec::with_capacity(fields.len() * 2);

    match output_type {
        Some(DataType::Struct(struct_type)) => {
            // We have a struct schema - use it for field names and types
            if fields.len() != struct_type.fields().count() {
                return Err(DfError::ExpressionLowering(format!(
                    "Struct expression has {} fields but output schema has {} fields",
                    fields.len(),
                    struct_type.fields().count()
                )));
            }

            for (field_expr, schema_field) in fields.iter().zip(struct_type.fields()) {
                // Add field name as literal string
                struct_args.push(lit(schema_field.name().to_string()));

                // Recursively lower the field expression with its expected type
                // Pass input_schema through for nested transforms
                let lowered = lower_expression(
                    field_expr.as_ref(),
                    Some(schema_field.data_type()),
                    input_schema,
                )?;
                struct_args.push(lowered);
            }
        }
        _ => {
            // No struct schema available - use generated field names and no type hints
            for (i, field_expr) in fields.iter().enumerate() {
                struct_args.push(lit(format!("c{}", i)));
                let lowered = lower_expression(field_expr.as_ref(), None, input_schema)?;
                struct_args.push(lowered);
            }
        }
    }

    // Use DataFusion's named_struct function
    Ok(datafusion_functions::core::expr_fn::named_struct(struct_args))
}

/// Lower a Transform expression to a DataFusion expression.
///
/// Transform expressions represent sparse modifications to struct schemas.
/// They support:
/// - `input_path`: Optional path to a nested struct to transform
/// - `prepended_fields`: Fields to emit before the first input field
/// - `field_transforms`: Per-field modifications (drop, replace, insert after)
///
/// The transform is converted to an equivalent named_struct expression by iterating
/// over the input schema fields and applying transformations.
fn lower_transform_expression(
    transform: &Transform,
    output_type: Option<&DataType>,
    input_schema: Option<&StructType>,
) -> DfResult<Expr> {
    // Require struct output type for field names
    let output_struct = match output_type {
        Some(DataType::Struct(st)) => st,
        Some(other) => {
            return Err(DfError::ExpressionLowering(format!(
                "Transform expression requires DataType::Struct output, got {:?}",
                other
            )));
        }
        None => {
            return Err(DfError::ExpressionLowering(
                "Transform expression requires output_type to determine output schema".to_string()
            ));
        }
    };

    // For identity transforms, just build a simple projection
    if transform.is_identity() {
        return lower_identity_transform(transform, output_struct);
    }

    // Determine the input struct to iterate over
    // If input_path is set, we need to find that nested struct in the input schema
    // Otherwise, we use the top-level input schema
    let source_struct: Option<&StructType> = match (&transform.input_path, input_schema) {
        (None, schema) => schema,
        (Some(path), Some(schema)) => {
            // Navigate to the nested struct
            let mut current: &StructType = schema;
            for field_name in path.as_ref().iter() {
                match current.field(field_name) {
                    Some(field) => match field.data_type() {
                        DataType::Struct(nested) => current = nested,
                        _ => return Err(DfError::ExpressionLowering(format!(
                            "Transform input_path '{}' does not lead to a struct",
                            path
                        ))),
                    },
                    None => return Err(DfError::ExpressionLowering(format!(
                        "Transform input_path field '{}' not found in schema",
                        field_name
                    ))),
                }
            }
            Some(current)
        }
        (Some(_), None) => None,
    };

    // Helper to get the base expression for accessing source fields
    let source_base: Option<Expr> = transform.input_path.as_ref().map(|path| lower_column(path));

    // Build the result by iterating over the OUTPUT schema fields
    // We track an iterator over output field types for type context
    let mut output_field_iter = output_struct.fields();
    let mut struct_args = Vec::new();

    // 1. Emit prepended_fields first
    for prepend_expr in &transform.prepended_fields {
        let output_field = output_field_iter.next().ok_or_else(|| {
            DfError::ExpressionLowering("Output schema has fewer fields than transform produces".to_string())
        })?;
        
        struct_args.push(lit(output_field.name().to_string()));
        let lowered = lower_expression(prepend_expr.as_ref(), Some(output_field.data_type()), input_schema)?;
        struct_args.push(lowered);
    }

    // 2. Iterate over INPUT schema fields and apply transforms
    if let Some(input_struct) = source_struct {
        let field_transforms = &transform.field_transforms;

        for input_field in input_struct.fields() {
            let field_name = input_field.name();
            let ft = field_transforms.get(field_name);

            // Check if this field is dropped (is_replace=true with empty exprs)
            if ft.is_some_and(|t| t.is_replace && t.exprs.is_empty()) {
                // Field is dropped - don't emit anything, don't consume output field
                continue;
            }

            // Check if this field is replaced (is_replace=true with exprs)
            if let Some(t) = ft {
                if t.is_replace && !t.exprs.is_empty() {
                    // Field is replaced - emit replacement expressions
                    for replace_expr in &t.exprs {
                        let output_field = output_field_iter.next().ok_or_else(|| {
                            DfError::ExpressionLowering("Output schema has fewer fields than transform produces".to_string())
                        })?;
                        
                        struct_args.push(lit(output_field.name().to_string()));
                        let lowered = lower_expression(replace_expr.as_ref(), Some(output_field.data_type()), input_schema)?;
                        struct_args.push(lowered);
                    }
                    continue;
                }
            }

            // Field passes through unchanged
            let output_field = output_field_iter.next().ok_or_else(|| {
                DfError::ExpressionLowering("Output schema has fewer fields than transform produces".to_string())
            })?;

            struct_args.push(lit(output_field.name().to_string()));
            let source_expr = match &source_base {
                Some(base) => {
                    datafusion_functions::core::expr_fn::get_field(base.clone(), field_name.to_string())
                }
                None => {
                    Expr::Column(Column::new(None::<String>, field_name.to_string()))
                }
            };
            struct_args.push(source_expr);

            // Check if there are insertions after this field (is_replace=false with exprs)
            if let Some(t) = ft {
                if !t.is_replace {
                    for insert_expr in &t.exprs {
                        let output_field = output_field_iter.next().ok_or_else(|| {
                            DfError::ExpressionLowering("Output schema has fewer fields than transform produces".to_string())
                        })?;
                        
                        struct_args.push(lit(output_field.name().to_string()));
                        let lowered = lower_expression(insert_expr.as_ref(), Some(output_field.data_type()), input_schema)?;
                        struct_args.push(lowered);
                    }
                }
            }
        }
    } else {
        // No input schema available - fall back to output-schema-based iteration
        // This is the old behavior for backwards compatibility
        for field in output_field_iter {
            let field_name = field.name();
            struct_args.push(lit(field_name.to_string()));

            // Check if this is a transformed field
            if let Some(ft) = transform.field_transforms.get(field_name) {
                if ft.is_replace && !ft.exprs.is_empty() {
                    let expr = &ft.exprs[0];
                    let lowered = lower_expression(expr.as_ref(), Some(field.data_type()), None)?;
                    struct_args.push(lowered);
                    continue;
                }
            }

            // Pass-through field
            let source_expr = match &source_base {
                Some(base) => {
                    datafusion_functions::core::expr_fn::get_field(base.clone(), field_name.to_string())
                }
                None => {
                    Expr::Column(Column::new(None::<String>, field_name.to_string()))
                }
            };
            struct_args.push(source_expr);
        }
    }

    // Build the named_struct
    Ok(datafusion_functions::core::expr_fn::named_struct(struct_args))
}

/// Lower an identity transform (no modifications, just schema application).
fn lower_identity_transform(
    transform: &Transform,
    output_struct: &StructType,
) -> DfResult<Expr> {
    let source_base: Option<Expr> = transform.input_path.as_ref().map(|path| lower_column(path));

    let mut struct_args = Vec::new();

    for field in output_struct.fields() {
        let field_name = field.name();
        struct_args.push(lit(field_name.to_string()));

        let source_expr = match &source_base {
            Some(base) => {
                datafusion_functions::core::expr_fn::get_field(base.clone(), field_name.to_string())
            }
            None => {
                Expr::Column(Column::new(None::<String>, field_name.to_string()))
            }
        };
        struct_args.push(source_expr);
    }

    Ok(datafusion_functions::core::expr_fn::named_struct(struct_args))
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

/// Lower a kernel ColumnName to a DataFusion column reference expression.
/// Handles nested column paths like ["add", "path"] -> get_field(col("add"), "path").
///
/// Note: We use `Expr::Column(Column::new(...))` instead of `col()` because `col()` 
/// normalizes column names to lowercase, but Delta table stats use camelCase field names
/// (e.g., `nullCount`, `minValues`, `maxValues`).
pub fn lower_column(col_name: &ColumnName) -> Expr {
    // ColumnName can be nested (e.g. ["add", "path"] for accessing add.path)
    let path = col_name.as_ref();
    if path.is_empty() {
        // Shouldn't happen, but handle gracefully
        Expr::Column(Column::new(None::<String>, ""))
    } else if path.len() == 1 {
        // Simple column reference - preserve case exactly
        Expr::Column(Column::new(None::<String>, path[0].clone()))
    } else {
        // Nested column access: struct field access using get_field function
        // Example: ["add", "size"] -> get_field(col("add"), "size")
        // Start with the root column, then chain get_field for each nested field
        let mut expr = Expr::Column(Column::new(None::<String>, path[0].clone()));
        for field_name in path.iter().skip(1) {
            expr = datafusion_functions::core::expr_fn::get_field(expr, field_name.clone());
        }
        expr
    }
}

// Helper: convert kernel DataType to Arrow DataType
fn kernel_type_to_arrow(dt: &DataType) -> DfResult<arrow::datatypes::DataType> {
    use delta_kernel::schema::PrimitiveType;
    use arrow::datatypes::DataType as ArrowType;
    
    Ok(match dt {
        DataType::Primitive(prim) => match prim {
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
        DataType::Struct(_) | DataType::Array(_) | DataType::Map(_) | DataType::Variant(_) => {
            return Err(DfError::ExpressionLowering(
                format!("Complex type {:?} not yet supported in expression lowering", dt)
            ));
        }
    })
}

// Unary expression lowering
fn lower_unary_expression(op: UnaryExpressionOp, expr: &Expression) -> DfResult<Expr> {
    let _inner = lower_expression(expr, None, None)?;
    match op {
        UnaryExpressionOp::ToJson => {
            // TODO: Map to DataFusion's to_json function when available
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
    let left_expr = lower_expression(left, None, None)?;
    let right_expr = lower_expression(right, None, None)?;
    
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
            let args: Result<Vec<_>, _> = exprs.iter().map(|e| lower_expression(e, None, None)).collect();
            // Build nested CASE WHEN: COALESCE(a, b, c) -> CASE WHEN a IS NOT NULL THEN a ... ELSE c
            let args = args?;
            if args.is_empty() {
                return Err(DfError::ExpressionLowering("COALESCE with no arguments".to_string()));
            }
            
            // Use recursive pattern: coalesce(a, b, c) = CASE WHEN a IS NOT NULL THEN a ELSE coalesce(b, c)
            let mut result = args.last().unwrap().clone();
            for arg in args.iter().rev().skip(1) {
                result = Expr::Case(datafusion_expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(Expr::IsNotNull(Box::new(arg.clone()))),
                        Box::new(arg.clone()),
                    )],
                    else_expr: Some(Box::new(result)),
                });
            }
            Ok(result)
        }
    }
}

// Unary predicate lowering
fn lower_unary_predicate(op: UnaryPredicateOp, expr: &Expression) -> DfResult<Expr> {
    let inner = lower_expression(expr, None, None)?;
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
    let left_expr = lower_expression(left, None, None)?;
    let right_expr = lower_expression(right, None, None)?;
    
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
            // Right should be an array literal
            match right {
                Expression::Literal(Scalar::Array(array_data)) => {
                    let list: Vec<Expr> = array_data.array_elements().iter()
                        .map(|s| lower_expression(&Expression::Literal(s.clone()), None, None))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Expr::InList(datafusion_expr::expr::InList {
                        expr: Box::new(left_expr),
                        list,
                        negated: false,
                    }))
                }
                _ => Err(DfError::ExpressionLowering(
                    "IN predicate right side must be array literal".to_string()
                ))
            }
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
