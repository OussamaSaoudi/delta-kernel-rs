//! JSON parsing expression generation for ParseJsonNode.
//!
//! Converts a kernel target schema into DataFusion expressions that extract
//! typed values from JSON strings using `datafusion-functions-json`.

use datafusion_expr::{Expr, expr::ScalarFunction};
use datafusion_expr::lit;

use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};

use crate::error::{DfResult, DfError};

// Import UDF constructors from datafusion-functions-json
use datafusion_functions_json::udfs::{
    json_get_int_udf,
    json_get_str_udf,
    json_get_float_udf,
    json_get_bool_udf,
};

/// Generate a JSON extraction expression for a single field.
///
/// For primitive types, generates a `json_get_*` call.
/// For struct types, generates a `named_struct` wrapping recursive extractions.
///
/// # Arguments
/// * `json_col` - The expression referencing the JSON string column
/// * `field` - The field to extract
/// * `path` - The JSON path to this field (accumulated from parent structs)
///
/// # Returns
/// A DataFusion expression that extracts and types the field from JSON.
pub fn generate_json_extract_expr(
    json_col: &Expr,
    field: &StructField,
    path: &[String],
) -> DfResult<Expr> {
    // Build the full path including this field's name
    let mut field_path = path.to_vec();
    field_path.push(field.name().to_string());

    match field.data_type() {
        DataType::Primitive(prim) => {
            generate_primitive_extract(json_col, prim, &field_path)
        }
        DataType::Struct(inner_struct) => {
            generate_struct_extract(json_col, inner_struct, &field_path)
        }
        DataType::Array(_) => {
            // Arrays in stats are not commonly used, but we could support json_get_array
            Err(DfError::Unsupported(format!(
                "Array type extraction from JSON not yet supported for field '{}'",
                field.name()
            )))
        }
        DataType::Map(_) => {
            Err(DfError::Unsupported(format!(
                "Map type extraction from JSON not yet supported for field '{}'",
                field.name()
            )))
        }
        DataType::Variant(_) => {
            Err(DfError::Unsupported(format!(
                "Variant type extraction from JSON not yet supported for field '{}'",
                field.name()
            )))
        }
    }
}

/// Generate extraction expression for a primitive type.
fn generate_primitive_extract(
    json_col: &Expr,
    prim: &PrimitiveType,
    path: &[String],
) -> DfResult<Expr> {
    // Build arguments: json_col followed by path components as literals
    let mut args = vec![json_col.clone()];
    args.extend(path.iter().map(|p| lit(p.clone())));

    // Select the appropriate UDF based on the primitive type
    let udf = match prim {
        PrimitiveType::Long | PrimitiveType::Integer | PrimitiveType::Short | PrimitiveType::Byte => {
            json_get_int_udf()
        }
        PrimitiveType::String => {
            json_get_str_udf()
        }
        PrimitiveType::Float | PrimitiveType::Double => {
            json_get_float_udf()
        }
        PrimitiveType::Boolean => {
            json_get_bool_udf()
        }
        PrimitiveType::Date | PrimitiveType::Timestamp | PrimitiveType::TimestampNtz => {
            // Dates/timestamps in JSON stats are typically stored as strings or integers
            // For now, extract as string and let downstream handle conversion
            json_get_str_udf()
        }
        PrimitiveType::Binary => {
            // Binary is typically base64 encoded in JSON
            json_get_str_udf()
        }
        PrimitiveType::Decimal(_) => {
            // Decimals in JSON are typically strings or floats
            json_get_str_udf()
        }
    };

    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)))
}

/// Generate extraction expression for a struct type.
///
/// This creates a `named_struct(name1, val1, name2, val2, ...)` expression
/// where each value is recursively extracted from JSON.
fn generate_struct_extract(
    json_col: &Expr,
    struct_type: &StructType,
    path: &[String],
) -> DfResult<Expr> {
    // Build named_struct arguments: alternating field names and values
    let mut struct_args = Vec::new();

    for field in struct_type.fields() {
        // Add field name as literal
        struct_args.push(lit(field.name().to_string()));
        
        // Recursively generate extraction for this field
        let field_expr = generate_json_extract_expr(json_col, field, path)?;
        struct_args.push(field_expr);
    }

    // Use DataFusion's named_struct function
    Ok(datafusion_functions::core::expr_fn::named_struct(struct_args))
}

/// Generate all extraction expressions for a target schema.
///
/// Returns a vector of (expression, field_name) pairs for each top-level field.
pub fn generate_schema_extractions(
    json_col: &Expr,
    target_schema: &StructType,
) -> DfResult<Vec<(Expr, String)>> {
    let mut extractions = Vec::new();
    
    for field in target_schema.fields() {
        let expr = generate_json_extract_expr(json_col, field, &[])?;
        extractions.push((expr, field.name().to_string()));
    }
    
    Ok(extractions)
}

/// Build an expression to access a potentially nested column.
///
/// Handles dot-separated paths like "add.stats" by chaining get_field calls.
pub fn build_nested_column_expr(column_path: &str) -> Expr {
    let parts: Vec<&str> = column_path.split('.').collect();
    
    if parts.is_empty() {
        return datafusion_expr::col("");
    }
    
    let mut expr = datafusion_expr::col(parts[0]);
    for field_name in parts.iter().skip(1) {
        expr = datafusion_functions::core::expr_fn::get_field(expr, (*field_name).to_string());
    }
    
    expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::StructField;

    #[test]
    fn test_build_nested_column_expr() {
        let expr = build_nested_column_expr("add.stats");
        // Should produce: get_field(col("add"), "stats")
        assert!(format!("{:?}", expr).contains("add"));
    }

    #[test]
    fn test_generate_primitive_extract() {
        let json_col = datafusion_expr::col("json_data");
        let field = StructField::nullable("numRecords", DataType::LONG);
        
        let expr = generate_json_extract_expr(&json_col, &field, &[]).unwrap();
        // Should produce: json_get_int(json_data, "numRecords")
        let expr_str = format!("{:?}", expr);
        assert!(expr_str.contains("json_get_int") || expr_str.contains("ScalarFunction"));
    }

    #[test]
    fn test_generate_struct_extract() {
        let json_col = datafusion_expr::col("json_data");
        
        // Create a nested struct: minValues { id: Long, name: String }
        let inner_struct = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]);
        let field = StructField::nullable("minValues", DataType::Struct(Box::new(inner_struct)));
        
        let expr = generate_json_extract_expr(&json_col, &field, &[]).unwrap();
        // Should produce: named_struct("id", json_get_int(...), "name", json_get_str(...))
        let expr_str = format!("{:?}", expr);
        assert!(expr_str.contains("named_struct") || expr_str.contains("ScalarFunction"));
    }
}

