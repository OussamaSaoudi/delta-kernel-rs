//! Convert the kernel declarative-plan IR (`ResultPlan` and friends) into the generated prost
//! proto types (`super::proto`), for the DuckDB Phase-2 proto-over-FFI transport.
//!
//! Coverage (P2.M0 — metadata scan): full plan structure + schema + scalars + the expression /
//! predicate variants the scan-metadata `ResultPlan` uses (Column, Literal, Struct, Unary, Binary,
//! Variadic, ParseJson, MapToStruct, and the predicate kinds). Variants that the metadata plan
//! does not emit (`Transform`, `If`, `Opaque`, `Unknown`, and struct/array/map *literal* scalars)
//! return an error rather than silently degrading — matching the proto's "never NULL" philosophy.
//! These are the natural P2.M1 extension points.

use delta_kernel::expressions::{
    BinaryExpressionOp, BinaryPredicateOp, Expression, JunctionPredicateOp, Predicate, Scalar,
    UnaryExpressionOp, UnaryPredicateOp, VariadicExpressionOp,
};
use delta_kernel::plans::ir::nodes::{DvKind, FileType, JoinKind, NodeKind};
use delta_kernel::plans::ir::plan::ResultPlan;
use delta_kernel::schema::{DataType, MetadataValue, PrimitiveType, StructField, StructType};

use super::proto::{expressions as pexpr, plan as pplan, schema as pschema};

type R<T> = Result<T, String>;

// ============================================================================
// Top level
// ============================================================================

/// Convert a kernel [`ResultPlan`] into its proto representation.
pub fn result_plan_to_proto(rp: &ResultPlan) -> R<pplan::ResultPlan> {
    let nodes = rp
        .plan
        .nodes
        .iter()
        .map(plan_node_to_proto)
        .collect::<R<Vec<_>>>()?;
    Ok(pplan::ResultPlan {
        plan: Some(pplan::Plan { nodes }),
        result: rp.result.0,
    })
}

/// Convert a kernel schema (a top-level [`StructType`]) into its proto representation. A Delta
/// table schema is a struct of fields, so the proto transport is the same `StructType` message the
/// plan nodes already use. Backs `delta_snapshot_schema`.
pub fn schema_to_proto(schema: &StructType) -> R<pschema::StructType> {
    struct_type_to_proto(schema)
}

fn plan_node_to_proto(node: &delta_kernel::plans::ir::plan::PlanNode) -> R<pplan::PlanNode> {
    Ok(pplan::PlanNode {
        op: Some(pplan::Operator {
            op: Some(node_kind_to_proto(&node.kind)?),
        }),
        inputs: node.inputs.iter().map(|r| r.0).collect(),
        output: node.output.0,
    })
}

// ============================================================================
// Nodes
// ============================================================================

fn node_kind_to_proto(kind: &NodeKind) -> R<pplan::operator::Op> {
    use pplan::operator::Op;
    Ok(match kind {
        NodeKind::ListFiles(n) => Op::ListFiles(pplan::ListFilesNode {
            start_from: n.start_from.to_string(),
        }),
        NodeKind::ScanParquet(n) => Op::ScanParquet(pplan::ScanParquetNode {
            files: n.files.iter().map(file_meta_to_proto).collect(),
            schema: Some(struct_type_to_proto(&n.schema)?),
            predicate: n
                .predicate
                .as_ref()
                .map(|p| predicate_to_proto(p))
                .transpose()?,
        }),
        NodeKind::ScanJson(n) => Op::ScanJson(pplan::ScanJsonNode {
            files: n.files.iter().map(file_meta_to_proto).collect(),
            schema: Some(struct_type_to_proto(&n.schema)?),
            predicate: n
                .predicate
                .as_ref()
                .map(|p| predicate_to_proto(p))
                .transpose()?,
        }),
        NodeKind::Values(n) => Op::Values(pplan::ValuesNode {
            schema: Some(struct_type_to_proto(&n.schema)?),
            rows: n
                .rows
                .iter()
                .map(|row| {
                    Ok(pplan::ValuesRow {
                        values: row.iter().map(scalar_to_proto).collect::<R<Vec<_>>>()?,
                    })
                })
                .collect::<R<Vec<_>>>()?,
        }),
        NodeKind::Project(n) => Op::Project(pplan::ProjectNode {
            named_exprs: n
                .named_exprs
                .iter()
                .map(|(name, e)| {
                    Ok(pplan::NamedExpr {
                        name: name.clone(),
                        expr: Some(expression_to_proto(e)?),
                    })
                })
                .collect::<R<Vec<_>>>()?,
            output_schema: Some(struct_type_to_proto(&n.output_schema)?),
        }),
        NodeKind::Filter(n) => Op::Filter(pplan::FilterNode {
            predicate: Some(predicate_to_proto(&n.predicate)?),
        }),
        NodeKind::Load(n) => Op::Load(pplan::LoadNode {
            file_schema: Some(struct_type_to_proto(&n.file_schema)?),
            file_type: match n.file_type {
                FileType::Parquet => pplan::FileType::Parquet,
                FileType::Json => pplan::FileType::Json,
            } as i32,
            base_url: n.base_url.as_ref().map(|u| u.to_string()),
            metadata_derived_columns: n
                .metadata_derived_columns
                .iter()
                .map(column_name_to_proto)
                .collect(),
            file_meta: Some(pplan::LoadColumnInfo {
                path_column: Some(column_name_to_proto(&n.file_meta.path_column)),
                file_size_column: n
                    .file_meta
                    .file_size_column
                    .as_ref()
                    .map(column_name_to_proto),
                num_records_column: n
                    .file_meta
                    .num_records_column
                    .as_ref()
                    .map(column_name_to_proto),
            }),
            dv_ref: n.dv_ref.as_ref().map(|dv| pplan::DvRef {
                column: Some(column_name_to_proto(&dv.column)),
                kind: match dv.kind {
                    DvKind::Bytes => pplan::DvKind::Bytes,
                    DvKind::Descriptor => pplan::DvKind::Descriptor,
                } as i32,
            }),
        }),
        NodeKind::MaxByVersion(n) => Op::MaxByVersion(pplan::MaxByVersionNode {
            group_by: n
                .group_by
                .iter()
                .map(|e| expression_to_proto(e))
                .collect::<R<Vec<_>>>()?,
            version_column: Some(expression_to_proto(&n.version_column)?),
            output_schema: Some(struct_type_to_proto(&n.output_schema)?),
        }),
        NodeKind::EquiJoin(n) => Op::EquiJoin(pplan::EquiJoinNode {
            kind: match n.kind {
                JoinKind::LeftAnti => pplan::JoinKind::LeftAnti,
            } as i32,
            left_keys: n
                .left_keys
                .iter()
                .map(|e| expression_to_proto(e))
                .collect::<R<Vec<_>>>()?,
            right_keys: n
                .right_keys
                .iter()
                .map(|e| expression_to_proto(e))
                .collect::<R<Vec<_>>>()?,
        }),
        NodeKind::UnionAll(n) => Op::UnionAll(pplan::UnionAllNode { ordered: n.ordered }),
    })
}

fn file_meta_to_proto(meta: &delta_kernel::FileMeta) -> pplan::FileMeta {
    pplan::FileMeta {
        location: meta.location.to_string(),
        size: meta.size,
        last_modified: meta.last_modified,
    }
}

// ============================================================================
// Schema
// ============================================================================

fn struct_type_to_proto(st: &StructType) -> R<pschema::StructType> {
    Ok(pschema::StructType {
        fields: st
            .fields()
            .map(struct_field_to_proto)
            .collect::<R<Vec<_>>>()?,
    })
}

fn struct_field_to_proto(f: &StructField) -> R<pschema::StructField> {
    let metadata = f
        .metadata
        .iter()
        .map(|(k, v)| Ok((k.clone(), metadata_value_to_proto(v))))
        .collect::<R<std::collections::HashMap<_, _>>>()?;
    Ok(pschema::StructField {
        name: f.name().clone(),
        data_type: Some(data_type_to_proto(&f.data_type())?),
        nullable: f.is_nullable(),
        metadata,
    })
}

fn metadata_value_to_proto(v: &MetadataValue) -> pschema::MetadataValue {
    use pschema::metadata_value::Value;
    let value = match v {
        MetadataValue::Number(n) => Value::Number(*n),
        MetadataValue::String(s) => Value::String(s.clone()),
        MetadataValue::Boolean(b) => Value::Boolean(*b),
        MetadataValue::Other(j) => Value::OtherJson(j.to_string()),
    };
    pschema::MetadataValue { value: Some(value) }
}

fn data_type_to_proto(dt: &DataType) -> R<pschema::DataType> {
    use pschema::data_type::Kind;
    let kind = match dt {
        DataType::Primitive(p) => Kind::Primitive(primitive_type_to_proto(p)),
        DataType::Array(a) => Kind::Array(Box::new(pschema::ArrayType {
            element_type: Some(Box::new(data_type_to_proto(a.element_type())?)),
            contains_null: a.contains_null(),
        })),
        DataType::Struct(s) => Kind::Struct(struct_type_to_proto(s)?),
        DataType::Map(m) => Kind::Map(Box::new(pschema::MapType {
            key_type: Some(Box::new(data_type_to_proto(m.key_type())?)),
            value_type: Some(Box::new(data_type_to_proto(m.value_type())?)),
            value_contains_null: m.value_contains_null(),
        })),
        other => return Err(format!("unsupported DataType for proto: {other:?}")),
    };
    Ok(pschema::DataType { kind: Some(kind) })
}

fn primitive_type_to_proto(p: &PrimitiveType) -> pschema::PrimitiveType {
    use pschema::primitive_type::Kind;
    use pschema::SimplePrimitiveType as S;
    let kind = match p {
        PrimitiveType::String => Kind::Simple(S::String as i32),
        PrimitiveType::Long => Kind::Simple(S::Long as i32),
        PrimitiveType::Integer => Kind::Simple(S::Integer as i32),
        PrimitiveType::Short => Kind::Simple(S::Short as i32),
        PrimitiveType::Byte => Kind::Simple(S::Byte as i32),
        PrimitiveType::Float => Kind::Simple(S::Float as i32),
        PrimitiveType::Double => Kind::Simple(S::Double as i32),
        PrimitiveType::Boolean => Kind::Simple(S::Boolean as i32),
        PrimitiveType::Binary => Kind::Simple(S::Binary as i32),
        PrimitiveType::Date => Kind::Simple(S::Date as i32),
        PrimitiveType::Timestamp => Kind::Simple(S::Timestamp as i32),
        PrimitiveType::TimestampNtz => Kind::Simple(S::TimestampNtz as i32),
        PrimitiveType::Decimal(d) => Kind::Decimal(pschema::DecimalType {
            precision: d.precision() as u32,
            scale: d.scale() as u32,
        }),
    };
    pschema::PrimitiveType { kind: Some(kind) }
}

// ============================================================================
// Scalars
// ============================================================================

fn scalar_to_proto(s: &Scalar) -> R<pexpr::Scalar> {
    use pexpr::scalar::Value;
    let value = match s {
        Scalar::Integer(v) => Value::Integer(*v),
        Scalar::Long(v) => Value::Long(*v),
        Scalar::Short(v) => Value::Short(*v as i32),
        Scalar::Byte(v) => Value::Byte(*v as i32),
        Scalar::Float(v) => Value::Float(*v),
        Scalar::Double(v) => Value::Double(*v),
        Scalar::String(v) => Value::String(v.clone()),
        Scalar::Boolean(v) => Value::Boolean(*v),
        Scalar::Timestamp(v) => Value::Timestamp(*v),
        Scalar::TimestampNtz(v) => Value::TimestampNtz(*v),
        Scalar::Date(v) => Value::Date(*v),
        Scalar::Binary(v) => Value::Binary(v.clone()),
        Scalar::Decimal(d) => Value::Decimal(pexpr::DecimalData {
            bits: d.bits().to_be_bytes().to_vec(),
            decimal_type: Some(pschema::DecimalType {
                precision: d.precision() as u32,
                scale: d.scale() as u32,
            }),
        }),
        Scalar::Null(dt) => Value::Null(data_type_to_proto(dt)?),
        other => {
            return Err(format!(
                "unsupported scalar literal for proto (P2.M1): {other:?}"
            ))
        }
    };
    Ok(pexpr::Scalar { value: Some(value) })
}

fn column_name_to_proto(c: &delta_kernel::expressions::ColumnName) -> pexpr::ColumnName {
    pexpr::ColumnName {
        path: c.path().to_vec(),
    }
}

// ============================================================================
// Expressions / Predicates
// ============================================================================

fn expression_to_proto(e: &Expression) -> R<pexpr::Expression> {
    use pexpr::expression::Kind;
    let kind = match e {
        Expression::Literal(s) => Kind::Literal(scalar_to_proto(s)?),
        Expression::Column(c) => Kind::Column(column_name_to_proto(c)),
        Expression::Predicate(p) => Kind::Predicate(Box::new(predicate_to_proto(p)?)),
        Expression::Struct(exprs, nullability) => Kind::StructExpr(Box::new(pexpr::StructExpression {
            exprs: exprs
                .iter()
                .map(|e| expression_to_proto(e))
                .collect::<R<Vec<_>>>()?,
            nullability_predicate: nullability
                .as_ref()
                .map(|n| expression_to_proto(n).map(Box::new))
                .transpose()?,
        })),
        Expression::Unary(u) => Kind::Unary(Box::new(pexpr::UnaryExpression {
            op: match u.op {
                UnaryExpressionOp::ToJson => pexpr::UnaryExpressionOp::ToJson,
            } as i32,
            expr: Some(Box::new(expression_to_proto(&u.expr)?)),
        })),
        Expression::Binary(b) => Kind::Binary(Box::new(pexpr::BinaryExpression {
            op: match b.op {
                BinaryExpressionOp::Plus => pexpr::BinaryExpressionOp::Plus,
                BinaryExpressionOp::Minus => pexpr::BinaryExpressionOp::Minus,
                BinaryExpressionOp::Multiply => pexpr::BinaryExpressionOp::Multiply,
                BinaryExpressionOp::Divide => pexpr::BinaryExpressionOp::Divide,
            } as i32,
            left: Some(Box::new(expression_to_proto(&b.left)?)),
            right: Some(Box::new(expression_to_proto(&b.right)?)),
        })),
        Expression::Variadic(v) => Kind::Variadic(pexpr::VariadicExpression {
            op: match v.op {
                VariadicExpressionOp::Coalesce => pexpr::VariadicExpressionOp::Coalesce,
                VariadicExpressionOp::Array => pexpr::VariadicExpressionOp::Array,
            } as i32,
            exprs: v
                .exprs
                .iter()
                .map(|e| expression_to_proto(e))
                .collect::<R<Vec<_>>>()?,
        }),
        Expression::ParseJson(p) => Kind::ParseJson(Box::new(pexpr::ParseJsonExpression {
            json_expr: Some(Box::new(expression_to_proto(&p.json_expr)?)),
            output_schema: Some(struct_type_to_proto(&p.output_schema)?),
        })),
        Expression::MapToStruct(m) => Kind::MapToStruct(Box::new(pexpr::MapToStructExpression {
            map_expr: Some(Box::new(expression_to_proto(&m.map_expr)?)),
        })),
        Expression::If(i) => Kind::IfExpr(Box::new(pexpr::IfExpression {
            condition: Some(Box::new(predicate_to_proto(&i.condition)?)),
            then_expr: Some(Box::new(expression_to_proto(&i.then_expr)?)),
            else_expr: Some(Box::new(expression_to_proto(&i.else_expr)?)),
        })),
        Expression::Transform(_) => {
            return Err("Transform expression not yet serialized (P2.M1)".to_string())
        }
        Expression::Opaque(_) => {
            return Err("Opaque expression cannot be serialized (engine must error)".to_string())
        }
        Expression::Unknown(s) => {
            return Err(format!("Unknown expression cannot be serialized: {s}"))
        }
    };
    Ok(pexpr::Expression { kind: Some(kind) })
}

fn predicate_to_proto(p: &Predicate) -> R<pexpr::Predicate> {
    use pexpr::predicate::Kind;
    let kind = match p {
        Predicate::BooleanExpression(e) => {
            Kind::BooleanExpression(Box::new(expression_to_proto(e)?))
        }
        Predicate::Not(inner) => Kind::Not(Box::new(predicate_to_proto(inner)?)),
        Predicate::Unary(u) => Kind::Unary(Box::new(pexpr::UnaryPredicate {
            op: match u.op {
                UnaryPredicateOp::IsNull => pexpr::UnaryPredicateOp::IsNull,
            } as i32,
            expr: Some(Box::new(expression_to_proto(&u.expr)?)),
        })),
        Predicate::Binary(b) => Kind::Binary(Box::new(pexpr::BinaryPredicate {
            op: match b.op {
                BinaryPredicateOp::LessThan => pexpr::BinaryPredicateOp::LessThan,
                BinaryPredicateOp::GreaterThan => pexpr::BinaryPredicateOp::GreaterThan,
                BinaryPredicateOp::Equal => pexpr::BinaryPredicateOp::Equal,
                BinaryPredicateOp::Distinct => pexpr::BinaryPredicateOp::Distinct,
                BinaryPredicateOp::In => pexpr::BinaryPredicateOp::In,
            } as i32,
            left: Some(Box::new(expression_to_proto(&b.left)?)),
            right: Some(Box::new(expression_to_proto(&b.right)?)),
        })),
        Predicate::Junction(j) => Kind::Junction(pexpr::JunctionPredicate {
            op: match j.op {
                JunctionPredicateOp::And => pexpr::JunctionPredicateOp::And,
                JunctionPredicateOp::Or => pexpr::JunctionPredicateOp::Or,
            } as i32,
            preds: j
                .preds
                .iter()
                .map(|p| predicate_to_proto(p))
                .collect::<R<Vec<_>>>()?,
        }),
        Predicate::Opaque(_) => {
            return Err("Opaque predicate cannot be serialized (engine must error)".to_string())
        }
        Predicate::Unknown(s) => {
            return Err(format!("Unknown predicate cannot be serialized: {s}"))
        }
    };
    Ok(pexpr::Predicate { kind: Some(kind) })
}
