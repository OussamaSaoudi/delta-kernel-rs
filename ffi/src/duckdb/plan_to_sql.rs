//! Lower a kernel declarative-plan `ResultPlan` into a single DuckDB SQL statement so DuckDB's
//! engine *executes* the plan (Approach B, the "DuckDB-executed plans" path).
//!
//! Each `PlanNode` becomes a CTE `n<output>` that references its inputs' CTEs; the terminal node's
//! CTE is the final `SELECT`. Expressions/predicates lower to DuckDB SQL. This covers the
//! scan-metadata reconciliation node/expression set; genuinely-unsupported constructs return an
//! error rather than emit wrong SQL.

use delta_kernel::expressions::{
    BinaryExpressionOp, BinaryPredicateOp, Expression, JunctionPredicateOp, Predicate, Scalar,
    UnaryExpressionOp, UnaryPredicateOp, VariadicExpressionOp,
};
use delta_kernel::plans::ir::nodes::{FileType, JoinKind, NodeKind};
use delta_kernel::plans::ir::plan::{PlanNode, RefId, ResultPlan};
use delta_kernel::schema::{DataType, PrimitiveType, StructType};

type R<T> = Result<T, String>;

/// Lower a `ResultPlan` to a single DuckDB SQL statement (terminal = `rp.result`).
pub fn result_plan_to_sql(rp: &ResultPlan) -> R<String> {
    result_plan_to_sql_until(&rp.plan, rp.result)
}

/// Lower a plan to DuckDB SQL using `terminal` as the output node. Emits a CTE for every node up
/// to and including `terminal` (the plan is SSA/topo-ordered, so all of `terminal`'s dependencies
/// have smaller indices). Used to peel a data-stage plan down to its `scan_file_row` node.
pub fn result_plan_to_sql_until(
    plan: &delta_kernel::plans::ir::plan::Plan,
    terminal: RefId,
) -> R<String> {
    let mut ctes = Vec::new();
    for node in &plan.nodes {
        if node.output.0 > terminal.0 {
            break;
        }
        let body = node_sql(node, plan)?;
        ctes.push(format!("n{} AS (\n{}\n)", node.output.0, body));
    }
    Ok(format!(
        "WITH {}\nSELECT * FROM n{}",
        ctes.join(",\n"),
        terminal.0
    ))
}

fn cte(r: RefId) -> String {
    format!("n{}", r.0)
}

fn node_sql(node: &PlanNode, plan: &delta_kernel::plans::ir::plan::Plan) -> R<String> {
    match &node.kind {
        NodeKind::Values(n) => {
            let cols: Vec<String> = n.schema.fields().map(|f| quote_ident(f.name())).collect();
            if n.rows.is_empty() {
                // Typed empty relation.
                let sel = n
                    .schema
                    .fields()
                    .map(|f| Ok(format!("NULL::{} AS {}", datatype_sql(&f.data_type())?, quote_ident(f.name()))))
                    .collect::<R<Vec<_>>>()?;
                return Ok(format!("SELECT {} WHERE 1=0", sel.join(", ")));
            }
            let rows = n
                .rows
                .iter()
                .map(|row| {
                    let vals = row.iter().map(scalar_sql).collect::<R<Vec<_>>>()?;
                    Ok(format!("({})", vals.join(", ")))
                })
                .collect::<R<Vec<_>>>()?;
            Ok(format!(
                "SELECT * FROM (VALUES {}) AS t({})",
                rows.join(", "),
                cols.join(", ")
            ))
        }
        NodeKind::Load(n) => load_sql(node, n, plan),
        NodeKind::Filter(n) => {
            let input = cte(node.inputs[0]);
            Ok(format!("SELECT * FROM {input} WHERE {}", predicate_sql(&n.predicate)?))
        }
        NodeKind::Project(n) => {
            let input = cte(node.inputs[0]);
            let mut sel = Vec::new();
            for ((name, expr), field) in n.named_exprs.iter().zip(n.output_schema.fields()) {
                let e = expr_sql(expr, Some(&field.data_type()))?;
                sel.push(format!("{e} AS {}", quote_ident(name)));
            }
            Ok(format!("SELECT {} FROM {input}", sel.join(", ")))
        }
        NodeKind::MaxByVersion(n) => {
            let input = cte(node.inputs[0]);
            let part = n
                .group_by
                .iter()
                .map(|e| expr_sql(e, None))
                .collect::<R<Vec<_>>>()?
                .join(", ");
            let order = expr_sql(&n.version_column, None)?;
            let out_cols = n
                .output_schema
                .fields()
                .map(|f| quote_ident(f.name()))
                .collect::<Vec<_>>()
                .join(", ");
            // ROW_NUMBER over the group ordered by version desc; keep rn=1. A stable tiebreaker
            // would need a synthesized input index; the kernel breaks ties by input order.
            Ok(format!(
                "SELECT {out_cols} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {order} DESC) AS __dk_rn FROM {input}) WHERE __dk_rn = 1"
            ))
        }
        NodeKind::EquiJoin(n) => {
            let left = cte(node.inputs[0]);
            let right = cte(node.inputs[1]);
            let on = n
                .left_keys
                .iter()
                .zip(n.right_keys.iter())
                .map(|(l, r)| {
                    Ok(format!(
                        "({} IS NOT DISTINCT FROM {})",
                        qualify("l_side", &expr_sql(l, None)?),
                        qualify("r_side", &expr_sql(r, None)?)
                    ))
                })
                .collect::<R<Vec<_>>>()?
                .join(" AND ");
            match n.kind {
                JoinKind::LeftAnti => Ok(format!(
                    "SELECT l_side.* FROM {left} AS l_side ANTI JOIN {right} AS r_side ON {on}"
                )),
            }
        }
        NodeKind::UnionAll(_) => {
            let parts = node
                .inputs
                .iter()
                .map(|r| format!("SELECT * FROM {}", cte(*r)))
                .collect::<Vec<_>>()
                .join("\nUNION ALL BY NAME\n");
            Ok(parts)
        }
        NodeKind::ScanParquet(n) => {
            // Concrete checkpoint / leaf-checkpoint files (FileMeta list is resolved by the SM).
            if n.files.is_empty() {
                let sel = n
                    .schema
                    .fields()
                    .map(|f| Ok(format!("NULL::{} AS {}", datatype_sql(&f.data_type())?, quote_ident(f.name()))))
                    .collect::<R<Vec<_>>>()?;
                return Ok(format!("SELECT {} WHERE 1=0", sel.join(", ")));
            }
            let files = n
                .files
                .iter()
                .map(|fm| Ok(sql_string(&url_to_path(&fm.location)?)))
                .collect::<R<Vec<_>>>()?;
            // CAST each action column to the expected schema type so the struct shape matches
            // downstream accesses regardless of what fields the checkpoint parquet physically has
            // (name-based struct cast: missing fields -> NULL). union_by_name tolerates files that
            // omit some top-level action columns.
            let cols = n
                .schema
                .fields()
                .map(|f| {
                    Ok(format!(
                        "CAST({} AS {}) AS {}",
                        quote_ident(f.name()),
                        datatype_sql(&f.data_type())?,
                        quote_ident(f.name())
                    ))
                })
                .collect::<R<Vec<_>>>()?
                .join(", ");
            Ok(format!(
                "SELECT {cols} FROM read_parquet([{}], union_by_name=true)",
                files.join(", ")
            ))
        }
        NodeKind::ScanJson(n) => {
            // Concrete newline-delimited JSON files (e.g. a v2 JSON checkpoint manifest).
            let cols = n
                .schema
                .fields()
                .map(|f| quote_ident(f.name()))
                .collect::<Vec<_>>()
                .join(", ");
            if n.files.is_empty() {
                let sel = n
                    .schema
                    .fields()
                    .map(|f| Ok(format!("NULL::{} AS {}", datatype_sql(&f.data_type())?, quote_ident(f.name()))))
                    .collect::<R<Vec<_>>>()?;
                return Ok(format!("SELECT {} WHERE 1=0", sel.join(", ")));
            }
            let read_cols = n
                .schema
                .fields()
                .map(|f| Ok(format!("'{}': '{}'", f.name(), datatype_sql(&f.data_type())?)))
                .collect::<R<Vec<_>>>()?
                .join(", ");
            let files = n
                .files
                .iter()
                .map(|fm| Ok(sql_string(&url_to_path(&fm.location)?)))
                .collect::<R<Vec<_>>>()?
                .join(", ");
            Ok(format!(
                "SELECT {cols} FROM read_json([{files}], format='newline_delimited', columns={{{read_cols}}})"
            ))
        }
        NodeKind::ListFiles(_) => Err(format!(
            "node {} not yet lowered to SQL (P2 follow-up)",
            node.kind
        )),
    }
}

/// Lower a `Load(JSON)` whose input is a `Values` of file metadata into a `read_json` over the
/// referenced commit files, broadcasting the metadata-derived columns.
fn load_sql(node: &PlanNode, n: &delta_kernel::plans::ir::nodes::LoadNode, plan: &delta_kernel::plans::ir::plan::Plan) -> R<String> {
    // Note: the data-stage Load (file_type=Parquet, dv_ref=Some) is peeled off before lowering;
    // the Loads reaching here are commit loads (JSON) and sidecar loads (Parquet), neither with DV.
    // Resolve the input Values node to get the concrete file list + broadcast columns.
    let input_idx = node.inputs[0].0 as usize;
    let input = plan.nodes.get(input_idx).ok_or("Load: missing input node")?;
    let NodeKind::Values(vals) = &input.kind else {
        // Runtime-relation input (the data-stage Load's `scan_file_row`): lower to the `delta_load`
        // table function, which streams the input rows and reads each file as a concurrent
        // sink+source operator (intra-file parallelism; DV + partition broadcast applied per file).
        return delta_load_sql(node, n);
    };
    let vfields: Vec<&delta_kernel::schema::StructField> = vals.schema.fields().collect();
    let field_idx = |name: &str| {
        vfields
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| format!("Load: column {name} not found in input Values"))
    };
    let path_col = n.file_meta.path_column.path();
    if path_col.len() != 1 {
        return Err("Load path_column must be a top-level column".into());
    }
    let path_pos = field_idx(&path_col[0])?;
    // CAST to the expected schema so the struct shape matches downstream (parquet sidecars may
    // have a different physical shape; for read_json the columns are already forced, so it's a noop).
    let file_cols = n
        .file_schema
        .fields()
        .map(|f| {
            Ok(format!(
                "CAST({} AS {}) AS {}",
                quote_ident(f.name()),
                datatype_sql(&f.data_type())?,
                quote_ident(f.name())
            ))
        })
        .collect::<R<Vec<_>>>()?
        .join(", ");

    // Empty input (e.g. a fully-checkpointed table's empty commit tail): a typed empty relation.
    if vals.rows.is_empty() {
        let mut sel = n
            .file_schema
            .fields()
            .map(|f| Ok(format!("NULL::{} AS {}", datatype_sql(&f.data_type())?, quote_ident(f.name()))))
            .collect::<R<Vec<_>>>()?;
        for col in &n.metadata_derived_columns {
            let cp = col.path();
            let pos = field_idx(&cp[0])?;
            sel.push(format!(
                "NULL::{} AS {}",
                datatype_sql(&vfields[pos].data_type())?,
                quote_ident(&cp[0])
            ));
        }
        return Ok(format!("SELECT {} WHERE 1=0", sel.join(", ")));
    }

    // Per-file reader: read_json (commits) or read_parquet (sidecars / materialized lists).
    let reader = |full: &str| -> R<String> {
        match n.file_type {
            FileType::Json => {
                let read_cols = n
                    .file_schema
                    .fields()
                    .map(|f| Ok(format!("'{}': '{}'", f.name(), datatype_sql(&f.data_type())?)))
                    .collect::<R<Vec<_>>>()?
                    .join(", ");
                Ok(format!(
                    "read_json('{full}', format='newline_delimited', columns={{{read_cols}}})"
                ))
            }
            FileType::Parquet => Ok(format!("read_parquet('{full}', union_by_name=true)")),
        }
    };

    let mut selects = Vec::new();
    for row in &vals.rows {
        let path_scalar = row.get(path_pos).ok_or("Load: row missing path value")?;
        let Scalar::String(rel) = path_scalar else {
            return Err("Load: path value is not a string".into());
        };
        let full = resolve_url(n.base_url.as_ref(), rel)?;
        let mut bcast = Vec::new();
        for col in &n.metadata_derived_columns {
            let cp = col.path();
            if cp.len() != 1 {
                return Err("Load metadata_derived_columns must be top-level".into());
            }
            let pos = field_idx(&cp[0])?;
            let v = row.get(pos).ok_or("Load: row missing metadata value")?;
            bcast.push(format!("{} AS {}", scalar_sql(v)?, quote_ident(&cp[0])));
        }
        let bcast_sql = if bcast.is_empty() {
            String::new()
        } else {
            format!(", {}", bcast.join(", "))
        };
        selects.push(format!("SELECT {file_cols}{bcast_sql} FROM {}", reader(&full)?));
    }
    Ok(selects.join("\nUNION ALL BY NAME\n"))
}

/// Lower a `Load` with a runtime-relation input to the `delta_load` table function. The input CTE
/// streams one file-descriptor row per file; `delta_load` opens each file (`file_type` / `base_url`),
/// reads `file_schema`, applies the per-row deletion vector (`dv_column`/`dv_kind`), and broadcasts
/// metadata-derived columns. This is the faithful, generic realization of the kernel `Load` IR node.
fn delta_load_sql(node: &PlanNode, n: &delta_kernel::plans::ir::nodes::LoadNode) -> R<String> {
    use delta_kernel::plans::ir::nodes::DvKind;
    let file_type = match n.file_type {
        FileType::Parquet => "parquet",
        FileType::Json => "json",
    };
    // file_schema rendered as a DuckDB STRUCT type so the operator knows the read columns/types.
    let fields = n
        .file_schema
        .fields()
        .map(|f| Ok(format!("{} {}", quote_ident(f.name()), datatype_sql(&f.data_type())?)))
        .collect::<R<Vec<_>>>()?
        .join(", ");
    let file_schema = format!("STRUCT({fields})").replace('\'', "''");
    let path_col = n.file_meta.path_column.path();
    if path_col.len() != 1 {
        return Err("delta_load path_column must be a top-level column".into());
    }
    // The table-valued argument must be a subquery expression for DuckDB to bind it as a relation
    // (a bare CTE name binds as a scalar). `(FROM n)` is a trivial CTE reference the optimizer
    // flattens — NOT an inlined subplan.
    let mut args = vec![
        format!("(FROM {})", cte(node.inputs[0])),
        format!("file_type := '{file_type}'"),
        format!("file_schema := '{file_schema}'"),
        format!("path_column := '{}'", path_col[0].replace('\'', "''")),
    ];
    if let Some(u) = &n.base_url {
        args.push(format!("base_url := '{}'", u.as_str().replace('\'', "''")));
    }
    if let Some(dv) = &n.dv_ref {
        let dvc = dv.column.path();
        if dvc.len() != 1 {
            return Err("delta_load dv_column must be a top-level column".into());
        }
        let kind = match dv.kind {
            DvKind::Descriptor => "descriptor",
            DvKind::Bytes => "bytes",
        };
        args.push(format!("dv_column := '{}'", dvc[0].replace('\'', "''")));
        args.push(format!("dv_kind := '{kind}'"));
    }
    // metadata_derived_columns: top-level input columns whose value is broadcast onto every output
    // row (e.g. `fileConstantValues` for the data Load — the terminal projection extracts partition
    // values from it; `version` for table-changes). The operator outputs file_schema + these columns.
    if !n.metadata_derived_columns.is_empty() {
        let cols = n
            .metadata_derived_columns
            .iter()
            .map(|c| {
                let p = c.path();
                if p.len() != 1 {
                    return Err("delta_load metadata_derived must be top-level columns".to_string());
                }
                Ok(format!("'{}'", p[0].replace('\'', "''")))
            })
            .collect::<R<Vec<_>>>()?
            .join(", ");
        args.push(format!("metadata_derived := [{cols}]"));
    }
    Ok(format!("SELECT * FROM delta_load({})", args.join(", ")))
}

fn resolve_url(base: Option<&url::Url>, rel: &str) -> R<String> {
    let joined = match base {
        Some(b) => b
            .join(rel)
            .map_err(|e| format!("Load: cannot join base_url with {rel}: {e}"))?,
        None => url::Url::parse(rel).map_err(|e| format!("Load: path is not an absolute URL: {e}"))?,
    };
    // DuckDB read_json wants a filesystem path for file:// URLs.
    if joined.scheme() == "file" {
        joined
            .to_file_path()
            .map(|p| p.display().to_string())
            .map_err(|_| format!("Load: bad file URL {joined}"))
    } else {
        Ok(joined.to_string())
    }
}

/// Convert an absolute URL (e.g. a `FileMeta.location`) to a path DuckDB's readers accept:
/// `file://` URLs become local filesystem paths; other schemes pass through as-is.
fn url_to_path(u: &url::Url) -> R<String> {
    if u.scheme() == "file" {
        u.to_file_path()
            .map(|p| p.display().to_string())
            .map_err(|_| format!("bad file url {u}"))
    } else {
        Ok(u.to_string())
    }
}

fn qualify(alias: &str, expr_sql: &str) -> String {
    // Prefix a single bracketed/identifier column expression with the table alias.
    format!("{alias}.{expr_sql}")
}

// ============================================================================
// Expressions / predicates
// ============================================================================

fn expr_sql(e: &Expression, expected: Option<&DataType>) -> R<String> {
    Ok(match e {
        Expression::Literal(s) => scalar_sql(s)?,
        Expression::Column(c) => column_sql(c.path()),
        Expression::Predicate(p) => format!("({})", predicate_sql(p)?),
        Expression::Variadic(v) => {
            let args = v.exprs.iter().map(|e| expr_sql(e, None)).collect::<R<Vec<_>>>()?;
            match v.op {
                VariadicExpressionOp::Coalesce => format!("coalesce({})", args.join(", ")),
                VariadicExpressionOp::Array => format!("[{}]", args.join(", ")),
            }
        }
        Expression::Binary(b) => {
            let l = expr_sql(&b.left, None)?;
            let r = expr_sql(&b.right, None)?;
            let op = match b.op {
                BinaryExpressionOp::Plus => "+",
                BinaryExpressionOp::Minus => "-",
                BinaryExpressionOp::Multiply => "*",
                BinaryExpressionOp::Divide => "/",
            };
            format!("({l} {op} {r})")
        }
        Expression::Unary(u) => {
            let inner = expr_sql(&u.expr, None)?;
            match u.op {
                UnaryExpressionOp::ToJson => format!("to_json({inner})"),
            }
        }
        Expression::If(i) => {
            let cond = predicate_sql(&i.condition)?;
            let then = expr_sql(&i.then_expr, expected)?;
            let els = expr_sql(&i.else_expr, expected)?;
            format!("CASE WHEN {cond} THEN {then} ELSE {els} END")
        }
        Expression::Struct(exprs, _nullability) => {
            // Field names AND types come from the expected struct type; thread each field's type
            // into its sub-expression so nested Struct/MapToStruct/ParseJson know their target.
            let st = expected
                .and_then(strip_struct)
                .ok_or("Struct expression without an expected struct type")?;
            let fields: Vec<(String, DataType)> = st
                .fields()
                .map(|f| (f.name().clone(), f.data_type().clone()))
                .collect();
            if fields.len() != exprs.len() {
                return Err("Struct expression arity != expected struct fields".into());
            }
            let parts = exprs
                .iter()
                .zip(fields.iter())
                .map(|(e, (name, ty))| {
                    Ok(format!("{} := {}", quote_ident(name), expr_sql(e, Some(ty))?))
                })
                .collect::<R<Vec<_>>>()?;
            format!("struct_pack({})", parts.join(", "))
        }
        Expression::ParseJson(p) => {
            let json = expr_sql(&p.json_expr, None)?;
            let ty = datatype_sql(&DataType::Struct(Box::new((*p.output_schema).clone())))?;
            format!("from_json({json}, '{ty}')")
        }
        Expression::MapToStruct(m) => {
            // Delta partition values live in a MAP(VARCHAR,VARCHAR); parse each target field by
            // extracting its key and casting the string to the field's type.
            let st = expected
                .and_then(strip_struct)
                .ok_or("MapToStruct needs an expected struct type")?;
            let map = expr_sql(&m.map_expr, None)?;
            let parts = st
                .fields()
                .map(|f| {
                    let raw = format!("map_extract(({map}), {})[1]", sql_string(f.name()));
                    // Delta stores partition values as strings. Binary values become the UTF-8
                    // bytes of the string (CAST string->BLOB rejects non-ASCII); others cast.
                    let val = match f.data_type() {
                        DataType::Primitive(PrimitiveType::Binary) => format!("encode({raw})"),
                        dt => format!("CAST({raw} AS {})", datatype_sql(&dt)?),
                    };
                    Ok(format!("{} := {}", quote_ident(f.name()), val))
                })
                .collect::<R<Vec<_>>>()?;
            if parts.is_empty() {
                return Err("MapToStruct with an empty target struct".into());
            }
            format!("struct_pack({})", parts.join(", "))
        }
        Expression::Transform(t) => {
            // Sparse struct transform. We handle the identity/projection case: no field_transforms and
            // no prepended fields, so the output is the input struct (at input_path) passed through.
            // Rebuild the expected struct by extracting each output field from the input struct path —
            // robust to field reordering/projection. (General replace/insert transforms: follow-up.)
            if !t.field_transforms.is_empty() || !t.prepended_fields.is_empty() {
                return Err(format!(
                    "Transform with field_transforms/prepended_fields not yet lowered to SQL: {t:?}"
                ));
            }
            let base = match &t.input_path {
                Some(p) => p.path().to_vec(),
                None => return Err("Transform without input_path not yet lowered to SQL".into()),
            };
            let st = match expected {
                Some(DataType::Struct(st)) => st,
                _ => return Err("Transform requires an expected struct output type".into()),
            };
            let fields = st
                .fields()
                .map(|f| {
                    let mut p = base.clone();
                    p.push(f.name().to_string());
                    format!("{} := {}", quote_ident(f.name()), column_sql(&p))
                })
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Err("Transform with an empty expected struct".into());
            }
            format!("struct_pack({})", fields.join(", "))
        }
        Expression::Opaque(_) => return Err("Opaque expression cannot be lowered".into()),
        Expression::Unknown(s) => return Err(format!("Unknown expression cannot be lowered: {s}")),
    })
}

fn predicate_sql(p: &Predicate) -> R<String> {
    Ok(match p {
        Predicate::BooleanExpression(e) => expr_sql(e, None)?,
        Predicate::Not(inner) => format!("(NOT ({}))", predicate_sql(inner)?),
        Predicate::Unary(u) => {
            let inner = expr_sql(&u.expr, None)?;
            match u.op {
                UnaryPredicateOp::IsNull => format!("({inner} IS NULL)"),
            }
        }
        Predicate::Binary(b) => {
            let l = expr_sql(&b.left, None)?;
            let r = expr_sql(&b.right, None)?;
            match b.op {
                BinaryPredicateOp::LessThan => format!("({l} < {r})"),
                BinaryPredicateOp::GreaterThan => format!("({l} > {r})"),
                BinaryPredicateOp::Equal => format!("({l} = {r})"),
                BinaryPredicateOp::Distinct => format!("({l} IS DISTINCT FROM {r})"),
                BinaryPredicateOp::In => format!("({l} IN {r})"),
            }
        }
        Predicate::Junction(j) => {
            let op = match j.op {
                JunctionPredicateOp::And => " AND ",
                JunctionPredicateOp::Or => " OR ",
            };
            let parts = j.preds.iter().map(|p| predicate_sql(p)).collect::<R<Vec<_>>>()?;
            if parts.is_empty() {
                match j.op {
                    JunctionPredicateOp::And => "TRUE".to_string(),
                    JunctionPredicateOp::Or => "FALSE".to_string(),
                }
            } else {
                format!("({})", parts.join(op))
            }
        }
        Predicate::Opaque(_) => return Err("Opaque predicate cannot be lowered".into()),
        Predicate::Unknown(s) => return Err(format!("Unknown predicate cannot be lowered: {s}")),
    })
}

// ============================================================================
// Columns / scalars / types
// ============================================================================

/// A column path `[a, b, c]` -> `"a"['b']['c']` (top-level identifier + struct-field access).
fn column_sql(path: &[String]) -> String {
    let mut s = quote_ident(&path[0]);
    for part in &path[1..] {
        s.push_str(&format!("['{}']", part.replace('\'', "''")));
    }
    s
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn scalar_sql(s: &Scalar) -> R<String> {
    Ok(match s {
        Scalar::Integer(v) => v.to_string(),
        Scalar::Long(v) => v.to_string(),
        Scalar::Short(v) => v.to_string(),
        Scalar::Byte(v) => v.to_string(),
        Scalar::Float(v) => format!("{v}::FLOAT"),
        Scalar::Double(v) => format!("{v}::DOUBLE"),
        Scalar::String(v) => sql_string(v),
        Scalar::Boolean(v) => v.to_string(),
        Scalar::Date(v) => format!("({v})::INTEGER::DATE"),
        Scalar::Timestamp(v) => format!("epoch_us({v})"),
        Scalar::TimestampNtz(v) => format!("make_timestamp({v})"),
        Scalar::Null(dt) => format!("NULL::{}", datatype_sql(dt)?),
        Scalar::Binary(_) => return Err("binary scalar literal not lowered (P2 follow-up)".into()),
        Scalar::Decimal(_) => return Err("decimal scalar literal not lowered (P2 follow-up)".into()),
        other => return Err(format!("scalar literal not lowered: {other:?}")),
    })
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn strip_struct(dt: &DataType) -> Option<&StructType> {
    match dt {
        DataType::Struct(s) => Some(s),
        _ => None,
    }
}

/// A DuckDB type string for a kernel `DataType` (used in casts and `read_json` column specs).
fn datatype_sql(dt: &DataType) -> R<String> {
    Ok(match dt {
        DataType::Primitive(p) => match p {
            PrimitiveType::String => "VARCHAR".into(),
            PrimitiveType::Long => "BIGINT".into(),
            PrimitiveType::Integer => "INTEGER".into(),
            PrimitiveType::Short => "SMALLINT".into(),
            PrimitiveType::Byte => "TINYINT".into(),
            PrimitiveType::Float => "FLOAT".into(),
            PrimitiveType::Double => "DOUBLE".into(),
            PrimitiveType::Boolean => "BOOLEAN".into(),
            PrimitiveType::Binary => "BLOB".into(),
            PrimitiveType::Date => "DATE".into(),
            PrimitiveType::Timestamp => "TIMESTAMP WITH TIME ZONE".into(),
            PrimitiveType::TimestampNtz => "TIMESTAMP".into(),
            PrimitiveType::Decimal(d) => format!("DECIMAL({},{})", d.precision(), d.scale()),
        },
        DataType::Array(a) => format!("{}[]", datatype_sql(a.element_type())?),
        DataType::Map(m) => format!(
            "MAP({}, {})",
            datatype_sql(m.key_type())?,
            datatype_sql(m.value_type())?
        ),
        DataType::Struct(s) => {
            let fields = s
                .fields()
                .map(|f| Ok(format!("{} {}", quote_ident(f.name()), datatype_sql(&f.data_type())?)))
                .collect::<R<Vec<_>>>()?;
            format!("STRUCT({})", fields.join(", "))
        }
        other => return Err(format!("DataType not lowered to DuckDB type: {other:?}")),
    })
}
