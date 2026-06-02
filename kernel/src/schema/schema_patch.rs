//! Schema patch builder for transforming Delta table schemas.
//!
//! This module provides a builder pattern for constructing schema transformations using two
//! primitive operations: [`Drop`](PatchOp::Drop) and [`Add`](PatchOp::Add). Complex operations
//! like replace are composed from these primitives.
//!
//! # Example
//!
//! ```ignore
//! let patch = SchemaPatchBuilder::new(schema)
//!     .drop_field(["a", "old_field"])
//!     .add_field(["a", "b"], "new_field", expr)
//!     .replace_field(["x"], replacement_expr)
//!     .build()?;
//!
//! let transform_expr = patch.into_expression()?;
//! ```

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::expressions::type_inference::synthesize_type;
use crate::expressions::{ColumnName, Expression, ExpressionRef, ExpressionStructPatch};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error};

// ================================================================================================
// PatchOp - The two primitive operations
// ================================================================================================

/// A single patch operation on a schema field.
#[derive(Debug, Clone, PartialEq)]
pub enum PatchOp {
    /// Remove a field from the schema. The field must exist.
    Drop {
        /// Full path of the field to drop.
        path: ColumnName,
    },
    /// Add a new field to the schema.
    Add {
        /// The field after which to insert the new field. `None` means prepend (insert before all
        /// fields at this level).
        after: Option<ColumnName>,
        /// Name of the new field.
        name: String,
        /// Expression that computes the new field's value.
        expr: ExpressionRef,
    },
}

impl PatchOp {
    /// Returns the effective path of this operation for validation and sorting.
    ///
    /// - For `Drop`: the path of the field being dropped
    /// - For `Add`: the path where the new field will be inserted (parent + name)
    pub fn effective_path(&self) -> ColumnName {
        match self {
            PatchOp::Drop { path } => path.clone(),
            PatchOp::Add { after, name, .. } => {
                match after {
                    Some(predecessor) => {
                        // New field is a sibling of predecessor: parent(predecessor) + name
                        match predecessor.parent() {
                            Some(parent) => ColumnName::new(
                                parent.into_iter().chain(std::iter::once(name.clone())),
                            ),
                            None => ColumnName::new([name.clone()]),
                        }
                    }
                    None => {
                        // Prepend at top level
                        ColumnName::new([name.clone()])
                    }
                }
            }
        }
    }

    /// Returns true if this is a Drop operation.
    fn is_drop(&self) -> bool {
        matches!(self, PatchOp::Drop { .. })
    }

    /// Returns the parent path of this operation's target location.
    fn parent(&self) -> Option<ColumnName> {
        match self {
            PatchOp::Drop { path } => path.parent(),
            PatchOp::Add { after: Some(p), .. } => p.parent(),
            PatchOp::Add { after: None, .. } => None,
        }
    }
}

// ================================================================================================
// SchemaPatchBuilder - Collects operations and validates them
// ================================================================================================

/// Builder for constructing a validated schema patch.
///
/// The builder collects patch operations and validates them at build time. Validation catches:
/// - Double drops (dropping the same field twice)
/// - Double adds (adding to the same path twice)
/// - Parent-child conflicts (modifying both a struct and its nested fields)
#[derive(Debug, Clone)]
pub struct SchemaPatchBuilder {
    schema: SchemaRef,
    ops: Vec<PatchOp>,
}

impl SchemaPatchBuilder {
    /// Creates a new builder for the given input schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            ops: Vec::new(),
        }
    }

    /// Drops a field at the given path.
    ///
    /// The field must exist in the schema. At build time, dropping the same field twice or
    /// dropping a parent of another modified field will produce an error.
    ///
    /// # Example
    /// ```ignore
    /// builder.drop_field(["a", "b"])  // drops field "b" inside struct "a"
    /// ```
    pub fn drop_field<I, S>(mut self, path: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ops.push(PatchOp::Drop {
            path: ColumnName::new(path),
        });
        self
    }

    /// Adds a new field after the specified predecessor.
    ///
    /// The new field will be inserted as a sibling of `after`, at the same nesting level.
    /// The `after` field may or may not be dropped by a previous operation.
    ///
    /// # Example
    /// ```ignore
    /// builder.add_field(["a"], "b", Expression::literal(42))
    /// ```
    pub fn add_field<I, S>(
        mut self,
        after: I,
        name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ops.push(PatchOp::Add {
            after: Some(ColumnName::new(after)),
            name: name.into(),
            expr: expr.into(),
        });
        self
    }

    /// Adds a new field at the beginning of the top-level schema (prepend).
    pub fn prepend_field(
        mut self,
        name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Self {
        self.ops.push(PatchOp::Add {
            after: None,
            name: name.into(),
            expr: expr.into(),
        });
        self
    }

    /// Replaces a field with a new expression, keeping the same name.
    ///
    /// This is equivalent to dropping the field and adding a new field with the same name in its
    /// position. The `path` must be non-empty.
    ///
    /// # Example
    /// ```ignore
    /// builder.replace_field(["stats"], Expression::literal(42))
    /// ```
    pub fn replace_field<I, S>(self, path: I, expr: impl Into<ExpressionRef>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let path = ColumnName::new(path);
        let name = path.last().map(|s| s.to_string()).unwrap_or_default();
        self.replace_field_as_internal(path, name, expr)
    }

    /// Replaces a field with a new field of a different name.
    ///
    /// This is equivalent to dropping the field at `path` and adding a new field named `new_name`
    /// in its position.
    ///
    /// # Example
    /// ```ignore
    /// builder.replace_field_as(["stats"], "stats_parsed", Expression::literal(42))
    /// ```
    pub fn replace_field_as<I, S>(
        self,
        path: I,
        new_name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.replace_field_as_internal(ColumnName::new(path), new_name, expr)
    }

    fn replace_field_as_internal(
        mut self,
        path: ColumnName,
        new_name: impl Into<String>,
        expr: impl Into<ExpressionRef>,
    ) -> Self {
        // If path is empty, this is a no-op
        if path.is_empty() {
            return self;
        }

        // Drop the old field
        self.ops.push(PatchOp::Drop { path: path.clone() });

        // Add the new field after the dropped field's position
        self.ops.push(PatchOp::Add {
            after: Some(path),
            name: new_name.into(),
            expr: expr.into(),
        });

        self
    }

    /// Validates and builds the schema patch.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The same field is dropped twice
    /// - The same field path is added twice
    /// - A field and one of its descendants are both modified
    pub fn build(self) -> DeltaResult<SchemaPatch> {
        let schema_index = SchemaIndex::new(&self.schema);
        let mut sorted_ops = self.ops;

        // Sort: all drops first (parent before children), then all adds (children before parent)
        sorted_ops.sort_by(|a, b| {
            let a_path = a.effective_path();
            let b_path = b.effective_path();
            let a_idx = schema_index.get(&a_path);
            let b_idx = schema_index.get(&b_path);

            match (a.is_drop(), b.is_drop()) {
                // Both drops: sort by schema order (parent before children)
                (true, true) => a_idx.cmp(&b_idx),
                // Both adds: sort by reverse schema order (children before parent)
                (false, false) => b_idx.cmp(&a_idx),
                // Drops before adds
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
            }
        });

        // Validate using sliding window
        validate_sorted_ops(&sorted_ops)?;

        Ok(SchemaPatch { ops: sorted_ops })
    }
}

// ================================================================================================
// SchemaIndex - Maps paths to DFS indices for sorting
// ================================================================================================

/// Index mapping field paths to their DFS order in the schema.
struct SchemaIndex {
    indices: HashMap<ColumnName, usize>,
    max_index: usize,
}

impl SchemaIndex {
    fn new(schema: &StructType) -> Self {
        let mut indices = HashMap::new();
        let mut index = 0;

        fn visit<'a>(
            fields: impl Iterator<Item = &'a StructField>,
            parent: &ColumnName,
            indices: &mut HashMap<ColumnName, usize>,
            index: &mut usize,
        ) {
            for field in fields {
                let path = if parent.is_empty() {
                    ColumnName::new([field.name.clone()])
                } else {
                    ColumnName::new(
                        parent
                            .iter()
                            .cloned()
                            .chain(std::iter::once(field.name.clone())),
                    )
                };

                indices.insert(path.clone(), *index);
                *index += 1;

                // Recurse into nested structs
                if let DataType::Struct(nested) = &field.data_type {
                    visit(nested.fields(), &path, indices, index);
                }
            }
        }

        let empty_path = ColumnName::new::<&str>([]);
        visit(schema.fields(), &empty_path, &mut indices, &mut index);

        Self {
            max_index: index,
            indices,
        }
    }

    /// Returns the DFS index for a path, or max_index + hash for unknown paths.
    fn get(&self, path: &ColumnName) -> usize {
        self.indices
            .get(path)
            .copied()
            .unwrap_or(self.max_index + path.len())
    }
}

// ================================================================================================
// Validation
// ================================================================================================

/// Validates sorted operations using a sliding window.
fn validate_sorted_ops(ops: &[PatchOp]) -> DeltaResult<()> {
    for window in ops.windows(2) {
        let prev = &window[0];
        let curr = &window[1];
        let prev_path = prev.effective_path();
        let curr_path = curr.effective_path();

        // Check for duplicate operations on the same path
        if prev_path == curr_path {
            match (prev, curr) {
                (PatchOp::Drop { .. }, PatchOp::Drop { .. }) => {
                    return Err(Error::generic(format!(
                        "Double drop on field '{prev_path}'"
                    )));
                }
                (PatchOp::Add { .. }, PatchOp::Add { .. }) => {
                    return Err(Error::generic(format!("Double add on field '{prev_path}'")));
                }
                // Drop followed by Add at same path is OK (this is a replace)
                (PatchOp::Drop { .. }, PatchOp::Add { .. }) => {}
                // Add followed by Drop shouldn't happen (drops come first)
                (PatchOp::Add { .. }, PatchOp::Drop { .. }) => {
                    unreachable!("all drops should be sorted before all adds")
                }
            }
        }

        // Check for parent-child conflicts
        // Drops are parent-first, adds are children-first, so check both directions
        if is_prefix(&prev_path, &curr_path) || is_prefix(&curr_path, &prev_path) {
            return Err(Error::generic(format!(
                "Cannot modify both '{prev_path}' and '{curr_path}' (parent-child conflict)"
            )));
        }
    }

    Ok(())
}

/// Returns true if `prefix` is a proper prefix of `path`.
fn is_prefix(prefix: &ColumnName, path: &ColumnName) -> bool {
    let prefix_path = prefix.path();
    let full_path = path.path();

    prefix_path.len() < full_path.len() && full_path.starts_with(prefix_path)
}

// ================================================================================================
// SchemaPatch - The validated, ready-to-apply patch
// ================================================================================================

/// A validated schema patch ready to be applied.
///
/// The patch contains a sorted list of operations that have been validated for conflicts.
#[derive(Debug, Clone)]
pub struct SchemaPatch {
    ops: Vec<PatchOp>,
}

impl SchemaPatch {
    /// Returns the operations in this patch (sorted and validated).
    pub fn ops(&self) -> &[PatchOp] {
        &self.ops
    }

    /// Converts this patch to an [`ExpressionStructPatch`].
    ///
    /// This method requires all operations to be at the same nesting level. If operations span
    /// multiple levels, apply operations level by level.
    ///
    /// # Errors
    ///
    /// Returns an error if operations span multiple nesting levels.
    pub fn into_struct_patch(self) -> DeltaResult<ExpressionStructPatch> {
        let mut ops = self.ops.into_iter().peekable();

        // Determine common parent from first op, or return empty patch
        let common_parent = match ops.peek() {
            Some(op) => op.parent(),
            None => return Ok(ExpressionStructPatch::new_top_level()),
        };

        let mut patch = match &common_parent {
            Some(path) => ExpressionStructPatch::new_nested(path.iter().cloned()),
            None => ExpressionStructPatch::new_top_level(),
        };

        for op in ops {
            // Validate parent matches
            if op.parent() != common_parent {
                return Err(Error::generic(
                    "Cannot convert SchemaPatch with operations at multiple nesting levels \
                     to a single ExpressionStructPatch. Operations must all be at the same level.",
                ));
            }

            // Process the operation
            match op {
                PatchOp::Drop { path } => {
                    if let Some(name) = path.last() {
                        patch = patch.with_dropped_field(name.to_string());
                    }
                }
                PatchOp::Add {
                    after: Some(predecessor),
                    expr,
                    ..
                } => {
                    if let Some(after_name) = predecessor.last() {
                        patch = patch.with_inserted_field(Some(after_name.to_string()), expr);
                    }
                }
                PatchOp::Add {
                    after: None, expr, ..
                } => {
                    patch = patch.with_inserted_field(None::<String>, expr);
                }
            }
        }

        Ok(patch)
    }

    /// Converts this patch to an [`Expression::StructPatch`].
    ///
    /// This is a convenience method that wraps [`Self::into_struct_patch`].
    ///
    /// # Errors
    ///
    /// Returns an error if operations span multiple nesting levels.
    pub fn into_expression(self) -> DeltaResult<Expression> {
        Ok(Expression::struct_patch(self.into_struct_patch()?))
    }

    /// Derives the output schema from applying this patch to the given input schema.
    ///
    /// Uses type synthesis to determine the data types of added fields based on their expressions.
    ///
    /// # Parameters
    ///
    /// - `input_schema`: The schema before the patch is applied
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Operations span multiple nesting levels
    /// - An added field's type cannot be synthesized from its expression
    /// - A dropped field path is empty
    ///
    /// # Example
    ///
    /// ```ignore
    /// let patch = SchemaPatchBuilder::new(schema.clone())
    ///     .drop_field(["old_col"])
    ///     .add_field(["x"], "new_col", Expression::literal(42i32))
    ///     .build()?;
    ///
    /// let output_schema = patch.into_struct_type(&schema)?;
    /// // output_schema has "old_col" removed and "new_col" added with INTEGER type
    /// ```
    pub fn into_struct_type(self, input_schema: &StructType) -> DeltaResult<StructType> {
        let mut ops = self.ops.into_iter().peekable();

        // Determine common parent from first op
        let common_parent = match ops.peek() {
            Some(op) => op.parent(),
            None => return Ok(input_schema.clone()),
        };

        // Get the fields at the target level
        let target_fields: Vec<StructField> = match &common_parent {
            Some(path) => {
                // Navigate to the nested struct
                let nested_type = get_nested_struct_type(input_schema, path)?;
                nested_type.fields().cloned().collect()
            }
            None => input_schema.fields().cloned().collect(),
        };

        // Apply operations to the fields
        let mut result_fields = target_fields;

        for op in ops {
            // Validate parent matches
            if op.parent() != common_parent {
                return Err(Error::generic(
                    "Cannot derive output schema from SchemaPatch with operations at multiple \
                     nesting levels. Operations must all be at the same level.",
                ));
            }

            match op {
                PatchOp::Drop { path } => {
                    let field_name = path
                        .last()
                        .ok_or_else(|| Error::generic("Cannot drop field with empty path"))?;
                    // Remove the field
                    result_fields.retain(|f| f.name != *field_name);
                }
                PatchOp::Add { after, name, expr } => {
                    // Synthesize the type from the expression
                    let data_type = synthesize_type(&expr, input_schema).ok_or_else(|| {
                        Error::generic(format!(
                            "Cannot infer type for added field '{}'. Expression type cannot be \
                             synthesized.",
                            name
                        ))
                    })?;

                    let new_field = StructField::nullable(&name, data_type);

                    // Find insertion position
                    let insert_pos = match &after {
                        Some(predecessor) => {
                            let after_name = predecessor.last();
                            // Find the position after the predecessor
                            result_fields
                                .iter()
                                .position(|f| after_name.map(|n| f.name == *n).unwrap_or(false))
                                .map(|pos| pos + 1)
                                .unwrap_or(result_fields.len())
                        }
                        None => 0, // Prepend
                    };

                    result_fields.insert(insert_pos, new_field);
                }
            }
        }

        // Reconstruct the schema
        match &common_parent {
            Some(path) => {
                // Rebuild the schema with the modified nested struct
                rebuild_with_nested(input_schema, path, result_fields)
            }
            None => StructType::try_new(result_fields),
        }
    }
}

/// Navigates to a nested struct type at the given path.
fn get_nested_struct_type<'a>(
    schema: &'a StructType,
    path: &ColumnName,
) -> DeltaResult<&'a StructType> {
    let mut current = schema;

    for segment in path.iter() {
        let field = current
            .field(segment)
            .ok_or_else(|| Error::generic(format!("Field '{}' not found in schema", segment)))?;

        match &field.data_type {
            DataType::Struct(nested) => current = nested,
            _ => {
                return Err(Error::generic(format!(
                    "Field '{}' is not a struct type",
                    segment
                )))
            }
        }
    }

    Ok(current)
}

/// Rebuilds the schema with modified fields at the given nested path.
fn rebuild_with_nested(
    schema: &StructType,
    path: &ColumnName,
    new_fields: Vec<StructField>,
) -> DeltaResult<StructType> {
    let path_vec: Vec<_> = path.iter().map(|s| s.as_str()).collect();
    rebuild_recursive(schema, &path_vec, 0, new_fields)
}

fn rebuild_recursive(
    schema: &StructType,
    path: &[&str],
    depth: usize,
    new_fields: Vec<StructField>,
) -> DeltaResult<StructType> {
    if depth >= path.len() {
        // We've reached the target depth, use the new fields
        return StructType::try_new(new_fields);
    }

    let target_name = path[depth];
    let mut rebuilt_fields = Vec::new();

    for field in schema.fields() {
        if field.name == target_name {
            // This is the field we need to modify
            match &field.data_type {
                DataType::Struct(nested) => {
                    let rebuilt_nested = rebuild_recursive(nested, path, depth + 1, new_fields)?;
                    rebuilt_fields.push(StructField::new(
                        &field.name,
                        DataType::Struct(Box::new(rebuilt_nested)),
                        field.nullable,
                    ));
                    // We've used new_fields, so we need to return early for remaining fields
                    for remaining in schema.fields().skip(rebuilt_fields.len()) {
                        rebuilt_fields.push(remaining.clone());
                    }
                    return StructType::try_new(rebuilt_fields);
                }
                _ => {
                    return Err(Error::generic(format!(
                        "Field '{}' is not a struct type",
                        target_name
                    )))
                }
            }
        } else {
            rebuilt_fields.push(field.clone());
        }
    }

    Err(Error::generic(format!(
        "Field '{}' not found in schema",
        target_name
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::schema::StructType;

    fn test_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable(
                    "a",
                    DataType::Struct(Box::new(
                        StructType::try_new([
                            StructField::nullable("b", DataType::INTEGER),
                            StructField::nullable("c", DataType::STRING),
                        ])
                        .unwrap(),
                    )),
                ),
                StructField::nullable("d", DataType::INTEGER),
            ])
            .unwrap(),
        )
    }

    fn dummy_expr() -> ExpressionRef {
        Arc::new(Expression::literal(42))
    }

    // ==================== Builder API tests ====================

    #[rstest]
    #[case::drop_top_level(
        |b: SchemaPatchBuilder| b.drop_field(["d"]),
        vec![PatchOp::Drop { path: ColumnName::new(["d"]) }]
    )]
    #[case::drop_nested(
        |b: SchemaPatchBuilder| b.drop_field(["a", "c"]),
        vec![PatchOp::Drop { path: ColumnName::new(["a", "c"]) }]
    )]
    #[case::add_after_field(
        |b: SchemaPatchBuilder| b.add_field(["d"], "e", dummy_expr()),
        vec![PatchOp::Add { after: Some(ColumnName::new(["d"])), name: "e".into(), expr: dummy_expr() }]
    )]
    #[case::replace_same_name(
        |b: SchemaPatchBuilder| b.replace_field(["d"], dummy_expr()),
        vec![
            PatchOp::Drop { path: ColumnName::new(["d"]) },
            PatchOp::Add { after: Some(ColumnName::new(["d"])), name: "d".into(), expr: dummy_expr() },
        ]
    )]
    #[case::replace_different_name(
        |b: SchemaPatchBuilder| b.replace_field_as(["d"], "e", dummy_expr()),
        vec![
            PatchOp::Drop { path: ColumnName::new(["d"]) },
            PatchOp::Add { after: Some(ColumnName::new(["d"])), name: "e".into(), expr: dummy_expr() },
        ]
    )]
    fn builder_creates_expected_ops(
        #[case] build_fn: fn(SchemaPatchBuilder) -> SchemaPatchBuilder,
        #[case] expected_ops: Vec<PatchOp>,
    ) {
        let patch = build_fn(SchemaPatchBuilder::new(test_schema()))
            .build()
            .unwrap();
        assert_eq!(patch.ops(), &expected_ops);
    }

    #[test]
    fn sorting_puts_drops_before_adds() {
        let patch = SchemaPatchBuilder::new(test_schema())
            .add_field(["d"], "e", dummy_expr())
            .drop_field(["d"])
            .build()
            .unwrap();

        assert!(matches!(&patch.ops()[0], PatchOp::Drop { .. }));
        assert!(matches!(&patch.ops()[1], PatchOp::Add { .. }));
    }

    // ==================== Validation error tests ====================

    #[rstest]
    #[case::double_drop(
        |b: SchemaPatchBuilder| b.drop_field(["d"]).drop_field(["d"]),
        "Double drop"
    )]
    #[case::double_add(
        |b: SchemaPatchBuilder| b.add_field(["d"], "e", dummy_expr()).add_field(["d"], "e", dummy_expr()),
        "Double add"
    )]
    #[case::parent_child_conflict(
        |b: SchemaPatchBuilder| b.drop_field(["a"]).drop_field(["a", "b"]),
        "parent-child conflict"
    )]
    fn build_validation_errors(
        #[case] build_fn: fn(SchemaPatchBuilder) -> SchemaPatchBuilder,
        #[case] expected_msg: &str,
    ) {
        let result = build_fn(SchemaPatchBuilder::new(test_schema())).build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(expected_msg));
    }

    // ==================== ExpressionStructPatch conversion tests ====================

    #[rstest]
    #[case::top_level(vec!["d"], None, "d")]
    #[case::nested(vec!["a", "c"], Some(ColumnName::new(["a"])), "c")]
    fn into_struct_patch_sets_correct_input_path(
        #[case] drop_path: Vec<&str>,
        #[case] expected_input_path: Option<ColumnName>,
        #[case] expected_field: &str,
    ) {
        let patch = SchemaPatchBuilder::new(test_schema())
            .drop_field(drop_path)
            .build()
            .unwrap();

        let struct_patch = patch.into_struct_patch().unwrap();

        assert_eq!(struct_patch.input_path(), expected_input_path.as_ref());
        assert!(struct_patch.field_patches.contains_key(expected_field));
    }

    #[test]
    fn into_struct_patch_rejects_mixed_nesting_levels() {
        let patch = SchemaPatchBuilder::new(test_schema())
            .drop_field(["d"])
            .drop_field(["a", "c"])
            .build()
            .unwrap();

        let result = patch.into_struct_patch();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("multiple nesting levels"));
    }

    // ==================== into_struct_type tests ====================

    #[test]
    fn into_struct_type_drop_field() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .drop_field(["d"])
            .build()
            .unwrap();

        let output = patch.into_struct_type(&schema).unwrap();

        // Should only have field "a" remaining
        assert_eq!(output.fields().count(), 1);
        assert!(output.field("a").is_some());
        assert!(output.field("d").is_none());
    }

    #[test]
    fn into_struct_type_add_field() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .add_field(["d"], "e", Expression::literal(42i64))
            .build()
            .unwrap();

        let output = patch.into_struct_type(&schema).unwrap();

        // Should have fields: a, d, e
        assert_eq!(output.fields().count(), 3);
        let e_field = output.field("e").expect("field 'e' should exist");
        assert_eq!(e_field.data_type, DataType::LONG);
    }

    #[test]
    fn into_struct_type_replace_field() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .replace_field(["d"], Expression::literal("hello"))
            .build()
            .unwrap();

        let output = patch.into_struct_type(&schema).unwrap();

        // Should still have 2 fields, but "d" is now STRING
        assert_eq!(output.fields().count(), 2);
        let d_field = output.field("d").expect("field 'd' should exist");
        assert_eq!(d_field.data_type, DataType::STRING);
    }

    #[test]
    fn into_struct_type_nested_drop() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .drop_field(["a", "c"])
            .build()
            .unwrap();

        let output = patch.into_struct_type(&schema).unwrap();

        // Top-level should still have 2 fields
        assert_eq!(output.fields().count(), 2);

        // Nested struct "a" should only have "b" remaining
        let a_field = output.field("a").expect("field 'a' should exist");
        if let DataType::Struct(nested) = &a_field.data_type {
            assert_eq!(nested.fields().count(), 1);
            assert!(nested.field("b").is_some());
            assert!(nested.field("c").is_none());
        } else {
            panic!("Expected 'a' to be a struct");
        }
    }

    #[test]
    fn into_struct_type_nested_add() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .add_field(["a", "c"], "new_field", Expression::literal(true))
            .build()
            .unwrap();

        let output = patch.into_struct_type(&schema).unwrap();

        // Nested struct "a" should have b, c, new_field
        let a_field = output.field("a").expect("field 'a' should exist");
        if let DataType::Struct(nested) = &a_field.data_type {
            assert_eq!(nested.fields().count(), 3);
            let new_field = nested
                .field("new_field")
                .expect("field 'new_field' should exist");
            assert_eq!(new_field.data_type, DataType::BOOLEAN);
        } else {
            panic!("Expected 'a' to be a struct");
        }
    }

    #[test]
    fn into_struct_type_prepend_field() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .prepend_field("first", Expression::literal(1.5f64))
            .build()
            .unwrap();

        let output = patch.into_struct_type(&schema).unwrap();

        // Should have 3 fields with "first" at position 0
        assert_eq!(output.fields().count(), 3);
        let fields: Vec<_> = output.fields().collect();
        assert_eq!(fields[0].name, "first");
        assert_eq!(fields[0].data_type, DataType::DOUBLE);
    }

    #[test]
    fn into_struct_type_empty_patch_returns_input() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone()).build().unwrap();

        let output = patch.into_struct_type(&schema).unwrap();
        assert_eq!(&output, schema.as_ref());
    }

    #[test]
    fn into_struct_type_rejects_mixed_nesting_levels() {
        let schema = test_schema();
        let patch = SchemaPatchBuilder::new(schema.clone())
            .drop_field(["d"])
            .drop_field(["a", "c"])
            .build()
            .unwrap();

        let result = patch.into_struct_type(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("multiple nesting levels"));
    }
}
