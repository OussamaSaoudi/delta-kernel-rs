// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Delta Kernel extension: Parquet root column matching by native parquet field ID.
//!
//! Delta Kernel (and Spark column-mapping tables) often write parquet files whose **physical**
//! column names differ from **logical** names while repeating stable parquet **field IDs**
//! (`PARQUET:field_id` in Arrow field metadata). DataFusion's default parquet adaptation matches
//! columns **by name**, which breaks renamed logical schemas.
//!
//! When the logical scan schema carries `PARQUET:field_id` on any **top-level** field,
//! [`maybe_adapt_physical_schema_for_parquet_field_ids`] clones the decoded physical Arrow schema
//! but renames each physical root field to the resolved logical name (field-ID match first, then
//! name fallback — mirroring Kernel parquet handlers).
//!
//! ## Caveats (same class as Kernel flat matching)
//!
//! - Only **root** Arrow fields / parquet columns are considered; nested struct paths are not
//!   independently resolved by field ID here.
//! - If pushdown filters reference renamed columns while staying wired to the raw physical schema,
//!   predicate pushdown may disagree with adapted expressions; callers disabling parquet predicate
//!   pushdown avoid this class of mismatch.

use std::sync::Arc;

use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Returns true when any root field on `logical_file_schema` carries parquet field-id metadata.
pub(crate) fn logical_root_has_any_parquet_field_id(logical_file_schema: &Schema) -> bool {
    logical_file_schema.fields().iter().any(|f| {
        f.metadata().contains_key(PARQUET_FIELD_ID_META_KEY)
    })
}

fn parse_parquet_field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

fn match_physical_root_to_logical_index(
    phys_field: &Field,
    logical_fields: &[FieldRef],
    logical_used: &mut [bool],
) -> Option<usize> {
    if let Some(pid) = parse_parquet_field_id(phys_field) {
        for (i, lf) in logical_fields.iter().enumerate() {
            if logical_used[i] {
                continue;
            }
            if parse_parquet_field_id(lf.as_ref()) == Some(pid) {
                return Some(i);
            }
        }
    }
    for (i, lf) in logical_fields.iter().enumerate() {
        if logical_used[i] {
            continue;
        }
        if lf.name() == phys_field.name() {
            return Some(i);
        }
    }
    None
}

/// Clone `physical_file_schema` but rename each root field to the Kernel/DataFusion logical name
/// resolved by parquet field ID (when present on the physical column) with name fallback.
///
/// Indices remain identical to `physical_file_schema`; only names (and duplicated logical metadata
/// hooks) change so [`DefaultPhysicalExprAdapter`] maps logical expressions onto decoded batches.
pub(crate) fn rename_physical_roots_for_kernel_field_id_matching(
    logical_file_schema: &SchemaRef,
    physical_file_schema: &SchemaRef,
) -> Result<SchemaRef> {
    let logical_roots = logical_file_schema.fields();
    let mut logical_used = vec![false; logical_roots.len()];
    let mut out = Vec::with_capacity(physical_file_schema.fields().len());

    for phys_field in physical_file_schema.fields().iter() {
        let matched = match_physical_root_to_logical_index(
            phys_field.as_ref(),
            logical_roots,
            &mut logical_used,
        );
        let new_field = if let Some(li) = matched {
            logical_used[li] = true;
            let logical_field = logical_roots[li].as_ref();
            Field::new(
                logical_field.name(),
                phys_field.data_type().clone(),
                phys_field.is_nullable(),
            )
            .with_metadata(logical_field.metadata().clone())
        } else {
            (**phys_field).clone()
        };
        out.push(Arc::new(new_field));
    }

    Ok(Arc::new(Schema::new_with_metadata(
        out,
        physical_file_schema.metadata().clone(),
    )))
}

pub(crate) fn maybe_adapt_physical_schema_for_parquet_field_ids(
    logical_file_schema: &SchemaRef,
    physical_file_schema: &SchemaRef,
) -> Result<SchemaRef> {
    if !logical_root_has_any_parquet_field_id(logical_file_schema) {
        return Ok(Arc::clone(physical_file_schema));
    }
    rename_physical_roots_for_kernel_field_id_matching(
        logical_file_schema,
        physical_file_schema,
    )
}

/// Decoder [`Schema`] columns keep parquet physical names; projection reordering narrows columns.
/// [`datafusion_physical_expr::utils::reassign_expr_columns`] looks columns up **by name**, so we
/// substitute logical names derived from [`maybe_adapt_physical_schema_for_parquet_field_ids`].
pub(crate) fn align_stream_schema_column_names_for_logical_projection(
    stream_schema: &Schema,
    adapted_physical_schema: &Schema,
    physical_schema_raw: &Schema,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(stream_schema.fields().len());
    for i in 0..stream_schema.fields().len() {
        let sf = stream_schema.field(i);
        let phys_idx = physical_schema_raw
            .index_of(sf.name())
            .map_err(DataFusionError::from)?;
        let logical_name = adapted_physical_schema.field(phys_idx).name();
        fields.push(Field::new(
            logical_name,
            sf.data_type().clone(),
            sf.is_nullable(),
        ));
    }
    Ok(Schema::new(fields))
}
