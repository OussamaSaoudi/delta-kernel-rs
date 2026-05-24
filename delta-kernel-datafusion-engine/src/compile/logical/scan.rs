//! Lowering for [`NodeKind::ScanParquet`] and [`NodeKind::ScanJson`].
//!
//! Both shapes share a file-format-independent listing-table builder ([`build_listing`])
//! and reject column-mapping / row-index schemas up-front so the DataFusion 53 listing
//! path is never asked to do work that needs DF54's expression-adapter wiring.
//!
//! [`NodeKind::ScanParquet`]: delta_kernel::plans::ir::plan::NodeKind::ScanParquet
//! [`NodeKind::ScanJson`]: delta_kernel::plans::ir::plan::NodeKind::ScanJson

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::{
    DataType as ArrowDataType, Schema as ArrowSchema,
};
use datafusion_common::error::DataFusionError;
use datafusion_common::DFSchema;
use datafusion_datasource::file_format::FileFormat as DfFileFormat;
use datafusion_datasource_json::file_format::JsonFormat;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan};
use datafusion_expr::LogicalPlanBuilder;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{ScanJsonNode, ScanParquetNode};
use delta_kernel::schema::{ColumnMetadataKey, MetadataColumnSpec, SchemaRef, StructField};
use delta_kernel::FileMeta;

use super::canonicalize::canonicalize_output_to_kernel_schema;
use crate::error::plan_compilation;

pub(super) fn scan_parquet_to_logical_plan(
    node: &ScanParquetNode,
) -> Result<LogicalPlan, DataFusionError> {
    reject_column_mapping(&node.schema, "ScanParquet")?;
    let arrow_schema = build_parquet_scan_arrow_schema(&node.schema)?;
    build_listing(
        &node.files,
        &node.schema,
        arrow_schema,
        Arc::new(ParquetFormat::default()),
        ".parquet",
    )
}

pub(super) fn scan_json_to_logical_plan(
    node: &ScanJsonNode,
) -> Result<LogicalPlan, DataFusionError> {
    reject_column_mapping(&node.schema, "ScanJson")?;
    let arrow_schema: ArrowSchema = node
        .schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| plan_compilation(format!("Logical Scan schema conversion failed: {e}")))?;
    build_listing(
        &node.files,
        &node.schema,
        arrow_schema,
        Arc::new(JsonFormat::default().with_newline_delimited(true)),
        ".json",
    )
}

/// Reject schemas annotated for column mapping. Physical-to-logical name reshape needs
/// DataFusion 54's `ListingTableConfig::with_expr_adapter_factory` to install
/// `FieldIdPhysicalExprAdapterFactory`; on DataFusion 53 the scan would silently fall
/// back to name-based decode and read nulls for columns whose physical name doesn't
/// match the logical schema. Surface a typed `plan_compilation` error at compile time so
/// callers fail fast.
fn reject_column_mapping(schema: &SchemaRef, node_label: &str) -> Result<(), DataFusionError> {
    if schema_has_column_mapping(schema.as_ref().fields()) {
        return Err(plan_compilation(format!(
            "{node_label}: column-mapping schemas require DataFusion 54 \
             (ListingTableConfig::with_expr_adapter_factory + \
             FieldIdPhysicalExprAdapterFactory); the engine currently targets DataFusion 53"
        )));
    }
    Ok(())
}

fn schema_has_column_mapping<'a>(fields: impl IntoIterator<Item = &'a StructField>) -> bool {
    fields.into_iter().any(field_has_column_mapping)
}

fn field_has_column_mapping(field: &StructField) -> bool {
    if field
        .metadata
        .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref())
        || field
            .metadata
            .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        return true;
    }
    data_type_has_column_mapping(field.data_type())
}

fn data_type_has_column_mapping(dt: &delta_kernel::schema::DataType) -> bool {
    use delta_kernel::schema::DataType;
    match dt {
        DataType::Struct(inner) => schema_has_column_mapping(inner.fields()),
        DataType::Array(arr) => data_type_has_column_mapping(arr.element_type()),
        // Walk BOTH key and value types; either side may carry struct fields with
        // column-mapping metadata. The matching helper in `exec/load_exec.rs` does the
        // same; a future consolidation into a shared helper must preserve key-side
        // recursion.
        DataType::Map(map) => {
            data_type_has_column_mapping(map.key_type())
                || data_type_has_column_mapping(map.value_type())
        }
        _ => false,
    }
}

/// File-format-independent body of the Scan lowering: empty-relation short-circuit,
/// listing-table construction, paths, options, and final canonicalization. The arrow
/// schema is passed in (Parquet caller rewrites the row-index field; JSON caller
/// passes the raw schema).
fn build_listing(
    files: &[FileMeta],
    kernel_schema: &SchemaRef,
    arrow_schema: ArrowSchema,
    format: Arc<dyn DfFileFormat>,
    file_extension: &str,
) -> Result<LogicalPlan, DataFusionError> {
    if files.is_empty() {
        let df_schema = Arc::new(DFSchema::try_from(arrow_schema).map_err(|e| {
            plan_compilation(format!("Logical Scan DF schema conversion failed: {e}"))
        })?);
        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        }));
    }
    // File-source planning rejects schemas stricter than the physical files (parquet
    // checkpoints commonly write `add.path` as nullable; JSON drops declared NOT NULL
    // on nested children). Pass the schema through as-is; per-batch nullability
    // re-assertion lives with the load executor (rejected at construction in DF53).
    let file_schema = Arc::new(arrow_schema);
    let partition_cols: Vec<(String, ArrowDataType)> = Vec::new();
    let options = ListingOptions::new(format)
        .with_file_extension(file_extension)
        .with_table_partition_cols(partition_cols)
        // Disable DataFusion file statistics collection. Kernel does its own file-level
        // skipping upstream of the engine, so the listing-table stats walk is redundant
        // here and just adds I/O.
        .with_collect_stat(false)
        .with_target_partitions(1);
    let paths = files
        .iter()
        .map(|f| ListingTableUrl::parse(f.location.as_str()))
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    // Column-mapping schemas are rejected up-front by `reject_column_mapping`, so the
    // listing table is only ever asked to decode against a logical name == physical
    // name schema. Once DataFusion 54 ships `ListingTableConfig::with_expr_adapter_factory`
    // we can install `FieldIdPhysicalExprAdapterFactory` here and drop the rejection.
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(options)
        .with_schema(Arc::clone(&file_schema));
    let listing: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
    let scan_plan = LogicalPlanBuilder::scan("scan", provider_as_source(listing), None)?.build()?;
    canonicalize_output_to_kernel_schema(scan_plan, kernel_schema)
}

/// Build the arrow schema used by the Parquet scan lowering. Rejects schemas that declare
/// a [`MetadataColumnSpec::RowIndex`] column: kernel-driven row-index materialization
/// needs the parquet `RowNumber` extension wired through DataFusion's virtual-column
/// decode path (`ParquetSource::with_row_number`), which is only available in DataFusion
/// 54. The crate targets DataFusion 53, so this returns a typed `plan_compilation`
/// rejection instead. The follow-up DF54 PR rewrites this branch to emit an int64 field
/// with the `RowNumber` extension.
fn build_parquet_scan_arrow_schema(schema: &SchemaRef) -> Result<ArrowSchema, DataFusionError> {
    if schema
        .index_of_metadata_column(&MetadataColumnSpec::RowIndex)
        .is_some()
    {
        return Err(plan_compilation(
            "ScanParquet: MetadataColumnSpec::RowIndex requires DataFusion 54 \
             (ParquetSource::with_row_number virtual column); the engine currently \
             targets DataFusion 53",
        ));
    }
    schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| plan_compilation(format!("Logical Scan schema conversion failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::{DataType, MetadataValue, StructType};

    fn cm_field() -> StructField {
        StructField::nullable("c0", DataType::INTEGER).with_metadata([(
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(7),
        )])
    }

    #[test]
    fn reject_column_mapping_flat_field_is_rejected() {
        let schema = Arc::new(StructType::try_new([cm_field()]).unwrap());
        let err = reject_column_mapping(&schema, "ScanParquet").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("column-mapping"), "msg = {msg}");
        assert!(msg.contains("DataFusion 54"), "msg = {msg}");
    }

    #[test]
    fn reject_column_mapping_nested_struct_is_rejected() {
        let nested = StructType::try_new([cm_field()]).unwrap();
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("outer", DataType::Struct(Box::new(nested)))])
                .unwrap(),
        );
        let err = reject_column_mapping(&schema, "ScanJson").unwrap_err();
        assert!(err.to_string().contains("column-mapping"));
    }

    #[test]
    fn reject_column_mapping_plain_schema_is_accepted() {
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("c0", DataType::INTEGER)]).unwrap(),
        );
        reject_column_mapping(&schema, "ScanParquet").expect("plain schema must be accepted");
    }

    #[test]
    fn build_parquet_scan_arrow_schema_rejects_row_index() {
        let schema = Arc::new(
            StructType::try_new([
                StructField::nullable("c0", DataType::INTEGER),
                StructField::create_metadata_column("_rid", MetadataColumnSpec::RowIndex),
            ])
            .unwrap(),
        );
        let err = build_parquet_scan_arrow_schema(&schema).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("RowIndex"), "msg = {msg}");
        assert!(msg.contains("DataFusion 54"), "msg = {msg}");
    }

    #[test]
    fn build_parquet_scan_arrow_schema_plain_schema_succeeds() {
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("c0", DataType::INTEGER)]).unwrap(),
        );
        let arrow = build_parquet_scan_arrow_schema(&schema).expect("plain schema must convert");
        assert_eq!(arrow.fields().len(), 1);
        assert_eq!(arrow.field(0).name(), "c0");
    }
}
