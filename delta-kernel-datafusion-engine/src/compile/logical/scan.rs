//! Lowering for [`NodeKind::ScanParquet`] and [`NodeKind::ScanJson`] plus row-index plumbing
//! helpers shared with [`super::ordered_union`].
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
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
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
use delta_kernel::schema::{MetadataColumnSpec, SchemaRef};
use delta_kernel::FileMeta;
use parquet::arrow::RowNumber;

use super::canonicalize::canonicalize_output_to_kernel_schema;
use crate::error::plan_compilation;
use crate::exec::FieldIdPhysicalExprAdapterFactory;

pub(super) fn scan_parquet_to_logical_plan(
    node: &ScanParquetNode,
) -> Result<LogicalPlan, DataFusionError> {
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
    // on nested children). Relax before passing in; `NullabilityEnforcingTableProvider`
    // re-asserts the strict contract per-batch.
    let file_schema = Arc::new(arrow_schema);
    let partition_cols: Vec<(String, ArrowDataType)> = Vec::new();
    let options = ListingOptions::new(format)
        .with_file_extension(file_extension)
        .with_table_partition_cols(partition_cols)
        // Disable DataFusion file statistics collection. The logical file schema may use
        // column-mapping names that do not exist physically; DataFusion's stats collector
        // then marks those columns all-null and constant-folds them to `Literal::NULL` before
        // field-id projection can run. Kernel does its own file-level skipping, so this path
        // is redundant here.
        .with_collect_stat(false)
        .with_target_partitions(1);
    let paths = files
        .iter()
        .map(|f| ListingTableUrl::parse(f.location.as_str()))
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    // Wire `FieldIdPhysicalExprAdapterFactory` so the parquet/json opener does
    // column-mapping-aware decode reshape (logical name + nested rename via
    // `PARQUET:field_id` / `delta.columnMapping.physicalName`). Eliminates the need for
    // any post-scan structural realignment.
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(options)
        .with_schema(Arc::clone(&file_schema))
        .with_expr_adapter_factory(Arc::new(FieldIdPhysicalExprAdapterFactory));
    let listing: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
    let scan_plan = LogicalPlanBuilder::scan("scan", provider_as_source(listing), None)?.build()?;
    canonicalize_output_to_kernel_schema(scan_plan, kernel_schema)
}

/// Build the arrow schema used by the Parquet scan lowering. When the kernel schema
/// declares a `MetadataColumnSpec::RowIndex` column, rewrite that field to a
/// row-number int64 with the parquet `RowNumber` extension so the parquet opener
/// materializes the row index instead of attempting to read it from the file.
fn build_parquet_scan_arrow_schema(schema: &SchemaRef) -> Result<ArrowSchema, DataFusionError> {
    let arrow: ArrowSchema = schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| plan_compilation(format!("Logical Scan schema conversion failed: {e}")))?;
    let Some(idx) = schema.index_of_metadata_column(&MetadataColumnSpec::RowIndex) else {
        return Ok(arrow);
    };
    let fields = arrow
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            if i == *idx {
                Arc::new(
                    ArrowField::new(field.name(), ArrowDataType::Int64, false)
                        .with_metadata(field.metadata().clone())
                        .with_extension_type(RowNumber),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    Ok(ArrowSchema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    ))
}
