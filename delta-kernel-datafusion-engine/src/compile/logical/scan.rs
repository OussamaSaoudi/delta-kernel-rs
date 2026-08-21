//! Lowering for static Parquet and JSON scan operators.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use delta_kernel::plans::ir::nodes::{FileType, ScanFile, ScanJson, ScanParquet};
use delta_kernel::schema::SchemaRef;

use crate::exec::StaticScanTableProvider;

pub(super) fn scan_parquet_to_logical_plan(
    node: &ScanParquet,
) -> Result<LogicalPlan, DataFusionError> {
    lower_static_scan(
        &node.files,
        &node.file_constant_columns,
        &node.schema,
        FileType::Parquet,
    )
}

pub(super) fn scan_json_to_logical_plan(node: &ScanJson) -> Result<LogicalPlan, DataFusionError> {
    lower_static_scan(
        &node.files,
        &node.file_constant_columns,
        &node.schema,
        FileType::Json,
    )
}

fn lower_static_scan(
    files: &[ScanFile],
    constants: &[String],
    output_schema: &SchemaRef,
    file_type: FileType,
) -> Result<LogicalPlan, DataFusionError> {
    let provider: Arc<dyn TableProvider> = Arc::new(StaticScanTableProvider::try_new(
        files,
        constants,
        output_schema,
        file_type,
    )?);
    LogicalPlanBuilder::scan("delta_static_scan", provider_as_source(provider), None)?.build()
}
