//! Native DataFusion provider for kernel file scans with a static file list.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{FileType, ScanFile};
use delta_kernel::schema::SchemaRef;

use crate::compile::expr_translator::scalar_value_to_df;
use crate::error::plan_compilation;
use crate::exec::load_helpers::{
    adapter_factory_for, build_file_source, into_partitioned_file, strip_nested_metadata_only,
};

/// A static Parquet or NDJSON scan backed directly by DataFusion's `DataSourceExec`.
pub(crate) struct StaticScanTableProvider {
    schema: ArrowSchemaRef,
    object_store_url: datafusion_execution::object_store::ObjectStoreUrl,
    file_groups: Vec<FileGroup>,
    file_field_count: usize,
    file_type: FileType,
}

impl StaticScanTableProvider {
    pub(crate) fn try_new(
        files: &[ScanFile],
        file_constant_columns: &[String],
        output_schema: &SchemaRef,
        file_type: FileType,
    ) -> Result<Self, DataFusionError> {
        let file_field_count = output_schema.fields().len() - file_constant_columns.len();
        let kernel_arrow_schema: ArrowSchema = output_schema
            .as_ref()
            .try_into_arrow()
            .map_err(|error| plan_compilation(format!("static scan output schema: {error}")))?;
        let fields = kernel_arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index < file_field_count {
                    Arc::new(strip_nested_metadata_only(field))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(
            ArrowSchema::new(fields).with_metadata(kernel_arrow_schema.metadata().clone()),
        );

        let mut object_store_url = None;
        let mut file_groups = Vec::with_capacity(files.len());
        for file in files {
            if file.file_constants.len() != file_constant_columns.len() {
                return Err(plan_compilation(format!(
                    "static scan file has {} constants, expected {}",
                    file.file_constants.len(),
                    file_constant_columns.len()
                )));
            }
            let size = i64::try_from(file.meta.size).map_err(|_| {
                plan_compilation(format!("file size {} does not fit in i64", file.meta.size))
            })?;
            let (file_store, mut partitioned_file) =
                into_partitioned_file(&file.meta.location, size)?;
            if let Some(expected) = object_store_url.as_ref() {
                if expected != &file_store {
                    return Err(plan_compilation(format!(
                        "static scan spans object stores `{expected}` and `{file_store}`"
                    )));
                }
            } else {
                object_store_url = Some(file_store.clone());
            }
            partitioned_file.partition_values = file
                .file_constants
                .iter()
                .map(scalar_value_to_df)
                .collect::<Result<Vec<_>, _>>()?;
            file_groups.push(FileGroup::new(vec![partitioned_file]));
        }
        let object_store_url = object_store_url
            .unwrap_or_else(datafusion_execution::object_store::ObjectStoreUrl::local_filesystem);
        Ok(Self {
            schema,
            object_store_url,
            file_groups,
            file_field_count,
            file_type,
        })
    }
}

impl std::fmt::Debug for StaticScanTableProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StaticScanTableProvider")
            .field("files", &self.file_groups.len())
            .field("file_type", &self.file_type)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for StaticScanTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let source = build_file_source(
            self.file_type,
            &self.schema,
            self.file_field_count,
            projection.map(Vec::as_slice),
            false,
        )?;
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_groups(self.file_groups.clone())
            .with_limit(limit)
            .with_expr_adapter(adapter_factory_for(self.file_type))
            .build();
        Ok(Arc::new(DataSourceExec::new(Arc::new(config))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
