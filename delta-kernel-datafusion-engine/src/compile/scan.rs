//! Native scan lowering using DataFusion file sources.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_json::source::JsonSource;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{FileType, ScanNode};
use delta_kernel::FileMeta;
use delta_kernel::object_store::path::Path as StorePath;

use crate::exec::{NullabilityValidationExec, RowIndexExec};

struct PreparedScanFiles {
    file_group: FileGroup,
    object_store_url: ObjectStoreUrl,
}

fn prepare_scan_files(files: &[FileMeta]) -> Result<PreparedScanFiles, DeltaError> {
    let first = files
        .first()
        .ok_or_else(|| crate::error::plan_compilation("Scan node has no files"))?;

    let partitioned_files = files
        .iter()
        .map(|f| file_meta_to_partitioned(f))
        .collect::<Result<Vec<_>, DeltaError>>()?;

    let base_url = format!("{}://{}", first.location.scheme(), first.location.host_str().unwrap_or(""));
    let object_store_url = ObjectStoreUrl::parse(&base_url).map_err(crate::error::datafusion_err_to_delta)?;

    Ok(PreparedScanFiles {
        file_group: FileGroup::new(partitioned_files),
        object_store_url,
    })
}

fn file_meta_to_partitioned(file: &FileMeta) -> Result<PartitionedFile, DeltaError> {
    let location = if file.location.scheme() == "file" {
        let fs_path = file.location.to_file_path().map_err(|()| {
            crate::error::plan_compilation(format!("file URL is not local path: {}", file.location))
        })?;
        StorePath::from_absolute_path(&fs_path).map_err(|e| {
            crate::error::plan_compilation(format!("object store path for {}: {e}", fs_path.display()))
        })?
    } else {
        StorePath::from(file.location.path())
    };
    let mut pf = PartitionedFile::new(location, file.size as u64);
    pf.object_meta.last_modified = Utc
        .timestamp_millis_opt(file.last_modified)
        .single()
        .unwrap_or_else(|| Utc.timestamp_nanos(0));
    Ok(pf)
}

/// Convert kernel [`ScanNode`] into DataFusion physical scan.
pub fn compile_scan(node: &ScanNode) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let target_schema: Arc<delta_kernel::arrow::datatypes::Schema> = Arc::new(
        node.schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e| crate::error::plan_compilation(format!("scan schema conversion: {e}")))?,
    );
    let prepared = prepare_scan_files(&node.files)?;

    let scan: Arc<dyn ExecutionPlan> = match node.file_type {
        FileType::Parquet => {
            let source = Arc::new(ParquetSource::new(target_schema.clone()));
            let cfg = FileScanConfigBuilder::new(prepared.object_store_url, source)
                .with_file_group(prepared.file_group)
                .with_projection_indices(None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build();
            DataSourceExec::from_data_source(cfg)
        }
        FileType::Json => {
            let source = Arc::new(JsonSource::new(target_schema.clone()));
            let cfg = FileScanConfigBuilder::new(prepared.object_store_url, source)
                .with_file_group(prepared.file_group)
                .with_projection_indices(None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build();
            DataSourceExec::from_data_source(cfg)
        }
    };

    // Phase 1.2: keep the scan-shape wrappers from the source crate. Validation
    // rules are wired empty for now; schema coercion still enforces the target
    // schema contract.
    let scan = Arc::new(NullabilityValidationExec::new(
        scan,
        Vec::new(),
        target_schema.clone(),
    ));

    if let Some(row_index_col) = &node.row_index_column {
        return Ok(Arc::new(RowIndexExec::new(scan, row_index_col.clone())));
    }
    Ok(scan)
}
