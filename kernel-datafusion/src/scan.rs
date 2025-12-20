//! Native scan lowering using DataFusion's ParquetSource and JsonSource.
//!
//! # Schema Nullability Handling
//!
//! When reading parquet files, we relax the kernel schema's nullability constraints
//! because parquet files store nullable parents' children as nullable. After reading,
//! we wrap the scan with a `NullabilityValidationExec` to enforce the kernel's
//! nullability semantics at runtime.

use std::sync::Arc;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::{
    source::DataSourceExec,
    file_scan_config::FileScanConfigBuilder,
    PartitionedFile,
    file_groups::FileGroup,
};
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_json::source::JsonSource;
use datafusion_execution::object_store::ObjectStoreUrl;

use delta_kernel::plans::{ScanNode, FileType};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::schema::{relax_nullability_for_reading, collect_fields_needing_validation};
use crate::error::{DfResult, DfError};
use crate::exec::NullabilityValidationExec;

/// Convert a kernel ScanNode into a DataFusion DataSourceExec with ParquetSource.
pub fn compile_scan(
    node: &ScanNode,
    session_state: &SessionState,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    match node.file_type {
        FileType::Parquet => compile_parquet_scan(node, session_state),
        FileType::Json => compile_json_scan(node, session_state),
    }
}

fn compile_parquet_scan(node: &ScanNode, _session_state: &SessionState) -> DfResult<Arc<dyn ExecutionPlan>> {
    // 1. Collect fields needing validation BEFORE relaxing the schema
    let validations = collect_fields_needing_validation(&node.schema);
    
    // 2. Relax schema for reading - nullable parents get nullable children
    // This avoids DataFusion's "Cannot cast nullable to non-nullable" error
    let relaxed_schema = relax_nullability_for_reading(&node.schema);
    let arrow_schema = (&relaxed_schema).try_into_arrow()
        .map_err(|e| DfError::PlanCompilation(format!("Failed to convert kernel schema to Arrow: {}", e)))?;
    let file_schema = Arc::new(arrow_schema);
    
    // 3. Convert FileMeta to PartitionedFile
    // All files go into a single group to preserve ordering (important for KDFs)
    let partitioned_files: Vec<PartitionedFile> = node.files.iter()
        .map(|file_meta| {
            PartitionedFile::new(file_meta.location.path().to_string(), file_meta.size as u64)
        })
        .collect();
    
    // Create a single FileGroup to preserve ordering
    let file_group = FileGroup::new(partitioned_files);
    
    // 4. Extract base URL for object store registration
    // Use the first file's directory as the base
    let first_file_url = node.files.first()
        .ok_or_else(|| DfError::PlanCompilation("Scan node has no files".to_string()))?
        .location.clone();
    
    // ObjectStoreUrl needs only scheme + authority (e.g. "file://" not "file:///path")
    let base_url_str = format!("{}://{}", 
        first_file_url.scheme(), 
        first_file_url.host_str().unwrap_or("")
    );
    let object_store_url = ObjectStoreUrl::parse(&base_url_str)?;
    
    // 5. Create ParquetSource
    let parquet_source = Arc::new(ParquetSource::default());
    
    // 6. Build FileScanConfig using the builder
    let file_scan_config = FileScanConfigBuilder::new(
        object_store_url,
        file_schema,
        parquet_source,
    )
    .with_file_group(file_group) // Single group preserves file order
    .with_projection_indices(None) // No column pruning for now
    .build();
    
    // 7. Create DataSourceExec from the config
    let scan_exec = DataSourceExec::from_data_source(file_scan_config);
    
    // 8. Wrap with validation if kernel schema has non-nullable nested fields
    if validations.is_empty() {
        Ok(scan_exec)
    } else {
        Ok(Arc::new(NullabilityValidationExec::new(scan_exec, validations)))
    }
}

fn compile_json_scan(node: &ScanNode, _session_state: &SessionState) -> DfResult<Arc<dyn ExecutionPlan>> {
    // 1. Convert kernel schema to Arrow schema
    let arrow_schema = node.schema.as_ref().try_into_arrow()
        .map_err(|e| DfError::PlanCompilation(format!("Failed to convert kernel schema to Arrow: {}", e)))?;
    let file_schema = Arc::new(arrow_schema);
    
    // 2. Convert FileMeta to PartitionedFile
    // All files go into a single group to preserve ordering (important for KDFs)
    let partitioned_files: Vec<PartitionedFile> = node.files.iter()
        .map(|file_meta| {
            PartitionedFile::new(file_meta.location.path().to_string(), file_meta.size as u64)
        })
        .collect();
    
    // Create a single FileGroup to preserve ordering
    let file_group = FileGroup::new(partitioned_files);
    
    // 3. Extract base URL for object store registration
    let first_file_url = node.files.first()
        .ok_or_else(|| DfError::PlanCompilation("Scan node has no files".to_string()))?
        .location.clone();
    
    // ObjectStoreUrl needs only scheme + authority (e.g. "file://" not "file:///path")
    let base_url_str = format!("{}://{}", 
        first_file_url.scheme(), 
        first_file_url.host_str().unwrap_or("")
    );
    let object_store_url = ObjectStoreUrl::parse(&base_url_str)?;
    
    // 4. Create JsonSource (newline-delimited JSON)
    let json_source = Arc::new(JsonSource::default());
    
    // 5. Build FileScanConfig using the builder
    let file_scan_config = FileScanConfigBuilder::new(
        object_store_url,
        file_schema,
        json_source,
    )
    .with_file_group(file_group) // Single group preserves file order
    .with_projection_indices(None) // No column pruning for now
    .build();
    
    // 6. Create DataSourceExec from the config
    Ok(DataSourceExec::from_data_source(file_scan_config))
}

