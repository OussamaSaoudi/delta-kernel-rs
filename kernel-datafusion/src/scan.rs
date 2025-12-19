//! Native scan lowering for Parquet and JSON files.

use std::sync::Arc;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion_common::Statistics;
use arrow::datatypes::Schema as ArrowSchema;
use object_store::path::Path as ObjectPath;

use delta_kernel::plans::{ScanNode, FileType};
use delta_kernel::FileMeta;
use crate::error::{DfResult, DfError};

/// Convert a kernel ScanNode into a native DataFusion scan (ParquetExec or NdJsonExec).
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
    // For a working parquet scan, we need:
    // 1. Convert kernel schema to Arrow schema
    // 2. Register files with an object_store
    // 3. Create FileScanConfig with the files
    // 4. Create ParquetExec
    
    // This is complex and requires proper object_store integration
    // For now, return unsupported with detailed requirements
    Err(DfError::Unsupported(
        format!(
            "Parquet scan compilation requires: \n\
             1. Kernel schema -> Arrow schema conversion\n\
             2. Object store registration for file URLs\n\
             3. FileScanConfig + ParquetExec creation\n\
             Files to scan: {} parquet files",
            node.files.len()
        )
    ))
}

fn compile_json_scan(node: &ScanNode, _session_state: &SessionState) -> DfResult<Arc<dyn ExecutionPlan>> {
    Err(DfError::Unsupported(
        format!(
            "JSON scan compilation requires similar setup to Parquet.\n\
             Files to scan: {} json files",
            node.files.len()
        )
    ))
}
