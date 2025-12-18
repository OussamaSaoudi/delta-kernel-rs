//! Error types for DataFusion executor.

use thiserror::Error;

pub type DfResult<T> = Result<T, DfError>;

#[derive(Debug, Error)]
pub enum DfError {
    #[error("Delta kernel error: {0}")]
    Kernel(#[from] delta_kernel::Error),
    
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    
    #[error("Expression lowering failed: {0}")]
    ExpressionLowering(String),
    
    #[error("Plan compilation failed: {0}")]
    PlanCompilation(String),
    
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<DfError> for delta_kernel::Error {
    fn from(e: DfError) -> Self {
        delta_kernel::Error::generic(e.to_string())
    }
}

