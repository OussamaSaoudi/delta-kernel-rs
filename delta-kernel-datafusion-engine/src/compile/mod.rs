//! Kernel plan -> DataFusion [`LogicalPlan`] compilation.
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan

use std::sync::Arc;

use delta_kernel::Engine;

pub mod expr_translator;
mod json_parse;
pub mod logical;
pub mod stamp_udf;

pub use logical::compile_plan;

/// Context shared by plan lowerings that need kernel I/O services.
#[derive(Clone)]
pub struct CompileContext {
    /// Kernel [`Engine`] used for deletion-vector reads.
    pub engine: Arc<dyn Engine>,
}

impl CompileContext {
    /// Builds a compiler context using `engine` for I/O services.
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self { engine }
    }
}
