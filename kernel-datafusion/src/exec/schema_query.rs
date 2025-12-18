//! Schema query execution node - reads parquet footer and stores schema in state.

use std::any::Any;
use std::sync::Arc;
use std::fmt;

use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType};
use datafusion::arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema};

use delta_kernel::plans::kdf_state::SchemaReaderState;

/// Custom physical operator for SchemaQueryNode.
///
/// Reads parquet metadata, stores schema in state, produces no output rows.
pub struct SchemaQueryExec {
    file_path: String,
    state: SchemaReaderState,
    schema: ArrowSchemaRef,
}

impl SchemaQueryExec {
    pub fn new(file_path: String, state: SchemaReaderState) -> Self {
        let schema = Arc::new(ArrowSchema::empty());
        Self {
            file_path,
            state,
            schema,
        }
    }
}

impl std::fmt::Debug for SchemaQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaQueryExec")
    }
}

impl DisplayAs for SchemaQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaQueryExec: {}", self.file_path)
    }
}

impl ExecutionPlan for SchemaQueryExec {
    fn name(&self) -> &str {
        "SchemaQueryExec"
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    
    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        todo!("SchemaQueryExec properties")
    }
    
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!("SchemaQueryExec execute")
    }
}

