//! KDF filter execution node - applies KDF and physically filters RecordBatch.

use std::any::Any;
use std::sync::Arc;
use std::fmt;

use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;

use delta_kernel::plans::kdf_state::FilterKdfState;

/// Custom physical operator for FilterByKDF nodes.
pub struct KdfFilterExec {
    input: Arc<dyn ExecutionPlan>,
    kdf_state: FilterKdfState,
    schema: ArrowSchemaRef,
}

impl KdfFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        kdf_state: FilterKdfState,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            kdf_state,
            schema,
        }
    }
}

impl std::fmt::Debug for KdfFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KdfFilterExec")
    }
}

impl DisplayAs for KdfFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KdfFilterExec")
    }
}

impl ExecutionPlan for KdfFilterExec {
    fn name(&self) -> &str {
        "KdfFilterExec"
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    
    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        todo!("KdfFilterExec properties")
    }
    
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!("KdfFilterExec with_new_children")
    }
    
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!("KdfFilterExec execute")
    }
}

