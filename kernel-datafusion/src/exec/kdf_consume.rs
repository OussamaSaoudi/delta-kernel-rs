//! KDF consumer execution node - applies consumer KDF with Continue/Break semantics.

use std::any::Any;
use std::sync::Arc;
use std::fmt;

use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;

use delta_kernel::plans::kdf_state::ConsumerKdfState;

/// Custom physical operator for ConsumeByKDF nodes.
pub struct ConsumeKdfExec {
    input: Arc<dyn ExecutionPlan>,
    consumer_state: ConsumerKdfState,
    schema: ArrowSchemaRef,
}

impl ConsumeKdfExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        consumer_state: ConsumerKdfState,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            consumer_state,
            schema,
        }
    }
}

impl std::fmt::Debug for ConsumeKdfExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConsumeKdfExec")
    }
}

impl DisplayAs for ConsumeKdfExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConsumeKdfExec")
    }
}

impl ExecutionPlan for ConsumeKdfExec {
    fn name(&self) -> &str {
        "ConsumeKdfExec"
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    
    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        todo!("ConsumeKdfExec properties")
    }
    
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!("ConsumeKdfExec with_new_children")
    }
    
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!("ConsumeKdfExec execute")
    }
}

