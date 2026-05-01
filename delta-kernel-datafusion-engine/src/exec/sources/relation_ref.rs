use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::RelationHandle;

/// In-memory relation materialization used by sequential multi-plan execution.
#[derive(Default)]
pub struct RelationBatchRegistry {
    inner: Mutex<HashMap<u64, Vec<RecordBatch>>>,
}

impl RelationBatchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, handle_id: u64, batches: Vec<RecordBatch>) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(handle_id, batches);
    }

    pub fn get_cloned(&self, handle_id: u64) -> Option<Vec<RecordBatch>> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.get(&handle_id).cloned()
    }
}

impl Debug for RelationBatchRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationBatchRegistry")
            .finish_non_exhaustive()
    }
}

pub struct RelationRefExec {
    handle: RelationHandle,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    registry: Arc<RelationBatchRegistry>,
    properties: Arc<PlanProperties>,
}

impl RelationRefExec {
    pub fn new(
        handle: RelationHandle,
        registry: Arc<RelationBatchRegistry>,
    ) -> Result<Self, DeltaError> {
        let schema: delta_kernel::arrow::datatypes::SchemaRef = Arc::new(
            handle
                .schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e| crate::error::internal_error(format!("relation schema conversion: {e}")))?,
        );
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            handle,
            schema,
            registry,
            properties,
        })
    }
}

impl Debug for RelationRefExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationRefExec")
            .field("name", &self.handle.name)
            .field("id", &self.handle.id)
            .finish()
    }
}

impl DisplayAs for RelationRefExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RelationRefExec(name={}, id={})", self.handle.name, self.handle.id)
    }
}

impl ExecutionPlan for RelationRefExec {
    fn name(&self) -> &str {
        "RelationRefExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "RelationRefExec cannot have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RelationRefExec supports only partition 0, got {partition}"
            )));
        }
        let batches = self.registry.get_cloned(self.handle.id).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "No batches registered for relation `{}` (id={})",
                self.handle.name, self.handle.id
            ))
        })?;
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.schema.clone(),
            None,
        )?))
    }
}
