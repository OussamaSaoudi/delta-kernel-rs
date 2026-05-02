use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_physical_plan::ExecutionPlan;
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationBatchRegistry")
            .finish_non_exhaustive()
    }
}

pub fn build_relation_ref_exec(
    handle: &RelationHandle,
    registry: &RelationBatchRegistry,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let schema: delta_kernel::arrow::datatypes::SchemaRef =
        Arc::new(handle.schema.as_ref().try_into_arrow().map_err(|e| {
            crate::error::internal_error(format!("relation schema conversion: {e}"))
        })?);
    let partitions = vec![registry.get_cloned(handle.id).unwrap_or_default()];
    MemorySourceConfig::try_new_exec(&partitions, schema, None)
        .map(|exec| exec as Arc<dyn ExecutionPlan>)
        .map_err(crate::error::datafusion_err_to_delta)
}
