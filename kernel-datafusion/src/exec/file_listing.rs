//! File listing execution node - outputs file metadata as RecordBatch rows.

use std::any::Any;
use std::sync::Arc;
use std::fmt;

use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType};
use datafusion::arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema, Field, DataType};

/// Custom physical operator for FileListingNode.
///
/// Produces a batch with columns: path (Utf8), size (Int64), modificationTime (Int64)
pub struct FileListingExec {
    path: url::Url,
    schema: ArrowSchemaRef,
}

impl FileListingExec {
    pub fn new(path: url::Url) -> Self {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, true),
            Field::new("modificationTime", DataType::Int64, true),
        ]));
        Self { path, schema }
    }
}

impl std::fmt::Debug for FileListingExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileListingExec")
    }
}

impl DisplayAs for FileListingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileListingExec: {}", self.path)
    }
}

impl ExecutionPlan for FileListingExec {
    fn name(&self) -> &str {
        "FileListingExec"
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    
    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        todo!("FileListingExec properties")
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
        todo!("FileListingExec execute")
    }
}

