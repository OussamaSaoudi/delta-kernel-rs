//! Schema query execution node - reads parquet footer and stores schema in state.
//!
//! This operator reads the parquet file footer to extract the schema and stores it
//! in the `SchemaStoreState`. It produces no output rows (empty stream).
//!
//! Used by the kernel to detect V2 checkpoints by checking for a 'sidecar' column.

use std::any::Any;
use std::sync::Arc;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType, SendableRecordBatchStream, RecordBatchStream, PlanProperties};
use datafusion::arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema};
use datafusion::arrow::array::RecordBatch;
use datafusion_physical_plan::execution_plan::{EmissionType, Boundedness};
use datafusion_physical_plan::Partitioning;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_common::{Result as DfResult, DataFusionError};
use datafusion_execution::object_store::ObjectStoreUrl;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use futures::{Stream, FutureExt};

use delta_kernel::plans::kdf_state::SchemaReaderState;
use delta_kernel::schema::StructType;
use delta_kernel::engine::arrow_conversion::TryFromArrow;

/// Custom physical operator for SchemaQueryNode.
///
/// Reads parquet metadata from file footer, converts Arrow schema to kernel StructType,
/// stores schema in `SchemaStoreState`, and produces no output rows (empty stream).
pub struct SchemaQueryExec {
    /// The URL path to the parquet file
    file_path: url::Url,
    /// State to store the extracted schema
    state: SchemaReaderState,
    /// Output schema (always empty - we don't produce data rows)
    schema: ArrowSchemaRef,
    /// Plan properties
    properties: PlanProperties,
}

impl SchemaQueryExec {
    /// Create a new SchemaQueryExec for the given file path and state.
    ///
    /// The ObjectStore will be resolved from the TaskContext at execution time.
    pub fn new(file_path: url::Url, state: SchemaReaderState) -> Self {
        let schema = Arc::new(ArrowSchema::empty());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            file_path,
            state,
            schema,
            properties,
        }
    }
    
    /// Get the file path being queried.
    pub fn file_path(&self) -> &url::Url {
        &self.file_path
    }
}

impl std::fmt::Debug for SchemaQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SchemaQueryExec")
            .field("file_path", &self.file_path.to_string())
            .finish()
    }
}

impl DisplayAs for SchemaQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaQueryExec: path={}", self.file_path)
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
    
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    
    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let state = self.state.clone();
        let file_path = self.file_path.clone();
        let schema = self.schema.clone();
        
        // Build ObjectStoreUrl from our path (scheme + authority only)
        let object_store_url_str = format!(
            "{}://{}",
            file_path.scheme(),
            file_path.host_str().unwrap_or("")
        );
        let object_store_url = ObjectStoreUrl::parse(&object_store_url_str)?;
        
        // Get the ObjectStore from RuntimeEnv
        let object_store = context.runtime_env().object_store(&object_store_url)?;
        
        // Build the path for the parquet file
        let path = object_store::path::Path::from(file_path.path());
        
        // Create the async future that reads metadata and stores schema
        let read_future = async move {
            // Get file metadata (for size - needed for efficient footer reading)
            let meta = object_store.head(&path).await.map_err(|e| {
                DataFusionError::External(Box::new(e))
            })?;
            
            // Create ParquetObjectReader with known file size
            let mut reader = ParquetObjectReader::new(object_store, path)
                .with_file_size(meta.size);
            
            // Read parquet metadata (just the footer, not the data)
            let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default())
                .await
                .map_err(|e| DataFusionError::ParquetError(Box::new(e)))?;
            
            // Convert Arrow schema to kernel StructType
            let kernel_schema = StructType::try_from_arrow(metadata.schema().as_ref())
                .map(Arc::new)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            
            // Store in SchemaStoreState
            match &state {
                SchemaReaderState::SchemaStore(store) => {
                    store.store(kernel_schema);
                }
            }
            
            Ok::<(), DataFusionError>(())
        };
        
        // Wrap in a stream that executes the future once and then ends
        Ok(Box::pin(SchemaQueryStream::new(read_future.boxed(), schema)))
    }
}

/// A stream that executes the schema query future and produces no data.
///
/// The stream runs the async metadata read operation and then terminates.
/// It doesn't yield any RecordBatches since schema queries produce no data.
struct SchemaQueryStream {
    /// The future that reads metadata - None after it completes
    future: Option<Pin<Box<dyn std::future::Future<Output = Result<(), DataFusionError>> + Send>>>,
    /// Output schema (empty)
    schema: ArrowSchemaRef,
    /// Whether we've completed
    done: bool,
}

impl SchemaQueryStream {
    fn new(
        future: Pin<Box<dyn std::future::Future<Output = Result<(), DataFusionError>> + Send>>,
        schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            future: Some(future),
            schema,
            done: false,
        }
    }
}

impl Stream for SchemaQueryStream {
    type Item = DfResult<RecordBatch>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        
        if let Some(future) = self.future.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    self.done = true;
                    self.future = None;
                    // Schema query produces no data rows - just end the stream
                    Poll::Ready(None)
                }
                Poll::Ready(Err(e)) => {
                    self.done = true;
                    self.future = None;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for SchemaQueryStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::plans::kdf_state::SchemaStoreState;
    
    #[test]
    fn test_schema_query_exec_schema() {
        let path = url::Url::parse("file:///tmp/test.parquet").unwrap();
        let state = SchemaReaderState::SchemaStore(SchemaStoreState::new());
        let exec = SchemaQueryExec::new(path, state);
        
        let schema = exec.schema();
        // Schema query produces empty output schema
        assert_eq!(schema.fields().len(), 0);
    }
    
    #[test]
    fn test_schema_query_exec_properties() {
        let path = url::Url::parse("file:///tmp/test.parquet").unwrap();
        let state = SchemaReaderState::SchemaStore(SchemaStoreState::new());
        let exec = SchemaQueryExec::new(path.clone(), state);
        
        assert_eq!(exec.name(), "SchemaQueryExec");
        assert_eq!(exec.file_path(), &path);
        assert!(exec.children().is_empty());
    }
}
