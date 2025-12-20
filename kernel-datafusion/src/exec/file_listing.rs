//! File listing execution node - outputs file metadata as RecordBatch rows.
//!
//! Uses DataFusion's ObjectStore for async file listing with streaming
//! conversion to Arrow RecordBatches.

use std::any::Any;
use std::sync::Arc;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType, SendableRecordBatchStream, RecordBatchStream, PlanProperties};
use datafusion::arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema, Field, DataType};
use datafusion::arrow::array::{StringArray, Int64Array, RecordBatch};
use datafusion_physical_plan::execution_plan::{EmissionType, Boundedness};
use datafusion_physical_plan::Partitioning;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_common::{Result as DfResult, DataFusionError};
use datafusion_execution::object_store::ObjectStoreUrl;
use object_store::ObjectMeta;
use futures::{Stream, StreamExt, TryStreamExt, ready, stream};

/// Batch size for streaming file listing results.
/// Each RecordBatch will contain up to this many file entries.
const BATCH_SIZE: usize = 1024;

/// Custom physical operator for FileListingNode.
///
/// Produces a stream of batches with columns: path (Utf8), size (Int64), modificationTime (Int64).
/// Uses DataFusion's ObjectStore obtained from TaskContext at execution time.
pub struct FileListingExec {
    /// The URL path to list files from
    path: url::Url,
    /// Output schema
    schema: ArrowSchemaRef,
    /// Plan properties
    properties: PlanProperties,
}

impl FileListingExec {
    /// Create a new FileListingExec for the given path.
    ///
    /// The ObjectStore will be resolved from the TaskContext at execution time.
    pub fn new(path: url::Url) -> Self {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modificationTime", DataType::Int64, false),
        ]));
        
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1), // Single partition for listing
            EmissionType::Incremental, // Emits batches incrementally as files are listed
            Boundedness::Bounded, // File listing is bounded
        );
        
        Self { path, schema, properties }
    }
    
    /// Get the path being listed.
    pub fn path(&self) -> &url::Url {
        &self.path
    }
}

impl std::fmt::Debug for FileListingExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FileListingExec")
            .field("path", &self.path.to_string())
            .finish()
    }
}

impl DisplayAs for FileListingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileListingExec: path={}", self.path)
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
        // Build ObjectStoreUrl from our path (scheme + authority only)
        let object_store_url_str = format!(
            "{}://{}",
            self.path.scheme(),
            self.path.host_str().unwrap_or("")
        );
        let object_store_url = ObjectStoreUrl::parse(&object_store_url_str)?;
        
        // Get the ObjectStore from RuntimeEnv
        let object_store = context.runtime_env().object_store(&object_store_url)?;
        
        // Build the prefix path for listing
        let prefix = object_store::path::Path::from(self.path.path());
        
        // Create the async listing stream
        let list_stream = object_store.list(Some(&prefix));
        
        // Local filesystem doesn't guarantee sorted listing order.
        // Cloud stores (S3, Azure, GCS) return lexicographically sorted results,
        // but local filesystem does not. We need sorted order for Delta log files.
        // See: delta-kernel-rs/kernel/src/engine/default/filesystem.rs
        let needs_sorting = self.path.scheme() == "file";
        
        // Transform ObjectMeta stream -> RecordBatch stream using chunking
        let schema = self.schema.clone();
        
        if needs_sorting {
            // For local filesystem: collect all, sort, then stream
            Ok(Box::pin(SortedFileListingStream::new(list_stream, schema)))
        } else {
            // For cloud stores: stream directly (already sorted)
            Ok(Box::pin(FileListingStream::new(list_stream, schema)))
        }
    }
}

/// A stream that converts ObjectMeta items into RecordBatches.
///
/// Uses chunked batching to avoid materializing the entire listing.
/// Each batch contains up to BATCH_SIZE file entries.
struct FileListingStream {
    /// The underlying chunked stream of ObjectMeta results
    inner: Pin<Box<dyn Stream<Item = Vec<Result<ObjectMeta, object_store::Error>>> + Send>>,
    /// Output schema
    schema: ArrowSchemaRef,
}

impl FileListingStream {
    fn new(
        list_stream: futures::stream::BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
        schema: ArrowSchemaRef,
    ) -> Self {
        // Use chunks() to batch ObjectMeta items
        let chunked = list_stream.chunks(BATCH_SIZE);
        Self {
            inner: Box::pin(chunked),
            schema,
        }
    }
    
    /// Convert a chunk of ObjectMeta items into a RecordBatch.
    fn chunk_to_batch(
        chunk: Vec<Result<ObjectMeta, object_store::Error>>,
        schema: &ArrowSchemaRef,
    ) -> DfResult<RecordBatch> {
        // Collect successful metas, propagating any errors
        let metas: Vec<ObjectMeta> = chunk
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        if metas.is_empty() {
            // Return empty batch with correct schema
            return RecordBatch::try_new(schema.clone(), vec![
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ]).map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
        
        // Build Arrow arrays from ObjectMeta
        // Extract just the filename from the ObjectStore path to match what
        // LogSegmentBuilder expects (it joins filenames with log_root)
        let paths = StringArray::from_iter_values(
            metas.iter().map(|m| {
                // m.location is an object_store::path::Path
                // Get the filename (last path segment)
                m.location.filename().unwrap_or(m.location.as_ref())
            })
        );
        let sizes = Int64Array::from_iter_values(
            metas.iter().map(|m| m.size as i64)
        );
        let mod_times = Int64Array::from_iter_values(
            metas.iter().map(|m| m.last_modified.timestamp_millis())
        );
        
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(paths),
                Arc::new(sizes),
                Arc::new(mod_times),
            ],
        ).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for FileListingStream {
    type Item = DfResult<RecordBatch>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.as_mut().poll_next(cx)) {
            Some(chunk) => {
                let batch = Self::chunk_to_batch(chunk, &self.schema);
                Poll::Ready(Some(batch))
            }
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for FileListingStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

/// A stream that collects all ObjectMeta items, sorts them, then emits batches.
///
/// Used for local filesystem which doesn't guarantee sorted listing order.
/// Cloud object stores (S3, Azure, GCS) return lexicographically sorted results,
/// but local filesystem does not.
struct SortedFileListingStream {
    /// State of the stream
    state: SortedStreamState,
    /// Output schema
    schema: ArrowSchemaRef,
}

enum SortedStreamState {
    /// Collecting all items from the source stream
    Collecting {
        inner: Pin<Box<dyn Stream<Item = Result<ObjectMeta, object_store::Error>> + Send>>,
        collected: Vec<ObjectMeta>,
    },
    /// Emitting sorted batches
    Emitting {
        inner: Pin<Box<dyn Stream<Item = Vec<ObjectMeta>> + Send>>,
    },
    /// Stream exhausted
    Done,
}

impl SortedFileListingStream {
    fn new(
        list_stream: futures::stream::BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
        schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            state: SortedStreamState::Collecting {
                inner: list_stream,
                collected: Vec::new(),
            },
            schema,
        }
    }
    
    /// Convert a chunk of ObjectMeta items into a RecordBatch (same as FileListingStream).
    fn metas_to_batch(
        metas: Vec<ObjectMeta>,
        schema: &ArrowSchemaRef,
    ) -> DfResult<RecordBatch> {
        if metas.is_empty() {
            return RecordBatch::try_new(schema.clone(), vec![
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ]).map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
        
        let paths = StringArray::from_iter_values(
            metas.iter().map(|m| m.location.filename().unwrap_or(m.location.as_ref()))
        );
        let sizes = Int64Array::from_iter_values(
            metas.iter().map(|m| m.size as i64)
        );
        let mod_times = Int64Array::from_iter_values(
            metas.iter().map(|m| m.last_modified.timestamp_millis())
        );
        
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(paths),
                Arc::new(sizes),
                Arc::new(mod_times),
            ],
        ).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for SortedFileListingStream {
    type Item = DfResult<RecordBatch>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                SortedStreamState::Collecting { inner, collected } => {
                    match ready!(inner.as_mut().poll_next(cx)) {
                        Some(Ok(meta)) => {
                            collected.push(meta);
                            // Continue collecting
                        }
                        Some(Err(e)) => {
                            self.state = SortedStreamState::Done;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                        None => {
                            // Done collecting - sort by path and switch to emitting
                            let mut sorted = std::mem::take(collected);
                            sorted.sort_by(|a, b| a.location.cmp(&b.location));
                            
                            // Create chunked stream from sorted items
                            let chunks: Vec<Vec<ObjectMeta>> = sorted
                                .chunks(BATCH_SIZE)
                                .map(|c| c.to_vec())
                                .collect();
                            
                            self.state = SortedStreamState::Emitting {
                                inner: Box::pin(stream::iter(chunks)),
                            };
                            // Continue to emitting state
                        }
                    }
                }
                SortedStreamState::Emitting { inner } => {
                    match ready!(inner.as_mut().poll_next(cx)) {
                        Some(chunk) => {
                            let batch = Self::metas_to_batch(chunk, &self.schema);
                            return Poll::Ready(Some(batch));
                        }
                        None => {
                            self.state = SortedStreamState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                SortedStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for SortedFileListingStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_file_listing_exec_schema() {
        let path = url::Url::parse("file:///tmp/test/").unwrap();
        let exec = FileListingExec::new(path);
        
        let schema = exec.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "path");
        assert_eq!(schema.field(1).name(), "size");
        assert_eq!(schema.field(2).name(), "modificationTime");
    }
    
    #[test]
    fn test_file_listing_exec_properties() {
        let path = url::Url::parse("file:///tmp/test/").unwrap();
        let exec = FileListingExec::new(path.clone());
        
        assert_eq!(exec.name(), "FileListingExec");
        assert_eq!(exec.path(), &path);
        assert!(exec.children().is_empty());
    }
}
