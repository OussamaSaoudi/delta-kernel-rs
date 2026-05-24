//! Streaming physical plan behind [`super::LoadTableProvider`].
//!
//! For each upstream metadata row, [`build_load_stream`] runs `buffer_unordered` over open
//! futures: each resolves the optional DV via [`resolve_dv_async`], builds a per-file plan
//! via [`build_per_file_plan`] (`DataSourceExec` plus an optional `FilterExec(not_in_dv)` ->
//! `ProjectionExec` stack when DV is present), and drains it. Output ordering across files
//! is unspecified; intra-file order is preserved. JSON+DV is rejected at construction.

use std::fmt;
use std::sync::Arc;

use datafusion::physical_plan::execute_stream;
use datafusion_common::error::DataFusionError;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::Result as DfResult;
use datafusion_datasource::file::FileSource;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::plans::ir::nodes::{FileType, LoadNode};
use delta_kernel::schema::{ColumnMetadataKey, DataType, StructField};
use delta_kernel::Engine;
use futures::stream::{Stream, StreamExt, TryStreamExt};

use crate::error::plan_compilation;
use crate::exec::load_helpers::{
    build_file_source, build_per_file_plan, extract_row_inputs, load_base_url, resolve_dv_async,
    RowInputs,
};

/// Per-partition file-open concurrency when `target_partitions` is zero.
const DEFAULT_LOAD_CONCURRENCY: usize = 8;

/// Caps tokio task pressure for very-wide scans.
const MAX_LOAD_CONCURRENCY: usize = 64;

/// Walk `fields` recursively (through struct / array-of-struct / map values) for any field
/// carrying `delta.columnMapping.id` or `delta.columnMapping.physicalName` metadata. This
/// duplicates the ScanParquet/ScanJson compile-time helper; both should be deduplicated
/// into a shared helper once a third caller appears.
fn schema_has_column_mapping<'a>(fields: impl IntoIterator<Item = &'a StructField>) -> bool {
    fields.into_iter().any(field_has_column_mapping)
}

fn field_has_column_mapping(field: &StructField) -> bool {
    if field
        .metadata
        .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref())
        || field
            .metadata
            .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        return true;
    }
    data_type_has_column_mapping(field.data_type())
}

fn data_type_has_column_mapping(dt: &DataType) -> bool {
    match dt {
        DataType::Struct(inner) => schema_has_column_mapping(inner.fields()),
        DataType::Array(arr) => data_type_has_column_mapping(arr.element_type()),
        // Walk BOTH key and value types; either side may carry struct fields with
        // column-mapping metadata. The matching helper in `scan.rs` does the same; any
        // future deduplication into a shared helper must preserve key-side recursion.
        DataType::Map(map) => {
            data_type_has_column_mapping(map.key_type())
                || data_type_has_column_mapping(map.value_type())
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::{MetadataValue, StructType};

    #[test]
    fn schema_has_column_mapping_detects_flat_metadata() {
        let f = StructField::nullable("c0", DataType::INTEGER).with_metadata([(
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(1),
        )]);
        assert!(schema_has_column_mapping(std::iter::once(&f)));
    }

    #[test]
    fn schema_has_column_mapping_detects_nested_metadata() {
        let inner = StructField::nullable("inner", DataType::STRING).with_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String("col-x".into()),
        )]);
        let nested = StructType::try_new([inner]).unwrap();
        let outer = StructField::nullable("outer", DataType::Struct(Box::new(nested)));
        assert!(schema_has_column_mapping(std::iter::once(&outer)));
    }

    #[test]
    fn schema_has_column_mapping_is_false_for_plain_schema() {
        let f = StructField::nullable("c0", DataType::INTEGER);
        assert!(!schema_has_column_mapping(std::iter::once(&f)));
    }
}

/// Streaming physical plan that opens one file per upstream metadata row.
pub struct LoadExec {
    node: Arc<LoadNode>,
    /// Used only for deletion-vector resolution; file decoding goes through DataFusion's
    /// parquet/json sources, not this engine.
    engine: Arc<dyn Engine>,
    upstream: Arc<dyn ExecutionPlan>,
    /// Pre-projection schema (= file_schema fields ++ passthrough fields). Kept so
    /// `with_new_children` can rebuild against the same shape.
    full_schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    limit: Option<usize>,
    /// Indices into `node.passthrough_columns` to materialize per row -- always the full
    /// `0..passthrough_count` range (see `LoadExec::new`'s rationale: the file source's
    /// `TableSchema` declares the full passthrough slice as partition columns, so each row's
    /// `PartitionedFile.partition_values` must carry every passthrough scalar). `Arc` so
    /// per-row open futures can clone cheaply.
    projected_passthrough: Arc<Vec<usize>>,
    /// File source without `_row_number`. Used for every row in this PR because DV plans
    /// are rejected up front; once the DataFusion-54 follow-up wires `_row_number`, this
    /// becomes the fallback for rows whose DV column is null.
    file_source_no_dv: Arc<dyn FileSource>,
    /// File source with `_row_number` virtual column appended for DV rows. Always `None`
    /// here: the DV-enabled construction path requires DataFusion 54 and the wrapper
    /// rejects DV plans before this field is ever populated.
    file_source_with_dv: Option<Arc<dyn FileSource>>,
    properties: Arc<PlanProperties>,
}

impl LoadExec {
    /// Build a load plan over `upstream` using the `LoadNode` payload and optional projection.
    pub fn new(
        upstream: Arc<dyn ExecutionPlan>,
        node: Arc<LoadNode>,
        engine: Arc<dyn Engine>,
        full_schema: ArrowSchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> DfResult<Self> {
        // JSON has no `_row_number` virtual column; delta DVs only apply to parquet anyway.
        if node.file_type == FileType::Json && node.dv_ref.is_some() {
            return Err(plan_compilation(
                "LoadNode with FileType::Json and dv_ref set is not supported (no \
                 _row_number virtual column for JSON)",
            ));
        }
        // DV-driven row filtering needs the `_row_number` virtual column path, which
        // requires DataFusion 54's `TableSchema::with_virtual_columns`
        // (apache/datafusion#22026). Until that lands we reject DV plans up front so the
        // file_source_with_dv field below is always `None`.
        if node.dv_ref.is_some() {
            return Err(plan_compilation(
                "LoadNode with deletion vectors requires DataFusion 54",
            ));
        }
        // Column-mapping schemas need a field-id-aware physical adapter
        // (`FieldIdPhysicalExprAdapterFactory`) that we cannot install on DF53. Without it
        // parquet decode falls back to name-based matching and reads nulls for any column
        // whose physical name differs from the logical name. Reject up front so callers
        // fail fast instead of silently corrupting data. The duplicate walk shared with
        // ScanParquet's compile-time rejection can be deduplicated once a third caller
        // motivates extracting the helper.
        if schema_has_column_mapping(node.file_schema.fields()) {
            return Err(plan_compilation(
                "LoadNode with column-mapping schema requires DataFusion 54 \
                 (FieldIdPhysicalExprAdapterFactory)",
            ));
        }

        let file_count = node.file_schema.fields().len();
        let passthrough_count = node.passthrough_columns.len();
        debug_assert_eq!(full_schema.fields().len(), file_count + passthrough_count);

        // Build the no-DV file source. With the dv_ref rejection above and the matching
        // compile-time rejection in `lower_load`, the DV-attached variant is unreachable
        // in this engine slice, so `file_source_with_dv` is always None here; it remains
        // present as a slot so DV support can be added without restructuring `LoadExec`.
        let file_source_no_dv = build_file_source(
            node.file_type,
            &full_schema,
            file_count,
            projection.as_deref(),
            false,
        )?;
        let file_source_with_dv: Option<Arc<dyn FileSource>> = None;

        let output_schema = match projection.as_ref() {
            Some(proj) => Arc::new(full_schema.project(proj)?),
            None => Arc::clone(&full_schema),
        };

        // Always materialize ALL passthrough columns per row. The `TableSchema` built in
        // `build_file_source` declares the full passthrough slice as partition columns, so
        // each `PartitionedFile.partition_values` MUST be a vector of the full passthrough
        // length regardless of the user's projection -- DataFusion's source pruning then
        // drops the unprojected ones at scan time. Filtering here would feed
        // `partition_values` a wrong-length vector and the file open would fail at runtime.
        let projected_passthrough: Vec<usize> = (0..passthrough_count).collect();

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            // Single partition: the merger interleaves files within one stream.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            upstream.properties().boundedness,
        ));
        Ok(Self {
            node,
            engine,
            upstream,
            full_schema,
            projection,
            output_schema,
            limit,
            projected_passthrough: Arc::new(projected_passthrough),
            file_source_no_dv,
            file_source_with_dv,
            properties,
        })
    }
}

impl fmt::Debug for LoadExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadExec")
            .field("file_type", &self.node.file_type)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .field("output_fields", &self.output_schema.fields().len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LoadExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LoadExec(file_type={:?}, projection={:?}, limit={:?}, output_fields={})",
            self.node.file_type,
            self.projection,
            self.limit,
            self.output_schema.fields().len(),
        )
    }
}

impl ExecutionPlan for LoadExec {
    fn name(&self) -> &str {
        "LoadExec"
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.upstream]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [upstream] = children.try_into().map_err(|c: Vec<_>| {
            DataFusionError::Plan(format!(
                "LoadExec requires exactly one child, got {}",
                c.len()
            ))
        })?;
        Ok(Arc::new(LoadExec::new(
            upstream,
            Arc::clone(&self.node),
            Arc::clone(&self.engine),
            Arc::clone(&self.full_schema),
            self.projection.clone(),
            self.limit,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan(format!(
                "LoadExec only supports partition 0, got {partition}"
            )));
        }
        // Coalesce the upstream's partitions into one stream so the row expander sees one
        // batch at a time.
        let upstream = execute_stream(Arc::clone(&self.upstream), Arc::clone(&context))?;
        // `target_partitions()` returns 0 when the session has not configured a value;
        // treat that as "default" before clamping (clamp(1, ..) would otherwise mask the
        // 0 sentinel and just yield 1).
        let target = context.session_config().target_partitions();
        let concurrency = if target == 0 {
            DEFAULT_LOAD_CONCURRENCY
        } else {
            target.clamp(1, MAX_LOAD_CONCURRENCY)
        };
        let stream = build_load_stream(
            upstream,
            Arc::clone(&self.node),
            Arc::clone(&self.engine),
            Arc::clone(&self.file_source_no_dv),
            self.file_source_with_dv.as_ref().map(Arc::clone),
            Arc::clone(&self.projected_passthrough),
            Arc::clone(&self.output_schema),
            context,
            self.limit,
            concurrency,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }
}

/// Up to `concurrency` per-row open futures run at once via `buffer_unordered`; the outer
/// `try_flatten` interleaves files freely while preserving intra-file batch order. `limit`
/// is enforced by slicing the final batch + early-terminating.
#[allow(clippy::too_many_arguments)]
fn build_load_stream(
    upstream: SendableRecordBatchStream,
    node: Arc<LoadNode>,
    engine: Arc<dyn Engine>,
    file_source_no_dv: Arc<dyn FileSource>,
    file_source_with_dv: Option<Arc<dyn FileSource>>,
    projected_passthrough: Arc<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    task_context: Arc<TaskContext>,
    limit: Option<usize>,
    concurrency: usize,
) -> impl Stream<Item = DfResult<RecordBatch>> + Send + 'static {
    // Explode upstream batches into one item per row.
    let row_stream = upstream
        .map_ok(|batch| {
            let n = batch.num_rows();
            let batch = Arc::new(batch);
            futures::stream::iter((0..n).map(move |row| DfResult::Ok((Arc::clone(&batch), row))))
        })
        .try_flatten();

    // Per row, an open future producing the per-file `RecordBatch` stream.
    let per_file_streams = row_stream.map(move |row_result: DfResult<_>| {
        let node = Arc::clone(&node);
        let engine = Arc::clone(&engine);
        let task_ctx = Arc::clone(&task_context);
        let pt = Arc::clone(&projected_passthrough);
        let file_source_no_dv = Arc::clone(&file_source_no_dv);
        let file_source_with_dv = file_source_with_dv.as_ref().map(Arc::clone);
        let output_schema = Arc::clone(&output_schema);

        async move {
            let (batch, row) = row_result?;
            let inputs: RowInputs = extract_row_inputs(&batch, row, &node, &pt)?;

            let dv = match inputs.dv_descriptor.clone() {
                Some(desc) => {
                    let base = load_base_url(&node)?.clone();
                    Some(resolve_dv_async(desc, base, Arc::clone(&engine)).await?)
                }
                None => None,
            };

            // `file_source_with_dv` is `Some` iff `node.dv_ref.is_some()`, which is the only
            // way `dv` can be `Some` here.
            let file_source = match (dv.is_some(), file_source_with_dv) {
                (true, Some(src)) => src,
                _ => file_source_no_dv,
            };

            let plan = build_per_file_plan(
                inputs,
                dv,
                file_source,
                node.file_type,
                &output_schema,
                task_ctx.as_ref(),
            )
            .await?;
            let stream = plan.execute(0, task_ctx)?;
            Ok::<_, DataFusionError>(stream)
        }
    });

    // Concurrent flatten + limit slicing.
    let flattened = per_file_streams.buffer_unordered(concurrency).try_flatten();
    async_stream::try_stream! {
        let mut remaining = limit;
        let mut s = std::pin::pin!(flattened);
        while let Some(batch) = s.try_next().await? {
            let mut out = batch;
            if let Some(rem) = remaining.as_mut() {
                if out.num_rows() > *rem {
                    out = out.slice(0, *rem);
                }
                *rem -= out.num_rows();
            }
            if out.num_rows() > 0 {
                yield out;
            }
            if matches!(remaining, Some(0)) {
                return;
            }
        }
    }
}
