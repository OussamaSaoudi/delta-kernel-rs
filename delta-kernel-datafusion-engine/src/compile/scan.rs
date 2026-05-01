//! Native scan lowering using DataFusion file sources.
//!
//! ## Parquet column matching (`PARQUET:field_id`)
//!
//! Kernel logical schemas encode parquet native field IDs on Arrow fields as `PARQUET:field_id`
//! during conversion from kernel `StructType`. The vendored [`ParquetSource`] / `ParquetOpener`
//! adapts decoded physical parquet columns **by ID first, then by name** at the Arrow-schema root
//! level—matching Delta Kernel parquet handlers for flat reads when writers kept stable IDs across
//! physical renames/reordering.
//!
//! When **no** root-level field IDs are present on the logical schema, behavior stays name-only
//! (upstream DataFusion).
//!
//! **Limits:** nested struct children are not independently matched by parquet field ID in this
//! path. Predicate pushdown inside the parquet decoder still keys off physical parquet statistics
//! paths; declarative scans prefer residual [`KernelFilterExec`] after decode to avoid
//! over-pushdown.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_json::source::JsonSource;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::object_store::path::Path as StorePath;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{FileType, ScanNode};
use delta_kernel::FileMeta;
use parquet::arrow::RowNumber;

use crate::exec::{KernelFilterExec, NullabilityValidationExec, OrderedUnionExec, RowIndexExec};

struct PreparedScanFiles {
    file_group: FileGroup,
    object_store_url: ObjectStoreUrl,
}

fn prepare_scan_files(files: &[FileMeta]) -> Result<PreparedScanFiles, DeltaError> {
    let first = files
        .first()
        .ok_or_else(|| crate::error::plan_compilation("Scan node has no files"))?;

    let partitioned_files = files
        .iter()
        .map(|f| file_meta_to_partitioned(f))
        .collect::<Result<Vec<_>, DeltaError>>()?;

    let base_url = format!(
        "{}://{}",
        first.location.scheme(),
        first.location.host_str().unwrap_or("")
    );
    let object_store_url =
        ObjectStoreUrl::parse(&base_url).map_err(crate::error::datafusion_err_to_delta)?;

    Ok(PreparedScanFiles {
        file_group: FileGroup::new(partitioned_files),
        object_store_url,
    })
}

fn file_meta_to_partitioned(file: &FileMeta) -> Result<PartitionedFile, DeltaError> {
    let location = if file.location.scheme() == "file" {
        let fs_path = file.location.to_file_path().map_err(|()| {
            crate::error::plan_compilation(format!("file URL is not local path: {}", file.location))
        })?;
        StorePath::from_absolute_path(&fs_path).map_err(|e| {
            crate::error::plan_compilation(format!(
                "object store path for {}: {e}",
                fs_path.display()
            ))
        })?
    } else {
        StorePath::from(file.location.path())
    };
    let mut pf = PartitionedFile::new(location, file.size as u64);
    pf.object_meta.last_modified = Utc
        .timestamp_millis_opt(file.last_modified)
        .single()
        .unwrap_or_else(|| Utc.timestamp_nanos(0));
    Ok(pf)
}

fn parquet_scan_arrow_schema_and_virtual_columns(
    node: &ScanNode,
    kernel_arrow_schema: Arc<Schema>,
) -> Result<(Arc<Schema>, Vec<Arc<Field>>), DeltaError> {
    if node.file_type != FileType::Parquet {
        return Ok((kernel_arrow_schema, Vec::new()));
    }
    let Some(name) = node.row_index_column.as_ref() else {
        return Ok((kernel_arrow_schema, Vec::new()));
    };

    let row_field =
        Arc::new(Field::new(name.as_str(), DataType::Int64, false).with_extension_type(RowNumber));
    let mut fields = kernel_arrow_schema
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::clone(&row_field));
    Ok((Arc::new(Schema::new(fields)), vec![row_field]))
}

fn build_raw_scan(
    node: &ScanNode,
    prepared: PreparedScanFiles,
    arrow_schema: Arc<Schema>,
    parquet_virtual_columns: Vec<Arc<Field>>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let scan: Arc<dyn ExecutionPlan> = match node.file_type {
        FileType::Parquet => {
            let mut source = ParquetSource::new(arrow_schema.clone());
            if !parquet_virtual_columns.is_empty() {
                source = source
                    .with_virtual_columns(parquet_virtual_columns)
                    .map_err(crate::error::datafusion_err_to_delta)?;
            }
            let source = Arc::new(source);
            let cfg = FileScanConfigBuilder::new(prepared.object_store_url, source)
                .with_file_group(prepared.file_group)
                .with_projection_indices(None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build();
            DataSourceExec::from_data_source(cfg)
        }
        FileType::Json => {
            let source = Arc::new(JsonSource::new(arrow_schema.clone()));
            let cfg = FileScanConfigBuilder::new(prepared.object_store_url, source)
                .with_file_group(prepared.file_group)
                .with_projection_indices(None)
                .map_err(crate::error::datafusion_err_to_delta)?
                .build();
            DataSourceExec::from_data_source(cfg)
        }
    };
    Ok(scan)
}

/// Applies nullability coercion, [`KernelFilterExec`] when [`ScanNode::predicate`] is set, and
/// optional row index augmentation.
///
/// ## Predicate
///
/// The scan predicate is evaluated **only** via [`KernelFilterExec`] on decoded record batches
/// using the kernel Arrow evaluator (same implementation shape as [`FilterNode`] lowering). Native
/// Parquet / JSON readers are not given pushdown predicates here, which guarantees evaluator parity
/// with the kernel reference and avoids over-filtering relative to [`ScanNode`] semantics.
///
/// ## Row index
///
/// Parquet scans with [`ScanNode::row_index_column`] decode arrow-rs virtual [`RowNumber`] columns
/// in the file reader so indices are physical offsets **before** filtering; [`KernelFilterExec`]
/// then masks rows without rewriting indices on survivors. JSON scans append contiguous indices
/// with [`RowIndexExec`] **after** decoding (and after any scan predicate filter).
///
/// Virtual Parquet row numbers are file-absolute in arrow-rs across batches from the same file.
fn wrap_scan_extensions(
    scan: Arc<dyn ExecutionPlan>,
    node: &ScanNode,
    arrow_schema: Arc<Schema>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let scan = Arc::new(NullabilityValidationExec::new(
        scan,
        Vec::new(),
        arrow_schema,
    ));

    let scan: Arc<dyn ExecutionPlan> = if let Some(pred) = &node.predicate {
        Arc::new(KernelFilterExec::try_new(
            scan,
            node.schema.clone(),
            Arc::clone(pred),
        )?)
    } else {
        scan
    };

    let native_parquet_row_index =
        matches!(node.file_type, FileType::Parquet) && node.row_index_column.is_some();

    if let Some(row_index_col) = &node.row_index_column {
        if !native_parquet_row_index {
            return Ok(Arc::new(RowIndexExec::new(scan, row_index_col.clone())));
        }
    }
    Ok(scan)
}

fn compile_scan_single_group(
    node: &ScanNode,
    files: &[FileMeta],
    kernel_arrow_schema: Arc<Schema>,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let (scan_arrow_schema, virtual_cols) =
        parquet_scan_arrow_schema_and_virtual_columns(node, kernel_arrow_schema)?;
    let prepared = prepare_scan_files(files)?;
    let scan = build_raw_scan(node, prepared, scan_arrow_schema.clone(), virtual_cols)?;
    wrap_scan_extensions(scan, node, scan_arrow_schema)
}

/// Convert kernel [`ScanNode`] into DataFusion physical scan.
///
/// ## Multi-file layout
///
/// When [`ScanNode::row_index_column`] is [`Some`], indices must restart at zero for each file.
/// That requires one native scan subtree per file (see [`OrderedUnionExec`]), even if
/// [`ScanNode::ordered`] is `false`. Parallel multi-file grouping is therefore disabled whenever a
/// row-index column is requested; file emission order still follows [`ScanNode::files`].
///
/// When **both** `ordered == false` **and** `row_index_column.is_none()`, all files share one
/// native file group so DataFusion may parallelize partition scheduling across files (unordered
/// scan).
///
/// ## Predicate
///
/// [`ScanNode::predicate`] is applied exclusively via [`KernelFilterExec`] after decode (no reader
/// pushdown). Engines may later add conservative pushdown optimizations while retaining this filter
/// as a residual guardrail.
///
/// ## Row-index implementation
///
/// Parquet uses arrow-rs [`RowNumber`] virtual columns decoded with each batch. JSON uses
/// [`RowIndexExec`].
pub fn compile_scan(node: &ScanNode) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let arrow_schema: Arc<Schema> = Arc::new(
        node.schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e| crate::error::plan_compilation(format!("scan schema conversion: {e}")))?,
    );

    let sequential_files = node.ordered || node.row_index_column.is_some();

    if node.files.len() <= 1 || !sequential_files {
        return compile_scan_single_group(node, &node.files, arrow_schema);
    }

    let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(node.files.len());
    for file in &node.files {
        let branch =
            compile_scan_single_group(node, std::slice::from_ref(file), arrow_schema.clone())?;
        children.push(Arc::new(CoalescePartitionsExec::new(branch)));
    }

    Ok(Arc::new(
        OrderedUnionExec::try_new(children).map_err(crate::error::datafusion_err_to_delta)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use delta_kernel::arrow::array::Int64Array;
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
    use delta_kernel::arrow::record_batch::RecordBatch as ArrowRecordBatch;
    use delta_kernel::expressions::{column_expr, Expression, Predicate};
    use delta_kernel::plans::ir::DeclarativePlanNode;
    use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};
    use delta_kernel::FileMeta;
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use super::compile_scan;
    use crate::DataFusionExecutor;

    fn kernel_schema_one_i64() -> Arc<StructType> {
        Arc::new(StructType::try_new([StructField::new("x", KernelDataType::LONG, false)]).unwrap())
    }

    fn write_i64_parquet(path: &std::path::Path, values: &[i64]) {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = ArrowRecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(
                values.iter().copied(),
            ))],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn file_meta(path: &std::path::Path) -> FileMeta {
        FileMeta::new(
            Url::from_file_path(path).unwrap(),
            0,
            std::fs::metadata(path).unwrap().len(),
        )
    }

    fn root_plan(scan: DeclarativePlanNode) -> delta_kernel::plans::ir::Plan {
        scan.into_results()
    }

    #[tokio::test]
    async fn row_index_resets_each_file() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("first.parquet");
        let p2 = dir.path().join("second.parquet");
        write_i64_parquet(&p1, &[10, 11]);
        write_i64_parquet(&p2, &[20]);

        let schema = kernel_schema_one_i64();
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(
                vec![file_meta(&p1), file_meta(&p2)],
                Arc::clone(&schema),
            )
            .with_row_index("rid")
            .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();
        assert_eq!(
            batches[0].num_columns(),
            2,
            "schema={:?}",
            batches[0].schema()
        );

        let rid_idx = batches[0].schema().column_with_name("rid").unwrap().0;
        let mut observed = Vec::new();
        for b in &batches {
            let arr = b
                .column(rid_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            observed.extend(arr.values().iter().copied());
        }
        assert_eq!(observed, vec![0, 1, 0]);
    }

    #[tokio::test]
    async fn ordered_scan_emits_files_in_declaration_order() {
        let dir = tempfile::tempdir().unwrap();
        let p_low = dir.path().join("low.parquet");
        let p_high = dir.path().join("high.parquet");
        write_i64_parquet(&p_low, &[1]);
        write_i64_parquet(&p_high, &[2]);

        let schema = kernel_schema_one_i64();
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(
                vec![file_meta(&p_high), file_meta(&p_low)],
                Arc::clone(&schema),
            )
            .with_ordered()
            .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();

        let x_idx = batches[0].schema().column_with_name("x").unwrap().0;
        let xs: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(x_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(xs, vec![2, 1]);
    }

    #[tokio::test]
    async fn scan_predicate_filters_without_extra_rows() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.parquet");
        let p2 = dir.path().join("b.parquet");
        write_i64_parquet(&p1, &[5, 8]);
        write_i64_parquet(&p2, &[25, 30]);

        let schema = kernel_schema_one_i64();
        let pred = Arc::new(Expression::from_pred(Predicate::gt(
            column_expr!("x"),
            Expression::literal(delta_kernel::expressions::Scalar::Long(10)),
        )));
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(
                vec![file_meta(&p1), file_meta(&p2)],
                Arc::clone(&schema),
            )
            .with_predicate(pred)
            .unwrap()
            .with_row_index("rid")
            .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();

        let x_idx = batches[0].schema().column_with_name("x").unwrap().0;
        let rid_idx = batches[0].schema().column_with_name("rid").unwrap().0;
        let mut xs = Vec::new();
        let mut rids = Vec::new();
        for b in &batches {
            let xa = b
                .column(x_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ra = b
                .column(rid_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            xs.extend(xa.values().iter().copied());
            rids.extend(ra.values().iter().copied());
        }
        assert_eq!(xs, vec![25, 30]);
        assert_eq!(rids, vec![0, 1]);
    }

    #[tokio::test]
    async fn parquet_row_index_physical_offsets_survive_residual_filter() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rows.parquet");
        write_i64_parquet(&p, &[8, 25, 30]);

        let schema = kernel_schema_one_i64();
        let pred = Arc::new(Expression::from_pred(Predicate::gt(
            column_expr!("x"),
            Expression::literal(delta_kernel::expressions::Scalar::Long(10)),
        )));
        let plan = root_plan(
            DeclarativePlanNode::scan_parquet(vec![file_meta(&p)], Arc::clone(&schema))
                .with_predicate(pred)
                .unwrap()
                .with_row_index("rid")
                .unwrap(),
        );

        let ex = DataFusionExecutor::try_new().unwrap();
        let batches = ex.execute_plan_collect(plan).await.unwrap();

        let rid_idx = batches[0].schema().column_with_name("rid").unwrap().0;
        let mut rids = Vec::new();
        for b in &batches {
            let ra = b
                .column(rid_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            rids.extend(ra.values().iter().copied());
        }
        assert_eq!(rids, vec![1, 2]);
    }

    fn plan_has_ordered_union(plan: &dyn datafusion_physical_plan::ExecutionPlan) -> bool {
        if plan.name() == "OrderedUnionExec" {
            return true;
        }
        plan.children()
            .iter()
            .any(|c| plan_has_ordered_union(c.as_ref()))
    }

    #[test]
    fn parallel_multi_file_scan_has_no_ordered_union_without_row_index_or_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("x.parquet");
        let p2 = dir.path().join("y.parquet");
        write_i64_parquet(&p1, &[1]);
        write_i64_parquet(&p2, &[2]);

        let schema = kernel_schema_one_i64();
        let node = DeclarativePlanNode::scan_parquet(vec![file_meta(&p1), file_meta(&p2)], schema);
        let scan = match node {
            DeclarativePlanNode::Scan(s) => s,
            _ => unreachable!(),
        };
        let physical = compile_scan(&scan).unwrap();
        assert!(
            !plan_has_ordered_union(physical.as_ref()),
            "unordered scan without row index should stay a single native file group"
        );
    }

    #[test]
    fn row_index_multi_file_wraps_ordered_union() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("x.parquet");
        let p2 = dir.path().join("y.parquet");
        write_i64_parquet(&p1, &[1]);
        write_i64_parquet(&p2, &[2]);

        let schema = kernel_schema_one_i64();
        let node = DeclarativePlanNode::scan_parquet(vec![file_meta(&p1), file_meta(&p2)], schema)
            .with_row_index("rid")
            .unwrap();
        let scan = match node {
            DeclarativePlanNode::Scan(s) => s,
            _ => unreachable!(),
        };
        let physical = compile_scan(&scan).unwrap();
        assert!(plan_has_ordered_union(physical.as_ref()));
    }
}
