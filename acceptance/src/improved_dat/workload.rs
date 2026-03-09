//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{DeltaResult, Engine, Error, Version};
use itertools::Itertools;
use url::Url;

use super::types::WorkloadSpec;

/// Result of executing a read workload
pub struct ReadResult {
    /// The record batches from the scan
    pub batches: Vec<RecordBatch>,
    /// The schema of the data
    pub schema: Option<Arc<ArrowSchema>>,
}

/// Strip field-level metadata from an Arrow data type (recursive for structs, lists, maps).
///
/// This is needed because the kernel's `transform_to_logical` inconsistently applies field
/// metadata: batches that go through `apply_schema` (when there's a transform expression) get
/// metadata like `delta.typeChanges`, while batches that don't need a transform are returned
/// without it. Arrow's `concat_batches` requires identical schemas, so we strip metadata to
/// normalize.
fn strip_field_metadata(dt: &ArrowDataType) -> ArrowDataType {
    match dt {
        ArrowDataType::Struct(fields) => {
            let new_fields: Vec<ArrowField> = fields
                .iter()
                .map(|f| {
                    let new_dt = strip_field_metadata(f.data_type());
                    ArrowField::new(f.name(), new_dt, f.is_nullable())
                })
                .collect();
            ArrowDataType::Struct(new_fields.into())
        }
        ArrowDataType::List(field) => {
            let new_dt = strip_field_metadata(field.data_type());
            ArrowDataType::List(Arc::new(ArrowField::new(
                field.name(),
                new_dt,
                field.is_nullable(),
            )))
        }
        ArrowDataType::Map(field, sorted) => {
            let new_dt = strip_field_metadata(field.data_type());
            ArrowDataType::Map(
                Arc::new(ArrowField::new(field.name(), new_dt, field.is_nullable())),
                *sorted,
            )
        }
        other => other.clone(),
    }
}

/// Strip all field-level metadata from an Arrow schema.
fn strip_schema_metadata(schema: &ArrowSchema) -> ArrowSchema {
    let new_fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|f| {
            let new_dt = strip_field_metadata(f.data_type());
            ArrowField::new(f.name(), new_dt, f.is_nullable())
        })
        .collect();
    ArrowSchema::new(new_fields)
}

impl ReadResult {
    /// Concatenate all batches into a single RecordBatch.
    ///
    /// Strips field-level metadata before concatenation to work around a kernel bug where
    /// `transform_to_logical` inconsistently applies schema metadata across batches.
    pub fn concat(self) -> DeltaResult<RecordBatch> {
        let schema = self.schema.ok_or_else(|| Error::generic("No schema"))?;
        let stripped_schema = Arc::new(strip_schema_metadata(&schema));

        let normalized_batches: Vec<RecordBatch> = self
            .batches
            .into_iter()
            .map(|batch| {
                let columns: Vec<_> = batch.columns().to_vec();
                RecordBatch::try_new(stripped_schema.clone(), columns)
                    .or_else(|_| {
                        let cast_columns: Vec<_> = batch
                            .columns()
                            .iter()
                            .zip(stripped_schema.fields())
                            .map(|(col, field)| {
                                if col.data_type() == field.data_type() {
                                    Ok(col.clone())
                                } else {
                                    delta_kernel::arrow::compute::cast(col, field.data_type())
                                        .map_err(|e| {
                                            delta_kernel::arrow::error::ArrowError::CastError(
                                                format!(
                                                    "Cannot cast column from {:?} to {:?}: {}",
                                                    col.data_type(),
                                                    field.data_type(),
                                                    e
                                                ),
                                            )
                                        })
                                }
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        RecordBatch::try_new(stripped_schema.clone(), cast_columns)
                    })
                    .map_err(Error::from)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        concat_batches(&stripped_schema, normalized_batches.iter()).map_err(Error::from)
    }
}

/// Result of executing a snapshot workload
pub struct SnapshotResult {
    /// The snapshot version
    pub version: Version,
    /// Minimum reader version
    pub min_reader_version: i32,
    /// Minimum writer version
    pub min_writer_version: i32,
    /// Reader features (if any)
    pub reader_features: Vec<String>,
    /// Writer features (if any)
    pub writer_features: Vec<String>,
    /// Table ID
    pub table_id: String,
    /// Schema string
    pub schema_string: String,
    /// Partition columns
    pub partition_columns: Vec<String>,
    /// Table configuration
    pub configuration: std::collections::HashMap<String, String>,
}

/// Workload execution result
pub enum WorkloadResult {
    Read(ReadResult),
    Snapshot(SnapshotResult),
}

/// Execute a workload specification and return the result
pub fn execute_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    spec: &WorkloadSpec,
) -> DeltaResult<WorkloadResult> {
    match spec {
        WorkloadSpec::Read {
            version,
            timestamp,
            columns,
            predicate,
            ..
        } => {
            let result = execute_read_workload(
                engine,
                table_root,
                *version,
                timestamp.as_deref(),
                columns.as_deref(),
                predicate.as_deref(),
            )?;
            Ok(WorkloadResult::Read(result))
        }
        WorkloadSpec::Snapshot {
            version, timestamp, ..
        } => {
            let result =
                execute_snapshot_workload(engine, table_root, *version, timestamp.as_deref())?;
            Ok(WorkloadResult::Snapshot(result))
        }
        WorkloadSpec::Cdf {
            start_version,
            end_version,
            predicate,
            ..
        } => {
            let result = execute_cdf_workload(
                engine,
                table_root,
                start_version.unwrap_or(0) as Version,
                end_version.map(|v| v as Version),
                predicate.as_deref(),
            )?;
            Ok(WorkloadResult::Read(result))
        }
        WorkloadSpec::Write {
            result_version, ..
        } => {
            // Write specs capture the post-write table state. We simply read at
            // result_version (or latest if not specified) and return the data for
            // validation against expected_data/.
            let result = execute_read_workload(
                engine,
                table_root,
                *result_version,
                None, // no timestamp
                None, // no column projection
                None, // no predicate
            )?;
            Ok(WorkloadResult::Read(result))
        }
        _ => Err(Error::generic(
            "Unsupported workload type in this harness build",
        )),
    }
}

/// Execute a read workload
pub fn execute_read_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    timestamp: Option<&str>,
    columns: Option<&[String]>,
    predicate: Option<&str>,
) -> DeltaResult<ReadResult> {
    // Resolve version from timestamp if needed
    let version = if let Some(ts) = timestamp {
        Some(resolve_timestamp_to_version(
            engine.as_ref(),
            table_root,
            ts,
        )?)
    } else {
        version.map(|v| v as Version)
    };

    // Build snapshot
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let table_schema = snapshot.schema();

    // Build scan with optional column projection
    let mut scan_builder = snapshot.scan_builder();
    if let Some(cols) = columns {
        use delta_kernel::schema::StructType;
        let projected_fields: Vec<_> = cols
            .iter()
            .filter_map(|col_name| table_schema.field(col_name).cloned())
            .collect();
        if !projected_fields.is_empty() {
            let projected_schema =
                Arc::new(StructType::try_new(projected_fields).map_err(|e| {
                    Error::generic(format!("Failed to create projected schema: {}", e))
                })?);
            scan_builder = scan_builder.with_schema(projected_schema);
        }
    }
    if let Some(pred_str) = predicate {
        use super::predicate_parser::parse_predicate_with_schema;
        match parse_predicate_with_schema(pred_str, &table_schema) {
            Ok(pred) => {
                scan_builder = scan_builder.with_predicate(Arc::new(pred));
            }
            Err(e) => {
                return Err(Error::generic(format!(
                    "Predicate '{}' not supported: {}",
                    pred_str, e
                )));
            }
        }
    }
    let scan = scan_builder.build()?;

    // Get schema from scan
    use delta_kernel::engine::arrow_conversion::TryFromKernel;
    let arrow_schema =
        delta_kernel::arrow::datatypes::Schema::try_from_kernel(scan.logical_schema().as_ref())
            .map_err(|e| Error::generic(format!("Failed to convert schema: {}", e)))?;
    let schema = Arc::new(arrow_schema);

    // Execute scan
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let record_batch = data?.try_into_record_batch()?;
            Ok(record_batch)
        })
        .try_collect()?;

    Ok(ReadResult {
        batches,
        schema: Some(schema),
    })
}

/// Execute a CDF (Change Data Feed) workload using kernel's TableChanges API.
pub fn execute_cdf_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    start_version: Version,
    end_version: Option<Version>,
    predicate: Option<&str>,
) -> DeltaResult<ReadResult> {
    if predicate.is_some() {
        return Err(Error::generic("CDF predicates not supported in this build"));
    }

    let table_changes =
        TableChanges::try_new(table_root.clone(), engine.as_ref(), start_version, end_version)?;

    let scan = table_changes.into_scan_builder().build()?;

    // Get schema from scan
    use delta_kernel::engine::arrow_conversion::TryFromKernel;
    let arrow_schema =
        delta_kernel::arrow::datatypes::Schema::try_from_kernel(scan.logical_schema().as_ref())
            .map_err(|e| Error::generic(format!("Failed to convert CDF schema: {}", e)))?;
    let schema = Arc::new(arrow_schema);

    // Execute scan
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let record_batch = data?.try_into_record_batch()?;
            Ok(record_batch)
        })
        .try_collect()?;

    Ok(ReadResult {
        batches,
        schema: Some(schema),
    })
}

/// Execute a snapshot workload (for metadata validation)
pub fn execute_snapshot_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    timestamp: Option<&str>,
) -> DeltaResult<SnapshotResult> {
    let version = if let Some(ts) = timestamp {
        Some(resolve_timestamp_to_version(
            engine.as_ref(),
            table_root,
            ts,
        )?)
    } else {
        version.map(|v| v as Version)
    };

    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let config = snapshot.table_configuration();
    let protocol = config.protocol();
    let metadata = config.metadata();

    Ok(SnapshotResult {
        version: snapshot.version(),
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: protocol
            .reader_features()
            .map(|f| f.iter().map(|feat| feat.to_string()).collect())
            .unwrap_or_default(),
        writer_features: protocol
            .writer_features()
            .map(|f| f.iter().map(|feat| feat.to_string()).collect())
            .unwrap_or_default(),
        table_id: metadata.id().to_string(),
        schema_string: metadata.schema_string().to_string(),
        partition_columns: metadata.partition_columns().to_vec(),
        configuration: metadata
            .configuration()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    })
}

/// Resolve a timestamp string to a table version.
fn resolve_timestamp_to_version(
    engine: &dyn Engine,
    table_root: &Url,
    timestamp_str: &str,
) -> DeltaResult<Version> {
    use chrono::NaiveDateTime;

    let target_ts = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.3f")
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| {
            Error::generic(format!(
                "Failed to parse timestamp '{}': {}",
                timestamp_str, e
            ))
        })?;

    let target_millis = target_ts.and_utc().timestamp_millis();
    let log_url = table_root.join("_delta_log/")?;
    let storage = engine.storage_handler();
    let files = storage.list_from(&log_url)?;

    let mut version_timestamps: Vec<(Version, i64)> = Vec::new();
    for file_meta_result in files {
        let file_meta = file_meta_result?;
        let path = file_meta.location.as_ref();
        let filename = path.rsplit('/').next().unwrap_or("");
        if filename.ends_with(".json") && !filename.contains("checkpoint") {
            if let Ok(version) = filename.trim_end_matches(".json").parse::<Version>() {
                version_timestamps.push((version, file_meta.last_modified));
            }
        }
    }

    version_timestamps.sort_by_key(|(v, _)| *v);

    let mut result_version: Option<Version> = None;
    for (version, ts) in version_timestamps {
        if ts <= target_millis {
            result_version = Some(version);
        } else {
            break;
        }
    }

    result_version.ok_or_else(|| {
        Error::generic(format!(
            "No version found at or before timestamp: {}",
            timestamp_str
        ))
    })
}

// ── Structured Write Spec Execution ─────────────────────────────────────────

use super::types::{WriteOp, WriteSpec};
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::schema::{ArrayType, DataType, MapType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::RowVisitor;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

type WriteEngine = DefaultEngine<TokioBackgroundExecutor>;


fn build_engine(table_url: &Url) -> DeltaResult<WriteEngine> {
    let store = store_from_url(table_url)?;
    Ok(DefaultEngineBuilder::new(store).build())
}

/// Check if all data files referenced by a write spec exist on disk.
/// Returns an error message if any are missing.
fn check_write_spec_data_files(test_case_root: &Path, write_spec: &WriteSpec) -> Option<String> {
    let mut missing = Vec::new();
    for commit in &write_spec.commits {
        for op in &commit.ops {
            let data_paths: Vec<&str> = match op {
                WriteOp::Create { data: Some(d), .. } => d.paths(),
                WriteOp::Append { data, .. } => vec![data.as_str()],
                WriteOp::Rewrite { data, .. } => data.paths(),
                WriteOp::Replace { data: Some(d), .. } => d.paths(),
                _ => vec![],
            };
            for p in data_paths {
                let full = test_case_root.join(p);
                if !full.exists() {
                    missing.push(p.to_string());
                }
            }
        }
    }
    if missing.is_empty() {
        None
    } else {
        Some(format!(
            "missing {} data file(s): {}",
            missing.len(),
            missing.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
        ))
    }
}

/// Execute a structured write spec: copy delta/ to temp dir, replay ops, return temp URL.
///
/// Returns the URL of the temp table directory after all writes are applied.
/// The caller is responsible for cleaning up the temp directory.
pub async fn execute_structured_write_spec(
    test_case_root: &Path,
    write_spec: &WriteSpec,
) -> DeltaResult<(Url, PathBuf)> {
    // Pre-check: verify all referenced data files exist
    if let Some(msg) = check_write_spec_data_files(test_case_root, write_spec) {
        return Err(Error::generic(format!("Write spec data files incomplete: {}", msg)));
    }

    // Create temp directory for the working table
    let temp_dir = tempfile::tempdir()
        .map_err(|e| Error::generic(format!("Failed to create temp dir: {}", e)))?;
    let working_table_path = temp_dir.path().join("table");

    // Step 1: Copy initial table state if delta/ exists
    let delta_dir = test_case_root.join("delta");
    if delta_dir.exists() {
        copy_dir_recursive(&delta_dir, &working_table_path)?;
    } else {
        std::fs::create_dir_all(&working_table_path)
            .map_err(|e| Error::generic(format!("Failed to create table dir: {}", e)))?;
    }

    let table_url = Url::from_directory_path(&working_table_path)
        .map_err(|_| Error::generic("Failed to create URL from temp dir"))?;

    // Step 2: Execute each commit's ops
    for (i, commit) in write_spec.commits.iter().enumerate() {
        println!("  Executing commit {} (ops: {})", i + 1, commit.ops.len());
        execute_write_commit(test_case_root, &table_url, commit).await?;
    }

    // Keep temp_dir alive
    let temp_path = temp_dir.keep();

    Ok((table_url, temp_path))
}

/// Execute a single commit's ops against the table.
async fn execute_write_commit(
    test_case_root: &Path,
    table_url: &Url,
    commit: &super::types::WriteCommit,
) -> DeltaResult<()> {
    let engine = build_engine(table_url)?;

    let has_create = commit
        .ops
        .iter()
        .any(|op| matches!(op, WriteOp::Create { .. }));

    if has_create {
        execute_create_op(test_case_root, table_url, &commit.ops, &engine).await?;
    } else if commit.ops.is_empty() {
        // Empty ops: legitimate no-match DML or metadata-only commit.
        // Commit an empty transaction to advance the table version.
        execute_empty_commit(table_url, &engine)?;
    } else {
        execute_existing_table_ops(test_case_root, table_url, &commit.ops, &engine).await?;
    }

    Ok(())
}

/// Execute an empty commit to advance the table version.
///
/// This handles legitimate cases where a DML commit matches zero rows (e.g., DELETE WHERE
/// with no matching rows) or metadata-only commits that don't produce structured ops.
fn execute_empty_commit(table_url: &Url, engine: &WriteEngine) -> DeltaResult<()> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let committer = Box::new(FileSystemCommitter::new());
    let txn = snapshot
        .transaction(committer, engine)?
        .with_operation("EMPTY_COMMIT".to_string())
        .with_engine_info("acceptance-test-harness/1.0");

    match txn.commit(engine)? {
        CommitResult::CommittedTransaction(committed) => {
            println!(
                "    Empty commit at version {}",
                committed.commit_version()
            );
        }
        _ => {
            return Err(Error::generic("Empty commit was not committed"));
        }
    }
    Ok(())
}

/// Execute a create table operation.
/// All properties are passed directly to kernel's CREATE TABLE API.
/// If kernel rejects a property, the test fails — that's a kernel limitation finding.
async fn execute_create_op(
    test_case_root: &Path,
    table_url: &Url,
    ops: &[WriteOp],
    engine: &WriteEngine,
) -> DeltaResult<()> {
    for op in ops {
        if let WriteOp::Create {
            schema,
            partition_columns,
            properties,
            data,
        } = op
        {
            let kernel_schema = if let Some(schema_val) = schema {
                parse_schema_from_json(schema_val)?
            } else {
                return Err(Error::generic("Create op must have a schema"));
            };

            let mut builder = create_table(
                table_url.as_str(),
                kernel_schema,
                "acceptance-test-harness/1.0",
            );

            // Pass all properties to kernel's CREATE TABLE.
            // If kernel rejects a property, the test will fail and should be skipped
            // via structured_write_skip_reason — NOT hacked around.
            if let Some(props) = properties {
                let pairs: Vec<(String, String)> = props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if !pairs.is_empty() {
                    builder = builder.with_table_properties(pairs);
                }
            }

            // Kernel create_table API doesn't support partition columns (TODO #1795)
            if let Some(ref parts) = partition_columns {
                if !parts.is_empty() {
                    return Err(Error::generic(format!(
                        "Kernel create_table API does not support partition columns (TODO #1795). Requested: {:?}",
                        parts
                    )));
                }
            }

            let committer = Box::new(FileSystemCommitter::new());
            let mut txn = builder.build(engine, committer)?;

            if let Some(data_ref) = data {
                let data_paths = data_ref.paths();
                for data_path in data_paths {
                    let parquet_path = test_case_root.join(data_path);
                    if parquet_path.exists() {
                        let write_context = Arc::new(txn.get_write_context());
                        let arrow_data = read_parquet_as_arrow(&parquet_path)?;
                        let file_metadata = engine
                            .write_parquet(
                                &arrow_data,
                                write_context.as_ref(),
                                HashMap::new(),
                            )
                            .await
                            .map_err(|e| {
                                Error::generic(format!("Parquet write failed: {}", e))
                            })?;
                        txn.add_files(file_metadata);
                    }
                }
            }

            match txn.commit(engine)? {
                CommitResult::CommittedTransaction(committed) => {
                    let commit_version = committed.commit_version();
                    println!(
                        "    Created table at version {}",
                        commit_version
                    );

                    // No post-commit patching. If kernel can't handle
                    // a property, the test should be skipped, not hacked around.
                }
                _ => {
                    return Err(Error::generic("Create table commit was not committed"));
                }
            }
        }
    }
    Ok(())
}

/// Visitor that extracts file paths from scan metadata for matching against delete/rewrite ops.
struct FilePathVisitor {
    paths: Vec<String>,
}

impl FilePathVisitor {
    fn new() -> Self {
        Self { paths: Vec::new() }
    }
}

impl RowVisitor for FilePathVisitor {
    fn selected_column_names_and_types(
        &self,
    ) -> (
        &'static [delta_kernel::expressions::ColumnName],
        &'static [DataType],
    ) {
        use delta_kernel::expressions::column_name;
        use std::sync::LazyLock;
        static NAMES_AND_TYPES: LazyLock<(
            Vec<delta_kernel::expressions::ColumnName>,
            Vec<DataType>,
        )> = LazyLock::new(|| {
            let names = vec![column_name!("path")];
            let types = vec![DataType::STRING];
            (names, types)
        });
        (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
    }

    fn visit<'a>(
        &mut self,
        row_count: usize,
        getters: &[&'a dyn delta_kernel::engine_data::GetData<'a>],
    ) -> DeltaResult<()> {
        use delta_kernel::engine_data::TypedGetData;
        for i in 0..row_count {
            let path: Option<String> = getters[0].get_opt(i, "path")?;
            self.paths.push(path.unwrap_or_default());
        }
        Ok(())
    }
}

/// Execute ops against an existing table.
async fn execute_existing_table_ops(
    test_case_root: &Path,
    table_url: &Url,
    ops: &[WriteOp],
    engine: &WriteEngine,
) -> DeltaResult<()> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;

    // Get partition columns from snapshot metadata (needed for rewrite ops)
    let partition_columns: Vec<String> = snapshot
        .table_configuration()
        .metadata()
        .partition_columns()
        .to_vec();

    // Classify ops to determine what capabilities we need
    let has_removes = ops.iter().any(|op| {
        matches!(
            op,
            WriteOp::Delete { .. } | WriteOp::Rewrite { .. }
        )
    });
    let has_dvs = ops.iter().any(|op| {
        matches!(
            op,
            WriteOp::SetDeletes { .. } | WriteOp::AddDeletes { .. }
        )
    });
    let has_appends_only = !has_removes
        && !has_dvs
        && ops.iter().any(|op| matches!(op, WriteOp::Append { .. }));

    // Collect all paths we need to remove (from delete and rewrite ops)
    let mut paths_to_remove: Vec<String> = Vec::new();
    for op in ops {
        match op {
            WriteOp::Delete { paths } => {
                paths_to_remove.extend(paths.iter().cloned());
            }
            WriteOp::Rewrite { paths, .. } => {
                paths_to_remove.extend(paths.iter().cloned());
            }
            _ => {}
        }
    }

    // Collect DV updates: map from file path -> row indexes to delete
    let mut dv_updates: HashMap<String, Vec<u64>> = HashMap::new();
    for op in ops {
        match op {
            WriteOp::SetDeletes {
                path, row_indexes, ..
            } => {
                // SetDeletes replaces the entire DV state
                dv_updates.insert(path.clone(), row_indexes.clone());
            }
            WriteOp::AddDeletes {
                path, row_indexes, ..
            } => {
                // AddDeletes appends to existing DV (same behavior for our harness)
                dv_updates
                    .entry(path.clone())
                    .or_default()
                    .extend(row_indexes.iter());
            }
            _ => {}
        }
    }

    // Build the transaction
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot
        .transaction(committer, engine)?
        .with_operation("ACCEPTANCE_TEST".to_string())
        .with_engine_info("acceptance-test-harness/1.0");

    let mut unsupported_ops: Vec<String> = Vec::new();

    // Process remove operations (delete + rewrite removes)
    if has_removes {
        // Scan the table to get file metadata in scan_row_schema format
        let remove_snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
        let scan = remove_snapshot.scan_builder().build()?;
        let metadata_iter = scan.scan_metadata(engine)?;

        for meta_result in metadata_iter {
            let meta = meta_result?;
            let (data, sel_vec) = meta.scan_files.into_parts();

            // Visit rows to extract file paths
            let mut visitor = FilePathVisitor::new();
            visitor.visit_rows_of(data.as_ref())?;

            // Build selection vector: true = file should be removed
            let data_len = data.len();
            let mut remove_selection = vec![false; data_len];
            let mut has_match = false;

            for i in 0..data_len {
                // Check if row is selected in the original scan
                let is_selected = if i < sel_vec.len() {
                    sel_vec[i]
                } else {
                    true // rows beyond selection vector are selected
                };
                if is_selected && paths_to_remove.contains(&visitor.paths[i]) {
                    remove_selection[i] = true;
                    has_match = true;
                }
            }

            if has_match {
                let remove_data = FilteredEngineData::try_new(data, remove_selection)?;
                txn.remove_files(remove_data);
            }
        }
    }

    // Process DV operations
    if has_dvs && !dv_updates.is_empty() {
        // Write DV files and build DV descriptors
        let mut dv_descriptors: HashMap<String, DeletionVectorDescriptor> = HashMap::new();

        // Write all DVs to a single file
        let table_path = table_url
            .to_file_path()
            .map_err(|_| Error::generic("Cannot convert table URL to path"))?;
        let dv_uuid = uuid::Uuid::new_v4();
        let dv_file_name = format!("deletion_vector_{}.bin", dv_uuid);
        let dv_file_path = table_path.join(&dv_file_name);

        let mut dv_file = std::fs::File::create(&dv_file_path)
            .map_err(|e| Error::generic(format!("Failed to create DV file: {}", e)))?;
        let mut writer = StreamingDeletionVectorWriter::new(&mut dv_file);

        for (file_path, row_indexes) in &dv_updates {
            let mut dv = KernelDeletionVector::new();
            dv.add_deleted_row_indexes(row_indexes.iter().copied());
            let write_result = writer.write_deletion_vector(dv)?;

            // Construct DeletionVectorDescriptor with persisted-relative storage type
            // The path is z85-encoded UUID (20 chars, no prefix)
            let encoded_path = z85::encode(dv_uuid.as_bytes());
            let descriptor = DeletionVectorDescriptor {
                storage_type: DeletionVectorStorageType::PersistedRelative,
                path_or_inline_dv: encoded_path,
                offset: Some(write_result.offset),
                size_in_bytes: write_result.size_in_bytes,
                cardinality: write_result.cardinality,
            };
            dv_descriptors.insert(file_path.clone(), descriptor);
        }
        writer.finalize()?;

        // Scan the table to get file metadata for DV updates
        let dv_snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
        let scan = dv_snapshot.scan_builder().build()?;
        let metadata_iter = scan.scan_metadata(engine)?;
        let scan_files_iter =
            delta_kernel::transaction::Transaction::scan_metadata_to_engine_data(metadata_iter);
        txn.update_deletion_vectors(dv_descriptors, scan_files_iter)?;
    }

    // Process append and rewrite-add operations
    for op in ops {
        match op {
            WriteOp::Append {
                data,
                partition_values,
            } => {
                let parquet_path = test_case_root.join(data);
                if parquet_path.exists() {
                    let write_context = Arc::new(txn.get_write_context());
                    let arrow_data = read_parquet_as_arrow(&parquet_path)?;
                    // Convert Option<String> values to String, filtering out nulls
                    let pv: HashMap<String, String> = partition_values
                        .as_ref()
                        .map(|m| {
                            m.iter()
                                .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
                                .collect()
                        })
                        .unwrap_or_default();
                    let file_metadata = engine
                        .write_parquet(&arrow_data, write_context.as_ref(), pv)
                        .await
                        .map_err(|e| Error::generic(format!("Parquet write failed: {}", e)))?;
                    txn.add_files(file_metadata);
                } else {
                    return Err(Error::generic(format!(
                        "Append data file not found: {}",
                        parquet_path.display()
                    )));
                }
            }
            WriteOp::Rewrite {
                data,
                partition_values,
                ..
            } => {
                // The remove part was handled above; here we handle the add part
                let data_paths = data.paths();
                for data_path in data_paths {
                    let parquet_path = test_case_root.join(data_path);
                    if parquet_path.exists() {
                        let write_context = Arc::new(txn.get_write_context());
                        let arrow_data = read_parquet_as_arrow(&parquet_path)?;

                        // Get partition values: use explicit values if provided,
                        // otherwise extract from the parquet data using table's partition columns
                        let pv: HashMap<String, String> = if let Some(m) = partition_values {
                            m.iter()
                                .filter_map(|(k, v)| {
                                    v.as_ref().map(|val| (k.clone(), val.clone()))
                                })
                                .collect()
                        } else {
                            extract_partition_values_from_data(&arrow_data, &partition_columns)?
                        };

                        let file_metadata = engine
                            .write_parquet(&arrow_data, write_context.as_ref(), pv)
                            .await
                            .map_err(|e| Error::generic(format!("Parquet write failed: {}", e)))?;
                        txn.add_files(file_metadata);
                    }
                }
            }
            WriteOp::SetTransaction { app_id, version } => {
                txn = txn.with_transaction_id(app_id.clone(), *version);
            }
            WriteOp::SetDomainMetadata {
                domain,
                configuration,
            } => {
                txn = txn.with_domain_metadata(domain.clone(), configuration.clone());
            }
            WriteOp::RemoveDomainMetadata { domain } => {
                txn = txn.with_domain_metadata_removed(domain.clone());
            }
            WriteOp::Delete { .. }
            | WriteOp::SetDeletes { .. }
            | WriteOp::AddDeletes { .. } => {
                // Already handled above
            }
            WriteOp::UpdateSchema { .. } => unsupported_ops.push("updateSchema".to_string()),
            WriteOp::UpdateProperties { .. } => {
                // TODO: Kernel Transaction API has no method to update table properties
                // on existing tables. This is a known kernel limitation.
                unsupported_ops.push("updateProperties (kernel has no API for this)".to_string());
            }
            WriteOp::Replace { .. } => unsupported_ops.push("replace".to_string()),
            WriteOp::RestoreRows { .. } => unsupported_ops.push("restoreRows".to_string()),
            WriteOp::RemoveDeletes { .. } => unsupported_ops.push("removeDeletes".to_string()),
            WriteOp::SetClustering { .. } => unsupported_ops.push("setClustering".to_string()),
            WriteOp::Create { .. } => {
                // Should not reach here (handled separately)
            }
        }
    }

    if !unsupported_ops.is_empty() {
        return Err(Error::generic(format!(
            "Unsupported write ops: {}",
            unsupported_ops.join(", ")
        )));
    }

    // Only use blind append when we have ONLY appends (no removes, no DVs)
    if has_appends_only {
        txn = txn.with_blind_append();
    }

    match txn.commit(engine)? {
        CommitResult::CommittedTransaction(committed) => {
            println!("    Committed version {}", committed.commit_version());
        }
        _ => {
            return Err(Error::generic("Commit was not committed"));
        }
    }

    Ok(())
}

// NOTE: inject_metadata_update and property_to_features were REMOVED.
// Kernel Transaction API has no method to update table properties on existing tables.
// This is a known kernel limitation. Tests that need updateProperties will fail honestly
// rather than having the harness hack around the limitation by patching commit JSON.

/// Extract partition values from a parquet data batch using the table's partition columns.
///
/// Reads the first row of the data and extracts the partition column values.
/// Returns empty map if there are no partition columns or data is empty.
fn extract_partition_values_from_data(
    data: &ArrowEngineData,
    partition_columns: &[String],
) -> DeltaResult<HashMap<String, String>> {
    use delta_kernel::arrow::array::Array;
    if partition_columns.is_empty() {
        return Ok(HashMap::new());
    }
    let batch = data.record_batch();
    if batch.num_rows() == 0 {
        return Ok(HashMap::new());
    }

    let mut pv = HashMap::new();
    for col_name in partition_columns {
        if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
            let col = batch.column(col_idx);
            if !col.is_null(0) {
                // Extract value as string from the first row
                let val = delta_kernel::arrow::util::display::ArrayFormatter::try_new(
                    col.as_ref(),
                    &delta_kernel::arrow::util::display::FormatOptions::default(),
                )
                .ok()
                .and_then(|f| {
                    let mut buf = String::new();
                    use std::fmt::Write;
                    write!(&mut buf, "{}", f.value(0)).ok()?;
                    Some(buf)
                });
                if let Some(v) = val {
                    pv.insert(col_name.clone(), v);
                }
            }
        }
    }
    Ok(pv)
}

/// Read a parquet file and return it as ArrowEngineData.
fn read_parquet_as_arrow(parquet_path: &Path) -> DeltaResult<ArrowEngineData> {
    let file = std::fs::File::open(parquet_path)
        .map_err(|e| Error::generic(format!("Failed to open parquet: {}", e)))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::generic(format!("Failed to create parquet reader: {}", e)))?;
    let reader = builder
        .build()
        .map_err(|e| Error::generic(format!("Failed to build parquet reader: {}", e)))?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| Error::generic(format!("Failed to read parquet batch: {}", e)))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(Error::generic("Empty parquet file"));
    }

    let schema = batches[0].schema();
    let combined = concat_batches(&schema, batches.iter())
        .map_err(|e| Error::generic(format!("Failed to concat batches: {}", e)))?;

    Ok(ArrowEngineData::new(combined))
}

/// Parse a JSON schema value into a kernel SchemaRef.
fn parse_schema_from_json(schema_val: &serde_json::Value) -> DeltaResult<SchemaRef> {
    let fields = schema_val
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| Error::generic("Schema must have 'fields' array"))?;

    let kernel_fields: Vec<StructField> = fields
        .iter()
        .map(|field| {
            let name = field
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("unknown");
            let type_val = field.get("type");
            let nullable = field
                .get("nullable")
                .and_then(|n| n.as_bool())
                .unwrap_or(true);

            let data_type = parse_data_type(type_val)?;
            Ok(StructField::new(name, data_type, nullable))
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    Ok(Arc::new(StructType::try_new(kernel_fields)?))
}

/// Parse a JSON type value into a kernel DataType.
fn parse_data_type(type_val: Option<&serde_json::Value>) -> DeltaResult<DataType> {
    let type_val = type_val.ok_or_else(|| Error::generic("Missing type"))?;

    match type_val {
        serde_json::Value::String(s) => match s.as_str() {
            "boolean" => Ok(DataType::BOOLEAN),
            "byte" | "tinyint" => Ok(DataType::BYTE),
            "short" | "smallint" => Ok(DataType::SHORT),
            "integer" | "int" => Ok(DataType::INTEGER),
            "long" | "bigint" => Ok(DataType::LONG),
            "float" => Ok(DataType::FLOAT),
            "double" => Ok(DataType::DOUBLE),
            "string" => Ok(DataType::STRING),
            "binary" => Ok(DataType::BINARY),
            "date" => Ok(DataType::DATE),
            "timestamp" => Ok(DataType::TIMESTAMP),
            "timestamp_ntz" => Ok(DataType::TIMESTAMP_NTZ),
            s if s.starts_with("decimal") => {
                if let Some(params) = s.strip_prefix("decimal(").and_then(|p| p.strip_suffix(')'))
                {
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        let precision: u8 = parts[0].trim().parse().unwrap_or(10);
                        let scale: u8 = parts[1].trim().parse().unwrap_or(0);
                        DataType::decimal(precision, scale)
                    } else {
                        DataType::decimal(10, 0)
                    }
                } else {
                    DataType::decimal(10, 0)
                }
            }
            _ => Err(Error::generic(format!("Unknown type string: {}", s))),
        },
        serde_json::Value::Object(obj) => {
            let type_name = obj
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("unknown");
            match type_name {
                "struct" => {
                    let schema = parse_schema_from_json(type_val)?;
                    Ok(DataType::Struct(Box::new(
                        StructType::try_new(schema.fields().cloned())?,
                    )))
                }
                "array" => {
                    let element_type = parse_data_type(obj.get("elementType"))?;
                    let contains_null = obj
                        .get("containsNull")
                        .and_then(|c| c.as_bool())
                        .unwrap_or(true);
                    Ok(DataType::Array(Box::new(ArrayType::new(
                        element_type,
                        contains_null,
                    ))))
                }
                "map" => {
                    let key_type = parse_data_type(obj.get("keyType"))?;
                    let value_type = parse_data_type(obj.get("valueType"))?;
                    let value_contains_null = obj
                        .get("valueContainsNull")
                        .and_then(|c| c.as_bool())
                        .unwrap_or(true);
                    Ok(DataType::Map(Box::new(MapType::new(
                        key_type,
                        value_type,
                        value_contains_null,
                    ))))
                }
                _ => Err(Error::generic(format!(
                    "Unknown compound type: {}",
                    type_name
                ))),
            }
        }
        _ => Err(Error::generic(format!(
            "Invalid type value: {:?}",
            type_val
        ))),
    }
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> DeltaResult<()> {
    std::fs::create_dir_all(dst)
        .map_err(|e| Error::generic(format!("Failed to create dir {}: {}", dst.display(), e)))?;

    for entry in std::fs::read_dir(src)
        .map_err(|e| Error::generic(format!("Failed to read dir {}: {}", src.display(), e)))?
    {
        let entry = entry.map_err(|e| Error::generic(format!("Dir entry error: {}", e)))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path).map_err(|e| {
                Error::generic(format!(
                    "Failed to copy {} -> {}: {}",
                    src_path.display(),
                    dst_path.display(),
                    e
                ))
            })?;
        }
    }
    Ok(())
}
