//! Workload execution logic for Delta workload specifications.

use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::snapshot::Snapshot;
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
                                    col.clone()
                                } else {
                                    delta_kernel::arrow::compute::cast(col, field.data_type())
                                        .unwrap_or_else(|_| col.clone())
                                }
                            })
                            .collect();
                        RecordBatch::try_new(stripped_schema.clone(), cast_columns)
                    })
                    .map_err(Error::from)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        concat_batches(&stripped_schema, normalized_batches.iter()).map_err(Error::from)
    }
}

/// Result of executing a snapshot workload.
/// Protocol and metadata are stored as `serde_json::Value` to allow flexible
/// comparison without duplicating kernel's Protocol/Metadata types.
#[derive(Debug)]
pub struct SnapshotResult {
    pub version: Version,
    pub protocol: serde_json::Value,
    pub metadata: serde_json::Value,
}

/// Result of executing a txn (SetTransaction) workload
pub struct TxnResult {
    /// The application ID queried
    pub app_id: String,
    /// The transaction version returned by the kernel (None if app_id not found)
    pub txn_version: Option<i64>,
}

/// Result of executing a domain metadata workload
pub struct DomainMetadataResult {
    /// The domain name
    pub domain: String,
    /// The configuration (None if domain is removed/not found)
    pub configuration: Option<String>,
}

/// Workload execution result
#[allow(clippy::large_enum_variant)]
pub enum WorkloadResult {
    Read(ReadResult),
    Snapshot(SnapshotResult),
    Txn(TxnResult),
    DomainMetadata(DomainMetadataResult),
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
        WorkloadSpec::Txn {
            version, expected, ..
        } => {
            let result = execute_txn_workload(engine, table_root, *version, &expected.app_id)?;
            Ok(WorkloadResult::Txn(result))
        }
        WorkloadSpec::DomainMetadata {
            version, expected, ..
        } => {
            let result =
                execute_domain_metadata_workload(engine, table_root, *version, &expected.domain)?;
            Ok(WorkloadResult::DomainMetadata(result))
        }
        WorkloadSpec::Cdf {
            start_version,
            end_version,
            ..
        } => {
            let result =
                execute_cdf_workload(engine, table_root, start_version.unwrap_or(0), *end_version)?;
            Ok(WorkloadResult::Read(result))
        }
        WorkloadSpec::Unsupported => Err(Error::generic(
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
    // Parse predicate for post-scan row filtering. We intentionally do NOT pass
    // the predicate to scan_builder.with_predicate() because kernel's data skipping
    // can incorrectly prune partition files when the predicate literal type doesn't
    // match the string-typed partition values in the delta log.
    let parsed_predicate = if let Some(pred_str) = predicate {
        use super::predicate_parser::parse_predicate_with_schema;
        let pred = parse_predicate_with_schema(pred_str, &table_schema).map_err(|e| {
            Error::generic(format!("Failed to parse predicate '{}': {}", pred_str, e))
        })?;
        Some(pred)
    } else {
        None
    };
    let scan = scan_builder.build()?;

    // Get schema from scan
    use delta_kernel::engine::arrow_conversion::TryFromKernel;
    let arrow_schema =
        delta_kernel::arrow::datatypes::Schema::try_from_kernel(scan.logical_schema().as_ref())
            .map_err(|e| Error::generic(format!("Failed to convert schema: {}", e)))?;
    let schema = Arc::new(arrow_schema);

    // Execute scan and apply post-scan predicate filtering.
    // Kernel uses predicates for data skipping (file pruning) but does not filter rows.
    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|data| -> DeltaResult<_> {
            let mut record_batch = data?.try_into_record_batch()?;
            if let Some(ref pred) = parsed_predicate {
                use delta_kernel::arrow::compute::filter_record_batch;
                use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
                let mask = evaluate_predicate(pred, &record_batch, false)?;
                record_batch = filter_record_batch(&record_batch, &mask)?;
            }
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

    // Serialize kernel types directly to serde_json::Value — no duplicate structs needed
    let protocol_value = serde_json::to_value(protocol)
        .map_err(|e| Error::generic(format!("Failed to serialize protocol: {e}")))?;
    let metadata_value = serde_json::to_value(metadata)
        .map_err(|e| Error::generic(format!("Failed to serialize metadata: {e}")))?;

    Ok(SnapshotResult {
        version: snapshot.version(),
        protocol: protocol_value,
        metadata: metadata_value,
    })
}

/// Execute a txn (SetTransaction) workload
pub fn execute_txn_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    app_id: &str,
) -> DeltaResult<TxnResult> {
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v as Version);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let txn_version = snapshot.get_app_id_version(app_id, engine.as_ref())?;

    Ok(TxnResult {
        app_id: app_id.to_string(),
        txn_version,
    })
}

/// Execute a domain metadata workload
pub fn execute_domain_metadata_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    domain: &str,
) -> DeltaResult<DomainMetadataResult> {
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v as Version);
    }
    let snapshot = builder.build(engine.as_ref())?;

    let configuration = snapshot.get_domain_metadata_internal(domain, engine.as_ref())?;

    Ok(DomainMetadataResult {
        domain: domain.to_string(),
        configuration,
    })
}

/// Execute a CDF (Change Data Feed) workload
pub fn execute_cdf_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    start_version: i64,
    end_version: Option<i64>,
) -> DeltaResult<ReadResult> {
    use delta_kernel::table_changes::TableChanges;

    let start = start_version as Version;
    let end = end_version.map(|v| v as Version);
    let table_changes = TableChanges::try_new(table_root.clone(), engine.as_ref(), start, end)?;

    use delta_kernel::engine::arrow_conversion::TryFromKernel;
    let kernel_schema = table_changes.schema().clone();
    let arrow_schema = delta_kernel::arrow::datatypes::Schema::try_from_kernel(&kernel_schema)
        .map_err(|e| Error::generic(format!("Failed to convert CDF schema: {}", e)))?;
    let schema = Arc::new(arrow_schema);

    let scan = table_changes.into_scan_builder().build()?;
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
