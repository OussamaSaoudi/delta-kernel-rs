//! Workload execution logic for improved_dat test cases.

use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch};
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::Predicate;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error, Version};
use itertools::Itertools;
use url::Url;

use super::predicate_parser::parse_predicate_with_schema;
use super::types::WorkloadSpec;

/// Result of executing a read workload
pub struct ReadResult {
    /// The record batches from the scan
    pub batches: Vec<RecordBatch>,
    /// The schema of the data
    pub schema: Option<Arc<delta_kernel::arrow::datatypes::Schema>>,
}

impl ReadResult {
    /// Concatenate all batches into a single RecordBatch
    pub fn concat(self) -> DeltaResult<RecordBatch> {
        let schema = self.schema.ok_or_else(|| Error::generic("No schema"))?;
        Ok(concat_batches(&schema, self.batches.iter()).map_err(Error::from)?)
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

/// Result of executing a domain metadata workload
pub struct DomainMetadataResult {
    /// The domain name
    pub domain: String,
    /// The configuration (None if domain is removed/not found)
    pub configuration: Option<String>,
}

/// Execute a workload specification and return the result
pub fn execute_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    spec: &WorkloadSpec,
) -> DeltaResult<WorkloadResult> {
    match spec {
        WorkloadSpec::Read {
            predicate,
            version,
            timestamp,
            ..
        } => {
            let result = execute_read_workload(
                engine,
                table_root,
                predicate.as_deref(),
                *version,
                timestamp.as_deref(),
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
        WorkloadSpec::Txn { version, .. } => {
            // Transaction workloads are validated against expected values in validation.rs
            let result = execute_snapshot_workload(engine, table_root, Some(*version), None)?;
            Ok(WorkloadResult::Snapshot(result))
        }
        WorkloadSpec::DomainMetadata {
            version, expected, ..
        } => {
            let result = execute_domain_metadata_workload(
                engine,
                table_root,
                *version,
                &expected.domain,
            )?;
            Ok(WorkloadResult::DomainMetadata(result))
        }
    }
}

/// Workload execution result
pub enum WorkloadResult {
    Read(ReadResult),
    Snapshot(SnapshotResult),
    DomainMetadata(DomainMetadataResult),
}

/// Execute a read workload
pub fn execute_read_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    predicate_str: Option<&str>,
    version: Option<i64>,
    timestamp: Option<&str>,
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

    // Get schema before scan_builder() takes ownership
    let table_schema = snapshot.schema();

    // Parse predicate (store for row-level filtering after scan)
    let predicate: Option<Predicate> = if let Some(pred_str) = predicate_str {
        // Use schema-aware parser to coerce literal types to match column types
        let pred = parse_predicate_with_schema(pred_str, table_schema.as_ref())
            .map_err(|e| Error::generic(format!("Failed to parse predicate: {}", e)))?;
        Some(pred)
    } else {
        None
    };

    // Build scan with optional predicate (for file-level data skipping)
    let mut scan_builder = snapshot.scan_builder();
    if let Some(ref pred) = predicate {
        scan_builder = scan_builder.with_predicate(Some(Arc::new(pred.clone())));
    }
    let scan = scan_builder.build()?;

    // Get schema from scan (needed for empty results)
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

    // Apply row-level filtering if predicate was provided
    let filtered_batches = if let Some(ref pred) = predicate {
        batches
            .into_iter()
            .map(|batch| {
                if batch.num_rows() == 0 {
                    return Ok(batch);
                }
                // Evaluate predicate to get a boolean mask
                let mask = evaluate_predicate(pred, &batch, false)?;
                // Filter the batch using the mask
                filter_record_batch(&batch, &mask).map_err(Error::from)
            })
            .collect::<DeltaResult<Vec<_>>>()?
    } else {
        batches
    };

    Ok(ReadResult {
        batches: filtered_batches,
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

    // Extract metadata
    let config = snapshot.table_configuration();
    let protocol = config.protocol();
    let metadata = config.metadata();

    Ok(SnapshotResult {
        version: snapshot.version(),
        min_reader_version: protocol.min_reader_version() as i32,
        min_writer_version: protocol.min_writer_version() as i32,
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

/// Execute a domain metadata workload
pub fn execute_domain_metadata_workload(
    engine: Arc<dyn Engine>,
    table_root: &Url,
    version: Option<i64>,
    domain: &str,
) -> DeltaResult<DomainMetadataResult> {
    // Build snapshot
    let mut builder = Snapshot::builder_for(table_root.clone());
    if let Some(v) = version {
        builder = builder.at_version(v as Version);
    }
    let snapshot = builder.build(engine.as_ref())?;

    // Get domain metadata
    let configuration = snapshot.get_domain_metadata(domain, engine.as_ref())?;

    Ok(DomainMetadataResult {
        domain: domain.to_string(),
        configuration,
    })
}

/// Resolve a timestamp string to a table version by scanning commit files.
///
/// This function reads commit file timestamps from the _delta_log directory
/// and finds the latest version that has a commit timestamp <= the requested timestamp.
fn resolve_timestamp_to_version(
    engine: &dyn Engine,
    table_root: &Url,
    timestamp_str: &str,
) -> DeltaResult<Version> {
    use chrono::NaiveDateTime;

    // Parse the timestamp (format: "YYYY-MM-DD HH:MM:SS.mmm")
    let target_ts = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.3f")
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| Error::generic(format!("Failed to parse timestamp '{}': {}", timestamp_str, e)))?;

    let target_millis = target_ts.and_utc().timestamp_millis();

    // Get the log path
    let log_url = table_root.join("_delta_log/")?;

    // List all commit files
    let storage = engine.storage_handler();
    let files = storage.list_from(&log_url)?;

    // Find all JSON commit files and their timestamps
    let mut version_timestamps: Vec<(Version, i64)> = Vec::new();

    for file_meta_result in files {
        let file_meta = file_meta_result?;
        let path = file_meta.location.as_ref();
        // Get the filename from the path
        let filename = path.rsplit('/').next().unwrap_or("");
        // Match pattern: 00000000000000000000.json
        if filename.ends_with(".json") && !filename.contains("checkpoint") {
            if let Ok(version) = filename.trim_end_matches(".json").parse::<Version>() {
                // Use file modification time as commit timestamp
                // This is a simplification - ideally we'd read the commitInfo from the file
                let ts_millis = file_meta.last_modified;
                version_timestamps.push((version, ts_millis));
            }
        }
    }

    // Sort by version
    version_timestamps.sort_by_key(|(v, _)| *v);

    // Find the latest version with timestamp <= target
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
