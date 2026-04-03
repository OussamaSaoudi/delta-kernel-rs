//! Write spec executor for Delta acceptance tests.
//!
//! Reads `write_spec.json` and executes write operations using kernel-rs APIs.
//! After execution, the resulting table is validated via read/snapshot specs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Int64Array, MapArray, StringArray, StructArray,
};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::schema::{SchemaRef, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Engine;
use delta_kernel_benchmarks::predicate_parser::parse_predicate;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Deserialize;
use tracing::{info, warn};
use url::Url;

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteSpec {
    pub description: Option<String>,
    pub commits: Vec<WriteCommit>,
    pub verification: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteCommit {
    pub operation: String,
    pub description: Option<String>,
    pub schema: Option<serde_json::Value>,
    pub partition_columns: Option<Vec<String>>,
    pub properties: Option<HashMap<String, String>>,
    pub data_files: Option<Vec<String>>,
    pub predicate: Option<String>,
    pub set: Option<HashMap<String, String>>,
    pub sql: Option<serde_json::Value>,
    pub app_id: Option<String>,
    pub version: Option<i64>,
    pub domain: Option<String>,
    pub configuration: Option<String>,
    pub removed: Option<bool>,
    pub retention_hours: Option<i64>,
    pub set_properties: Option<HashMap<String, String>>,
    pub remove_properties: Option<Vec<String>>,
    pub add_columns: Option<Vec<serde_json::Value>>,
    pub rename_columns: Option<HashMap<String, String>>,
    pub drop_columns: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct WriteResult {
    pub commits_applied: usize,
    pub commits_skipped: usize,
    pub skipped_reasons: Vec<String>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

pub fn execute_write_spec(
    write_spec: &WriteSpec,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<WriteResult, Box<dyn std::error::Error>> {
    let mut applied = 0;
    let mut skipped = 0;
    let mut skipped_reasons = Vec::new();

    for (i, commit) in write_spec.commits.iter().enumerate() {
        match execute_commit(commit, table_root, test_case_root, engine) {
            Ok(Outcome::Applied) => {
                info!(commit = i, op = commit.operation.as_str(), "Applied");
                applied += 1;
            }
            Ok(Outcome::Skipped(reason)) => {
                info!(commit = i, op = commit.operation.as_str(), reason = reason.as_str(), "Skipped");
                skipped_reasons.push(format!("commit {}: {} — {}", i, commit.operation, reason));
                skipped += 1;
            }
            Err(e) => {
                return Err(format!("commit {} ({}) failed: {}", i, commit.operation, e).into());
            }
        }
    }

    Ok(WriteResult { commits_applied: applied, commits_skipped: skipped, skipped_reasons })
}

enum Outcome {
    Applied,
    Skipped(String),
}

fn execute_commit(
    commit: &WriteCommit,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    match commit.operation.as_str() {
        "create_table" => execute_create_table(commit, table_root, test_case_root, engine),
        "insert" => execute_insert(commit, table_root, test_case_root, engine),
        "delete" => execute_delete(commit, table_root, engine),
        "set_transaction" => execute_set_transaction(commit, table_root, engine),
        "set_domain_metadata" => execute_set_domain_metadata(commit, table_root, engine),
        "checkpoint" => execute_checkpoint(table_root, engine),
        "alter_table" => Ok(Outcome::Skipped("alter_table not yet supported".into())),
        "update" => Ok(Outcome::Skipped("update not yet supported".into())),
        "truncate" => Ok(Outcome::Skipped("truncate not yet supported".into())),
        "replace_table" => Ok(Outcome::Skipped("replace_table not yet supported".into())),
        "insert_overwrite" => Ok(Outcome::Skipped("insert_overwrite not yet supported".into())),
        "vacuum" => Ok(Outcome::Skipped("vacuum is engine-level".into())),
        other => Ok(Outcome::Skipped(format!("unknown operation: {}", other))),
    }
}

// ---------------------------------------------------------------------------
// create_table
// ---------------------------------------------------------------------------

fn execute_create_table(
    commit: &WriteCommit,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    use delta_kernel::transaction::create_table::create_table;

    let schema_json = commit.schema.as_ref().ok_or("create_table: missing schema")?;
    let schema: StructType = serde_json::from_value(schema_json.clone())
        .map_err(|e| format!("create_table: invalid schema: {}", e))?;
    let schema = Arc::new(schema);

    let mut builder = create_table(table_root.as_str(), schema, "delta-dat-write-executor");

    if let Some(ref props) = commit.properties {
        builder = builder.with_table_properties(props.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }

    if let Some(ref part_cols) = commit.partition_columns {
        if !part_cols.is_empty() {
            warn!(partition_columns = ?part_cols, "Partition columns not yet supported by kernel create_table");
        }
    }

    let txn = builder.build(engine, Box::new(FileSystemCommitter::new()))?;
    let result = txn.commit(engine)?;

    if result.is_committed() {
        if let Some(ref data_files) = commit.data_files {
            if !data_files.is_empty() {
                append_data_files(data_files, &commit.partition_columns, table_root, test_case_root, engine)?;
            }
        }
        Ok(Outcome::Applied)
    } else {
        Err("create_table: commit was not committed".into())
    }
}

// ---------------------------------------------------------------------------
// insert (blind append)
// ---------------------------------------------------------------------------

fn execute_insert(
    commit: &WriteCommit,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let data_files = commit.data_files.as_ref().ok_or("insert: missing dataFiles")?;
    if data_files.is_empty() {
        return Ok(Outcome::Skipped("insert: empty dataFiles".into()));
    }
    append_data_files(data_files, &None, table_root, test_case_root, engine)?;
    Ok(Outcome::Applied)
}

// ---------------------------------------------------------------------------
// delete (via DVs)
// ---------------------------------------------------------------------------

fn execute_delete(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let predicate_str = commit.predicate.as_ref().ok_or("delete: missing predicate")?;

    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let dvs_enabled = snapshot
        .table_configuration()
        .is_feature_supported(&delta_kernel::table_features::TableFeature::DeletionVectors);

    if !dvs_enabled {
        return Ok(Outcome::Skipped("delete: DVs not enabled".into()));
    }

    // Parse the predicate
    let predicate = parse_predicate(predicate_str)
        .map_err(|e| format!("delete: failed to parse predicate '{}': {}", predicate_str, e))?;

    let table_dir = table_root
        .to_file_path()
        .map_err(|_| "Cannot convert table URL to file path")?;

    // Get write context for DV path generation
    let write_context = {
        let txn_tmp = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine)?;
        txn_tmp.get_write_context()
    };

    // Scan the delta log to get current active file paths
    let log_dir = table_dir.join("_delta_log");
    let mut add_paths = std::collections::HashSet::new();
    let mut remove_paths = std::collections::HashSet::new();
    if log_dir.exists() {
        let mut entries: Vec<_> = std::fs::read_dir(&log_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let content = std::fs::read_to_string(entry.path())?;
            for line in content.lines() {
                if let Ok(node) = serde_json::from_str::<serde_json::Value>(line) {
                    if let Some(add) = node.get("add") {
                        if let Some(p) = add.get("path").and_then(|p| p.as_str()) {
                            add_paths.insert(p.to_string());
                            remove_paths.remove(p);
                        }
                    }
                    if let Some(rem) = node.get("remove") {
                        if let Some(p) = rem.get("path").and_then(|p| p.as_str()) {
                            remove_paths.insert(p.to_string());
                            add_paths.remove(p);
                        }
                    }
                }
            }
        }
    }

    // For each active file, read parquet, evaluate predicate, collect matching row indexes
    let mut dv_map = HashMap::new();

    for file_path in &add_paths {
        let full_path = table_dir.join(file_path);
        if !full_path.exists() {
            continue;
        }

        // Read parquet as RecordBatches
        let parquet_file = std::fs::File::open(&full_path)?;
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(parquet_file)?;
        let mut batch_reader = builder.build()?;

        let mut row_offset: u64 = 0;
        let mut deleted_indexes = Vec::new();

        while let Some(batch) = batch_reader.next() {
            let batch = batch?;
            let selection = evaluate_predicate(&predicate, &batch, false)?;
            for (i, val) in selection.iter().enumerate() {
                if val == Some(true) {
                    deleted_indexes.push(row_offset + i as u64);
                }
            }
            row_offset += batch.num_rows() as u64;
        }

        if !deleted_indexes.is_empty() {
            let mut dv = KernelDeletionVector::new();
            dv.add_deleted_row_indexes(deleted_indexes);

            // Generate DV path and write binary directly to filesystem
            let dv_path = write_context.new_deletion_vector_path(String::new());
            let dv_absolute = dv_path.absolute_path()?;
            let dv_fs_path = dv_absolute
                .to_file_path()
                .map_err(|_| "Cannot convert DV URL to file path")?;

            // Write DV binary
            let mut dv_buffer = Vec::new();
            let mut dv_writer = StreamingDeletionVectorWriter::new(&mut dv_buffer);
            let dv_write_result = dv_writer.write_deletion_vector(dv)?;
            dv_writer.finalize()?;

            if let Some(parent) = dv_fs_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dv_fs_path, &dv_buffer)?;

            let descriptor = dv_write_result.to_descriptor(&dv_path);
            dv_map.insert(file_path.clone(), descriptor);
        }
    }

    if dv_map.is_empty() {
        info!("delete: predicate matched no rows");
        return Ok(Outcome::Applied);
    }

    // Commit DVs via update_deletion_vectors (internal API)
    // Need two snapshots: one for the transaction, one for scan_metadata
    let snapshot_for_scan = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let scan = snapshot_for_scan.scan_builder().build()?;
    let scan_metadata: Vec<_> = scan
        .scan_metadata(engine)?
        .collect::<Result<Vec<_>, _>>()?;
    let scan_files: Vec<_> = scan_metadata.into_iter().map(|sm| sm.scan_files).collect();

    let snapshot_for_txn = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let mut txn = snapshot_for_txn
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_operation("DELETE".to_string());

    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;
    let result = txn.commit(engine)?;

    if result.is_committed() {
        Ok(Outcome::Applied)
    } else {
        Err("delete: commit was not committed".into())
    }
}

// ---------------------------------------------------------------------------
// set_transaction
// ---------------------------------------------------------------------------

fn execute_set_transaction(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let app_id = commit.app_id.as_ref().ok_or("set_transaction: missing appId")?;
    let version = commit.version.ok_or("set_transaction: missing version")?;

    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_transaction_id(app_id.clone(), version);
    let result = txn.commit(engine)?;

    if result.is_committed() {
        Ok(Outcome::Applied)
    } else {
        Err("set_transaction: commit was not committed".into())
    }
}

// ---------------------------------------------------------------------------
// set_domain_metadata
// ---------------------------------------------------------------------------

fn execute_set_domain_metadata(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let domain = commit.domain.as_ref().ok_or("set_domain_metadata: missing domain")?;
    let config = commit.configuration.as_deref().unwrap_or("");
    let removed = commit.removed.unwrap_or(false);

    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine)?;

    if removed {
        txn = txn.with_domain_metadata_removed(domain.clone());
    } else {
        txn = txn.with_domain_metadata(domain.clone(), config.to_string());
    }

    let result = txn.commit(engine)?;
    if result.is_committed() {
        Ok(Outcome::Applied)
    } else {
        Err("set_domain_metadata: commit was not committed".into())
    }
}

// ---------------------------------------------------------------------------
// checkpoint
// ---------------------------------------------------------------------------

fn execute_checkpoint(
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    snapshot.checkpoint(engine)?;
    Ok(Outcome::Applied)
}

// ---------------------------------------------------------------------------
// Helpers: append data files
// ---------------------------------------------------------------------------

fn append_data_files(
    data_files: &[String],
    partition_columns: &Option<Vec<String>>,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine)?
        .with_blind_append()
        .with_operation("delta-dat-write-executor".to_string());

    let add_schema = txn.add_files_schema();
    let mut paths = Vec::new();
    let mut sizes = Vec::new();
    let mut mod_times = Vec::new();
    let mut row_counts = Vec::new();
    let mut partition_maps: Vec<HashMap<String, String>> = Vec::new();

    let table_dir = table_root.to_file_path()
        .map_err(|_| "Cannot convert table URL to file path")?;

    for data_file in data_files {
        let resolved = test_case_root.join(data_file);
        if !resolved.exists() {
            return Err(format!("Data file not found: {}", resolved.display()).into());
        }

        // Read real row count from parquet footer
        let file = std::fs::File::open(&resolved)?;
        let reader = SerializedFileReader::new(file)?;
        let num_rows = reader.metadata().file_metadata().num_rows();

        let file_meta = std::fs::metadata(&resolved)?;
        let file_size = file_meta.len() as i64;
        let mod_time = file_meta.modified()?.duration_since(std::time::UNIX_EPOCH)?.as_millis() as i64;

        let file_name = resolved.file_name().ok_or("no filename")?.to_string_lossy().to_string();

        let part_values = extract_partition_values(data_file, partition_columns);

        let target_relative = if !part_values.is_empty() {
            let part_dir: String = part_values.iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>().join("/");
            let target_dir = table_dir.join(&part_dir);
            std::fs::create_dir_all(&target_dir)?;
            std::fs::copy(&resolved, target_dir.join(&file_name))?;
            format!("{}/{}", part_dir, file_name)
        } else {
            std::fs::copy(&resolved, table_dir.join(&file_name))?;
            file_name
        };

        paths.push(target_relative);
        sizes.push(file_size);
        mod_times.push(mod_time);
        row_counts.push(num_rows);
        partition_maps.push(part_values);
    }

    if paths.is_empty() {
        return Err("No data files to append".into());
    }

    let add_metadata = build_add_files_metadata(&add_schema, &paths, &sizes, &mod_times, &row_counts, &partition_maps)?;
    txn.add_files(add_metadata);

    let result = txn.commit(engine)?;
    if result.is_committed() {
        Ok(())
    } else {
        Err("append: commit was not committed".into())
    }
}

fn extract_partition_values(data_file: &str, partition_columns: &Option<Vec<String>>) -> HashMap<String, String> {
    let mut result = HashMap::new();
    if let Some(ref cols) = partition_columns {
        for component in data_file.split('/') {
            if let Some(eq_pos) = component.find('=') {
                let key = &component[..eq_pos];
                let value = &component[eq_pos + 1..];
                if cols.iter().any(|c| c == key) {
                    result.insert(key.to_string(), value.to_string());
                }
            }
        }
    }
    result
}

fn build_add_files_metadata(
    add_schema: &SchemaRef,
    paths: &[String],
    sizes: &[i64],
    mod_times: &[i64],
    row_counts: &[i64],
    partition_maps: &[HashMap<String, String>],
) -> Result<Box<dyn delta_kernel::EngineData>, Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_conversion::TryFromKernel;

    let num_files = paths.len();

    let path_array = StringArray::from(paths.to_vec());
    let size_array = Int64Array::from(sizes.to_vec());
    let mod_time_array = Int64Array::from(mod_times.to_vec());
    let num_records_array = Int64Array::from(row_counts.to_vec());
    let partition_values_array = build_partition_values_array(partition_maps)?;

    let empty_struct_fields: delta_kernel::arrow::datatypes::Fields = Vec::<Arc<Field>>::new().into();
    let empty_struct = StructArray::new_empty_fields(num_files, None);
    let tight_bounds_array = BooleanArray::from(vec![true; num_files]);

    let stats_struct = StructArray::from(vec![
        (Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)), Arc::new(num_records_array) as ArrayRef),
        (Arc::new(Field::new("nullCount", ArrowDataType::Struct(empty_struct_fields.clone()), true)), Arc::new(empty_struct.clone()) as ArrayRef),
        (Arc::new(Field::new("minValues", ArrowDataType::Struct(empty_struct_fields.clone()), true)), Arc::new(empty_struct.clone()) as ArrayRef),
        (Arc::new(Field::new("maxValues", ArrowDataType::Struct(empty_struct_fields), true)), Arc::new(empty_struct) as ArrayRef),
        (Arc::new(Field::new("tightBounds", ArrowDataType::Boolean, true)), Arc::new(tight_bounds_array) as ArrayRef),
    ]);

    let arrow_schema = TryFromKernel::try_from_kernel(add_schema.as_ref())?;
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(path_array) as ArrayRef,
            partition_values_array,
            Arc::new(size_array) as ArrayRef,
            Arc::new(mod_time_array) as ArrayRef,
            Arc::new(stats_struct) as ArrayRef,
        ],
    )?;

    Ok(Box::new(ArrowEngineData::new(batch)))
}

fn build_partition_values_array(partition_maps: &[HashMap<String, String>]) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let entries_field = Arc::new(Field::new(
        "key_value",
        ArrowDataType::Struct(vec![
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
        ].into()),
        false,
    ));

    let mut all_keys = Vec::new();
    let mut all_values: Vec<Option<String>> = Vec::new();
    let mut offsets = Vec::with_capacity(partition_maps.len() + 1);
    offsets.push(0i32);

    for pmap in partition_maps {
        let mut sorted_keys: Vec<&String> = pmap.keys().collect();
        sorted_keys.sort();
        for key in &sorted_keys {
            all_keys.push(key.as_str().to_string());
            all_values.push(Some(pmap[*key].clone()));
        }
        offsets.push(all_keys.len() as i32);
    }

    let keys_array = StringArray::from(all_keys);
    let values_array = StringArray::from(all_values);
    let entries = StructArray::from(vec![
        (Arc::new(Field::new("key", ArrowDataType::Utf8, false)), Arc::new(keys_array) as ArrayRef),
        (Arc::new(Field::new("value", ArrowDataType::Utf8, true)), Arc::new(values_array) as ArrayRef),
    ]);

    let offset_buffer = OffsetBuffer::new(offsets.into());
    Ok(Arc::new(MapArray::new(entries_field, offset_buffer, entries, None, false)))
}
