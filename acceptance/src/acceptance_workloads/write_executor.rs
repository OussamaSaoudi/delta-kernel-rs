//! Write spec executor for Delta acceptance tests.
//!
//! Reads `write_spec.json` and executes write operations using kernel-rs APIs.
//! After execution, the resulting table is validated via read/snapshot specs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Int64Array, MapArray, StringArray, StructArray,
};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{SchemaRef, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Engine;
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
                info!(
                    commit = i,
                    op = commit.operation.as_str(),
                    reason = reason.as_str(),
                    "Skipped"
                );
                skipped_reasons
                    .push(format!("commit {}: {} — {}", i, commit.operation, reason));
                skipped += 1;
            }
            Err(e) => {
                return Err(
                    format!("commit {} ({}) failed: {}", i, commit.operation, e).into(),
                );
            }
        }
    }

    Ok(WriteResult {
        commits_applied: applied,
        commits_skipped: skipped,
        skipped_reasons,
    })
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
        "insert_overwrite" => Ok(Outcome::Skipped(
            "insert_overwrite not yet supported".into(),
        )),
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
        builder = builder
            .with_table_properties(props.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }

    if let Some(ref part_cols) = commit.partition_columns {
        if !part_cols.is_empty() {
            warn!(
                partition_columns = ?part_cols,
                "Partition columns not yet supported by kernel create_table"
            );
        }
    }

    let txn = builder.build(engine, Box::new(FileSystemCommitter::new()))?;
    let result = txn.commit(engine)?;

    if result.is_committed() {
        if let Some(ref data_files) = commit.data_files {
            if !data_files.is_empty() {
                append_data_files(
                    data_files,
                    &commit.partition_columns,
                    table_root,
                    test_case_root,
                    engine,
                )?;
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
    let data_files = commit
        .data_files
        .as_ref()
        .ok_or("insert: missing dataFiles")?;
    if data_files.is_empty() {
        return Ok(Outcome::Skipped("insert: empty dataFiles".into()));
    }
    append_data_files(data_files, &None, table_root, test_case_root, engine)?;
    Ok(Outcome::Applied)
}

// ---------------------------------------------------------------------------
// delete (via DVs when enabled)
// ---------------------------------------------------------------------------

fn execute_delete(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let _predicate = commit
        .predicate
        .as_ref()
        .ok_or("delete: missing predicate")?;

    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let dvs_enabled = snapshot
        .table_configuration()
        .is_feature_supported(&delta_kernel::table_features::TableFeature::DeletionVectors);

    if !dvs_enabled {
        return Ok(Outcome::Skipped(
            "delete: DVs not enabled, file-level delete not yet implemented".into(),
        ));
    }

    // TODO: implement scan → predicate eval → DV write pipeline
    // This requires:
    // 1. Scan the table to get files + row data
    // 2. Evaluate the predicate against each row to get matching row indexes per file
    // 3. Build KernelDeletionVector with those indexes
    // 4. Write DV files via StreamingDeletionVectorWriter
    // 5. Call txn.update_deletion_vectors() (internal API)
    Ok(Outcome::Skipped(
        "delete with DVs: predicate eval + DV write pipeline not yet implemented".into(),
    ))
}

// ---------------------------------------------------------------------------
// set_transaction
// ---------------------------------------------------------------------------

fn execute_set_transaction(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let app_id = commit
        .app_id
        .as_ref()
        .ok_or("set_transaction: missing appId")?;
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
    let domain = commit
        .domain
        .as_ref()
        .ok_or("set_domain_metadata: missing domain")?;
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
// Helpers: append data files with proper metadata
// ---------------------------------------------------------------------------

/// Append data files to an existing table via blind append.
///
/// For each file: reads parquet footer (size, row count), extracts partition
/// values from the file path, copies file to table dir, constructs AddFile
/// metadata, and commits.
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

    let table_dir = table_root
        .to_file_path()
        .map_err(|_| "Cannot convert table URL to file path")?;

    for data_file in data_files {
        let resolved = test_case_root.join(data_file);
        if !resolved.exists() {
            return Err(format!("Data file not found: {}", resolved.display()).into());
        }

        // Read parquet footer for real row count
        let file = std::fs::File::open(&resolved)?;
        let reader = SerializedFileReader::new(file)?;
        let num_rows = reader.metadata().file_metadata().num_rows();

        let file_meta = std::fs::metadata(&resolved)?;
        let file_size = file_meta.len() as i64;
        let mod_time = file_meta
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;

        let file_name = resolved
            .file_name()
            .ok_or("no filename")?
            .to_string_lossy()
            .to_string();

        // Extract partition values from the data file path
        let part_values = extract_partition_values(data_file, partition_columns);

        // Build target path and copy file
        let target_relative = if !part_values.is_empty() {
            let part_dir: String = part_values
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("/");
            let target_dir = table_dir.join(&part_dir);
            std::fs::create_dir_all(&target_dir)?;
            let target = target_dir.join(&file_name);
            std::fs::copy(&resolved, &target)?;
            format!("{}/{}", part_dir, file_name)
        } else {
            let target = table_dir.join(&file_name);
            std::fs::copy(&resolved, &target)?;
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

    let add_metadata = build_add_files_metadata(
        &add_schema,
        &paths,
        &sizes,
        &mod_times,
        &row_counts,
        &partition_maps,
    )?;
    txn.add_files(add_metadata);

    let result = txn.commit(engine)?;
    if result.is_committed() {
        Ok(())
    } else {
        Err("append: commit was not committed".into())
    }
}

/// Extract partition values from a data file path.
///
/// e.g., "data/commit_1/region=us/part-00000.parquet" with partition_columns=["region"]
/// → {"region": "us"}
fn extract_partition_values(
    data_file: &str,
    partition_columns: &Option<Vec<String>>,
) -> HashMap<String, String> {
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

/// Build EngineData for the Transaction::add_files() call.
///
/// Constructs a RecordBatch matching the add_files_schema with:
/// - path, partitionValues, size, modificationTime
/// - stats (numRecords from actual parquet footer, empty min/max/nullCount)
fn build_add_files_metadata(
    add_schema: &SchemaRef,
    paths: &[String],
    sizes: &[i64],
    mod_times: &[i64],
    row_counts: &[i64],
    partition_maps: &[HashMap<String, String>],
) -> Result<Box<dyn delta_kernel::EngineData>, Box<dyn std::error::Error>> {
    let num_files = paths.len();

    let path_array = StringArray::from(paths.to_vec());
    let size_array = Int64Array::from(sizes.to_vec());
    let mod_time_array = Int64Array::from(mod_times.to_vec());
    let num_records_array = Int64Array::from(row_counts.to_vec());

    let partition_values_array = build_partition_values_array(partition_maps)?;

    // Stats: numRecords from real parquet metadata, empty column-level stats
    let empty_struct_fields: delta_kernel::arrow::datatypes::Fields =
        Vec::<Arc<Field>>::new().into();
    let empty_struct = StructArray::new_empty_fields(num_files, None);
    let tight_bounds_array = BooleanArray::from(vec![true; num_files]);

    let stats_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(num_records_array) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "nullCount",
                ArrowDataType::Struct(empty_struct_fields.clone()),
                true,
            )),
            Arc::new(empty_struct.clone()) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "minValues",
                ArrowDataType::Struct(empty_struct_fields.clone()),
                true,
            )),
            Arc::new(empty_struct.clone()) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "maxValues",
                ArrowDataType::Struct(empty_struct_fields),
                true,
            )),
            Arc::new(empty_struct) as ArrayRef,
        ),
        (
            Arc::new(Field::new("tightBounds", ArrowDataType::Boolean, true)),
            Arc::new(tight_bounds_array) as ArrayRef,
        ),
    ]);

    // Convert kernel schema to arrow schema
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

/// Build a MapArray for partition values.
fn build_partition_values_array(
    partition_maps: &[HashMap<String, String>],
) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let entries_field = Arc::new(Field::new(
        "key_value",
        ArrowDataType::Struct(
            vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            ]
            .into(),
        ),
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
        (
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(keys_array) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            Arc::new(values_array) as ArrayRef,
        ),
    ]);

    let offset_buffer = OffsetBuffer::new(offsets.into());
    Ok(Arc::new(MapArray::new(
        entries_field,
        offset_buffer,
        entries,
        None,
        false,
    )))
}
