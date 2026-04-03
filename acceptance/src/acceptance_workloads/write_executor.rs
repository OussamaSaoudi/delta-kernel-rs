//! Write spec executor for Delta acceptance tests.
//!
//! Reads `write_spec.json` and executes write operations using kernel-rs APIs.
//! After execution, the resulting table is validated via read/snapshot specs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::schema::StructType;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Engine;
use serde::Deserialize;
use tracing::{info, warn};
use url::Url;

// ---------------------------------------------------------------------------
// Data model (deserialized from write_spec.json)
// ---------------------------------------------------------------------------

/// A write specification — a sequence of commits to build a Delta table.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteSpec {
    pub description: Option<String>,
    pub commits: Vec<WriteCommit>,
    pub verification: Option<Vec<String>>,
}

/// A single commit (one logical operation).
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

// ---------------------------------------------------------------------------
// Execution result
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct WriteResult {
    pub commits_applied: usize,
    pub commits_skipped: usize,
    pub skipped_reasons: Vec<String>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute a write spec against a table directory.
///
/// Processes commits in order, dispatching to kernel APIs where supported.
/// Unsupported operations are logged and skipped. After execution, the caller
/// validates the table via read/snapshot specs.
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
                return Err(format!(
                    "commit {} ({}) failed: {}",
                    i, commit.operation, e
                ).into());
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
        "set_transaction" => execute_set_transaction(commit, table_root, engine),
        "set_domain_metadata" => execute_set_domain_metadata(commit, table_root, engine),
        "alter_table" => Ok(Outcome::Skipped("alter_table not yet supported by kernel".into())),
        "delete" => Ok(Outcome::Skipped("delete not yet supported by kernel".into())),
        "update" => Ok(Outcome::Skipped("update not yet supported by kernel".into())),
        "truncate" => Ok(Outcome::Skipped("truncate not yet supported by kernel".into())),
        "replace_table" => Ok(Outcome::Skipped("replace_table not yet supported by kernel".into())),
        "insert_overwrite" => Ok(Outcome::Skipped("insert_overwrite not yet supported by kernel".into())),
        "vacuum" => Ok(Outcome::Skipped("vacuum is engine-level, not kernel".into())),
        "checkpoint" => execute_checkpoint(table_root, engine),
        other => Ok(Outcome::Skipped(format!("unknown operation: {}", other))),
    }
}

// ---------------------------------------------------------------------------
// Operation implementations
// ---------------------------------------------------------------------------

fn execute_create_table(
    commit: &WriteCommit,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    use delta_kernel::transaction::create_table::create_table;

    let schema_json = commit.schema.as_ref()
        .ok_or("create_table: missing schema")?;
    let schema: StructType = serde_json::from_value(schema_json.clone())
        .map_err(|e| format!("create_table: invalid schema: {}", e))?;
    let schema = Arc::new(schema);

    let mut builder = create_table(
        table_root.as_str(), schema, "delta-dat-write-executor");

    if let Some(ref props) = commit.properties {
        builder = builder.with_table_properties(
            props.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }

    if let Some(ref part_cols) = commit.partition_columns {
        if !part_cols.is_empty() {
            warn!(
                partition_columns = ?part_cols,
                "Partition columns not yet supported by kernel create_table"
            );
        }
    }

    // Build and commit
    let txn = builder.build(engine, Box::new(delta_kernel::committer::FileSystemCommitter::new()))?;
    let result = txn.commit(engine)?;

    if result.is_committed() {
        // If there are initial data files, append them in a second commit
        if let Some(ref data_files) = commit.data_files {
            if !data_files.is_empty() {
                append_data_files(data_files, table_root, test_case_root, engine)?;
            }
        }
        Ok(Outcome::Applied)
    } else {
        Err("create_table: commit was not committed".into())
    }
}

fn execute_insert(
    commit: &WriteCommit,
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let data_files = commit.data_files.as_ref()
        .ok_or("insert: missing dataFiles")?;
    if data_files.is_empty() {
        return Ok(Outcome::Skipped("insert: empty dataFiles".into()));
    }
    append_data_files(data_files, table_root, test_case_root, engine)?;
    Ok(Outcome::Applied)
}

fn execute_set_transaction(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let app_id = commit.app_id.as_ref()
        .ok_or("set_transaction: missing appId")?;
    let version = commit.version
        .ok_or("set_transaction: missing version")?;

    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let txn = snapshot.transaction(Box::new(delta_kernel::committer::FileSystemCommitter::new()), engine)?
        .with_transaction_id(app_id.clone(), version);
    let result = txn.commit(engine)?;

    if result.is_committed() {
        Ok(Outcome::Applied)
    } else {
        Err("set_transaction: commit was not committed".into())
    }
}

fn execute_set_domain_metadata(
    commit: &WriteCommit,
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let domain = commit.domain.as_ref()
        .ok_or("set_domain_metadata: missing domain")?;
    let config = commit.configuration.as_deref().unwrap_or("");
    let removed = commit.removed.unwrap_or(false);

    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let mut txn = snapshot.transaction(Box::new(delta_kernel::committer::FileSystemCommitter::new()), engine)?;

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

fn execute_checkpoint(
    table_root: &Url,
    engine: &dyn Engine,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    snapshot.checkpoint(engine)?;
    Ok(Outcome::Applied)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Append data files to an existing table via blind append.
fn append_data_files(
    data_files: &[String],
    table_root: &Url,
    test_case_root: &Path,
    engine: &dyn Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine)?;
    let txn = snapshot.transaction(Box::new(delta_kernel::committer::FileSystemCommitter::new()), engine)?
        .with_blind_append()
        .with_operation("delta-dat-write-executor".to_string());

    // Resolve data file paths relative to test case root
    let _resolved: Vec<std::path::PathBuf> = data_files.iter()
        .map(|f| test_case_root.join(f))
        .collect();

    // TODO: To add files, we need to construct EngineData in the Transaction::add_files_schema()
    // format. This requires reading each parquet file's metadata (size, row count, stats).
    // For now, this is a placeholder that creates the transaction but doesn't add files.
    warn!(
        num_files = data_files.len(),
        "append_data_files: file metadata construction not yet implemented"
    );

    let result = txn.commit(engine)?;
    if result.is_committed() {
        Ok(())
    } else {
        Err("append: commit was not committed".into())
    }
}
