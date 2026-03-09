//! Unified types for Delta workload specifications.

use serde::Deserialize;
use std::path::PathBuf;

// ── Table info ──────────────────────────────────────────────────────────────

/// Table metadata loaded from `table_info.json`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    pub name: String,
    pub description: Option<String>,
    #[serde(alias = "table_root_path")]
    pub table_path: Option<String>,
    #[serde(skip, default)]
    pub table_info_dir: PathBuf,
}

impl TableInfo {
    pub fn resolved_table_root(&self) -> String {
        self.table_path.clone().unwrap_or_else(|| {
            self.table_info_dir
                .join("delta")
                .to_string_lossy()
                .to_string()
        })
    }
}

// ── Workload specification ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkloadSpec {
    Read {
        #[serde(default)]
        version: Option<i64>,
        #[serde(default)]
        predicate: Option<String>,
        #[serde(default)]
        timestamp: Option<String>,
        #[serde(default)]
        columns: Option<Vec<String>>,
        #[serde(default)]
        error: Option<ExpectedError>,
        #[serde(default)]
        expected: Option<ReadExpected>,
    },
    #[serde(alias = "snapshot_construction")]
    Snapshot {
        #[serde(default)]
        version: Option<i64>,
        #[serde(default)]
        timestamp: Option<String>,
        #[serde(default)]
        error: Option<ExpectedError>,
        #[serde(default)]
        expected: Option<SnapshotExpected>,
    },
    /// Transaction workload — validate SetTransaction
    Txn {
        #[serde(default)]
        version: Option<i64>,
        expected: ExpectedTxn,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        description: Option<String>,
    },
    /// Domain metadata workload
    DomainMetadata {
        #[serde(default)]
        version: Option<i64>,
        expected: ExpectedDomainMetadata,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        description: Option<String>,
    },
    /// CDF (Change Data Feed) workload — read table changes
    Cdf {
        #[serde(alias = "startVersion", alias = "start_version", default)]
        start_version: Option<i64>,
        #[serde(alias = "endVersion", alias = "end_version", default)]
        end_version: Option<i64>,
        #[serde(default)]
        predicate: Option<String>,
        #[serde(default)]
        error: Option<ExpectedError>,
    },
    /// Catch-all for workload types not yet supported in this build.
    #[serde(other)]
    Unsupported,
}

impl WorkloadSpec {
    pub fn expected_error(&self) -> Option<&ExpectedError> {
        match self {
            Self::Read { error, .. } | Self::Snapshot { error, .. } | Self::Cdf { error, .. } => {
                error.as_ref()
            }
            Self::Txn { .. } | Self::DomainMetadata { .. } | Self::Unsupported => None,
        }
    }
}

// ── Expected-value types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedError {
    pub error_code: String,
    #[serde(default)]
    pub error_message: Option<String>,
}

/// Expected transaction information
#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedTxn {
    #[serde(alias = "appId")]
    pub app_id: String,
    #[serde(alias = "txnVersion")]
    pub txn_version: i64,
    #[serde(alias = "lastUpdated", default)]
    pub last_updated: Option<i64>,
}

/// Expected domain metadata
#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedDomainMetadata {
    pub domain: String,
    pub configuration: String,
    pub removed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedSummary {
    pub actual_row_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadExpected {
    pub row_count: u64,
}

/// Expected snapshot values. Uses `serde_json::Value` for protocol/metadata
/// to allow flexible comparison (set-based feature ordering, optional schema, etc.)
/// without duplicating kernel's Protocol/Metadata structs.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotExpected {
    #[serde(default)]
    pub protocol: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}
