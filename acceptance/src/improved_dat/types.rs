//! JSON deserialization types for improved_dat test cases.

use std::collections::HashMap;

use serde::Deserialize;

/// Test case metadata from test_info.json
#[derive(Debug, Deserialize)]
pub struct TestInfo {
    /// Human-readable test name
    pub test_name: String,
    /// Unique test identifier
    pub test_id: String,
    /// Number of workloads in this test
    pub workload_count: u32,
    /// Source file name
    pub source_file: String,
    /// Source code location or "synthetic"
    pub source_lines: String,
    /// Whether test involves schema evolution
    #[serde(default)]
    pub has_schema_evolution: bool,
}

/// Table definition from table_info.json
#[derive(Debug, Deserialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Table description
    pub description: String,
    /// Final version of the table after all operations
    pub version: i64,
    /// List of SQL operations (CREATE, INSERT, ALTER, etc.)
    #[serde(default)]
    pub sql_operations: Vec<String>,
}

/// Expected error specification
#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedError {
    /// Error code (e.g., "DELTA_CDC_NOT_ALLOWED_ON_NON_CDC_TABLE")
    pub error_code: String,
    /// Optional error message pattern
    #[serde(default)]
    pub error_message: Option<String>,
}

/// Expected transaction information
#[derive(Debug, Deserialize)]
pub struct ExpectedTxn {
    /// Application ID
    pub app_id: String,
    /// Transaction version
    pub txn_version: i64,
    /// Last updated timestamp
    pub last_updated: Option<i64>,
}

/// Expected domain metadata
#[derive(Debug, Deserialize)]
pub struct ExpectedDomainMetadata {
    /// Domain name
    pub domain: String,
    /// Configuration JSON string
    pub configuration: String,
    /// Whether the domain is removed
    pub removed: bool,
}

/// Workload specification from specs/*.json
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkloadSpec {
    /// Read workload - execute a scan and validate data
    Read {
        /// Optional predicate filter (e.g., "id = 2")
        #[serde(default)]
        predicate: Option<String>,
        /// Optional version for time travel
        #[serde(default)]
        version: Option<i64>,
        /// Optional timestamp for time travel (format: "YYYY-MM-DD HH:MM:SS.mmm")
        #[serde(default)]
        timestamp: Option<String>,
        /// Optional column projection (e.g., ["id", "name"])
        #[serde(default)]
        columns: Option<Vec<String>>,
        /// Expected error if this should fail
        #[serde(default)]
        error: Option<ExpectedError>,
    },
    /// Snapshot workload - validate metadata at a version
    Snapshot {
        /// Optional version (latest if not specified)
        #[serde(default)]
        version: Option<i64>,
        /// Optional timestamp for time travel
        #[serde(default)]
        timestamp: Option<String>,
        /// Workload name
        #[serde(default)]
        name: Option<String>,
        /// Expected error if this should fail
        #[serde(default)]
        error: Option<ExpectedError>,
    },
    /// Transaction workload - validate SetTransaction
    Txn {
        /// Version at which to check (optional, latest if not specified)
        #[serde(default)]
        version: Option<i64>,
        /// Expected transaction information
        expected: ExpectedTxn,
        /// Workload name
        name: String,
        /// Description
        description: String,
    },
    /// Domain metadata workload
    DomainMetadata {
        /// Optional version
        #[serde(default)]
        version: Option<i64>,
        /// Expected domain metadata
        expected: ExpectedDomainMetadata,
        /// Workload name
        name: String,
        /// Description
        description: String,
    },
}

impl WorkloadSpec {
    /// Check if this workload expects an error
    pub fn expects_error(&self) -> bool {
        match self {
            WorkloadSpec::Read { error, .. } => error.is_some(),
            WorkloadSpec::Snapshot { error, .. } => error.is_some(),
            WorkloadSpec::Txn { .. } => false,
            WorkloadSpec::DomainMetadata { .. } => false,
        }
    }

    /// Get the expected error if any
    pub fn expected_error(&self) -> Option<&ExpectedError> {
        match self {
            WorkloadSpec::Read { error, .. } => error.as_ref(),
            WorkloadSpec::Snapshot { error, .. } => error.as_ref(),
            WorkloadSpec::Txn { .. } => None,
            WorkloadSpec::DomainMetadata { .. } => None,
        }
    }
}

/// Summary of expected read results from summary.json
#[derive(Debug, Deserialize)]
pub struct ExpectedSummary {
    /// Actual row count from running the query
    pub actual_row_count: u64,
    /// Number of files involved
    pub file_count: u32,
    /// Expected number of rows
    pub expected_row_count: u64,
    /// Whether actual matches expected
    pub matches_expected: bool,
}

/// Protocol wrapper from protocol.json
#[derive(Debug, Deserialize)]
pub struct ProtocolWrapper {
    pub protocol: ExpectedProtocol,
}

/// Expected protocol definition
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedProtocol {
    /// Minimum reader version
    pub min_reader_version: i32,
    /// Minimum writer version
    pub min_writer_version: i32,
    /// Optional reader features
    #[serde(default)]
    pub reader_features: Option<Vec<String>>,
    /// Optional writer features
    #[serde(default)]
    pub writer_features: Option<Vec<String>>,
}

/// Metadata wrapper from metadata.json
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataWrapper {
    pub meta_data: ExpectedMetadata,
}

/// Format specification within metadata
#[derive(Debug, Deserialize)]
pub struct MetadataFormat {
    pub provider: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Expected metadata definition
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedMetadata {
    /// Table ID (UUID)
    pub id: String,
    /// Format specification
    pub format: MetadataFormat,
    /// JSON-encoded schema string
    pub schema_string: String,
    /// Partition columns
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Table configuration
    #[serde(default)]
    pub configuration: HashMap<String, String>,
    /// Created time in milliseconds
    #[serde(default)]
    pub created_time: Option<i64>,
}

/// Actual metadata match result from actual_meta.json
#[derive(Debug, Deserialize)]
pub struct ActualMeta {
    pub actual_row_count: u64,
    pub matches_expected: bool,
}
