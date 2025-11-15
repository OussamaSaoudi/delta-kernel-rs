//! Data models for workload specifications.
//!
//! These structures define the JSON schema for workload specifications that can be loaded
//! and executed as benchmarks. The models support polymorphic deserialization to handle
//! different workload types (read, snapshot_construction, etc.).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Operation type for read workloads
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOperationType {
    /// Read scan metadata (file listing)
    ReadMetadata,
    /// Read actual data files (not yet implemented)
    ReadData,
}

impl ReadOperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadOperationType::ReadMetadata => "read_metadata",
            ReadOperationType::ReadData => "read_data",
        }
    }
}

/// Information about a Delta table used in benchmarks.
///
/// This corresponds to the `table_info.json` file in each workload directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Name of the table (used for identifying the table in results)
    pub name: String,
    
    /// Human-readable description of the table
    pub description: String,
    
    /// Optional engine information (e.g., "Apache-Spark/3.5.1 Delta-Lake/3.1.0")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine_info: Option<String>,
    
    /// Optional table path (for remote tables like S3)
    /// If not provided, assumes table is in the delta/ subdirectory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_path: Option<String>,
    
    /// Internal: path to the directory containing table_info.json
    /// This is set during loading and not part of the JSON schema
    #[serde(skip)]
    pub(crate) table_info_dir: PathBuf,
}

impl TableInfo {
    /// Get the resolved table root path.
    /// 
    /// If `table_path` is set, uses that directly.
    /// Otherwise, resolves to `{table_info_dir}/delta/`
    pub fn resolved_table_root(&self) -> String {
        if let Some(ref path) = self.table_path {
            path.clone()
        } else {
            self.table_info_dir
                .join("delta")
                .to_string_lossy()
                .to_string()
        }
    }
    
    /// Load TableInfo from a JSON file path
    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(serde_json::Error::io)?;
        let mut table_info: TableInfo = serde_json::from_str(&content)?;
        
        // Set the table_info_dir to the parent directory of the JSON file
        if let Some(parent) = path.as_ref().parent() {
            table_info.table_info_dir = parent.to_path_buf();
        }
        
        Ok(table_info)
    }
}

/// Base trait for all workload specifications.
///
/// Workload specifications define the parameters for a benchmark run, such as which
/// table to read, which version to use, etc. Each workload type (read, snapshot_construction)
/// has its own struct implementing this trait.
pub trait WorkloadSpec {
    /// Get the type of this workload (e.g., "read", "snapshot_construction")
    fn workload_type(&self) -> &str;
    
    /// Get the table information for this workload
    fn table_info(&self) -> &TableInfo;
    
    /// Get the case name for this workload
    fn case_name(&self) -> &str;
    
    /// Get the full name for reporting (combines table name, case name, and workload type)
    fn full_name(&self) -> String {
        format!(
            "{}/{}/{}",
            self.table_info().name,
            self.case_name(),
            self.workload_type()
        )
    }
}

/// Specification for read workloads.
///
/// Read workloads can be either:
/// - `read_metadata`: Read scan metadata (file listing)
/// - `read_data`: Read actual data files (not yet implemented)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadSpec {
    /// Optional snapshot version to read (if None, reads latest)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    
    /// Operation type: ReadMetadata or ReadData
    /// This is set during variant generation, not in the spec file
    #[serde(skip)]
    pub operation_type: Option<ReadOperationType>,
    
    /// Table information (set during loading)
    #[serde(skip)]
    pub table_info: Option<TableInfo>,
    
    /// Case name (set during loading)
    #[serde(skip)]
    pub case_name: Option<String>,
}

impl ReadSpec {
    /// Create a new ReadSpec variant with the specified operation type
    pub fn with_operation(&self, operation_type: ReadOperationType) -> Self {
        ReadSpec {
            version: self.version,
            operation_type: Some(operation_type),
            table_info: self.table_info.clone(),
            case_name: self.case_name.clone(),
        }
    }
    
    /// Generate workload variants from this spec.
    /// 
    /// A single ReadSpec can generate multiple variants (read_metadata, read_data, etc.)
    pub fn variants(&self) -> Vec<ReadSpec> {
        // For now, only support read_metadata
        vec![self.with_operation(ReadOperationType::ReadMetadata)]
    }
}

impl WorkloadSpec for ReadSpec {
    fn workload_type(&self) -> &str {
        self.operation_type
            .map(|op| op.as_str())
            .unwrap_or("read")
    }
    
    fn table_info(&self) -> &TableInfo {
        self.table_info.as_ref().expect("table_info not set")
    }
    
    fn case_name(&self) -> &str {
        self.case_name.as_ref().expect("case_name not set")
    }
}

/// Specification for snapshot construction workloads.
///
/// These workloads measure the time to construct a snapshot at a specific version
/// or the latest version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConstructionSpec {
    /// Optional snapshot version to construct (if None, constructs latest)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    
    /// Table information (set during loading)
    #[serde(skip)]
    pub table_info: Option<TableInfo>,
    
    /// Case name (set during loading)
    #[serde(skip)]
    pub case_name: Option<String>,
}

impl WorkloadSpec for SnapshotConstructionSpec {
    fn workload_type(&self) -> &str {
        "snapshot_construction"
    }
    
    fn table_info(&self) -> &TableInfo {
        self.table_info.as_ref().expect("table_info not set")
    }
    
    fn case_name(&self) -> &str {
        self.case_name.as_ref().expect("case_name not set")
    }
}

/// Enum representing any workload specification type.
///
/// This uses serde's tagged enum feature to deserialize JSON based on the "type" field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WorkloadSpecVariant {
    #[serde(rename = "read")]
    Read(ReadSpec),
    
    #[serde(rename = "snapshot_construction")]
    SnapshotConstruction(SnapshotConstructionSpec),
}

impl WorkloadSpecVariant {
    /// Load a WorkloadSpecVariant from a JSON file
    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(serde_json::Error::io)?;
        serde_json::from_str(&content)
    }
    
    /// Set the table info for this workload spec
    pub fn set_table_info(&mut self, table_info: TableInfo) {
        match self {
            WorkloadSpecVariant::Read(spec) => {
                spec.table_info = Some(table_info);
            }
            WorkloadSpecVariant::SnapshotConstruction(spec) => {
                spec.table_info = Some(table_info);
            }
        }
    }
    
    /// Set the case name for this workload spec
    pub fn set_case_name(&mut self, case_name: String) {
        match self {
            WorkloadSpecVariant::Read(spec) => {
                spec.case_name = Some(case_name);
            }
            WorkloadSpecVariant::SnapshotConstruction(spec) => {
                spec.case_name = Some(case_name);
            }
        }
    }
    
    /// Get the workload type string
    pub fn workload_type(&self) -> &str {
        match self {
            WorkloadSpecVariant::Read(spec) => spec.workload_type(),
            WorkloadSpecVariant::SnapshotConstruction(spec) => spec.workload_type(),
        }
    }
    
    /// Get the full name for this workload
    pub fn full_name(&self) -> String {
        match self {
            WorkloadSpecVariant::Read(spec) => spec.full_name(),
            WorkloadSpecVariant::SnapshotConstruction(spec) => spec.full_name(),
        }
    }
    
    /// Generate all variants from this workload spec.
    ///
    /// Some workload types (like Read) can generate multiple variants from a single spec.
    /// For example, a Read spec generates separate read_metadata and read_data variants.
    pub fn generate_variants(self) -> Vec<WorkloadSpecVariant> {
        match self {
            WorkloadSpecVariant::Read(spec) => {
                spec.variants()
                    .into_iter()
                    .map(WorkloadSpecVariant::Read)
                    .collect()
            }
            WorkloadSpecVariant::SnapshotConstruction(spec) => {
                // Snapshot construction doesn't generate variants, return as-is
                vec![WorkloadSpecVariant::SnapshotConstruction(spec)]
            }
        }
    }
}

