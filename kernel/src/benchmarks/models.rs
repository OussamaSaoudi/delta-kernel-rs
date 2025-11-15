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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine_info: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_path: Option<String>,
    #[serde(skip)]
    pub(crate) table_info_dir: PathBuf,
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
    
    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(serde_json::Error::io)?;
        let mut table_info: TableInfo = serde_json::from_str(&content)?;
        if let Some(parent) = path.as_ref().parent() {
            table_info.table_info_dir = parent.to_path_buf();
        }
        Ok(table_info)
    }
}

/// Specification for read workloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(skip)]
    pub operation_type: Option<ReadOperationType>,
}

/// Specification for snapshot construction workloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConstructionSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
}

/// Enum representing the type-specific data for a workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WorkloadSpecType {
    #[serde(rename = "read")]
    Read(ReadSpec),
    #[serde(rename = "snapshot_construction")]
    SnapshotConstruction(SnapshotConstructionSpec),
}

/// Complete workload specification with metadata.
#[derive(Debug, Clone)]
pub struct WorkloadSpecVariant {
    pub table_info: TableInfo,
    pub case_name: String,
    pub spec_type: WorkloadSpecType,
}

impl WorkloadSpecVariant {
    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<WorkloadSpecType, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(serde_json::Error::io)?;
        serde_json::from_str(&content)
    }
    
    pub fn full_name(&self) -> String {
        let workload_type = match &self.spec_type {
            WorkloadSpecType::Read(spec) => {
                spec.operation_type.map(|op| op.as_str()).unwrap_or("read")
            }
            WorkloadSpecType::SnapshotConstruction(_) => "snapshot_construction",
        };
        format!("{}/{}/{}", self.table_info.name, self.case_name, workload_type)
    }
}


