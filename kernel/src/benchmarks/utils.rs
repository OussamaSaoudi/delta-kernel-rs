//! Utilities for loading and managing workload specifications.
//!
//! This module provides functions to discover and load workload specifications from
//! a directory structure matching the Java kernel's layout:
//!
//! ```text
//! workload_specs/
//!   ├── table_name/
//!   │   ├── table_info.json
//!   │   ├── delta/
//!   │   │   └── _delta_log/
//!   │   │       ├── 00000000000000000000.json
//!   │   │       └── ...
//!   │   └── specs/
//!   │       ├── case_name_1/
//!   │       │   └── spec.json
//!   │       └── case_name_2/
//!   │           └── spec.json
//! ```

use crate::benchmarks::models::{TableInfo, WorkloadSpecVariant};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Errors that can occur when loading workload specifications
#[derive(Error, Debug)]
pub enum WorkloadLoadError {
    #[error("Workload directory does not exist: {0}")]
    DirectoryNotFound(PathBuf),
    
    #[error("Path is not a directory: {0}")]
    NotADirectory(PathBuf),
    
    #[error("Cannot read directory: {0}")]
    CannotReadDirectory(PathBuf, #[source] std::io::Error),
    
    #[error("No table directories found in {0}")]
    NoTableDirectories(PathBuf),
    
    #[error("Specs directory not found: {0}")]
    SpecsDirectoryNotFound(PathBuf),
    
    #[error("No spec directories found in {0}")]
    NoSpecDirectories(PathBuf),
    
    #[error("Spec file not found: {0}")]
    SpecFileNotFound(PathBuf),
    
    #[error("Failed to parse spec file {0}")]
    ParseError(PathBuf, #[source] serde_json::Error),
    
    #[error("Failed to parse table_info.json {0}")]
    TableInfoParseError(PathBuf, #[source] serde_json::Error),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

const TABLE_INFO_FILE: &str = "table_info.json";
const SPECS_DIR: &str = "specs";
const SPEC_FILE: &str = "spec.json";

/// Load all workload specifications from a directory.
///
/// This function scans the provided directory for table directories, loads their
/// table_info.json files, and discovers all spec.json files in their specs/
/// subdirectories.
///
/// # Arguments
///
/// * `spec_dir_path` - Path to the directory containing workload specifications
///
/// # Returns
///
/// A vector of WorkloadSpecVariant objects, including all variants generated from
/// each base spec (e.g., read specs generate read_metadata variants).
///
/// # Example
///
/// ```rust,ignore
/// let specs = load_all_workloads("kernel/benches/workload_specs")?;
/// for spec in specs {
///     println!("Loaded: {}", spec.full_name());
/// }
/// ```
pub fn load_all_workloads<P: AsRef<Path>>(
    spec_dir_path: P,
) -> Result<Vec<WorkloadSpecVariant>, WorkloadLoadError> {
    let spec_dir = spec_dir_path.as_ref();
    validate_workload_directory(spec_dir)?;
    
    let table_directories = find_table_directories(spec_dir)?;
    
    let mut all_specs = Vec::new();
    for table_dir in table_directories {
        let specs = load_specs_from_table(&table_dir)?;
        all_specs.extend(specs);
    }
    
    Ok(all_specs)
}

/// Validate that a workload directory exists and is accessible
fn validate_workload_directory(path: &Path) -> Result<(), WorkloadLoadError> {
    if !path.exists() {
        return Err(WorkloadLoadError::DirectoryNotFound(path.to_path_buf()));
    }
    
    if !path.is_dir() {
        return Err(WorkloadLoadError::NotADirectory(path.to_path_buf()));
    }
    
    Ok(())
}

/// Find all table directories within the workload specifications directory
fn find_table_directories(spec_dir: &Path) -> Result<Vec<PathBuf>, WorkloadLoadError> {
    let entries = std::fs::read_dir(spec_dir)
        .map_err(|e| WorkloadLoadError::CannotReadDirectory(spec_dir.to_path_buf(), e))?;
    
    let table_dirs: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    
    if table_dirs.is_empty() {
        return Err(WorkloadLoadError::NoTableDirectories(spec_dir.to_path_buf()));
    }
    
    Ok(table_dirs)
}

/// Load all workload specifications from a single table directory
fn load_specs_from_table(table_dir: &Path) -> Result<Vec<WorkloadSpecVariant>, WorkloadLoadError> {
    validate_table_structure(table_dir)?;
    
    let table_info_path = table_dir.join(TABLE_INFO_FILE);
    let table_info = TableInfo::from_json_path(&table_info_path)
        .map_err(|e| WorkloadLoadError::TableInfoParseError(table_info_path.clone(), e))?;
    
    let specs_dir = table_dir.join(SPECS_DIR);
    let spec_directories = find_spec_directories(&specs_dir)?;
    
    let mut all_variants = Vec::new();
    for spec_dir in spec_directories {
        match load_single_spec(&spec_dir, table_info.clone()) {
            Ok(variants) => all_variants.extend(variants),
            Err(WorkloadLoadError::ParseError(path, e)) => {
                // Skip unsupported workload types gracefully
                eprintln!("Warning: Skipping unsupported workload spec {}: {}", path.display(), e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    
    Ok(all_variants)
}

/// Validate that a table directory has the required structure
fn validate_table_structure(table_dir: &Path) -> Result<(), WorkloadLoadError> {
    let specs_dir = table_dir.join(SPECS_DIR);
    
    if !specs_dir.exists() || !specs_dir.is_dir() {
        return Err(WorkloadLoadError::SpecsDirectoryNotFound(specs_dir));
    }
    
    Ok(())
}

/// Find all specification directories within the specs directory
fn find_spec_directories(specs_dir: &Path) -> Result<Vec<PathBuf>, WorkloadLoadError> {
    let entries = std::fs::read_dir(specs_dir)
        .map_err(|e| WorkloadLoadError::CannotReadDirectory(specs_dir.to_path_buf(), e))?;
    
    let spec_dirs: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    
    if spec_dirs.is_empty() {
        return Err(WorkloadLoadError::NoSpecDirectories(specs_dir.to_path_buf()));
    }
    
    Ok(spec_dirs)
}

/// Load a single workload specification and generate all its variants
fn load_single_spec(
    spec_dir: &Path,
    table_info: TableInfo,
) -> Result<Vec<WorkloadSpecVariant>, WorkloadLoadError> {
    let spec_file = spec_dir.join(SPEC_FILE);
    
    if !spec_file.exists() || !spec_file.is_file() {
        return Err(WorkloadLoadError::SpecFileNotFound(spec_file));
    }
    
    let case_name = spec_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| WorkloadLoadError::SpecFileNotFound(spec_dir.to_path_buf()))?
        .to_string();
    
    let mut spec = WorkloadSpecVariant::from_json_path(&spec_file)
        .map_err(|e| WorkloadLoadError::ParseError(spec_file.clone(), e))?;
    
    // Set table info and case name on the loaded spec
    spec.set_table_info(table_info);
    spec.set_case_name(case_name);
    
    // Generate variants from the base spec
    Ok(spec.generate_variants())
}
