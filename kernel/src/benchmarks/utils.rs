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
) -> Result<Vec<WorkloadSpecVariant>, String> {
    let spec_dir = spec_dir_path.as_ref();
    
    // Validate that the workload directory exists and is a directory
    if !spec_dir.is_dir() {
        return Err(format!("Path does not exist or is not a directory: {}", spec_dir.display()));
    }
    
    let table_directories = find_table_directories(spec_dir)?;
    
    let mut all_specs = Vec::new();
    for table_dir in table_directories {
        let specs = load_specs_from_table(&table_dir)?;
        all_specs.extend(specs);
    }
    
    Ok(all_specs)
}

/// Find all table directories within the workload specifications directory
fn find_table_directories(spec_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = std::fs::read_dir(spec_dir)
        .map_err(|e| format!("Cannot read directory {}: {}", spec_dir.display(), e))?;
    
    let table_dirs: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    
    if table_dirs.is_empty() {
        return Err(format!("No table directories found in {}", spec_dir.display()));
    }
    
    Ok(table_dirs)
}

/// Load all workload specifications from a single table directory
fn load_specs_from_table(table_dir: &Path) -> Result<Vec<WorkloadSpecVariant>, String> {
    let specs_dir = table_dir.join(SPECS_DIR);
    
    // Validate that the specs directory exists
    if !specs_dir.exists() || !specs_dir.is_dir() {
        return Err(format!("Specs directory not found: {}", specs_dir.display()));
    }
    
    let table_info_path = table_dir.join(TABLE_INFO_FILE);
    let table_info = TableInfo::from_json_path(&table_info_path)
        .map_err(|e| format!("Failed to parse table_info.json {}: {}", table_info_path.display(), e))?;
    
    let spec_directories = find_spec_directories(&specs_dir)?;
    
    let mut all_specs = Vec::new();
    for spec_dir in spec_directories {
        match load_single_spec(&spec_dir, table_info.clone()) {
            Ok(spec) => all_specs.push(spec),
            Err(e) if e.contains("unknown variant") => {
                // Skip unsupported workload types gracefully
                eprintln!("Warning: Skipping unsupported workload spec: {}", e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    
    Ok(all_specs)
}

/// Find all specification directories within the specs directory
fn find_spec_directories(specs_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = std::fs::read_dir(specs_dir)
        .map_err(|e| format!("Cannot read directory {}: {}", specs_dir.display(), e))?;
    
    let spec_dirs: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    
    if spec_dirs.is_empty() {
        return Err(format!("No spec directories found in {}", specs_dir.display()));
    }
    
    Ok(spec_dirs)
}

/// Load a single workload specification
fn load_single_spec(
    spec_dir: &Path,
    table_info: TableInfo,
) -> Result<WorkloadSpecVariant, String> {
    let spec_file = spec_dir.join(SPEC_FILE);
    
    if !spec_file.exists() || !spec_file.is_file() {
        return Err(format!("Spec file not found: {}", spec_file.display()));
    }
    
    let case_name = spec_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("Invalid spec directory name: {}", spec_dir.display()))?
        .to_string();
    
    let spec_type = WorkloadSpecVariant::from_json_path(&spec_file)
        .map_err(|e| format!("Failed to parse spec file {}: {}", spec_file.display(), e))?;
    
    Ok(WorkloadSpecVariant {
        table_info,
        case_name,
        spec_type,
        operation_type: None,
    })
}
