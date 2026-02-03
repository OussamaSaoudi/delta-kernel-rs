//! Improved DAT (Delta Acceptance Testing) test framework.
//!
//! This module provides support for running the improved_dat test suite,
//! which tests various Delta Lake features including:
//! - Statistics-based data skipping
//! - Checkpoints (V1 and V2)
//! - Row tracking
//! - Domain metadata
//! - Change data capture (CDC)
//! - In-commit timestamps
//! - Type widening
//! - Protocol/metadata evolution

pub mod predicate_parser;
pub mod types;
pub mod validation;
pub mod workload;

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use url::Url;

use crate::{AssertionError, TestResult};
use types::{TableInfo, TestInfo, WorkloadSpec};

/// Information about an improved_dat test case
#[derive(Debug)]
pub struct ImprovedDatTestCase {
    /// Test metadata
    pub test_info: TestInfo,
    /// Table definition
    pub table_info: TableInfo,
    /// Root directory of the test case
    root_dir: PathBuf,
}

impl ImprovedDatTestCase {
    /// Get the root directory of the test case
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Get the URL to the Delta table
    pub fn table_root(&self) -> TestResult<Url> {
        let table_root = self.root_dir.join("delta");
        Url::from_directory_path(table_root).map_err(|_| AssertionError::InvalidTestCase)
    }

    /// Get the expected directory for a specific workload
    pub fn expected_dir(&self, workload_name: &str) -> PathBuf {
        self.root_dir.join("expected").join(workload_name)
    }

    /// Load all workload specifications for this test case
    pub fn load_workloads(&self) -> TestResult<Vec<(String, WorkloadSpec)>> {
        let specs_dir = self.root_dir.join("specs");
        if !specs_dir.exists() {
            return Ok(Vec::new());
        }

        let mut workloads = Vec::new();
        for entry in fs::read_dir(&specs_dir).map_err(|_| AssertionError::InvalidTestCase)? {
            let entry = entry.map_err(|_| AssertionError::InvalidTestCase)?;
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "json") {
                let file = File::open(&path).map_err(|_| AssertionError::InvalidTestCase)?;
                let spec: WorkloadSpec =
                    serde_json::from_reader(file).map_err(|_| AssertionError::InvalidTestCase)?;
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                workloads.push((name, spec));
            }
        }

        // Sort by name for consistent ordering
        workloads.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(workloads)
    }
}

/// Read an improved_dat test case from the given directory
pub fn read_improved_dat_case(case_root: impl AsRef<Path>) -> TestResult<ImprovedDatTestCase> {
    let root_dir = case_root.as_ref().to_path_buf();

    // Read test_info.json
    let test_info_path = root_dir.join("test_info.json");
    let file = File::open(test_info_path).map_err(|_| AssertionError::InvalidTestCase)?;
    let test_info: TestInfo =
        serde_json::from_reader(file).map_err(|_| AssertionError::InvalidTestCase)?;

    // Read table_info.json
    let table_info_path = root_dir.join("table_info.json");
    let file = File::open(table_info_path).map_err(|_| AssertionError::InvalidTestCase)?;
    let table_info: TableInfo =
        serde_json::from_reader(file).map_err(|_| AssertionError::InvalidTestCase)?;

    Ok(ImprovedDatTestCase {
        test_info,
        table_info,
        root_dir,
    })
}
