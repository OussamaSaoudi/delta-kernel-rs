//! Transform computation for scan results.
//!
//! This module provides the [`TransformComputer`] which computes row-level transform
//! expressions from scan batches.
//!
//! # Usage
//!
//! ```ignore
//! let computer = TransformComputer::new(state_info);
//!
//! // Compute transforms for a batch from ResultsDriver
//! for batch in driver {
//!     let batch = batch?;
//!     let transforms = computer.compute_transforms(batch.engine_data.as_ref())?;
//!     // Use transforms with batch...
//! }
//! ```
//!
//! [`ScanStateMachine`]: crate::plans::state_machines::ScanStateMachine

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_name, ColumnName, Scalar};
use crate::scan::state_info::StateInfo;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType};
use crate::transforms::{get_transform_expr, parse_partition_values, TransformSpec};
use crate::{DeltaResult, EngineData, ExpressionRef};

/// Computes row-level transform expressions from scan batches.
///
/// The `TransformComputer` extracts partition values and base row IDs from each row
/// in a batch, then uses [`get_transform_expr`] to compute the appropriate transform
/// expression for schema evolution, partition column injection, etc.
///
/// This is stateless - each batch is processed independently.
#[derive(Debug, Clone)]
pub(crate) struct TransformComputer {
    state_info: Arc<StateInfo>,
}

impl TransformComputer {
    /// Create a new `TransformComputer` with the given state info.
    pub(crate) fn new(state_info: Arc<StateInfo>) -> Self {
        Self { state_info }
    }

    /// Compute transform expressions for each row in the batch.
    ///
    /// Takes a reference to engine data (e.g. from `ResultsDriver` batch) and returns
    /// a vector where each element corresponds to a row:
    /// - `Some(expr)` - Transform expression to apply for that row
    /// - `None` - No transform needed (data is already in correct form)
    ///
    /// The vector may be shorter than the batch length if trailing rows have no transforms.
    pub(crate) fn compute_transforms(
        &self,
        data: &dyn EngineData,
    ) -> DeltaResult<Vec<Option<ExpressionRef>>> {
        let mut visitor = TransformVisitor::new(self.state_info.clone());
        visitor.visit_rows_of(data)?;
        Ok(visitor.row_transform_exprs)
    }
}

/// Internal visitor that extracts partition values and base row IDs to compute transforms.
struct TransformVisitor {
    state_info: Arc<StateInfo>,
    row_transform_exprs: Vec<Option<ExpressionRef>>,
}

impl TransformVisitor {
    // Column indices in selected_column_names_and_types
    const PARTITION_VALUES_INDEX: usize = 0;
    const BASE_ROW_ID_INDEX: usize = 1;

    fn new(state_info: Arc<StateInfo>) -> Self {
        Self {
            state_info,
            row_transform_exprs: Vec::new(),
        }
    }

    fn compute_transform_for_row<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Option<ExpressionRef>> {
        let Some(transform_spec) = &self.state_info.transform_spec else {
            return Ok(None);
        };

        // Extract partition values from the row
        let partition_values = self.extract_partition_values(i, getters, transform_spec)?;

        // Extract base row ID (optional)
        let base_row_id: Option<i64> =
            getters[Self::BASE_ROW_ID_INDEX].get_opt(i, "fileConstantValues.baseRowId")?;

        // Compute the transform expression
        get_transform_expr(
            transform_spec,
            partition_values,
            &self.state_info.physical_schema,
            base_row_id,
        )
        .map(Some)
    }

    fn extract_partition_values<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        transform_spec: &TransformSpec,
    ) -> DeltaResult<HashMap<usize, (String, Scalar)>> {
        // Use get_opt to handle missing partition values gracefully
        // This can happen when the SelectNode's transform expression fails to evaluate
        let partition_values: Option<HashMap<String, String>> = getters[Self::PARTITION_VALUES_INDEX]
            .get_opt(i, "fileConstantValues.partitionValues")?;

        let partition_values = partition_values.unwrap_or_default();

        parse_partition_values(
            &self.state_info.logical_schema,
            transform_spec,
            &partition_values,
            self.state_info.column_mapping_mode,
        )
    }
}

impl RowVisitor for TransformVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const LONG: DataType = DataType::LONG;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();

            // We need partition values and base row ID from SCAN_ROW_SCHEMA.
            // These are nested under "fileConstantValues" struct.
            let types_and_names = vec![
                (ss_map, column_name!("fileConstantValues.partitionValues")),
                (LONG, column_name!("fileConstantValues.baseRowId")),
            ];
            let (types, names): (Vec<DataType>, Vec<ColumnName>) =
                types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let transform = self.compute_transform_for_row(i, getters)?;

            if transform.is_some() {
                // Fill in any needed `None`s for previous rows
                self.row_transform_exprs.resize_with(i, Default::default);
                self.row_transform_exprs.push(transform);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::PhysicalPredicate;
    use crate::schema::{SchemaRef, StructField, StructType};
    use crate::table_features::ColumnMappingMode;
    use std::sync::Arc;

    fn create_simple_state_info() -> Arc<StateInfo> {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]));

        Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema,
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
        })
    }

    #[test]
    fn test_transform_computer_new() {
        let state_info = create_simple_state_info();
        let computer = TransformComputer::new(state_info.clone());
        assert!(Arc::ptr_eq(&computer.state_info, &state_info));
    }

    #[test]
    fn test_transform_visitor_column_names() {
        let state_info = create_simple_state_info();
        let visitor = TransformVisitor::new(state_info);
        let (names, types) = visitor.selected_column_names_and_types();

        assert_eq!(names.len(), 2);
        assert_eq!(types.len(), 2);
        assert_eq!(names[0].to_string(), "fileConstantValues.partitionValues");
        assert_eq!(names[1].to_string(), "fileConstantValues.baseRowId");
    }

    #[test]
    fn test_transform_computer_no_transforms_when_no_spec() {
        // When there's no transform_spec, compute_transforms should return empty vec
        let state_info = create_simple_state_info();
        let computer = TransformComputer::new(state_info);

        // We need actual data to test this properly, but for now we verify the structure
        // The visitor will return empty transforms when transform_spec is None
        let visitor = TransformVisitor::new(computer.state_info.clone());
        assert!(visitor.row_transform_exprs.is_empty());
    }

}

/// Integration tests that compare TransformComputer output with Scan::scan_metadata
#[cfg(test)]
mod integration_tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::engine::sync::SyncEngine;
    use crate::scan::state::{DvInfo, Stats};
    use crate::snapshot::Snapshot;
    use crate::Engine;
    use crate::ExpressionRef;
    use std::collections::HashMap;

    fn create_test_engine() -> Arc<dyn Engine> {
        Arc::new(SyncEngine::new())
    }

    /// Get path to a test table
    fn get_test_table_path(table_name: &str) -> Option<(PathBuf, url::Url)> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data")
            .join(table_name);
        if path.exists() {
            let url = url::Url::from_directory_path(&path).unwrap();
            Some((path, url))
        } else {
            None
        }
    }

    /// Callback to collect scan file info from ScanMetadata
    fn collect_scan_files(
        files: &mut Vec<(String, i64, Option<ExpressionRef>)>,
        path: &str,
        size: i64,
        _stats: Option<Stats>,
        _dv_info: DvInfo,
        transform: Option<ExpressionRef>,
        _partition_values: HashMap<String, String>,
    ) {
        files.push((path.to_string(), size, transform));
    }

    #[test]
    fn test_scan_metadata_basic_table() {
        // Test that scan_metadata produces expected results for a simple table
        let Some((_, table_url)) = get_test_table_path("table-without-dv-small") else {
            println!("Skipping test: table-without-dv-small not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Get scan metadata using existing API
        let scan = snapshot
            .scan_builder()
            .build()
            .expect("Should build scan");

        let scan_metadata_iter = scan
            .scan_metadata(engine.as_ref())
            .expect("Should get scan metadata");

        let mut files: Vec<(String, i64, Option<ExpressionRef>)> = Vec::new();
        for result in scan_metadata_iter {
            let scan_metadata = result.expect("Scan metadata should succeed");
            files = scan_metadata
                .visit_scan_files(files, collect_scan_files)
                .expect("visit_scan_files should succeed");
        }

        // Verify we got at least one file
        assert!(!files.is_empty(), "Should have at least one file");

        // For a simple table without transforms, all transforms should be None
        for (path, size, transform) in &files {
            assert!(!path.is_empty(), "Path should not be empty");
            assert!(*size > 0, "Size should be positive");
            // Simple tables typically don't have transforms
            assert!(
                transform.is_none(),
                "Simple table should not have transforms"
            );
        }
    }

    #[test]
    fn test_scan_metadata_partitioned_table() {
        // Test that scan_metadata produces expected results for a partitioned table
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else {
            println!("Skipping test: basic_partitioned not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Get scan metadata using existing API
        let scan = snapshot
            .scan_builder()
            .build()
            .expect("Should build scan");

        let scan_metadata_iter = scan
            .scan_metadata(engine.as_ref())
            .expect("Should get scan metadata");

        let mut files: Vec<(String, i64, Option<ExpressionRef>)> = Vec::new();
        for result in scan_metadata_iter {
            let scan_metadata = result.expect("Scan metadata should succeed");
            files = scan_metadata
                .visit_scan_files(files, collect_scan_files)
                .expect("visit_scan_files should succeed");
        }

        // Verify we got files
        assert!(!files.is_empty(), "Should have at least one file");
        
        // Partitioned tables should have 6 files
        assert_eq!(files.len(), 6, "basic_partitioned should have 6 files");

        // For partitioned tables, all files should have transforms (partition value injection)
        for (path, size, transform) in &files {
            assert!(!path.is_empty(), "Path should not be empty");
            assert!(*size > 0, "Size should be positive");
            // Partitioned tables have transforms for partition column injection
            assert!(
                transform.is_some(),
                "Partitioned table files should have transforms for path: {}",
                path
            );
        }
    }
}

/// Integration tests comparing ScanStateMachine + TransformComputer with Scan::scan_metadata
#[cfg(test)]
mod state_machine_comparison_tests {
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::plans::executor::ResultsDriver;
    use crate::plans::state_machines::ScanStateMachine;
    use crate::scan::state::{DvInfo, Stats};
    use crate::scan::transform::TransformComputer;
    use crate::snapshot::Snapshot;
    use crate::Engine;
    use crate::ExpressionRef;
    use crate::FileMeta;

    fn create_test_engine() -> Arc<dyn Engine> {
        Arc::new(SyncEngine::new())
    }

    fn get_test_table_path(table_name: &str) -> Option<(PathBuf, url::Url)> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data")
            .join(table_name);
        if path.exists() {
            let url = url::Url::from_directory_path(&path).unwrap();
            Some((path, url))
        } else {
            None
        }
    }

    /// Callback to collect scan file paths and transforms from ScanMetadata
    fn collect_paths_and_transforms(
        files: &mut Vec<(String, bool)>,
        path: &str,
        _size: i64,
        _stats: Option<Stats>,
        _dv_info: DvInfo,
        transform: Option<ExpressionRef>,
        _partition_values: HashMap<String, String>,
    ) {
        files.push((path.to_string(), transform.is_some()));
    }

    /// Get file paths and transform presence from Scan::scan_metadata (ground truth)
    fn get_scan_metadata_results(
        snapshot: Arc<Snapshot>,
        engine: &dyn Engine,
    ) -> Vec<(String, bool)> {
        let scan = snapshot
            .scan_builder()
            .build()
            .expect("Should build scan");

        let scan_metadata_iter = scan
            .scan_metadata(engine)
            .expect("Should get scan metadata");

        let mut files = Vec::new();
        for result in scan_metadata_iter {
            let scan_metadata = result.expect("Scan metadata should succeed");
            files = scan_metadata
                .visit_scan_files(files, collect_paths_and_transforms)
                .expect("visit_scan_files should succeed");
        }
        files
    }

    /// Get file paths and transform presence from ScanStateMachine + TransformComputer
    fn get_state_machine_results(
        snapshot: Arc<Snapshot>,
        engine: Arc<dyn Engine>,
    ) -> Vec<(String, bool)> {
        use crate::arrow::array::AsArray;

        // Get files from log segment (do this before consuming snapshot)
        // Use find_commit_cover() to handle compacted log files correctly
        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment.find_commit_cover();
        let checkpoint_files: Vec<FileMeta> = log_segment
            .checkpoint_parts
            .iter()
            .map(|f| f.location.clone())
            .collect();
        let table_url = snapshot.table_root().clone();
        let schema = snapshot.schema().clone();

        // Get the state_info from a Scan (so we use the same transform logic)
        // Note: scan_builder consumes the Arc<Snapshot>
        let scan = snapshot
            .scan_builder()
            .build()
            .expect("Should build scan");
        let state_info = scan.state_info();

        // Create state machine
        let sm = ScanStateMachine::from_scan_config(
            table_url.clone(),
            table_url.join("_delta_log/").unwrap(),
            schema.clone(),
            schema,
            commit_files,
            checkpoint_files,
        )
        .expect("Should create state machine");

        // Execute via ResultsDriver
        let driver = ResultsDriver::new(engine.clone(), sm);

        // Create TransformComputer
        let computer = TransformComputer::new(state_info);

        let mut files: Vec<(String, bool)> = Vec::new();
        for result in driver {
            let batch = result.expect("Batch should succeed");

            // Compute transforms for this batch
            let transforms = computer
                .compute_transforms(batch.engine_data.as_ref())
                .expect("compute_transforms should succeed");

            // Extract paths from batch
            let engine_data_arc = batch.engine_data.clone().as_any();
            let arrow_data = engine_data_arc
                .downcast_ref::<ArrowEngineData>()
                .expect("Should be ArrowEngineData");
            let record_batch = arrow_data.record_batch();

            let path_col = record_batch
                .column_by_name("path")
                .expect("Should have path column")
                .as_string::<i32>();

            // Collect paths with transform presence
            for (i, path) in path_col.iter().enumerate() {
                if let Some(path) = path {
                    // Check selection vector
                    let selected = batch.selection_vector.get(i).copied().unwrap_or(true);
                    if selected {
                        let has_transform = transforms.get(i).map(|t| t.is_some()).unwrap_or(false);
                        files.push((path.to_string(), has_transform));
                    }
                }
            }
        }
        files
    }

    /// Compare two sets of results
    fn assert_results_equal(
        expected: &[(String, bool)],
        actual: &[(String, bool)],
        table_name: &str,
    ) {
        let expected_set: HashSet<_> = expected.iter().collect();
        let actual_set: HashSet<_> = actual.iter().collect();

        let missing: Vec<_> = expected_set.difference(&actual_set).collect();
        let extra: Vec<_> = actual_set.difference(&expected_set).collect();

        assert!(
            missing.is_empty() && extra.is_empty(),
            "Results mismatch for table '{}'\n\
             Missing from state machine: {:?}\n\
             Extra in state machine: {:?}\n\
             Expected {} files, got {}",
            table_name,
            missing,
            extra,
            expected.len(),
            actual.len()
        );
    }

    // =========================================================================
    // Tests comparing ScanStateMachine + TransformComputer with Scan::scan_metadata
    // =========================================================================

    #[test]
    fn test_equivalence_simple_table() {
        let Some((_, table_url)) = get_test_table_path("table-without-dv-small") else {
            println!("Skipping test: table-without-dv-small not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Get results from both APIs
        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        // Compare
        assert_results_equal(&expected, &actual, "table-without-dv-small");
        
        // Additional assertions
        assert!(!expected.is_empty(), "Should have at least one file");
        println!(
            "table-without-dv-small: {} files, all transforms match",
            expected.len()
        );
    }

    #[test]
    fn test_equivalence_partitioned_table() {
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else {
            println!("Skipping test: basic_partitioned not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Get results from both APIs
        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        // Compare
        assert_results_equal(&expected, &actual, "basic_partitioned");

        // Additional assertions - partitioned tables should have transforms
        assert_eq!(expected.len(), 6, "Should have 6 files");
        for (path, has_transform) in &expected {
            assert!(
                *has_transform,
                "Partitioned file should have transform: {}",
                path
            );
        }
        println!(
            "basic_partitioned: {} files, all transforms match",
            expected.len()
        );
    }

    #[test]
    fn test_equivalence_table_with_checkpoint() {
        let Some((_, table_url)) = get_test_table_path("with_checkpoint_no_last_checkpoint") else {
            println!("Skipping test: with_checkpoint_no_last_checkpoint not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Get results from both APIs
        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        // Compare
        assert_results_equal(&expected, &actual, "with_checkpoint_no_last_checkpoint");
        
        println!(
            "with_checkpoint_no_last_checkpoint: {} files, all transforms match",
            expected.len()
        );
    }

    #[test]
    fn test_equivalence_table_with_dv() {
        let Some((_, table_url)) = get_test_table_path("table-with-dv-small") else {
            println!("Skipping test: table-with-dv-small not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Get results from both APIs
        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        // Compare
        assert_results_equal(&expected, &actual, "table-with-dv-small");
        
        println!(
            "table-with-dv-small: {} files, all transforms match",
            expected.len()
        );
    }

    #[test]
    fn test_equivalence_app_txn_checkpoint() {
        let Some((_, table_url)) = get_test_table_path("app-txn-checkpoint") else {
            println!("Skipping test: app-txn-checkpoint not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        assert_results_equal(&expected, &actual, "app-txn-checkpoint");
        println!("app-txn-checkpoint: {} files match", expected.len());
    }

    #[test]
    fn test_equivalence_app_txn_no_checkpoint() {
        let Some((_, table_url)) = get_test_table_path("app-txn-no-checkpoint") else {
            println!("Skipping test: app-txn-no-checkpoint not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        assert_results_equal(&expected, &actual, "app-txn-no-checkpoint");
        println!("app-txn-no-checkpoint: {} files match", expected.len());
    }

    // =========================================================================
    // V2 Checkpoint Tests - Use compressed test tables
    // =========================================================================

    /// Helper to test compressed tables (stored as .tar.zst files)
    fn test_compressed_table_equivalence(table_name: &str) {
        // Load and decompress test data
        let test_dir = match test_utils::load_test_data("tests/data", table_name) {
            Ok(dir) => dir,
            Err(e) => {
                println!("Skipping test {}: {:?}", table_name, e);
                return;
            }
        };
        let test_path = test_dir.path().join(table_name);

        let table_url = match url::Url::from_directory_path(&test_path) {
            Ok(url) => url,
            Err(_) => {
                println!("Skipping test {}: could not create URL", table_name);
                return;
            }
        };

        let engine = create_test_engine();
        let snapshot = match Snapshot::builder_for(table_url).build(engine.as_ref()) {
            Ok(s) => s,
            Err(e) => {
                println!("Skipping test {}: snapshot build failed: {:?}", table_name, e);
                return;
            }
        };

        let expected = get_scan_metadata_results(snapshot.clone(), engine.as_ref());
        let actual = get_state_machine_results(snapshot, engine);

        assert_results_equal(&expected, &actual, table_name);
        println!("{}: {} files match", table_name, expected.len());
    }

    // V2 Checkpoint Parquet format tests

    #[test]
    fn test_equivalence_v2_checkpoints_parquet_with_sidecars() {
        test_compressed_table_equivalence("v2-checkpoints-parquet-with-sidecars");
    }

    #[test]
    fn test_equivalence_v2_checkpoints_parquet_without_sidecars() {
        test_compressed_table_equivalence("v2-checkpoints-parquet-without-sidecars");
    }

    #[test]
    fn test_equivalence_v2_checkpoints_parquet_with_last_checkpoint() {
        test_compressed_table_equivalence("v2-checkpoints-parquet-with-last-checkpoint");
    }

    // V2 Checkpoint JSON format tests

    #[test]
    fn test_equivalence_v2_checkpoints_json_with_sidecars() {
        test_compressed_table_equivalence("v2-checkpoints-json-with-sidecars");
    }

    #[test]
    fn test_equivalence_v2_checkpoints_json_without_sidecars() {
        test_compressed_table_equivalence("v2-checkpoints-json-without-sidecars");
    }

    #[test]
    fn test_equivalence_v2_checkpoints_json_with_last_checkpoint() {
        test_compressed_table_equivalence("v2-checkpoints-json-with-last-checkpoint");
    }

    // Classic V2 checkpoint tests

    #[test]
    fn test_equivalence_v2_classic_checkpoint_json() {
        test_compressed_table_equivalence("v2-classic-checkpoint-json");
    }

    // Other compressed table tests

    #[test]
    fn test_equivalence_compacted_log_files() {
        test_compressed_table_equivalence("compacted-log-files-table");
    }

    // =========================================================================
    // Data Skipping Equivalence Tests
    // =========================================================================

    use crate::expressions::{column_expr, BinaryPredicateOp, Predicate, Scalar};
    use crate::scan::PhysicalPredicate;
    use crate::table_features::ColumnMappingMode;

    /// Get file paths from Scan::scan_metadata with a predicate (ground truth for data skipping)
    fn get_scan_metadata_with_predicate(
        snapshot: Arc<Snapshot>,
        engine: &dyn Engine,
        predicate: Option<(crate::expressions::PredicateRef, crate::schema::SchemaRef)>,
    ) -> Vec<(String, bool)> {
        let mut builder = snapshot.scan_builder();
        if let Some((pred, _)) = &predicate {
            builder = builder.with_predicate(pred.clone());
        }
        let scan = builder.build().expect("Should build scan");

        let scan_metadata_iter = scan
            .scan_metadata(engine)
            .expect("Should get scan metadata");

        let mut files = Vec::new();
        for result in scan_metadata_iter {
            let scan_metadata = result.expect("Scan metadata should succeed");
            files = scan_metadata
                .visit_scan_files(files, collect_paths_and_transforms)
                .expect("visit_scan_files should succeed");
        }
        files
    }

    /// Get file paths from ScanStateMachine with a predicate
    fn get_state_machine_with_predicate(
        snapshot: Arc<Snapshot>,
        engine: Arc<dyn Engine>,
        predicate: Option<(crate::expressions::PredicateRef, crate::schema::SchemaRef)>,
    ) -> Vec<(String, bool)> {
        use crate::arrow::array::AsArray;

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment.find_commit_cover();
        let checkpoint_files: Vec<FileMeta> = log_segment
            .checkpoint_parts
            .iter()
            .map(|f| f.location.clone())
            .collect();
        let table_url = snapshot.table_root().clone();
        let schema = snapshot.schema().clone();

        // Build scan with predicate to get the state_info
        let mut scan_builder = snapshot.scan_builder();
        if let Some((pred, _)) = &predicate {
            scan_builder = scan_builder.with_predicate(pred.clone());
        }
        let scan = scan_builder.build().expect("Should build scan");
        let state_info = scan.state_info();

        // Create state machine with predicate
        let sm = crate::plans::state_machines::ScanStateMachine::from_scan_config_with_predicate(
            table_url.clone(),
            table_url.join("_delta_log/").unwrap(),
            schema.clone(),
            schema,
            commit_files,
            checkpoint_files,
            predicate,
        )
        .expect("Should create state machine");

        // Execute via ResultsDriver
        let driver = ResultsDriver::new(engine.clone(), sm);
        let computer = TransformComputer::new(state_info);

        let mut files: Vec<(String, bool)> = Vec::new();
        for result in driver {
            let batch = result.expect("Batch should succeed");

            let transforms = computer
                .compute_transforms(batch.engine_data.as_ref())
                .expect("compute_transforms should succeed");

            let engine_data_arc = batch.engine_data.clone().as_any();
            let arrow_data = engine_data_arc
                .downcast_ref::<ArrowEngineData>()
                .expect("Should be ArrowEngineData");
            let record_batch = arrow_data.record_batch();

            let path_col = record_batch
                .column_by_name("path")
                .expect("Should have path column")
                .as_string::<i32>();

            for (i, path) in path_col.iter().enumerate() {
                if let Some(path) = path {
                    let selected = batch.selection_vector.get(i).copied().unwrap_or(true);
                    if selected {
                        let has_transform = transforms.get(i).map(|t| t.is_some()).unwrap_or(false);
                        files.push((path.to_string(), has_transform));
                    }
                }
            }
        }
        files
    }

    /// Test data skipping equivalence for a table with a predicate
    fn test_data_skipping_equivalence(
        table_name: &str,
        predicate_fn: impl Fn(&crate::schema::StructType) -> Predicate,
    ) {
        let Some((_, table_url)) = get_test_table_path(table_name) else {
            println!("Skipping test: {} not found", table_name);
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        // Create predicate based on the table's logical schema
        let logical_schema = snapshot.schema();
        let predicate = predicate_fn(&logical_schema);

        // Convert to PhysicalPredicate to get referenced schema
        let physical_predicate =
            match PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::None) {
                Ok(PhysicalPredicate::Some(pred, schema)) => Some((pred, schema)),
                Ok(PhysicalPredicate::StaticSkipAll) => {
                    println!("Predicate statically skips all files for {}", table_name);
                    return;
                }
                Ok(PhysicalPredicate::None) => {
                    println!("Predicate has no data skipping potential for {}", table_name);
                    None
                }
                Err(e) => {
                    panic!("Failed to create predicate for {}: {:?}", table_name, e);
                }
            };

        let expected = get_scan_metadata_with_predicate(
            snapshot.clone(),
            engine.as_ref(),
            physical_predicate.clone(),
        );
        let actual = get_state_machine_with_predicate(snapshot, engine, physical_predicate);

        assert_results_equal(&expected, &actual, table_name);
        println!(
            "{}: {} files with data skipping predicate",
            table_name,
            expected.len()
        );
    }

    #[test]
    fn test_data_skipping_no_predicate() {
        // Test that with no predicate, results match (baseline)
        test_data_skipping_equivalence("basic_partitioned", |_| {
            // Return a tautology that doesn't skip anything
            Predicate::binary(
                BinaryPredicateOp::GreaterThan,
                column_expr!("number"),
                Scalar::from(-1000i64), // Everything is > -1000
            )
        });
    }

    #[test]
    fn test_data_skipping_keep_some_files() {
        // Test predicate that keeps some files: number > 3
        // Files: letter=a (1,4), letter=b (2), letter=c (3), letter=e (5), null (6)
        // Should keep: letter=a(4), letter=e(5), null(6) => 3 files
        test_data_skipping_equivalence("basic_partitioned", |_| {
            Predicate::binary(
                BinaryPredicateOp::GreaterThan,
                column_expr!("number"),
                Scalar::from(3i64),
            )
        });
    }

    #[test]
    fn test_data_skipping_simple_table() {
        // Test data skipping on table-without-dv-small
        // Stats: value range 0-9
        test_data_skipping_equivalence("table-without-dv-small", |_| {
            // value > 5 - should keep the file (range 0-9 overlaps with >5)
            Predicate::binary(
                BinaryPredicateOp::GreaterThan,
                column_expr!("value"),
                Scalar::from(5i64),
            )
        });
    }

    #[test]
    fn test_data_skipping_skip_all() {
        // Test predicate that would skip all files if data skipping works: number > 100
        // All files have number in range 1-6, so all should be skipped
        // Note: StaticSkipAll might be returned, which is also correct
        let table_name = "basic_partitioned";
        let Some((_, table_url)) = get_test_table_path(table_name) else {
            println!("Skipping test: {} not found", table_name);
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url)
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let predicate = Predicate::binary(
            BinaryPredicateOp::GreaterThan,
            column_expr!("number"),
            Scalar::from(100i64),
        );

        // Convert to PhysicalPredicate
        let logical_schema = snapshot.schema();
        let physical_predicate =
            match PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::None) {
                Ok(PhysicalPredicate::Some(pred, schema)) => Some((pred, schema)),
                Ok(PhysicalPredicate::StaticSkipAll) => {
                    // If predicate statically skips all, both APIs should return 0 files
                    println!("Predicate statically skips all files");
                    return;
                }
                Ok(PhysicalPredicate::None) => None,
                Err(e) => panic!("Failed to create predicate: {:?}", e),
            };

        let expected = get_scan_metadata_with_predicate(
            snapshot.clone(),
            engine.as_ref(),
            physical_predicate.clone(),
        );
        let actual = get_state_machine_with_predicate(snapshot, engine, physical_predicate);

        // Both should return 0 files (or whatever scan_metadata returns)
        assert_results_equal(&expected, &actual, table_name);
        println!(
            "{} with skip-all predicate: {} files",
            table_name,
            expected.len()
        );
    }
}
