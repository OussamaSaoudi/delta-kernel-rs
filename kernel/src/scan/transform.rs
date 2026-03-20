//! Transform computation for scan results.
//!
//! This module provides the [`TransformComputer`] which computes row-level transform
//! expressions from scan batches.

use std::collections::HashMap;
use std::sync::Arc;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_name, ColumnName, Scalar};
use crate::scan::state_info::StateInfo;
use crate::schema::{DataType, MapType};
use crate::scan::transform_spec::{get_transform_expr, parse_partition_values, TransformSpec};
use crate::{DeltaResult, EngineData, ExpressionRef};

macro_rules! column_names_and_types {
    ($($dt:expr => $col:expr),+ $(,)?) => {{
        static NAMES_AND_TYPES: std::sync::LazyLock<crate::schema::ColumnNamesAndTypes> =
            std::sync::LazyLock::new(|| {
                let types_and_names = vec![$(($dt, $col)),+];
                let (types, names) = types_and_names.into_iter().unzip();
                (names, types).into()
            });
        NAMES_AND_TYPES.as_ref()
    }};
}

/// Computes row-level transform expressions from scan batches.
#[derive(Debug, Clone)]
pub struct TransformComputer {
    state_info: Arc<StateInfo>,
}

impl TransformComputer {
    /// Create a new `TransformComputer` with the given state info.
    pub fn new(state_info: Arc<StateInfo>) -> Self {
        Self { state_info }
    }

    /// Compute transform expressions for each row in the batch.
    pub fn compute_transforms(
        &self,
        data: &dyn EngineData,
    ) -> DeltaResult<Vec<Option<ExpressionRef>>> {
        let mut visitor = TransformVisitor::new(self.state_info.clone());
        visitor.visit_rows_of(data)?;
        Ok(visitor.row_transform_exprs)
    }
}

struct TransformVisitor {
    state_info: Arc<StateInfo>,
    row_transform_exprs: Vec<Option<ExpressionRef>>,
}

impl TransformVisitor {
    const PARTITION_VALUES_INDEX: usize = 0;
    const BASE_ROW_ID_INDEX: usize = 1;

    fn new(state_info: Arc<StateInfo>) -> Self {
        Self { state_info, row_transform_exprs: Vec::new() }
    }

    fn compute_transform_for_row<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Option<ExpressionRef>> {
        let Some(transform_spec) = &self.state_info.transform_spec else {
            return Ok(None);
        };
        let partition_values = self.extract_partition_values(i, getters, transform_spec)?;
        let base_row_id: Option<i64> =
            getters[Self::BASE_ROW_ID_INDEX].get_opt(i, "fileConstantValues.baseRowId")?;
        get_transform_expr(transform_spec, partition_values, &self.state_info.physical_schema, base_row_id).map(Some)
    }

    fn extract_partition_values<'a>(
        &self, i: usize, getters: &[&'a dyn GetData<'a>], transform_spec: &TransformSpec,
    ) -> DeltaResult<HashMap<usize, (String, Scalar)>> {
        let partition_values: Option<HashMap<String, String>> =
            getters[Self::PARTITION_VALUES_INDEX].get_opt(i, "fileConstantValues.partitionValues")?;
        parse_partition_values(
            &self.state_info.logical_schema, transform_spec,
            &partition_values.unwrap_or_default(), self.state_info.column_mapping_mode,
        )
    }
}

impl RowVisitor for TransformVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        const STRING: DataType = DataType::STRING;
        const LONG: DataType = DataType::LONG;
        column_names_and_types![
            MapType::new(STRING, STRING, true).into() => column_name!("fileConstantValues.partitionValues"),
            LONG => column_name!("fileConstantValues.baseRowId"),
        ]
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let transform = self.compute_transform_for_row(i, getters)?;
            if transform.is_some() {
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

    #[test]
    fn test_visitor_columns() {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::LONG),
        ]));
        let state_info = Arc::new(StateInfo {
            logical_schema: schema.clone(),
            physical_schema: schema,
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
            physical_stats_schema: None,
            physical_partition_schema: None,
        });
        let visitor = TransformVisitor::new(state_info);
        let (names, types) = visitor.selected_column_names_and_types();
        assert_eq!(names.len(), 2);
        assert_eq!(types.len(), 2);
        assert_eq!(names[0].to_string(), "fileConstantValues.partitionValues");
        assert_eq!(names[1].to_string(), "fileConstantValues.baseRowId");
    }
}

/// Integration tests comparing ScanStateMachine + TransformComputer with Scan::scan_metadata
#[cfg(all(test, feature = "arrow"))]
mod integration_tests {
    use super::*;
    use crate::arrow::array::AsArray;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{column_expr, BinaryPredicateOp, Predicate, Scalar};
    use crate::plans::executor::ResultsDriver;
    use crate::plans::state_machines::ScanStateMachine;
    use crate::scan::state::ScanFile;
    use crate::snapshot::Snapshot;
    use crate::{Engine, PredicateRef};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn engine() -> Arc<dyn Engine> { Arc::new(SyncEngine::new()) }

    fn table_url(name: &str) -> Option<url::Url> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data").join(name);
        path.exists().then(|| url::Url::from_directory_path(&path).unwrap())
    }

    fn compressed_table_url(name: &str) -> Option<(tempfile::TempDir, url::Url)> {
        let dir = test_utils::load_test_data("tests/data", name).ok()?;
        let url = url::Url::from_directory_path(dir.path().join(name)).ok()?;
        Some((dir, url))
    }

    fn scan_metadata_results(snap: Arc<Snapshot>, engine: &dyn Engine, pred: Option<PredicateRef>) -> Vec<(String, bool)> {
        fn callback(files: &mut Vec<(String, bool)>, sf: ScanFile) {
            files.push((sf.path.to_string(), sf.transform.is_some()));
        }
        let mut builder = snap.scan_builder();
        if let Some(p) = pred { builder = builder.with_predicate(p); }
        let scan = builder.build().unwrap();
        let mut files = Vec::new();
        for result in scan.scan_metadata(engine).unwrap() {
            files = result.unwrap().visit_scan_files(files, callback).unwrap();
        }
        files
    }

    fn state_machine_results(snap: Arc<Snapshot>, engine: Arc<dyn Engine>, pred: Option<PredicateRef>) -> Vec<(String, bool)> {
        let mut builder = snap.scan_builder();
        if let Some(p) = pred { builder = builder.with_predicate(p); }
        let scan = builder.build().unwrap();
        let state_info = scan.state_info();
        let sm = ScanStateMachine::from_scan(&scan).unwrap();
        let computer = TransformComputer::new(state_info);
        let mut files = Vec::new();
        for batch in ResultsDriver::new(engine.as_ref(), sm) {
            let batch = batch.unwrap();
            let transforms = computer.compute_transforms(batch.data()).unwrap();
            let arrow = batch.data().any_ref().downcast_ref::<ArrowEngineData>().unwrap();
            let paths = arrow.record_batch().column_by_name("path").unwrap().as_string::<i32>();
            for (i, path) in paths.iter().enumerate() {
                if let Some(p) = path {
                    if batch.selection_vector().get(i).copied().unwrap_or(true) {
                        let has_transform = transforms.get(i).and_then(|t| t.as_ref()).is_some();
                        files.push((p.to_string(), has_transform));
                    }
                }
            }
        }
        files
    }

    fn assert_eq_results(expected: &[(String, bool)], actual: &[(String, bool)], table: &str) {
        let exp: HashSet<_> = expected.iter().collect();
        let act: HashSet<_> = actual.iter().collect();
        assert!(exp == act, "Mismatch for '{}': missing {:?}, extra {:?}",
            table, exp.difference(&act).collect::<Vec<_>>(), act.difference(&exp).collect::<Vec<_>>());
    }

    #[rstest]
    #[case("table-without-dv-small", false)]
    #[case("basic_partitioned", true)]
    #[case("table-with-dv-small", false)]
    #[case("app-txn-checkpoint", false)]
    fn test_equivalence_uncompressed(#[case] table: &str, #[case] expect_transforms: bool) {
        let Some(url) = table_url(table) else { return; };
        let eng = engine();
        let snap = Snapshot::builder_for(url).build(eng.as_ref()).unwrap();
        let expected = scan_metadata_results(snap.clone(), eng.as_ref(), None);
        let actual = state_machine_results(snap, eng, None);
        assert_eq_results(&expected, &actual, table);
        if expect_transforms {
            assert!(expected.iter().all(|(_, t)| *t), "Expected transforms for {}", table);
        }
    }

    #[rstest]
    #[case("v2-checkpoints-parquet-with-sidecars")]
    #[case("v2-checkpoints-json-with-sidecars")]
    #[case("compacted-log-files-table")]
    fn test_equivalence_compressed(#[case] table: &str) {
        let Some((_dir, url)) = compressed_table_url(table) else { return; };
        let eng = engine();
        let Ok(snap) = Snapshot::builder_for(url).build(eng.as_ref()) else { return; };
        let expected = scan_metadata_results(snap.clone(), eng.as_ref(), None);
        let actual = state_machine_results(snap, eng, None);
        assert_eq_results(&expected, &actual, table);
    }

    #[test]
    fn test_data_skipping_with_predicate() {
        let Some(url) = table_url("basic_partitioned") else { return; };
        let eng = engine();
        let snap = Snapshot::builder_for(url).build(eng.as_ref()).unwrap();
        let pred: PredicateRef = Arc::new(Predicate::binary(
            BinaryPredicateOp::GreaterThan, column_expr!("number"), Scalar::from(3i64),
        ));
        let expected = scan_metadata_results(snap.clone(), eng.as_ref(), Some(pred.clone()));
        let actual = state_machine_results(snap, eng, Some(pred));
        assert_eq_results(&expected, &actual, "basic_partitioned with predicate");
    }
}
