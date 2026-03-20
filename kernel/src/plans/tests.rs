//! Tests for plan serialization and conversion.
//!
//! This module consolidates tests using rstest parameterization to minimize code duplication.
//!
//! Note: This entire module is already gated by #[cfg(test)] in mod.rs, so inner modules
//! don't need the attribute.

// =============================================================================
// Common Test Helpers
// =============================================================================

mod test_helpers {
    use std::sync::Arc;

    use crate::arrow::array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray};
    use crate::arrow::buffer::OffsetBuffer;
    use crate::arrow::datatypes::{DataType, Field, Fields, Schema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::schema::{DataType as KernelDataType, SchemaRef, StructField, StructType};

    /// Standard test schema with id (int) and name (string) columns.
    pub fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", KernelDataType::INTEGER, false),
            StructField::new("name", KernelDataType::STRING, true),
        ]))
    }

    /// Schema with sidecar field for v2 checkpoint testing.
    pub fn schema_with_sidecar() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(
                "add",
                KernelDataType::struct_type_unchecked(vec![
                    StructField::not_null("path", KernelDataType::STRING),
                    StructField::not_null("size", KernelDataType::LONG),
                ]),
            ),
            StructField::nullable(
                "sidecar",
                KernelDataType::struct_type_unchecked(vec![
                    StructField::not_null("path", KernelDataType::STRING),
                    StructField::not_null("sizeInBytes", KernelDataType::LONG),
                ]),
            ),
        ]))
    }

    /// Schema without sidecar field (classic checkpoint).
    pub fn schema_without_sidecar() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(
                "add",
                KernelDataType::struct_type_unchecked(vec![
                    StructField::not_null("path", KernelDataType::STRING),
                    StructField::not_null("size", KernelDataType::LONG),
                ]),
            ),
            StructField::nullable(
                "remove",
                KernelDataType::struct_type_unchecked(vec![
                    StructField::not_null("path", KernelDataType::STRING),
                ]),
            ),
        ]))
    }

    /// Creates a batch with add/remove structs for dedup filter testing.
    /// This is the canonical helper - do not duplicate elsewhere.
    pub fn create_add_remove_batch(paths: Vec<&str>) -> RecordBatch {
        let len = paths.len();
        let path_array = StringArray::from(paths);
        let dv_storage = StringArray::from(vec![None::<&str>; len]);
        let dv_path = StringArray::from(vec![None::<&str>; len]);
        let dv_offset = Int32Array::from(vec![None::<i32>; len]);
        let dv_fields = Fields::from(vec![
            Field::new("storageType", DataType::Utf8, true),
            Field::new("pathOrInlineDv", DataType::Utf8, true),
            Field::new("offset", DataType::Int32, true),
        ]);
        let dv_struct = StructArray::from(vec![
            (Arc::new(Field::new("storageType", DataType::Utf8, true)), Arc::new(dv_storage.clone()) as Arc<dyn Array>),
            (Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)), Arc::new(dv_path.clone()) as Arc<dyn Array>),
            (Arc::new(Field::new("offset", DataType::Int32, true)), Arc::new(dv_offset.clone()) as Arc<dyn Array>),
        ]);
        let add_fields = Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("deletionVector", DataType::Struct(dv_fields.clone()), true),
        ]);
        let add_struct = StructArray::from(vec![
            (Arc::new(Field::new("path", DataType::Utf8, true)), Arc::new(path_array) as Arc<dyn Array>),
            (Arc::new(Field::new("deletionVector", DataType::Struct(dv_fields.clone()), true)), Arc::new(dv_struct) as Arc<dyn Array>),
        ]);

        // Remove struct (all null)
        let remove_path = StringArray::from(vec![None::<&str>; len]);
        let remove_dv_struct = StructArray::from(vec![
            (Arc::new(Field::new("storageType", DataType::Utf8, true)), Arc::new(dv_storage) as Arc<dyn Array>),
            (Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)), Arc::new(dv_path) as Arc<dyn Array>),
            (Arc::new(Field::new("offset", DataType::Int32, true)), Arc::new(dv_offset) as Arc<dyn Array>),
        ]);
        let remove_fields = Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("deletionVector", DataType::Struct(dv_fields.clone()), true),
        ]);
        let remove_struct = StructArray::from(vec![
            (Arc::new(Field::new("path", DataType::Utf8, true)), Arc::new(remove_path) as Arc<dyn Array>),
            (Arc::new(Field::new("deletionVector", DataType::Struct(dv_fields), true)), Arc::new(remove_dv_struct) as Arc<dyn Array>),
        ]);

        let schema = Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields), true),
            Field::new("remove", DataType::Struct(remove_fields), true),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(add_struct), Arc::new(remove_struct)]).unwrap()
    }

    /// Creates a batch with add/remove structs from owned strings.
    pub fn create_add_remove_batch_owned(paths: Vec<String>) -> RecordBatch {
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        create_add_remove_batch(refs)
    }

    /// Creates a sidecar batch for testing SidecarCollectorState.
    pub fn create_sidecar_batch(paths: &[&str], sizes: &[i64]) -> ArrowEngineData {
        use crate::arrow::array::MapArray;

        let len = paths.len();
        let path_array = StringArray::from(paths.to_vec());
        let size_array = Int64Array::from(sizes.to_vec());
        let modification_time_array = Int64Array::from(vec![0i64; len]);

        // Empty map array for tags
        let keys_array = StringArray::new_null(0);
        let values_array = StringArray::new_null(0);
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]),
            vec![Arc::new(keys_array), Arc::new(values_array)],
            None,
        );
        let offsets = OffsetBuffer::from_lengths(vec![0; len]);
        let tags_array = MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        );

        let sidecar_struct = StructArray::from(vec![
            (Arc::new(Field::new("path", DataType::Utf8, false)), Arc::new(path_array) as Arc<dyn Array>),
            (Arc::new(Field::new("sizeInBytes", DataType::Int64, false)), Arc::new(size_array) as Arc<dyn Array>),
            (Arc::new(Field::new("modificationTime", DataType::Int64, false)), Arc::new(modification_time_array) as Arc<dyn Array>),
            (
                Arc::new(Field::new("tags", DataType::Map(
                    Arc::new(Field::new("entries", DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])), false)),
                    false,
                ), true)),
                Arc::new(tags_array) as Arc<dyn Array>,
            ),
        ]);

        let schema = Schema::new(vec![Field::new("sidecar", sidecar_struct.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(sidecar_struct)]).unwrap();
        ArrowEngineData::new(batch)
    }

    /// Helper to assert selection vector values.
    pub fn assert_selection(result: &BooleanArray, expected: &[bool]) {
        assert_eq!(result.len(), expected.len(), "Selection length mismatch");
        for (i, &exp) in expected.iter().enumerate() {
            assert_eq!(result.value(i), exp, "Selection mismatch at index {}", i);
        }
    }
}

// =============================================================================
// Proto Roundtrip Tests (Parameterized)
// =============================================================================

mod proto_roundtrip_tests {
    use std::sync::Arc;
    use prost::Message;
    use rstest::rstest;

    use crate::plans::*;
    use crate::proto_generated as proto;
    use super::test_helpers::test_schema;

    /// Tests proto roundtrip for different plan configurations.
    #[rstest]
    #[case::scan_parquet(FileType::Parquet)]
    #[case::scan_json(FileType::Json)]
    fn test_scan_node_proto_roundtrip(#[case] file_type: FileType) {
        let scan = ScanNode {
            file_type,
            files: vec![],
            schema: test_schema(),
        };

        let proto_scan: proto::ScanNode = (&scan).into();
        let expected_type = match file_type {
            FileType::Parquet => proto::scan_node::FileType::Parquet,
            FileType::Json => proto::scan_node::FileType::Json,
        };
        assert_eq!(proto_scan.file_type, expected_type as i32);

        // Roundtrip via bytes
        let bytes = proto_scan.encode_to_vec();
        let decoded = proto::ScanNode::decode(bytes.as_slice()).expect("decode");
        assert_eq!(decoded.file_type, expected_type as i32);
    }

    #[test]
    fn test_filter_node_proto() {
        let (filter, _receiver) = FilterByKDF::add_remove_dedup();
        let proto_filter: proto::FilterByKdf = (&filter).into();
        assert_ne!(proto_filter.state_ptr, 0);
    }

    #[test]
    fn test_declarative_plan_roundtrip() {
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![],
            schema: test_schema(),
        });

        let (dedup_sender, _receiver) = FilterByKDF::add_remove_dedup();
        let plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: dedup_sender,
        };

        let proto_plan: proto::DeclarativePlanNode = (&plan).into();
        let bytes = proto_plan.encode_to_vec();
        assert!(!bytes.is_empty());

        let decoded = proto::DeclarativePlanNode::decode(bytes.as_slice()).expect("decode");
        assert!(decoded.node.is_some());

        match decoded.node.unwrap() {
            proto::declarative_plan_node::Node::FilterByKdf(filter_plan) => {
                assert!(filter_plan.child.is_some());
                assert!(filter_plan.node.is_some());
            }
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_commit_phase_plan_proto() {
        let (dedup_sender, _receiver) = FilterByKDF::add_remove_dedup();
        let commit_plan = CommitPhasePlan {
            scans: vec![ScanWithVersion {
                scan: ScanNode { file_type: FileType::Json, files: vec![], schema: test_schema() },
                select: SelectNode { columns: vec![], output_schema: test_schema() },
            }],
            data_skipping: None,
            partition_prune_filter: None,
            dedup_filter: dedup_sender,
            project: SelectNode { columns: vec![], output_schema: test_schema() },
            sink: SinkNode::results(),
        };

        let proto_plan: proto::CommitPhasePlan = (&commit_plan).into();
        assert!(proto_plan.dedup_filter.is_some());
        assert!(proto_plan.project.is_some());
    }
}

// =============================================================================
// Typed State Tests (Consolidated)
// =============================================================================

mod typed_state_tests {
    use crate::arrow::array::BooleanArray;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::plans::*;
    use rstest::rstest;

    use super::test_helpers::{create_add_remove_batch, create_add_remove_batch_owned, assert_selection};

    #[test]
    fn test_filter_kdf_state_raw_roundtrip() {
        let state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        let ptr = state.into_raw();
        assert_ne!(ptr, 0);
        let recovered = unsafe { FilterKdfState::from_raw(ptr) };
        assert!(matches!(recovered, FilterKdfState::AddRemoveDedup(_)));
    }

    #[test]
    fn test_filter_kdf_state_serialize_roundtrip() {
        let state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        let bytes = state.serialize().expect("serialize");
        assert!(!bytes.is_empty());
        let recovered = AddRemoveDedupState::deserialize(&bytes).expect("deserialize");
        assert!(recovered.is_empty());
    }

    #[rstest]
    #[case::add_remove_dedup(FilterByKDF::add_remove_dedup, |s: &FilterKdfState| matches!(s, FilterKdfState::AddRemoveDedup(_)))]
    #[case::checkpoint_dedup(FilterByKDF::checkpoint_dedup, |s: &FilterKdfState| matches!(s, FilterKdfState::CheckpointDedup(_)))]
    fn test_filter_by_kdf_constructors(
        #[case] constructor: fn() -> (FilterByKDF, FilterStateReceiver),
        #[case] matcher: fn(&FilterKdfState) -> bool,
    ) {
        let (filter, _receiver) = constructor();
        assert!(matcher(filter.template()));
    }

    #[test]
    fn test_filter_kdf_apply_dedup() {
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());

        // First batch - both new files should be selected
        let batch = create_add_remove_batch(vec!["file1.parquet", "file2.parquet"]);
        let engine_data = ArrowEngineData::new(batch);
        let result = state.apply(&engine_data, BooleanArray::from(vec![true, true])).unwrap();
        assert_selection(&result, &[true, true]);

        // Second batch - file1 is duplicate, file3 is new
        let batch2 = create_add_remove_batch(vec!["file1.parquet", "file3.parquet"]);
        let engine_data2 = ArrowEngineData::new(batch2);
        let result2 = state.apply(&engine_data2, BooleanArray::from(vec![true, true])).unwrap();
        assert_selection(&result2, &[false, true]);
    }

    #[test]
    fn test_filter_kdf_apply_large_batch() {
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());

        let paths: Vec<String> = (0..100).map(|i| format!("file{}.parquet", i)).collect();
        let batch = create_add_remove_batch_owned(paths.clone());
        let engine_data = ArrowEngineData::new(batch);

        let result = state.apply(&engine_data, BooleanArray::from(vec![true; 100])).unwrap();
        assert!(result.iter().all(|v| v == Some(true)), "All 100 should be selected first time");

        // Apply same files again - all should be filtered
        let batch2 = create_add_remove_batch_owned(paths);
        let engine_data2 = ArrowEngineData::new(batch2);
        let result2 = state.apply(&engine_data2, BooleanArray::from(vec![true; 100])).unwrap();
        assert!(result2.iter().all(|v| v == Some(false)), "All should be filtered as duplicates");
    }

    #[test]
    fn test_filter_kdf_state_isolated() {
        let mut state1 = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        let mut state2 = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());

        let batch = create_add_remove_batch(vec!["shared.parquet"]);
        let engine_data = ArrowEngineData::new(batch);

        // Both states should select the same file independently
        let r1 = state1.apply(&engine_data, BooleanArray::from(vec![true])).unwrap();
        let r2 = state2.apply(&engine_data, BooleanArray::from(vec![true])).unwrap();
        assert!(r1.value(0) && r2.value(0), "Both states should select new file");

        // state1 marks as duplicate, state2 still fresh
        let r1_again = state1.apply(&engine_data, BooleanArray::from(vec![true])).unwrap();
        assert!(!r1_again.value(0), "state1 should filter duplicate");
    }
}

// =============================================================================
// Schema Query and Sidecar Tests
// =============================================================================

mod schema_tests {
    use prost::Message;

    use crate::plans::*;
    use crate::plans::kdf_state::{SidecarCollectorState, ConsumerKdfState, StateSender};
    use crate::proto_generated as proto;

    use super::test_helpers::{test_schema, schema_with_sidecar, schema_without_sidecar, create_sidecar_batch};

    #[test]
    fn test_schema_has_sidecar_field() {
        assert!(schema_with_sidecar().field("sidecar").is_some());
        assert!(schema_without_sidecar().field("sidecar").is_none());
    }

    #[test]
    fn test_schema_store_state() {
        let mut state = SchemaStoreState::new();
        assert!(state.get().is_none());

        state.store(test_schema());
        assert!(state.get().is_some());
        assert_eq!(state.get().unwrap().num_fields(), 2);
    }

    #[test]
    fn test_schema_query_proto_roundtrip() {
        let node = SchemaQueryNode::schema_store("/path/to/checkpoint.parquet");
        let proto_node: proto::SchemaQueryNode = (&node).into();
        let bytes = proto_node.encode_to_vec();

        let decoded = proto::SchemaQueryNode::decode(bytes.as_slice()).expect("decode");
        assert_eq!(decoded.file_path, "/path/to/checkpoint.parquet");
    }

    #[test]
    fn test_schema_query_plan_as_query_plan() {
        let plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store("/path/to/checkpoint.parquet"),
            sink: SinkNode::drop(),
        };

        let query_plan = plan.as_query_plan();
        assert!(query_plan.is_drop_sink());

        match query_plan {
            DeclarativePlanNode::Sink { child, .. } => {
                match child.as_ref() {
                    DeclarativePlanNode::SchemaQuery(node) => {
                        assert_eq!(node.file_path, "/path/to/checkpoint.parquet");
                    }
                    _ => panic!("Expected SchemaQuery"),
                }
            }
            _ => panic!("Expected Sink"),
        }
    }

    #[test]
    fn test_sidecar_collector_basic() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let state = SidecarCollectorState::new(log_root);
        assert!(!state.has_sidecars());
        assert_eq!(state.sidecar_count(), 0);
    }

    #[test]
    fn test_sidecar_collector_collects_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = SidecarCollectorState::new(log_root);

        let batch = create_sidecar_batch(&["sidecar1.parquet", "sidecar2.parquet"], &[1000, 2000]);
        state.apply(&batch).unwrap();

        assert!(state.has_sidecars());
        assert_eq!(state.sidecar_count(), 2);

        let files = state.take_sidecar_files();
        assert_eq!(files.len(), 2);
        assert!(files[0].location.path().contains("sidecar1.parquet"));
        assert_eq!(files[0].size, 1000);
    }

    #[test]
    fn test_sidecar_collector_via_consumer_state() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = ConsumerKdfState::SidecarCollector(SidecarCollectorState::new(log_root));

        let batch = create_sidecar_batch(&["sidecar1.parquet"], &[1000]);
        assert!(state.apply(&batch).unwrap());
        assert!(!state.has_error());
        state.finalize();
    }
}

// =============================================================================
// Declarative Phase Tests
// =============================================================================

mod declarative_phase_tests {
    use std::sync::Arc;
    use prost::Message;

    use crate::plans::*;
    use crate::plans::kdf_state::{StateSender, ConsumerKdfState, LogSegmentBuilderState};
    use crate::proto_generated as proto;
    use crate::schema::{DataType, StructField, StructType};

    #[test]
    fn test_snapshot_build_phase() {
        let snapshot_data = SnapshotData {
            version: 42,
            table_schema: Arc::new(StructType::new_unchecked(vec![
                StructField::new("id", DataType::INTEGER, false),
            ])),
        };

        let phase = SnapshotBuildPhase::Ready(snapshot_data);
        let declarative = phase.as_declarative_phase();

        assert_eq!(declarative.operation, OperationType::SnapshotBuild);
        assert_eq!(declarative.phase_type, PhaseType::Ready);
        assert!(declarative.is_terminal());

        match declarative.terminal_data {
            Some(TerminalData::SnapshotReady { version, .. }) => assert_eq!(version, 42),
            _ => panic!("Expected SnapshotReady"),
        }
    }

    #[test]
    fn test_declarative_phase_proto_roundtrip() {
        let (_sender, receiver) = StateSender::build(ConsumerKdfState::LogSegmentBuilder(
            LogSegmentBuilderState::new(
                url::Url::parse("file:///test/_delta_log/").unwrap(),
                None,
                None,
            ),
        ));

        let phase = SnapshotPhase::ListFiles { consumer_receiver: receiver };
        let declarative = phase.as_declarative_phase();

        let proto_phase: proto::DeclarativePhase = (&declarative).into();
        let bytes = proto_phase.encode_to_vec();

        let decoded = proto::DeclarativePhase::decode(bytes.as_slice()).expect("decode");
        assert_eq!(decoded.operation, proto::OperationType::SnapshotBuild as i32);
        assert_eq!(decoded.phase_type, proto::PhaseType::ListFiles as i32);
    }
}

// =============================================================================
// Plan Structure Tests (Consolidated)
// =============================================================================

mod plan_structure_tests {
    use std::sync::Arc;

    use crate::plans::*;
    use crate::plans::kdf_state::{StateSender, ConsumerKdfState, SidecarCollectorState};
    use crate::schema::StructType;

    use super::test_helpers::test_schema;

    #[test]
    fn test_commit_phase_as_query_plan() {
        let (dedup_sender, _receiver) = FilterByKDF::add_remove_dedup();
        let commit_plan = CommitPhasePlan {
            scans: vec![ScanWithVersion {
                scan: ScanNode { file_type: FileType::Json, files: vec![], schema: test_schema() },
                select: SelectNode { columns: vec![], output_schema: test_schema() },
            }],
            data_skipping: None,
            partition_prune_filter: None,
            dedup_filter: dedup_sender,
            project: SelectNode { columns: vec![], output_schema: test_schema() },
            sink: SinkNode::results(),
        };

        let tree = commit_plan.as_query_plan();

        // Verify structure: Sink -> Select -> Filter -> Select -> Scan
        match tree {
            DeclarativePlanNode::Sink { child, .. } => {
                match *child {
                    DeclarativePlanNode::Select { child, .. } => {
                        match *child {
                            DeclarativePlanNode::FilterByKDF { child: inner, node } => {
                                assert!(matches!(node.template(), FilterKdfState::AddRemoveDedup(_)));
                                match *inner {
                                    DeclarativePlanNode::Select { child: scan_child, .. } => {
                                        assert!(matches!(*scan_child, DeclarativePlanNode::Scan(_)));
                                    }
                                    _ => panic!("Expected Select"),
                                }
                            }
                            _ => panic!("Expected Filter"),
                        }
                    }
                    _ => panic!("Expected Select"),
                }
            }
            _ => panic!("Expected Sink"),
        }
    }

    #[test]
    fn test_checkpoint_leaf_has_results_sink() {
        let (dedup_sender, _receiver) = FilterByKDF::checkpoint_dedup();
        let leaf_plan = CheckpointLeafPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: vec![],
                schema: Arc::new(StructType::new_unchecked(vec![])),
            },
            partition_prune_filter: None,
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![],
                output_schema: Arc::new(StructType::new_unchecked(vec![])),
            },
            sink: SinkNode::results(),
        };

        let plan = leaf_plan.as_query_plan();
        assert!(plan.is_results_sink());

        match plan {
            DeclarativePlanNode::Sink { node, .. } => {
                assert_eq!(node.sink_type, SinkType::Results);
            }
            _ => panic!("Expected Sink"),
        }
    }

    #[test]
    fn test_checkpoint_manifest_has_drop_sink() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let (sender, _receiver) = StateSender::build(ConsumerKdfState::SidecarCollector(
            SidecarCollectorState::new(log_root),
        ));
        let manifest_plan = CheckpointManifestPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: vec![],
                schema: Arc::new(StructType::new_unchecked(vec![])),
            },
            project: SelectNode {
                columns: vec![],
                output_schema: Arc::new(StructType::new_unchecked(vec![])),
            },
            sidecar_collector: sender,
            sink: SinkNode::drop(),
        };

        let plan = manifest_plan.as_query_plan();
        assert!(!plan.is_results_sink());

        match plan {
            DeclarativePlanNode::Sink { node, .. } => {
                assert_eq!(node.sink_type, SinkType::Drop);
            }
            _ => panic!("Expected Sink"),
        }
    }

    #[test]
    fn test_checkpoint_types() {
        assert_eq!(format!("{:?}", CheckpointType::Classic), "Classic");
        assert_eq!(format!("{:?}", CheckpointType::MultiPart), "MultiPart");
        assert_eq!(format!("{:?}", CheckpointType::V2), "V2");
    }

    #[test]
    fn test_scan_phase_complete_is_terminal() {
        assert!(ScanStateMachinePhase::Complete.is_complete());
    }
}

// =============================================================================
// Integration Tests with Real Tables
// =============================================================================

mod real_table_tests {
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::arrow::array::{Array, AsArray};
    use crate::engine::sync::SyncEngine;
    use crate::plans::executor::{DeclarativePlanExecutor, FilteredEngineData};
    use crate::plans::nodes::{FileType, ScanNode, SinkNode};
    use crate::plans::DeclarativePlanNode;
    use crate::schema::{DataType, MapType, StructField, StructType};
    use crate::Engine;
    use crate::FileMeta;
    use crate::Snapshot;

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

    fn create_raw_scan_plan(commit_files: Vec<FileMeta>) -> DeclarativePlanNode {
        let partition_values_type: DataType = MapType::new(DataType::STRING, DataType::STRING, true).into();
        let add_schema = DataType::struct_type_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("partitionValues", partition_values_type),
            StructField::not_null("size", DataType::LONG),
            StructField::nullable("modificationTime", DataType::LONG),
            StructField::nullable("dataChange", DataType::BOOLEAN),
            StructField::nullable("stats", DataType::STRING),
        ]);
        let remove_schema = DataType::struct_type_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("deletionTimestamp", DataType::LONG),
            StructField::nullable("dataChange", DataType::BOOLEAN),
        ]);
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("add", add_schema),
            StructField::nullable("remove", remove_schema),
        ]));

        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::Scan(ScanNode {
                file_type: FileType::Json,
                files: commit_files,
                schema,
            })),
            node: SinkNode::results(),
        }
    }

    fn extract_add_paths(batches: &[FilteredEngineData]) -> Vec<String> {
        use crate::engine::arrow_data::extract_record_batch;

        let mut paths = Vec::new();
        for batch in batches {
            let record_batch = extract_record_batch(batch.data()).expect("ArrowEngineData");

            // Try top-level "path" first, then nested "add.path"
            if let Ok(idx) = record_batch.schema().index_of("path") {
                if let Some(arr) = record_batch.column(idx).as_string_opt::<i32>() {
                    for i in 0..arr.len() {
                        if batch.selection_vector().get(i).copied().unwrap_or(false) && !arr.is_null(i) {
                            paths.push(arr.value(i).to_string());
                        }
                    }
                    continue;
                }
            }

            if let Ok(idx) = record_batch.schema().index_of("add") {
                if let Some(add_struct) = record_batch.column(idx).as_struct_opt() {
                    if let Some(path_col) = add_struct.column_by_name("path") {
                        if let Some(arr) = path_col.as_string_opt::<i32>() {
                            for i in 0..arr.len() {
                                if batch.selection_vector().get(i).copied().unwrap_or(false) && !arr.is_null(i) {
                                    paths.push(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        paths
    }

    // Static expected data
    const EXPECTED_TABLE_WITHOUT_DV_SMALL_FILE: &str =
        "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet";

    const EXPECTED_BASIC_PARTITIONED_FILES: [&str; 6] = [
        "letter=a/part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet",
        "letter=b/part-00000-41954fb0-ef91-47e5-bd41-b75169c41c17.c000.snappy.parquet",
        "letter=c/part-00000-27a17b8f-be68-485c-9c49-70c742be30c0.c000.snappy.parquet",
        "letter=__HIVE_DEFAULT_PARTITION__/part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet",
        "letter=a/part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet",
        "letter=e/part-00000-847cf2d1-1247-4aa0-89ef-2f90c68ea51e.c000.snappy.parquet",
    ];

    #[test]
    fn test_table_without_dv_small() {
        let Some((_, table_url)) = get_test_table_path("table-without-dv-small") else {
            println!("Skipping: table-without-dv-small not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref()).expect("snapshot");

        let commit_files: Vec<FileMeta> = snapshot
            .log_segment()
            .listed.ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        assert_eq!(commit_files.len(), 1);

        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let batches: Vec<_> = executor.execute(plan).unwrap().filter_map(|r| r.ok()).collect();

        // Verify 4 rows (commitInfo, protocol, metaData, add)
        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        assert_eq!(total_rows, 4);

        // Verify add path
        let add_paths = extract_add_paths(&batches);
        assert_eq!(add_paths.len(), 1);
        assert_eq!(add_paths[0], EXPECTED_TABLE_WITHOUT_DV_SMALL_FILE);
    }

    #[test]
    fn test_basic_partitioned() {
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else {
            println!("Skipping: basic_partitioned not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref()).expect("snapshot");

        let commit_files: Vec<FileMeta> = snapshot
            .log_segment()
            .listed.ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        assert_eq!(commit_files.len(), 2);

        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let batches: Vec<_> = executor.execute(plan).unwrap().filter_map(|r| r.ok()).collect();

        // Verify 10 total rows
        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        assert_eq!(total_rows, 10);

        // Verify all 6 add paths
        let add_paths = extract_add_paths(&batches);
        assert_eq!(add_paths.len(), 6);

        let add_set: HashSet<_> = add_paths.iter().map(|s| s.as_str()).collect();
        let expected_set: HashSet<_> = EXPECTED_BASIC_PARTITIONED_FILES.iter().copied().collect();
        assert_eq!(add_set, expected_set);
    }

    #[test]
    fn test_version_row_counts() {
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else { return };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref()).expect("snapshot");

        let commit_files: Vec<FileMeta> = snapshot
            .log_segment()
            .listed.ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Version 0: 6 rows (protocol, metadata, 3 adds, commitInfo)
        let plan0 = create_raw_scan_plan(vec![commit_files[0].clone()]);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let batches0: Vec<_> = executor.execute(plan0).unwrap().filter_map(|r| r.ok()).collect();
        assert_eq!(batches0.iter().map(|b| b.data().len()).sum::<usize>(), 6);

        // Version 1: 4 rows (3 adds, commitInfo)
        let plan1 = create_raw_scan_plan(vec![commit_files[1].clone()]);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let batches1: Vec<_> = executor.execute(plan1).unwrap().filter_map(|r| r.ok()).collect();
        assert_eq!(batches1.iter().map(|b| b.data().len()).sum::<usize>(), 4);
    }
}

// =============================================================================
// Parquet Schema Detection Tests (Integration)
// =============================================================================

mod parquet_schema_tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use object_store_12::local::LocalFileSystem;
    use url::Url;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::parquet::DefaultParquetHandler;
    use crate::FileMeta;
    use crate::ParquetHandler;

    fn test_data_path(relative_path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data")
            .join(relative_path)
    }

    fn file_meta_from_path(path: &PathBuf) -> FileMeta {
        FileMeta {
            location: Url::from_file_path(path).unwrap(),
            last_modified: 0,
            size: std::fs::metadata(path).unwrap().len(),
        }
    }

    fn create_parquet_handler() -> DefaultParquetHandler<TokioBackgroundExecutor> {
        DefaultParquetHandler::new(
            Arc::new(LocalFileSystem::new()),
            Arc::new(TokioBackgroundExecutor::new()),
        )
    }

    #[test]
    fn test_classic_checkpoint_no_sidecar() {
        let path = test_data_path(
            "with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        );
        if !path.exists() {
            eprintln!("Skipping: checkpoint file not found");
            return;
        }

        let handler = create_parquet_handler();
        let footer = handler.read_parquet_footer(&file_meta_from_path(&path)).unwrap();

        assert!(footer.schema.field("sidecar").is_none());
        assert!(footer.schema.field("add").is_some());
    }

    #[test]
    fn test_multipart_checkpoint_no_sidecar() {
        let path = test_data_path(
            "parquet_row_group_skipping/_delta_log/00000000000000000001.checkpoint.0000000001.0000000005.parquet",
        );
        if !path.exists() {
            eprintln!("Skipping: checkpoint file not found");
            return;
        }

        let handler = create_parquet_handler();
        let footer = handler.read_parquet_footer(&file_meta_from_path(&path)).unwrap();

        assert!(footer.schema.field("sidecar").is_none());
    }
}
