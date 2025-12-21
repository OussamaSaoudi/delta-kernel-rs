//! Tests for plan serialization and conversion.

#[cfg(test)]
mod proto_roundtrip_tests {
    use std::sync::Arc;
    use prost::Message;

    use crate::plans::*;
    use crate::proto_generated as proto;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::Expression;

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]))
    }

    #[test]
    fn test_scan_node_to_proto() {
        let scan = ScanNode {
            file_type: FileType::Parquet,
            files: vec![],
            schema: test_schema(),
        };

        let proto_scan: proto::ScanNode = (&scan).into();
        assert_eq!(proto_scan.file_type, proto::scan_node::FileType::Parquet as i32);
    }

    #[test]
    fn test_filter_node_to_proto() {
        let (filter, _receiver) = FilterByKDF::add_remove_dedup();

        let proto_filter: proto::FilterByKdf = (&filter).into();
        // state_ptr should be non-zero (points to the typed state)
        assert_ne!(proto_filter.state_ptr, 0);
    }

    #[test]
    fn test_declarative_plan_roundtrip() {
        // Build a plan: Scan -> Filter
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

        // Convert to proto
        let proto_plan: proto::DeclarativePlanNode = (&plan).into();

        // Serialize to bytes
        let bytes = proto_plan.encode_to_vec();
        assert!(!bytes.is_empty(), "Serialized bytes should not be empty");

        // Deserialize
        let decoded = proto::DeclarativePlanNode::decode(bytes.as_slice())
            .expect("Should decode successfully");

        // Verify structure
        assert!(decoded.node.is_some());
        match decoded.node.unwrap() {
            proto::declarative_plan_node::Node::FilterByKdf(filter_plan) => {
                assert!(filter_plan.child.is_some());
                assert!(filter_plan.node.is_some());
                let filter_node = filter_plan.node.unwrap();
                assert_ne!(filter_node.state_ptr, 0);
            }
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_commit_phase_plan_to_proto() {
        let (dedup_sender, _receiver) = FilterByKDF::add_remove_dedup();
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            partition_prune_filter: None,
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
            sink: SinkNode::results(),
        };

        // Convert to proto
        let proto_plan: proto::CommitPhasePlan = (&commit_plan).into();

        // Verify
        assert!(proto_plan.scan.is_some());
        assert!(proto_plan.data_skipping.is_none());
        assert!(proto_plan.dedup_filter.is_some());
        assert!(proto_plan.project.is_some());
    }

    #[test]
    fn test_declarative_plan_to_proto_roundtrip() {
        let schema = test_schema();
        
        // Create a simple plan
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: schema.clone(),
        });

        let (dedup_sender, _receiver) = FilterByKDF::add_remove_dedup();
        let plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: dedup_sender,
        };

        // Convert to proto
        let proto_plan: proto::DeclarativePlanNode = (&plan).into();

        // Serialize to bytes
        let bytes = proto_plan.encode_to_vec();
        assert!(!bytes.is_empty());

        // Deserialize
        let decoded = proto::DeclarativePlanNode::decode(bytes.as_slice())
            .expect("Should decode successfully");

        // Verify it's a FilterByKDF with child
        assert!(decoded.node.is_some());
    }

    #[test]
    fn test_as_query_plan_produces_tree() {
        let (dedup_sender, _receiver) = FilterByKDF::add_remove_dedup();
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            partition_prune_filter: None,
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
            sink: SinkNode::results(),
        };

        // Get tree representation
        let tree = commit_plan.as_query_plan();

        // Verify tree structure: Sink -> Select -> Filter -> Scan
        match tree {
            DeclarativePlanNode::Sink { child, .. } => {
                match *child {
                    DeclarativePlanNode::Select { child, .. } => {
                        match *child {
                            DeclarativePlanNode::FilterByKDF { child: inner, node } => {
                                // Verify it's AddRemoveDedup (variant IS the identity)
                                // Use template() to access the state without locking
                                assert!(matches!(node.template(), FilterKdfState::AddRemoveDedup(_)));
                                match *inner {
                                    DeclarativePlanNode::Scan(scan) => {
                                        assert_eq!(scan.file_type, FileType::Json);
                                    }
                                    _ => panic!("Expected Scan at leaf"),
                                }
                            }
                            _ => panic!("Expected Filter after Select"),
                        }
                    }
                    _ => panic!("Expected Select after Sink"),
                }
            }
            _ => panic!("Expected Sink at root"),
        }
    }
}

#[cfg(test)]
mod typed_state_tests {
    use crate::plans::*;

    #[test]
    fn test_filter_kdf_state_create() {
        // Create typed state directly
        let state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Verify it's the right variant
        assert!(matches!(state, FilterKdfState::AddRemoveDedup(_)));
    }

    #[test]
    fn test_filter_kdf_state_raw_roundtrip() {
        let state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Convert to raw
        let ptr = state.into_raw();
        assert_ne!(ptr, 0);
        
        // Convert back
        let recovered = unsafe { FilterKdfState::from_raw(ptr) };
        assert!(matches!(recovered, FilterKdfState::AddRemoveDedup(_)));
    }

    #[test]
    fn test_filter_kdf_state_serialize_roundtrip() {
        let state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Serialize
        let bytes = state.serialize().expect("Should serialize");
        assert!(!bytes.is_empty());
        
        // Deserialize (directly on AddRemoveDedupState)
        let recovered = AddRemoveDedupState::deserialize(&bytes).expect("Should deserialize");
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_filter_by_kdf_constructors() {
        // Test convenience constructors return (sender, receiver) pairs
        let (filter1, _receiver1) = FilterByKDF::add_remove_dedup();
        // Use template() to access the template state (no locking needed)
        assert!(matches!(filter1.template(), FilterKdfState::AddRemoveDedup(_)));
        
        let (filter2, _receiver2) = FilterByKDF::checkpoint_dedup();
        assert!(matches!(filter2.template(), FilterKdfState::CheckpointDedup(_)));
    }

    #[test]
    fn test_filter_kdf_apply() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray, StructArray};
        use crate::arrow::array::Int32Array;
        use crate::arrow::datatypes::{DataType, Field, Fields, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Helper to create add struct with path column
        fn create_add_struct_batch(paths: Vec<&str>) -> RecordBatch {
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
                (
                    Arc::new(Field::new("storageType", DataType::Utf8, true)),
                    Arc::new(dv_storage) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                    Arc::new(dv_path) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, true)),
                    Arc::new(dv_offset) as Arc<dyn crate::arrow::array::Array>,
                ),
            ]);
            let add_fields = Fields::from(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("deletionVector", DataType::Struct(dv_fields.clone()), true),
            ]);
            let add_struct = StructArray::from(vec![(
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(path_array) as Arc<dyn crate::arrow::array::Array>,
            ), (
                Arc::new(Field::new("deletionVector", DataType::Struct(dv_fields), true)),
                Arc::new(dv_struct) as Arc<dyn crate::arrow::array::Array>,
            )]);
            // remove struct (all null) to satisfy AddRemoveDedupState expected leaves
            let remove_path = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_storage = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_path = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_offset = Int32Array::from(vec![None::<i32>; len]);
            let remove_dv_fields = Fields::from(vec![
                Field::new("storageType", DataType::Utf8, true),
                Field::new("pathOrInlineDv", DataType::Utf8, true),
                Field::new("offset", DataType::Int32, true),
            ]);
            let remove_dv_struct = StructArray::from(vec![
                (
                    Arc::new(Field::new("storageType", DataType::Utf8, true)),
                    Arc::new(remove_dv_storage) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                    Arc::new(remove_dv_path) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, true)),
                    Arc::new(remove_dv_offset) as Arc<dyn crate::arrow::array::Array>,
                ),
            ]);
            let remove_fields = Fields::from(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("deletionVector", DataType::Struct(remove_dv_fields.clone()), true),
            ]);
            let remove_struct = StructArray::from(vec![(
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(remove_path) as Arc<dyn crate::arrow::array::Array>,
            ), (
                Arc::new(Field::new("deletionVector", DataType::Struct(remove_dv_fields), true)),
                Arc::new(remove_dv_struct) as Arc<dyn crate::arrow::array::Array>,
            )]);
            let schema = Schema::new(vec![
                Field::new("add", DataType::Struct(add_fields), true),
                Field::new("remove", DataType::Struct(remove_fields), true),
            ]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(add_struct), Arc::new(remove_struct)]).unwrap()
        }
        
        // Create typed state
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Create test batch with file paths in add.path
        let batch = create_add_struct_batch(vec!["file1.parquet", "file2.parquet"]);
        let engine_data = ArrowEngineData::new(batch);
        
        // Create selection vector (all true)
        let selection = BooleanArray::from(vec![true, true]);
        
        // Apply filter directly via typed state
        let result = state.apply(&engine_data, selection)
            .expect("Should apply filter");
        
        // First time seeing files - both should be selected
        assert!(result.value(0), "file1 should be selected");
        assert!(result.value(1), "file2 should be selected");
        
        // Apply again with same files (duplicates) + new file
        let batch2 = create_add_struct_batch(vec!["file1.parquet", "file3.parquet"]);
        let engine_data2 = ArrowEngineData::new(batch2);
        let selection2 = BooleanArray::from(vec![true, true]);
        
        let result2 = state.apply(&engine_data2, selection2)
            .expect("Should apply filter");
        
        // file1 is duplicate, file3 is new
        assert!(!result2.value(0), "file1 should be filtered (duplicate)");
        assert!(result2.value(1), "file3 should be selected (new)");
    }

    #[test]
    fn test_filter_kdf_apply_large_batch() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray, StructArray};
        use crate::arrow::array::Int32Array;
        use crate::arrow::datatypes::{DataType, Field, Fields, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Helper to create add struct batch
        fn create_add_struct_batch(paths: Vec<String>) -> RecordBatch {
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
                (
                    Arc::new(Field::new("storageType", DataType::Utf8, true)),
                    Arc::new(dv_storage) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                    Arc::new(dv_path) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, true)),
                    Arc::new(dv_offset) as Arc<dyn crate::arrow::array::Array>,
                ),
            ]);
            let add_fields = Fields::from(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("deletionVector", DataType::Struct(dv_fields.clone()), true),
            ]);
            let add_struct = StructArray::from(vec![(
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(path_array) as Arc<dyn crate::arrow::array::Array>,
            ), (
                Arc::new(Field::new("deletionVector", DataType::Struct(dv_fields), true)),
                Arc::new(dv_struct) as Arc<dyn crate::arrow::array::Array>,
            )]);
            let remove_path = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_storage = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_path = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_offset = Int32Array::from(vec![None::<i32>; len]);
            let remove_dv_fields = Fields::from(vec![
                Field::new("storageType", DataType::Utf8, true),
                Field::new("pathOrInlineDv", DataType::Utf8, true),
                Field::new("offset", DataType::Int32, true),
            ]);
            let remove_dv_struct = StructArray::from(vec![
                (
                    Arc::new(Field::new("storageType", DataType::Utf8, true)),
                    Arc::new(remove_dv_storage) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                    Arc::new(remove_dv_path) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, true)),
                    Arc::new(remove_dv_offset) as Arc<dyn crate::arrow::array::Array>,
                ),
            ]);
            let remove_fields = Fields::from(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("deletionVector", DataType::Struct(remove_dv_fields.clone()), true),
            ]);
            let remove_struct = StructArray::from(vec![(
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(remove_path) as Arc<dyn crate::arrow::array::Array>,
            ), (
                Arc::new(Field::new("deletionVector", DataType::Struct(remove_dv_fields), true)),
                Arc::new(remove_dv_struct) as Arc<dyn crate::arrow::array::Array>,
            )]);
            let schema = Schema::new(vec![
                Field::new("add", DataType::Struct(add_fields), true),
                Field::new("remove", DataType::Struct(remove_fields), true),
            ]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(add_struct), Arc::new(remove_struct)]).unwrap()
        }
        
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Create batch with 100 files
        let paths: Vec<String> = (0..100).map(|i| format!("file{}.parquet", i)).collect();
        let batch = create_add_struct_batch(paths.clone());
        let engine_data = ArrowEngineData::new(batch);
        
        let selection = BooleanArray::from(vec![true; 100]);
        let result = state.apply(&engine_data, selection).unwrap();
        
        // All 100 should be selected (first time)
        for i in 0..100 {
            assert!(result.value(i), "file{} should be selected on first pass", i);
        }
        
        // Now apply with same 100 files - ALL should be filtered as duplicates
        let batch2 = create_add_struct_batch(paths);
        let engine_data2 = ArrowEngineData::new(batch2);
        
        let selection2 = BooleanArray::from(vec![true; 100]);
        let result2 = state.apply(&engine_data2, selection2).unwrap();
        
        // All 100 should be filtered (duplicates)
        for i in 0..100 {
            assert!(!result2.value(i), "file{} should be filtered as duplicate", i);
        }
    }

    #[test]
    fn test_filter_kdf_state_isolated_between_instances() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray, StructArray};
        use crate::arrow::array::Int32Array;
        use crate::arrow::datatypes::{DataType, Field, Fields, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Helper to create add struct batch
        fn create_add_struct_batch(paths: Vec<&str>) -> RecordBatch {
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
                (
                    Arc::new(Field::new("storageType", DataType::Utf8, true)),
                    Arc::new(dv_storage) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                    Arc::new(dv_path) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, true)),
                    Arc::new(dv_offset) as Arc<dyn crate::arrow::array::Array>,
                ),
            ]);
            let add_fields = Fields::from(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("deletionVector", DataType::Struct(dv_fields.clone()), true),
            ]);
            let add_struct = StructArray::from(vec![(
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(path_array) as Arc<dyn crate::arrow::array::Array>,
            ), (
                Arc::new(Field::new("deletionVector", DataType::Struct(dv_fields), true)),
                Arc::new(dv_struct) as Arc<dyn crate::arrow::array::Array>,
            )]);
            let remove_path = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_storage = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_path = StringArray::from(vec![None::<&str>; len]);
            let remove_dv_offset = Int32Array::from(vec![None::<i32>; len]);
            let remove_dv_fields = Fields::from(vec![
                Field::new("storageType", DataType::Utf8, true),
                Field::new("pathOrInlineDv", DataType::Utf8, true),
                Field::new("offset", DataType::Int32, true),
            ]);
            let remove_dv_struct = StructArray::from(vec![
                (
                    Arc::new(Field::new("storageType", DataType::Utf8, true)),
                    Arc::new(remove_dv_storage) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("pathOrInlineDv", DataType::Utf8, true)),
                    Arc::new(remove_dv_path) as Arc<dyn crate::arrow::array::Array>,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, true)),
                    Arc::new(remove_dv_offset) as Arc<dyn crate::arrow::array::Array>,
                ),
            ]);
            let remove_fields = Fields::from(vec![
                Field::new("path", DataType::Utf8, true),
                Field::new("deletionVector", DataType::Struct(remove_dv_fields.clone()), true),
            ]);
            let remove_struct = StructArray::from(vec![(
                Arc::new(Field::new("path", DataType::Utf8, true)),
                Arc::new(remove_path) as Arc<dyn crate::arrow::array::Array>,
            ), (
                Arc::new(Field::new("deletionVector", DataType::Struct(remove_dv_fields), true)),
                Arc::new(remove_dv_struct) as Arc<dyn crate::arrow::array::Array>,
            )]);
            let schema = Schema::new(vec![
                Field::new("add", DataType::Struct(add_fields), true),
                Field::new("remove", DataType::Struct(remove_fields), true),
            ]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(add_struct), Arc::new(remove_struct)]).unwrap()
        }
        
        // Create two separate states
        let mut state1 = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        let mut state2 = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        let batch = create_add_struct_batch(vec!["shared_file.parquet"]);
        let engine_data = ArrowEngineData::new(batch);
        
        // Apply to state1
        let result1 = state1.apply(&engine_data, BooleanArray::from(vec![true])).unwrap();
        assert!(result1.value(0), "state1: file should be selected (new)");
        
        // Apply same file to state2 - should ALSO be selected (different state)
        let result2 = state2.apply(&engine_data, BooleanArray::from(vec![true])).unwrap();
        assert!(result2.value(0), "state2: file should be selected (new in this state)");
        
        // Apply again to state1 - now it's a duplicate
        let result1_again = state1.apply(&engine_data, BooleanArray::from(vec![true])).unwrap();
        assert!(!result1_again.value(0), "state1: file should be filtered (duplicate)");
    }

}

#[cfg(test)]
mod declarative_phase_tests {
    use std::sync::Arc;
    use crate::plans::*;
    use crate::schema::{DataType, StructField, StructType};

    fn test_schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
        ]))
    }

    #[test]
    fn test_snapshot_build_as_declarative_phase() {
        let snapshot_data = SnapshotData {
            version: 42,
            table_schema: test_schema(),
        };
        
        let phase = SnapshotBuildPhase::Ready(snapshot_data);
        
        // Convert to declarative phase
        let declarative = phase.as_declarative_phase();
        
        // Verify structure
        assert_eq!(declarative.operation, OperationType::SnapshotBuild);
        assert_eq!(declarative.phase_type, PhaseType::Ready);
        assert!(declarative.query_plan.is_none());
        assert!(declarative.terminal_data.is_some());
        assert!(declarative.is_terminal());
        
        // Verify terminal data
        match declarative.terminal_data {
            Some(TerminalData::SnapshotReady { version, .. }) => {
                assert_eq!(version, 42);
            }
            _ => panic!("Expected SnapshotReady terminal data"),
        }
    }

    #[test]
    fn test_declarative_phase_proto_roundtrip() {
        use prost::Message;
        use crate::proto_generated as proto;
        use crate::plans::kdf_state::{StateSender, ConsumerKdfState, LogSegmentBuilderState};
        
        // Create a sender/receiver pair for the log segment builder
        let (_sender, receiver) = StateSender::build(ConsumerKdfState::LogSegmentBuilder(
            LogSegmentBuilderState::new(
                url::Url::parse("file:///test/_delta_log/").unwrap(),
                None, // end_version
                None, // checkpoint_hint_version
            ),
        ));
        
        // Create a SnapshotPhase with the receiver
        let phase = SnapshotPhase::ListFiles { consumer_receiver: receiver };
        let declarative = phase.as_declarative_phase();
        
        // Convert to proto
        let proto_phase: proto::DeclarativePhase = (&declarative).into();
        
        // Serialize
        let bytes = proto_phase.encode_to_vec();
        assert!(!bytes.is_empty());
        
        // Deserialize
        let decoded = proto::DeclarativePhase::decode(bytes.as_slice())
            .expect("Should decode successfully");
        
        // Verify
        assert_eq!(decoded.operation, proto::OperationType::SnapshotBuild as i32);
        assert_eq!(decoded.phase_type, proto::PhaseType::ListFiles as i32);
        // Note: query_plan is now None because plans are stored separately from phases
        assert!(decoded.query_plan.is_none());
        assert!(decoded.terminal_data.is_none());
    }
}

#[cfg(test)]
mod schema_query_tests {
    use std::sync::Arc;
    use prost::Message;
    
    use crate::plans::*;
    use crate::proto_generated as proto;
    use crate::schema::{DataType, SchemaRef, StructField, StructType};

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]))
    }

    fn schema_with_sidecar() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(
                "add",
                DataType::struct_type_unchecked(vec![
                    StructField::not_null("path", DataType::STRING),
                    StructField::not_null("size", DataType::LONG),
                ]),
            ),
            StructField::nullable(
                "sidecar",
                DataType::struct_type_unchecked(vec![
                    StructField::not_null("path", DataType::STRING),
                    StructField::not_null("sizeInBytes", DataType::LONG),
                ]),
            ),
        ]))
    }

    fn schema_without_sidecar() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable(
                "add",
                DataType::struct_type_unchecked(vec![
                    StructField::not_null("path", DataType::STRING),
                    StructField::not_null("size", DataType::LONG),
                ]),
            ),
            StructField::nullable(
                "remove",
                DataType::struct_type_unchecked(vec![
                    StructField::not_null("path", DataType::STRING),
                ]),
            ),
        ]))
    }

    #[test]
    fn test_schema_query_phase_plan_to_proto() {
        let plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store("/path/to/checkpoint.parquet"),
            sink: SinkNode::drop(),
        };

        // Convert to proto
        let proto_plan: proto::SchemaQueryPhasePlan = (&plan).into();

        // Verify
        assert!(proto_plan.schema_query.is_some());
        let schema_query = proto_plan.schema_query.unwrap();
        assert_eq!(schema_query.file_path, "/path/to/checkpoint.parquet");
    }

    #[test]
    fn test_schema_query_phase_as_query_plan() {
        let plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store("/path/to/checkpoint.parquet"),
            sink: SinkNode::drop(),
        };

        // Get tree representation
        let tree = plan.as_query_plan();

        // Verify tree structure: Sink(Drop) -> SchemaQuery
        match tree {
            DeclarativePlanNode::Sink { child, node } => {
                assert!(matches!(node.sink_type, SinkType::Drop));
                match child.as_ref() {
                    DeclarativePlanNode::SchemaQuery(schema_node) => {
                        assert_eq!(schema_node.file_path, "/path/to/checkpoint.parquet");
                        assert!(matches!(&schema_node.state, SchemaReaderState::SchemaStore(_)));
                    }
                    _ => panic!("Expected SchemaQuery as child of Sink"),
                }
            }
            _ => panic!("Expected Sink at root"),
        }
    }

    #[test]
    fn test_schema_query_plan_as_query_plan() {
        let plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store("/path/to/checkpoint.parquet"),
            sink: SinkNode::drop(),
        };

        // Get the query plan
        let query_plan = plan.as_query_plan();

        // Verify it's a Sink wrapping SchemaQuery
        match query_plan {
            DeclarativePlanNode::Sink { child, .. } => {
                match child.as_ref() {
            DeclarativePlanNode::SchemaQuery(node) => {
                assert_eq!(node.file_path, "/path/to/checkpoint.parquet");
            }
                    _ => panic!("Expected SchemaQuery node inside Sink"),
                }
            }
            _ => panic!("Expected Sink node at root"),
        }
    }

    #[test]
    fn test_schema_query_proto_roundtrip() {
        let node = SchemaQueryNode::schema_store("/path/to/checkpoint.parquet");
        
        // Convert to proto
        let proto_node: proto::SchemaQueryNode = (&node).into();
        
        // Serialize to bytes
        let bytes = proto_node.encode_to_vec();
        assert!(!bytes.is_empty());

        // Deserialize
        let decoded = proto::SchemaQueryNode::decode(bytes.as_slice())
            .expect("Should decode successfully");

        // Verify the file path
        assert_eq!(decoded.file_path, "/path/to/checkpoint.parquet");
    }

    #[test]
    fn test_schema_has_sidecar_field() {
        let with_sidecar = schema_with_sidecar();
        let without_sidecar = schema_without_sidecar();

        // Check that we can detect sidecar field
        assert!(with_sidecar.field("sidecar").is_some(), "Schema with sidecar should have sidecar field");
        assert!(without_sidecar.field("sidecar").is_none(), "Schema without sidecar should not have sidecar field");
    }

    #[test]
    fn test_schema_store_state_captures_schema() {
        let mut state = SchemaStoreState::new();
        
        // Initially no schema
        assert!(state.get().is_none());

        // Store a schema
        let schema = test_schema();
        state.store(schema.clone());

        // Verify it was stored
        assert!(state.get().is_some());
        assert_eq!(state.get().unwrap().num_fields(), 2);

        // OnceLock-based state can only be written once
        // Additional stores are ignored
    }

    #[test]
    fn test_schema_store_state_sidecar_detection() {
        let state = SchemaStoreState::new();
        
        // Store schema with sidecar
        state.store(schema_with_sidecar());
        
        // Verify we can detect sidecar column
        let schema = state.get().unwrap();
        assert!(schema.field("sidecar").is_some());

        // Test with a new state for schema without sidecar
        let state2 = SchemaStoreState::new();
        state2.store(schema_without_sidecar());
        let schema2 = state2.get().unwrap();
        assert!(schema2.field("sidecar").is_none());
    }

    #[test]
    fn test_sidecar_collector_state_basic() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = SidecarCollectorState::new(log_root);

        // Initially no sidecars
        assert!(!state.has_sidecars());
        assert_eq!(state.sidecar_count(), 0);
        assert!(!state.has_error());
    }

    #[test]
    fn test_sidecar_collector_state_from_consumer_sender() {
        use crate::plans::kdf_state::{StateSender, ConsumerKdfState, SidecarCollectorState};
        
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let (sender, _receiver) = StateSender::build(ConsumerKdfState::SidecarCollector(
            SidecarCollectorState::new(log_root),
        ));

        // Verify template is the right variant
        assert!(matches!(sender.template(), ConsumerKdfState::SidecarCollector(_)));
    }
}

/// Integration tests that read actual parquet checkpoint files
#[cfg(test)]
mod parquet_schema_detection_tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    
    use object_store::local::LocalFileSystem;
    use url::Url;
    
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::parquet::DefaultParquetHandler;
    use crate::FileMeta;
    use crate::ParquetHandler;

    /// Helper to get test data path
    fn test_data_path(relative_path: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(relative_path);
        path
    }

    /// Helper to create a FileMeta from a path
    fn file_meta_from_path(path: &PathBuf) -> FileMeta {
        let file_size = std::fs::metadata(path).unwrap().len();
        let url = Url::from_file_path(path).unwrap();
        FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
        }
    }

    /// Helper to create parquet handler
    fn create_parquet_handler() -> DefaultParquetHandler<TokioBackgroundExecutor> {
        let store = Arc::new(LocalFileSystem::new());
        DefaultParquetHandler::new(store, Arc::new(TokioBackgroundExecutor::new()))
    }

    #[test]
    fn test_classic_checkpoint_schema_has_no_sidecar() {
        // Read schema from a classic checkpoint file
        let path = test_data_path(
            "with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        );
        
        if !path.exists() {
            eprintln!("Skipping test: checkpoint file not found at {:?}", path);
            return;
        }

        let handler = create_parquet_handler();
        let file_meta = file_meta_from_path(&path);
        
        let footer = handler.read_parquet_footer(&file_meta).unwrap();
        
        // Classic checkpoints should NOT have sidecar column
        assert!(
            footer.schema.field("sidecar").is_none(),
            "Classic checkpoint should not have 'sidecar' field"
        );
        
        // But should have standard action fields
        assert!(footer.schema.field("add").is_some(), "Should have 'add' field");
    }

    #[test]
    fn test_app_txn_checkpoint_schema_has_no_sidecar() {
        // Read schema from another classic checkpoint file
        let path = test_data_path(
            "app-txn-checkpoint/_delta_log/00000000000000000001.checkpoint.parquet",
        );
        
        if !path.exists() {
            eprintln!("Skipping test: checkpoint file not found at {:?}", path);
            return;
        }

        let handler = create_parquet_handler();
        let file_meta = file_meta_from_path(&path);
        
        let footer = handler.read_parquet_footer(&file_meta).unwrap();
        
        // Classic checkpoints should NOT have sidecar column
        assert!(
            footer.schema.field("sidecar").is_none(),
            "Classic checkpoint should not have 'sidecar' field"
        );
    }

    #[test]
    fn test_multipart_checkpoint_schema_has_no_sidecar() {
        // Read schema from a multi-part checkpoint file
        let path = test_data_path(
            "parquet_row_group_skipping/_delta_log/00000000000000000001.checkpoint.0000000001.0000000005.parquet",
        );
        
        if !path.exists() {
            eprintln!("Skipping test: checkpoint file not found at {:?}", path);
            return;
        }

        let handler = create_parquet_handler();
        let file_meta = file_meta_from_path(&path);
        
        let footer = handler.read_parquet_footer(&file_meta).unwrap();
        
        // Multi-part checkpoints (v1) should NOT have sidecar column
        assert!(
            footer.schema.field("sidecar").is_none(),
            "Multi-part checkpoint should not have 'sidecar' field"
        );
    }

    #[test]
    fn test_can_read_checkpoint_and_detect_action_columns() {
        let path = test_data_path(
            "with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        );
        
        if !path.exists() {
            eprintln!("Skipping test: checkpoint file not found at {:?}", path);
            return;
        }

        let handler = create_parquet_handler();
        let file_meta = file_meta_from_path(&path);
        
        let footer = handler.read_parquet_footer(&file_meta).unwrap();
        
        // Standard checkpoint action columns
        let standard_columns = ["add", "remove", "metaData", "protocol", "txn"];
        
        for col in &standard_columns {
            // At least some of these should exist
            if footer.schema.field(col).is_some() {
                println!("Found standard column: {}", col);
            }
        }
        
        // Verify we got a schema with fields
        assert!(footer.schema.num_fields() > 0, "Schema should have fields");
    }

}

/// Tests for SidecarCollector consumer KDF with batch data
#[cfg(test)]
mod sidecar_collector_batch_tests {
    use std::sync::Arc;
    
    use crate::arrow::array::{Int64Array, RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::plans::kdf_state::SidecarCollectorState;

    fn create_sidecar_batch(paths: &[&str], sizes: &[i64]) -> ArrowEngineData {
        use crate::arrow::array::{Array, MapArray, StructArray};
        use crate::arrow::buffer::OffsetBuffer;
        use crate::arrow::datatypes::Fields;
        
        let len = paths.len();
        
        // Create the 4 leaf columns inside the sidecar struct
        let path_array = StringArray::from(paths.to_vec());
        let size_array = Int64Array::from(sizes.to_vec());
        let modification_time_array = Int64Array::from(vec![0i64; len]);
        
        // Create empty map array for tags (nullable)
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
        
        // Create the sidecar struct
        let sidecar_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("path", DataType::Utf8, false)),
                Arc::new(path_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("sizeInBytes", DataType::Int64, false)),
                Arc::new(size_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("modificationTime", DataType::Int64, false)),
                Arc::new(modification_time_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("tags", DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ), true)),
                Arc::new(tags_array) as Arc<dyn Array>,
            ),
        ]);
        
        // Create the top-level schema with the sidecar struct
        let schema = Schema::new(vec![
            Field::new("sidecar", sidecar_struct.data_type().clone(), true),
        ]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(sidecar_struct)],
        ).unwrap();
        
        ArrowEngineData::new(batch)
    }

    #[test]
    fn test_sidecar_collector_collects_files() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = SidecarCollectorState::new(log_root);

        // Create batch with sidecar paths
        let batch = create_sidecar_batch(
            &["sidecar1.parquet", "sidecar2.parquet"],
            &[1000, 2000],
        );

        // Apply
        let result = state.apply(&batch).unwrap();
        assert!(result, "Should continue processing");

        // Verify sidecars were collected
        assert!(state.has_sidecars());
        assert_eq!(state.sidecar_count(), 2);

        // Take the files and verify paths
        let files = state.take_sidecar_files();
        assert_eq!(files.len(), 2);
        assert!(files[0].location.path().contains("sidecar1.parquet"));
        assert!(files[1].location.path().contains("sidecar2.parquet"));
        assert_eq!(files[0].size, 1000);
        assert_eq!(files[1].size, 2000);
    }

    #[test]
    fn test_sidecar_collector_handles_empty_batch() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = SidecarCollectorState::new(log_root);

        // Create empty batch
        let batch = create_sidecar_batch(&[], &[]);

        // Apply
        let result = state.apply(&batch).unwrap();
        assert!(result, "Should continue processing");

        // No sidecars collected
        assert!(!state.has_sidecars());
        assert_eq!(state.sidecar_count(), 0);
    }

    #[test]
    fn test_sidecar_collector_multiple_batches() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = SidecarCollectorState::new(log_root);

        // First batch
        let batch1 = create_sidecar_batch(&["sidecar1.parquet"], &[1000]);
        let result1 = state.apply(&batch1).unwrap();
        assert!(result1);
        assert_eq!(state.sidecar_count(), 1);

        // Second batch
        let batch2 = create_sidecar_batch(&["sidecar2.parquet", "sidecar3.parquet"], &[2000, 3000]);
        let result2 = state.apply(&batch2).unwrap();
        assert!(result2);
        assert_eq!(state.sidecar_count(), 3);

        // Verify all collected
        let files = state.get_sidecar_files();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_sidecar_collector_via_consumer_kdf_state() {
        use crate::plans::kdf_state::ConsumerKdfState;

        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let mut state = ConsumerKdfState::SidecarCollector(SidecarCollectorState::new(log_root));

        // Create batch
        let batch = create_sidecar_batch(&["sidecar1.parquet"], &[1000]);

        // Apply through ConsumerKdfState
        let result = state.apply(&batch).unwrap();
        assert!(result);

        // Check no error
        assert!(!state.has_error());

        // Finalize
        state.finalize();
    }
}

/// Tests for state machine transitions with SchemaQuery phase
#[cfg(test)]
mod state_machine_transition_tests {
    use std::sync::Arc;
    use url::Url;
    
    use crate::plans::*;
    use crate::schema::{DataType, StructField, StructType};
    use crate::FileMeta;

    fn create_test_checkpoint_file() -> FileMeta {
        FileMeta {
            location: Url::parse("file:///test/_delta_log/00000000000000000001.checkpoint.parquet").unwrap(),
            last_modified: 0,
            size: 1000,
        }
    }

    #[test]
    fn test_scan_phase_complete_is_terminal() {
        // Verify Complete is terminal
        assert!(ScanStateMachinePhase::Complete.is_complete());
        // Complete phase has no associated plan - this is verified by the enum structure
    }

    #[test]
    fn test_scan_phase_schema_query_has_drop_sink() {
        // SchemaQuery should have a Drop sink
        let schema_query_plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store("test.parquet"),
            sink: SinkNode::drop(),
        };
        
        let plan = schema_query_plan.as_query_plan();
        
        // SchemaQuery produces a Sink(Drop) -> SchemaQuery
        assert!(
            plan.is_drop_sink(),
            "SchemaQuery phase should have a Drop sink"
        );
        assert!(!plan.is_results_sink(), "SchemaQuery should not be a Results sink");
    }

    #[test]
    fn test_scan_phase_checkpoint_manifest_has_drop_sink() {
        use crate::plans::kdf_state::{StateSender, ConsumerKdfState, SidecarCollectorState};
        
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
        
        // Verify it's a Drop sink (not Results)
        assert!(!plan.is_results_sink(), "CheckpointManifest should have a Drop sink, not Results");
        
        // Verify it IS a Sink node
        match plan {
            DeclarativePlanNode::Sink { node, .. } => {
                assert_eq!(node.sink_type, SinkType::Drop, "CheckpointManifest should have Drop sink type");
            }
            _ => panic!("CheckpointManifest plan should be wrapped in a Sink node"),
        }
    }

    #[test]
    fn test_scan_phase_checkpoint_leaf_has_results_sink() {
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
        
        // Verify it's a Results sink
        assert!(plan.is_results_sink(), "CheckpointLeaf should have a Results sink");
        
        // Verify the sink type
        match plan {
            DeclarativePlanNode::Sink { node, .. } => {
                assert_eq!(node.sink_type, SinkType::Results, "CheckpointLeaf should have Results sink type");
            }
            _ => panic!("CheckpointLeaf plan should be wrapped in a Sink node"),
        }
    }

    #[test]
    fn test_checkpoint_type_enum() {
        // Verify CheckpointType variants exist
        let classic = CheckpointType::Classic;
        let multi_part = CheckpointType::MultiPart;
        let v2 = CheckpointType::V2;

        assert_eq!(format!("{:?}", classic), "Classic");
        assert_eq!(format!("{:?}", multi_part), "MultiPart");
        assert_eq!(format!("{:?}", v2), "V2");
    }

    // =========================================================================
    // Content Verification Tests
    // =========================================================================

    #[test]
    fn test_checkpoint_leaf_phase_plan_contents() {
        use crate::FileMeta;
        
        let checkpoint_file = FileMeta {
            location: Url::parse("file:///test/table/_delta_log/00000000000000000010.checkpoint.parquet").unwrap(),
            last_modified: 1234567890,
            size: 10000,
        };
        
        let (dedup_sender, _receiver) = FilterByKDF::checkpoint_dedup();
        let leaf_plan = CheckpointLeafPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: vec![checkpoint_file.clone()],
                schema: Arc::new(StructType::new_unchecked(vec![
                    StructField::nullable("add", DataType::struct_type_unchecked(vec![
                        StructField::not_null("path", DataType::STRING),
                        StructField::not_null("size", DataType::LONG),
                    ])),
                ])),
            },
            partition_prune_filter: None,
            dedup_filter: dedup_sender,
            project: SelectNode {
                columns: vec![],
                output_schema: Arc::new(StructType::new_unchecked(vec![
                    StructField::nullable("path", DataType::STRING),
                    StructField::nullable("size", DataType::LONG),
                ])),
            },
            sink: SinkNode::results(),
        };
        
        let plan = leaf_plan.as_query_plan();
        
        // Verify plan structure: Sink(Results) -> Select -> FilterByKDF(CheckpointDedup) -> Scan(Parquet)
        match plan {
            DeclarativePlanNode::Sink { child, node: sink_node } => {
                // 1. Verify Results sink
                assert_eq!(sink_node.sink_type, SinkType::Results, "CheckpointLeaf must have Results sink");
                
                match *child {
                    DeclarativePlanNode::Select { child: filter_box, node: select_node } => {
                        // 2. Verify output schema
                        assert!(select_node.output_schema.field("path").is_some());
                        assert!(select_node.output_schema.field("size").is_some());
                        
                        match *filter_box {
                            DeclarativePlanNode::FilterByKDF { child: scan_box, node: filter_node } => {
                                // 3. Verify CheckpointDedup filter using template()
                                assert!(
                                    matches!(filter_node.template(), FilterKdfState::CheckpointDedup(_)),
                                    "CheckpointLeaf must use CheckpointDedup filter"
                                );
                                
                                match *scan_box {
                                    DeclarativePlanNode::Scan(scan_node) => {
                                        // 4. Verify Parquet file type for checkpoints
                                        assert_eq!(scan_node.file_type, FileType::Parquet, "Checkpoint files are Parquet");
                                        
                                        // 5. Verify checkpoint files are included
                                        assert_eq!(scan_node.files.len(), 1);
                                        assert_eq!(
                                            scan_node.files[0].location.as_str(),
                                            checkpoint_file.location.as_str()
                                        );
                                        
                                        // 6. Verify schema has add field
                                        assert!(scan_node.schema.field("add").is_some());
                                    }
                                    _ => panic!("Expected Scan at leaf"),
                                }
                            }
                            _ => panic!("Expected FilterByKDF"),
                        }
                    }
                    _ => panic!("Expected Select"),
                }
            }
            _ => panic!("Expected Sink at root"),
        }
    }

    #[test]
    fn test_checkpoint_manifest_phase_plan_contents() {
        use crate::plans::kdf_state::{StateSender, ConsumerKdfState, SidecarCollectorState};
        
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let (sender, _receiver) = StateSender::build(ConsumerKdfState::SidecarCollector(
            SidecarCollectorState::new(log_root),
        ));
        let manifest_plan = CheckpointManifestPlan {
            scan: ScanNode {
                file_type: FileType::Parquet,
                files: vec![],
                schema: Arc::new(StructType::new_unchecked(vec![
                    StructField::not_null("sidecar", DataType::struct_type_unchecked(vec![
                        StructField::not_null("path", DataType::STRING),
                        StructField::not_null("sizeInBytes", DataType::LONG),
                    ])),
                ])),
            },
            project: SelectNode {
                columns: vec![],
                output_schema: Arc::new(StructType::new_unchecked(vec![
                    StructField::not_null("path", DataType::STRING),
                    StructField::not_null("sizeInBytes", DataType::LONG),
                ])),
            },
            sidecar_collector: sender,
            sink: SinkNode::drop(),
        };
        
        let plan = manifest_plan.as_query_plan();
        
        // Verify plan structure: Sink(Drop) -> ConsumeByKDF -> Select -> Scan(Parquet)
        match plan {
            DeclarativePlanNode::Sink { child, node: sink_node } => {
                // 1. Verify Drop sink (side effects only)
                assert_eq!(sink_node.sink_type, SinkType::Drop, "Manifest must have Drop sink");
                
                match *child {
                    DeclarativePlanNode::ConsumeByKDF { child: select_box, .. } => {
                        // ConsumeByKDF collects sidecar files
                        match *select_box {
                            DeclarativePlanNode::Select { child: scan_box, node: select_node } => {
                                // 2. Verify output schema has path and sizeInBytes
                                assert!(select_node.output_schema.field("path").is_some());
                                assert!(select_node.output_schema.field("sizeInBytes").is_some());
                                
                                match *scan_box {
                                    DeclarativePlanNode::Scan(scan_node) => {
                                        // 3. Verify Parquet file type
                                        assert_eq!(scan_node.file_type, FileType::Parquet);
                                        
                                        // 4. Verify schema has sidecar field
                                        assert!(
                                            scan_node.schema.field("sidecar").is_some(),
                                            "Manifest schema should have 'sidecar' field"
                                        );
                                    }
                                    _ => panic!("Expected Scan at leaf"),
                                }
                            }
                            _ => panic!("Expected Select after ConsumeByKDF"),
                        }
                    }
                    _ => panic!("Expected ConsumeByKDF after Sink"),
                }
            }
            _ => panic!("Expected Sink at root"),
        }
    }

    #[test]
    fn test_schema_query_phase_plan_contents() {
        let file_path = "file:///test/table/_delta_log/00000000000000000005.checkpoint.parquet";
        
        let schema_plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store(file_path),
            sink: SinkNode::drop(),
        };
        
        let plan = schema_plan.as_query_plan();
        
        // SchemaQuery produces Sink(Drop) -> SchemaQuery
        match plan {
            DeclarativePlanNode::Sink { child, node } => {
                assert!(matches!(node.sink_type, SinkType::Drop), "Should be Drop sink");
                match child.as_ref() {
                    DeclarativePlanNode::SchemaQuery(schema_node) => {
                        // 1. Verify file path
                        assert_eq!(schema_node.file_path, file_path, "File path should match");
                        
                        // 2. Verify it's a SchemaStore state
                        match &schema_node.state {
                            SchemaReaderState::SchemaStore(store) => {
                                // State should be empty initially (no schema stored yet)
                                assert!(store.get().is_none(), "Initial schema store should be empty");
                            }
                        }
                    }
                    _ => panic!("Expected SchemaQuery inside Sink"),
                }
            }
            _ => panic!("Expected Sink at root"),
        }
    }

}

// =============================================================================
// Integration Tests with Real Tables - Plan Execution
// =============================================================================

/// Tests that EXECUTE ScanStateMachine plans and verify the results against static expected data.
#[cfg(test)]
mod real_table_execution_tests {
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use std::sync::Arc;
    
    use crate::arrow::array::{Array, AsArray};
    use crate::engine::sync::SyncEngine;
    use crate::expressions::ExpressionRef;
    use crate::plans::executor::{DeclarativePlanExecutor, FilteredEngineData, ResultsDriver};
    use crate::plans::state_machines::{AdvanceResult, ScanStateMachine};
    use crate::plans::*;
    use crate::scan::state::DvInfo;
    use crate::scan::{Scan, ScanMetadata};
    use crate::schema::StructType;
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

    /// Helper to extract files from a scan node in a plan
    fn extract_scan_files(plan: &DeclarativePlanNode) -> Vec<FileMeta> {
        match plan {
            DeclarativePlanNode::Scan(s) => s.files.clone(),
            DeclarativePlanNode::Sink { child, .. }
            | DeclarativePlanNode::Select { child, .. }
            | DeclarativePlanNode::FilterByKDF { child, .. }
            | DeclarativePlanNode::ConsumeByKDF { child, .. }
            | DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. } => extract_scan_files(child),
            DeclarativePlanNode::FileListing(_) | DeclarativePlanNode::SchemaQuery(_) => vec![],
        }
    }

    // =========================================================================
    // Static Expected Data - Hardcoded from Delta Log Files
    // =========================================================================
    
    /// Static expected data for table-without-dv-small
    /// From: 00000000000000000000.json containing 1 add action
    mod expected_table_without_dv_small {
        /// Expected data file path from the add action
        pub(super) const EXPECTED_DATA_FILE: &str = 
            "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet";
        
        /// Expected number of rows in the commit log (commitInfo, protocol, metaData, add)
        #[allow(dead_code)]
        pub(super) const EXPECTED_COMMIT_ROWS: usize = 4;
    }
    
    /// Static expected data for basic_partitioned table
    /// From: 00000000000000000000.json (3 adds) + 00000000000000000001.json (3 adds)
    mod expected_basic_partitioned {
        /// Expected data file paths from all add actions across both versions
        pub(super) const EXPECTED_DATA_FILES: [&str; 6] = [
            // Version 0 adds:
            "letter=a/part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet",
            "letter=b/part-00000-41954fb0-ef91-47e5-bd41-b75169c41c17.c000.snappy.parquet",
            "letter=c/part-00000-27a17b8f-be68-485c-9c49-70c742be30c0.c000.snappy.parquet",
            // Version 1 adds:
            "letter=__HIVE_DEFAULT_PARTITION__/part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet",
            "letter=a/part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet",
            "letter=e/part-00000-847cf2d1-1247-4aa0-89ef-2f90c68ea51e.c000.snappy.parquet",
        ];
        
        /// Expected number of rows in version 0 commit (protocol, metaData, 3 adds, commitInfo)
        pub(super) const EXPECTED_VERSION_0_ROWS: usize = 6;
        
        /// Expected number of rows in version 1 commit (3 adds, commitInfo)  
        pub(super) const EXPECTED_VERSION_1_ROWS: usize = 4;
        
        /// Total expected rows across both commits
        pub(super) const EXPECTED_TOTAL_COMMIT_ROWS: usize = 10;
    }
    
    // =========================================================================
    // Helper Functions for Extracting Data from Execution Results
    // =========================================================================
    
    /// Extract the 'path' column values from add actions in executed batches.
    /// The path field can be at:
    /// - Top level "path" (after transform using SCAN_ROW_SCHEMA)
    /// - Nested "add.path" (before transform, raw commit data)
    fn extract_add_paths_from_batches(batches: &[FilteredEngineData]) -> Vec<String> {
        use crate::engine::arrow_data::extract_record_batch;
        
        let mut paths = Vec::new();
        
        for batch in batches {
            // Use the helper to extract the RecordBatch from the EngineData
            let record_batch = extract_record_batch(batch.data())
                .expect("Should be ArrowEngineData");
            
            // First try top-level "path" column (after transform)
            if let Some(path_idx) = record_batch.schema().index_of("path").ok() {
                let path_column = record_batch.column(path_idx);
                if let Some(string_array) = path_column.as_string_opt::<i32>() {
                    for i in 0..string_array.len() {
                        // Only include rows that are selected
                        if batch.selection_vector().get(i).copied().unwrap_or(false)
                            && !string_array.is_null(i)
                        {
                            paths.push(string_array.value(i).to_string());
                        }
                    }
                    continue; // Found top-level path, skip nested check
                }
            }
            
            // Fallback: Look for the "add" struct column, then extract "path" from it
            if let Some(add_idx) = record_batch.schema().index_of("add").ok() {
                let add_column = record_batch.column(add_idx);
                if let Some(add_struct) = add_column.as_struct_opt() {
                    if let Some(path_col) = add_struct.column_by_name("path") {
                        if let Some(string_array) = path_col.as_string_opt::<i32>() {
                            for i in 0..string_array.len() {
                                // Only include rows that are selected
                                if batch.selection_vector().get(i).copied().unwrap_or(false)
                                    && !string_array.is_null(i)
                                {
                                    paths.push(string_array.value(i).to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        paths
    }
    
    // =========================================================================
    // Ground Truth Comparison Helpers
    // =========================================================================
    
    /// Get ground truth file paths using Scan::scan_metadata
    /// This uses the existing, well-tested Scan infrastructure to get the authoritative
    /// list of files that should be included in a scan.
    fn get_groundtruth_paths(snapshot: crate::snapshot::SnapshotRef, engine: &dyn Engine) -> Vec<String> {
        let scan = snapshot.scan_builder().build().expect("Should build scan");
        let scan_metadata_iter = scan.scan_metadata(engine).expect("Should get scan metadata");
        
        fn scan_metadata_callback(
            paths: &mut Vec<String>,
            path: &str,
            _size: i64,
            _stats: Option<crate::scan::state::Stats>,
            _dv_info: DvInfo,
            _transform: Option<ExpressionRef>,
            _partition_values: HashMap<String, String>,
        ) {
            paths.push(path.to_string());
        }
        
        let mut paths = vec![];
        for res in scan_metadata_iter {
            let scan_metadata = res.expect("Scan metadata should succeed");
            paths = scan_metadata.visit_scan_files(paths, scan_metadata_callback)
                .expect("visit_scan_files should succeed");
        }
        paths
    }
    
    /// Maximum iterations for state machine execution to detect infinite loops
    const MAX_DRIVER_ITERATIONS: usize = 1000;
    
    
    // =========================================================================
    // Plan Execution Tests with Static Expected Data Comparison
    // =========================================================================

    #[test]
    fn test_execute_and_verify_table_without_dv_small() {
        // Execute raw scan on table-without-dv-small and compare against static expected data
        let Some((_, table_url)) = get_test_table_path("table-without-dv-small") else {
            println!("Skipping test: table-without-dv-small not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment
            .ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Verify we have exactly 1 commit file
        assert_eq!(commit_files.len(), 1, "Expected exactly 1 commit file");
        assert!(
            commit_files[0].location.path().ends_with("00000000000000000000.json"),
            "Expected version 0 commit file"
        );
        
        // Execute raw scan plan
        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();
        
        // STATIC ASSERTION 1: Verify row count (4 lines in the commit file)
        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        assert_eq!(total_rows, 4, "Raw scan should have 4 rows from the commit file");
        
        // STATIC ASSERTION 2: Verify the add action path
        let add_paths = extract_add_paths_from_batches(&batches);
        assert_eq!(add_paths.len(), 1, "Expected exactly 1 add action");
        assert_eq!(
            add_paths[0],
            expected_table_without_dv_small::EXPECTED_DATA_FILE,
            "Add path mismatch: expected '{}', got '{}'",
            expected_table_without_dv_small::EXPECTED_DATA_FILE,
            add_paths[0]
        );
    }

    /// Helper to create a simple Scan -> Sink plan for reading commit files
    fn create_raw_scan_plan(commit_files: Vec<FileMeta>) -> DeclarativePlanNode {
        use crate::schema::{DataType, MapType, StructField, StructType};
        use crate::plans::nodes::{FileType, ScanNode, SinkNode};
        
        let partition_values_type: DataType =
            MapType::new(DataType::STRING, DataType::STRING, true).into();
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
        
        let raw_scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: commit_files,
            schema,
        });
        
        DeclarativePlanNode::Sink {
            child: Box::new(raw_scan),
            node: SinkNode::results(),
        }
    }

    #[test]
    fn test_execute_and_verify_basic_partitioned() {
        // Execute raw scan on basic_partitioned and compare against static expected data
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else {
            println!("Skipping test: basic_partitioned not found");
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment
            .ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Verify we have exactly 2 commit files
        assert_eq!(commit_files.len(), 2, "Expected exactly 2 commit files");

        // Execute raw scan plan
        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();

        // STATIC ASSERTION 1: Verify total row count matches expected
        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        assert_eq!(
            total_rows, 
            expected_basic_partitioned::EXPECTED_TOTAL_COMMIT_ROWS,
            "Expected {} total commit rows, got {}",
            expected_basic_partitioned::EXPECTED_TOTAL_COMMIT_ROWS,
            total_rows
        );

        // STATIC ASSERTION 2: Verify all add action paths match expected
        let add_paths = extract_add_paths_from_batches(&batches);
        let add_paths_set: HashSet<&str> = add_paths.iter().map(|s| s.as_str()).collect();
        let expected_set: HashSet<&str> = expected_basic_partitioned::EXPECTED_DATA_FILES
            .iter()
            .copied()
            .collect();
        
        assert_eq!(
            add_paths.len(), 
            expected_basic_partitioned::EXPECTED_DATA_FILES.len(),
            "Expected {} add actions, got {}",
            expected_basic_partitioned::EXPECTED_DATA_FILES.len(),
            add_paths.len()
        );
        
        assert_eq!(
            add_paths_set, 
            expected_set,
            "Add paths mismatch.\nExpected: {:?}\nGot: {:?}",
            expected_set,
            add_paths_set
        );
    }

    #[test]
    fn test_execute_verifies_exact_row_count() {
        // Verify execution produces exactly the expected number of rows
        let Some((_, table_url)) = get_test_table_path("table-without-dv-small") else {
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment
            .ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Execute raw scan plan
        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();

        // EXACT static assertion - the commit has exactly 4 lines
        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        assert_eq!(
            total_rows, 
            4,  // Static: commitInfo, protocol, metaData, add
            "Delta log 00000000000000000000.json must have exactly 4 rows"
        );
    }

    #[test]
    fn test_execute_add_path_exact_match() {
        // Verify the exact add.path value matches our static expectation
        let Some((_, table_url)) = get_test_table_path("table-without-dv-small") else {
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment
            .ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Execute raw scan plan
        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();

        let add_paths = extract_add_paths_from_batches(&batches);
        
        // EXACT STRING MATCH against static expected value
        const EXPECTED_PATH: &str = "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet";
        
        assert_eq!(add_paths.len(), 1, "Must have exactly 1 add action");
        assert_eq!(
            add_paths[0], 
            EXPECTED_PATH,
            "Add path must exactly match: expected '{}', got '{}'",
            EXPECTED_PATH,
            add_paths[0]
        );
    }

    #[test]
    fn test_execute_basic_partitioned_all_paths_match() {
        // Verify all 6 add paths from basic_partitioned match static expectations exactly
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else {
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment
            .ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Execute raw scan plan
        let plan = create_raw_scan_plan(commit_files);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();

        let add_paths = extract_add_paths_from_batches(&batches);
        
        // Static expected paths - every single path must be present
        const EXPECTED_PATHS: [&str; 6] = [
            "letter=a/part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet",
            "letter=b/part-00000-41954fb0-ef91-47e5-bd41-b75169c41c17.c000.snappy.parquet",
            "letter=c/part-00000-27a17b8f-be68-485c-9c49-70c742be30c0.c000.snappy.parquet",
            "letter=__HIVE_DEFAULT_PARTITION__/part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet",
            "letter=a/part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet",
            "letter=e/part-00000-847cf2d1-1247-4aa0-89ef-2f90c68ea51e.c000.snappy.parquet",
        ];
        
        assert_eq!(add_paths.len(), 6, "Must have exactly 6 add actions");
        
        let add_set: HashSet<String> = add_paths.into_iter().collect();
        for expected in EXPECTED_PATHS {
            assert!(
                add_set.contains(expected),
                "Missing expected path: '{}'",
                expected
            );
        }
    }

    #[test]
    fn test_commit_file_produces_correct_action_count() {
        // Test that each commit file produces the correct number of actions
        let Some((_, table_url)) = get_test_table_path("basic_partitioned") else {
            return;
        };

        let engine = create_test_engine();
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .expect("Should build snapshot");

        let log_segment = snapshot.log_segment();
        let commit_files: Vec<FileMeta> = log_segment
            .ascending_commit_files
            .iter()
            .map(|f| f.location.clone())
            .collect();

        // Test version 0 only (should have 6 lines: protocol, metadata, 3 adds, commitInfo)
        let plan = create_raw_scan_plan(vec![commit_files[0].clone()]);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();

        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        
        // Static assertion: version 0 has exactly 6 lines
        assert_eq!(
            total_rows, 
            expected_basic_partitioned::EXPECTED_VERSION_0_ROWS,
            "Version 0 must have exactly {} rows (protocol, metadata, 3 adds, commitInfo)",
            expected_basic_partitioned::EXPECTED_VERSION_0_ROWS
        );

        // Test version 1 only (should have 4 lines: 3 adds, commitInfo)
        let plan = create_raw_scan_plan(vec![commit_files[1].clone()]);
        let executor = DeclarativePlanExecutor::new(engine.as_ref());
        let results = executor.execute(plan).expect("Execution should succeed");
        let batches: Vec<FilteredEngineData> = results
            .filter_map(|r| r.ok())
            .collect();

        let total_rows: usize = batches.iter().map(|b| b.data().len()).sum();
        
        // Static assertion: version 1 has exactly 4 lines
        assert_eq!(
            total_rows, 
            expected_basic_partitioned::EXPECTED_VERSION_1_ROWS,
            "Version 1 must have exactly {} rows (3 adds, commitInfo)",
            expected_basic_partitioned::EXPECTED_VERSION_1_ROWS
        );
    }

    // =========================================================================
    // Full State Machine Execution Tests (via ResultsDriver)
    // =========================================================================

}
