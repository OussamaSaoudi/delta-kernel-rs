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
        let filter = FilterByKDF::add_remove_dedup();

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

        let plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF::add_remove_dedup(),
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
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
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
    fn test_log_replay_phase_to_proto() {
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
        };

        let phase = LogReplayPhase::Commit(commit_plan);

        // Convert to proto
        let proto_phase: proto::LogReplayPhase = (&phase).into();

        // Serialize to bytes
        let bytes = proto_phase.encode_to_vec();
        assert!(!bytes.is_empty());

        // Deserialize
        let decoded = proto::LogReplayPhase::decode(bytes.as_slice())
            .expect("Should decode successfully");

        // Verify it's a Commit phase with both plan and query_plan
        assert!(decoded.phase.is_some());
        match decoded.phase.unwrap() {
            proto::log_replay_phase::Phase::Commit(commit_data) => {
                assert!(commit_data.plan.is_some(), "Should have typed plan");
                assert!(commit_data.query_plan.is_some(), "Should have query plan tree");
            }
            _ => panic!("Expected Commit phase"),
        }
    }

    #[test]
    fn test_as_query_plan_produces_tree() {
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
        };

        // Get tree representation
        let tree = commit_plan.as_query_plan();

        // Verify tree structure: Select -> Filter -> Scan
        match tree {
            DeclarativePlanNode::Select { child, .. } => {
                match *child {
                    DeclarativePlanNode::FilterByKDF { child: inner, node } => {
                        // Verify it's AddRemoveDedup (variant IS the identity)
                        assert!(matches!(&node.state, FilterKdfState::AddRemoveDedup(_)));
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
            _ => panic!("Expected Select at root"),
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
        // Test convenience constructors
        let filter1 = FilterByKDF::add_remove_dedup();
        assert!(matches!(&filter1.state, FilterKdfState::AddRemoveDedup(_)));
        
        let filter2 = FilterByKDF::checkpoint_dedup();
        assert!(matches!(&filter2.state, FilterKdfState::CheckpointDedup(_)));
    }

    #[test]
    fn test_advance_error_propagates() {
        // Create a phase with typed state
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
        };
        
        let phase = LogReplayPhase::Commit(commit_plan);
        
        // Advance with error - should propagate
        let result = phase.advance(Err(crate::Error::generic("Test error")));
        assert!(result.is_err(), "Error should propagate");
    }

    #[test]
    fn test_filter_kdf_apply() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Create typed state
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Create test batch with file paths
        let schema = Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
        ]);
        let path_array = StringArray::from(vec!["file1.parquet", "file2.parquet"]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(path_array)],
        ).unwrap();
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
        let schema2 = Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
        ]);
        let path_array2 = StringArray::from(vec!["file1.parquet", "file3.parquet"]);
        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(path_array2)],
        ).unwrap();
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
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        let mut state = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        // Create batch with 100 files
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths: Vec<&str> = (0..100).map(|i| {
            Box::leak(format!("file{}.parquet", i).into_boxed_str()) as &str
        }).collect();
        let path_array = StringArray::from(paths.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(path_array)]).unwrap();
        let engine_data = ArrowEngineData::new(batch);
        
        let selection = BooleanArray::from(vec![true; 100]);
        let result = state.apply(&engine_data, selection).unwrap();
        
        // All 100 should be selected (first time)
        for i in 0..100 {
            assert!(result.value(i), "file{} should be selected on first pass", i);
        }
        
        // Now apply with same 100 files - ALL should be filtered as duplicates
        let schema2 = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let path_array2 = StringArray::from(paths);
        let batch2 = RecordBatch::try_new(Arc::new(schema2), vec![Arc::new(path_array2)]).unwrap();
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
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Create two separate states
        let mut state1 = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        let mut state2 = FilterKdfState::AddRemoveDedup(AddRemoveDedupState::new());
        
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths: Vec<&str> = vec!["shared_file.parquet"];
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(paths))],
        ).unwrap();
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

    #[test]
    fn test_advance_success_transitions() {
        // Create a phase
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
        };
        
        let phase = LogReplayPhase::Commit(commit_plan);
        
        // Create a simple plan to advance with
        let executed_plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
        });
        
        // Advance should succeed
        let next_phase = phase.advance(Ok(executed_plan)).expect("Should advance");
        
        // Should transition to Complete (our simple implementation)
        assert!(next_phase.is_complete(), "Should transition to complete");
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
    fn test_log_replay_as_declarative_phase() {
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
        };
        
        let phase = LogReplayPhase::Commit(commit_plan);
        
        // Convert to declarative phase
        let declarative = phase.as_declarative_phase();
        
        // Verify structure
        assert_eq!(declarative.operation, OperationType::LogReplay);
        assert_eq!(declarative.phase_type, PhaseType::Commit);
        assert!(declarative.query_plan.is_some());
        assert!(declarative.terminal_data.is_none());
        assert!(!declarative.is_terminal());
        assert_eq!(declarative.phase_name(), "Commit");
        assert_eq!(declarative.operation_name(), "LogReplay");
    }

    #[test]
    fn test_log_replay_complete_as_declarative_phase() {
        let phase = LogReplayPhase::Complete;
        
        // Convert to declarative phase
        let declarative = phase.as_declarative_phase();
        
        // Verify structure
        assert_eq!(declarative.operation, OperationType::LogReplay);
        assert_eq!(declarative.phase_type, PhaseType::Complete);
        assert!(declarative.query_plan.is_none());
        assert!(declarative.terminal_data.is_some());
        assert!(declarative.is_terminal());
        
        // Verify terminal data
        match declarative.terminal_data {
            Some(TerminalData::LogReplayComplete) => {}
            _ => panic!("Expected LogReplayComplete terminal data"),
        }
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
        
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF::add_remove_dedup(),
            project: SelectNode {
                columns: vec![],
                output_schema: test_schema(),
            },
        };
        
        let phase = LogReplayPhase::Commit(commit_plan);
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
        assert_eq!(decoded.operation, proto::OperationType::LogReplay as i32);
        assert_eq!(decoded.phase_type, proto::PhaseType::Commit as i32);
        assert!(decoded.query_plan.is_some());
        assert!(decoded.terminal_data.is_none());
    }
}
