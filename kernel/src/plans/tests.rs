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
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        let filter = FilterByKDF {
            function_id: KernelFunctionId::AddRemoveDedup,
            state_ptr,
            serialized_state: None,
        };

        let proto_filter: proto::FilterByKDF = (&filter).into();
        assert_eq!(proto_filter.function_id, proto::KernelFunctionId::AddRemoveDedup as i32);
        assert_eq!(proto_filter.state_ptr, state_ptr);
        assert!(proto_filter.serialized_state.is_none());
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_declarative_plan_roundtrip() {
        // Create state for the filter
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        // Build a plan: Scan -> Filter
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![],
            schema: test_schema(),
        });

        let plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
                assert_eq!(filter_node.function_id, proto::KernelFunctionId::AddRemoveDedup as i32);
                assert_eq!(filter_node.state_ptr, state_ptr);
            }
            _ => panic!("Expected Filter node"),
        }
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_commit_phase_plan_to_proto() {
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_log_replay_phase_to_proto() {
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_as_query_plan_produces_tree() {
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
                        assert_eq!(node.function_id, KernelFunctionId::AddRemoveDedup);
                        assert_eq!(node.state_ptr, state_ptr);
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
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }
}

#[cfg(test)]
mod function_registry_tests {
    use crate::plans::*;

    #[test]
    fn test_registry_lookup_all_functions() {
        // Verify all function_ids resolve correctly
        assert!(FUNCTION_REGISTRY.get(&KernelFunctionId::AddRemoveDedup).is_some());
        assert!(FUNCTION_REGISTRY.get(&KernelFunctionId::CheckpointDedup).is_some());
        assert!(FUNCTION_REGISTRY.get(&KernelFunctionId::StatsSkipping).is_some());
    }

    #[test]
    fn test_kdf_create_all_functions() {
        // Test create for each function type
        for function_id in [
            KernelFunctionId::AddRemoveDedup,
            KernelFunctionId::CheckpointDedup,
            KernelFunctionId::StatsSkipping,
        ] {
            let state_ptr = kdf_create_state(function_id).expect("Should create state");
            assert_ne!(state_ptr, 0, "State pointer should not be null");
            kdf_free(function_id, state_ptr);
        }
    }

    #[test]
    fn test_kdf_serialize_roundtrip() {
        // Create state
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        // Serialize
        let bytes = kdf_serialize(KernelFunctionId::AddRemoveDedup, state_ptr)
            .expect("Should serialize");
        assert!(!bytes.is_empty(), "Serialized bytes should not be empty");
        
        // Deserialize
        let new_state_ptr = kdf_deserialize(KernelFunctionId::AddRemoveDedup, &bytes)
            .expect("Should deserialize");
        assert_ne!(new_state_ptr, 0, "Deserialized state should not be null");
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
        kdf_free(KernelFunctionId::AddRemoveDedup, new_state_ptr);
    }

    #[test]
    fn test_advance_error_propagates() {
        // Create a phase
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
            project: SelectNode {
                columns: vec![],
                output_schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
        };
        
        let phase = LogReplayPhase::Commit(commit_plan);
        
        // Advance with error - should propagate
        let result = phase.advance(Err(crate::Error::generic("Test error")));
        assert!(result.is_err(), "Error should propagate");
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_kdf_apply_via_registry() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Create state
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
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
        
        // Apply filter via registry
        let result = kdf_apply(KernelFunctionId::AddRemoveDedup, state_ptr, &engine_data, selection)
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
        
        let result2 = kdf_apply(KernelFunctionId::AddRemoveDedup, state_ptr, &engine_data2, selection2)
            .expect("Should apply filter");
        
        // file1 is duplicate, file3 is new
        assert!(!result2.value(0), "file1 should be filtered (duplicate)");
        assert!(result2.value(1), "file3 should be selected (new)");
        
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_kdf_apply_filters_all_duplicates_in_large_batch() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        // Create batch with 100 files
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths: Vec<&str> = (0..100).map(|i| {
            // Leak strings to get &'static str (only for test)
            Box::leak(format!("file{}.parquet", i).into_boxed_str()) as &str
        }).collect();
        let path_array = StringArray::from(paths.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(path_array)]).unwrap();
        let engine_data = ArrowEngineData::new(batch);
        
        let selection = BooleanArray::from(vec![true; 100]);
        let result = kdf_apply(KernelFunctionId::AddRemoveDedup, state_ptr, &engine_data, selection).unwrap();
        
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
        let result2 = kdf_apply(KernelFunctionId::AddRemoveDedup, state_ptr, &engine_data2, selection2).unwrap();
        
        // All 100 should be filtered (duplicates)
        for i in 0..100 {
            assert!(!result2.value(i), "file{} should be filtered as duplicate", i);
        }
        
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_kdf_apply_mixed_new_and_duplicate() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        // Batch 1: Add files 0, 2, 4, 6, 8 (even numbers)
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths1: Vec<&str> = vec!["file0.parquet", "file2.parquet", "file4.parquet", "file6.parquet", "file8.parquet"];
        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(paths1))],
        ).unwrap();
        
        let result1 = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state_ptr,
            &ArrowEngineData::new(batch1),
            BooleanArray::from(vec![true; 5]),
        ).unwrap();
        
        assert_eq!(result1.true_count(), 5, "All 5 even files should be selected");
        
        // Batch 2: Mix of odd (new) and even (duplicate)
        // file1 (new), file2 (dup), file3 (new), file4 (dup), file5 (new)
        let paths2: Vec<&str> = vec!["file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet", "file5.parquet"];
        let batch2 = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(paths2))],
        ).unwrap();
        
        let result2 = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state_ptr,
            &ArrowEngineData::new(batch2),
            BooleanArray::from(vec![true; 5]),
        ).unwrap();
        
        // Expected: true, false, true, false, true (odd=new, even=dup)
        assert!(result2.value(0), "file1 should be selected (new)");
        assert!(!result2.value(1), "file2 should be filtered (duplicate)");
        assert!(result2.value(2), "file3 should be selected (new)");
        assert!(!result2.value(3), "file4 should be filtered (duplicate)");
        assert!(result2.value(4), "file5 should be selected (new)");
        
        assert_eq!(result2.true_count(), 3, "3 new files should be selected");
        assert_eq!(result2.false_count(), 2, "2 duplicates should be filtered");
        
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_kdf_apply_respects_pre_filtered_selection() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        // Create batch with 5 files, but only select rows 0, 2, 4 (pre-filter)
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths: Vec<&str> = vec!["a.parquet", "b.parquet", "c.parquet", "d.parquet", "e.parquet"];
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(paths.clone()))],
        ).unwrap();
        
        // Pre-filter: only a, c, e are selected
        let selection = BooleanArray::from(vec![true, false, true, false, true]);
        
        let result = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state_ptr,
            &ArrowEngineData::new(batch),
            selection,
        ).unwrap();
        
        // a, c, e should be selected (new), b, d should stay filtered
        assert!(result.value(0), "a should be selected");
        assert!(!result.value(1), "b should stay filtered (pre-filter)");
        assert!(result.value(2), "c should be selected");
        assert!(!result.value(3), "d should stay filtered (pre-filter)");
        assert!(result.value(4), "e should be selected");
        
        // Now apply again with same files - but b and d were never seen!
        let batch2 = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(paths))],
        ).unwrap();
        
        // All selected this time
        let selection2 = BooleanArray::from(vec![true; 5]);
        
        let result2 = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state_ptr,
            &ArrowEngineData::new(batch2),
            selection2,
        ).unwrap();
        
        // a, c, e are duplicates now; b, d are NEW (were filtered before, never seen)
        assert!(!result2.value(0), "a should be filtered (duplicate)");
        assert!(result2.value(1), "b should be selected (was pre-filtered, never seen before)");
        assert!(!result2.value(2), "c should be filtered (duplicate)");
        assert!(result2.value(3), "d should be selected (was pre-filtered, never seen before)");
        assert!(!result2.value(4), "e should be filtered (duplicate)");
        
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_kdf_apply_empty_batch() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        // Empty batch
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths: Vec<&str> = vec![];
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(paths))],
        ).unwrap();
        
        let selection = BooleanArray::from(vec![] as Vec<bool>);
        
        let result = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state_ptr,
            &ArrowEngineData::new(batch),
            selection,
        ).unwrap();
        
        assert_eq!(result.len(), 0, "Empty batch should produce empty result");
        
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }

    #[test]
    fn test_kdf_state_isolated_between_instances() {
        use crate::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType, Field, Schema};
        use crate::engine::arrow_data::ArrowEngineData;
        use std::sync::Arc;
        
        // Create two separate states
        let state1 = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        let state2 = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);
        let paths: Vec<&str> = vec!["shared_file.parquet"];
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(paths))],
        ).unwrap();
        let engine_data = ArrowEngineData::new(batch);
        
        // Apply to state1
        let result1 = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state1,
            &engine_data,
            BooleanArray::from(vec![true]),
        ).unwrap();
        assert!(result1.value(0), "state1: file should be selected (new)");
        
        // Apply same file to state2 - should ALSO be selected (different state)
        let result2 = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state2,
            &engine_data,
            BooleanArray::from(vec![true]),
        ).unwrap();
        assert!(result2.value(0), "state2: file should be selected (new in this state)");
        
        // Apply again to state1 - now it's a duplicate
        let result1_again = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state1,
            &engine_data,
            BooleanArray::from(vec![true]),
        ).unwrap();
        assert!(!result1_again.value(0), "state1: file should be filtered (duplicate)");
        
        // But state2 still only saw it once
        let result2_again = kdf_apply(
            KernelFunctionId::AddRemoveDedup,
            state2,
            &engine_data,
            BooleanArray::from(vec![true]),
        ).unwrap();
        assert!(!result2_again.value(0), "state2: file should be filtered (duplicate)");
        
        kdf_free(KernelFunctionId::AddRemoveDedup, state1);
        kdf_free(KernelFunctionId::AddRemoveDedup, state2);
    }

    #[test]
    fn test_advance_success_transitions() {
        // Create a phase
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![])),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
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
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
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
        
        let state_ptr = kdf_create_state(KernelFunctionId::AddRemoveDedup).unwrap();
        
        let commit_plan = CommitPhasePlan {
            scan: ScanNode {
                file_type: FileType::Json,
                files: vec![],
                schema: test_schema(),
            },
            data_skipping: None,
            dedup_filter: FilterByKDF {
                function_id: KernelFunctionId::AddRemoveDedup,
                state_ptr,
                serialized_state: None,
            },
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
        
        // Clean up
        kdf_free(KernelFunctionId::AddRemoveDedup, state_ptr);
    }
}


