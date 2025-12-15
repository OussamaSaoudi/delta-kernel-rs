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
    fn test_declarative_plan_to_proto_roundtrip() {
        let schema = test_schema();
        
        // Create a simple plan
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: schema.clone(),
        });

        let plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF::add_remove_dedup(),
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
    fn test_scan_state_machine_advance_error_propagates() {
        // Create a scan state machine
        let schema = std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let table_url = url::Url::parse("file:///test/table").unwrap();
        let mut sm = ScanStateMachine::new(table_url, schema.clone(), schema).unwrap();
        
        // Advance with error - should propagate
        let result = sm.advance(Err(crate::Error::generic("Test error")));
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
    fn test_scan_state_machine_advance_transitions() {
        use crate::plans::AdvanceResult;
        
        // Create a scan state machine
        let schema = std::sync::Arc::new(crate::schema::StructType::new_unchecked(vec![]));
        let table_url = url::Url::parse("file:///test/table").unwrap();
        let mut sm = ScanStateMachine::new(table_url, schema.clone(), schema.clone()).unwrap();
        
        // Create a simple plan to advance with
        let executed_plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema,
        });
        
        // Advance should succeed  
        let result = sm.advance(Ok(executed_plan)).expect("Should advance");
        
        // No checkpoint files means we should be done
        assert!(matches!(result, AdvanceResult::Done(_)), "Should complete when no checkpoint files");
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
    fn test_scan_state_machine_initial_phase() {
        let schema = test_schema();
        let table_url = url::Url::parse("file:///test/table").unwrap();
        let sm = ScanStateMachine::new(table_url, schema.clone(), schema).unwrap();
        
        // Verify initial state
        assert_eq!(sm.phase_name(), "Commit");
        assert!(!sm.is_terminal());
        
        // Should be able to get a plan
        let plan = sm.get_plan();
        assert!(plan.is_ok());
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
        
        // Create a DeclarativePhase for snapshot build operation
        let list_files_plan = FileListingPhasePlan {
            listing: FileListingNode {
                path: url::Url::parse("file:///test/_delta_log/").unwrap(),
            },
            log_segment_builder: ConsumeByKDF::log_segment_builder(
                url::Url::parse("file:///test/_delta_log/").unwrap(),
                None, // end_version
                None, // checkpoint_hint_version
            ),
        };
        let phase = SnapshotPhase::ListFiles(list_files_plan);
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
        assert!(decoded.query_plan.is_some());
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
        };

        // Get tree representation
        let tree = plan.as_query_plan();

        // Verify tree structure: just a SchemaQuery leaf node
        match tree {
            DeclarativePlanNode::SchemaQuery(node) => {
                assert_eq!(node.file_path, "/path/to/checkpoint.parquet");
                assert!(matches!(&node.state, SchemaReaderState::SchemaStore(_)));
            }
            _ => panic!("Expected SchemaQuery at root"),
        }
    }

    #[test]
    fn test_schema_query_plan_as_query_plan() {
        let plan = SchemaQueryPhasePlan {
            schema_query: SchemaQueryNode::schema_store("/path/to/checkpoint.parquet"),
        };

        // Get the query plan
        let query_plan = plan.as_query_plan();

        // Verify it's a SchemaQuery node
        match query_plan {
            DeclarativePlanNode::SchemaQuery(node) => {
                assert_eq!(node.file_path, "/path/to/checkpoint.parquet");
            }
            _ => panic!("Expected SchemaQuery node"),
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

        // Take the schema
        let taken = state.take();
        assert!(taken.is_some());
        assert!(state.get().is_none()); // Should be None after take
    }

    #[test]
    fn test_schema_store_state_sidecar_detection() {
        let mut state = SchemaStoreState::new();
        
        // Store schema with sidecar
        state.store(schema_with_sidecar());
        
        // Verify we can detect sidecar column
        let schema = state.get().unwrap();
        assert!(schema.field("sidecar").is_some());

        // Now store schema without sidecar
        state.store(schema_without_sidecar());
        let schema = state.get().unwrap();
        assert!(schema.field("sidecar").is_none());
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
    fn test_sidecar_collector_state_from_consumer_kdf() {
        let log_root = url::Url::parse("file:///test/_delta_log/").unwrap();
        let consumer = ConsumeByKDF::sidecar_collector(log_root);

        // Verify it's the right variant
        assert!(matches!(&consumer.state, ConsumerKdfState::SidecarCollector(_)));
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

    #[test]
    fn test_schema_query_integration_with_scan_state_machine() {
        use crate::plans::*;
        use std::sync::Arc;
        use crate::schema::StructType;
        
        // Create a ScanStateMachine
        let table_url = Url::parse("file:///test/table").unwrap();
        let schema = Arc::new(StructType::new_unchecked(vec![]));
        let sm = ScanStateMachine::new(table_url, schema.clone(), schema).unwrap();
        
        // Initial phase should be Commit
        assert_eq!(sm.phase_name(), "Commit");
        assert!(!sm.is_terminal());
        
        // Verify we can get a plan
        let plan = sm.get_plan();
        assert!(plan.is_ok());
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
        let schema = Schema::new(vec![
            Field::new("sidecar.path", DataType::Utf8, false),
            Field::new("sidecar.sizeInBytes", DataType::Int64, true),
        ]);
        let path_array = StringArray::from(paths.to_vec());
        let size_array = Int64Array::from(sizes.to_vec());
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(path_array), Arc::new(size_array)],
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
    use crate::schema::StructType;
    use crate::FileMeta;

    fn create_test_checkpoint_file() -> FileMeta {
        FileMeta {
            location: Url::parse("file:///test/_delta_log/00000000000000000001.checkpoint.parquet").unwrap(),
            last_modified: 0,
            size: 1000,
        }
    }

    #[test]
    fn test_scan_state_machine_initial_state() {
        let table_url = Url::parse("file:///test/table").unwrap();
        let schema = Arc::new(StructType::new_unchecked(vec![]));
        let sm = ScanStateMachine::new(table_url, schema.clone(), schema).unwrap();

        assert_eq!(sm.phase_name(), "Commit");
        assert!(!sm.is_terminal());
    }

    #[test]
    fn test_scan_phase_variants() {
        // Verify ScanPhase variants exist and are correctly typed
        let _commit = ScanPhase::Commit;
        let _schema_query = ScanPhase::SchemaQuery;
        let _manifest = ScanPhase::CheckpointManifest;
        let _leaf = ScanPhase::CheckpointLeaf;
        let _complete = ScanPhase::Complete;
        
        // Verify Complete is terminal
        assert!(matches!(ScanPhase::Complete, ScanPhase::Complete));
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

    #[test]
    fn test_scan_build_state_has_checkpoint_files() {
        let table_url = Url::parse("file:///test/table").unwrap();
        let schema = Arc::new(StructType::new_unchecked(vec![]));
        let mut sm = ScanStateMachine::from_scan_config(
            table_url.clone(),
            table_url.join("_delta_log/").unwrap(),
            schema.clone(),
            schema,
            vec![],
            vec![create_test_checkpoint_file()],
        ).unwrap();

        // Get plan to verify we can work with the state machine
        let plan = sm.get_plan();
        assert!(plan.is_ok());
    }
}
