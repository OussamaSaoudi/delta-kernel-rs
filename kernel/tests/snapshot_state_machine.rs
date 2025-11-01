//! End-to-end and integration tests for Snapshot state machine.
//!
//! This test suite verifies that:
//! 1. The state machine approach produces identical results to the current implementation
//! 2. All phases transition correctly
//! 3. The state machine can handle various table configurations

use std::sync::Arc;

use delta_kernel::{
    engine::default::executor::tokio::TokioBackgroundExecutor,
    engine::default::DefaultEngine,
    kernel_df::{DefaultPlanExecutor, PhysicalPlanExecutor},
    state_machine::StateMachinePhase,
    Engine, Snapshot,
};
use itertools::Itertools;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use serde_json::json;
use url::Url;

/// Setup a test table with protocol and metadata
fn setup_test_table() -> (
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Arc<InMemory>,
    Url,
) {
    let table_root = Url::parse("memory:///test_table").unwrap();
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngine::new(
        store.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    create_basic_table(store.as_ref()).unwrap();

    (engine, store, table_root)
}

/// Create a basic Delta table with protocol and metadata in commit 0
fn create_basic_table(store: &InMemory) -> Result<(), Box<dyn std::error::Error>> {
    let protocol = json!({
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["columnMapping"],
        "writerFeatures": ["columnMapping"],
    });

    let metadata = json!({
        "id": "test-snapshot-sm-id",
        "format": {
            "provider": "parquet",
            "options": {}
        },
        "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 1587968585495i64
    });

    let commit0 = [json!({"protocol": protocol}), json!({"metaData": metadata})];

    let commit0_data = commit0
        .iter()
        .map(ToString::to_string)
        .collect_vec()
        .join("\n");

    let path = object_store::path::Path::from("_delta_log/00000000000000000000.json");
    futures::executor::block_on(async { store.put(&path, commit0_data.into()).await })?;

    Ok(())
}

/// Create a table with multiple commits
fn create_table_with_commits(
    store: &InMemory,
    num_commits: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    create_basic_table(store)?;

    for i in 1..num_commits {
        let commit = [json!({
            "add": {
                "path": format!("part-{:05}.parquet", i),
                "partitionValues": {},
                "size": 1024 * i as i64,
                "modificationTime": 1587968586000i64 + i as i64,
                "dataChange": true,
            }
        })];

        let commit_data = commit
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");

        let path = object_store::path::Path::from(format!("_delta_log/{:020}.json", i).as_str());
        futures::executor::block_on(async { store.put(&path, commit_data.into()).await })?;
    }

    Ok(())
}

/// Helper function to execute a state machine to completion
fn execute_state_machine(
    engine: Arc<dyn Engine>,
    mut phase: StateMachinePhase<Snapshot>,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let executor = DefaultPlanExecutor {
        engine: engine.clone(),
    };

    loop {
        phase = match phase {
            StateMachinePhase::Terminus(snapshot) => {
                return Ok(snapshot.into());
            }
            StateMachinePhase::PartialResult(_) => {
                panic!("Snapshot construction should not have PartialResult phases");
            }
            StateMachinePhase::Consume(consume_phase) => {
                let plan = consume_phase.get_plan()?;
                let data_iter = executor.execute(plan)?;

                // Convert FilteredEngineDataArc to RecordBatch
                // Handle FileNotFound errors gracefully (e.g., missing _last_checkpoint file)
                let batches_result = data_iter
                    .map(|result| {
                        result.and_then(|fed| {
                            use delta_kernel::engine::arrow_data::extract_record_batch;
                            extract_record_batch(fed.engine_data.as_ref()).cloned()
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("Failed batches result {e}"));

                let batches = match batches_result {
                    Ok(batches) => batches,
                    Err(e)
                        if e.to_string().contains("File not found")
                            || e.to_string().contains("NotFound") =>
                    {
                        // Missing file (e.g., _last_checkpoint) - treat as empty
                        Vec::new()
                    }
                    Err(e) => return Err(e.into()),
                };

                let batch_iter = Box::new(batches.into_iter().map(Ok));
                consume_phase.next(batch_iter)?
            }
        };
    }
}

#[test]
fn test_snapshot_state_machine_basic() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _store, table_root) = setup_test_table();

    let traditional = Snapshot::builder_for(table_root.clone())
        .build(engine.as_ref())
        .map_err(|e| format!("Failed normal {e}"))?;
    let initial_phase = Snapshot::builder_for(table_root.clone())
        .into_state_machine()
        .map_err(|e| format!("Failed state machine {e}"))?;
    let state_machine = execute_state_machine(engine.clone(), initial_phase)
        .map_err(|e| format!("Failed execute state machine{e}"))?;

    // Compare versions
    assert_eq!(state_machine.version(), traditional.version());

    // Compare table roots
    assert_eq!(state_machine.table_root(), traditional.table_root());

    // Compare protocols (full struct comparison)
    assert_eq!(
        state_machine.protocol(),
        traditional.protocol(),
        "Protocols should match"
    );

    // Compare metadata (full struct comparison)
    assert_eq!(
        state_machine.metadata(),
        traditional.metadata(),
        "Metadata should match"
    );

    // Compare schemas
    assert_eq!(state_machine.schema(), traditional.schema());

    println!("✓ Basic snapshot state machine test passed!");
    println!("  Version: {}", state_machine.version());
    println!("  Protocol: {:?}", state_machine.protocol());
    println!("  Metadata: {:?}", state_machine.metadata());
    Ok(())
}

#[test]
fn test_snapshot_state_machine_multiple_commits() -> Result<(), Box<dyn std::error::Error>> {
    let table_root = Url::parse("memory:///test_table_commits").unwrap();
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngine::new(
        store.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    create_table_with_commits(store.as_ref(), 5)?;

    let traditional = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let initial_phase = Snapshot::builder_for(table_root.clone()).into_state_machine()?;
    let state_machine = execute_state_machine(engine.clone(), initial_phase)?;

    assert_eq!(state_machine.version(), traditional.version());
    assert_eq!(state_machine.version(), 4);

    println!("✓ Multiple commits test passed!");
    Ok(())
}

#[test]
fn test_snapshot_state_machine_at_specific_version() -> Result<(), Box<dyn std::error::Error>> {
    let table_root = Url::parse("memory:///test_table_version").unwrap();
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngine::new(
        store.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    create_table_with_commits(store.as_ref(), 5)?;

    let traditional = Snapshot::builder_for(table_root.clone())
        .at_version(2)
        .build(engine.as_ref())?;

    let initial_phase = Snapshot::builder_for(table_root.clone())
        .at_version(2)
        .into_state_machine()?;

    let state_machine = execute_state_machine(engine.clone(), initial_phase)?;

    assert_eq!(state_machine.version(), 2);
    assert_eq!(state_machine.version(), traditional.version());

    println!("✓ Specific version test passed!");
    Ok(())
}

#[test]
fn test_state_machine_phase_transitions() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _store, table_root) = setup_test_table();

    let executor = DefaultPlanExecutor {
        engine: engine.clone(),
    };

    let initial_phase = Snapshot::builder_for(table_root.clone()).into_state_machine()?;

    let mut phase_count = 0;

    // Phase 1: CheckpointHintPhase
    let phase1 = match initial_phase {
        StateMachinePhase::Consume(consume_phase) => {
            phase_count += 1;
            println!("✓ Phase {}: CheckpointHintPhase", phase_count);

            let plan = consume_phase.get_plan()?;
            let data_iter = executor.execute(plan)?;
            let batches_result = data_iter
                .map(|result| {
                    result.and_then(|fed| {
                        use delta_kernel::engine::arrow_data::extract_record_batch;
                        extract_record_batch(fed.engine_data.as_ref()).map(|rb| rb.clone())
                    })
                })
                .collect::<Result<Vec<_>, _>>();
            
            let batches = match batches_result {
                Ok(batches) => batches,
                Err(e)
                    if e.to_string().contains("File not found")
                        || e.to_string().contains("NotFound") =>
                {
                    // Missing file (e.g., _last_checkpoint) - treat as empty
                    Vec::new()
                }
                Err(e) => return Err(e.into()),
            };
            
            println!("  Collected {} batches", batches.len());
            let batch_iter = Box::new(batches.into_iter().map(Ok));

            consume_phase.next(batch_iter)?
        }
        _ => panic!("Expected Consume phase"),
    };

    // Phase 2: ListingPhase
    let phase2 = match phase1 {
        StateMachinePhase::Consume(consume_phase) => {
            phase_count += 1;
            println!("✓ Phase {}: ListingPhase", phase_count);

            let plan = consume_phase.get_plan()?;
            let data_iter = executor.execute(plan)?;
            let batches_result = data_iter
                .map(|result| {
                    result.and_then(|fed| {
                        use delta_kernel::engine::arrow_data::extract_record_batch;
                        extract_record_batch(fed.engine_data.as_ref()).map(|rb| rb.clone())
                    })
                })
                .collect::<Result<Vec<_>, _>>();
            
            let batches = match batches_result {
                Ok(batches) => batches,
                Err(e)
                    if e.to_string().contains("File not found")
                        || e.to_string().contains("NotFound") =>
                {
                    // Missing file (e.g., _last_checkpoint) - treat as empty
                    Vec::new()
                }
                Err(e) => return Err(e.into()),
            };
            
            println!("  Collected {} batches", batches.len());
            let batch_iter = Box::new(batches.into_iter().map(Ok));

            consume_phase.next(batch_iter)?
        }
        _ => panic!("Expected Consume phase"),
    };

    // Phase 3: ProtocolMetadataPhase
    let final_phase = match phase2 {
        StateMachinePhase::Consume(consume_phase) => {
            phase_count += 1;
            println!("✓ Phase {}: ProtocolMetadataPhase", phase_count);

            let plan = consume_phase.get_plan()?;
            let data_iter = executor.execute(plan)?;
            let batches_result = data_iter
                .map(|result| {
                    result.and_then(|fed| {
                        use delta_kernel::engine::arrow_data::extract_record_batch;
                        extract_record_batch(fed.engine_data.as_ref()).map(|rb| rb.clone())
                    })
                })
                .collect::<Result<Vec<_>, _>>();
            
            let batches = match batches_result {
                Ok(batches) => batches,
                Err(e)
                    if e.to_string().contains("File not found")
                        || e.to_string().contains("NotFound") =>
                {
                    // Missing file (e.g., _last_checkpoint) - treat as empty
                    Vec::new()
                }
                Err(e) => return Err(e.into()),
            };
            
            println!("  Collected {} batches", batches.len());
            let batch_iter = Box::new(batches.into_iter().map(Ok));

            consume_phase.next(batch_iter)?
        }
        _ => panic!("Expected Consume phase"),
    };

    // Final: Should be Terminus with Snapshot
    match final_phase {
        StateMachinePhase::Terminus(snapshot) => {
            phase_count += 1;
            println!("✓ Phase {}: Terminus with Snapshot", phase_count);
            println!("  Snapshot version: {}", snapshot.version());
            assert_eq!(snapshot.version(), 0);
        }
        _ => panic!("Expected Terminus phase"),
    }

    assert_eq!(phase_count, 4, "Should have exactly 4 phases");
    println!("✓ All {} phase transitions succeeded!", phase_count);
    Ok(())
}

#[test]
fn test_state_machine_vs_traditional_consistency() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing consistency across 10 different table configurations...");

    for i in 1..=10 {
        let table_root = Url::parse(&format!("memory:///consistency_test_{}", i)).unwrap();
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngine::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));

        create_table_with_commits(store.as_ref(), i)?;

        let traditional = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
        let initial_phase = Snapshot::builder_for(table_root.clone()).into_state_machine()?;
        let state_machine = execute_state_machine(engine.clone(), initial_phase)?;

        // Full comparison
        assert_eq!(state_machine.version(), traditional.version());
        assert_eq!(state_machine.protocol(), traditional.protocol());
        assert_eq!(state_machine.metadata(), traditional.metadata());
        assert_eq!(state_machine.schema(), traditional.schema());

        println!(
            "  ✓ Configuration {}: {} commits, version {}",
            i,
            i,
            state_machine.version()
        );
    }

    println!("✓ State machine consistent with traditional approach across 10 configurations!");
    Ok(())
}

#[test]
fn test_state_machine_empty_table() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _store, table_root) = setup_test_table();

    let initial_phase = Snapshot::builder_for(table_root.clone()).into_state_machine()?;
    let snapshot = execute_state_machine(engine.clone(), initial_phase)?;

    assert_eq!(snapshot.version(), 0);
    println!("✓ Empty table test passed!");
    Ok(())
}

#[test]
fn test_state_machine_large_table() -> Result<(), Box<dyn std::error::Error>> {
    let table_root = Url::parse("memory:///test_large_table").unwrap();
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngine::new(
        store.clone(),
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    create_table_with_commits(store.as_ref(), 100)?;

    let traditional = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    let initial_phase = Snapshot::builder_for(table_root.clone()).into_state_machine()?;
    let state_machine = execute_state_machine(engine.clone(), initial_phase)?;

    assert_eq!(state_machine.version(), 99);
    assert_eq!(state_machine.version(), traditional.version());

    println!("✓ Large table (100 commits) test passed!");
    Ok(())
}
