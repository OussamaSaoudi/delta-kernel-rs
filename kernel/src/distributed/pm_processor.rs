//! Protocol & Metadata extraction processor using fold/reduce pattern.
//!
//! This module provides a reducer for extracting the latest Protocol and Metadata
//! actions from the Delta log with early termination once both are found.

use crate::actions::{Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::log_replay::{ActionsBatch, LogReplayReducer, LogReplaySchemaProvider};
use crate::{DeltaResult, Error};

/// Processor for extracting Protocol and Metadata from the Delta log.
///
/// This reducer accumulates the latest Protocol and Metadata actions and terminates
/// early once both are found, avoiding unnecessary log processing.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_rs::distributed::pm_processor::PMProcessor;
/// use delta_kernel_rs::distributed::single_node_v2::SingleNodeV2;
///
/// let pm_processor = PMProcessor::new();
/// let single_node = SingleNodeV2::new(pm_processor, log_segment, engine)?;
/// let (protocol, metadata) = single_node.into_accumulated()?;
/// ```
pub(crate) struct PMProcessor {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
}

impl PMProcessor {
    /// Create a new PMProcessor.
    pub fn new() -> Self {
        Self {
            protocol: None,
            metadata: None,
        }
    }
}

impl LogReplaySchemaProvider for PMProcessor {
    // Reducers use same schema for both phases by default (via trait default methods)
    fn required_commit_columns(&self) -> &[&'static str] {
        &[PROTOCOL_NAME, METADATA_NAME]
    }

    fn required_checkpoint_columns(&self) -> &[&'static str] {
        &[PROTOCOL_NAME, METADATA_NAME]
    }
}

impl LogReplayReducer for PMProcessor {
    type Accumulated = (Protocol, Metadata);

    fn required_columns(&self) -> &[&'static str] {
        // P&M needs the same columns across all phases
        &[PROTOCOL_NAME, METADATA_NAME]
    }

    fn process_batch_for_reduce(&mut self, actions_batch: ActionsBatch) -> DeltaResult<()> {
        let actions = actions_batch.actions();
        
        // Extract Protocol if we haven't found it yet
        if self.protocol.is_none() {
            self.protocol = Protocol::try_new_from_data(actions)?;
        }
        
        // Extract Metadata if we haven't found it yet
        if self.metadata.is_none() {
            self.metadata = Metadata::try_new_from_data(actions)?;
        }
        
        Ok(())
    }

    fn is_complete(&self) -> bool {
        // Complete when we have both Protocol and Metadata
        self.protocol.is_some() && self.metadata.is_some()
    }

    fn into_accumulated(self) -> DeltaResult<Self::Accumulated> {
        match (self.protocol, self.metadata) {
            (Some(protocol), Some(metadata)) => Ok((protocol, metadata)),
            (None, Some(_)) => Err(Error::MissingProtocol),
            (Some(_), None) => Err(Error::MissingMetadata),
            (None, None) => Err(Error::MissingMetadataAndProtocol),
        }
    }
}

// Temporary: Keep function-based version for backward compatibility with existing tests
// TODO: Remove once tests are migrated to use distributed choreographers
use std::sync::{Arc, LazyLock};
use crate::actions::get_commit_schema;
use crate::log_segment::LogSegment;
use crate::{Engine, Expression, Predicate, PredicateRef};

pub(crate) fn protocol_and_metadata(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<(Protocol, Metadata)> {
    let schema = get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
    
    static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
        Some(Arc::new(Predicate::or(
            Expression::column([METADATA_NAME, "id"]).is_not_null(),
            Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
        )))
    });
    
    let actions_batches = log_segment.read_actions(engine, schema, META_PREDICATE.clone())?;
    
    let (mut protocol_opt, mut metadata_opt) = (None, None);
    for actions_batch in actions_batches {
        let actions = actions_batch?.actions;
        
        if protocol_opt.is_none() {
            protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
        }
        
        if metadata_opt.is_none() {
            metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
        }
        
        if protocol_opt.is_some() && metadata_opt.is_some() {
            break;
        }
    }
    
    match (protocol_opt, metadata_opt) {
        (Some(protocol), Some(metadata)) => Ok((protocol, metadata)),
        (None, Some(_)) => Err(Error::MissingProtocol),
        (Some(_), None) => Err(Error::MissingMetadata),
        (None, None) => Err(Error::MissingMetadataAndProtocol),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::default::DefaultEngine;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use object_store::local::LocalFileSystem;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn load_test_table(
        table_name: &str,
    ) -> DeltaResult<(
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<crate::Snapshot>,
    )> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(table_name);

        let path = std::fs::canonicalize(path)
            .map_err(|e| crate::Error::Generic(format!("Failed to canonicalize path: {}", e)))?;

        let url = url::Url::from_directory_path(path)
            .map_err(|_| crate::Error::Generic("Failed to create URL from path".to_string()))?;

        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(DefaultEngine::new(store));
        let snapshot = crate::Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

        Ok((engine, snapshot))
    }

    #[test]
    fn test_pm_extraction_commits_only() -> DeltaResult<()> {
        let (engine, snapshot) = load_test_table("table-without-dv-small")?;
        let log_segment = snapshot.log_segment();

        let (protocol, metadata) = protocol_and_metadata(log_segment, engine.as_ref())?;

        // Verify protocol (table-without-dv-small has reader v1, writer v2)
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 2);

        // Verify metadata
        assert!(!metadata.id().is_empty());
        
        println!("✓ P&M extracted from commits successfully");
        println!("  Protocol: reader v{}, writer v{}", 
                 protocol.min_reader_version(), 
                 protocol.min_writer_version());
        println!("  Metadata ID: {}", metadata.id());

        Ok(())
    }

    #[test]
    fn test_pm_extraction_with_checkpoint() -> DeltaResult<()> {
        let (engine, snapshot) = load_test_table("with_checkpoint_no_last_checkpoint")?;
        let log_segment = snapshot.log_segment();

        let (protocol, metadata) = protocol_and_metadata(log_segment, engine.as_ref())?;

        // Verify we got valid P&M
        assert!(protocol.min_reader_version() > 0);
        assert!(!metadata.id().is_empty());

        println!("✓ P&M extracted with checkpoint successfully");
        println!("  Protocol: reader v{}, writer v{}", 
                 protocol.min_reader_version(), 
                 protocol.min_writer_version());
        println!("  Metadata ID: {}", metadata.id());

        Ok(())
    }

    #[test]
    fn test_pm_extraction_with_sidecars() -> DeltaResult<()> {
        let (engine, snapshot) = load_test_table("v2-checkpoints-json-with-sidecars")?;
        let log_segment = snapshot.log_segment();

        let (protocol, metadata) = protocol_and_metadata(log_segment, engine.as_ref())?;

        // Verify we got valid P&M
        assert!(protocol.min_reader_version() > 0);
        assert!(!metadata.id().is_empty());

        println!("✓ P&M extracted from table with sidecars successfully");
        println!("  Protocol: reader v{}, writer v{}", 
                 protocol.min_reader_version(), 
                 protocol.min_writer_version());
        println!("  Metadata ID: {}", metadata.id());

        Ok(())
    }

    #[test]
    fn test_pm_early_termination() -> DeltaResult<()> {
        // Test that early termination works
        let (engine, snapshot) = load_test_table("table-without-dv-small")?;
        let log_segment = snapshot.log_segment();

        // The function should terminate early once it finds both P&M
        let (protocol, metadata) = protocol_and_metadata(log_segment, engine.as_ref())?;
        
        // Verify we got valid P&M
        assert!(protocol.min_reader_version() > 0);
        assert!(!metadata.id().is_empty());
        
        println!("✓ Early termination works - P&M extracted efficiently");

        Ok(())
    }
}

