//! Workload runners for executing benchmarks.
//!
//! Runners encapsulate the logic for executing a specific workload type. They handle
//! setup, execution, and cleanup phases of benchmark runs.

use crate::benchmarks::models::{WorkloadSpecType, WorkloadSpecVariant};
use crate::snapshot::Snapshot;
use crate::Engine;

use std::sync::Arc;

/// Trait for workload runners that can execute benchmarks.
///
/// Runners follow a three-phase lifecycle:
/// 1. `setup()` - Prepare any state needed for execution (e.g., load snapshot)
/// 2. `execute()` - Run the actual workload being benchmarked
/// 3. `cleanup()` - Clean up any resources (usually a no-op for read workloads)
pub trait WorkloadRunner {
    /// Set up any state needed for benchmark execution.
    /// 
    /// This is called once before benchmark iterations begin.
    fn setup(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    
    /// Execute the workload being benchmarked.
    /// 
    /// This is the hot path that will be timed by Criterion.
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>>;
    
    /// Clean up any state created during execution.
    /// 
    /// This is called after benchmark iterations complete.
    fn cleanup(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    
    /// Get the name of this workload for reporting purposes
    fn name(&self) -> String;
}

/// Runner for read_metadata workloads.
///
/// Measures the time to read scan metadata (list of files to read) from a Delta table.
pub struct ReadMetadataRunner {
    spec: WorkloadSpecVariant,
    engine: Arc<dyn Engine>,
    snapshot: Option<Arc<Snapshot>>,
}

impl ReadMetadataRunner {
    /// Create a new ReadMetadataRunner from a spec and engine
    pub fn new(spec: WorkloadSpecVariant, engine: Arc<dyn Engine>) -> Self {
        Self {
            spec,
            engine,
            snapshot: None,
        }
    }
}

impl WorkloadRunner for ReadMetadataRunner {
    fn setup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let table_root = self.spec.table_info.resolved_table_root();
        let url = crate::try_parse_uri(&table_root)?;
        
        // Get version from the Read spec
        let version = match &self.spec.spec_type {
            WorkloadSpecType::Read(read_spec) => read_spec.version,
            _ => unreachable!("ReadMetadataRunner should only be used with Read specs"),
        };
        
        // Build snapshot (potentially at a specific version)
        let mut builder = Snapshot::builder_for(url);
        if let Some(version) = version {
            builder = builder.at_version(version);
        }
        let snapshot = builder.build(self.engine.as_ref())?;
        
        self.snapshot = Some(snapshot);
        Ok(())
    }
    
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = self.snapshot.as_ref()
            .ok_or("Snapshot not initialized - call setup() first")?;
        
        // Build and execute scan
        let scan = snapshot.clone().scan_builder().build()?;
        let metadata_iter = scan.scan_metadata(self.engine.as_ref())?;
        
        // Consume the iterator to perform actual work
        for result in metadata_iter {
            let _ = result?;
        }
        
        Ok(())
    }
    
    fn cleanup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Read-only workload, no cleanup needed
        Ok(())
    }
    
    fn name(&self) -> String {
        self.spec.full_name()
    }
}

/// Runner for snapshot_construction workloads.
///
/// Measures the time to construct a Snapshot object for a Delta table.
pub struct SnapshotConstructionRunner {
    spec: WorkloadSpecVariant,
    engine: Arc<dyn Engine>,
}

impl SnapshotConstructionRunner {
    /// Create a new SnapshotConstructionRunner from a spec and engine
    pub fn new(spec: WorkloadSpecVariant, engine: Arc<dyn Engine>) -> Self {
        Self { spec, engine }
    }
}

impl WorkloadRunner for SnapshotConstructionRunner {
    fn setup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // No setup needed for snapshot construction benchmarks
        Ok(())
    }
    
    fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let table_root = self.spec.table_info.resolved_table_root();
        let url = crate::try_parse_uri(&table_root)?;
        
        // Get version from the SnapshotConstruction spec
        let version = match &self.spec.spec_type {
            WorkloadSpecType::SnapshotConstruction(snapshot_spec) => snapshot_spec.version,
            _ => unreachable!("SnapshotConstructionRunner should only be used with SnapshotConstruction specs"),
        };
        
        // Build snapshot - this is what we're benchmarking
        let mut builder = Snapshot::builder_for(url);
        if let Some(version) = version {
            builder = builder.at_version(version);
        }
        let _snapshot = builder.build(self.engine.as_ref())?;
        
        Ok(())
    }
    
    fn cleanup(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Read-only workload, no cleanup needed
        Ok(())
    }
    
    fn name(&self) -> String {
        self.spec.full_name()
    }
}

/// Create a workload runner from a workload spec variant.
///
/// This is a factory function that creates the appropriate runner type based on
/// the workload specification.
pub fn create_runner(
    spec: WorkloadSpecVariant,
    engine: Arc<dyn Engine>,
) -> Result<Box<dyn WorkloadRunner>, Box<dyn std::error::Error>> {
    use crate::benchmarks::models::{ReadOperationType, WorkloadSpecType};
    
    match &spec.spec_type {
        WorkloadSpecType::Read(read_spec) => {
            // Check operation type
            let operation_type = read_spec.operation_type
                .ok_or("ReadSpec must have operation_type set")?;
            
            match operation_type {
                ReadOperationType::ReadMetadata => {
                    Ok(Box::new(ReadMetadataRunner::new(spec, engine)))
                }
                ReadOperationType::ReadData => {
                    Err("read_data operation not yet implemented".into())
                }
            }
        }
        WorkloadSpecType::SnapshotConstruction(_) => {
            Ok(Box::new(SnapshotConstructionRunner::new(spec, engine)))
        }
    }
}

