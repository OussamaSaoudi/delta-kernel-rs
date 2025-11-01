//! State machine phases for incremental Snapshot building.
//!
//! Snapshot building is decomposed into three Consume phases:
//! 1. CheckpointHintPhase - Reads _last_checkpoint file (optional)
//! 2. ListingPhase - Lists files in _delta_log/
//! 3. ProtocolMetadataPhase - Reads Protocol+Metadata from files → Terminus(Snapshot)

use crate::{
    DeltaResult, Error, FileMeta, Snapshot,
    kernel_df::LogicalPlanNode,
    listed_log_files::ListedLogFiles,
    log_segment::LogSegment,
    state_machine::{ConsumePhase, StateMachinePhase},
    table_configuration::TableConfiguration,
    Version,
};
use crate::arrow::array::Array; // For is_null method
use std::sync::Arc;
use url::Url;

/// Phase 1: Read _last_checkpoint hint file (if exists)
pub struct CheckpointHintPhase {
    table_root: Url,
    log_root: Url,
    version: Option<Version>,
    log_tail: Vec<crate::path::ParsedLogPath>,
}

impl CheckpointHintPhase {
    pub fn new(
        table_root: Url,
        log_root: Url,
        version: Option<Version>,
        log_tail: Vec<crate::path::ParsedLogPath>,
    ) -> Self {
        Self {
            table_root,
            log_root,
            version,
            log_tail,
        }
    }
}

impl ConsumePhase for CheckpointHintPhase {
    type Output = Snapshot;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        // Read _last_checkpoint file
        let checkpoint_path = self.log_root.join("_last_checkpoint")?;
        let file_meta = FileMeta {
            location: checkpoint_path,
            last_modified: 0,
            size: 0, // Size doesn't matter for reading
        };
        
        // Schema for _last_checkpoint file
        use crate::schema::{DataType, StructField, StructType};
        let checkpoint_schema = Arc::new(StructType::new_unchecked([
            StructField::new("version", DataType::LONG, false),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("parts", DataType::INTEGER),
            StructField::nullable("sizeInBytes", DataType::LONG),
            StructField::nullable("numOfAddFiles", DataType::LONG),
        ]));
        
        LogicalPlanNode::scan_json(vec![file_meta], checkpoint_schema)
    }
    
    fn next(
        self: Box<Self>,
        mut data: Box<dyn Iterator<Item = DeltaResult<crate::arrow::array::RecordBatch>> + Send>
    ) -> DeltaResult<StateMachinePhase<Self::Output>> {
        use crate::last_checkpoint_hint::LastCheckpointHint;
        use crate::arrow::array::AsArray;
        
        // Take first batch (checkpoint hint file is small, single batch)
        // If the file doesn't exist or is empty, treat as no checkpoint hint
        let checkpoint_hint = match data.next() {
            None => None,
            Some(Err(e)) => {
                // If file not found, that's OK - just means no checkpoint hint
                if e.to_string().contains("FileNotFound") || e.to_string().contains("NotFound") {
                    None
                } else {
                    return Err(e);
                }
            }
            Some(Ok(rb)) if rb.num_rows() == 0 => None,
            Some(Ok(rb)) => {
                // Parse checkpoint hint from data
                // Extract version from first row
                let version_col = rb.column_by_name("version")
                    .ok_or_else(|| Error::generic("Missing version column"))?;
                let version_array = version_col.as_primitive::<crate::arrow::datatypes::Int64Type>();
                
                if !version_array.is_null(0) {
                let version = version_array.value(0) as u64;
                
                // Extract optional fields
                let size = rb.column_by_name("size")
                    .and_then(|col| {
                        let arr = col.as_primitive::<crate::arrow::datatypes::Int64Type>();
                        if !arr.is_null(0) { Some(arr.value(0)) } else { None }
                    })
                    .unwrap_or(0);
                
                let parts = rb.column_by_name("parts")
                    .and_then(|col| {
                        let arr = col.as_primitive::<crate::arrow::datatypes::Int32Type>();
                        if !arr.is_null(0) { Some(arr.value(0) as i64) } else { None }
                    });
                
                let size_in_bytes = rb.column_by_name("sizeInBytes")
                    .and_then(|col| {
                        let arr = col.as_primitive::<crate::arrow::datatypes::Int64Type>();
                        if !arr.is_null(0) { Some(arr.value(0) as u64) } else { None }
                    });
                
                let num_of_add_files = rb.column_by_name("numOfAddFiles")
                    .and_then(|col| {
                        let arr = col.as_primitive::<crate::arrow::datatypes::Int64Type>();
                        if !arr.is_null(0) { Some(arr.value(0)) } else { None }
                    });
                
                Some(LastCheckpointHint {
                    version,
                    size,
                    parts: parts.map(|p| p as usize),
                    size_in_bytes: size_in_bytes.map(|s| s as i64),
                    num_of_add_files,
                    checkpoint_schema: None,
                    checksum: None,
                })
            } else {
                None
            }
        }
    };
        
        Ok(StateMachinePhase::Consume(Box::new(ListingPhase {
            table_root: self.table_root,
            log_root: self.log_root,
            version: self.version,
            log_tail: self.log_tail,
            checkpoint_hint,
        })))
    }
}

/// Phase 2: List files in _delta_log/ directory
pub struct ListingPhase {
    table_root: Url,
    log_root: Url,
    version: Option<Version>,
    log_tail: Vec<crate::path::ParsedLogPath>,
    checkpoint_hint: Option<crate::last_checkpoint_hint::LastCheckpointHint>,
}

impl ConsumePhase for ListingPhase {
    type Output = Snapshot;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        use crate::kernel_df::FileListingNode;
        
        // Determine path to start listing from
        let path = if let Some(hint) = &self.checkpoint_hint {
            // Start listing from checkpoint version (as a prefix)
            // This will list all files >= this version
            self.log_root.join(&format!("{:020}", hint.version))?
        } else {
            // List entire directory from version 0
            self.log_root.join("00000000000000000000")?
        };
        
        Ok(LogicalPlanNode::FileListing(FileListingNode { path }))
    }
    
    fn next(
        self: Box<Self>,
        data: Box<dyn Iterator<Item = DeltaResult<crate::arrow::array::RecordBatch>> + Send>
    ) -> DeltaResult<StateMachinePhase<Self::Output>> {
        use crate::arrow::array::AsArray;
        use crate::path::ParsedLogPath;
        
        // Collect all file metas from all batches
        let mut file_metas = vec![];
        
        for batch_result in data {
            let rb = batch_result?;
            
            // Extract file paths from Arrow data
            let location_col = rb.column_by_name("location")
                .ok_or_else(|| Error::generic("Missing location column"))?;
            let last_modified_col = rb.column_by_name("last_modified")
                .ok_or_else(|| Error::generic("Missing last_modified column"))?;
            let size_col = rb.column_by_name("size")
                .ok_or_else(|| Error::generic("Missing size column"))?;
            
            let location_array = location_col.as_string::<i32>();
            let last_modified_array = last_modified_col.as_primitive::<crate::arrow::datatypes::Int64Type>();
            let size_array = size_col.as_primitive::<crate::arrow::datatypes::Int64Type>();
            
            for i in 0..rb.num_rows() {
                if !location_array.is_null(i) {
                    let location = location_array.value(i);
                    let last_modified = last_modified_array.value(i);
                    let size = size_array.value(i) as u64;
                    
                    file_metas.push(FileMeta {
                        location: Url::parse(location)?,
                        last_modified,
                        size,
                    });
                }
            }
        }
        
        // Parse file names into ParsedLogPath
        let mut parsed_paths = vec![];
        for file_meta in file_metas {
            if let Ok(Some(parsed)) = ParsedLogPath::try_from(file_meta) {
                // Filter by version if specified
                if let Some(max_version) = self.version {
                    if parsed.version > max_version {
                        continue; // Skip files beyond requested version
                    }
                }
                parsed_paths.push(parsed);
            }
        }
        
        // Combine with log_tail if provided
        if !self.log_tail.is_empty() {
            parsed_paths.extend(self.log_tail);
            // Sort and deduplicate
            parsed_paths.sort_by_key(|p| (p.version, p.filename.clone()));
            parsed_paths.dedup_by_key(|p| (p.version, p.filename.clone()));
        }
        
        // Categorize paths into commits, checkpoints, etc.
        let mut ascending_commit_files = vec![];
        let mut ascending_compaction_files = vec![];
        let mut checkpoint_parts = vec![];
        let mut latest_crc_file = None;
        let mut latest_commit_file = None;
        
        for path in parsed_paths {
            match path.file_type {
                crate::path::LogPathFileType::Commit | crate::path::LogPathFileType::StagedCommit => {
                    latest_commit_file = Some(path.clone());
                    ascending_commit_files.push(path);
                }
                crate::path::LogPathFileType::SinglePartCheckpoint 
                | crate::path::LogPathFileType::UuidCheckpoint
                | crate::path::LogPathFileType::MultiPartCheckpoint { .. } => {
                    checkpoint_parts.push(path);
                }
                crate::path::LogPathFileType::CompactedCommit { .. } => {
                    ascending_compaction_files.push(path);
                }
                crate::path::LogPathFileType::Crc => {
                    latest_crc_file = Some(path);
                }
                crate::path::LogPathFileType::Unknown => {
                    // Ignore unknown file types
                }
            }
        }
        
        // Build ListedLogFiles
        let listed_files = ListedLogFiles::try_new(
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
            latest_commit_file,
        )?;
        
        // Validate and construct LogSegment
        let log_segment = LogSegment::try_new(
            listed_files,
            self.log_root.clone(),
            self.version,
        )?;
        
        Ok(StateMachinePhase::Consume(Box::new(ProtocolMetadataPhase {
            table_root: self.table_root,
            log_segment,
        })))
    }
}

/// Phase 3: Read Protocol and Metadata from checkpoint/commits
pub struct ProtocolMetadataPhase {
    table_root: Url,
    log_segment: LogSegment,
}

impl ConsumePhase for ProtocolMetadataPhase {
    type Output = Snapshot;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        use crate::actions::{get_commit_schema, METADATA_NAME, PROTOCOL_NAME};
        use crate::kernel_df::{FileType, FirstNonNullNode};
        use crate::{Expression, Predicate};
        
        // Determine which files to read
        let files: Vec<FileMeta> = if !self.log_segment.checkpoint_parts.is_empty() {
            // Read from checkpoint
            self.log_segment.checkpoint_parts.iter()
                .map(|p| p.location.clone())
                .collect()
        } else {
            // Read from commits
            self.log_segment.ascending_commit_files.iter()
                .map(|p| p.location.clone())
                .collect()
        };
        
        let file_type = if !self.log_segment.checkpoint_parts.is_empty() {
            FileType::Parquet
        } else {
            FileType::Json
        };
        
        // Project schema to just protocol and metadata
        let schema = get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        
        // Build plan: Scan → Filter → FirstNonNull
        let plan = LogicalPlanNode::Scan(crate::kernel_df::ScanNode {
            file_type,
            files,
            schema,
        });
        
        // Filter for rows with non-null protocol or metadata
        let filter_predicate = Arc::new(Predicate::or(
            Expression::column([METADATA_NAME, "id"]).is_not_null(),
            Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
        ));
        
        let plan = LogicalPlanNode::FilterByExpression(
            crate::kernel_df::FilterByExpressionNode {
                child: Box::new(plan),
                predicate: filter_predicate,
            }
        );
        
        // Extract first non-null for protocol and metadata
        Ok(LogicalPlanNode::FirstNonNull(FirstNonNullNode {
            child: Box::new(plan),
            columns: vec![PROTOCOL_NAME.to_string(), METADATA_NAME.to_string()],
        }))
    }
    
    fn next(
        self: Box<Self>,
        mut data: Box<dyn Iterator<Item = DeltaResult<crate::arrow::array::RecordBatch>> + Send>
    ) -> DeltaResult<StateMachinePhase<Self::Output>> {
        use crate::actions::visitors::{ProtocolVisitor, MetadataVisitor};
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::RowVisitor;
        
        // Take first batch (FirstNonNull already reduced to single row)
        let rb = data.next()
            .transpose()?
            .ok_or_else(|| Error::generic("No protocol/metadata data"))?;
        
        if rb.num_rows() == 0 {
            return Err(Error::generic("No protocol/metadata found"));
        }
        
        // Convert RecordBatch to ArrowEngineData
        let engine_data = ArrowEngineData::new(rb);
        
        // Visit protocol
        let mut protocol_visitor = ProtocolVisitor::default();
        protocol_visitor.visit_rows_of(&engine_data)?;
        let protocol = protocol_visitor.protocol
            .ok_or_else(|| Error::MissingProtocol)?;
        
        // Visit metadata
        let mut metadata_visitor = MetadataVisitor::default();
        metadata_visitor.visit_rows_of(&engine_data)?;
        let metadata = metadata_visitor.metadata
            .ok_or_else(|| Error::MissingMetadata)?;
        
        // Build TableConfiguration
        let table_configuration = TableConfiguration::try_new(
            metadata,
            protocol,
            self.table_root,
            self.log_segment.end_version,
        )?;
        
        // Construct final Snapshot
        let snapshot = Snapshot {
            log_segment: self.log_segment,
            table_configuration,
        };
        
        Ok(StateMachinePhase::Terminus(snapshot))
    }
}

