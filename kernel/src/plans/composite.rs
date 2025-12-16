//! Composite plan types for Delta operations.
//!
//! These plans compose individual nodes into complete operation plans.
//! Engines that want compile-time knowledge of plan structure can use these directly.

use super::nodes::*;
use super::{AsQueryPlan, DeclarativePlanNode};

// =============================================================================
// Sub-Plans (reusable components)
// =============================================================================

/// Data skipping optimization plan.
///
/// Parses stats JSON and filters files based on min/max values.
#[derive(Debug, Clone)]
pub struct DataSkippingPlan {
    /// Parse stats JSON column (outputs stats schema at root level)
    pub parse_json: ParseJsonNode,
    /// Filter based on parsed stats
    pub filter: FilterByExpressionNode,
}

// =============================================================================
// Phase Plans (complete operation plans)
// =============================================================================

/// Plan for processing commit files (JSON log files).
///
/// Structure: Scan → [DataSkipping] → AddRemoveDedup → Project → Sink
#[derive(Debug, Clone)]
pub struct CommitPhasePlan {
    /// Scan commit JSON files
    pub scan: ScanNode,
    /// Optional data skipping optimization
    pub data_skipping: Option<DataSkippingPlan>,
    /// Deduplication filter (AddRemoveDedup KDF)
    pub dedup_filter: FilterByKDF,
    /// Project to output schema
    pub project: SelectNode,
    /// Terminal sink (default: Results)
    pub sink: SinkNode,
}

/// Plan for reading checkpoint manifest (v2 checkpoints).
///
/// Structure: Scan → Project → ConsumeByKDF(SidecarCollector) → Sink
///
/// For V2 checkpoints, we need to:
/// 1. Scan the checkpoint file
/// 2. Project the sidecar.path column
/// 3. Collect the sidecar file paths via ConsumeByKDF
/// 4. These paths are then used in the CheckpointLeaf phase
#[derive(Debug, Clone)]
pub struct CheckpointManifestPlan {
    /// Scan manifest parquet file
    pub scan: ScanNode,
    /// Project sidecar file paths
    pub project: SelectNode,
    /// Consumer KDF to collect sidecar file paths
    pub sidecar_collector: ConsumeByKDF,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for reading checkpoint leaf/sidecar files.
///
/// Structure: Scan → Dedup → Project → Sink
#[derive(Debug, Clone)]
pub struct CheckpointLeafPlan {
    /// Scan checkpoint parquet files
    pub scan: ScanNode,
    /// Deduplication KDF for checkpoint
    pub dedup_filter: FilterByKDF,
    /// Project to output schema
    pub project: SelectNode,
    /// Terminal sink (default: Results)
    pub sink: SinkNode,
}

/// Plan for listing log files.
///
/// Structure: FileListing → ConsumeByKDF (LogSegmentBuilder) → Sink
///
/// The consumer KDF processes file listing results to build a LogSegment.
#[derive(Debug, Clone)]
pub struct FileListingPhasePlan {
    /// List files from _delta_log
    pub listing: FileListingNode,
    /// Consumer KDF to build LogSegment from listing results
    pub log_segment_builder: ConsumeByKDF,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for loading table metadata (protocol and metadata actions).
///
/// Structure: Scan → ConsumeByKDF (MetadataProtocolReader) → Sink
///
/// The consumer KDF processes scan results to extract the first non-null
/// protocol and metadata actions needed for snapshot construction.
#[derive(Debug, Clone)]
pub struct MetadataLoadPlan {
    /// Scan protocol/metadata files (JSON commits or Parquet checkpoints)
    pub scan: ScanNode,
    /// Consumer KDF to extract protocol and metadata from scan results
    pub metadata_reader: ConsumeByKDF,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for reading checkpoint hint file (_last_checkpoint).
///
/// Structure: Scan (JSON) → ConsumeByKDF (CheckpointHintReader) → Sink
///
/// The consumer KDF processes scan results to extract the checkpoint hint.
#[derive(Debug, Clone)]
pub struct CheckpointHintPlan {
    /// Scan the _last_checkpoint JSON file
    pub scan: ScanNode,
    /// Consumer KDF to extract checkpoint hint from scan results
    pub hint_reader: ConsumeByKDF,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for querying checkpoint schema to detect V2 checkpoints.
///
/// Structure: Sink(Drop) -> SchemaQuery
///
/// Used to determine if a single-part checkpoint is a V2 checkpoint with sidecars
/// by checking if the schema contains a 'sidecar' column.
#[derive(Debug, Clone)]
pub struct SchemaQueryPhasePlan {
    /// Query the parquet file schema (footer read only)
    pub schema_query: SchemaQueryNode,
    /// Sink for the plan (Drop - schema query produces no user-facing data)
    pub sink: SinkNode,
}

/// Plan for reading JSON checkpoints that may or may not have sidecars.
///
/// Structure: Scan → ConsumeByKDF(SidecarCollector) → FilterByKDF(AddRemoveDedup) → Select → Sink(Results)
///
/// This plan handles both V2 JSON checkpoints with sidecars and V2 JSON checkpoints
/// where add actions are embedded directly. The SidecarCollector sees all rows to
/// collect sidecar references, while FilterByKDF deduplicates add actions.
///
/// After execution:
/// - If sidecars were collected: transition to CheckpointLeaf to read sidecar files
/// - If no sidecars: all add actions were in the checkpoint, done
#[derive(Debug, Clone)]
pub struct JsonCheckpointPhasePlan {
    /// Scan JSON checkpoint file
    pub scan: ScanNode,
    /// Consumer KDF to collect sidecar file paths (sees all rows before filtering)
    pub sidecar_collector: ConsumeByKDF,
    /// Deduplication filter for add/remove actions
    pub dedup_filter: FilterByKDF,
    /// Project add actions to output schema
    pub project: SelectNode,
    /// Results sink - streams add actions found in checkpoint
    pub sink: SinkNode,
}

// =============================================================================
// AsQueryPlan Implementations
// =============================================================================

impl AsQueryPlan for CommitPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let mut plan = DeclarativePlanNode::Scan(self.scan.clone());

        // Add data skipping if present
        if let Some(ds) = &self.data_skipping {
            plan = DeclarativePlanNode::ParseJson {
                child: Box::new(plan),
                node: ds.parse_json.clone(),
            };
            plan = DeclarativePlanNode::FilterByExpression {
                child: Box::new(plan),
                node: ds.filter.clone(),
            };
        }

        // Add dedup KDF filter
        plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(plan),
            node: self.dedup_filter.clone(),
        };

        // Add projection and sink
        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::Select {
                child: Box::new(plan),
                node: self.project.clone(),
            }),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for CheckpointManifestPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        let project = DeclarativePlanNode::Select {
            child: Box::new(scan),
            node: self.project.clone(),
        };
        let consume = DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(project),
            node: self.sidecar_collector.clone(),
        };
        DeclarativePlanNode::Sink {
            child: Box::new(consume),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for CheckpointLeafPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        let dedup = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: self.dedup_filter.clone(),
        };
        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::Select {
                child: Box::new(dedup),
                node: self.project.clone(),
            }),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for FileListingPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let listing = DeclarativePlanNode::FileListing(self.listing.clone());
        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::ConsumeByKDF {
                child: Box::new(listing),
                node: self.log_segment_builder.clone(),
            }),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for MetadataLoadPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::ConsumeByKDF {
                child: Box::new(scan),
                node: self.metadata_reader.clone(),
            }),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for CheckpointHintPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::ConsumeByKDF {
                child: Box::new(scan),
                node: self.hint_reader.clone(),
            }),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for SchemaQueryPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        // SchemaQuery produces no user-facing data - wrap in Drop sink
        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::SchemaQuery(self.schema_query.clone())),
            node: self.sink.clone(),
        }
    }
}

impl AsQueryPlan for JsonCheckpointPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        // Order: Scan → ConsumeByKDF → FilterByKDF → Select → Sink
        // The consumer KDF sees all rows (including sidecar actions) before filtering
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        let consume = DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(scan),
            node: self.sidecar_collector.clone(),
        };
        let filter = DeclarativePlanNode::FilterByKDF {
            child: Box::new(consume),
            node: self.dedup_filter.clone(),
        };
        let select = DeclarativePlanNode::Select {
            child: Box::new(filter),
            node: self.project.clone(),
        };
        DeclarativePlanNode::Sink {
            child: Box::new(select),
            node: self.sink.clone(),
        }
    }
}
