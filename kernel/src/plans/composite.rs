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
    /// Parse stats JSON column
    pub parse_json: ParseJsonNode,
    /// Filter based on parsed stats
    pub filter: FilterByExpressionNode,
}

// =============================================================================
// Phase Plans (complete operation plans)
// =============================================================================

/// Plan for processing commit files (JSON log files).
///
/// Structure: Scan → [DataSkipping] → AddRemoveDedup → Project
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
}

/// Plan for reading checkpoint manifest (v2 checkpoints).
///
/// Structure: Scan → Project (sidecar paths)
#[derive(Debug, Clone)]
pub struct CheckpointManifestPlan {
    /// Scan manifest parquet file
    pub scan: ScanNode,
    /// Project sidecar file paths
    pub project: SelectNode,
}

/// Plan for reading checkpoint leaf/sidecar files.
///
/// Structure: Scan → Dedup → Project
#[derive(Debug, Clone)]
pub struct CheckpointLeafPlan {
    /// Scan checkpoint parquet files
    pub scan: ScanNode,
    /// Deduplication KDF for checkpoint
    pub dedup_filter: FilterByKDF,
    /// Project to output schema
    pub project: SelectNode,
}

/// Plan for listing log files.
///
/// Structure: FileListing → ConsumeByKDF (LogSegmentBuilder)
///
/// The consumer KDF processes file listing results to build a LogSegment.
#[derive(Debug, Clone)]
pub struct FileListingPhasePlan {
    /// List files from _delta_log
    pub listing: FileListingNode,
    /// Consumer KDF to build LogSegment from listing results
    pub log_segment_builder: ConsumeByKDF,
}

/// Plan for loading table metadata (protocol and metadata actions).
///
/// Structure: Scan → ConsumeByKDF (MetadataProtocolReader)
///
/// The consumer KDF processes scan results to extract the first non-null
/// protocol and metadata actions needed for snapshot construction.
#[derive(Debug, Clone)]
pub struct MetadataLoadPlan {
    /// Scan protocol/metadata files (JSON commits or Parquet checkpoints)
    pub scan: ScanNode,
    /// Consumer KDF to extract protocol and metadata from scan results
    pub metadata_reader: ConsumeByKDF,
}

/// Plan for reading checkpoint hint file (_last_checkpoint).
///
/// Structure: Scan (JSON) → ConsumeByKDF (CheckpointHintReader)
///
/// The consumer KDF processes scan results to extract the checkpoint hint.
#[derive(Debug, Clone)]
pub struct CheckpointHintPlan {
    /// Scan the _last_checkpoint JSON file
    pub scan: ScanNode,
    /// Consumer KDF to extract checkpoint hint from scan results
    pub hint_reader: ConsumeByKDF,
}

/// Plan for querying checkpoint schema to detect V2 checkpoints.
///
/// Structure: SchemaQuery (parquet footer read only)
///
/// Used to determine if a single-part checkpoint is a V2 checkpoint with sidecars
/// by checking if the schema contains a 'sidecar' column.
#[derive(Debug, Clone)]
pub struct SchemaQueryPhasePlan {
    /// Query the parquet file schema (footer read only)
    pub schema_query: SchemaQueryNode,
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

        // Add projection
        DeclarativePlanNode::Select {
            child: Box::new(plan),
            node: self.project.clone(),
        }
    }
}

impl AsQueryPlan for CheckpointManifestPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        DeclarativePlanNode::Select {
            child: Box::new(scan),
            node: self.project.clone(),
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
        DeclarativePlanNode::Select {
            child: Box::new(dedup),
            node: self.project.clone(),
        }
    }
}

impl AsQueryPlan for FileListingPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let listing = DeclarativePlanNode::FileListing(self.listing.clone());
        DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(listing),
            node: self.log_segment_builder.clone(),
        }
    }
}

impl AsQueryPlan for MetadataLoadPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(scan),
            node: self.metadata_reader.clone(),
        }
    }
}

impl AsQueryPlan for CheckpointHintPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        DeclarativePlanNode::ConsumeByKDF {
            child: Box::new(scan),
            node: self.hint_reader.clone(),
        }
    }
}

impl AsQueryPlan for SchemaQueryPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        DeclarativePlanNode::SchemaQuery(self.schema_query.clone())
    }
}
