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
/// Structure: Scan → [Dedup] → Project
#[derive(Debug, Clone)]
pub struct CheckpointLeafPlan {
    /// Scan checkpoint parquet files
    pub scan: ScanNode,
    /// Optional deduplication KDF for checkpoint
    pub dedup_filter: Option<FilterByKDF>,
    /// Project to output schema
    pub project: SelectNode,
}

/// Plan for listing log files.
///
/// Structure: FileListing → [ConsumeByKDF (LogSegmentBuilder)]
///
/// The optional consumer KDF processes file listing results to build a LogSegment.
/// When present, the plan becomes: FileListing → ConsumeByKDF
#[derive(Debug, Clone)]
pub struct FileListingPhasePlan {
    /// List files from _delta_log
    pub listing: FileListingNode,
    /// Optional consumer KDF to build LogSegment from listing results
    pub log_segment_builder: Option<ConsumeByKDF>,
}

/// Plan for loading table metadata.
///
/// Structure: Scan → FirstNonNull
#[derive(Debug, Clone)]
pub struct MetadataLoadPlan {
    /// Scan protocol/metadata files
    pub scan: ScanNode,
    /// Extract first non-null protocol/metadata
    pub extract: FirstNonNullNode,
}

/// Plan for reading checkpoint hint file (_last_checkpoint).
///
/// Structure: Scan (JSON)
#[derive(Debug, Clone)]
pub struct CheckpointHintPlan {
    /// Scan the _last_checkpoint JSON file
    pub scan: ScanNode,
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
        let mut plan = DeclarativePlanNode::Scan(self.scan.clone());

        if let Some(dedup) = &self.dedup_filter {
            plan = DeclarativePlanNode::FilterByKDF {
                child: Box::new(plan),
                node: dedup.clone(),
            };
        }

        DeclarativePlanNode::Select {
            child: Box::new(plan),
            node: self.project.clone(),
        }
    }
}

impl AsQueryPlan for FileListingPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let mut plan = DeclarativePlanNode::FileListing(self.listing.clone());

        // Add consumer KDF if present
        if let Some(consumer) = &self.log_segment_builder {
            plan = DeclarativePlanNode::ConsumeByKDF {
                child: Box::new(plan),
                node: consumer.clone(),
            };
        }

        plan
    }
}

impl AsQueryPlan for MetadataLoadPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        let scan = DeclarativePlanNode::Scan(self.scan.clone());
        DeclarativePlanNode::FirstNonNull {
            child: Box::new(scan),
            node: self.extract.clone(),
        }
    }
}

impl AsQueryPlan for CheckpointHintPlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        DeclarativePlanNode::Scan(self.scan.clone())
    }
}


