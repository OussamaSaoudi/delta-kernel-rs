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
    /// Deduplication filter (AddRemoveDedup)
    pub dedup_filter: FilterNode,
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
    /// Optional deduplication for checkpoint
    pub dedup_filter: Option<FilterNode>,
    /// Project to output schema
    pub project: SelectNode,
}

/// Plan for listing log files.
///
/// Structure: FileListing → Filter → FirstNonNull
#[derive(Debug, Clone)]
pub struct FileListingPhasePlan {
    /// List files from _delta_log
    pub listing: FileListingNode,
    /// Filter by version/file type
    pub filter: FilterByExpressionNode,
    /// Extract checkpoint hint if present
    pub extract_hint: FirstNonNullNode,
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

        // Add dedup filter
        plan = DeclarativePlanNode::Filter {
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
            plan = DeclarativePlanNode::Filter {
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
        let listing = DeclarativePlanNode::FileListing(self.listing.clone());
        let filtered = DeclarativePlanNode::FilterByExpression {
            child: Box::new(listing),
            node: self.filter.clone(),
        };
        DeclarativePlanNode::FirstNonNull {
            child: Box::new(filtered),
            node: self.extract_hint.clone(),
        }
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


