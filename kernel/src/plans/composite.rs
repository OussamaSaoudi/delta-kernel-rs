//! Composite plan types for Delta operations.
//!
//! These plans compose individual nodes into complete operation plans.
//! Engines that want compile-time knowledge of plan structure can use these directly.

use super::nodes::*;
use super::kdf_state::{ConsumerKdfState, ConsumerStateSender};
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

/// A scan node with a select that adds a version column.
///
/// Used to build Union of per-file scans where each file's version is added as a column.
#[derive(Debug, Clone)]
pub struct ScanWithVersion {
    /// The scan node for this file
    pub scan: ScanNode,
    /// Select node that adds the version column via Transform
    pub select: SelectNode,
}

/// Plan for processing commit files (JSON log files).
///
/// Structure: Union(ScanWithVersion...) → [DataSkipping] → [PartitionPrune] → AddRemoveDedup → Project → Sink
///
/// Each commit file is scanned individually with a version literal added via Transform.
/// This enables version DESC ordering which is required for AddRemoveDedup.
#[derive(Debug, Clone)]
pub struct CommitPhasePlan {
    /// Per-file scans with version - combined into a Union
    pub scans: Vec<ScanWithVersion>,
    /// Optional data skipping optimization
    pub data_skipping: Option<DataSkippingPlan>,
    /// Optional partition pruning filter (partitionValues-only pruning)
    pub partition_prune_filter: Option<FilterByKDF>,
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
///
/// Uses `ConsumerStateSender` directly - the corresponding receiver is stored in the phase.
#[derive(Debug, Clone)]
pub struct CheckpointManifestPlan {
    /// Scan manifest parquet file
    pub scan: ScanNode,
    /// Project sidecar file paths
    pub project: SelectNode,
    /// Consumer KDF sender to collect sidecar file paths
    pub sidecar_collector: ConsumerStateSender,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for reading checkpoint leaf/sidecar files.
///
/// Structure: Scan → [PartitionPrune] → Dedup → Project → Sink
#[derive(Debug, Clone)]
pub struct CheckpointLeafPlan {
    /// Scan checkpoint parquet files
    pub scan: ScanNode,
    /// Optional partition pruning filter (partitionValues-only pruning)
    pub partition_prune_filter: Option<FilterByKDF>,
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
/// Uses `ConsumerStateSender` directly - the corresponding receiver is stored in the phase.
#[derive(Debug, Clone)]
pub struct FileListingPhasePlan {
    /// List files from _delta_log
    pub listing: FileListingNode,
    /// Consumer KDF sender to build LogSegment from listing results
    pub log_segment_builder: ConsumerStateSender,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for loading table metadata (protocol and metadata actions).
///
/// Structure: Union(ScanWithVersion...) → ConsumeByKDF (MetadataProtocolReader) → Sink
///
/// Each file is scanned individually with a version literal added via Transform.
/// This enables version DESC ordering which ensures the first P&M found is from
/// the latest version.
///
/// Uses `ConsumerStateSender` directly - the corresponding receiver is stored in the phase.
#[derive(Debug, Clone)]
pub struct MetadataLoadPlan {
    /// Per-file scans with version - combined into a Union
    pub scans: Vec<ScanWithVersion>,
    /// Consumer KDF sender to extract protocol and metadata from scan results
    pub metadata_reader: ConsumerStateSender,
    /// Terminal sink (default: Drop)
    pub sink: SinkNode,
}

/// Plan for reading checkpoint hint file (_last_checkpoint).
///
/// Structure: Scan (JSON) → ConsumeByKDF (CheckpointHintReader) → Sink
///
/// The consumer KDF processes scan results to extract the checkpoint hint.
/// Uses `ConsumerStateSender` directly - the corresponding receiver is stored in the phase.
#[derive(Debug, Clone)]
pub struct CheckpointHintPlan {
    /// Scan the _last_checkpoint JSON file
    pub scan: ScanNode,
    /// Consumer KDF sender to extract checkpoint hint from scan results
    pub hint_reader: ConsumerStateSender,
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
/// Structure: Scan → ConsumeByKDF(SidecarCollector) → [PartitionPrune] → FilterByKDF(AddRemoveDedup) → Select → Sink(Results)
///
/// This plan handles both V2 JSON checkpoints with sidecars and V2 JSON checkpoints
/// where add actions are embedded directly. The SidecarCollector sees all rows to
/// collect sidecar references, while FilterByKDF deduplicates add actions.
///
/// After execution:
/// - If sidecars were collected: transition to CheckpointLeaf to read sidecar files
/// - If no sidecars: all add actions were in the checkpoint, done
///
/// Uses `ConsumerStateSender` directly - the corresponding receiver is stored in the phase.
#[derive(Debug, Clone)]
pub struct JsonCheckpointPhasePlan {
    /// Scan JSON checkpoint file
    pub scan: ScanNode,
    /// Consumer KDF sender to collect sidecar file paths (sees all rows before filtering)
    pub sidecar_collector: ConsumerStateSender,
    /// Optional partition pruning filter (partitionValues-only pruning)
    pub partition_prune_filter: Option<FilterByKDF>,
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

#[inline]
fn maybe_filter_by_kdf(plan: DeclarativePlanNode, filter: Option<&FilterByKDF>) -> DeclarativePlanNode {
    match filter {
        Some(node) => DeclarativePlanNode::FilterByKDF {
            child: Box::new(plan),
            node: node.clone(),
        },
        None => plan,
    }
}

impl AsQueryPlan for CommitPhasePlan {
    fn as_query_plan(&self) -> DeclarativePlanNode {
        // Build Union of per-file Scan→Select pipelines
        let children: Vec<DeclarativePlanNode> = self
            .scans
            .iter()
            .map(|swv| {
                DeclarativePlanNode::Select {
                    child: Box::new(DeclarativePlanNode::Scan(swv.scan.clone())),
                    node: swv.select.clone(),
                }
            })
            .collect();

        // Create source - Union if multiple children, or single child
        let mut plan = if children.len() == 1 {
            children.into_iter().next().unwrap()
        } else {
            DeclarativePlanNode::Union { children }
        };

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

        plan = maybe_filter_by_kdf(plan, self.partition_prune_filter.as_ref());

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
        let plan = maybe_filter_by_kdf(
            DeclarativePlanNode::Scan(self.scan.clone()),
            self.partition_prune_filter.as_ref(),
        );
        let dedup = DeclarativePlanNode::FilterByKDF {
            child: Box::new(plan),
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
        // Build Union of per-file Scan→Select pipelines
        let children: Vec<DeclarativePlanNode> = self
            .scans
            .iter()
            .map(|swv| {
                DeclarativePlanNode::Select {
                    child: Box::new(DeclarativePlanNode::Scan(swv.scan.clone())),
                    node: swv.select.clone(),
                }
            })
            .collect();

        // Create source - Union if multiple children, or single child
        let source = if children.len() == 1 {
            children.into_iter().next().unwrap()
        } else {
            DeclarativePlanNode::Union { children }
        };

        DeclarativePlanNode::Sink {
            child: Box::new(DeclarativePlanNode::ConsumeByKDF {
                child: Box::new(source),
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
        let plan = maybe_filter_by_kdf(consume, self.partition_prune_filter.as_ref());
        let filter = DeclarativePlanNode::FilterByKDF {
            child: Box::new(plan),
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
