//! Conversion between native Rust plan types and protobuf types.
//!
//! Implements `From` and `TryFrom` traits for converting between
//! `crate::plans::*` types and `crate::proto_generated::*` types.

use std::sync::Arc;

use crate::proto_generated as proto;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error};

use super::nodes::*;
use super::declarative::DeclarativePlanNode;
use super::composite::*;
use super::state_machines::SnapshotPhase;
use super::AsQueryPlan;

// =============================================================================
// FileType Conversion
// =============================================================================

impl From<FileType> for i32 {
    fn from(ft: FileType) -> i32 {
        match ft {
            FileType::Parquet => proto::scan_node::FileType::Parquet as i32,
            FileType::Json => proto::scan_node::FileType::Json as i32,
        }
    }
}

impl TryFrom<i32> for FileType {
    type Error = Error;

    fn try_from(value: i32) -> DeltaResult<Self> {
        match proto::scan_node::FileType::try_from(value) {
            Ok(proto::scan_node::FileType::Parquet) => Ok(FileType::Parquet),
            Ok(proto::scan_node::FileType::Json) => Ok(FileType::Json),
            _ => Err(Error::generic(format!("Unknown file type: {}", value))),
        }
    }
}

// =============================================================================
// SchemaReaderFunctionId Conversion
// =============================================================================
//
// Note: FilterKernelFunctionId has been removed. Filter KDFs now use typed state
// via the FilterKdfState enum. The enum variant IS the function identity.
// See kdf_state.rs for the implementation.

impl From<SchemaReaderFunctionId> for i32 {
    fn from(id: SchemaReaderFunctionId) -> i32 {
        match id {
            SchemaReaderFunctionId::SchemaStore => proto::SchemaReaderFunctionId::SchemaStore as i32,
        }
    }
}

impl TryFrom<i32> for SchemaReaderFunctionId {
    type Error = Error;

    fn try_from(value: i32) -> DeltaResult<Self> {
        match proto::SchemaReaderFunctionId::try_from(value) {
            Ok(proto::SchemaReaderFunctionId::SchemaStore) => Ok(SchemaReaderFunctionId::SchemaStore),
            _ => Err(Error::generic(format!("Unknown schema reader function id: {}", value))),
        }
    }
}

// =============================================================================
// Node Conversions: Rust -> Proto
// =============================================================================

impl From<&ScanNode> for proto::ScanNode {
    fn from(node: &ScanNode) -> Self {
        proto::ScanNode {
            file_type: node.file_type.into(),
            files: node.files.iter().map(|f| f.location.to_string()).collect(),
            schema: Some(schema_to_proto(&node.schema)),
        }
    }
}

impl From<&FileListingNode> for proto::FileListingNode {
    fn from(node: &FileListingNode) -> Self {
        proto::FileListingNode {
            path: node.path.to_string(),
        }
    }
}

impl From<&FilterByKDF> for proto::FilterByKdf {
    fn from(node: &FilterByKDF) -> Self {
        // Convert typed state to raw pointer for FFI
        // Clone the state since we're taking ownership for the raw pointer
        proto::FilterByKdf {
            state_ptr: node.state.clone().into_raw(),
        }
    }
}

impl From<&ConsumeByKDF> for proto::ConsumeByKdf {
    fn from(node: &ConsumeByKDF) -> Self {
        // Convert typed state to raw pointer for FFI
        // Clone the state since we're taking ownership for the raw pointer
        proto::ConsumeByKdf {
            state_ptr: node.state.clone().into_raw(),
        }
    }
}

impl From<&SchemaQueryNode> for proto::SchemaQueryNode {
    fn from(node: &SchemaQueryNode) -> Self {
        // Convert typed state to raw pointer for FFI
        proto::SchemaQueryNode {
            file_path: node.file_path.clone(),
            state_ptr: node.state.clone().into_raw(),
        }
    }
}

impl From<&FilterByExpressionNode> for proto::FilterByExpressionNode {
    fn from(node: &FilterByExpressionNode) -> Self {
        proto::FilterByExpressionNode {
            predicate: Some(expression_to_proto(&node.predicate)),
        }
    }
}

impl From<&SelectNode> for proto::SelectNode {
    fn from(node: &SelectNode) -> Self {
        proto::SelectNode {
            columns: node.columns.iter().map(|e| expression_to_proto(e)).collect(),
            output_schema: Some(schema_to_proto(&node.output_schema)),
        }
    }
}

impl From<&ParseJsonNode> for proto::ParseJsonNode {
    fn from(node: &ParseJsonNode) -> Self {
        proto::ParseJsonNode {
            json_column: node.json_column.clone(),
            target_schema: Some(schema_to_proto(&node.target_schema)),
            output_column: node.output_column.clone(),
        }
    }
}

impl From<&FirstNonNullNode> for proto::FirstNonNullNode {
    fn from(node: &FirstNonNullNode) -> Self {
        proto::FirstNonNullNode {
            columns: node.columns.clone(),
        }
    }
}

// =============================================================================
// Sink Node Conversions: Rust -> Proto
// =============================================================================

impl From<SinkType> for i32 {
    fn from(st: SinkType) -> i32 {
        match st {
            SinkType::Drop => proto::SinkType::Drop as i32,
            SinkType::Results => proto::SinkType::Results as i32,
        }
    }
}

impl TryFrom<i32> for SinkType {
    type Error = Error;

    fn try_from(value: i32) -> DeltaResult<Self> {
        match proto::SinkType::try_from(value) {
            Ok(proto::SinkType::Drop) => Ok(SinkType::Drop),
            Ok(proto::SinkType::Results) => Ok(SinkType::Results),
            _ => Err(Error::generic(format!("Unknown sink type: {}", value))),
        }
    }
}

impl From<&SinkNode> for proto::SinkNode {
    fn from(node: &SinkNode) -> Self {
        proto::SinkNode {
            sink_type: node.sink_type.into(),
        }
    }
}

// =============================================================================
// DeclarativePlanNode Conversion: Rust -> Proto
// =============================================================================

impl From<&DeclarativePlanNode> for proto::DeclarativePlanNode {
    fn from(node: &DeclarativePlanNode) -> Self {
        use proto::declarative_plan_node::Node;

        let node_variant = match node {
            DeclarativePlanNode::Scan(n) => Node::Scan(n.into()),
            DeclarativePlanNode::FileListing(n) => Node::FileListing(n.into()),
            DeclarativePlanNode::SchemaQuery(n) => Node::SchemaQuery(n.into()),
            DeclarativePlanNode::FilterByKDF { child, node: n } => {
                Node::FilterByKdf(Box::new(proto::FilterByKdfPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
            DeclarativePlanNode::ConsumeByKDF { child, node: n } => {
                Node::ConsumeByKdf(Box::new(proto::ConsumeByKdfPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
            DeclarativePlanNode::FilterByExpression { child, node: n } => {
                Node::FilterByExpression(Box::new(proto::FilterByExpressionPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
            DeclarativePlanNode::Select { child, node: n } => {
                Node::Select(Box::new(proto::SelectPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
            DeclarativePlanNode::ParseJson { child, node: n } => {
                Node::ParseJson(Box::new(proto::ParseJsonPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
            DeclarativePlanNode::FirstNonNull { child, node: n } => {
                Node::FirstNonNull(Box::new(proto::FirstNonNullPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
            DeclarativePlanNode::Sink { child, node: n } => {
                Node::Sink(Box::new(proto::SinkPlan {
                    child: Some(Box::new(child.as_ref().into())),
                    node: Some(n.into()),
                }))
            }
        };

        proto::DeclarativePlanNode { node: Some(node_variant) }
    }
}

// =============================================================================
// Composite Plan Conversions: Rust -> Proto
// =============================================================================

impl From<&CommitPhasePlan> for proto::CommitPhasePlan {
    fn from(plan: &CommitPhasePlan) -> Self {
        proto::CommitPhasePlan {
            scan: Some((&plan.scan).into()),
            data_skipping: plan.data_skipping.as_ref().map(|ds| proto::DataSkippingPlan {
                parse_json: Some((&ds.parse_json).into()),
                filter: Some((&ds.filter).into()),
            }),
            dedup_filter: Some((&plan.dedup_filter).into()),
            project: Some((&plan.project).into()),
            sink: Some((&plan.sink).into()),
        }
    }
}

impl From<&CheckpointManifestPlan> for proto::CheckpointManifestPlan {
    fn from(plan: &CheckpointManifestPlan) -> Self {
        proto::CheckpointManifestPlan {
            scan: Some((&plan.scan).into()),
            project: Some((&plan.project).into()),
            sink: Some((&plan.sink).into()),
        }
    }
}

impl From<&CheckpointLeafPlan> for proto::CheckpointLeafPlan {
    fn from(plan: &CheckpointLeafPlan) -> Self {
        proto::CheckpointLeafPlan {
            scan: Some((&plan.scan).into()),
            dedup_filter: Some((&plan.dedup_filter).into()),
            project: Some((&plan.project).into()),
            sink: Some((&plan.sink).into()),
        }
    }
}

impl From<&CheckpointHintPlan> for proto::CheckpointHintPlan {
    fn from(plan: &CheckpointHintPlan) -> Self {
        proto::CheckpointHintPlan {
            scan: Some((&plan.scan).into()),
            hint_reader: Some((&plan.hint_reader).into()),
            sink: Some((&plan.sink).into()),
        }
    }
}

impl From<&FileListingPhasePlan> for proto::FileListingPhasePlan {
    fn from(plan: &FileListingPhasePlan) -> Self {
        proto::FileListingPhasePlan {
            listing: Some((&plan.listing).into()),
            log_segment_builder: Some((&plan.log_segment_builder).into()),
            sink: Some((&plan.sink).into()),
        }
    }
}

impl From<&MetadataLoadPlan> for proto::MetadataLoadPlan {
    fn from(plan: &MetadataLoadPlan) -> Self {
        proto::MetadataLoadPlan {
            scan: Some((&plan.scan).into()),
            metadata_reader: Some((&plan.metadata_reader).into()),
            sink: Some((&plan.sink).into()),
        }
    }
}

impl From<&SchemaQueryPhasePlan> for proto::SchemaQueryPhasePlan {
    fn from(plan: &SchemaQueryPhasePlan) -> Self {
        proto::SchemaQueryPhasePlan {
            schema_query: Some((&plan.schema_query).into()),
            // No sink - SchemaQuery produces no data
        }
    }
}

// =============================================================================
// Snapshot Phase Conversion: Rust -> Proto
// =============================================================================

impl From<&SnapshotPhase> for proto::SnapshotBuildPhase {
    fn from(phase: &SnapshotPhase) -> Self {
        use proto::snapshot_build_phase::Phase;

        let phase_variant = match phase {
            SnapshotPhase::CheckpointHint(p) => Phase::CheckpointHint(proto::CheckpointHintData {
                plan: Some(p.into()),
                query_plan: Some((&p.as_query_plan()).into()),
            }),
            SnapshotPhase::ListFiles(p) => Phase::ListFiles(proto::FileListingPhaseData {
                plan: Some(p.into()),
                query_plan: Some((&p.as_query_plan()).into()),
            }),
            SnapshotPhase::LoadMetadata(p) => Phase::LoadMetadata(proto::MetadataLoadData {
                plan: Some(p.into()),
                query_plan: Some((&p.as_query_plan()).into()),
            }),
            SnapshotPhase::Complete => Phase::Ready(proto::SnapshotReady {
                version: 0, // TODO: populate from actual snapshot data
                table_schema: None,
            }),
        };

        proto::SnapshotBuildPhase {
            phase: Some(phase_variant),
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn schema_to_proto(_schema: &SchemaRef) -> proto::Schema {
    // TODO: Implement full schema conversion
    proto::Schema { fields: vec![] }
}

fn expression_to_proto(_expr: &Arc<crate::Expression>) -> proto::Expression {
    // TODO: Implement full expression conversion
    proto::Expression { expr: None }
}

