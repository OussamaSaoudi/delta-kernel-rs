//! Declarative [`Plan`] → DataFusion [`ExecutionPlan`] compilation (Phase 1.1 subset).

use std::sync::Arc;

use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};

use crate::exec::LiteralExec;

/// Compile a complete [`Plan`] when the envelope is supported by this scaffold.
pub fn compile_plan(plan: &Plan) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match &plan.sink.sink_type {
        SinkType::Results => compile_declarative_node(&plan.root),
        SinkType::Relation(_) => Err(crate::error::unsupported(
            "Relation sink is not implemented for the DataFusion engine scaffold",
        )),
        SinkType::ConsumeByKdf(_) => Err(crate::error::unsupported(
            "ConsumeByKdf sink is not implemented for the DataFusion engine scaffold",
        )),
        SinkType::Load(_) => Err(crate::error::unsupported(
            "Load sink is not implemented for the DataFusion engine scaffold",
        )),
        SinkType::Write(_) => Err(crate::error::unsupported(
            "Write sink is not implemented for the DataFusion engine scaffold",
        )),
        SinkType::PartitionedWrite(_) => Err(crate::error::unsupported(
            "PartitionedWrite sink is not implemented for the DataFusion engine scaffold",
        )),
    }
}

fn compile_declarative_node(
    node: &DeclarativePlanNode,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match node {
        DeclarativePlanNode::Literal(n) => Ok(Arc::new(LiteralExec::try_new(
            n.schema.clone(),
            n.rows.clone(),
        )?)),
        other => Err(crate::error::unsupported(format!(
            "DataFusion engine scaffold only supports Literal roots inside Results plans; got {}",
            declarative_node_kind(other)
        ))),
    }
}

fn declarative_node_kind(node: &DeclarativePlanNode) -> &'static str {
    match node {
        DeclarativePlanNode::Scan(_) => "Scan",
        DeclarativePlanNode::FileListing(_) => "FileListing",
        DeclarativePlanNode::Literal(_) => "Literal",
        DeclarativePlanNode::Relation(_) => "Relation",
        DeclarativePlanNode::Filter { .. } => "Filter",
        DeclarativePlanNode::Project { .. } => "Project",
        DeclarativePlanNode::Window { .. } => "Window",
        DeclarativePlanNode::Assert { .. } => "Assert",
        DeclarativePlanNode::Union { .. } => "Union",
        DeclarativePlanNode::Join { .. } => "Join",
    }
}
