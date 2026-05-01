//! Declarative [`Plan`] -> DataFusion [`ExecutionPlan`] compilation.
//!
//! Phase 1.2 extends the scaffold with leaf support:
//! - `Literal`
//! - `Scan`
//! - `FileListing`
//! - `Relation`

use std::sync::Arc;

use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{RelationHandle, SinkType};
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};

use crate::exec::{FileListingExec, LiteralExec, RelationBatchRegistry, RelationRefExec};

mod scan;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    pub relation_registry: Arc<RelationBatchRegistry>,
}

impl CompileContext {
    pub fn new(relation_registry: Arc<RelationBatchRegistry>) -> Self {
        Self { relation_registry }
    }
}

/// Compile a complete [`Plan`] when the sink envelope is supported.
pub fn compile_plan(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match &plan.sink.sink_type {
        SinkType::Results => compile_declarative_node(&plan.root, ctx),
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
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match node {
        DeclarativePlanNode::Literal(n) => Ok(Arc::new(LiteralExec::try_new(
            n.schema.clone(),
            n.rows.clone(),
        )?)),
        DeclarativePlanNode::Scan(node) => scan::compile_scan(node),
        DeclarativePlanNode::FileListing(node) => Ok(Arc::new(FileListingExec::new(node.path.clone()))),
        DeclarativePlanNode::Relation(handle) => compile_relation(handle, ctx),
        other => Err(crate::error::unsupported(format!(
            "DataFusion scaffold does not yet compile `{}` nodes",
            declarative_node_kind(other)
        ))),
    }
}

fn compile_relation(
    handle: &RelationHandle,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    Ok(Arc::new(RelationRefExec::new(
        handle.clone(),
        Arc::clone(&ctx.relation_registry),
    )?))
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
