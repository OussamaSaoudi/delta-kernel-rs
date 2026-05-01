//! Declarative [`Plan`] -> DataFusion [`ExecutionPlan`] compilation.
//!
//! Phase 1.7 extends sinks beyond [`SinkType::Results`]:
//! [`SinkType::Relation`] (materialize into [`RelationBatchRegistry`]),
//! [`SinkType::ConsumeByKdf`] (drain via [`KernelConsumeByKdfExec`]).
//!
//! Phase 2.1 adds [`SinkType::Write`] for single-file Parquet / JsonLines via DataFusion
//! [`DataSinkExec`](datafusion_datasource::sink::DataSinkExec) (see [`write_sink`]).
//!
//! Phase 2.2 adds [`SinkType::PartitionedWrite`] ([`KernelPartitionedWriteExec`]): Hive-style
//! directories under a `file://` destination with Parquet or newline-delimited JSON.
//!
//! Phase 2.3 adds [`SinkType::Load`] ([`crate::exec::KernelLoadSinkExec`]): per-row parquet/json
//! reads via kernel handlers into [`crate::exec::RelationBatchRegistry`].
//!
//! Phase 1.2 extends the scaffold with leaf support:
//! - `Literal`
//! - `Scan`
//! - `FileListing`
//! - `Relation`

use std::sync::{Arc, Mutex};

use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{JoinType, RelationHandle, SinkType, WriteSink};
use delta_kernel::plans::ir::{DeclarativePlanNode, Plan};
use delta_kernel::plans::kdf::FinishedHandle;
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use delta_kernel::schema::SchemaRef;
use delta_kernel::Engine;

use crate::exec::{
    FileListingExec, KernelAssertExec, KernelConsumeByKdfExec, KernelFilterExec,
    KernelPartitionedWriteExec, KernelProjectExec, LiteralExec, OrderedUnionExec,
    RelationBatchRegistry, RelationRefExec, RelationSinkExec,
};

pub mod expr_translator;
mod join;
mod load_sink;
pub mod scan;
mod window;
mod write_sink;

/// Context shared by the compiler for leaf nodes that need runtime side state.
#[derive(Clone)]
pub struct CompileContext {
    pub relation_registry: Arc<RelationBatchRegistry>,
    /// Latest finalized [`FinishedHandle`] from a [`SinkType::ConsumeByKdf`] plan run on this
    /// executor.
    pub kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    /// When [`Some`], [`SinkType::ConsumeByKdf`] pipelines submit finalized handles into this
    /// [`PhaseState`] for
    /// [`StateMachine`](delta_kernel::plans::state_machines::framework::state_machine::StateMachine)
    /// phases instead of populating [`Self::kdf_harvest_slot`] only.
    ///
    /// [`None`] preserves single-plan harvesting via
    /// [`crate::executor::DataFusionExecutor::take_last_kdf_finished`].
    pub phase_state: Option<PhaseState>,
    /// Kernel [`Engine`] for sinks that delegate IO to parquet/json handlers ([`SinkType::Load`]).
    pub engine: Arc<dyn Engine>,
}

impl CompileContext {
    pub fn new(
        relation_registry: Arc<RelationBatchRegistry>,
        kdf_harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
        engine: Arc<dyn Engine>,
    ) -> Self {
        Self {
            relation_registry,
            kdf_harvest_slot,
            phase_state: None,
            engine,
        }
    }
}

/// Compile a complete [`Plan`] when the sink envelope is supported.
pub fn compile_plan(
    plan: &Plan,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match &plan.sink.sink_type {
        SinkType::Results => compile_declarative_node(&plan.root, ctx),
        SinkType::Relation(handle) => {
            let inner = compile_declarative_node(&plan.root, ctx)?;
            Ok(Arc::new(RelationSinkExec::new(
                inner,
                handle.id,
                Arc::clone(&ctx.relation_registry),
            )))
        }
        SinkType::ConsumeByKdf(sink) => {
            let inner = compile_declarative_node(&plan.root, ctx)?;
            Ok(Arc::new(KernelConsumeByKdfExec::try_new(
                inner,
                sink.clone(),
                Arc::clone(&ctx.kdf_harvest_slot),
                ctx.phase_state.clone(),
            )?))
        }
        SinkType::Load(_) => load_sink::compile_load_terminal(plan, ctx),
        SinkType::Write(write) => compile_write_terminal(&plan.root, ctx, write),
        SinkType::PartitionedWrite(sink) => {
            let inner = compile_declarative_node(&plan.root, ctx)?;
            Ok(Arc::new(KernelPartitionedWriteExec::try_new(
                inner,
                sink.clone(),
            )?))
        }
    }
}

fn compile_write_terminal(
    root: &DeclarativePlanNode,
    ctx: &CompileContext,
    sink: &WriteSink,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    let inner = compile_declarative_node(root, ctx)?;
    write_sink::compile_write_sink(inner, sink)
}

pub(super) fn compile_declarative_node(
    node: &DeclarativePlanNode,
    ctx: &CompileContext,
) -> Result<Arc<dyn ExecutionPlan>, DeltaError> {
    match node {
        DeclarativePlanNode::Literal(n) => Ok(Arc::new(LiteralExec::try_new(
            n.schema.clone(),
            n.rows.clone(),
        )?)),
        DeclarativePlanNode::Scan(node) => scan::compile_scan(node),
        DeclarativePlanNode::FileListing(node) => {
            Ok(Arc::new(FileListingExec::new(node.path.clone())))
        }
        DeclarativePlanNode::Relation(handle) => compile_relation(handle, ctx),
        DeclarativePlanNode::Filter { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            Ok(Arc::new(KernelFilterExec::try_new(
                child_plan,
                input_schema,
                node.predicate.clone(),
            )?))
        }
        DeclarativePlanNode::Project { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            Ok(Arc::new(KernelProjectExec::try_new(
                child_plan,
                input_schema,
                &node.columns,
                node.output_schema.clone(),
            )?))
        }
        DeclarativePlanNode::Assert { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            Ok(Arc::new(KernelAssertExec::try_new(
                child_plan,
                input_schema,
                &node.checks,
            )?))
        }
        DeclarativePlanNode::Window { child, node } => {
            let child_plan = compile_declarative_node(child, ctx)?;
            let input_schema = node_output_schema(child)?;
            window::compile_window_node(child_plan, input_schema, node)
        }
        DeclarativePlanNode::Union { children, node } => {
            if children.is_empty() {
                return Err(crate::error::unsupported(
                    "Union with zero children is not supported yet",
                ));
            }
            let child_plans = children
                .iter()
                .map(|child| compile_declarative_node(child, ctx))
                .collect::<Result<Vec<_>, DeltaError>>()?;
            if node.ordered {
                let coalesced = child_plans
                    .into_iter()
                    .map(|plan| {
                        Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>
                    })
                    .collect::<Vec<_>>();
                Ok(Arc::new(
                    OrderedUnionExec::try_new(coalesced)
                        .map_err(crate::error::datafusion_err_to_delta)?,
                ))
            } else {
                let unioned = UnionExec::try_new(child_plans)
                    .map_err(crate::error::datafusion_err_to_delta)?;
                Ok(Arc::new(CoalescePartitionsExec::new(unioned)))
            }
        }
        DeclarativePlanNode::Join { build, probe, node } => {
            join::compile_join(build.as_ref(), probe.as_ref(), node, ctx)
        }
    }
}

fn node_output_schema(node: &DeclarativePlanNode) -> Result<SchemaRef, DeltaError> {
    match node {
        DeclarativePlanNode::Scan(n) => Ok(n.schema.clone()),
        DeclarativePlanNode::Literal(n) => Ok(n.schema.clone()),
        DeclarativePlanNode::Relation(h) => Ok(h.schema.clone()),
        DeclarativePlanNode::Project { node, .. } => Ok(node.output_schema.clone()),
        DeclarativePlanNode::Union { children, .. } => {
            let Some(first) = children.first() else {
                return Err(crate::error::unsupported("Union has no children"));
            };
            node_output_schema(first)
        }
        DeclarativePlanNode::Filter { child, .. } => node_output_schema(child),
        DeclarativePlanNode::Assert { child, .. } => node_output_schema(child),
        DeclarativePlanNode::Window { child, node } => {
            let child_schema = node_output_schema(child)?;
            window::window_output_kernel_schema(&child_schema, node)
        }
        DeclarativePlanNode::Join { build, probe, node } => match node.join_type {
            JoinType::LeftAnti => node_output_schema(probe),
            JoinType::Inner => {
                let build_schema = node_output_schema(build)?;
                let probe_schema = node_output_schema(probe)?;
                build_schema
                    .as_ref()
                    .add(probe_schema.fields().cloned())
                    .map(Arc::new)
                    .map_err(|e| {
                        crate::error::plan_compilation(format!(
                            "inner join combined output schema is invalid: {e}"
                        ))
                    })
            }
            other_join => Err(crate::error::unsupported(format!(
                "Schema inference for join type {other_join:?} is not implemented yet",
            ))),
        },
        DeclarativePlanNode::FileListing(_) => Err(crate::error::unsupported(
            "FileListing schema inference for Filter/Project is not wired yet",
        )),
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
