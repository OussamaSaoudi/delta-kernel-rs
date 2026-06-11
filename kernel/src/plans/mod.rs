//! Plan IR and execution framework.
//!
//! The `plans` module defines an engine-agnostic intermediate representation for the work an
//! engine must perform on behalf of the kernel (scan files, apply filters, project columns,
//! collect results) along with the state-machine framework that drives the plan executor.
//!
//! Entry points:
//! - [`ir::plan`][crate::plans::ir::plan] -- `Plan` / `PlanNode` / `NodeKind` / `RefId` and the
//!   terminal `ResultPlan` the engine compiles to a single dataflow DAG.
//! - [`ir::operation`][crate::plans::ir::operation] -- top-level `Operation` dispatch (I/O vs
//!   query) consumed by [`PlanExecutor`].
//! - [`state_machines::framework::plan_context`][crate::plans::state_machines::framework::plan_context] --
//!   the `Context` / `PlanBuilder` API SM bodies use to construct plans.
//! - [`state_machines::framework::coroutine`][crate::plans::state_machines::framework::coroutine]
//!   -- coroutine-backed `StateMachine` implementation.
//!
//! # Feature gate
//!
//! This module is opt-in behind `declarative-plans`. The kernel's existing
//! `Scan`/`Snapshot`/`Transaction` APIs continue to work without it.

pub mod errors;
pub mod ir;
pub mod kernel_reducers;
mod query_builder;
pub mod schema_expr;
pub mod state_machines;

use bytes::Bytes;
pub use ir::{IoOperation, Operation};
pub use query_builder::QueryPlanBuilder;

use crate::{AsAny, DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileMeta};

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, relational plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and returns the result.
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult>;
}

/// The result of executing an [`Operation`].
///
/// Each variant describes a different shape of output that a plan can possibly produce.
pub enum PlanResult {
    /// A stream of columnar data batches (as [`EngineData`]) produced by the plan.
    Data(DeltaResultIteratorStatic<Box<dyn EngineData>>),
    /// A stream of file metadata entries.
    FileMeta(DeltaResultIteratorStatic<FileMeta>),
    /// A stream of raw byte buffers.
    Bytes(DeltaResultIteratorStatic<Bytes>),
    /// Represents the successful completion of a plan, but with no return value.
    Unit,
}

impl PlanResult {
    pub fn into_data(self) -> DeltaResult<DeltaResultIteratorStatic<Box<dyn EngineData>>> {
        match self {
            Self::Data(iter) => Ok(iter),
            other => Err(other.type_mismatch("Data")),
        }
    }

    pub fn into_file_meta(self) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
        match self {
            Self::FileMeta(iter) => Ok(iter),
            other => Err(other.type_mismatch("FileMeta")),
        }
    }

    pub fn into_bytes(self) -> DeltaResult<DeltaResultIteratorStatic<Bytes>> {
        match self {
            Self::Bytes(iter) => Ok(iter),
            other => Err(other.type_mismatch("Bytes")),
        }
    }

    pub fn into_unit(self) -> DeltaResult<()> {
        match self {
            Self::Unit => Ok(()),
            other => Err(other.type_mismatch("Unit")),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Data(_) => "Data",
            Self::FileMeta(_) => "FileMeta",
            Self::Bytes(_) => "Bytes",
            Self::Unit => "Unit",
        }
    }

    /// Build an [`Error::PlanResultTypeMismatch`] reporting `self`'s variant as the actual one.
    fn type_mismatch(&self, expected: &'static str) -> Error {
        Error::plan_result_type_mismatch(expected, self.variant_name())
    }
}
