//! Plan and state machine types for Delta Kernel operations.
//!
//! This module provides a layered architecture with dual representations:
//!
//! ## Plans
//!
//! 1. **Individual Nodes** (`nodes.rs`) - Basic building blocks like `ScanNode`, `FilterNode`
//! 2. **Composite Plans** (`composite.rs`) - Typed operation plans like `CommitPhasePlan`
//! 3. **Declarative Plan** (`declarative.rs`) - Tree structure for runtime interpretation
//! 4. **Proto Conversion** (`proto_convert.rs`) - Convert to/from protobuf types
//!
//! ## State Machines
//!
//! 1. **Typed State Machines** (`state_machines.rs`) - `LogReplayPhase`, `SnapshotBuildPhase`
//! 2. **Declarative Phase** (`state_machines.rs`) - Abstract `DeclarativePhase` for runtime
//!
//! ## Dual Representation
//!
//! Both plans and state machines support compile-time and runtime representations:
//! - **Compile-time**: Use typed structures directly (know exact structure)
//! - **Runtime/Interpreter**: Use `DeclarativePlanNode` via `as_query_plan()` or
//!   `DeclarativePhase` via `as_declarative_phase()`

pub mod nodes;
pub mod composite;
pub mod declarative;
pub mod proto_convert;
pub mod state_machines;
pub mod function_registry;
pub mod kdf_implementations;
pub mod executor;

#[cfg(test)]
mod tests;

pub mod integration_test_data;

pub use nodes::*;
pub use composite::*;
pub use declarative::*;
pub use state_machines::*;
pub use function_registry::*;
pub use executor::*;

/// Trait for converting typed plans to tree representation.
///
/// This allows engines to choose between:
/// - Using typed composite plans directly (compile-time known structure)
/// - Converting to `DeclarativePlanNode` for generic tree execution
pub trait AsQueryPlan {
    /// Convert this plan to a `DeclarativePlanNode` tree.
    fn as_query_plan(&self) -> DeclarativePlanNode;
}

