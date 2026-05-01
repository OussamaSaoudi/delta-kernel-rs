//! DataFusion execution scaffold for Delta Kernel declarative [`Plan`] trees.
//!
//! Phase 1.1 supports only [`delta_kernel::plans::ir::nodes::SinkType::Results`] envelopes whose
//! root is a [`delta_kernel::plans::ir::DeclarativePlanNode::Literal`] leaf. Everything else
//! returns a typed [`delta_kernel::plans::errors::DeltaError`] via [`error::unsupported`].

pub mod compile;
pub mod error;
pub mod exec;
pub mod executor;

pub use error::{datafusion_err_to_delta, LiftDeltaErr};
pub use executor::DataFusionExecutor;
