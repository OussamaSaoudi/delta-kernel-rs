//! The unit of work an SM hands to the executor each tick.

use crate::plans::ir::Plan;

/// A metadata-only read: ask the engine to open a parquet file, read its
/// schema from the footer, and deliver it back through a
/// [`PhaseKdfState`](super::phase_kdf_state::PhaseKdfState) entry keyed by
/// `token`.
///
/// Distinct from a data-carrying [`Plan`]: no row stream, no sink, no
/// KDF-producing pipeline — the executor just does a footer read.
#[derive(Debug, Clone)]
pub struct SchemaQueryNode {
    /// Path to the parquet file whose schema the kernel wants.
    pub file_path: String,
    /// Kernel-internal token matching the SM's eventual
    /// `PhaseKdfState::take_by_token` call.
    pub token: String,
}

impl SchemaQueryNode {
    pub fn new(file_path: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
            token: token.into(),
        }
    }
}

/// What [`StateMachine::get_operation`](super::state_machine::StateMachine::get_operation)
/// hands to the executor.
///
/// Separates the two concerns the executor understands:
///
/// - [`Plans`](Self::Plans) — one or more independent data pipelines
///   terminated by sinks. When the vec holds a single plan, this is the
///   common case. When it holds multiple, they are independent and the
///   executor may run them concurrently; KDF state is merged across all.
/// - [`SchemaQuery`](Self::SchemaQuery) — metadata-only footer read.
#[derive(Debug, Clone)]
pub enum PhaseOperation {
    /// One or more independent plans to run, plus merged KDF state.
    Plans(Vec<Plan>),
    /// Read a file's schema without reading data.
    SchemaQuery(SchemaQueryNode),
}
