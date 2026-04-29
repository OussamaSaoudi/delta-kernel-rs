//! The [`Plan`] envelope.
//!
//! A [`Plan`] pairs a transforms-only plan tree with the sink describing how
//! its output is consumed. Every complete plan has exactly one sink, and the
//! sink lives on the envelope — never inside the tree.

use super::declarative::DeclarativePlanNode;
use super::nodes::SinkNode;

/// Complete plan: a transforms-only [`DeclarativePlanNode`] tree terminated
/// by a [`SinkNode`].
#[derive(Debug, Clone)]
pub struct Plan {
    pub root: DeclarativePlanNode,
    pub sink: SinkNode,
}

impl Plan {
    /// Assemble a plan from a root subtree and a sink.
    pub fn new(root: DeclarativePlanNode, sink: SinkNode) -> Self {
        Self { root, sink }
    }
}
