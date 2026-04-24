//! Coarse categorization of what an SM is doing. Used for diagnostic tags
//! (tracing spans, metrics labels) — not on any execution-critical path.

/// The top-level category of an SM's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Loading a [`Snapshot`](crate::Snapshot) at a specific version.
    SnapshotBuild,
    /// Building a read `Scan` on top of a snapshot.
    Scan,
    /// Any write-path SM (insert / delete / update / table create / etc.).
    Write,
}
