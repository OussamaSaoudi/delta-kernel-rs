//! Shared schema constants used by multiple state machines.
//!
//! For schemas that map 1:1 to a Rust struct, prefer
//! [`crate::plans::record_schemas`] — those let [`scan_*_as::<T>`] infer the
//! schema at plan-build time. This module is for schemas that don't have a
//! natural struct counterpart: projections of existing schemas, runtime
//! compositions, and schemas with fields that appear elsewhere in the kernel.
//!
//! Constants are lazily built via [`std::sync::LazyLock`] so each schema is
//! constructed at most once per process, on first access.
//!
//! [`scan_*_as::<T>`]: crate::plans::ir::DeclarativePlanNode::scan_as
