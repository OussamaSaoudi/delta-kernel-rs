//! Schema/expression utilities shared by plan builders.
//!
//! These tools operate on [`StructType`][crate::schema::StructType] and
//! [`Expression`][crate::expressions::Expression] without depending on the plan-construction
//! state machinery. They live here -- separate from `ir` (IR data types) and the SM framework
//! -- because they're builder-time helpers reused by multiple call sites (the kernel
//! [`PlanBuilder`][crate::plans::state_machines::framework::plan_context::PlanBuilder] *and*
//! engine-side lowering).
//!
//! - `check` -- bidirectional type checker for builder schema derivation
//!   (`check_expression`, `check_column_refs`). Kernel-internal.
//! - [`field_op`] -- nested struct edits (`FieldOp` + `compile_field_op`) plus a small set of
//!   schema/expression conveniences (`arc_struct_or_invariant`, `identity_named_expr`,
//!   plus the publicly re-exported [`field_op::load_output_schema`] shared with engine-side
//!   lowering).

pub(crate) mod check;
pub mod field_op;
