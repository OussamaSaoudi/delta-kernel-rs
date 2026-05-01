//! Custom physical operators for the DataFusion engine.

mod literal;
mod shape;
mod sources;

pub use literal::LiteralExec;
pub use shape::{
    ApplySchemaExec, KernelFilterExec, KernelProjectExec, NullabilityValidationExec,
    OrderedUnionExec, RowIndexExec,
};
pub use sources::{FileListingExec, RelationBatchRegistry, RelationRefExec};
