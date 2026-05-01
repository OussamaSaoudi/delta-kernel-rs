//! Custom physical operators for the DataFusion engine.

mod literal;
mod shape;
mod sources;

pub use literal::LiteralExec;
pub use shape::{
    ApplySchemaExec, KernelAssertExec, KernelConsumeByKdfExec, KernelFilterExec,
    KernelPartitionedWriteExec, KernelProjectExec, KernelRowNumberWindowExec,
    NullabilityValidationExec, OrderedUnionExec, RelationSinkExec, RowIndexExec,
};
pub use sources::{FileListingExec, RelationBatchRegistry, RelationRefExec};
