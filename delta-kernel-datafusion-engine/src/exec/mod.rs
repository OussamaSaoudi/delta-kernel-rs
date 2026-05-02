//! Custom physical operators for the DataFusion engine.

mod literal;
mod shape;
mod sources;

pub use literal::LiteralExec;
pub use shape::{
    KernelAssertExec, KernelConsumeByKdfExec, KernelLoadSinkExec, KernelPartitionedWriteExec,
    NullabilityValidationExec, OrderedUnionExec, RelationSinkExec, RowIndexExec,
};
pub use sources::{FileListingExec, RelationBatchRegistry, build_relation_ref_exec};
