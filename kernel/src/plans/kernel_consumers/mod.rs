//! Kernel-Defined Functions (KDFs) -- stateful per-row logic the kernel owns.
//!
//! KDFs encapsulate Delta-specific per-row work (checkpoint hint extraction,
//! protocol/metadata harvesting, sidecar collection) that engines can't interpret.
//!
//! The IR exposes one KDF shape: [`KernelConsumer`], an observer over batches
//! returning `Continue` / `Break`. It's wired into a plan via
//! [`EngineRequest::Consume`](crate::plans::state_machines::framework::step::EngineRequest::Consume);
//! the consumer drains the terminal row stream and accumulates finalized state for the engine
//! to harvest.
//!
//! KDFs dispatch in-process and never cross a serialization boundary.
//!
//! Each [`ConsumerHandle`] carries a [`KernelConsumerToken`] (`{ kind, id }`, stamped at
//! plan-build time, keys the executor's state table and the paired [`Extractor`]).

pub mod checkpoint_hint;
pub mod consumer;
pub mod handle;
pub mod metadata_protocol;
pub mod sidecar_collector;

pub use checkpoint_hint::{CheckpointHintReader, CheckpointHintRecord};
pub use consumer::{
    KdfControl, KernelConsumer, KernelConsumerKind, KernelConsumerOutput, KernelConsumerToken,
};
pub use handle::{ConsumerHandle, Extractor, FinishedHandle};
pub use metadata_protocol::MetadataProtocolReader;
pub use sidecar_collector::SidecarCollector;
