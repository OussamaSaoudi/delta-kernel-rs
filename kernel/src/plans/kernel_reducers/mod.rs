//! Kernel-Defined Functions (KDFs) -- stateful per-row logic the kernel owns.
//!
//! KDFs encapsulate Delta-specific per-row work (checkpoint hint extraction,
//! protocol/metadata harvesting, sidecar collection) that engines can't interpret.
//!
//! The IR exposes one KDF shape: [`KernelReducer`], an observer over batches
//! returning `Continue` / `Break`. It's wired into a plan via
//! [`EngineRequest::Reduce`](crate::plans::state_machines::framework::step::EngineRequest::Reduce);
//! the reducer drains the terminal row stream and accumulates finalized state for the engine
//! to harvest.
//!
//! KDFs dispatch in-process and never cross a serialization boundary.
//!
//! Each [`ReducerHandle`] carries a [`KernelReducerToken`] (`{ kind, id }`, stamped at
//! plan-build time, keys the executor's state table and the paired [`Extractor`]).

pub mod checkpoint_hint;
pub mod handle;
pub mod metadata_protocol;
pub mod reducer;
pub mod sidecar_collector;

pub use checkpoint_hint::{CheckpointHintReader, CheckpointHintRecord};
pub use handle::{Extractor, FinishedHandle, ReducerHandle};
pub use metadata_protocol::MetadataProtocolReader;
pub use reducer::{
    KdfControl, KernelReducer, KernelReducerKind, KernelReducerOutput, KernelReducerToken,
};
pub use sidecar_collector::SidecarCollector;
