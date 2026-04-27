//! Consumer KDF implementations — observers that fold batches into typed
//! output without altering the row stream.
//!
//! Each submodule declares one state type + its `Kdf` / `ConsumerKdf` /
//! `KdfOutput` / `RowVisitor` impls. The [`impl_kdf!`](crate::impl_kdf)
//! macro collapses the trivial `Kdf` boilerplate (`kdf_id` + `finish`) to a
//! single line per KDF.

pub mod checkpoint_hint;
pub mod metadata_protocol;
pub mod sidecar_collector;

pub use checkpoint_hint::CheckpointHintReader;
pub use metadata_protocol::MetadataProtocolReader;
pub use sidecar_collector::SidecarCollector;
