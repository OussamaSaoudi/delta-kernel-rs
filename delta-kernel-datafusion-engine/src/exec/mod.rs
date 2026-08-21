//! Custom physical operators and table providers for the DataFusion engine.

mod cache_provider;
mod field_id_adapter;
mod load_exec;
mod load_helpers;
mod load_provider;
mod static_scan_provider;

pub(crate) use cache_provider::SharedNodeTableProvider;
pub(crate) use load_exec::LoadExec;
pub(crate) use load_provider::LoadTableProvider;
pub(crate) use static_scan_provider::StaticScanTableProvider;
