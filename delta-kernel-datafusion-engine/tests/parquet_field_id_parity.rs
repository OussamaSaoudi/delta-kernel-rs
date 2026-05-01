//! Field-ID parity between Delta Kernel parquet reads and DataFusion lowering is **not** wired yet.
//!
//! Kernel [`delta_kernel::engine::Engine`](delta_kernel::engine::Engine) parquet handlers match
//! logical columns using parquet field IDs when present on [`delta_kernel::schema::StructField`].
//! This declarative scan path builds a [`datafusion_datasource_parquet::source::ParquetSource`]
//! from a flat Arrow schema converted from kernel `StructType`, so column-mapping / DV-heavy tables
//! risk name-first coercion drift versus writers that renamed physical columns.
//!
//! Planned integration surfaces (pick one when implementing):
//! - propagate parquet field IDs into Arrow schema field metadata for reads, plus adapt projection
//!   pushdown to consult IDs; or
//! - plug a kernel-aware [`PhysicalExprAdapterFactory`] into
//!   [`FileScanConfig`](datafusion_datasource::file_scan_config::FileScanConfig).
//!
//! Blockers today: declarative [`delta_kernel::plans::ir::nodes::ScanNode`] carries logical
//! [`delta_kernel::schema::StructType`] without guaranteed per-column parquet field-id hints for
//! arbitrary connector-provided scans.

#[tokio::test]
#[ignore = "field-id-aware parquet projection for declarative scans is not implemented"]
async fn parquet_scan_respects_kernel_parquet_field_ids() {
    unimplemented!(
        "tracked in delta-kernel-datafusion-engine/tests/parquet_field_id_parity.rs module docs"
    );
}
