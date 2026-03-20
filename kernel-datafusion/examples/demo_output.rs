//! Demo: DataFusion query output in SQL-style format.

use std::sync::Arc;
use std::path::PathBuf;

use arrow::util::pretty::pretty_format_batches;
use delta_kernel::expressions::{column_expr, Expression, Scalar};
use delta_kernel::scan::ScanMetadata;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel_datafusion::{DataFusionExecutor, SnapshotAsyncBuilderExt, ScanAsyncExt};
use futures::StreamExt;

fn test_data_path(relative_path: &str) -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(format!("{}/../acceptance/tests/dat/out/reader_tests/generated/{}", manifest_dir, relative_path))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table_path = test_data_path("basic_append/delta");
    if !table_path.exists() {
        eprintln!("Table not found: {:?}", table_path);
        return Ok(());
    }

    let mut table_url = url::Url::from_file_path(table_path.canonicalize()?).unwrap();
    if !table_url.path().ends_with('/') {
        table_url.set_path(&format!("{}/", table_url.path()));
    }

    let executor = Arc::new(DataFusionExecutor::new()?);

    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║   DataFusion Delta Query Demo                                        ║");
    println!("║   Table: basic_append (version 1)                                    ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!();

    // Build snapshot at version 1
    let snapshot = delta_kernel::snapshot::Snapshot::async_builder(table_url.clone())
        .with_version(1)
        .build(executor.as_ref())
        .await?;

    println!("── Equivalent SQL: SELECT path, size FROM read_metadata('<table>', 1) ──");
    println!();

    // Scan metadata (equivalent to read_metadata)
    let scan = Arc::new(snapshot).scan_builder().build()?;
    let mut stream = std::pin::pin!(scan.scan_metadata_async(executor.clone()));

    let mut all_batches = Vec::new();
    while let Some(result) = stream.next().await {
        let metadata: ScanMetadata = result?;
        let batch = metadata.scan_files.data()
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("Should be ArrowEngineData")
            .record_batch()
            .clone();
        all_batches.push(batch);
    }

    if !all_batches.is_empty() {
        println!("{}", pretty_format_batches(&all_batches)?);
    }
    println!();

    // Scan with predicate (equivalent to read_data with WHERE)
    println!("── Equivalent SQL: SELECT * FROM read_data('<table>', 1) WHERE number >= 2 ──");
    println!();

    let predicate = Arc::new(column_expr!("number").ge(Expression::Literal(Scalar::Long(2))));
    let snapshot2 = delta_kernel::snapshot::Snapshot::async_builder(table_url)
        .with_version(1)
        .build(executor.as_ref())
        .await?;
    let scan_with_pred = Arc::new(snapshot2).scan_builder()
        .with_predicate(predicate)
        .build()?;

    let mut stream2 = std::pin::pin!(scan_with_pred.scan_metadata_async(executor.clone()));
    let mut file_count = 0;
    while let Some(result) = stream2.next().await {
        let metadata: ScanMetadata = result?;
        // Count files that pass data skipping
        let sv = metadata.scan_files.selection_vector();
        let selected: usize = sv.iter().filter(|&&b| b).count();
        file_count += selected;
    }

    println!("Files after data skipping (number >= 2): {}", file_count);
    println!();
    println!("Note: DataFusion uses async streaming APIs rather than SQL table functions.");
    println!("The same data is returned, just via a different interface.");

    Ok(())
}
