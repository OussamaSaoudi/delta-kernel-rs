use std::{path::PathBuf, sync::Arc};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::arrow::util::pretty::print_batches;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Snapshot};
use delta_kernel::async_df::AsyncPlanExecutor;
use delta_kernel::kernel_df::FilteredEngineDataArc;
use num_format::{Locale, ToFormattedString};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> DeltaResult<()> {
    // Get table path from command line argument
    let table_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| {
            eprintln!("Usage: {} <table_path>", std::env::args().next().unwrap());
            eprintln!("Example: {} ./test_table", std::env::args().next().unwrap());
            std::process::exit(1);
        });

    let path = std::fs::canonicalize(PathBuf::from(&table_path))
        .unwrap_or_else(|e| {
            eprintln!("Error: Cannot find table at '{}': {}", table_path, e);
            std::process::exit(1);
        });

    let url = url::Url::from_directory_path(&path).unwrap();

    println!("=== Delta Table Async Benchmark ===");
    println!("Table path: {}", path.display());

    let start = std::time::Instant::now();

    // Create async engine
    let task_executor = TokioMultiThreadExecutor::new(tokio::runtime::Handle::current());
    let object_store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngine::new(object_store, task_executor.into());

    let snapshot = crate::Snapshot::builder_for(url).build(&engine)?;
    let plan = snapshot.get_scan_plan()?;
    println!("Plan created in {:?}", start.elapsed());

    // Create async executor
    let executor = AsyncPlanExecutor::new(Arc::new(engine));

    // Execute the plan and get a stream
    let exec_start = std::time::Instant::now();
    let mut stream = executor.execute(plan).await?;
    println!("Stream created in {:?}", exec_start.elapsed());

    // Collect results from the stream with metrics
    let mut batch_count = 0;
    let mut total_rows = 0;
    let mut total_selected = 0;

    let stream_start = std::time::Instant::now();
    let mut last_report = stream_start;

    while let Some(result) = stream.next().await {
        let batch = result?;
        let FilteredEngineDataArc {
            engine_data,
            selection_vector,
        } = batch;

        batch_count += 1;
        total_rows += engine_data.len();
        total_selected += selection_vector.iter().filter(|&&x| x).count();

        let record_batch: RecordBatch = engine_data
            .as_any()
            .downcast_ref::<ArrowEngineData>()
            .unwrap()
            .record_batch()
            .clone();

        let filtered = filter_record_batch(&record_batch, &selection_vector.into())?;

        // Report progress every second
        if last_report.elapsed() >= std::time::Duration::from_secs(1) {
            let elapsed = stream_start.elapsed();
            let batches_per_sec = batch_count as f64 / elapsed.as_secs_f64();
            let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
            println!(
                "Progress: {} batches, {} rows, {:.2} batches/sec, {} rows/sec",
                batch_count.to_formatted_string(&Locale::en),
                total_rows.to_formatted_string(&Locale::en),
                batches_per_sec,
                (rows_per_sec as usize).to_formatted_string(&Locale::en)
            );
            last_report = std::time::Instant::now();
        }
    }

    let total_time = start.elapsed();
    let total_rows_per_sec = total_rows as f64 / total_time.as_secs_f64();
    let total_batches_per_sec = batch_count as f64 / total_time.as_secs_f64();

    println!("\n=== Summary ===");
    println!("Total time: {:?}", total_time);
    println!("Total batches: {}", batch_count.to_formatted_string(&Locale::en));
    println!("Total rows: {}", total_rows.to_formatted_string(&Locale::en));
    println!("Selected rows: {}", total_selected.to_formatted_string(&Locale::en));
    println!("Batches/sec: {:.2}", total_batches_per_sec);
    println!("Rows/sec: {}", (total_rows_per_sec as usize).to_formatted_string(&Locale::en));

    // Optionally print first few batches
    // if !batches.is_empty() {
    //     println!("\n=== First few results ===");
    //     print_batches(&batches[..batches.len().min(5)])?;
    // }

    Ok(())
}
