
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

#[tokio::main]
async fn main() -> DeltaResult<()> {

    let path = std::fs::canonicalize(PathBuf::from(
        "/Users/oussama.saoudi/pyspark_playground/test_2000_large_commits",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    println!("Starting async execution...");
    let start = std::time::Instant::now();

    // Create async engine
    let task_executor = TokioMultiThreadExecutor::new(tokio::runtime::Handle::current());
    let object_store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngine::new(object_store, task_executor.into());

    let snapshot = Snapshot::builder(url).build(&engine)?;
    let plan = snapshot.get_scan_plan()?;

    println!("Plan created in {:?}", start.elapsed());

    // Create async executor
    let executor = AsyncPlanExecutor::new(Arc::new(engine));

    // Execute the plan and get a stream
    let exec_start = std::time::Instant::now();
    let mut stream = executor.execute(plan).await?;
    println!("Stream created in {:?}", exec_start.elapsed());

    // Collect results from the stream with metrics
    let mut batches = Vec::new();
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
        batches.push(filtered);

        // Report progress every second
        if last_report.elapsed() >= std::time::Duration::from_secs(1) {
            let elapsed = stream_start.elapsed();
            let batches_per_sec = batch_count as f64 / elapsed.as_secs_f64();
            let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
            println!(
                "Progress: {} batches, {} rows, {:.2} batches/sec, {:.2} rows/sec",
                batch_count, total_rows, batches_per_sec, rows_per_sec
            );
            last_report = std::time::Instant::now();
        }
    }

    let total_time = start.elapsed();
    println!("\n=== Summary ===");
    println!("Total time: {:?}", total_time);
    println!("Total batches: {}", batch_count);
    println!("Total rows: {}", total_rows);
    println!("Selected rows: {}", total_selected);
    println!("Batches/sec: {:.2}", batch_count as f64 / total_time.as_secs_f64());
    println!("Rows/sec: {:.2}", total_rows as f64 / total_time.as_secs_f64());

    // Optionally print first few batches
    // if !batches.is_empty() {
    //     println!("\n=== First few results ===");
    //     print_batches(&batches[..batches.len().min(5)])?;
    // }

    Ok(())
}
