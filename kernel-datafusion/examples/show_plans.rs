//! Example that shows all plans executed when building a snapshot.
//!
//! Run with:
//! ```
//! RUST_LOG=info cargo run --package delta_kernel_datafusion --example show_plans -- /path/to/delta/table
//! ```

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion_common::config::ConfigOptions;
use delta_kernel_datafusion::executor::DataFusionExecutor;
use delta_kernel_datafusion::snapshot_builder::AsyncSnapshotBuilder;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to see the plan logs
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("delta_kernel_datafusion=info".parse()?),
        )
        .with_target(false)
        .init();

    // Get table path from command line args
    let args: Vec<String> = std::env::args().collect();
    let table_path = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("file:///Users/oussama.saoudi/pyspark_playground/test_2000_large_commits/");

    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║   Building Snapshot for: {}", table_path);
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!();

    // Create the table URL
    let mut table_url = if table_path.starts_with("file://") || table_path.starts_with("s3://") {
        Url::parse(table_path)?
    } else {
        Url::from_file_path(table_path).map_err(|_| format!("Invalid path: {}", table_path))?
    };
    // Get mutable path segments
    {
        let mut path_segments = table_url
            .path_segments_mut()
            .expect("Cannot get path segments");

        // pop_if_empty() removes a trailing slash if one exists
        // so that we don't end up with a double slash (e.g., //)
        path_segments.pop_if_empty();

        // push("") adds an empty segment, which results in a single trailing slash
        path_segments.push("");
    }

    // Create config with row group parallelism enabled
    let mut config = ConfigOptions::default();
    config.optimizer.repartition_file_scans = true;

    let runtime = Arc::new(RuntimeEnv::default());
    let session_state = SessionStateBuilder::new()
        .with_config(config.into())
        .with_runtime_env(runtime)
        .build();

    // Create executor with custom session state and parallel dedup enabled
    let executor = DataFusionExecutor::with_session_state(session_state).with_parallel_dedup(true);

    // Build snapshot - this will log all the plans
    println!("Building snapshot... for table: {table_url}\n");
    let snapshot = AsyncSnapshotBuilder::new(table_url)
        .build(&executor)
        .await?;

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║   Snapshot Built Successfully!                                        ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!(
        "║   Version: {:>10}                                                 ║",
        snapshot.version()
    );
    println!("╚══════════════════════════════════════════════════════════════════════╝");

    Ok(())
}
