//! Async streaming driver for state machines.
//!
//! This module provides async drivers for executing Delta kernel state machines
//! via DataFusion. It supports:
//!
//! - **Snapshot construction**: `build_snapshot_async`, `build_snapshot_at_version_async`
//! - **Scan metadata streaming**: `scan_metadata_stream_async`
//! - **Generic state machine execution**: `execute_state_machine_async`, `results_stream`
//!
//! # Sink Handling
//!
//! State machine plans can have two sink types:
//! - **Results sink**: Yields `FilteredEngineData` batches to the caller
//! - **Drop sink**: Drains silently for side effects (e.g., dedup state accumulation)
//!
//! The drivers in this module automatically handle both sink types correctly.

use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::plans::state_machines::{AdvanceResult, SnapshotStateMachine, StateMachine};
use delta_kernel::plans::DeclarativePlanNode;
use delta_kernel::scan::{ScanMetadata, ScanState};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::DeltaResult;

use crate::executor::DataFusionExecutor;

/// Async stream-based results driver for state machines.
///
/// This is the async equivalent of `ResultsDriver` in the kernel crate.
/// It drives a state machine by:
/// - Getting the next plan via `get_plan()`
/// - Executing it via DataFusion
/// - Yielding batches for `Results` sinks
/// - Draining batches for `Drop` sinks
/// - Advancing the state machine via `advance()`
pub struct ResultsStreamDriver<SM: StateMachine> {
    executor: DataFusionExecutor,
    sm: Option<SM>,
    current_stream:
        Option<Pin<Box<dyn Stream<Item = Result<FilteredEngineData, delta_kernel::Error>> + Send>>>,
    current_plan: Option<DeclarativePlanNode>,
    is_done: bool,
}

impl<SM: StateMachine> ResultsStreamDriver<SM> {
    pub fn new(executor: DataFusionExecutor, sm: SM) -> Self {
        Self {
            executor,
            sm: Some(sm),
            current_stream: None,
            current_plan: None,
            is_done: false,
        }
    }

    /// Check if the driver has finished.
    pub fn is_done(&self) -> bool {
        self.is_done
    }
}

impl<SM: StateMachine + Unpin> Stream for ResultsStreamDriver<SM> {
    type Item = DeltaResult<FilteredEngineData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_done {
            return Poll::Ready(None);
        }

        loop {
            // 1) If we have an active stream, poll it
            if let Some(stream) = self.current_stream.as_mut() {
                match stream.as_mut().poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(batch))) => {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        self.is_done = true;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                    Poll::Ready(None) => {
                        // Stream exhausted: advance the state machine
                        self.current_stream = None;
                        let plan = self.current_plan.take().expect("plan must exist");

                        let sm = self.sm.as_mut().expect("sm must exist");
                        match sm.advance(Ok(plan)) {
                            Ok(AdvanceResult::Continue) => continue,
                            Ok(AdvanceResult::Done(_)) => {
                                self.is_done = true;
                                return Poll::Ready(None);
                            }
                            Err(e) => {
                                self.is_done = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }
            }

            // 2) No active stream: get next plan from state machine
            let sm = match self.sm.as_mut() {
                Some(sm) => sm,
                None => {
                    self.is_done = true;
                    return Poll::Ready(None);
                }
            };

            let plan = match sm.get_plan() {
                Ok(p) => p,
                Err(e) => {
                    self.is_done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            };

            // TODO: For now, we need to execute synchronously. In a real impl, we'd spawn tasks.
            // This is a placeholder that will be filled with proper async execution.
            return Poll::Ready(Some(Err(delta_kernel::Error::generic(
                "ResultsStreamDriver async execution not yet implemented - use helper function",
            ))));
        }
    }
}

/// Helper function to create an async stream from a state machine.
///
/// This is the recommended API for now as it handles async execution properly.
pub fn results_stream<SM: StateMachine + Send + 'static>(
    sm: SM,
    executor: DataFusionExecutor,
) -> impl Stream<Item = DeltaResult<FilteredEngineData>> + Send
where
    SM::Result: Send,
{
    async_stream::try_stream! {
        let mut sm = sm;

        loop {
            let plan = sm.get_plan()?;

            // Execute the plan via DataFusion
            let df_stream = executor.execute_to_stream(plan.clone()).await
                .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

            if plan.is_results_sink() {
                // Results sink: yield batches
                futures::pin_mut!(df_stream);
                while let Some(batch_result) = df_stream.next().await {
                    let batch = batch_result
                        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

                    // Convert RecordBatch to FilteredEngineData with all-true selection
                    let engine_data: Box<dyn delta_kernel::EngineData> =
                        Box::new(delta_kernel::engine::arrow_data::ArrowEngineData::new(batch));
                    let len = engine_data.len();
                    yield delta_kernel::engine_data::FilteredEngineData::try_new(
                        engine_data,
                        vec![true; len],
                    )?;
                }
            } else {
                // Drop sink: drain for side effects
                futures::pin_mut!(df_stream);
                while let Some(batch_result) = df_stream.next().await {
                    let _batch = batch_result
                        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
                }
            }

            // Advance the state machine
            match sm.advance(Ok(plan))? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(_) => break,
            }
        }
    }
}

/// Execute a state machine to completion asynchronously.
///
/// This is the async equivalent of `execute_state_machine` in the kernel.
/// It drives the state machine by executing each plan via DataFusion
/// and advancing until completion.
///
/// # Error Handling
///
/// Some state machine phases (like `CheckpointHint`) handle execution errors gracefully.
/// For example, if `_last_checkpoint` doesn't exist, the state machine continues
/// without the hint. This driver passes execution errors to `advance()` to let the
/// state machine decide how to handle them.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_datafusion::{DataFusionExecutor, execute_state_machine_async};
/// use delta_kernel::plans::state_machines::SnapshotStateMachine;
///
/// let executor = DataFusionExecutor::new()?;
/// let sm = SnapshotStateMachine::new(table_root)?;
/// let snapshot = execute_state_machine_async(&executor, sm).await?;
/// ```
pub async fn execute_state_machine_async<SM>(
    executor: &DataFusionExecutor,
    mut sm: SM,
) -> DeltaResult<SM::Result>
where
    SM: StateMachine + Send,
    SM::Result: Send,
{
    loop {
        let plan = sm.get_plan()?;

        // Try to compile and execute via DataFusion
        // If execution fails, we pass the error to advance() to let the state machine
        // decide how to handle it (e.g., CheckpointHint phase handles FileNotFound gracefully)
        let execution_result = execute_plan_async(executor, plan.clone()).await;

        // Advance with the execution result
        // The state machine may handle certain errors gracefully (e.g., missing _last_checkpoint)
        let advance_result = match execution_result {
            Ok(()) => sm.advance(Ok(plan))?,
            Err(e) => sm.advance(Err(e))?,
        };

        match advance_result {
            AdvanceResult::Continue => continue,
            AdvanceResult::Done(result) => return Ok(result),
        }
    }
}

/// Execute a single plan via DataFusion, draining the stream for side effects.
///
/// Returns Ok(()) if execution succeeded, or an appropriate kernel error.
async fn execute_plan_async(
    executor: &DataFusionExecutor,
    plan: DeclarativePlanNode,
) -> DeltaResult<()> {
    // Compile and execute via DataFusion
    let df_stream = executor
        .execute_to_stream(plan)
        .await
        .map_err(|e| convert_datafusion_error(e))?;

    // Drain the stream to trigger side effects (KDF state mutations)
    futures::pin_mut!(df_stream);
    while let Some(batch_result) = df_stream.next().await {
        batch_result.map_err(|e| convert_datafusion_error(e))?;
    }

    Ok(())
}

/// Convert a DataFusion error to an appropriate kernel error type.
///
/// This ensures that file-not-found errors are properly typed so that
/// state machines can handle them gracefully.
fn convert_datafusion_error(e: impl std::fmt::Display) -> delta_kernel::Error {
    let msg = e.to_string();

    // Check for common "not found" patterns in the error message
    if msg.contains("not found") || msg.contains("No such file") || msg.contains("NotFound") {
        delta_kernel::Error::file_not_found(msg)
    } else {
        delta_kernel::Error::generic(msg)
    }
}

/// Build a snapshot asynchronously using DataFusion execution.
///
/// This is the recommended entry point for DataFusion-backed snapshot construction.
/// It creates a `SnapshotStateMachine` and drives it to completion using DataFusion
/// for plan execution.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_datafusion::{DataFusionExecutor, build_snapshot_async};
///
/// let executor = DataFusionExecutor::new()?;
/// let table_url = url::Url::parse("file:///path/to/delta/table")?;
/// let snapshot = build_snapshot_async(&executor, table_url).await?;
/// println!("Table version: {}", snapshot.version());
/// ```
pub async fn build_snapshot_async(
    executor: &DataFusionExecutor,
    table_root: url::Url,
) -> DeltaResult<Snapshot> {
    let sm = SnapshotStateMachine::new(table_root)?;
    execute_state_machine_async(executor, sm).await
}

/// Build a snapshot at a specific version asynchronously.
///
/// Similar to `build_snapshot_async`, but targets a specific table version
/// instead of the latest version.
///
/// # Example
///
/// ```ignore
/// use delta_kernel_datafusion::{DataFusionExecutor, build_snapshot_at_version_async};
///
/// let executor = DataFusionExecutor::new()?;
/// let table_url = url::Url::parse("file:///path/to/delta/table")?;
/// let snapshot = build_snapshot_at_version_async(&executor, table_url, 5).await?;
/// assert_eq!(snapshot.version(), 5);
/// ```
pub async fn build_snapshot_at_version_async(
    executor: &DataFusionExecutor,
    table_root: url::Url,
    version: i64,
) -> DeltaResult<Snapshot> {
    let sm = SnapshotStateMachine::with_version(table_root, version)?;
    execute_state_machine_async(executor, sm).await
}

// =============================================================================
// Scan Metadata Streaming
// =============================================================================

/// Create an async stream of [`ScanMetadata`] from a [`ScanState`].
///
/// This is the async equivalent of [`Scan::scan_metadata`] but uses DataFusion
/// for plan execution instead of the sync kernel engine.
///
/// Takes **ownership** of `ScanState` and `Arc<DataFusionExecutor>` to avoid
/// lifetime complications. The stream owns all its data.
///
/// # Usage
///
/// ```ignore
/// use delta_kernel_datafusion::{DataFusionExecutor, scan_metadata_stream_async};
/// use futures::StreamExt;
/// use std::sync::Arc;
///
/// let executor = Arc::new(DataFusionExecutor::new()?);
/// let snapshot = build_snapshot_async(&executor, table_url).await?;
/// let scan = snapshot.scan_builder().build()?;
/// let scan_state = scan.into_scan_state()?;
///
/// let mut stream = scan_metadata_stream_async(scan_state, executor);
/// while let Some(result) = stream.next().await {
///     let scan_metadata = result?;
///     // Process scan_metadata.scan_files and scan_metadata.scan_file_transforms
/// }
/// ```
pub fn scan_metadata_stream_async(
    scan_state: ScanState,
    executor: Arc<DataFusionExecutor>,
) -> impl Stream<Item = DeltaResult<ScanMetadata>> + Send {
    let ScanState {
        state_machine,
        transform_computer,
    } = scan_state;

    async_stream::try_stream! {
        let mut sm = state_machine;
        let computer = transform_computer;

        loop {
            let plan = sm.get_plan()?;

            // Execute the plan via DataFusion
            let df_stream = executor.execute_to_stream(plan.clone()).await
                .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

            if plan.is_results_sink() {
                // Results sink: process batches and yield ScanMetadata
                futures::pin_mut!(df_stream);
                while let Some(batch_result) = df_stream.next().await {
                    let batch = batch_result
                        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

                    // Convert RecordBatch to FilteredEngineData
                    let engine_data: Box<dyn delta_kernel::EngineData> =
                        Box::new(delta_kernel::engine::arrow_data::ArrowEngineData::new(batch));
                    let len = engine_data.len();
                    let filtered = delta_kernel::engine_data::FilteredEngineData::try_new(
                        engine_data,
                        vec![true; len],
                    )?;

                    // Skip empty batches (all rows filtered)
                    if !filtered.selection_vector().contains(&true) {
                        continue;
                    }

                    // Compute transforms for this batch
                    let transforms = computer.compute_transforms(filtered.data())?;

                    // Create ScanMetadata combining batch + transforms
                    let scan_metadata = ScanMetadata::from_filtered_with_transforms(
                        filtered,
                        transforms,
                    )?;

                    yield scan_metadata;
                }
            } else {
                // Drop sink: drain for side effects
                futures::pin_mut!(df_stream);
                while let Some(batch_result) = df_stream.next().await {
                    let _batch = batch_result
                        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
                }
            }

            // Advance the state machine
            match sm.advance(Ok(plan))? {
                AdvanceResult::Continue => {}
                AdvanceResult::Done(_) => break,
            }
        }
    }
}
