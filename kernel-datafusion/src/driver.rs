//! Async streaming driver for state machines.

use std::pin::Pin;
use std::task::{Context, Poll};
use futures::Stream;
use futures::StreamExt;

use delta_kernel::plans::state_machines::{StateMachine, AdvanceResult};
use delta_kernel::plans::DeclarativePlanNode;
use delta_kernel::engine_data::FilteredEngineData;
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
    current_stream: Option<Pin<Box<dyn Stream<Item = Result<FilteredEngineData, delta_kernel::Error>> + Send>>>,
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
                "ResultsStreamDriver async execution not yet implemented - use helper function"
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

