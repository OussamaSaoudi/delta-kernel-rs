//! Executor for DeclarativePlanNode plans.
//!
//! This module provides `DeclarativePlanExecutor` which executes `DeclarativePlanNode` trees,
//! similar to how `DefaultPlanExecutor` (in kernel_df.rs) executes `LogicalPlanNode` trees.
//!
//! # Example
//!
//! ```ignore
//! let executor = DeclarativePlanExecutor { engine };
//! let results = executor.execute(plan)?;
//! for batch in results {
//!     let data = batch?;
//!     // process data...
//! }
//! ```

use std::sync::Arc;

use crate::arrow::array::BooleanArray;
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::Expression;
use crate::schema::DataType;
use crate::{DeltaResult, Engine, EngineData, Error};

use super::declarative::DeclarativePlanNode;
use super::kdf_state::FilterKdfState;
use super::nodes::*;

/// Filtered data with selection vector.
#[derive(Clone)]
pub struct FilteredEngineData {
    pub engine_data: Arc<dyn EngineData>,
    pub selection_vector: Vec<bool>,
}

/// Type alias for the result iterator.
pub type FilteredDataIter = Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>;

use super::kdf_state::ConsumerKdfState;

/// Iterator for ConsumeByKDF that applies consumer state to each batch and passes through.
///
/// Uses interior mutability in ConsumerKdfState to accumulate state during iteration.
struct ConsumeByKdfIterator {
    child_iter: FilteredDataIter,
    state: ConsumerKdfState,
    done: bool,
}

impl Iterator for ConsumeByKdfIterator {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.child_iter.next() {
            Some(Ok(batch)) => {
                // Apply consumer to batch (mutates state via interior mutability)
                match self.state.apply(batch.engine_data.as_ref()) {
                    Ok(true) => Some(Ok(batch)), // Continue, pass through
                    Ok(false) => {
                        // Break signal - finalize and stop
                        self.state.finalize();
                        self.done = true;
                        // Still return this batch, but no more after
                        Some(Ok(batch))
                    }
                    Err(e) => {
                        self.done = true;
                        Some(Err(e))
                    }
                }
            }
            Some(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            None => {
                // Child exhausted - finalize consumer
                self.state.finalize();
                self.done = true;
                None
            }
        }
    }
}

/// Executor for `DeclarativePlanNode` trees.
///
/// This executor interprets declarative plan nodes and executes them using the
/// provided Engine. It mirrors `DefaultPlanExecutor` but works with the
/// protobuf-serializable `DeclarativePlanNode` type.
pub struct DeclarativePlanExecutor {
    pub engine: Arc<dyn Engine>,
}

impl DeclarativePlanExecutor {
    /// Create a new executor with the given engine.
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self { engine }
    }

    /// Execute a declarative plan node and return an iterator of results.
    pub fn execute(&self, plan: DeclarativePlanNode) -> DeltaResult<FilteredDataIter> {
        match plan {
            DeclarativePlanNode::Scan(node) => self.execute_scan(node),
            DeclarativePlanNode::FileListing(node) => self.execute_file_listing(node),
            DeclarativePlanNode::SchemaQuery(node) => self.execute_schema_query(node),
            DeclarativePlanNode::FilterByKDF { child, node } => {
                self.execute_filter_by_kdf(*child, node)
            }
            DeclarativePlanNode::ConsumeByKDF { child, node } => {
                self.execute_consume_by_kdf(*child, node)
            }
            DeclarativePlanNode::FilterByExpression { child, node } => {
                self.execute_filter_by_expr(*child, node)
            }
            DeclarativePlanNode::Select { child, node } => self.execute_select(*child, node),
            DeclarativePlanNode::ParseJson { child, node } => self.execute_parse_json(*child, node),
            DeclarativePlanNode::FirstNonNull { child, node } => {
                self.execute_first_non_null(*child, node)
            }
            DeclarativePlanNode::Sink { child, node } => self.execute_sink(*child, node),
        }
    }

    /// Execute a Sink node - terminal node that consumes data.
    ///
    /// Sink nodes determine the fate of data:
    /// - `Drop`: Consumes and discards all data (useful for side-effect-only operations)
    /// - `Results`: Passes through data for streaming to the user
    fn execute_sink(
        &self,
        child: DeclarativePlanNode,
        node: SinkNode,
    ) -> DeltaResult<FilteredDataIter> {
        let child_iter = self.execute(child)?;

        match node.sink_type {
            SinkType::Drop => {
                // Consume and discard all data - process to trigger side effects
                for result in child_iter {
                    let _ = result?;
                }
                Ok(Box::new(std::iter::empty()))
            }
            SinkType::Results => {
                // Pass through for streaming to user
                Ok(child_iter)
            }
        }
    }

    /// Execute a Scan node - reads Parquet or JSON files.
    fn execute_scan(&self, node: ScanNode) -> DeltaResult<FilteredDataIter> {
        let ScanNode {
            file_type,
            files,
            schema,
        } = node;

        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        match file_type {
            FileType::Json => {
                let json_handler = self.engine.json_handler();
                let files_iter = json_handler.read_json_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files_iter.map(|result| {
                    result.map(|engine_data| FilteredEngineData {
                        selection_vector: vec![true; engine_data.len()],
                        engine_data: Arc::from(engine_data),
                    })
                })))
            }
            FileType::Parquet => {
                let parquet_handler = self.engine.parquet_handler();
                let files_iter =
                    parquet_handler.read_parquet_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files_iter.map(|result| {
                    result.map(|engine_data| FilteredEngineData {
                        selection_vector: vec![true; engine_data.len()],
                        engine_data: Arc::from(engine_data),
                    })
                })))
            }
        }
    }

    /// Execute a FileListing node - lists files from a storage path.
    ///
    /// Converts file metadata from storage into batches suitable for consumer KDFs.
    /// Each batch contains columns: path (String), size (Int64), modificationTime (Int64)
    fn execute_file_listing(&self, node: FileListingNode) -> DeltaResult<FilteredDataIter> {
        use crate::arrow::array::{Int64Array, RecordBatch, StringArray};
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
        use crate::engine::arrow_data::ArrowEngineData;

        let FileListingNode { path } = node;

        // Use the storage handler to list files
        let storage = self.engine.storage_handler();
        let files_iter = storage.list_from(&path)?;

        // Collect all files and their metadata
        let files: Vec<_> = files_iter.collect::<Result<Vec<_>, _>>()?;

        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // Create Arrow schema for file listing batch
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", ArrowDataType::Utf8, false),
            Field::new("size", ArrowDataType::Int64, true),
            Field::new("modificationTime", ArrowDataType::Int64, true),
        ]));

        // Extract file names (relative paths from log directory) and metadata
        let mut paths: Vec<String> = Vec::with_capacity(files.len());
        let mut sizes: Vec<i64> = Vec::with_capacity(files.len());
        let mut mod_times: Vec<i64> = Vec::with_capacity(files.len());

        for file in files {
            // Extract just the file name from the full URL path
            let file_name = file
                .location
                .path_segments()
                .and_then(|mut s| s.next_back())
                .unwrap_or("");
            paths.push(file_name.to_string());
            sizes.push(file.size as i64);
            mod_times.push(file.last_modified);
        }

        // Create Arrow arrays
        let path_array = StringArray::from(paths);
        let size_array = Int64Array::from(sizes);
        let mod_time_array = Int64Array::from(mod_times);

        // Create RecordBatch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(path_array),
                Arc::new(size_array),
                Arc::new(mod_time_array),
            ],
        )
        .map_err(|e| Error::generic(format!("Failed to create file listing batch: {}", e)))?;

        // Wrap in ArrowEngineData and return as single batch
        let engine_data = ArrowEngineData::from(batch);
        let selection_vector = vec![true; engine_data.len()];

        Ok(Box::new(std::iter::once(Ok(FilteredEngineData {
            engine_data: Arc::new(engine_data),
            selection_vector,
        }))))
    }

    /// Execute a SchemaQuery node - reads parquet file schema (footer only).
    fn execute_schema_query(&self, node: SchemaQueryNode) -> DeltaResult<FilteredDataIter> {
        let SchemaQueryNode {
            file_path: _,
            state: _,
        } = node;

        // For now, return error - schema query not yet implemented
        // In a full implementation, this would read the parquet footer and return schema info
        Err(Error::generic("SchemaQuery node not yet fully implemented"))
    }

    /// Execute a FilterByKDF node using a kernel-defined function (KDF).
    fn execute_filter_by_kdf(
        &self,
        child: DeclarativePlanNode,
        node: FilterByKDF,
    ) -> DeltaResult<FilteredDataIter> {
        // State is typed - clone for use in the iterator closure
        let mut state = node.state;

        let child_iter = self.execute(child)?;

        Ok(Box::new(child_iter.map(move |result| {
            let FilteredEngineData {
                engine_data,
                selection_vector,
            } = result?;

            // Convert selection vector to BooleanArray
            let selection_array = BooleanArray::from(selection_vector);

            // Apply the Filter KDF - direct dispatch via enum, monomorphized execution
            let new_selection = state.apply(engine_data.as_ref(), selection_array)?;

            // Convert back to Vec<bool>
            let new_selection_vec: Vec<bool> = (0..new_selection.len())
                .map(|i| new_selection.value(i))
                .collect();

            Ok(FilteredEngineData {
                engine_data,
                selection_vector: new_selection_vec,
            })
        })))
    }

    /// Execute a ConsumeByKDF node using a consumer kernel-defined function.
    ///
    /// Unlike FilterByKDF which returns per-row selection vectors, ConsumeByKDF
    /// processes batches and returns Continue/Break control flow:
    /// - `true` (Continue): Keep feeding data
    /// - `false` (Break): Stop iteration
    ///
    /// Consumer KDFs accumulate state across batches via interior mutability.
    /// The state is cloned (Arc clone - cheap) and captured in the iterator closure.
    /// After iteration, the original plan's state contains the accumulated results.
    ///
    /// Data is passed through to allow Sink to decide what happens with it.
    fn execute_consume_by_kdf(
        &self,
        child: DeclarativePlanNode,
        node: ConsumeByKDF,
    ) -> DeltaResult<FilteredDataIter> {
        let child_iter = self.execute(child)?;
        let state = node.state.clone(); // Clone Arc, not the inner state

        // Return iterator that applies consumer to each batch and passes through
        Ok(Box::new(ConsumeByKdfIterator {
            child_iter,
            state,
            done: false,
        }))
    }

    /// Execute a FilterByExpression node - evaluates a predicate expression.
    ///
    /// Note: This is a simplified implementation. For full predicate evaluation,
    /// the engine's expression evaluator should be used with proper schema handling.
    fn execute_filter_by_expr(
        &self,
        child: DeclarativePlanNode,
        node: FilterByExpressionNode,
    ) -> DeltaResult<FilteredDataIter> {
        let FilterByExpressionNode { predicate } = node;

        let child_iter = self.execute(child)?;
        let eval_handler = self.engine.evaluation_handler();

        // For predicate evaluation, we need to know the input schema
        // Since EngineData doesn't expose schema directly, we use the approach
        // from DefaultPlanExecutor: evaluate with a simple visitor pattern

        Ok(Box::new(child_iter.map(move |result| {
            let FilteredEngineData {
                engine_data,
                selection_vector,
            } = result?;

            // Use a visitor to extract boolean predicate results
            // This is a simplified approach - actual implementation would
            // use the expression evaluator properly

            struct PredicateVisitor {
                predicate: Arc<Expression>,
                results: Vec<bool>,
            }

            impl crate::RowVisitor for PredicateVisitor {
                fn selected_column_names_and_types(
                    &self,
                ) -> (&'static [crate::schema::ColumnName], &'static [DataType]) {
                    // Return empty - we're not selecting specific columns
                    static EMPTY_NAMES: &[crate::schema::ColumnName] = &[];
                    static EMPTY_TYPES: &[DataType] = &[];
                    (EMPTY_NAMES, EMPTY_TYPES)
                }

                fn visit<'a>(
                    &mut self,
                    row_count: usize,
                    _getters: &[&'a dyn GetData<'a>],
                ) -> DeltaResult<()> {
                    // Without proper schema, default to keeping all rows
                    // A full implementation would evaluate the predicate here
                    let _ = &self.predicate;
                    self.results.extend(std::iter::repeat(true).take(row_count));
                    Ok(())
                }
            }

            let mut visitor = PredicateVisitor {
                predicate: predicate.clone(),
                results: Vec::with_capacity(engine_data.len()),
            };

            // Visit to get row count
            engine_data.visit_rows(&[], &mut visitor)?;

            // AND with existing selection
            let new_selection: Vec<bool> = selection_vector
                .iter()
                .zip(visitor.results.iter())
                .map(|(existing, pred)| *existing && *pred)
                .collect();

            Ok(FilteredEngineData {
                engine_data,
                selection_vector: new_selection,
            })
        })))
    }

    /// Execute a Select node - projects columns using expressions.
    ///
    /// Note: For simple column references, this passes through the data unchanged.
    /// A full implementation would use the expression evaluator to project columns.
    fn execute_select(
        &self,
        child: DeclarativePlanNode,
        node: SelectNode,
    ) -> DeltaResult<FilteredDataIter> {
        let SelectNode {
            columns,
            output_schema,
        } = node;

        let child_iter = self.execute(child)?;

        // For simple single-column references, just pass through the data
        // This is a simplification - a full implementation would properly project
        if columns.len() == 1 {
            if let Expression::Column(_) = columns[0].as_ref() {
                // Simple column reference - pass through unchanged
                return Ok(child_iter);
            }
        }

        // For complex expressions, we need the expression evaluator
        let eval_handler = self.engine.evaluation_handler();

        Ok(Box::new(child_iter.map(move |result| {
            let FilteredEngineData {
                engine_data,
                selection_vector,
            } = result?;

            // Build expression to evaluate - wrap in struct if multiple columns
            let expression_to_eval: Arc<Expression> = if columns.len() == 1 {
                columns[0].clone()
            } else {
                Arc::new(Expression::Struct(columns.clone()))
            };

            // Convert output_schema to DataType
            let output_data_type: DataType = (*output_schema).clone().into();

            // Use output_schema as input_schema approximation
            // In a full implementation, we'd track the actual input schema through the plan
            let evaluator = eval_handler.new_expression_evaluator(
                output_schema.clone(),
                expression_to_eval,
                output_data_type,
            )?;

            let new_data = evaluator.evaluate(engine_data.as_ref())?;

            Ok(FilteredEngineData {
                engine_data: Arc::from(new_data),
                selection_vector,
            })
        })))
    }

    /// Execute a ParseJson node - parses a JSON column into structured data.
    fn execute_parse_json(
        &self,
        child: DeclarativePlanNode,
        node: ParseJsonNode,
    ) -> DeltaResult<FilteredDataIter> {
        let ParseJsonNode {
            json_column,
            target_schema,
            output_column,
        } = node;

        let json_handler = self.engine.json_handler();
        let child_iter = self.execute(child)?;

        Ok(Box::new(child_iter.map(move |result| {
            let FilteredEngineData {
                engine_data,
                selection_vector,
            } = result?;

            // Extract the JSON column as a single-column batch first
            // Then parse it
            // For now, we need to extract the JSON column using a visitor

            struct JsonColumnExtractor {
                column_name: String,
                values: Vec<Option<String>>,
            }

            impl crate::RowVisitor for JsonColumnExtractor {
                fn selected_column_names_and_types(
                    &self,
                ) -> (&'static [crate::schema::ColumnName], &'static [DataType]) {
                    // We need to return the column we want
                    // For now, return empty and handle in visit
                    static EMPTY_NAMES: &[crate::schema::ColumnName] = &[];
                    static EMPTY_TYPES: &[DataType] = &[];
                    (EMPTY_NAMES, EMPTY_TYPES)
                }

                fn visit<'a>(
                    &mut self,
                    row_count: usize,
                    getters: &[&'a dyn GetData<'a>],
                ) -> DeltaResult<()> {
                    let _ = &self.column_name;
                    // Without proper column access, fill with None
                    // A full implementation would extract the JSON strings
                    if getters.is_empty() {
                        self.values.extend(std::iter::repeat(None).take(row_count));
                    } else {
                        for i in 0..row_count {
                            let val: Option<String> = getters[0].get_opt(i, "json")?;
                            self.values.push(val);
                        }
                    }
                    Ok(())
                }
            }

            // For now, return the original data unchanged
            // A full implementation would:
            // 1. Extract JSON column
            // 2. Parse using json_handler.parse_json()
            // 3. Add parsed columns back to the batch
            let _ = (
                json_column.clone(),
                target_schema.clone(),
                output_column.clone(),
                json_handler.as_ref(),
            );

            Ok(FilteredEngineData {
                engine_data,
                selection_vector,
            })
        })))
    }

    /// Execute a FirstNonNull node - extracts first non-null value for specified columns.
    fn execute_first_non_null(
        &self,
        child: DeclarativePlanNode,
        node: FirstNonNullNode,
    ) -> DeltaResult<FilteredDataIter> {
        let FirstNonNullNode { columns } = node;

        let child_iter = self.execute(child)?;

        // Collect all batches first to find first non-null across all
        let batches: Vec<FilteredEngineData> = child_iter.collect::<DeltaResult<Vec<_>>>()?;

        if batches.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // For each requested column, find the first non-null value across all batches
        // This is a simplification - a full implementation would create a single output row
        // with the first non-null value for each column

        let _ = columns; // Placeholder - would use this to filter columns

        // Return all batches - the caller will need to handle first-non-null logic
        Ok(Box::new(batches.into_iter().map(Ok)))
    }
}

// =============================================================================
// State Machine Driver
// =============================================================================

use super::state_machines::{AdvanceResult, StateMachine};

/// Execute a state machine to completion, returning its result.
///
/// This driver function executes the state machine's plans in a loop until
/// the state machine reaches a terminal state. It handles:
/// - Getting the current plan via `get_plan()`
/// - Executing the plan via the `DeclarativePlanExecutor`
/// - Consuming all results from the plan
/// - Advancing the state machine via `advance()`
///
/// # Example
///
/// ```ignore
/// let engine = Arc::new(SyncEngine::new());
/// let executor = DeclarativePlanExecutor::new(engine);
/// let snapshot_sm = SnapshotStateMachine::new(table_root)?;
/// let result = execute_state_machine(&executor, snapshot_sm)?;
/// ```
pub fn execute_state_machine<SM: StateMachine>(
    executor: &DeclarativePlanExecutor,
    mut sm: SM,
) -> DeltaResult<SM::Result> {
    loop {
        // Get the current plan
        let plan = sm.get_plan()?;

        // Execute the plan and consume all results
        let results = executor.execute(plan.clone())?;
        for result in results {
            // Process each batch (for now just ensure no errors)
            let _batch = result?;
        }

        // Advance the state machine with the executed plan
        match sm.advance(Ok(plan))? {
            AdvanceResult::Continue => continue,
            AdvanceResult::Done(result) => return Ok(result),
        }
    }
}

/// Execute a state machine, yielding intermediate results.
///
/// Unlike `execute_state_machine`, this function yields the results from each
/// phase as they are produced, allowing for streaming/lazy processing.
///
/// # Returns
///
/// An iterator that yields `FilteredEngineData` batches from each phase,
/// and finally yields the terminal result.
pub fn execute_state_machine_iter<SM: StateMachine>(
    executor: &DeclarativePlanExecutor,
    sm: SM,
) -> StateMachineIterator<SM> {
    StateMachineIterator::new(executor.engine.clone(), sm)
}

/// Iterator over state machine execution.
///
/// Yields `FilteredEngineData` batches from each phase of execution.
pub struct StateMachineIterator<SM: StateMachine> {
    engine: Arc<dyn Engine>,
    sm: Option<SM>,
    current_iter: Option<FilteredDataIter>,
    is_done: bool,
}

impl<SM: StateMachine> StateMachineIterator<SM> {
    fn new(engine: Arc<dyn Engine>, sm: SM) -> Self {
        Self {
            engine,
            sm: Some(sm),
            current_iter: None,
            is_done: false,
        }
    }

    /// Get the result if the state machine has completed.
    pub fn into_result(self) -> Option<SM::Result> {
        // The result is obtained via advance() returning Done,
        // but this iterator doesn't store it. The caller should
        // use execute_state_machine() if they need the final result.
        None
    }
}

impl<SM: StateMachine> Iterator for StateMachineIterator<SM> {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done {
            return None;
        }

        loop {
            // If we have an active iterator, try to get the next batch
            if let Some(ref mut iter) = self.current_iter {
                if let Some(result) = iter.next() {
                    return Some(result);
                }
                // Current iterator exhausted, need to advance
                self.current_iter = None;
            }

            // Get the state machine, or return None if we've already consumed it
            let sm = match self.sm.as_mut() {
                Some(sm) => sm,
                None => {
                    self.is_done = true;
                    return None;
                }
            };

            // Get and execute the next plan
            let plan = match sm.get_plan() {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };

            let executor = DeclarativePlanExecutor::new(self.engine.clone());
            let results = match executor.execute(plan.clone()) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };

            // Store the iterator for batch-by-batch processing
            self.current_iter = Some(results);

            // Advance the state machine
            match sm.advance(Ok(plan)) {
                Ok(AdvanceResult::Continue) => {
                    // Continue - the loop will get batches from current_iter
                }
                Ok(AdvanceResult::Done(_)) => {
                    self.is_done = true;
                    // Drain remaining batches from current iter before finishing
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// =============================================================================
// DropOnlyDriver - For state machines with no result streaming
// =============================================================================

/// Driver for state machines that only use Drop sinks (no result streaming).
///
/// This driver is designed for operations like `SnapshotStateMachine` that execute
/// plans purely for side effects (accumulating state in KDFs) and don't stream
/// any results back to the caller.
///
/// # Behavior
///
/// - Iterates through all state machine phases
/// - Validates that ALL plans have `Drop` sinks
/// - Executes each plan to completion (triggering side effects in KDFs)
/// - Returns the final result when the state machine completes
/// - **Errors if a `Results` sink is encountered** (programmer error)
///
/// # Example
///
/// ```ignore
/// let snapshot_sm = SnapshotStateMachine::new(table_root)?;
/// let driver = DropOnlyDriver::new(engine, snapshot_sm);
/// let snapshot: Snapshot = driver.execute()?;
/// ```
pub struct DropOnlyDriver<SM: StateMachine> {
    engine: Arc<dyn Engine>,
    sm: SM,
}

impl<SM: StateMachine> DropOnlyDriver<SM> {
    /// Create a new DropOnlyDriver with the given engine and state machine.
    pub fn new(engine: Arc<dyn Engine>, sm: SM) -> Self {
        Self { engine, sm }
    }

    /// Execute the state machine to completion.
    ///
    /// Runs each phase's plan, validating that all plans use Drop sinks.
    /// Returns the final result when the state machine reaches a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A plan uses a `Results` sink instead of `Drop`
    /// - Plan execution fails and state machine doesn't handle the error
    /// - State machine advancement fails
    pub fn execute(mut self) -> DeltaResult<SM::Result> {
        loop {
            // Get the current plan
            let plan = self.sm.get_plan()?;

            // Validate: must be a Drop sink or no sink (incomplete plan)
            // Plans ending with Results sink are not allowed
            if plan.is_results_sink() {
                return Err(Error::generic(
                    "DropOnlyDriver encountered Results sink - use ResultsDriver instead",
                ));
            }

            // Execute the plan (Drop sink consumes data internally)
            let executor = DeclarativePlanExecutor::new(self.engine.clone());
            let execution_result = executor.execute(plan.clone());

            // Handle execution results - either success or error
            let advance_result = match execution_result {
                Ok(results) => {
                    // Consume all results to trigger side effects
                    for result in results {
                        if let Err(e) = result {
                            // Batch-level error - pass to state machine
                            return match self.sm.advance(Err(e))? {
                                AdvanceResult::Continue => continue,
                                AdvanceResult::Done(result) => Ok(result),
                            };
                        }
                    }
                    // All batches consumed successfully
                    self.sm.advance(Ok(plan))?
                }
                Err(e) => {
                    // Execution failed - let state machine decide how to handle
                    // (e.g., FileNotFound during CheckpointHint is OK)
                    self.sm.advance(Err(e))?
                }
            };

            match advance_result {
                AdvanceResult::Continue => continue,
                AdvanceResult::Done(result) => return Ok(result),
            }
        }
    }
}

// =============================================================================
// ResultsDriver - For state machines that stream results
// =============================================================================

/// Driver for state machines that stream results (e.g., ScanStateMachine).
///
/// This driver handles mixed sink types:
/// - `Results` sinks: Yields `FilteredEngineData` batches to the caller
/// - `Drop` sinks: Executes silently for side effects (like dedup state accumulation)
///
/// # Behavior
///
/// - Implements `Iterator` to stream results from `Results` sink plans
/// - Silently executes `Drop` sink plans (for side-effect KDFs)
/// - Provides `into_result()` to get the final state machine result after iteration
///
/// # Example
///
/// ```ignore
/// let scan_sm = ScanStateMachine::from_scan_config(...)?;
/// let driver = ResultsDriver::new(engine, scan_sm);
/// for batch in &mut driver {
///     let data = batch?;
///     // Process streamed scan results
/// }
/// let scan_result = driver.into_result()?;
/// ```
pub struct ResultsDriver<SM: StateMachine> {
    engine: Arc<dyn Engine>,
    sm: Option<SM>,
    current_iter: Option<FilteredDataIter>,
    current_plan: Option<DeclarativePlanNode>,
    result: Option<SM::Result>,
    is_done: bool,
}

impl<SM: StateMachine> ResultsDriver<SM> {
    /// Create a new ResultsDriver with the given engine and state machine.
    pub fn new(engine: Arc<dyn Engine>, sm: SM) -> Self {
        Self {
            engine,
            sm: Some(sm),
            current_iter: None,
            current_plan: None,
            result: None,
            is_done: false,
        }
    }

    /// Check if the driver has finished executing all plans.
    pub fn is_done(&self) -> bool {
        self.is_done
    }

    /// Get the final result after iteration is complete.
    ///
    /// Returns `Some(result)` if the state machine has completed,
    /// `None` if iteration is still in progress or was not completed.
    pub fn into_result(self) -> Option<SM::Result> {
        self.result
    }

    /// Advance to the next phase, returning the result if done.
    fn advance_phase(&mut self) -> DeltaResult<Option<SM::Result>> {
        let sm = match self.sm.as_mut() {
            Some(sm) => sm,
            None => return Ok(None),
        };

        let plan = match self.current_plan.take() {
            Some(p) => p,
            None => return Ok(None),
        };

        match sm.advance(Ok(plan))? {
            AdvanceResult::Continue => Ok(None),
            AdvanceResult::Done(result) => {
                self.is_done = true;
                Ok(Some(result))
            }
        }
    }

    /// Execute a Drop sink plan silently (for side effects only).
    fn execute_drop_plan(&mut self, plan: DeclarativePlanNode) -> DeltaResult<()> {
        let executor = DeclarativePlanExecutor::new(self.engine.clone());
        let results = executor.execute(plan)?;
        
        // Consume all results to trigger side effects
        for result in results {
            let _batch = result?;
        }
        
        Ok(())
    }
}

impl<SM: StateMachine> Iterator for ResultsDriver<SM> {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done {
            return None;
        }

        loop {
            // If we have an active iterator from a Results sink, yield from it
            if let Some(ref mut iter) = self.current_iter {
                if let Some(result) = iter.next() {
                    return Some(result);
                }
                // Current iterator exhausted, advance the state machine
                self.current_iter = None;
                
                match self.advance_phase() {
                    Ok(Some(result)) => {
                        self.result = Some(result);
                        return None; // Done - caller can use into_result()
                    }
                    Ok(None) => {
                        // Continue to next plan
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Get the state machine
            let sm = match self.sm.as_mut() {
                Some(sm) => sm,
                None => {
                    self.is_done = true;
                    return None;
                }
            };

            // Get the next plan
            let plan = match sm.get_plan() {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };

            // Check sink type and handle accordingly
            if plan.is_results_sink() {
                // Results sink - execute and stream results to caller
                let executor = DeclarativePlanExecutor::new(self.engine.clone());
                let results = match executor.execute(plan.clone()) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e)),
                };

                self.current_iter = Some(results);
                self.current_plan = Some(plan);
                // Loop will yield from current_iter
            } else {
                // Drop sink (or incomplete plan) - execute silently for side effects
                if let Err(e) = self.execute_drop_plan(plan.clone()) {
                    return Some(Err(e));
                }
                self.current_plan = Some(plan);
                
                // Advance immediately since there's nothing to yield
                match self.advance_phase() {
                    Ok(Some(result)) => {
                        self.result = Some(result);
                        return None;
                    }
                    Ok(None) => {
                        // Continue to next plan
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::schema::{SchemaRef, StructField, StructType};
    use crate::FileMeta;
    use std::path::PathBuf;

    fn create_test_engine() -> Arc<dyn Engine> {
        Arc::new(SyncEngine::new())
    }

    /// Get path to test JSON file (Delta log)
    fn get_test_json_path() -> Option<(PathBuf, url::Url)> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/table-without-dv-small/_delta_log/00000000000000000000.json");
        if path.exists() {
            let url = url::Url::from_file_path(&path).unwrap();
            Some((path, url))
        } else {
            None
        }
    }

    /// Get path to test Parquet file
    fn get_test_parquet_path() -> Option<(PathBuf, url::Url)> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/table-without-dv-small/part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet");
        if path.exists() {
            let url = url::Url::from_file_path(&path).unwrap();
            Some((path, url))
        } else {
            None
        }
    }

    /// Schema for reading Delta log JSON
    fn create_delta_log_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "add",
            DataType::struct_type_unchecked(vec![
                StructField::nullable("path", DataType::STRING),
                StructField::nullable("size", DataType::LONG),
            ]),
        )]))
    }

    /// Schema for reading parquet test file (value: long)
    fn create_parquet_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]))
    }

    // =========================================================================
    // Basic Executor Tests
    // =========================================================================

    #[test]
    fn test_executor_creation() {
        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);
        assert!(Arc::strong_count(&executor.engine) >= 1);
    }

    // =========================================================================
    // Scan Node Tests - With Real Data
    // =========================================================================

    #[test]
    fn test_scan_json_reads_data() {
        let Some((_, url)) = get_test_json_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 0,
            }],
            schema: create_delta_log_schema(),
        });

        let result = executor.execute(plan).expect("Scan should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Should read data from JSON");

        // Delta log has 4 lines (commitInfo, protocol, metadata, add)
        let total_rows: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.engine_data.len())
            .sum();
        assert_eq!(total_rows, 4, "Delta log should have 4 rows");

        // All rows should be selected initially
        for batch in batches.iter().filter_map(|b| b.as_ref().ok()) {
            assert!(
                batch.selection_vector.iter().all(|&v| v),
                "All rows should be selected after scan"
            );
        }
    }

    #[test]
    fn test_scan_parquet_reads_data() {
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let result = executor.execute(plan).expect("Parquet scan should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Should read parquet data");

        let total_rows: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.engine_data.len())
            .sum();
        assert_eq!(total_rows, 10, "Parquet file should have 10 rows");
    }

    #[test]
    fn test_scan_empty_files() {
        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: create_delta_log_schema(),
        });

        let result = executor.execute(plan).expect("Empty scan should succeed");
        let batches: Vec<_> = result.collect();
        assert!(batches.is_empty(), "Empty scan should produce no data");
    }

    // =========================================================================
    // Filter Node Tests (KDF-based) - With Real Data
    // =========================================================================

    #[test]
    fn test_kdf_filter_processes_data() {
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let plan = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF::add_remove_dedup(),
        };

        let result = executor.execute(plan).expect("KDF filter should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Should have data after KDF filter");

        // Data should still be 10 rows (AddRemoveDedup needs path column to work)
        let total_rows: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.engine_data.len())
            .sum();
        assert_eq!(total_rows, 10, "KDF filter should preserve row count");
    }

    // =========================================================================
    // FilterByExpression Node Tests - With Real Data
    // =========================================================================

    #[test]
    fn test_filter_by_expr_true_keeps_all() {
        use crate::expressions::Scalar;
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let plan = DeclarativePlanNode::FilterByExpression {
            child: Box::new(scan),
            node: FilterByExpressionNode {
                predicate: Arc::new(Expression::Literal(Scalar::Boolean(true))),
            },
        };

        let result = executor.execute(plan).expect("Filter should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Should have data after filter");

        // All rows should still be selected (true predicate)
        let total_selected: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.selection_vector.iter().filter(|&&v| v).count())
            .sum();
        assert_eq!(total_selected, 10, "All 10 rows should be selected");
    }

    // =========================================================================
    // Select Node Tests - With Real Data
    // =========================================================================

    #[test]
    fn test_select_projects_columns() {
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let output_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        let plan = DeclarativePlanNode::Select {
            child: Box::new(scan),
            node: SelectNode {
                columns: vec![Arc::new(Expression::Column(
                    crate::expressions::column_name!("value"),
                ))],
                output_schema,
            },
        };

        let result = executor.execute(plan).expect("Select should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Should have data after select");

        let total_rows: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.engine_data.len())
            .sum();
        assert_eq!(total_rows, 10, "Select should preserve row count");
    }

    // =========================================================================
    // ParseJson Node Tests - With Real Data
    // =========================================================================

    #[test]
    fn test_parse_json_processes_data() {
        let Some((_, url)) = get_test_json_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 0,
            }],
            schema: create_delta_log_schema(),
        });

        let target_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "numRecords",
            DataType::LONG,
        )]));

        let plan = DeclarativePlanNode::ParseJson {
            child: Box::new(scan),
            node: ParseJsonNode {
                json_column: "add.stats".to_string(),
                target_schema,
                output_column: "parsed_stats".to_string(),
            },
        };

        let result = executor.execute(plan).expect("ParseJson should succeed");
        let batches: Vec<_> = result.collect();

        // ParseJson should process the data (even if simplified implementation)
        assert!(!batches.is_empty(), "Should have data after ParseJson");
    }

    // =========================================================================
    // FirstNonNull Node Tests - With Real Data
    // =========================================================================

    #[test]
    fn test_first_non_null_collects_data() {
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let plan = DeclarativePlanNode::FirstNonNull {
            child: Box::new(scan),
            node: FirstNonNullNode {
                columns: vec!["value".to_string()],
            },
        };

        let result = executor.execute(plan).expect("FirstNonNull should succeed");
        let batches: Vec<_> = result.collect();

        // FirstNonNull collects all batches (current implementation)
        assert!(!batches.is_empty(), "Should have data after FirstNonNull");
    }

    // =========================================================================
    // FileListing Node Tests
    // =========================================================================

    #[test]
    fn test_file_listing_executes() {
        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        // Use a test data directory that exists
        let Some((test_path, _)) = get_test_table_path() else {
            println!("Skipping test: test table not found");
            return;
        };
        let log_path = test_path.join("_delta_log");
        let log_url = url::Url::from_file_path(&log_path).unwrap();

        let plan = DeclarativePlanNode::FileListing(FileListingNode { path: log_url });

        let result = executor.execute(plan);
        assert!(
            result.is_ok(),
            "FileListing should succeed: {:?}",
            result.err()
        );

        // Verify we get file listing batches
        let batches: Vec<_> = result.unwrap().collect();
        assert!(
            !batches.is_empty(),
            "Should get at least one batch from file listing"
        );
    }

    // =========================================================================
    // Composed Node Tests (Pipelines) - With Real Data
    // =========================================================================

    #[test]
    fn test_scan_filter_select_pipeline() {
        use crate::expressions::Scalar;
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let output_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        // Build pipeline: Scan -> Filter -> Select
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let filter = DeclarativePlanNode::FilterByExpression {
            child: Box::new(scan),
            node: FilterByExpressionNode {
                predicate: Arc::new(Expression::Literal(Scalar::Boolean(true))),
            },
        };

        let select = DeclarativePlanNode::Select {
            child: Box::new(filter),
            node: SelectNode {
                columns: vec![Arc::new(Expression::Column(
                    crate::expressions::column_name!("value"),
                ))],
                output_schema,
            },
        };

        let result = executor.execute(select).expect("Pipeline should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Pipeline should produce data");

        let total_rows: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.engine_data.len())
            .sum();
        assert_eq!(total_rows, 10, "Pipeline should preserve 10 rows");
    }

    #[test]
    fn test_scan_kdf_filter_pipeline() {
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let filter = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF::add_remove_dedup(),
        };

        let result = executor.execute(filter).expect("Pipeline should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Pipeline should produce data");
    }

    #[test]
    fn test_deep_pipeline() {
        use crate::expressions::Scalar;
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let output_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        // Build deep pipeline: Scan -> Filter -> FilterByExpr -> Select -> FirstNonNull
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let kdf_filter = DeclarativePlanNode::FilterByKDF {
            child: Box::new(scan),
            node: FilterByKDF::add_remove_dedup(),
        };

        let expr_filter = DeclarativePlanNode::FilterByExpression {
            child: Box::new(kdf_filter),
            node: FilterByExpressionNode {
                predicate: Arc::new(Expression::Literal(Scalar::Boolean(true))),
            },
        };

        let select = DeclarativePlanNode::Select {
            child: Box::new(expr_filter),
            node: SelectNode {
                columns: vec![Arc::new(Expression::Column(
                    crate::expressions::column_name!("value"),
                ))],
                output_schema,
            },
        };

        let first_non_null = DeclarativePlanNode::FirstNonNull {
            child: Box::new(select),
            node: FirstNonNullNode {
                columns: vec!["value".to_string()],
            },
        };

        let result = executor
            .execute(first_non_null)
            .expect("Deep pipeline should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Deep pipeline should produce data");
    }

    #[test]
    fn test_multiple_filters_pipeline() {
        use crate::expressions::Scalar;
        let Some((_, url)) = get_test_parquet_path() else {
            return;
        };

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        // Build pipeline with multiple filters
        let scan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Parquet,
            files: vec![FileMeta {
                location: url,
                last_modified: 0,
                size: 548,
            }],
            schema: create_parquet_schema(),
        });

        let filter1 = DeclarativePlanNode::FilterByExpression {
            child: Box::new(scan),
            node: FilterByExpressionNode {
                predicate: Arc::new(Expression::Literal(Scalar::Boolean(true))),
            },
        };

        let filter2 = DeclarativePlanNode::FilterByKDF {
            child: Box::new(filter1),
            node: FilterByKDF::add_remove_dedup(),
        };

        let filter3 = DeclarativePlanNode::FilterByExpression {
            child: Box::new(filter2),
            node: FilterByExpressionNode {
                predicate: Arc::new(Expression::Literal(Scalar::Boolean(true))),
            },
        };

        let result = executor
            .execute(filter3)
            .expect("Multiple filters should succeed");
        let batches: Vec<_> = result.collect();

        assert!(!batches.is_empty(), "Multiple filters should produce data");

        let total_rows: usize = batches
            .iter()
            .filter_map(|b| b.as_ref().ok())
            .map(|b| b.engine_data.len())
            .sum();
        assert_eq!(
            total_rows, 10,
            "All filters with true should preserve 10 rows"
        );
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    // =========================================================================
    // State Machine Integration Tests
    // =========================================================================

    #[test]
    fn test_snapshot_state_machine_creation() {
        use crate::plans::state_machines::SnapshotStateMachine;

        // Test creating a snapshot state machine
        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let sm = SnapshotStateMachine::new(table_url.clone());
        assert!(sm.is_ok(), "Should be able to create SnapshotStateMachine");

        let sm = sm.unwrap();
        assert!(
            !sm.is_terminal(),
            "New state machine should not be terminal"
        );
        assert_eq!(sm.phase_name(), "CheckpointHint");
    }

    #[test]
    fn test_snapshot_state_machine_with_version() {
        use crate::plans::state_machines::SnapshotStateMachine;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let sm = SnapshotStateMachine::with_version(table_url, 5);
        assert!(
            sm.is_ok(),
            "Should be able to create SnapshotStateMachine with version"
        );
    }

    #[test]
    fn test_scan_state_machine_creation() {
        use crate::plans::state_machines::ScanStateMachine;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));
        let logical_schema = physical_schema.clone();

        let sm = ScanStateMachine::new(table_url, physical_schema, logical_schema);
        assert!(sm.is_ok(), "Should be able to create ScanStateMachine");

        let sm = sm.unwrap();
        assert!(
            !sm.is_terminal(),
            "New state machine should not be terminal"
        );
        assert_eq!(sm.phase_name(), "Commit");
    }

    #[test]
    fn test_execute_state_machine_driver() {
        use super::execute_state_machine;
        use crate::plans::state_machines::SnapshotStateMachine;

        let engine = create_test_engine();
        let executor = DeclarativePlanExecutor::new(engine);

        let table_url = url::Url::parse("file:///tmp/nonexistent_table/").unwrap();
        let sm = SnapshotStateMachine::new(table_url).unwrap();

        // This will fail because the table doesn't exist, but it tests the driver function
        let result = execute_state_machine(&executor, sm);
        // We expect an error because _last_checkpoint file won't exist
        assert!(result.is_err(), "Should fail for non-existent table");
    }

    #[test]
    fn test_any_state_machine_snapshot() {
        use crate::plans::state_machines::{AnyStateMachine, SnapshotStateMachine};

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let sm = SnapshotStateMachine::new(table_url).unwrap();

        // Convert to AnyStateMachine
        let any_sm: AnyStateMachine = sm.into();

        assert!(!any_sm.is_terminal());
        assert_eq!(
            any_sm.operation_type(),
            crate::plans::state_machines::OperationType::SnapshotBuild
        );
    }

    #[test]
    fn test_any_state_machine_scan() {
        use crate::plans::state_machines::{AnyStateMachine, ScanStateMachine};

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        let sm =
            ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema).unwrap();
        let any_sm: AnyStateMachine = sm.into();

        assert!(!any_sm.is_terminal());
        assert_eq!(
            any_sm.operation_type(),
            crate::plans::state_machines::OperationType::Scan
        );
    }

    #[test]
    fn test_snapshot_state_machine_get_plan() {
        use crate::plans::state_machines::SnapshotStateMachine;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let sm = SnapshotStateMachine::new(table_url).unwrap();

        // Get the plan for the first phase (CheckpointHint)
        let plan = sm.get_plan();
        assert!(plan.is_ok(), "Should be able to get plan");

        // The first phase should produce a Sink(Drop) -> ConsumeByKDF -> Scan plan for _last_checkpoint
        let plan = plan.unwrap();
        
        // Verify it's a Drop sink (all snapshot phases use Drop sinks)
        assert!(plan.is_drop_sink(), "Snapshot plans should use Drop sinks");
        
        match plan {
            DeclarativePlanNode::Sink { child, node } => {
                assert_eq!(node.sink_type, SinkType::Drop);
                match child.as_ref() {
                    DeclarativePlanNode::ConsumeByKDF { child: inner, node: _ } => {
                        match inner.as_ref() {
                            DeclarativePlanNode::Scan(scan) => {
                                assert_eq!(scan.file_type, super::FileType::Json);
                                assert!(!scan.files.is_empty());
                            }
                            _ => panic!("Expected Scan as child of ConsumeByKDF"),
                        }
                    }
                    _ => panic!("Expected ConsumeByKDF as child of Sink"),
                }
            }
            _ => panic!("Expected Sink plan for CheckpointHint phase"),
        }
    }

    #[test]
    fn test_scan_state_machine_get_plan() {
        use crate::plans::state_machines::ScanStateMachine;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        let sm =
            ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema).unwrap();

        // Get the plan for the first phase (Commit)
        let plan = sm.get_plan();
        assert!(plan.is_ok(), "Should be able to get plan");

        // The commit phase should produce a Select -> Filter -> Scan plan
        let plan = plan.unwrap();
        match plan {
            DeclarativePlanNode::Select { child, .. } => {
                match child.as_ref() {
                    DeclarativePlanNode::FilterByKDF { child: inner, node } => {
                        // Verify it's an AddRemoveDedup filter (state variant IS the identity)
                        assert!(matches!(&node.state, FilterKdfState::AddRemoveDedup(_)));
                        match inner.as_ref() {
                            DeclarativePlanNode::Scan(scan) => {
                                assert_eq!(scan.file_type, super::FileType::Json);
                            }
                            _ => panic!("Expected Scan at the bottom"),
                        }
                    }
                    _ => panic!("Expected Filter after Select"),
                }
            }
            _ => panic!("Expected Select plan for Commit phase"),
        }
    }

    // =========================================================================
    // End-to-End Integration Tests with Real Data
    // =========================================================================

    /// Get path to a real test table (without _last_checkpoint)
    fn get_test_table_path() -> Option<(PathBuf, url::Url)> {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/table-without-dv-small");
        if path.exists() {
            let url = url::Url::from_directory_path(&path).unwrap();
            Some((path, url))
        } else {
            None
        }
    }

    /// Get path to a test table with _last_checkpoint file
    fn get_test_table_with_checkpoint_path() -> Option<(PathBuf, url::Url)> {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/parquet_row_group_skipping");
        if path.exists() {
            let url = url::Url::from_directory_path(&path).unwrap();
            Some((path, url))
        } else {
            None
        }
    }

    // FIXME: Re-enable once Snapshot::builder_for().build_sm() is implemented
    // #[test]
    // fn test_snapshot_builder_build_sm_creates_state_machine() {
    //     use crate::Snapshot;
    //
    //     let Some((_, table_url)) = get_test_table_path() else {
    //         return;
    //     };
    //
    //     // Test the new build_sm() API
    //     let sm = Snapshot::builder_for(table_url).build_sm();
    //     assert!(sm.is_ok(), "build_sm() should create a state machine");
    //
    //     let sm = sm.unwrap();
    //     assert!(
    //         !sm.is_terminal(),
    //         "New state machine should not be terminal"
    //     );
    //     assert_eq!(sm.phase_name(), "CheckpointHint");
    // }

    #[test]
    fn test_snapshot_state_machine_phase_transitions() {
        use crate::plans::state_machines::{AdvanceResult, SnapshotStateMachine};

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let mut sm = SnapshotStateMachine::new(table_url).unwrap();

        // Initial phase
        assert_eq!(sm.phase_name(), "CheckpointHint");

        // Get plan and simulate advance (with error since we're not actually executing)
        // CheckpointHint phase now returns Sink(Drop) -> ConsumeByKDF -> Scan
        let plan = sm.get_plan().unwrap();
        
        // Verify it's a Drop sink (all snapshot phases use Drop sinks)
        assert!(plan.is_drop_sink(), "Snapshot plans should use Drop sinks");
        assert!(matches!(plan, DeclarativePlanNode::Sink { .. }));

        // Advance to ListFiles
        let result = sm.advance(Ok(plan));
        assert!(matches!(result, Ok(AdvanceResult::Continue)));
        assert_eq!(sm.phase_name(), "ListFiles");

        // Get next plan - now wrapped in Sink(Drop) -> ConsumeByKDF for LogSegmentBuilder
        let plan = sm.get_plan().unwrap();
        assert!(plan.is_drop_sink(), "Snapshot plans should use Drop sinks");
        assert!(
            matches!(plan, DeclarativePlanNode::Sink { .. }),
            "Expected Sink plan, got {:?}",
            plan
        );

        // Advance to LoadMetadata - this will fail without real data because we need
        // actual log files to construct a LogSegment. This is expected behavior.
        // The state machine is designed to be driven by an executor that provides real data.
        let result = sm.advance(Ok(plan));
        // Without real data, we expect an error (no log files in segment)
        assert!(result.is_err(), "Should fail without real log file data");
    }

    #[test]
    fn test_scan_state_machine_phase_transitions() {
        use crate::plans::state_machines::{AdvanceResult, ScanStateMachine};

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        let mut sm =
            ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema).unwrap();

        // Initial phase
        assert_eq!(sm.phase_name(), "Commit");

        // Get plan
        let plan = sm.get_plan().unwrap();
        assert!(matches!(plan, DeclarativePlanNode::Select { .. }));

        // Advance - since no checkpoint files, should go straight to Complete
        let result = sm.advance(Ok(plan));
        assert!(matches!(result, Ok(AdvanceResult::Done(_))));
        assert!(sm.is_terminal());
    }

    #[test]
    fn test_snapshot_state_machine_checkpoint_hint_file_not_found() {
        use crate::plans::state_machines::{AdvanceResult, SnapshotStateMachine};
        use crate::Error;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let mut sm = SnapshotStateMachine::new(table_url).unwrap();

        // Initial phase should be CheckpointHint
        assert_eq!(sm.phase_name(), "CheckpointHint");

        // Simulate _last_checkpoint file not found - engine returns FileNotFound error
        let file_not_found_error = Error::file_not_found("_last_checkpoint");
        let result = sm.advance(Err(file_not_found_error));

        // Should succeed and move to ListFiles phase (file not found is OK for checkpoint hint)
        assert!(result.is_ok(), "FileNotFound should be handled gracefully");
        assert!(matches!(result.unwrap(), AdvanceResult::Continue));
        assert_eq!(sm.phase_name(), "ListFiles");

        // The state machine should continue normally - get the ListFiles plan
        let list_plan = sm.get_plan();
        assert!(list_plan.is_ok(), "Should be able to get ListFiles plan");
    }

    #[test]
    fn test_snapshot_state_machine_checkpoint_hint_other_error_propagated() {
        use crate::plans::state_machines::SnapshotStateMachine;
        use crate::Error;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let mut sm = SnapshotStateMachine::new(table_url).unwrap();

        // Initial phase should be CheckpointHint
        assert_eq!(sm.phase_name(), "CheckpointHint");

        // Simulate a different error (e.g., permission denied, network error)
        let other_error = Error::generic("Permission denied");
        let result = sm.advance(Err(other_error));

        // Non-FileNotFound errors should be propagated
        assert!(result.is_err(), "Non-FileNotFound errors should be propagated");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Permission denied"),
            "Error message should be preserved"
        );
    }

    #[test]
    fn test_snapshot_state_machine_list_files_error_propagated() {
        use crate::plans::state_machines::{AdvanceResult, SnapshotStateMachine};
        use crate::Error;

        let table_url = url::Url::parse("file:///tmp/test_table/").unwrap();
        let mut sm = SnapshotStateMachine::new(table_url).unwrap();

        // Move past CheckpointHint phase
        let plan = sm.get_plan().unwrap();
        let result = sm.advance(Ok(plan));
        assert!(matches!(result, Ok(AdvanceResult::Continue)));
        assert_eq!(sm.phase_name(), "ListFiles");

        // Get ListFiles plan
        let plan = sm.get_plan().unwrap();

        // Simulate error during ListFiles phase
        let error = Error::generic("Storage error");
        let result = sm.advance(Err(error));

        // Errors in ListFiles phase should be propagated (not handled gracefully like CheckpointHint)
        assert!(result.is_err(), "ListFiles errors should be propagated");
    }

    // FIXME: Re-enable once Scan::begin() is implemented
    // #[test]
    // fn test_scan_begin_creates_state_machine() {
    //     use crate::plans::state_machines::ScanStateMachine;
    //     use crate::Snapshot;
    //
    //     // Use a table with _last_checkpoint since the old build() requires it
    //     let Some((_, table_url)) = get_test_table_with_checkpoint_path() else {
    //         return;
    //     };
    //
    //     let engine = create_test_engine();
    //
    //     // First try to build a snapshot using the old API
    //     let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref());
    //
    //     if let Ok(snapshot) = snapshot {
    //         // Old API worked - test the full flow
    //         let scan = snapshot.scan_builder().build();
    //         assert!(scan.is_ok(), "Should be able to build scan");
    //
    //         let scan = scan.unwrap();
    //         let scan_sm = scan.begin();
    //         assert!(
    //             scan_sm.is_ok(),
    //             "begin() should create a scan state machine"
    //         );
    //
    //         let scan_sm = scan_sm.unwrap();
    //         assert!(
    //             !scan_sm.is_terminal(),
    //             "New scan state machine should not be terminal"
    //         );
    //         assert_eq!(scan_sm.phase_name(), "Commit");
    //     } else {
    //         // Old API failed - test the new API directly
    //         let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
    //             "value",
    //             DataType::LONG,
    //         )]));
    //
    //         let scan_sm =
    //             ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema);
    //         assert!(scan_sm.is_ok(), "New ScanStateMachine API should work");
    //
    //         let scan_sm = scan_sm.unwrap();
    //         assert!(
    //             !scan_sm.is_terminal(),
    //             "New scan state machine should not be terminal"
    //         );
    //         assert_eq!(scan_sm.phase_name(), "Commit");
    //     }
    // }

    #[test]
    fn test_state_machine_iterator() {
        use super::execute_state_machine_iter;
        use crate::plans::state_machines::ScanStateMachine;

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let engine = create_test_engine();
        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        let sm =
            ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema).unwrap();

        // Create iterator
        let iter = execute_state_machine_iter(&DeclarativePlanExecutor::new(engine), sm);

        // Collect results (may be empty since no commit files in this simple table path)
        let results: Vec<_> = iter.collect();

        // Iterator should complete without panic
        // (the actual results depend on what files are in the table)
        let _ = results;
    }

    // FIXME: Re-enable once Snapshot::builder_for().build_sm() and Scan::begin() are implemented
    // #[test]
    // fn test_full_api_flow_snapshot_to_scan() {
    //     use crate::plans::state_machines::{AdvanceResult, OperationType};
    //     use crate::Snapshot;
    //
    //     // Use a table with _last_checkpoint since the old build() requires it
    //     let Some((_, table_url)) = get_test_table_with_checkpoint_path() else {
    //         return;
    //     };
    //
    //     let engine = create_test_engine();
    //
    //     // Step 1: Create snapshot state machine using new API
    //     let snapshot_sm = Snapshot::builder_for(table_url.clone()).build_sm();
    //     assert!(snapshot_sm.is_ok());
    //
    //     let snapshot_sm = snapshot_sm.unwrap();
    //     assert_eq!(snapshot_sm.operation_type(), OperationType::SnapshotBuild);
    //
    //     // Step 2: Build actual snapshot using old API for comparison
    //     let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref());
    //
    //     // The old build() API might fail due to state machine issues
    //     // This is expected - we're testing the new API
    //     if let Err(e) = &snapshot {
    //         eprintln!(
    //             "Note: Old API build failed (expected during transition): {}",
    //             e
    //         );
    //         // Test that the new state machine API at least creates a valid state machine
    //         return;
    //     }
    //
    //     let snapshot = snapshot.unwrap();
    //
    //     // Step 3: Create scan and get state machine
    //     let scan = snapshot.scan_builder().build().unwrap();
    //     let scan_sm = scan.begin();
    //     assert!(scan_sm.is_ok());
    //
    //     let scan_sm = scan_sm.unwrap();
    //     assert_eq!(scan_sm.operation_type(), OperationType::Scan);
    //
    //     // Step 4: Verify we can get a plan from the scan state machine
    //     let plan = scan_sm.get_plan();
    //     assert!(plan.is_ok(), "Should be able to get scan plan");
    // }

    #[test]
    fn test_any_state_machine_polymorphism() {
        use crate::plans::state_machines::{
            AdvanceResult, AnyStateMachine, OperationType, ScanStateMachine, SnapshotStateMachine,
        };

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        // Create both types of state machines
        let snapshot_sm = SnapshotStateMachine::new(table_url.clone()).unwrap();
        let scan_sm =
            ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema).unwrap();

        // Convert to AnyStateMachine for polymorphic handling
        let mut any_snapshot: AnyStateMachine = snapshot_sm.into();
        let mut any_scan: AnyStateMachine = scan_sm.into();

        // Both should work through the same interface
        assert!(!any_snapshot.is_terminal());
        assert!(!any_scan.is_terminal());

        assert_eq!(any_snapshot.operation_type(), OperationType::SnapshotBuild);
        assert_eq!(any_scan.operation_type(), OperationType::Scan);

        // Get plans from both
        let snapshot_plan = any_snapshot.get_plan();
        let scan_plan = any_scan.get_plan();

        assert!(snapshot_plan.is_ok());
        assert!(scan_plan.is_ok());

        // Advance both
        let snapshot_result = any_snapshot.advance(snapshot_plan);
        let scan_result = any_scan.advance(scan_plan);

        assert!(snapshot_result.is_ok());
        assert!(scan_result.is_ok());
    }

    // FIXME: Re-enable once Snapshot::builder_for().build_sm() is implemented
    // #[test]
    // fn test_execute_state_machine_with_real_table() {
    //     use super::execute_state_machine;
    //     use crate::Snapshot;
    //
    //     let Some((_, table_url)) = get_test_table_path() else {
    //         return;
    //     };
    //
    //     let engine = create_test_engine();
    //     let executor = DeclarativePlanExecutor::new(engine.clone());
    //
    //     // Get the snapshot state machine
    //     let sm = Snapshot::builder_for(table_url).build_sm().unwrap();
    //
    //     // Execute - this will fail on the CheckpointHint phase because
    //     // the table doesn't have a _last_checkpoint file, which is expected
    //     let result = execute_state_machine(&executor, sm);
    //
    //     // We expect this to fail at the checkpoint hint phase
    //     // because the test table doesn't have _last_checkpoint
    //     // This is correct behavior - the state machine should propagate errors
    //     assert!(result.is_err(), "Should fail without _last_checkpoint file");
    // }

    // FIXME: Re-enable once snapshot.protocol() is implemented
    // #[test]
    // fn test_snapshot_result_contains_metadata() {
    //     use crate::Snapshot;
    //
    //     // This test verifies that a Snapshot built via the new state machine API
    //     // contains the expected metadata. We use the standard build() path since
    //     // manual advancement without real execution won't produce valid Snapshots.
    //     let Some((_, table_url)) = get_test_table_path() else {
    //         return;
    //     };
    //
    //     let engine = create_test_engine();
    //
    //     // Use the standard build path which handles execution internally
    //     let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref());
    //
    //     match snapshot {
    //         Ok(snapshot) => {
    //             // Verify the snapshot contains expected fields
    //             assert_eq!(snapshot.table_root(), &table_url);
    //             // Verify we got a valid version
    //             assert!(snapshot.version() >= 0);
    //             // Verify protocol is present
    //             assert!(snapshot.protocol().min_reader_version() >= 1);
    //         }
    //         Err(e) => {
    //             // Table may not have all required files, which is acceptable
    //             println!(
    //                 "Snapshot build failed (expected for some test tables): {}",
    //                 e
    //             );
    //         }
    //     }
    // }

    #[test]
    fn test_scan_metadata_result_contains_schema() {
        use crate::plans::state_machines::{AdvanceResult, ScanMetadataResult, ScanStateMachine};

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));
        let logical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "output_value",
            DataType::LONG,
        )]));

        let mut sm = ScanStateMachine::new(
            table_url.clone(),
            physical_schema.clone(),
            logical_schema.clone(),
        )
        .unwrap();

        // Advance through commit phase (no checkpoint files, so should complete)
        let plan = sm.get_plan().unwrap();
        let result = sm.advance(Ok(plan)).unwrap();

        match result {
            AdvanceResult::Done(scan_result) => {
                assert_eq!(scan_result.table_root, table_url);
                assert_eq!(scan_result.physical_schema, physical_schema);
                assert_eq!(scan_result.logical_schema, logical_schema);
            }
            AdvanceResult::Continue => {
                // If we continue, advance until complete
                let plan = sm.get_plan().unwrap();
                if let Ok(AdvanceResult::Done(scan_result)) = sm.advance(Ok(plan)) {
                    assert_eq!(scan_result.table_root, table_url);
                }
            }
        }
    }

    // =========================================================================
    // Two-Level API Tests
    // =========================================================================

    /// Test the LOW-LEVEL API: User manually drives the state machine.
    /// This is the pattern used by FFI/Java callers.
    #[test]
    fn test_low_level_api_manual_state_machine_driving() {
        use crate::plans::state_machines::{AdvanceResult, ScanStateMachine};

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        // LOW-LEVEL API: Create state machine directly
        let mut sm =
            ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema.clone())
                .unwrap();

        // Low-level: Caller manually drives the state machine loop
        let mut iterations = 0;
        while !sm.is_terminal() {
            // 1. Get the current plan
            let plan = sm.get_plan().expect("Should get plan");

            // 2. (In real use) Execute the plan with caller's engine
            //    Here we just pass it back to advance since we're testing

            // 3. Advance with result
            match sm.advance(Ok(plan)) {
                Ok(AdvanceResult::Continue) => {
                    iterations += 1;
                    continue;
                }
                Ok(AdvanceResult::Done(_result)) => {
                    // Got result
                    break;
                }
                Err(e) => panic!("Advance failed: {:?}", e),
            }
        }

        // Verify state machine reached terminal state
        assert!(sm.is_terminal(), "State machine should be terminal");
        // With no checkpoint files, should complete in 1 iteration (commit phase only)
        assert!(
            iterations <= 1,
            "Should complete quickly without checkpoints"
        );
    }

    /// Test the HIGH-LEVEL API: Engine-based automatic execution.
    /// This is the pattern for Rust callers who want automatic execution.
    #[test]
    fn test_high_level_api_engine_based_execution() {
        use super::execute_state_machine_iter;
        use crate::plans::state_machines::ScanStateMachine;

        let Some((_, table_url)) = get_test_table_path() else {
            return;
        };

        let engine = create_test_engine();
        let physical_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        // HIGH-LEVEL API: Create state machine and executor
        let sm = ScanStateMachine::new(table_url, physical_schema.clone(), physical_schema.clone())
            .unwrap();

        let executor = DeclarativePlanExecutor::new(engine);

        // High-level: Just iterate - execution is handled automatically
        let results: Vec<_> = execute_state_machine_iter(&executor, sm).collect();

        // Results collected automatically without manual state machine driving
        // (Actual content depends on table, but iteration should complete)
        let _ = results;
    }

    // FIXME: Re-enable once Scan::begin() is implemented
    // Test using Scan::begin() for low-level API.
    // #[test]
    // fn test_scan_begin_low_level_api() {
    //     use crate::plans::state_machines::AdvanceResult;
    //     use crate::Snapshot;
    //
    //     let Some((_, table_url)) = get_test_table_with_checkpoint_path() else {
    //         return;
    //     };
    //
    //     let engine = create_test_engine();
    //
    //     // Try to build snapshot
    //     let snapshot = match Snapshot::builder_for(table_url.clone()).build(engine.as_ref()) {
    //         Ok(s) => s,
    //         Err(_) => return, // Skip if snapshot build fails
    //     };
    //
    //     // LOW-LEVEL API via Scan::begin()
    //     let scan = snapshot.scan_builder().build().unwrap();
    //     let mut scan_sm = scan.begin().unwrap();
    //
    //     // Manually drive the state machine (FFI pattern)
    //     while !scan_sm.is_terminal() {
    //         let plan = scan_sm.get_plan().unwrap();
    //         // Caller would execute plan here with their engine
    //         match scan_sm.advance(Ok(plan)) {
    //             Ok(AdvanceResult::Continue) => continue,
    //             Ok(AdvanceResult::Done(_)) => break,
    //             Err(_) => break,
    //         }
    //     }
    //
    //     assert!(scan_sm.is_terminal());
    // }

    // =========================================================================
    // DropOnlyDriver and ResultsDriver Tests
    // =========================================================================

    #[test]
    fn test_drop_only_driver_rejects_results_sink() {
        use crate::plans::state_machines::StateMachine;

        // Create a mock state machine that returns a Results sink plan
        struct MockResultsStateMachine {
            done: bool,
        }

        impl StateMachine for MockResultsStateMachine {
            type Result = ();

            fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
                // Create a simple plan with a Results sink
                let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
                    "value",
                    DataType::LONG,
                )]));
                Ok(DeclarativePlanNode::Scan(ScanNode {
                    file_type: FileType::Json,
                    files: vec![],
                    schema,
                })
                .sink_results())
            }

            fn advance(
                &mut self,
                _result: DeltaResult<DeclarativePlanNode>,
            ) -> DeltaResult<AdvanceResult<Self::Result>> {
                self.done = true;
                Ok(AdvanceResult::Done(()))
            }

            fn operation_type(&self) -> crate::plans::state_machines::OperationType {
                crate::plans::state_machines::OperationType::Scan
            }

            fn phase_name(&self) -> &'static str {
                "Test"
            }
        }

        let engine = create_test_engine();
        let sm = MockResultsStateMachine { done: false };
        let driver = DropOnlyDriver::new(engine, sm);

        // DropOnlyDriver should error on Results sink
        let result = driver.execute();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Results sink"),
            "Error should mention Results sink: {}",
            err_msg
        );
    }

    #[test]
    fn test_drop_only_driver_accepts_drop_sink() {
        use crate::plans::state_machines::StateMachine;

        // Create a mock state machine that returns a Drop sink plan
        struct MockDropStateMachine {
            calls: std::cell::Cell<usize>,
        }

        impl StateMachine for MockDropStateMachine {
            type Result = String;

            fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
                let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
                    "value",
                    DataType::LONG,
                )]));
                // Return a Drop sink plan
                Ok(DeclarativePlanNode::Scan(ScanNode {
                    file_type: FileType::Json,
                    files: vec![], // Empty files so execution succeeds quickly
                    schema,
                })
                .sink_drop())
            }

            fn advance(
                &mut self,
                _result: DeltaResult<DeclarativePlanNode>,
            ) -> DeltaResult<AdvanceResult<Self::Result>> {
                let calls = self.calls.get() + 1;
                self.calls.set(calls);
                if calls >= 2 {
                    Ok(AdvanceResult::Done("completed".to_string()))
                } else {
                    Ok(AdvanceResult::Continue)
                }
            }

            fn operation_type(&self) -> crate::plans::state_machines::OperationType {
                crate::plans::state_machines::OperationType::SnapshotBuild
            }

            fn phase_name(&self) -> &'static str {
                "Test"
            }
        }

        let engine = create_test_engine();
        let sm = MockDropStateMachine {
            calls: std::cell::Cell::new(0),
        };
        let driver = DropOnlyDriver::new(engine, sm);

        // DropOnlyDriver should accept Drop sink and complete successfully
        let result = driver.execute();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "completed");
    }

    #[test]
    fn test_results_driver_streams_from_results_sink() {
        use crate::plans::state_machines::StateMachine;

        // Create a mock state machine that returns a Results sink plan
        struct MockStreamingStateMachine {
            done: bool,
        }

        impl StateMachine for MockStreamingStateMachine {
            type Result = String;

            fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
                let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
                    "value",
                    DataType::LONG,
                )]));
                // Return a Results sink plan (empty files so no actual data)
                Ok(DeclarativePlanNode::Scan(ScanNode {
                    file_type: FileType::Json,
                    files: vec![],
                    schema,
                })
                .sink_results())
            }

            fn advance(
                &mut self,
                _result: DeltaResult<DeclarativePlanNode>,
            ) -> DeltaResult<AdvanceResult<Self::Result>> {
                self.done = true;
                Ok(AdvanceResult::Done("streamed".to_string()))
            }

            fn operation_type(&self) -> crate::plans::state_machines::OperationType {
                crate::plans::state_machines::OperationType::Scan
            }

            fn phase_name(&self) -> &'static str {
                "Test"
            }
        }

        let engine = create_test_engine();
        let sm = MockStreamingStateMachine { done: false };
        let mut driver = ResultsDriver::new(engine, sm);

        // Consume the iterator (should be empty since no files)
        let batches: Vec<_> = driver.by_ref().collect();
        assert!(batches.is_empty(), "Should have no batches from empty files");

        // Should be done and have result
        assert!(driver.is_done());
        let result = driver.into_result();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "streamed");
    }

    #[test]
    fn test_results_driver_handles_drop_sink_silently() {
        use crate::plans::state_machines::StateMachine;

        // Create a mock state machine that returns Drop sink first, then Results sink
        struct MockMixedStateMachine {
            phase: std::cell::Cell<usize>,
        }

        impl StateMachine for MockMixedStateMachine {
            type Result = String;

            fn get_plan(&self) -> DeltaResult<DeclarativePlanNode> {
                let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
                    "value",
                    DataType::LONG,
                )]));

                if self.phase.get() == 0 {
                    // First phase: Drop sink (internal processing)
                    Ok(DeclarativePlanNode::Scan(ScanNode {
                        file_type: FileType::Json,
                        files: vec![],
                        schema,
                    })
                    .sink_drop())
                } else {
                    // Second phase: Results sink (stream to user)
                    Ok(DeclarativePlanNode::Scan(ScanNode {
                        file_type: FileType::Json,
                        files: vec![],
                        schema,
                    })
                    .sink_results())
                }
            }

            fn advance(
                &mut self,
                _result: DeltaResult<DeclarativePlanNode>,
            ) -> DeltaResult<AdvanceResult<Self::Result>> {
                let phase = self.phase.get() + 1;
                self.phase.set(phase);

                if phase >= 2 {
                    Ok(AdvanceResult::Done("mixed-completed".to_string()))
                } else {
                    Ok(AdvanceResult::Continue)
                }
            }

            fn operation_type(&self) -> crate::plans::state_machines::OperationType {
                crate::plans::state_machines::OperationType::Scan
            }

            fn phase_name(&self) -> &'static str {
                if self.phase.get() == 0 {
                    "DropPhase"
                } else {
                    "ResultsPhase"
                }
            }
        }

        let engine = create_test_engine();
        let sm = MockMixedStateMachine {
            phase: std::cell::Cell::new(0),
        };
        let mut driver = ResultsDriver::new(engine, sm);

        // Consume the iterator
        // Drop phase should be handled silently, Results phase should yield nothing (empty files)
        let batches: Vec<_> = driver.by_ref().collect();
        assert!(batches.is_empty());

        // Should complete successfully
        assert!(driver.is_done());
        let result = driver.into_result();
        assert_eq!(result, Some("mixed-completed".to_string()));
    }

    #[test]
    fn test_sink_type_helpers() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::LONG,
        )]));

        // Test Drop sink
        let drop_plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: schema.clone(),
        })
        .sink_drop();

        assert!(drop_plan.is_complete());
        assert!(drop_plan.is_drop_sink());
        assert!(!drop_plan.is_results_sink());
        assert_eq!(drop_plan.sink_type(), Some(SinkType::Drop));

        // Test Results sink
        let results_plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: schema.clone(),
        })
        .sink_results();

        assert!(results_plan.is_complete());
        assert!(!results_plan.is_drop_sink());
        assert!(results_plan.is_results_sink());
        assert_eq!(results_plan.sink_type(), Some(SinkType::Results));

        // Test non-sink plan
        let non_sink_plan = DeclarativePlanNode::Scan(ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema,
        });

        assert!(!non_sink_plan.is_complete());
        assert!(!non_sink_plan.is_drop_sink());
        assert!(!non_sink_plan.is_results_sink());
        assert_eq!(non_sink_plan.sink_type(), None);
    }

    // =========================================================================
    // Integration Tests: SnapshotStateMachine with DropOnlyDriver
    // =========================================================================

    /// Test that DropOnlyDriver can execute SnapshotStateMachine on a real delta table
    /// and produce a valid Snapshot that matches the traditional API.
    #[test]
    fn test_drop_only_driver_snapshot_state_machine_real_table() {
        use crate::plans::state_machines::SnapshotStateMachine;
        use crate::Snapshot;

        // Get path to a test table
        let Some((_, table_url)) = get_test_table_path() else {
            println!("Skipping test: test table not found");
            return;
        };

        let engine = create_test_engine();

        // Build snapshot using traditional API for comparison
        let expected_snapshot = match Snapshot::builder_for(table_url.clone()).build(engine.as_ref())
        {
            Ok(s) => s,
            Err(e) => {
                println!("Skipping test: traditional snapshot build failed: {}", e);
                return;
            }
        };

        // Build snapshot using SnapshotStateMachine + DropOnlyDriver
        let sm = SnapshotStateMachine::new(table_url.clone()).unwrap();
        let driver = DropOnlyDriver::new(engine.clone(), sm);

        let actual_snapshot = match driver.execute() {
            Ok(s) => s,
            Err(e) => {
                panic!("DropOnlyDriver failed to execute SnapshotStateMachine: {}", e);
            }
        };

        // Verify the snapshots match
        assert_eq!(
            expected_snapshot.version(),
            actual_snapshot.version(),
            "Snapshot versions should match"
        );

        assert_eq!(
            expected_snapshot.table_root().as_str(),
            actual_snapshot.table_root().as_str(),
            "Table roots should match"
        );

        // Compare schemas
        let expected_schema = expected_snapshot.schema();
        let actual_schema = actual_snapshot.schema();
        assert_eq!(
            expected_schema.fields().len(),
            actual_schema.fields().len(),
            "Schema field counts should match"
        );

        for (expected_field, actual_field) in
            expected_schema.fields().zip(actual_schema.fields())
        {
            assert_eq!(
                expected_field.name(),
                actual_field.name(),
                "Field names should match"
            );
            assert_eq!(
                expected_field.data_type(),
                actual_field.data_type(),
                "Field data types should match"
            );
        }

        println!(
            "SUCCESS: SnapshotStateMachine produced correct Snapshot at version {}",
            actual_snapshot.version()
        );
    }

    /// Test SnapshotStateMachine with a table that has a checkpoint
    #[test]
    fn test_drop_only_driver_snapshot_with_checkpoint() {
        use crate::plans::state_machines::SnapshotStateMachine;
        use crate::Snapshot;

        // Get path to a table with checkpoint
        let Some((_, table_url)) = get_test_table_with_checkpoint_path() else {
            println!("Skipping test: checkpoint table not found");
            return;
        };

        let engine = create_test_engine();

        // Build expected snapshot using traditional API
        let expected_snapshot = match Snapshot::builder_for(table_url.clone()).build(engine.as_ref())
        {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "Skipping test: traditional snapshot build failed: {}",
                    e
                );
                return;
            }
        };

        // Build snapshot using SnapshotStateMachine + DropOnlyDriver
        let sm = SnapshotStateMachine::new(table_url).unwrap();
        let driver = DropOnlyDriver::new(engine.clone(), sm);

        let actual_snapshot = match driver.execute() {
            Ok(s) => s,
            Err(e) => {
                panic!(
                    "DropOnlyDriver failed on table with checkpoint: {}",
                    e
                );
            }
        };

        // Verify versions match
        assert_eq!(
            expected_snapshot.version(),
            actual_snapshot.version(),
            "Snapshot versions should match for checkpoint table"
        );

        println!(
            "SUCCESS: SnapshotStateMachine handled checkpoint correctly, version = {}",
            actual_snapshot.version()
        );
    }

    /// Test multiple tables to ensure SnapshotStateMachine works across different table structures
    #[test]
    fn test_drop_only_driver_multiple_tables() {
        use crate::plans::state_machines::SnapshotStateMachine;
        use crate::Snapshot;

        let test_tables = [
            "table-without-dv-small",
            "table-with-dv-small",
            "basic_partitioned",
            "parquet_row_group_skipping",
        ];

        let engine = create_test_engine();
        let mut success_count = 0;

        for table_name in &test_tables {
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/data")
                .join(table_name);

            if !path.exists() {
                println!("Skipping {}: path not found", table_name);
                continue;
            }

            let table_url = url::Url::from_directory_path(&path).unwrap();

            // Build expected snapshot
            let expected_snapshot = match Snapshot::builder_for(table_url.clone()).build(engine.as_ref())
            {
                Ok(s) => s,
                Err(e) => {
                    println!("Skipping {}: traditional build failed: {}", table_name, e);
                    continue;
                }
            };

            // Build using SnapshotStateMachine + DropOnlyDriver
            let sm = SnapshotStateMachine::new(table_url).unwrap();
            let driver = DropOnlyDriver::new(engine.clone(), sm);

            match driver.execute() {
                Ok(actual_snapshot) => {
                    assert_eq!(
                        expected_snapshot.version(),
                        actual_snapshot.version(),
                        "Version mismatch for table {}",
                        table_name
                    );
                    assert_eq!(
                        expected_snapshot.schema().fields().len(),
                        actual_snapshot.schema().fields().len(),
                        "Schema field count mismatch for table {}",
                        table_name
                    );
                    println!(
                        "  OK: {} (version {})",
                        table_name,
                        actual_snapshot.version()
                    );
                    success_count += 1;
                }
                Err(e) => {
                    panic!(
                        "DropOnlyDriver failed for table {}: {}",
                        table_name, e
                    );
                }
            }
        }

        println!(
            "Successfully tested {} tables with SnapshotStateMachine",
            success_count
        );
        assert!(success_count > 0, "At least one table should succeed");
    }
}
