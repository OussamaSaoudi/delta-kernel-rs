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
pub use crate::engine_data::FilteredEngineData;
use crate::engine_data::{GetData, TypedGetData};
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::expressions::Expression;
use crate::schema::DataType;
use crate::{DeltaResult, Engine, EngineData, Error};

use super::declarative::DeclarativePlanNode;
use super::kdf_state::FilterKdfState;
use super::nodes::*;

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
                match self.state.apply(batch.data()) {
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

/// Helper to extract a nested string column from a RecordBatch.
///
/// Handles dot-separated paths like "add.stats" by navigating through struct columns.
fn extract_nested_string_column(
    batch: &crate::arrow::array::RecordBatch,
    column_path: &str,
) -> DeltaResult<crate::arrow::array::StringArray> {
    use crate::arrow::array::{Array, StringArray};
    use crate::arrow::array::cast::AsArray;

    let parts: Vec<&str> = column_path.split('.').collect();
    
    if parts.is_empty() {
        return Err(Error::generic("Empty column path"));
    }

    // Start with the first part
    let mut current_array: Arc<dyn Array> = batch
        .column_by_name(parts[0])
        .ok_or_else(|| Error::generic(format!("Column '{}' not found", parts[0])))?
        .clone();

    // Navigate through nested struct fields
    for part in parts.iter().skip(1) {
        let struct_array = current_array
            .as_struct_opt()
            .ok_or_else(|| Error::generic(format!("Expected struct for path '{}'", column_path)))?;
        
        current_array = struct_array
            .column_by_name(part)
            .ok_or_else(|| Error::generic(format!("Field '{}' not found in struct", part)))?
            .clone();
    }

    // Final array should be a string
    let string_array = current_array
        .as_string_opt::<i32>()
        .ok_or_else(|| Error::generic(format!("Column '{}' is not a string type", column_path)))?;

    Ok(string_array.clone())
}

/// Executor for `DeclarativePlanNode` trees.
///
/// This executor interprets declarative plan nodes and executes them using the
/// provided Engine. It mirrors `DefaultPlanExecutor` but works with the
/// protobuf-serializable `DeclarativePlanNode` type.
pub struct DeclarativePlanExecutor<'a> {
    pub engine: &'a dyn Engine,
}

impl<'a> DeclarativePlanExecutor<'a> {
    /// Create a new executor with the given engine reference.
    pub fn new(engine: &'a dyn Engine) -> Self {
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

    /// Infer the output schema (as a kernel `SchemaRef`) for a declarative plan node.
    ///
    /// This is used by nodes like `Select` to construct expression evaluators with the correct
    /// input schema (matching the nested struct layout of log actions, e.g. `add.path`,
    /// `remove.deletionVector.*`).
    fn infer_output_schema(&self, plan: &DeclarativePlanNode) -> DeltaResult<crate::schema::SchemaRef> {
        use crate::schema::{DataType, StructField, StructType};

        Ok(match plan {
            DeclarativePlanNode::Scan(node) => node.schema.clone(),
            DeclarativePlanNode::FileListing(_) => {
                // Must match the schema produced by `execute_file_listing`.
                Arc::new(StructType::new_unchecked(vec![
                    StructField::nullable("path", DataType::STRING),
                    StructField::nullable("size", DataType::LONG),
                    StructField::nullable("modificationTime", DataType::LONG),
                ]))
            }
            DeclarativePlanNode::SchemaQuery(_) => Arc::new(StructType::new_unchecked(vec![])),
            DeclarativePlanNode::FilterByKDF { child, .. }
            | DeclarativePlanNode::ConsumeByKDF { child, .. }
            | DeclarativePlanNode::FilterByExpression { child, .. }
            | DeclarativePlanNode::FirstNonNull { child, .. }
            | DeclarativePlanNode::ParseJson { child, .. }
            | DeclarativePlanNode::Sink { child, .. } => self.infer_output_schema(child)?,
            DeclarativePlanNode::Select { node, .. } => node.output_schema.clone(),
        })
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
                    result.and_then(|engine_data| {
                        let len = engine_data.len();
                        FilteredEngineData::try_new(engine_data, vec![true; len])
                    })
                })))
            }
            FileType::Parquet => {
                let parquet_handler = self.engine.parquet_handler();
                let files_iter =
                    parquet_handler.read_parquet_files(files.as_slice(), schema, None)?;
                Ok(Box::new(files_iter.map(|result| {
                    result.and_then(|engine_data| {
                        let len = engine_data.len();
                        FilteredEngineData::try_new(engine_data, vec![true; len])
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
        let engine_data = Box::new(ArrowEngineData::from(batch)) as Box<dyn EngineData>;
        let len = engine_data.len();
        Ok(Box::new(std::iter::once(
            FilteredEngineData::try_new(engine_data, vec![true; len]),
        )))
    }

    /// Execute a SchemaQuery node - reads parquet file schema (footer only).
    /// 
    /// This reads only the parquet footer to extract the schema, without reading data.
    /// The schema is stored in the node's state for later retrieval.
    fn execute_schema_query(&self, node: SchemaQueryNode) -> DeltaResult<FilteredDataIter> {
        let file_path = &node.file_path;
        
        // Read the schema from the parquet footer
        let parquet_handler = self.engine.parquet_handler();
        let url =
            url::Url::parse(file_path).map_err(|e| Error::generic(format!("Invalid file path: {}", e)))?;

        // Schema queries need accurate file metadata (especially size) for many parquet readers.
        // Fetch it through the storage handler.
        let file_meta = self.engine.storage_handler().head(&url)?;
        
        // Get the parquet footer which contains the schema
        let footer = parquet_handler.read_parquet_footer(&file_meta)?;
        let schema = footer.schema;
        
        // Store the schema in the node's state
        match &node.state {
            super::kdf_state::SchemaReaderState::SchemaStore(store) => {
                store.store(schema);
            }
        }
        
        // SchemaQuery produces no data rows - return empty iterator
        Ok(Box::new(std::iter::empty()))
    }

    /// Execute a FilterByKDF node using a kernel-defined function (KDF).
    fn execute_filter_by_kdf(
        &self,
        child: DeclarativePlanNode,
        node: FilterByKDF,
    ) -> DeltaResult<FilteredDataIter> {
        // Create owned state from the sender - state is cloned from template
        // and will be sent back to the receiver when dropped
        let mut owned_state = node.create_owned();

        let child_iter = self.execute(child)?;

        Ok(Box::new(child_iter.map(move |result| {
            let (engine_data, selection_vector) = result?.into_parts();

            // Convert selection vector to BooleanArray
            let selection_array = BooleanArray::from(selection_vector);

            // Apply the Filter KDF - zero-lock access via OwnedState
            let new_selection = owned_state.apply(engine_data.as_ref(), selection_array)?;

            // Convert back to Vec<bool>
            let new_selection_vec: Vec<bool> = (0..new_selection.len())
                .map(|i| new_selection.value(i))
                .collect();

            FilteredEngineData::try_new(engine_data, new_selection_vec)
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
    /// This evaluates the predicate against each batch and updates the selection vector.
    /// For data skipping, the predicate evaluates against parsed stats and produces
    /// true (keep), null (keep), or false (skip) for each file.
    fn execute_filter_by_expr(
        &self,
        child: DeclarativePlanNode,
        node: FilterByExpressionNode,
    ) -> DeltaResult<FilteredDataIter> {
        use crate::actions::visitors::SelectionVectorVisitor;
        use crate::engine::arrow_data::extract_record_batch;
        use crate::engine::arrow_conversion::TryFromArrow;
        use crate::expressions::{column_expr, Expression, Predicate};
        use crate::RowVisitor as _;
        use crate::PredicateRef;
        use std::sync::LazyLock;

        // The filter predicate: DISTINCT(output, false) treats true/null as keep, false as skip
        static FILTER_PRED: LazyLock<PredicateRef> =
            LazyLock::new(|| Arc::new(column_expr!("output").distinct(Expression::literal(false))));

        let FilterByExpressionNode { predicate } = node;
        let eval_handler = self.engine.evaluation_handler();
        let child_iter = self.execute(child)?;

        Ok(Box::new(child_iter.map(move |result| {
            let (engine_data, selection_vector) = result?.into_parts();

            // Extract the RecordBatch to get the schema
            let record_batch = extract_record_batch(engine_data.as_ref())?;
            let arrow_schema = record_batch.schema();
            
            // Convert Arrow schema to kernel schema for predicate evaluator
            let input_schema = Arc::new(crate::schema::StructType::try_from_arrow(
                arrow_schema.as_ref()
            )?);

            // Wrap Expression in a Predicate::BooleanExpression
            let pred = Predicate::BooleanExpression(predicate.as_ref().clone());

            // Create predicate evaluator for this batch's schema
            let predicate_evaluator = eval_handler.new_predicate_evaluator(
                input_schema.clone(),
                Arc::new(pred),
            )?;

            // Step 1: Evaluate the skipping predicate (outputs to "output" column)
            let skipping_result = predicate_evaluator.evaluate(engine_data.as_ref())?;

            // Step 2: Apply DISTINCT(output, false) to convert to selection vector
            // For data skipping, we use DISTINCT(output, false) semantics:
            // - true → keep (file may have matching data)
            // - null → keep (can't determine from stats, be conservative)
            // - false → skip (file definitely has no matching data)
            let filter_evaluator = eval_handler.new_predicate_evaluator(
                // The skipping_result has schema { output: BOOLEAN }
                Arc::new(crate::schema::StructType::try_new([
                    crate::schema::StructField::nullable("output", crate::schema::DataType::BOOLEAN)
                ])?),
                FILTER_PRED.clone(),
            )?;
            let selection_result = filter_evaluator.evaluate(skipping_result.as_ref())?;

            // Visit the engine's selection vector to produce Vec<bool>
            let mut visitor = SelectionVectorVisitor::default();
            visitor.visit_rows_of(selection_result.as_ref())?;

            // AND the predicate result with existing selection vector
            let new_selection: Vec<bool> = selection_vector
                .iter()
                .zip(visitor.selection_vector.iter())
                .map(|(existing, pred_result)| *existing && *pred_result)
                .collect();

            FilteredEngineData::try_new(engine_data, new_selection)
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

        let input_schema = self.infer_output_schema(&child)?;
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
            let (engine_data, selection_vector) = result?.into_parts();

            // Build expression to evaluate - wrap in struct if multiple columns
            let expression_to_eval: Arc<Expression> = if columns.len() == 1 {
                columns[0].clone()
            } else {
                Arc::new(Expression::Struct(columns.clone()))
            };

            // Convert output_schema to DataType
            let output_data_type: DataType = (*output_schema).clone().into();

            let evaluator = eval_handler.new_expression_evaluator(
                input_schema.clone(),
                expression_to_eval,
                output_data_type,
            )?;

            let new_data = evaluator.evaluate(engine_data.as_ref())?;

            FilteredEngineData::try_new(new_data, selection_vector)
        })))
    }

    /// Execute a ParseJson node - parses a JSON column into structured data.
    ///
    /// This extracts a JSON string column (like "add.stats"), parses it according to the
    /// target schema.
    /// - If `output_column` is non-empty: adds parsed struct as a new column with that name
    /// - If `output_column` is empty: outputs parsed data at root level (replaces batch)
    fn execute_parse_json(
        &self,
        child: DeclarativePlanNode,
        node: ParseJsonNode,
    ) -> DeltaResult<FilteredDataIter> {
        use crate::arrow::array::{Array, RecordBatch, StructArray};
        use crate::arrow::datatypes::{Field, Schema as ArrowSchema};
        use crate::engine::arrow_data::{ArrowEngineData, extract_record_batch};

        let ParseJsonNode {
            json_column,
            target_schema,
            output_column,
        } = node;

        let json_handler = self.engine.json_handler();
        let child_iter = self.execute(child)?;

        Ok(Box::new(child_iter.map(move |result| {
            let (engine_data, selection_vector) = result?.into_parts();

            // Extract the RecordBatch from EngineData
            let record_batch = extract_record_batch(engine_data.as_ref())?;

            // Find and extract the JSON column (handles nested columns like "add.stats")
            let json_array = extract_nested_string_column(record_batch, &json_column)?;

            // Create a single-column batch with the JSON strings
            let json_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("json", crate::arrow::datatypes::DataType::Utf8, true),
            ]));
            let json_batch = RecordBatch::try_new(
                json_schema,
                vec![Arc::new(json_array) as Arc<dyn Array>],
            ).map_err(|e| Error::Arrow(e.into()))?;
            let json_engine_data: Box<dyn crate::EngineData> = Box::new(ArrowEngineData::new(json_batch));

            // Parse the JSON using the json handler
            let parsed_data = json_handler.parse_json(json_engine_data, target_schema.clone())?;
            let parsed_batch = extract_record_batch(parsed_data.as_ref())?;

            let output_batch = if output_column.is_empty() {
                // Empty output_column: add parsed data fields at root level (merge with existing batch)
                // This is used for data skipping where predicates reference stats directly
                let mut new_fields = record_batch.schema().fields().to_vec();
                new_fields.extend(parsed_batch.schema().fields().iter().cloned());
                let new_schema = Arc::new(ArrowSchema::new(new_fields));

                let mut new_columns: Vec<Arc<dyn Array>> = record_batch.columns().to_vec();
                new_columns.extend(parsed_batch.columns().iter().cloned());

                RecordBatch::try_new(new_schema, new_columns)
                    .map_err(|e| Error::Arrow(e.into()))?
            } else {
                // Non-empty output_column: add parsed struct as new column
                let parsed_struct = StructArray::from(parsed_batch.clone());
                let parsed_field = Field::new(
                    &output_column,
                    crate::arrow::datatypes::DataType::Struct(parsed_batch.schema().fields().clone()),
                    true,
                );

                let mut new_fields = record_batch.schema().fields().to_vec();
                new_fields.push(Arc::new(parsed_field));
                let new_schema = Arc::new(ArrowSchema::new(new_fields));

                let mut new_columns: Vec<Arc<dyn Array>> = record_batch.columns().to_vec();
                new_columns.push(Arc::new(parsed_struct));

                RecordBatch::try_new(new_schema, new_columns)
                    .map_err(|e| Error::Arrow(e.into()))?
            };

            let output_engine_data: Box<dyn crate::EngineData> =
                Box::new(ArrowEngineData::new(output_batch));
            FilteredEngineData::try_new(output_engine_data, selection_vector)
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
/// let executor = DeclarativePlanExecutor::new(engine.as_ref());
/// let snapshot_sm = SnapshotStateMachine::new(table_root)?;
/// let result = execute_state_machine(&executor, snapshot_sm)?;
/// ```
pub fn execute_state_machine<SM: StateMachine>(
    executor: &DeclarativePlanExecutor<'_>,
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

// =============================================================================
// DropOnlyDriver - For state machines with no result streaming
// =============================================================================

/// Driver for state machines that only use Drop sinks (no result streaming).
///
/// This is a convenience wrapper around `ResultsDriver` that validates
/// no Results sinks are used and provides a simpler synchronous API.
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
/// let driver = DropOnlyDriver::new(engine.as_ref(), snapshot_sm);
/// let snapshot: Snapshot = driver.execute()?;
/// ```
pub struct DropOnlyDriver<'a, SM: StateMachine> {
    inner: ResultsDriver<'a, SM>,
}

impl<'a, SM: StateMachine> DropOnlyDriver<'a, SM> {
    /// Create a new DropOnlyDriver with the given engine and state machine.
    pub fn new(engine: &'a dyn Engine, sm: SM) -> Self {
        Self {
            inner: ResultsDriver::new(engine, sm),
        }
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
        // Iterate through the ResultsDriver
        // If we get ANY batch, that means a Results sink was used
        for result in &mut self.inner {
            // Propagate any batch errors
            result?;
            // If we reach here, we got data from a Results sink - this is an error
            return Err(Error::generic(
                "DropOnlyDriver encountered Results sink - use ResultsDriver instead",
            ));
        }
        
        // Iterator exhausted (all Drop sinks executed silently)
        // Get the final result
        self.inner.into_result().ok_or_else(|| {
            Error::generic("State machine did not produce a result")
        })
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
/// let scan = ScanBuilder::new(snapshot).build()?;
/// let scan_sm = ScanStateMachine::from_scan(&scan)?;
/// let driver = ResultsDriver::new(engine.as_ref(), scan_sm);
/// for batch in &mut driver {
///     let data = batch?;
///     // Process streamed scan results
/// }
/// let scan_result = driver.into_result()?;
/// ```
pub struct ResultsDriver<'a, SM: StateMachine> {
    engine: &'a dyn Engine,
    sm: Option<SM>,
    current_iter: Option<FilteredDataIter>,
    current_plan: Option<DeclarativePlanNode>,
    result: Option<SM::Result>,
    is_done: bool,
}

impl<'a, SM: StateMachine> ResultsDriver<'a, SM> {
    /// Create a new ResultsDriver with the given engine and state machine.
    pub fn new(engine: &'a dyn Engine, sm: SM) -> Self {
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
        let executor = DeclarativePlanExecutor::new(self.engine);
        let results = executor.execute(plan)?;
        
        // Consume all results to trigger side effects
        for result in results {
            let _batch = result?;
        }
        
        Ok(())
    }
}

impl<'a, SM: StateMachine> Iterator for ResultsDriver<'a, SM> {
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
                let executor = DeclarativePlanExecutor::new(self.engine);
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

