<!-- 6aa95509-39e6-46a1-aafe-a16903b54e6a 4dd42bb8-e9ea-4467-ae6a-74eca5fa1092 -->
# State Machine Architecture for Delta Kernel Scans

## Core State Machine Design

```rust
/// Result of calling .next() on any phase
pub enum StateMachinePhase<T> {
    /// All phases complete - concrete terminal value
    Terminus(T),
    
    /// Execute plan, yields partial query results
    PartialResult(Box<dyn PartialResultPhase<Output = T>>),
    
    /// Execute plan, consumes result as configuration/metadata
    Consume(Box<dyn ConsumePhase<Output = T>>),
}

/// Phase that yields partial query results
pub trait PartialResultPhase: Send + Sync {
    type Output;
    
    /// Get the plan to execute
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode>;
    
    /// Transition after consuming plan execution batches
    /// Batches are the actual partial query results (e.g., filtered Add actions)
    fn next(
        self: Box<Self>, 
        batches: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>>
    ) -> DeltaResult<StateMachinePhase<Self::Output>>;
    
    /// Whether this phase can execute in parallel
    fn is_parallelizable(&self) -> bool { false }
}

/// Phase that consumes plan execution as input data (not query results)
pub trait ConsumePhase: Send + Sync {
    type Output;
    
    /// Get the plan to execute
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode>;
    
    /// Transition after consuming plan execution as EngineData
    /// EngineData contains configuration (e.g., Protocol, Metadata, file list)
    fn next(
        self: Box<Self>,
        data: Box<dyn EngineData>
    ) -> DeltaResult<StateMachinePhase<Self::Output>>;
}
```

**Key Principles**:

1. No `ExecutionResult` wrapper - just batches or EngineData
2. Accumulated state (like tombstone_set) lives INSIDE phase structs
3. Partial results = actual query data that can be yielded to users
4. Consumed data = configuration/metadata used to build next phase

## Phase Implementations for Snapshot Creation

### SnapshotBuilder returns initial phase

```rust
impl SnapshotBuilder {
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<StateMachinePhase<Snapshot>> {
        Ok(StateMachinePhase::Consume(Box::new(FileListingPhase {
            table_root: self.table_root,
            version: self.version,
        })))
    }
}
```

### Phase 1: FileListingPhase (ConsumePhase)

```rust
pub struct FileListingPhase {
    table_root: Url,
    version: Option<Version>,
}

impl ConsumePhase for FileListingPhase {
    type Output = Snapshot;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let log_root = self.table_root.join("_delta_log/")?;
        let start_version = self.version.unwrap_or(0);
        
        Ok(LogicalPlanNode::ListFiles {
            path: log_root,
            start_from: format!("{:020}", start_version),
        })
    }
    
    fn next(
        self: Box<Self>, 
        file_data: Box<dyn EngineData>
    ) -> DeltaResult<StateMachinePhase<Snapshot>> {
        // file_data = EngineData with columns [path, last_modified, size]
        // Consumed to build LogSegment
        
        let log_segment_builder = LogSegmentBuilder::new();
        let log_segment = log_segment_builder.build_from_engine_data(
            file_data,
            self.table_root.join("_delta_log/")?,
            self.version,
        )?;
        
        Ok(StateMachinePhase::Consume(Box::new(ProtocolMetadataPhase {
            table_root: self.table_root,
            log_segment,
        })))
    }
}
```

### Phase 2: ProtocolMetadataPhase (ConsumePhase)

```rust
pub struct ProtocolMetadataPhase {
    table_root: Url,
    log_segment: LogSegment,
}

impl ConsumePhase for ProtocolMetadataPhase {
    type Output = Snapshot;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let files = self.log_segment.all_log_files();
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        
        LogicalPlanNode::scan_json(files, schema)?
            .filter_ordered(FirstProtocolMetadataFilter::new())
    }
    
    fn next(
        self: Box<Self>,
        pm_data: Box<dyn EngineData>
    ) -> DeltaResult<StateMachinePhase<Snapshot>> {
        // pm_data = EngineData with Protocol and Metadata actions
        // Consumed to build TableConfiguration
        
        let protocol = Protocol::try_new_from_data(pm_data.as_ref())?
            .ok_or(Error::MissingProtocol)?;
        let metadata = Metadata::try_new_from_data(pm_data.as_ref())?
            .ok_or(Error::MissingMetadata)?;
        
        let table_config = TableConfiguration::try_new(
            metadata,
            protocol,
            self.table_root.clone(),
            self.log_segment.end_version,
        )?;
        
        Ok(StateMachinePhase::Terminus(Snapshot::new(
            self.log_segment,
            table_config,
        )))
    }
}
```

## Phase Implementations for Scan

### ScanBuilder returns initial phase

```rust
impl ScanBuilder {
    pub fn build(self) -> DeltaResult<StateMachinePhase<Scan>> {
        let logical_schema = self.schema.unwrap_or_else(|| self.snapshot.schema());
        let state_info = StateInfo::try_new(/* ... */)?;
        
        // Phase owns the stateful filter
        let dedup_filter = Arc::new(SharedAddRemoveDedupFilter::new(
            Arc::new(DashSet::new()),
            logical_schema.clone(),
            true, // is_log_batch
        ));
        
        Ok(StateMachinePhase::PartialResult(Box::new(CommitReplayPhase {
            snapshot: self.snapshot,
            logical_schema,
            physical_schema: Arc::new(StructType::new(state_info.read_fields)),
            physical_predicate,
            all_fields: Arc::new(state_info.all_fields),
            have_partition_cols: state_info.have_partition_cols,
            dedup_filter, // Phase owns the stateful filter!
        })))
    }
}
```

### Scan Phase 1: CommitReplayPhase (PartialResultPhase)

```rust
pub struct CommitReplayPhase {
    snapshot: Arc<Snapshot>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    physical_predicate: Option<(PredicateRef, SchemaRef)>,
    all_fields: Arc<Vec<ColumnType>>,
    have_partition_cols: bool,
    dedup_filter: Arc<SharedAddRemoveDedupFilter>, // Owns stateful filter!
}

impl PartialResultPhase for CommitReplayPhase {
    type Output = Scan;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let commit_files: Vec<_> = self.snapshot
            .log_segment()
            .ascending_commit_files
            .iter()
            .rev()
            .map(|p| p.location.clone())
            .collect();
        
        LogicalPlanNode::scan_json(commit_files, COMMIT_READ_SCHEMA.clone())?
            .filter_ordered(self.dedup_filter.clone()) // Use phase's filter
    }
    
    fn next(
        self: Box<Self>, 
        batches: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>>
    ) -> DeltaResult<StateMachinePhase<Scan>> {
        // batches = filtered Add actions from commits (PARTIAL QUERY RESULT)
        // Can be yielded to user for streaming!
        
        // After execution, self.dedup_filter.tombstone_set is populated
        let tombstone_set = self.dedup_filter.tombstone_set.clone();
        
        if self.snapshot.log_segment().checkpoint_parts.is_empty() {
            // No checkpoints - terminal
            return Ok(StateMachinePhase::Terminus(Scan {
                snapshot: self.snapshot,
                logical_schema: self.logical_schema,
                physical_schema: self.physical_schema,
                physical_predicate: self.physical_predicate,
                all_fields: self.all_fields,
                have_partition_cols: self.have_partition_cols,
            }));
        }
        
        // Create next phase with frozen tombstone set
        let checkpoint_filter = Arc::new(SharedAddRemoveDedupFilter::new(
            tombstone_set, // Frozen from phase 1!
            self.logical_schema.clone(),
            false, // is_log_batch
        ));
        
        Ok(StateMachinePhase::PartialResult(Box::new(CheckpointReplayPhase {
            snapshot: self.snapshot,
            logical_schema: self.logical_schema,
            physical_schema: self.physical_schema,
            physical_predicate: self.physical_predicate,
            all_fields: self.all_fields,
            have_partition_cols: self.have_partition_cols,
            dedup_filter: checkpoint_filter,
        })))
    }
    
    fn is_parallelizable(&self) -> bool {
        false // SEQUENTIAL - building tombstone_set
    }
}
```

### Scan Phase 2: CheckpointReplayPhase (PartialResultPhase)

```rust
pub struct CheckpointReplayPhase {
    snapshot: Arc<Snapshot>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    physical_predicate: Option<(PredicateRef, SchemaRef)>,
    all_fields: Arc<Vec<ColumnType>>,
    have_partition_cols: bool,
    dedup_filter: Arc<SharedAddRemoveDedupFilter>, // Frozen tombstone_set inside
}

impl PartialResultPhase for CheckpointReplayPhase {
    type Output = Scan;
    
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode> {
        let checkpoint_files: Vec<_> = self.snapshot
            .log_segment()
            .checkpoint_parts
            .iter()
            .map(|p| p.location.clone())
            .collect();
        
        LogicalPlanNode::scan_parquet(checkpoint_files, CHECKPOINT_READ_SCHEMA.clone())?
            .filter(self.dedup_filter.clone()) // Read-only tombstone_set
    }
    
    fn next(
        self: Box<Self>, 
        batches: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>>
    ) -> DeltaResult<StateMachinePhase<Scan>> {
        // batches = filtered Add actions from checkpoints (PARTIAL QUERY RESULT)
        // Can be yielded to user for streaming!
        
        // All metadata collected - terminal
        Ok(StateMachinePhase::Terminus(Scan {
            snapshot: self.snapshot,
            logical_schema: self.logical_schema,
            physical_schema: self.physical_schema,
            physical_predicate: self.physical_predicate,
            all_fields: self.all_fields,
            have_partition_cols: self.have_partition_cols,
        }))
    }
    
    fn is_parallelizable(&self) -> bool {
        true // PARALLEL - tombstone_set is frozen!
    }
}
```

## Stateful UDF: LogSegmentBuilder

```rust
/// Stateful UDF that constructs LogSegment from file listing
pub struct LogSegmentBuilder {
    log_root: Url,
    version: Option<Version>,
    // Internal state accumulated during processing
    ascending_commit_files: Vec<ParsedLogPath>,
    ascending_compaction_files: Vec<ParsedLogPath>,
    checkpoint_parts: Vec<ParsedLogPath>,
    latest_crc_file: Option<ParsedLogPath>,
}

impl RowVisitor for LogSegmentBuilder {
    fn visit(&mut self, row_count: usize, getters: &[&dyn GetData]) -> DeltaResult<()> {
        // Accumulate files into internal state
        for i in 0..row_count {
            let path: &str = getters[0].get(i, "path")?;
            let parsed = ParsedLogPath::try_from(path)?;
            
            match parsed.file_type {
                LogPathFileType::Commit => self.ascending_commit_files.push(parsed),
                LogPathFileType::CompactedCommit{..} => self.ascending_compaction_files.push(parsed),
                // ... etc
            }
        }
        Ok(())
    }
}

impl LogSegmentBuilder {
    /// Finalize into LogSegment after visiting all file batches
    pub fn finalize(self) -> DeltaResult<LogSegment> {
        LogSegment::try_new(
            ListedLogFiles {
                ascending_commit_files: self.ascending_commit_files,
                // ... other fields
            },
            self.log_root,
            self.version,
        )
    }
}
```

## State Machine Iterator (To Be Designed Separately)

The iterator implementation will be designed separately. It needs to handle:

1. **Automatic Consume phase transitions** (invisible to user)
2. **Lazy batch-by-batch yielding** from PartialResult phases
3. **Terminal detection** when all phases complete

This complexity will be addressed in a follow-up design document.

For now, phases can be executed manually as shown in the Sequential Example below.

```rust
/// Iterator that executes phases and yields partial results batch-by-batch
pub struct StateMachineIterator<T> {
    /// Current state machine phase
    phase: Option<StateMachinePhase<T>>,
    /// Executor for running plans
    executor: Arc<AsyncPlanExecutor>,
    /// Active batch stream from current PartialResultPhase (if any)
    current_batch_stream: Option<BoxStream<DeltaResult<Box<dyn EngineData>>>>,
    /// PartialResultPhase being consumed (held for next() transition after batches exhausted)
    held_partial_phase: Option<Box<dyn PartialResultPhase<Output = T>>>,
}

impl<T> StateMachineIterator<T> {
    pub fn new(initial_phase: StateMachinePhase<T>, executor: Arc<AsyncPlanExecutor>) -> Self {
        Self {
            phase: Some(initial_phase),
            executor,
            current_batch_stream: None,
            held_partial_phase: None,
        }
    }
    
    /// Get next partial result batch, or None if terminal reached
    pub async fn next(&mut self) -> Option<DeltaResult<Box<dyn EngineData>>> {
        loop {
            // CASE 1: We have an active batch stream - yield from it
            if let Some(stream) = &mut self.current_batch_stream {
                match stream.next().await {
                    Some(batch) => return Some(batch), // YIELD batch to user
                    None => {
                        // Batches exhausted - transition to next phase
                        self.current_batch_stream = None;
                        let partial_phase = self.held_partial_phase.take()?;
                        
                        // Call next() with empty iterator (batches already consumed)
                        match partial_phase.next(Box::new(std::iter::empty())) {
                            Ok(next_phase) => {
                                self.phase = Some(next_phase);
                                continue; // STATE TRANSITION - loop to handle new phase
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }
            }
            
            // CASE 2: No active stream - process current phase
            let current_phase = self.phase.take()?;
            
            match current_phase {
                StateMachinePhase::Terminus(_) => {
                    // Terminal reached - iterator exhausted
                    self.phase = Some(current_phase); // Put back for into_terminal()
                    return None; // Iterator done
                }
                
                StateMachinePhase::PartialResult(p) => {
                    // Execute plan and start yielding batches
                    let plan = match p.get_plan() {
                        Ok(plan) => plan,
                        Err(e) => return Some(Err(e)),
                    };
                    
                    let batch_stream = if p.is_parallelizable() {
                        match self.executor.execute_parallel(plan).await {
                            Ok(s) => s,
                            Err(e) => return Some(Err(e)),
                        }
                    } else {
                        match self.executor.execute(plan).await {
                            Ok(s) => s,
                            Err(e) => return Some(Err(e)),
                        }
                    };
                    
                    // Store stream and phase for later
                    self.current_batch_stream = Some(batch_stream);
                    self.held_partial_phase = Some(p);
                    continue; // Loop to yield first batch
                }
                
                StateMachinePhase::Consume(p) => {
                    // STATE TRANSITION - Consume phases don't yield, handled internally
                    let plan = match p.get_plan() {
                        Ok(plan) => plan,
                        Err(e) => return Some(Err(e)),
                    };
                    
                    let batch_stream = match self.executor.execute(plan).await {
                        Ok(s) => s,
                        Err(e) => return Some(Err(e)),
                    };
                    
                    // Collect all batches into single EngineData
                    let data = match Self::collect_stream(batch_stream).await {
                        Ok(d) => d,
                        Err(e) => return Some(Err(e)),
                    };
                    
                    // Transition immediately (consume doesn't yield)
                    match p.next(data) {
                        Ok(next_phase) => {
                            self.phase = Some(next_phase);
                            continue; // STATE TRANSITION - loop to next phase
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }
    
    /// Get terminal result after iterator exhausted
    pub fn into_terminal(self) -> DeltaResult<T> {
        match self.phase {
            Some(StateMachinePhase::Terminus(t)) => Ok(t),
            Some(_) => Err(Error::internal("State machine not yet terminal - call next() until None")),
            None => Err(Error::internal("State machine phase was taken")),
        }
    }
    
    async fn collect_stream(
        mut stream: BoxStream<DeltaResult<Box<dyn EngineData>>>
    ) -> DeltaResult<Box<dyn EngineData>> {
        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        // Concatenate all batches or return single
        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            // Concatenate logic
            engine.concatenate_batches(batches)
        }
    }
}
```

## Iterator Execution Flow

### Consume Phase Behavior (Internal State Transitions)

```
User: iter.next()
  ↓
Check: current_batch_stream? NO
Check: phase? = Consume(FileListingPhase)
  → Execute ListFiles plan
  → Collect ALL batches → EngineData
  → phase.next(EngineData) → Consume(ProtocolMetadataPhase)
  → LOOP (don't yield, transition again)
  ↓
Check: phase? = Consume(ProtocolMetadataPhase)
  → Execute P&M plan
  → Collect ALL batches → EngineData
  → phase.next(EngineData) → PartialResult(CommitReplayPhase)
  → LOOP (don't yield, transition again)
  ↓
Check: phase? = PartialResult(CommitReplayPhase)
  → Execute commit plan
  → Get batch STREAM
  → Store stream in current_batch_stream
  → Store phase in held_partial_phase
  → LOOP (go to case 1)
  ↓
Check: current_batch_stream? YES
  → stream.next() → Some(batch1)
  → RETURN Some(Ok(batch1)) ← USER GETS FIRST BATCH
```

### PartialResult Phase Behavior (Lazy Yielding)

```
User: iter.next() (again)
  ↓
Check: current_batch_stream? YES
  → stream.next() → Some(batch2)
  → RETURN Some(Ok(batch2)) ← USER GETS BATCH 2

User: iter.next() (again)
  ↓
Check: current_batch_stream? YES
  → stream.next() → Some(batch3)
  → RETURN Some(Ok(batch3)) ← USER GETS BATCH 3

User: iter.next() (again)
  ↓
Check: current_batch_stream? YES
  → stream.next() → None (exhausted!)
  → held_partial_phase.next(empty_iter) → PartialResult(CheckpointReplayPhase)
  → current_batch_stream = None
  → LOOP (process next phase)
  ↓
Check: phase? = PartialResult(CheckpointReplayPhase)
  → Execute checkpoint plan
  → Get batch STREAM
  → Store stream in current_batch_stream
  → LOOP
  ↓
Check: current_batch_stream? YES
  → stream.next() → Some(batch4)
  → RETURN Some(Ok(batch4)) ← USER GETS FIRST CHECKPOINT BATCH
```

### Terminal Behavior

```
User: iter.next()
  ↓
Check: current_batch_stream? YES
  → stream.next() → None (exhausted!)
  → held_partial_phase.next(empty_iter) → Terminus(Scan)
  → LOOP
  ↓
Check: phase? = Terminus(Scan)
  → RETURN None ← Iterator done

User: iter.into_terminal()
  → extract Scan from Terminus
  → RETURN Ok(Scan)
```

## Usage Patterns

### Pattern 1: Lazy streaming (yields partial results)

```rust
let mut iter = StateMachineIterator::new(
    snapshot.scan_builder().build()?,
    executor,
);

// Yields batches one at a time
while let Some(batch) = iter.next().await {
    let batch = batch?;
    // Process batch immediately
    process_batch(batch)?;
}

// After iteration complete, get terminal
let scan = iter.into_terminal()?;
```

### Pattern 2: Run to terminal (skip partials)

```rust
let scan = snapshot.scan_builder().build()?
    .run_to_terminal(executor).await?;
```

### Pattern 3: Collect partials + terminal

```rust
let iter = StateMachineIterator::new(
    snapshot.scan_builder().build()?,
    executor,
);

let batches: Vec<_> = iter.collect().await?;
let scan = iter.into_terminal()?;
```

## How Iterator Handles Phases

### Consume Phases (internal state transitions)

```
User calls: iter.next()
  ↓
State = Consume(FileListingPhase)
  → Execute ListFiles plan
  → Collect batches into EngineData
  → Call phase.next(data)
  → Transition to Consume(ProtocolMetadataPhase)
  ↓
State = Consume(ProtocolMetadataPhase)  
  → Execute P&M scan plan
  → Collect batches into EngineData
  → Call phase.next(data)
  → Transition to PartialResult(CommitReplayPhase)
  ↓
State = PartialResult(CommitReplayPhase)
  → Execute commit scan plan
  → Get batch iterator
  → Store iterator + phase
  → Yield first batch ← USER SEES THIS
```

### PartialResult Phases (lazy batch iteration)

```
User calls: iter.next() again
  ↓
Have active batch iterator from CommitReplayPhase
  → Yield next batch ← USER SEES THIS
  
User calls: iter.next() again
  → Yield next batch ← USER SEES THIS
  
... (repeat)

User calls: iter.next() again
  → No more batches in iterator
  → Call phase.next(empty_iter) to transition
  → Transition to PartialResult(CheckpointReplayPhase)
  → Execute checkpoint scan plan
  → Yield first batch ← USER SEES THIS
```

## Key Differences from Previous Design

1. **Consume phases are invisible** - iterator handles them internally
2. **Partial results yielded batch-by-batch** - not all at once
3. **State transitions automated** - user just calls next()
4. **Terminal obtained after iteration** - via into_terminal()

## Complete Sequential Example (kept for clarity)

```rust
let url = Url::parse("s3://bucket/my_table")?;

// ============= SNAPSHOT PHASE 1: FILE LISTING (Consume) =============
let snapshot_phase = Snapshot::builder(url.clone()).build(engine)?;

let listing_phase = match snapshot_phase {
    StateMachinePhase::Consume(p) => p,
    _ => unreachable!(),
};

let listing_plan = listing_phase.get_plan()?;
// Plan: ListFiles("_delta_log/", "00000000000000000000")

let file_batches = executor.execute(listing_plan).await?;
let file_data = collect_batches(file_batches)?; // Single EngineData

let pm_phase_sm = listing_phase.next(file_data)?; // CONSUME file list

// ============= SNAPSHOT PHASE 2: P&M (Consume) =============
let pm_phase = match pm_phase_sm {
    StateMachinePhase::Consume(p) => p,
    _ => unreachable!(),
};

let pm_plan = pm_phase.get_plan()?;
// Plan: Scan([...], [protocol, metaData]) → Filter(FirstPM)

let pm_batches = executor.execute(pm_plan).await?;
let pm_data = collect_batches(pm_batches)?; // Single EngineData

let snapshot_terminal = pm_phase.next(pm_data)?; // CONSUME P&M

// ============= SNAPSHOT TERMINUS =============
let snapshot = match snapshot_terminal {
    StateMachinePhase::Terminus(s) => Arc::new(s),
    _ => unreachable!(),
};

// ============= SCAN PHASE 1: COMMIT REPLAY (PartialResult) =============
let scan_phase = snapshot.scan_builder().with_predicate(pred).build()?;

let commit_phase = match scan_phase {
    StateMachinePhase::PartialResult(p) => p,
    _ => unreachable!(),
};

let commit_plan = commit_phase.get_plan()?;
// Plan: Scan([commits], COMMIT_SCHEMA) → Filter(DedupFilter{ordered=true})

let commit_batches = executor.execute(commit_plan).await?;
// commit_batches = Iterator<EngineData> of filtered Add actions

// Can YIELD commit_batches to user as partial results!
let partial_result_1 = commit_batches.clone();

let checkpoint_phase_or_terminal = commit_phase.next(commit_batches)?;

// ============= SCAN PHASE 2: CHECKPOINT REPLAY (PartialResult) =============
let checkpoint_phase = match checkpoint_phase_or_terminal {
    StateMachinePhase::PartialResult(p) => p,
    StateMachinePhase::Terminus(scan) => return Ok(scan),
    _ => unreachable!(),
};

let checkpoint_plan = checkpoint_phase.get_plan()?;
// Plan: Scan([checkpoints], CHECKPOINT_SCHEMA) → Filter(DedupFilter{ordered=false})

let checkpoint_batches = executor.execute_parallel(checkpoint_plan).await?;
// checkpoint_batches = Iterator<EngineData> of filtered Add actions

// Can YIELD checkpoint_batches as partial results!
let partial_result_2 = checkpoint_batches.clone();

let scan_terminal = checkpoint_phase.next(checkpoint_batches)?;

// ============= SCAN TERMINUS =============
let scan = match scan_terminal {
    StateMachinePhase::Terminus(s) => s,
    _ => unreachable!(),
};
```

## Iterator Pattern Usage

```rust
// Streaming with partial results
let mut sm_iter = StateMachineIterator::new(
    Snapshot::builder(url).build(engine)?,
    executor.clone(),
);

while let Some(result) = sm_iter.next().await {
    match result? {
        PartialOrTerminal::Partial(batches) => {
            // Process partial results as they come!
            for batch in batches {
                process_batch(batch?)?;
            }
        }
        PartialOrTerminal::Terminal(snapshot) => {
            println!("Snapshot ready at version {}", snapshot.version());
            break;
        }
    }
}

// Or just run to terminal
let snapshot = StateMachineIterator::new(
    Snapshot::builder(url).build(engine)?,
    executor,
).run_to_terminal().await?;
```

## Key Design Points

### 1. Phases Own Stateful Filters/UDFs

```rust
pub struct CommitReplayPhase {
    dedup_filter: Arc<SharedAddRemoveDedupFilter>, // Phase owns it
}

// SharedAddRemoveDedupFilter stores tombstone_set internally
pub struct SharedAddRemoveDedupFilter {
    tombstone_set: Arc<DashSet<FileActionKey>>, // Mutated during filtering
    is_log_batch: bool,
}
```

### 2. State Flows Through Phase Transitions

```rust
// Phase 1 creates filter with empty tombstone_set
let filter1 = SharedAddRemoveDedupFilter::new(DashSet::new(), true);

// During execution, filter1.tombstone_set gets populated

// Phase 1's next() extracts tombstone_set and passes to Phase 2
fn next(self, batches) {
    let tombstone_set = self.dedup_filter.tombstone_set.clone(); // Extract
    let filter2 = SharedAddRemoveDedupFilter::new(tombstone_set, false); // Pass to phase 2
    Ok(PartialResult(CheckpointReplayPhase { dedup_filter: filter2, ... }))
}
```

### 3. PartialResult vs Consume

- **PartialResult**: Batches = query results (Add actions) → YIELD to user
- **Consume**: Batches = configuration data (P&M, file list) → consumed internally

### 4. LogSegment as Stateful UDF

LogSegmentBuilder implements RowVisitor, accumulates files internally, then finalizes to LogSegment.

## Benefits

1. **Clean separation**: Executor doesn't know about accumulated_state
2. **Streaming**: Partial results can be yielded incrementally
3. **Parallelization**: CheckpointReplayPhase marked as parallelizable
4. **Testability**: Each phase independently testable
5. **State encapsulation**: Filters/UDFs own their state

## Design Decisions

### ✅ **RESOLVED: Sidecar File Handling**

**Decision**: Store sidecar paths internally in CheckpointDedupFilter UDF (max 200 items, very small)

Current: `LogSegment::process_sidecars()` (line 443 of log_segment.rs) reads checkpoint, finds sidecar references, then reads sidecar files.

**Question**: How do sidecars fit in state machine?

**Option A**: Nested state machine

```rust
CheckpointReplayPhase.get_plan() → 
  Scan(checkpoint.parquet) → 
    discovers sidecars →
      spawns Scan(sidecar1.parquet) + Scan(sidecar2.parquet)
```

**Option B**: Explicit sidecar phase

```rust
CheckpointReplayPhase → SidecarReplayPhase → Terminus
```

**Option C**: Handle in Filter node

```rust
// Filter discovers sidecars and internally reads them
Filter(CheckpointDedupFilter) // internally handles sidecars
```

### 2. **batches Parameter in PartialResultPhase.next()**

Current signature:

```rust
fn next(
    self: Box<Self>, 
    batches: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>>
) -> DeltaResult<StateMachinePhase<Self::Output>>;
```

**Issue**: Iterator consumes batches as it yields them. By the time `next()` is called, batches are empty!

**Option A**: Don't pass batches

```rust
fn next(self: Box<Self>) -> DeltaResult<StateMachinePhase<Self::Output>>;
// Phase extracts what it needs from internal state (e.g., tombstone_set)
```

**Option B**: Pass summary data

```rust
fn next(self: Box<Self>, summary: PhaseSummary) -> DeltaResult<StateMachinePhase<Self::Output>>;
// summary = { batch_count, row_count, accumulated_state_extracted }
```

### 3. **Data Scanning After Scan Terminal**

After `Terminus(Scan)`, user calls `scan.execute()` to read actual table data.

**Question**: Should `scan.execute()` also return a state machine?

Currently: `scan.execute()` returns `Iterator<ScanResult>`

With state machine: `scan.execute()` returns `StateMachinePhase<Iterator<ScanResult>>`?

Or is data scanning fundamentally different (no multi-phase)?

### 4. **Incremental Scan Updates (scan_metadata_from)**

Current: `scan.scan_metadata_from(version, existing_data, predicate)` (line 571 of scan/mod.rs)

**Question**: How does this work with phases?

**Option A**: Special InjectPhase

```rust
IncrementalScanPhase {
    existing_metadata: Vec<ScanMetadata>,
    existing_version: Version,
}
```

**Option B**: Different state machine entry point

```rust
ScanBuilder::build_incremental(existing_data) → different phase chain
```

### 5. **Table Changes (CDF) Two-Phase Per Commit**

Current: `LogReplayScanner::try_new()` reads entire commit twice (line 145 of table_changes/log_replay.rs):

- Phase 1: Find CDC actions, build remove_dvs map
- Phase 2: Generate selection vectors

**Question**: How to model this?

**Option A**: Two phases per commit (expensive)

```rust
PrepareCommitPhase(commit_v001) → ScanCommitPhase(commit_v001) →
PrepareCommitPhase(commit_v002) → ScanCommitPhase(commit_v002) → ...
```

**Option B**: Single phase that reads once but processes twice

```rust
// Cache commit data in memory, process twice
CDFCommitPhase { cached_actions: Vec<EngineData> }
```

### 6. **Write Path / Transaction Phases**

Current: `Transaction::commit()` (line 146 of transaction/mod.rs):

- Generate Add actions
- Write JSON to _delta_log
- Handle conflicts

**Question**: Should writes also use state machines?

Probably not - writes are single-phase atomic operations. But worth confirming.

### 7. **Error Handling & Resumption**

**Question**: If a phase fails mid-execution, can we resume?

Phases are immutable and self-contained, so theoretically yes. But need to think through:

- Partial file writes
- Corrupted state in stateful filters
- Network failures mid-stream

### 8. **Caching / Memoization**

**Question**: Can engines cache phase execution results?

```rust
// Cache commit replay results keyed by (snapshot_version, predicate)
cache.get(key) → skip CommitReplayPhase, inject cached metadata
```

How would this work with ConsumePhase vs PartialResultPhase?

## Recommended Priority

**Must Resolve Before Implementation**:

1. Sidecar handling (critical for correctness)
2. batches parameter in next() (API design)

**Can Defer**:

3. Data scanning phases (maybe not needed)
4. Incremental scans (optimization)
5. Table Changes (separate feature)
6. Write path (likely doesn't need SM)
7. Error handling (can evolve)
8. Caching (optimization)

## Required Implementation

1. Update `SharedAddRemoveDedupFilter` to actually filter (not return `true` always)
2. Make `SharedAddRemoveDedupFilter.tombstone_set` accessible for extraction in `next()`
3. Implement `LogSegmentBuilder` as stateful RowVisitor
4. Update `ScanBuilder::build()` to return `StateMachinePhase<Scan>`
5. **Resolve sidecars question**
6. **Resolve batches parameter question**