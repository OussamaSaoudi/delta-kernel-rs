# Delta Kernel FFI Connector Implementation Guide

This guide provides everything needed to implement a Delta Lake connector (e.g., for Apache Spark/Java, C++, Python, etc.) using the Delta Kernel Rust FFI.

**Last Updated:** November 1, 2025  
**Target Audience:** Developers building connectors for engines like Apache Spark, Presto, Flink, etc.

---

## Table of Contents

1. [Overview](#overview)
2. [Core Architecture](#core-architecture)
3. [State Machine Pattern](#state-machine-pattern)
4. [Logical Plan Visitor Pattern](#logical-plan-visitor-pattern)
5. [Expression Visitor Pattern](#expression-visitor-pattern)
6. [Arrow C Data Interface](#arrow-c-data-interface)
7. [Snapshot State Machine Phases](#snapshot-state-machine-phases)
8. [Scan State Machine Phases](#scan-state-machine-phases)
9. [Implementation Workflow](#implementation-workflow)
10. [Handle Management](#handle-management)
11. [Example Code](#example-code)
12. [Testing Strategy](#testing-strategy)

---

## Overview

### What is Delta Kernel?

Delta Kernel is a library that provides read and write capabilities for Delta Lake tables. The Rust implementation exposes its functionality through a C FFI (Foreign Function Interface), allowing any language with C interop to use it.

### Key Design Principles

1. **State Machine Architecture**: All operations (Snapshot construction, Scan execution) are modeled as state machines with explicit phases.
2. **Logical Plans**: Instead of executing operations directly, the kernel returns logical plans that the engine must execute.
3. **Visitor Pattern**: Complex data structures (logical plans, expressions, schemas) are exposed through visitor callbacks.
4. **Arrow Integration**: Data is passed between Rust and the engine using the Arrow C Data Interface.
5. **Opaque Handles**: Rust objects are exposed as opaque handles that must be freed by the caller.

### Why State Machines?

The state machine pattern allows the engine (your connector) to:
- **Control I/O**: The engine performs all file operations (listing, reading)
- **Control Parallelism**: The engine decides what to parallelize
- **Control Resource Management**: The engine manages memory, threads, and scheduling
- **Incremental Processing**: Process large datasets in chunks without loading everything into memory

---

## Core Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Your Engine (Java/C++/etc)               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Snapshot   │  │     Scan     │  │   Execute    │         │
│  │ State Machine│→ │ State Machine│→ │ Logical Plans│         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│         ↓                  ↓                  ↓                 │
└─────────┼──────────────────┼──────────────────┼─────────────────┘
          │                  │                  │
          │ Arrow C Data     │ Arrow C Data     │ Arrow C Data
          ↓                  ↓                  ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Delta Kernel Rust FFI                      │
│  • visit_logical_plan()  • visit_expression()                   │
│  • ConsumePhase::next()  • PartialResultPhase::get_result()    │
└─────────────────────────────────────────────────────────────────┘
```

---

## State Machine Pattern

### State Machine Phases

Every state machine operation returns one of three phase types:

```rust
pub enum StateMachinePhase<T> {
    // Terminal phase: operation complete, result available
    Terminus(T),
    
    // Partial result available, but more work needed
    PartialResult(Box<dyn PartialResultPhase<Output = T>>),
    
    // Need data from engine to continue
    Consume(Box<dyn ConsumePhase<Output = T>>),
}
```

### PartialResultPhase Trait

```rust
pub trait PartialResultPhase: Send + Sync {
    type Output;
    
    // Get the partial result without consuming the phase
    fn get_result(&self) -> DeltaResult<Self::Output>;
    
    // Continue to the next phase
    fn next(self: Box<Self>) -> DeltaResult<StateMachinePhase<Self::Output>>;
}
```

**Use case**: When you have intermediate results (e.g., a partially constructed Snapshot) that can be used immediately, but the state machine needs to continue processing.

### ConsumePhase Trait

```rust
pub trait ConsumePhase: Send + Sync {
    type Output;
    
    // Get the logical plan to execute
    fn get_plan(&self) -> DeltaResult<LogicalPlanNode>;
    
    // Provide data and get the next phase
    fn next(
        self: Box<Self>,
        data: Box<dyn Iterator<Item = DeltaResult<RecordBatch>> + Send>
    ) -> DeltaResult<StateMachinePhase<Self::Output>>;
}
```

**Use case**: When the kernel needs data from the engine (e.g., file listing, file contents) to continue processing.

### State Machine Workflow

```
1. Start with initial phase
2. Loop:
   a. Match on phase type:
      - Terminus(result) → Done! Return result
      - PartialResult(phase) → Get result if needed, call phase.next()
      - Consume(phase) → Call phase.get_plan(), execute plan, call phase.next(data)
   b. Repeat with next phase
```

---

## Logical Plan Visitor Pattern

### Overview

Logical plans represent operations the engine must execute. Instead of exposing the plan structure directly, Delta Kernel uses the visitor pattern.

### Logical Plan Node Types

| Node Type | Purpose | Key Fields |
|-----------|---------|------------|
| `Scan` | Read files from storage | files, schema, file_type (Parquet/JSON) |
| `Filter` | Apply a row-level filter | child plan, filter visitor |
| `Select` | Project columns | child plan, output schema |
| `Union` | Combine two plans | left plan, right plan |
| `ParseJson` | Parse JSON string column | child plan, json_column, target_schema, output_column |
| `FilterByExpression` | Filter using a predicate | child plan, predicate |
| `FileListing` | List files from storage | path |
| `FirstNonNull` | Get first non-null value | child plan, column names |
| `DataVisitor` | Apply custom visitor | child plan, visitor |

### Visitor Pattern Implementation

**Step 1: Define your plan representation**

```c
// Your engine's representation of plans
typedef struct {
    enum PlanType type;  // Scan, Filter, Select, etc.
    void* plan_data;     // Type-specific data
} MyEnginePlan;
```

**Step 2: Implement visitor callbacks**

```c
void visit_scan_callback(
    void* user_data,
    uintptr_t sibling_list_id,
    uint8_t file_type,
    const KernelStringSlice* file_paths,
    uintptr_t num_files,
    HandleSharedSchema schema)
{
    // Allocate your engine's Scan plan
    // Store file paths, schema, etc.
    // Add to sibling list
}

void visit_filter_by_expression_callback(
    void* user_data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    HandleSharedPredicate predicate)
{
    // Allocate your FilterByExpression plan
    // Visit the predicate using visit_predicate()
    // Store child plan reference
}

// ... implement for all node types
```

**Step 3: Create visitor and call visit_logical_plan**

```c
EnginePlanVisitor visitor = {
    .data = &my_plan_builder,
    .make_plan_list = make_plan_list_impl,
    .visit_scan = visit_scan_callback,
    .visit_filter = visit_filter_callback,
    .visit_select = visit_select_callback,
    .visit_union = visit_union_callback,
    .visit_parse_json = visit_parse_json_callback,
    .visit_filter_by_expression = visit_filter_by_expression_callback,
    .visit_file_listing = visit_file_listing_callback,
    .visit_first_non_null = visit_first_non_null_callback,
    .visit_data_visitor = visit_data_visitor_callback,
};

uintptr_t root_id = visit_logical_plan(plan_handle, &visitor);
MyEnginePlan* plan = get_plan_from_list(root_id);
```

### Key Implementation Details

1. **List Management**: The visitor uses lists to manage child plans. Your implementation must:
   - Allocate lists with `make_plan_list(data, reserve_count)`
   - Add plans to lists with their sibling_list_id
   - Retrieve lists by their ID to get child plans

2. **Recursive Structure**: Plans are trees. Child plans are passed as list IDs that you must resolve.

3. **Schema Handles**: Schemas are passed as opaque handles. Use `visit_schema()` to extract schema information.

4. **Predicate Handles**: Predicates are passed as opaque handles. Use `visit_predicate()` to extract predicate information.

---

## Expression Visitor Pattern

### Overview

Expressions represent computations (predicates, column references, literals, etc.). Like plans, they use the visitor pattern.

### Expression Types

| Expression Type | Purpose | Example |
|----------------|---------|---------|
| Column | Reference a column | `id`, `add.path` |
| Literal | Constant value | `10`, `"hello"`, `true` |
| BinaryOp | Binary operations | `a + b`, `a > b` |
| UnaryOp | Unary operations | `NOT a`, `IS NULL a` |
| Variadic | N-ary operations | `AND(a, b, c)`, `OR(...)` |
| Transform | Struct field transformations | Complex struct manipulation |

### Visitor Implementation

Similar to logical plans, you implement callbacks for each expression type:

```c
EngineExpressionVisitor visitor = {
    .data = &my_expr_builder,
    .make_field_list = make_field_list_impl,
    .visit_literal_int = visit_int_literal_callback,
    .visit_literal_string = visit_string_literal_callback,
    .visit_column = visit_column_callback,
    .visit_eq = visit_eq_callback,
    .visit_gt = visit_gt_callback,
    .visit_and = visit_and_callback,
    .visit_or = visit_or_callback,
    // ... etc
};

uintptr_t expr_id = visit_expression(&expr_handle, &visitor);
```

**See `ffi/examples/visit-expression/` for a complete working example.**

---

## Arrow C Data Interface

### Overview

All data exchange between Delta Kernel and your engine uses Apache Arrow's C Data Interface. This is a standardized way to pass columnar data across language boundaries.

### Key Structures

```c
// Defined by Arrow C Data Interface specification
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};
```

### Converting to Arrow

When the kernel needs data from your engine (via `ConsumePhase::next()`), you must:

1. Execute the logical plan
2. Get results as your engine's native format
3. Convert to Arrow RecordBatch (ArrowArray + ArrowSchema)
4. Pass to `ConsumePhase::next()`

**Example (pseudo-code for Java/Spark):**

```java
// Execute plan and get Spark DataFrame/Dataset
Dataset<Row> result = executePlan(plan);

// Convert to Arrow
ArrowRecordBatch arrowBatch = convertToArrow(result);

// Pass to FFI (via JNI)
nativeConsumePhaseNext(phaseHandle, arrowBatch);
```

### Important Notes

1. **Ownership**: Arrow arrays must remain valid until the `release` callback is called.
2. **Schema Matching**: The schema must exactly match what the kernel expects (check via `get_plan()`).
3. **Batching**: You can pass data in multiple batches via an iterator.

---

## Snapshot State Machine Phases

### Purpose

A `Snapshot` represents the state of a Delta table at a specific version. The state machine builds this incrementally.

### Phase Flow

```
SnapshotBuilder::into_state_machine()
    ↓
ConsumePhase: CheckpointHintPhase
    → Logical Plan: FileListing("_delta_log/_last_checkpoint")
    → Consumes: Arrow data with checkpoint hint info
    ↓
ConsumePhase: ListingPhase
    → Logical Plan: FileListing("_delta_log/00000000000000000000")
    → Consumes: Arrow data with FileMeta (location, last_modified, size)
    ↓
ConsumePhase: ProtocolMetadataPhase
    → Logical Plan: Scan + FirstNonNull (to get protocol and metadata)
    → Consumes: Arrow data with protocol and metadata actions
    ↓
Terminus: Snapshot
```

### Phase Details

#### 1. CheckpointHintPhase

**Purpose**: Read `_last_checkpoint` file to know where to start.

**Logical Plan**: 
- `FileListingNode` with path `<table>/_delta_log/_last_checkpoint`

**Expected Data**:
```json
{
  "version": 10,
  "size": 12345,
  "parts": 1  // Optional: for multi-part checkpoints
}
```

**Arrow Schema**:
```
version: Int64
size: Int64
parts: Int64 (nullable)
```

**Error Handling**: If file doesn't exist (FileNotFound), return empty Arrow batch → phase will list from version 0.

#### 2. ListingPhase

**Purpose**: List all log files (commits and checkpoints) from the starting version.

**Logical Plan**:
- `FileListingNode` with path `<table>/_delta_log/<version_prefix>`
- Example: `<table>/_delta_log/00000000000000000005` to list from version 5

**Expected Data**: List of files with metadata

**Arrow Schema**:
```
location: Utf8 (file URL)
last_modified: Int64 (milliseconds since epoch)
size: Int64 (bytes)
```

**Filtering**: Only return files that match Delta log patterns:
- Commits: `00000000000000000123.json`
- Checkpoints: `00000000000000000123.checkpoint.parquet`
- Multi-part checkpoints: `00000000000000000123.checkpoint.0000000001.0000000010.parquet`

**Version Filtering**: Only return files ≤ requested version.

#### 3. ProtocolMetadataPhase

**Purpose**: Extract Protocol and Metadata actions from the log.

**Logical Plan**:
- `Scan` of categorized commit/checkpoint files
- `FirstNonNull` to get the latest protocol and metadata (first non-null in the stream)

**Expected Data**: Protocol and Metadata actions

**Arrow Schema**: The kernel provides the schema via `get_commit_schema().project(&["protocol", "metadata"])`

**Key Fields**:
```
protocol: Struct {
    minReaderVersion: Int32
    minWriterVersion: Int32
    readerFeatures: List<Utf8>
    writerFeatures: List<Utf8>
}

metadata: Struct {
    id: Utf8
    name: Utf8
    description: Utf8
    schemaString: Utf8
    partitionColumns: List<Utf8>
    createdTime: Int64
    configuration: Map<Utf8, Utf8>
}
```

**Result**: After this phase, `Terminus(Snapshot)` is returned.

---

## Scan State Machine Phases

### Purpose

A `Scan` reads data from a Delta table at a specific snapshot, with optional filters.

### Phase Flow

```
ScanBuilder::into_state_machine()
    ↓
ConsumePhase: CommitReplayPhase (Sequential)
    → Logical Plan: Scan commits + data skipping
    → Consumes: Add/Remove actions
    → Builds tombstone set
    ↓
ConsumePhase: CheckpointReplayPhase (Parallelizable)
    → Logical Plan: Scan checkpoints + data skipping  
    → Consumes: Add/Remove actions
    → Uses frozen tombstone set for deduplication
    ↓
PartialResultPhase: Contains filtered Add actions
    → Can get partial results immediately
    → Continues to transform physical → logical columns
    ↓
Terminus: Final Scan with LogicalPlanNode
```

### Data Skipping

When a predicate is provided to `ScanBuilder`, the kernel automatically integrates data skipping:

**Transformed Plan**:
```
Scan
  ↓
ParseJson (parse add.stats column)
  ↓
FilterByExpression (filter on stats.minValues/maxValues)
  ↓
Select (project needed columns)
```

**This happens automatically** - the engine just executes the plan.

---

## Implementation Workflow

### Minimal Implementation Steps

**For Java/Spark Connector:**

1. **JNI Wrapper Layer**
   ```java
   public class DeltaKernelFFI {
       static {
           System.loadLibrary("delta_kernel_ffi");
       }
       
       public native long snapshotBuilderNew(String tablePath);
       public native long snapshotBuilderIntoStateMachine(long builder);
       public native int stateMachinePhaseType(long phase);
       public native long consumePhaseGetPlan(long phase);
       public native long consumePhaseNext(long phase, long arrowStream);
       // ... etc
   }
   ```

2. **State Machine Driver**
   ```java
   public Snapshot buildSnapshot(String tablePath) {
       long builder = ffi.snapshotBuilderNew(tablePath);
       long phase = ffi.snapshotBuilderIntoStateMachine(builder);
       
       while (true) {
           int phaseType = ffi.stateMachinePhaseType(phase);
           
           if (phaseType == TERMINUS) {
               return ffi.snapshotFromTerminus(phase);
           } else if (phaseType == CONSUME) {
               long plan = ffi.consumePhaseGetPlan(phase);
               Dataset<Row> data = executePlan(plan);
               long arrowStream = convertToArrow(data);
               phase = ffi.consumePhaseNext(phase, arrowStream);
           } else if (phaseType == PARTIAL_RESULT) {
               // Handle partial result if needed
               phase = ffi.partialResultPhaseNext(phase);
           }
       }
   }
   ```

3. **Plan Executor**
   ```java
   public Dataset<Row> executePlan(long planHandle) {
       PlanVisitor visitor = new PlanVisitor();
       long planId = ffi.visitLogicalPlan(planHandle, visitor);
       MyEnginePlan plan = visitor.getBuiltPlan(planId);
       
       switch (plan.type) {
           case SCAN:
               return spark.read().parquet(plan.files);
           case FILTER_BY_EXPRESSION:
               Dataset<Row> child = executePlan(plan.child);
               Column filter = convertPredicate(plan.predicate);
               return child.filter(filter);
           // ... etc
       }
   }
   ```

4. **Arrow Conversion**
   ```java
   public long convertToArrow(Dataset<Row> data) {
       // Use Spark's Arrow support
       List<byte[]> batches = data.toArrowBatch();
       return createArrowStreamFFI(batches);
   }
   ```

---

## Handle Management

### Rules

1. **All handles must be freed**: Every `Handle<T>` returned from FFI must be freed with the appropriate `free_*` function.
   ```c
   HandleSharedLogicalPlan plan = consumePhaseGetPlan(phase);
   // ... use plan ...
   free_logical_plan(plan);
   ```

2. **Handles are NOT thread-safe**: Don't share handles across threads unless documented.

3. **Handles are consumed**: Some functions consume handles (take ownership). Check documentation.

4. **Resource order**: Free resources in reverse order of creation.

### Common Handle Types

| Handle Type | Free Function | Description |
|-------------|---------------|-------------|
| `Handle<SharedLogicalPlan>` | `free_logical_plan()` | Logical plan node |
| `Handle<SharedExpression>` | `free_kernel_expression()` | Expression |
| `Handle<SharedPredicate>` | `free_kernel_predicate()` | Predicate |
| `Handle<SharedSchema>` | `free_schema()` | Schema |
| `Handle<SharedSnapshot>` | (varies) | Snapshot |

---

## Example Code

### Complete C++ Example

See `ffi/examples/visit-plan/` for a complete working example that:
- Creates a test logical plan from Rust
- Uses the visitor pattern to reconstruct it in C++
- Validates all node types
- Properly frees resources

### Key Snippets

**Visitor Setup:**
```c
EnginePlanVisitor visitor = {
    .data = &plan_builder,
    .make_plan_list = make_plan_list_impl,
    .visit_scan = visit_scan_callback,
    .visit_filter_by_expression = visit_filter_by_expression_callback,
    .visit_parse_json = visit_parse_json_callback,
    // ... all other callbacks
};

uintptr_t root_id = visit_logical_plan(plan_handle, &visitor);
```

**State Machine Loop:**
```c
while (true) {
    // Check phase type (implement based on FFI)
    if (is_terminus(phase)) {
        Snapshot* snapshot = get_snapshot_from_terminus(phase);
        break;
    } else if (is_consume_phase(phase)) {
        LogicalPlan* plan = consume_phase_get_plan(phase);
        ArrowArray* data = execute_plan(plan);
        phase = consume_phase_next(phase, data);
    }
}
```

---

## Testing Strategy

### Unit Tests

1. **Visitor Tests**: Verify you can reconstruct plans and expressions
2. **Arrow Conversion**: Verify round-trip conversion works
3. **Handle Management**: Verify no leaks

### Integration Tests

1. **Simple Table**: Read a table with 1 file, no filters
2. **Multi-File Table**: Read table with 10+ files
3. **Partitioned Table**: Read with partition pruning
4. **With Predicates**: Read with filters, verify data skipping
5. **Checkpoint Tables**: Read table with checkpoints
6. **Version Travel**: Read table at specific version

### Golden Tests

Compare your output against Delta Kernel's own tests. The Rust kernel has extensive test tables in `kernel/tests/data/` and `kernel/tests/golden_data/`.

---

## Advanced Topics

### Custom Logical Plan Nodes

If you're using `default-engine` feature, you may encounter custom nodes. These are internal and can be ignored or handled as opaque.

### Parallel Execution

Some phases (like `CheckpointReplayPhase`) are safe to parallelize. Check phase metadata or documentation.

### Memory Management

For large tables:
1. Stream Arrow data in batches (don't load all into memory)
2. Process partial results as they arrive
3. Release handles as soon as possible

### Error Handling

All FFI functions that can error return a `DeltaResult`. Check error codes and extract error messages.

---

## Additional Resources

1. **FFI Examples**: `ffi/examples/`
   - `visit-expression/` - Expression visitor example
   - `visit-plan/` - Logical plan visitor example

2. **FFI Headers**: `target/ffi-headers/delta_kernel_ffi.h`
   - Generated C header with all FFI functions

3. **State Machine Tests**: `kernel/tests/snapshot_state_machine.rs`
   - Shows how to use state machines from Rust

4. **Arrow C Data Interface**: https://arrow.apache.org/docs/format/CDataInterface.html

5. **Delta Protocol**: https://github.com/delta-io/delta/blob/master/PROTOCOL.md

---

## Quick Start Checklist

- [ ] Set up FFI bindings for your language (JNI for Java, ctypes for Python, etc.)
- [ ] Implement logical plan visitor pattern
- [ ] Implement expression visitor pattern  
- [ ] Implement Arrow C Data Interface conversion
- [ ] Implement state machine driver loop
- [ ] Implement plan executor for your engine
- [ ] Test with simple table (1 file, no filters)
- [ ] Test with complex table (multiple files, checkpoints, filters)
- [ ] Test memory management (no leaks)
- [ ] Profile and optimize hot paths

---

## Getting Help

For questions or issues:
1. Check this guide first
2. Look at working examples in `ffi/examples/`
3. Read the generated FFI header in `target/ffi-headers/`
4. Check Delta Kernel Rust source code for implementation details

**Good luck building your connector!** 🚀

