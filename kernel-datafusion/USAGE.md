# DataFusion Executor - Usage Guide

## Basic Usage

### 1. Enable the DataFusion Feature

```toml
[dependencies]
delta-kernel = { version = "0.18", features = ["datafusion"] }
delta-kernel-datafusion = "0.18"
```

### 2. Create an Executor

```rust
use delta_kernel_datafusion::DataFusionExecutor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DataFusion executor with default configuration
    let executor = DataFusionExecutor::new()?;
    
    // Or create with custom SessionState
    let ctx = datafusion::prelude::SessionContext::new();
    let session_state = ctx.state();
    let executor = DataFusionExecutor::with_session_state(session_state);
    
    Ok(())
}
```

### 3. Execute a Plan

```rust
use delta_kernel::plans::DeclarativePlanNode;
use futures::StreamExt;

async fn execute_plan(
    plan: DeclarativePlanNode,
    executor: &DataFusionExecutor,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a results driver
    let driver = executor.results_driver(plan);
    
    // Get the async stream
    let mut stream = driver.into_stream().await?;
    
    // Consume results
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!("Received batch with {} rows", batch.num_rows());
    }
    
    Ok(())
}
```

### 4. Helper Function for One-Shot Execution

```rust
use delta_kernel_datafusion::results_stream;
use datafusion::prelude::SessionContext;

async fn quick_execute(
    plan: DeclarativePlanNode,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let session_state = ctx.state();
    
    // results_stream handles both Results and Drop sinks
    let mut stream = results_stream(&plan, &session_state).await?;
    
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Process batch...
    }
    
    Ok(())
}
```

## Plan Compilation Examples

### Example 1: Simple Scan

```rust
use delta_kernel::plans::{DeclarativePlanNode, ScanNode, FileType, SinkNode};
use delta_kernel::{FileMeta, schema::StructType};

let scan = ScanNode {
    file_type: FileType::Parquet,
    files: vec![
        FileMeta {
            location: url::Url::parse("file:///data/table/part0.parquet")?,
            size: 1000000,
            last_modified: 0,
        }
    ],
    schema: my_schema, // Arc<StructType>
};

let plan = DeclarativePlanNode::Sink {
    child: Box::new(DeclarativePlanNode::Scan(scan)),
    node: SinkNode::results(),
};
```

### Example 2: Scan with Filter

```rust
use delta_kernel::plans::{FilterByExpressionNode};
use delta_kernel::{Expression, Predicate};

let predicate = Predicate::gt(
    Expression::column(["id"]),
    Expression::literal(100i32),
);

let filter = FilterByExpressionNode {
    predicate: Arc::new(predicate),
};

let plan = DeclarativePlanNode::Sink {
    child: Box::new(DeclarativePlanNode::FilterByExpression {
        child: Box::new(DeclarativePlanNode::Scan(scan)),
        node: filter,
    }),
    node: SinkNode::results(),
};
```

### Example 3: Scan with KDF Filter

```rust
use delta_kernel::plans::FilterByKDF;

// Add/Remove deduplication (stateful, single-threaded)
let kdf = FilterByKDF::add_remove_dedup();

let plan = DeclarativePlanNode::Sink {
    child: Box::new(DeclarativePlanNode::FilterByKDF {
        child: Box::new(DeclarativePlanNode::Scan(scan)),
        node: kdf,
    }),
    node: SinkNode::results(),
};
```

### Example 4: Checkpoint Deduplication (Parallelizable)

```rust
// Checkpoint deduplication (stateless, parallelizable)
let kdf = FilterByKDF::checkpoint_dedup();

let plan = DeclarativePlanNode::Sink {
    child: Box::new(DeclarativePlanNode::FilterByKDF {
        child: Box::new(DeclarativePlanNode::Scan(scan)),
        node: kdf,
    }),
    node: SinkNode::results(),
};

// This KDF is marked as parallel-safe and can leverage
// DataFusion's distributed execution
```

### Example 5: Drop Sink (Side Effects Only)

```rust
use delta_kernel::plans::ConsumeByKDF;

// Consumer KDF that builds log segment as side effect
let consumer = ConsumeByKDF::log_segment_builder();

let plan = DeclarativePlanNode::Sink {
    child: Box::new(DeclarativePlanNode::ConsumeByKDF {
        child: Box::new(DeclarativePlanNode::Scan(scan)),
        node: consumer,
    }),
    node: SinkNode::drop(), // Drop sink - no results returned
};

// The stream will be drained automatically, side effects applied
let mut stream = results_stream(&plan, &session_state).await?;
while let Some(_) = stream.next().await {}
```

## Advanced Usage

### Custom Session Configuration

```rust
use datafusion::execution::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;

let config = SessionConfig::new()
    .with_batch_size(8192)
    .with_target_partitions(4);

let runtime_env = Arc::new(RuntimeEnv::default());
let ctx = SessionContext::new_with_config_rt(config, runtime_env);
let executor = DataFusionExecutor::with_session_state(ctx.state());
```

### Direct Plan Compilation

```rust
use delta_kernel_datafusion::compile::compile_plan;

let session_state = ctx.state();
let execution_plan = compile_plan(&plan, &session_state)?;

// Now you can use DataFusion's API directly
let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
let stream = execution_plan.execute(0, task_ctx)?;
```

## Error Handling

```rust
use delta_kernel_datafusion::error::DfError;

match executor.results_driver(plan).into_stream().await {
    Ok(stream) => {
        // Process stream...
    }
    Err(DfError::PlanCompilation(msg)) => {
        eprintln!("Plan compilation failed: {}", msg);
    }
    Err(DfError::ExpressionLowering(msg)) => {
        eprintln!("Expression lowering failed: {}", msg);
    }
    Err(DfError::Unsupported(msg)) => {
        eprintln!("Feature not yet supported: {}", msg);
    }
    Err(e) => {
        eprintln!("Execution error: {}", e);
    }
}
```

## Ordering Guarantees

The DataFusion executor preserves batch ordering for KDFs:

```rust
// Files MUST be in descending version order for KDFs
let files = vec![
    FileMeta { location: url::Url::parse("file:///v5.parquet")?, ... },
    FileMeta { location: url::Url::parse("file:///v4.parquet")?, ... },
    FileMeta { location: url::Url::parse("file:///v3.parquet")?, ... },
];

let scan = ScanNode {
    file_type: FileType::Parquet,
    files, // Order preserved!
    schema,
};

// The executor will:
// 1. Place all files in a single FileGroup
// 2. Use single partition for stateful KDFs
// 3. Validate version ordering at runtime (if version column present)
```

## Performance Tips

### 1. Use Parallel-Safe KDFs When Possible

```rust
// Parallel-safe (CheckpointDedupState)
let kdf = FilterByKDF::checkpoint_dedup();
// Can leverage multiple partitions

// vs.

// Stateful (AddRemoveDedupState)  
let kdf = FilterByKDF::add_remove_dedup();
// Forces single partition
```

### 2. Batch Size Configuration

```rust
let config = SessionConfig::new()
    .with_batch_size(16384); // Larger batches = better throughput

let ctx = SessionContext::new_with_config_rt(config, runtime_env);
```

### 3. Pre-filter Before KDFs

```rust
// Apply cheap filters first to reduce data volume before expensive KDF
let plan = DeclarativePlanNode::FilterByKDF {
    child: Box::new(DeclarativePlanNode::FilterByExpression {
        child: Box::new(DeclarativePlanNode::Scan(scan)),
        node: FilterByExpressionNode { predicate: cheap_filter },
    }),
    node: expensive_kdf,
};
```

## Testing

```rust
#[tokio::test]
async fn test_datafusion_executor() {
    let executor = DataFusionExecutor::new().unwrap();
    let plan = /* your plan */;
    
    let driver = executor.results_driver(plan);
    let mut stream = driver.into_stream().await.unwrap();
    
    let mut row_count = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        row_count += batch.num_rows();
    }
    
    assert_eq!(row_count, expected_count);
}
```

## Migration from Sync Executor

```rust
// Before (sync executor)
use delta_kernel::plans::executor::DeclarativePlanExecutor;

let executor = DeclarativePlanExecutor::new(&engine);
let driver = executor.results_driver(plan);

for batch in driver {
    let batch = batch?;
    // Process batch
}

// After (DataFusion executor)
use delta_kernel_datafusion::DataFusionExecutor;
use futures::StreamExt;

let executor = DataFusionExecutor::new()?;
let driver = executor.results_driver(plan);
let mut stream = driver.into_stream().await?;

while let Some(batch) = stream.next().await {
    let batch = batch?;
    // Process batch
}
```

## Limitations

### Not Yet Implemented
- **FirstNonNull**: Requires `first_value` UDAF implementation
- **ParseJson**: Requires JSON parsing UDF
- **SchemaQuery**: Requires ParquetHandler integration

### Workarounds
For these nodes, you'll receive an `Unsupported` error. Options:
1. Use the sync executor for these specific plans
2. Implement the missing nodes yourself (see contribution guide)
3. Pre-process data to avoid needing these nodes

## See Also
- [DataFusion Integration Summary](./DATAFUSION_INTEGRATION_SUMMARY.md)
- [Kernel Plans Documentation](../kernel/src/plans/README.md)
- [DataFusion Documentation](https://docs.rs/datafusion/)


