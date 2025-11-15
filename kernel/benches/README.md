# Delta Kernel Benchmarking

This directory contains benchmarking infrastructure for Delta Kernel using Criterion and JSON workload specifications.

## Overview

The benchmarking system is designed to be compatible with the Java Delta Kernel benchmarking specifications, allowing for easy comparison of performance between implementations. It uses:

- **JSON workload specifications**: Define benchmarks declaratively
- **Criterion**: Industry-standard Rust benchmarking framework
- **Modular runners**: Separate implementations for different workload types

## Running Benchmarks

To run all workload benchmarks:

```bash
cargo bench --bench workload_bench --features "arrow,default-engine-rustls,internal-api"
```

To compare against a baseline:

```bash
# Save current main as baseline
git checkout main
cargo bench --bench workload_bench --features "arrow,default-engine-rustls,internal-api" -- --save-baseline main

# Compare your changes
git checkout your-branch
cargo bench --bench workload_bench --features "arrow,default-engine-rustls,internal-api" -- --baseline main
```

## Workload Specification Structure

Workloads are organized in the following directory structure:

```
workload_specs/
├── table_name/
│   ├── table_info.json          # Table metadata
│   ├── delta/                    # Delta table data
│   │   └── _delta_log/
│   │       ├── 00000000000000000000.json
│   │       └── ...
│   └── specs/                    # Test case specifications
│       ├── case_name_1/
│       │   └── spec.json
│       └── case_name_2/
│           └── spec.json
```

### table_info.json

Defines metadata about the table:

```json
{
  "name": "basic_append",
  "description": "A basic table with two append writes.",
  "engine_info": "Apache-Spark/3.5.1 Delta-Lake/3.1.0"
}
```

For remote tables (S3/cloud), include `table_path`:

```json
{
  "name": "s3_table",
  "description": "A table on S3",
  "table_path": "s3://bucket/path/to/table/"
}
```

### spec.json

Defines individual test cases. Different types of workloads are supported:

#### Read Workloads

```json
{
  "type": "read"
}
```

Or with a specific version:

```json
{
  "type": "read",
  "version": 0
}
```

Read workloads automatically generate variants:
- `read_metadata`: Measure time to read scan metadata (file listing)

#### Snapshot Construction Workloads

```json
{
  "type": "snapshot_construction"
}
```

Or with a specific version:

```json
{
  "type": "snapshot_construction",
  "version": 0
}
```

## Supported Workload Types

Currently supported:
- ✅ **Read (Metadata)**: Benchmark scan metadata reading
- ✅ **Snapshot Construction**: Benchmark snapshot creation

Future support:
- ⏳ **Read (Data)**: Benchmark actual data file reading
- ⏳ **Write**: Benchmark table writes

## Adding New Workloads

1. Create a new directory under `workload_specs/`
2. Add `table_info.json` with table metadata
3. Add Delta table data in `delta/_delta_log/`
4. Create spec files in `specs/<case_name>/spec.json`
5. Run benchmarks - they will be automatically discovered

## Architecture

The benchmarking system consists of three main components:

### Models (`src/benchmarks/models.rs`)

Defines the data structures for workload specifications:
- `TableInfo`: Table metadata
- `WorkloadSpec`: Trait for all workload types
- `ReadSpec`, `SnapshotConstructionSpec`: Concrete workload types
- `WorkloadSpecVariant`: Enum for polymorphic deserialization

### Runners (`src/benchmarks/runners.rs`)

Implements the execution logic for each workload type:
- `WorkloadRunner`: Trait defining setup/execute/cleanup lifecycle
- `ReadMetadataRunner`: Executes read_metadata workloads
- `SnapshotConstructionRunner`: Executes snapshot construction workloads

### Utils (`src/benchmarks/utils.rs`)

Provides utilities for loading workload specifications from disk:
- `load_all_workloads()`: Recursively discovers and loads all specs
- Error handling and validation

## Examples

See `workload_specs/basic_append/` for a complete example with:
- Two versions of a Delta table
- Read workloads at different versions
- Snapshot construction benchmarks

## Compatibility with Java Kernel

The JSON specification format is designed to be compatible with the Java Delta Kernel benchmarking system. You can copy workload specs from the Java kernel's `kernel-benchmarks/src/test/resources/workload_specs/` directory to run equivalent benchmarks in Rust.

Note: Currently only local tables are supported. Remote table support (S3/cloud) is planned for future releases.

