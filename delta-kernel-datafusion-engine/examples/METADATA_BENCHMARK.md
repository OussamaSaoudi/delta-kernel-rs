# Metadata execution benchmark

This benchmark compares three ways of producing the complete live-file set from the same Delta
snapshot. It never reads table data files.

| Method | Execution path |
|---|---|
| `kernel` | `Scan::scan_metadata`, fully consumed |
| `kernel-parallel` | `Scan::parallel_scan_metadata`, then checkpoint leaves distributed across worker threads |
| `datafusion` | `scan_metadata_state_machine`, compiled and executed by `DataFusionExecutor` |

Every method consumes the metadata output, counts selected live files, and validates the result
against the fixture manifest. The DataFusion path streams full output batches instead of applying
`COUNT(*)`, preventing projection pushdown from dropping metadata columns. Snapshot construction is
performed once before measurement and reported separately as `snapshot_ms`. Each method runs in a
separate process in the matrix script, making `VmHWM` peak RSS values comparable. The script uses
strict `kernel -> kernel-parallel -> datafusion` cycles so transient system load is distributed
across methods instead of placing every run of one method in the same time window.

The named data-skipping cases use the uniformly distributed `upstream_header_time` and
`upstream_q_time` statistics:

| Predicate | Expression | Expected file retention |
|---|---|---:|
| `none` | no predicate | 100% |
| `keep-all` | `upstream_header_time >= 0.0` | 100% plus predicate overhead |
| `keep-some` | `upstream_header_time > 0.99` | about 10% |
| `keep-few` | `upstream_header_time > 0.999` | about 1% |
| `conjunctive` | both header and queue time greater than `0.99` | about 1% |
| `skip-all` | `upstream_header_time > 2.0` | 0% |

## Tables

| Name | Shape | Expected live files |
|---|---|---:|
| `large_log_no_checkpoint` | JSON commits, no checkpoint | 2,000,000 |
| `large_log_checkpoint` | Multipart checkpoint | 5,000,000 |
| `large_log_dvs` | Checkpoint plus deletion-vector keys | 5,000,000 |

The no-checkpoint table is an important control: Kernel's parallel API only distributes checkpoint
leaf files, so this case can expose parallelism available from executing the declarative plan as a
general DataFusion dataflow. The two checkpoint tables show checkpoint-level parallelism, while
the DV table adds the cost of composite reconciliation keys.

## Authentication

Use the normal AWS credential chain: environment credentials, `AWS_PROFILE`, web identity, or the
instance role. The bucket is in `us-west-2`.

```bash
export AWS_PROFILE=your-profile
export AWS_REGION=us-west-2
```

## Build and smoke-test

Always benchmark an optimized build:

```bash
cargo build --locked --release \
  -p delta-kernel-datafusion-engine \
  --example metadata_benchmark

target/release/examples/metadata_benchmark --list-tables
```

Run one method first:

```bash
target/release/examples/metadata_benchmark \
  --table large_log_no_checkpoint \
  --method datafusion \
  --runs 1 \
  --header
```

## Full matrix

The matrix defaults to three cold ABC cycles. Every measurement gets a fresh process.

```bash
CYCLES=3 RUNS=1 WARMUPS=0 \
  bash delta-kernel-datafusion-engine/examples/run_metadata_benchmarks.sh
```

The default matrix uses `none`, `keep-all`, `keep-some`, `conjunctive`, and `skip-all`. Override
the set when needed:

```bash
TABLES="large_log_checkpoint" PREDICATES="none keep-few skip-all" CYCLES=5 \
  bash delta-kernel-datafusion-engine/examples/run_metadata_benchmarks.sh
```

`WORKERS` controls only `kernel-parallel`. Omit it for the default available-CPU count, which
matches DataFusion's default target-partition setting.

For a more stable warm-connection comparison:

```bash
RUNS=3 WARMUPS=1 \
  OUTPUT=metadata-benchmark-warm.csv \
  bash delta-kernel-datafusion-engine/examples/run_metadata_benchmarks.sh
```

`CYCLES` controls the number of interleaved ABC cycles. `RUNS` controls repeated scans inside each
process and normally remains `1` for unbiased cold-process comparisons.

The CSV columns are:

- `snapshot_ms`: shared Kernel snapshot setup, excluded from `elapsed_ms`;
- `parallelism`: `1` for Kernel, `WORKERS` for Kernel parallel, and DataFusion's default
  target-partition count (the host's available CPUs) for DataFusion;
- `cycle`: the outer ABC cycle number;
- `elapsed_ms`: complete live-file production and counting;
- `files_per_second`: validated live files divided by execution time;
- `peak_rss_mib`: Linux process high-water RSS, including snapshot and executor setup.

## Local correctness check

The same binary accepts any table path. These commands require no S3 credentials:

```bash
for method in kernel kernel-parallel datafusion; do
  target/debug/examples/metadata_benchmark \
    --path "file://${PWD}/kernel/tests/data/v2-parquet-sidecars-struct-stats-only" \
    --name local-v2 \
    --expected 5 \
    --method "$method" \
    --workers 4
done
```

## Reading results

Compare `elapsed_ms` and `peak_rss_mib` within each table. Do not compare raw time across tables.
Report median and range when using multiple runs. A useful summary includes speedup relative to
`kernel` and memory amplification relative to `kernel`:

```text
speedup = kernel elapsed_ms / candidate elapsed_ms
memory ratio = candidate peak_rss_mib / kernel peak_rss_mib
```

Keep the AWS region, instance type, worker count, build commit, and warmup policy fixed in any
published comparison.
