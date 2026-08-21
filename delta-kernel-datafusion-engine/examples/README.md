# Interactive DataFusion CLI for Delta Kernel

This is the standard DataFusion SQL REPL extended with Delta Kernel. It remains interactive until
you enter `\q`, and supports direct Delta paths, table functions, `EXPLAIN`, and
`EXPLAIN ANALYZE`.

Start it from the repository root:

```bash
cargo run --locked -p delta-kernel-datafusion-engine --example execute_delta_plan
```

## Query a Delta table directly

No registration is required:

```sql
SELECT *
FROM 'kernel/tests/data/table-without-dv-small'
WHERE value >= 5;
```

The equivalent explicit table functions are `delta_table()` and `delta_scan()`:

```sql
SELECT * FROM delta_table('/path/to/delta-table');
SELECT * FROM delta_scan('/path/to/delta-table');
```

## Query Kernel scan metadata

`delta_metadata()` executes Kernel's metadata-only declarative plan. It returns one row per live
Delta file without scanning the table's data rows.

```sql
SELECT path, size
FROM delta_metadata('kernel/tests/data/basic_partitioned')
ORDER BY path;
```

It exposes the fields of Kernel's live `add` action, including `path`, `size`,
`partitionValues`, `deletionVector`, stats, and optional parsed fields. The CLI requests typed
partition values, so partitioned tables also expose the case-sensitive struct
`"partitionValues_parsed"`:

```sql
SELECT path, "partitionValues_parsed".letter
FROM delta_metadata('kernel/tests/data/basic_partitioned');
```

The declarative IR carries commit versions through `ScanJson.file_constants`; DataFusion
broadcasts each file's constant from `PartitionedFile.partition_values`. Partition values are
Delta `add.partitionValues` metadata, so they remain in the `add` output rather than being copied
into the scan operator's internal file-constant list.

## Inspect and execute plans

Both data and metadata relations work with the normal SQL commands:

```sql
EXPLAIN
SELECT path, size
FROM delta_metadata('kernel/tests/data/basic_partitioned');

EXPLAIN ANALYZE
SELECT path, size
FROM delta_metadata('kernel/tests/data/basic_partitioned');

EXPLAIN ANALYZE
SELECT *
FROM 'kernel/tests/data/table-without-dv-small'
WHERE value >= 5;
```

`EXPLAIN` uses a compact physical tree. `EXPLAIN ANALYZE` executes the relation and adds output
rows and CPU time to each operator. Commit reconciliation uses an `AggregateExec` with ordered
`first_value`, not a window sort. A checkpoint at the snapshot version eliminates the JSON,
aggregate, and anti-join arms entirely. Shared DAG nodes are materialized once behind
`SharedNodeExec`, so a checkpoint plus JSON tail does not read the commit log twice.

## Optional named tables

Named catalog tables are still supported when useful for repeated queries:

```sql
CREATE EXTERNAL TABLE numbers
STORED AS DELTA
LOCATION 'kernel/tests/data/table-without-dv-small';

SELECT * FROM numbers;
```

SQL filters currently run in DataFusion above the Kernel scan. SQL-to-Kernel predicate pushdown is
not part of this demo.

Run the complete demo from inside the REPL with:

```text
\i delta-kernel-datafusion-engine/examples/demo.sql
```

Standard CLI commands remain available, including `\?`, `\d`, `\pset`, `\i`, and `\q`.
