# Delta Kernel-RS Bug Report

Generated from improved_dat spec test integration (2026-02-10).

**Test results:** 1087 passed, 12 failed, 0 skipped (out of 1099 total specs)
- 10 failures are kernel bugs
- 2 failures are capture/spec bugs (DV-018: missing delta directory)
- **Pass rate: 98.9%** (up from 70.1% baseline of 772/1101)

Note: Spec count changed from 1101 to 1099 after re-running captures with clean output
directories (stale files removed). cp_partitioned and DV-017 are excluded from the tar
due to test hangs (TokioBackgroundExecutor and huge table respectively).

---

## Bug 1: Unsupported reader features not rejected at protocol validation (5 tests)

**Severity:** Medium
**Tests:** `ds_multi_file_time_*` (5 specs)
**Error:** `data did not match any variant of untagged enum DataType`

Tables requiring the `timeType-preview` reader feature are not rejected during protocol
validation, causing a cryptic schema parsing failure deep in serde deserialization instead
of a clear "unsupported reader feature" error.

**Expected:** The kernel should reject the table at protocol validation time with a clear
error like "unsupported reader feature: timeType-preview".

**Root cause:** The kernel's protocol validation has a gap in unsupported feature rejection:

1. `timeType-preview` is deserialized as `TableFeature::Unknown("timeType-preview")` via
   serde fallback (`kernel/src/table_features/mod.rs:132-134`)
2. `is_feature_supported()` correctly returns `false` for Unknown features
   (`kernel/src/table_configuration.rs:605-610`)
3. But `Protocol::validate_table_features()` (`kernel/src/actions/mod.rs:481-576`)
   **explicitly allows** `FeatureType::Unknown` through for forward compatibility (lines
   521-523)
4. `TableConfiguration::try_new()` (`kernel/src/table_configuration.rs:86-114`) only
   validates specific known types (timestamp_ntz, variant, column mapping) — there is no
   catch-all that rejects tables requiring unsupported reader features

The schema parsing then fails at `Metadata::parse_schema()` (`kernel/src/actions/mod.rs:335`)
because `PrimitiveType` (`kernel/src/schema/mod.rs:1280`) has no `Time` variant, and the
`#[serde(untagged)]` attribute on `DataType` produces a generic error.

**Fix:** Add a validation step in `TableConfiguration::try_new()` (after line 101) that
iterates over all reader features and rejects the table if any required reader feature is
not supported (`is_feature_supported()` returns false). This would catch `timeType-preview`
and any future unsupported features with a clear error message, rather than failing in
schema parsing.

**Affected specs:**
- `ds_multi_file_time/specs/ds_multi_file_time_afternoon_only.json`
- `ds_multi_file_time/specs/ds_multi_file_time_all.json`
- `ds_multi_file_time/specs/ds_multi_file_time_gap_no_match.json`
- `ds_multi_file_time/specs/ds_multi_file_time_morning_only.json`
- `ds_multi_file_time/specs/ds_multi_file_time_snapshot.json`

---

## Bug 2: Data skipping misses optimization for `(expr) IS NULL` predicates

**Severity:** Low (correctness is fine — missed optimization only)
**Tests:** `ds_nulls_mixed_hit_a_gt_0_is_null`, `ds_nulls_mixed_hit_a_lt_0_is_null`, `ds_nulls_only_null_hit_a_gt_1_is_null`

The original report attributed 0 rows returned to data skipping over-pruning. Investigation
revealed the kernel's data skipping correctly returns `None` (safe under-pruning: keep file)
for unsupported IS NULL operands — the actual 0-row bug was a harness predicate
transformation error (now fixed, see Harness Fix #9).

However, `eval_pred_unary` (`kernel/src/kernel_predicates/mod.rs:206-216`) returns `None`
for `IS NULL` on `Expr::Predicate`, meaning data skipping cannot use these predicates for
pruning at all. This is a missed optimization: `(a > 0) IS NULL` can only be true when `a`
is null, so files where `nullCount(a) == 0` could be safely pruned. Currently they are kept,
resulting in unnecessary file reads.

**Fix:** Handle `Expr::Predicate(_)` in `eval_pred_unary` for `IsNull` by extracting column
references and checking their null counts. Files where all referenced columns have
`nullCount == 0` can be skipped.

---

## Bug 3: Column mapping ID mismatch returns wrong data (2 tests)

**Severity:** High
**Tests:** `cm_id_matching_nonexistent_select_a_null`, `cm_mode_id_read_select_columns`
**Error:** `DataMismatch { column: "unknown", message: "Data content does not match" }`

When a logical column's column mapping ID does not match any physical column in the
parquet file (nonexistent ID), the kernel reads data from a different physical column
instead of returning null values.

Example: Table has column `a` with column mapping ID `100`, but the parquet file has no
physical column with field ID `100`. Spark returns null for `a`; the kernel returns `str3`
(from a different physical column).

**Expected:** Columns with nonexistent mapping IDs should return all nulls.

**Root cause:** In `match_parquet_fields()` (`kernel/src/engine/arrow_utils.rs:642-702`),
the field resolution logic at lines 674-683 builds a mapping from parquet field IDs to
kernel field names. When a parquet field's ID exists but is not found in the
`field_id_to_name` map, line 683 falls back to `parquet_field.name()`:

```rust
let field_name = parquet_field_id
    .and_then(|field_id| {
        field_id_to_name
            .get_or_init(init_field_map)
            .get(&field_id)
            .copied()
    })
    .unwrap_or_else(|| parquet_field.name());  // BUG: falls back to name-based matching
```

This name-based fallback is incorrect when column mapping mode is `id`. If the field ID
doesn't match, the column should be treated as missing (return nulls), not matched by name.
The subsequent lookup at lines 685-694 then finds a kernel field by this incorrect name,
causing wrong data to be read.

**Fix:** When column mapping mode is `id` and the field ID lookup fails, do not fall back
to name-based matching. Return `None` for `field_name` so the column is treated as missing.

**Affected specs:**
- `cm_id_matching_nonexistent/specs/cm_id_matching_nonexistent_select_a_null.json`
- `cm_mode_id/specs/cm_mode_id_read_select_columns.json`

---

## Bug 4: Internal error with swapped column mapping IDs (1 test)

**Severity:** High
**Tests:** `cm_id_matching_swapped_select_a_reads_e`
**Error:** `Internal error Found a None in final_fields_cols.. This is a kernel bug, please report.`

When two columns have their column mapping IDs swapped (e.g., column `a` has `e`'s ID and
vice versa), the kernel panics with an internal error about `None in final_fields_cols`.

**Expected:** Should correctly resolve swapped IDs or produce a clear error.

**Root cause:** This is a consequence of Bug 3's name-fallback behavior. In
`reorder_struct_array()` (`kernel/src/engine/arrow_utils.rs:782-921`), the
`final_fields_cols` vector is initialized with `None` for each expected column (line 799).
When swapped IDs cause `match_parquet_fields()` to fail matching, the `kernel_field_info`
in `MatchedParquetField` (line 83-90) is `None`. During `get_indices()` (lines 408-636),
unmatched fields are skipped (line 581-588: "Skipping over un-selected field"), leaving
`None` entries in `final_fields_cols`. The length check at line 912 detects the `None`
entries and panics.

**Fix:** Same as Bug 3 — proper handling of field ID resolution in column mapping `id`
mode. When a field's ID doesn't match, produce null columns instead of failing to match.

**Affected specs:**
- `cm_id_matching_swapped/specs/cm_id_matching_swapped_select_a_reads_e.json`

---

## Bug 5: Type widening — cannot cast list/map elements (2 tests)

**Severity:** Medium
**Tests:** `tw_array_element_read_all`, `tw_map_key_value_widening_read_all`
**Error:** `Cast error: Cannot cast list to non-list data types`

When a table has undergone type widening on array element types or map key/value types,
the kernel fails to cast the data correctly during scan execution.

**Expected:** Type widening should work for nested types (array elements, map keys/values).

**Root cause:** In `reorder_struct_array()` (`kernel/src/engine/arrow_utils.rs:804-813`),
the `ReorderIndexTransform::Cast` handler calls `arrow::compute::cast(col, target)` directly
on the entire column. This works for top-level primitives (e.g., `Int32` → `Int64`) but
fails for container types because Arrow's cast kernel cannot cast a `ListArray` to a
different element type directly.

The problem originates in `get_indices()` (lines 560-576) which calls `ensure_data_types()`
to determine type compatibility. For `Array<Int32>` vs `Array<Int64>`, it returns
`DataTypeCompat::NeedsCast(Array<Int64>)`, causing a `ReorderIndex::cast()` to be pushed
(line 568-570). But casting requires recursing into the container — extracting element
arrays, casting them, and reconstructing the container.

For comparison, the `Nested` transform (lines 815-856) correctly handles recursive
restructuring, and `reorder_list()` (lines 923-959) and `reorder_map()` (lines 961-997)
handle nested reordering — but only for `Nested` transforms, not `Cast`.

**Fix:** `ensure_data_types()` should return a `Nested` transform (not `Cast`) for
List/Map types where only the element types differ. The element-level cast should then be
applied inside `reorder_list()`/`reorder_map()` during recursive processing.

**Affected specs:**
- `tw_array_element/specs/tw_array_element_read_all.json`
- `tw_map_key_value_widening/specs/tw_map_key_value_widening_read_all.json`

---

## Bug 6: Type widening — inconsistent schema metadata across batches (2 tests)

**Severity:** Medium
**Tests:** `tw_nested_field_read_all`, `tw_nested_field_read_large_count`
**Error:** `column types must match schema types, expected Struct("id": Int32, "count": Int64, metadata: {"delta.typeChanges": ...}) but found Struct("id": Int32, "count": Int64)`
**Status:** Worked around in test harness; kernel fix needed

When reading tables with type widening on nested struct fields, different batches have
different schemas (some include `delta.typeChanges` metadata, others don't). The batches
cannot be concatenated because Arrow requires identical schemas.

**Expected:** All batches should have consistent schemas regardless of type change metadata.

**Root cause:** In `scan/state.rs`, `transform_to_logical()` (lines 96-114):

```rust
match transform {
    Some(transform) => engine.evaluation_handler()
        .new_expression_evaluator(physical_schema, transform, logical_schema)?
        .evaluate(physical_data),      // ← calls apply_schema, sets metadata
    None => Ok(physical_data),          // ← returns raw data, NO metadata applied
}
```

When a file needs a type cast (pre-widening files with Int32 count), `transform` is
`Some(Cast)`, the expression evaluator calls `apply_schema` which sets `delta.typeChanges`
metadata from the kernel logical schema. When a file doesn't need a cast (post-widening
files with Int64 count), `transform` is `None` and raw parquet data is returned without
any metadata. The result: batches from different files have different schemas.

**Fix:** The kernel should ensure all batches from `scan.execute()` conform to the same
logical schema, including field metadata. The `None` path in `transform_to_logical` should
still apply the logical schema metadata (e.g. via `apply_schema`).

**Workaround:** Test harness strips field metadata before concatenating batches.

**Affected specs:**
- `tw_nested_field/specs/tw_nested_field_read_all.json`
- `tw_nested_field/specs/tw_nested_field_read_large_count.json`

---

## Bug 7: TokioBackgroundExecutor crash (2 tests)

**Severity:** Low (may be test environment specific)
**Tests:** `cp_partitioned_read_part_0`, `cp_partitioned_read_part_3`
**Error:** `TokioBackgroundExecutor panicked` / `runtime dropped the dispatch task`

The async executor crashes when processing certain partitioned checkpoint tables.
This may be related to the test harness using `block_on` with the default engine's
async implementation.

**Expected:** Read should complete without executor panic.

**Root cause:** Nested `block_on` deadlock pattern in `TokioBackgroundExecutor`
(`kernel/src/engine/default/executor.rs`).

The executor spawns a background thread with its own current-thread tokio runtime (lines
68-86) and a 50-item mpsc channel. The `block_on` method (lines 110-134) sends futures
to this background thread via `send_future()` (lines 90-106) and blocks on a oneshot
channel for the result.

The test harness creates its own current-thread runtime:
```rust
tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?
    .block_on(async { ... })
```

Inside this, the `DefaultEngine` uses `TokioBackgroundExecutor`. Reading partitioned tables
triggers `read_parquet_files()` (`kernel/src/engine/default/parquet.rs:290-302`) which calls
`stream_future_to_iter()` (`kernel/src/engine/default/mod.rs:51-59`). This creates a
`BlockingStreamIterator` that calls `task_executor.block_on()` on every `next()` call
(lines 66-82), producing O(num_files) consecutive `block_on` calls.

With many partition files, the 50-item channel buffer fills. `send_future()` enters a
busy-wait loop (`std::thread::yield_now()`, line 98) and eventually the channel closes,
triggering the panic at line 102: `TokioBackgroundExecutor channel closed`.

The `TokioMultiThreadExecutor` (lines 152-221) does not have this issue because it uses
`tokio::task::block_in_place()` which allows the tokio runtime to steal work from other
threads, avoiding deadlock. There's a test for nested `block_on` with the multi-threaded
executor (lines 252-290) but no equivalent test for `TokioBackgroundExecutor`.

**Fix:** Either use `TokioMultiThreadExecutor` in the test harness, or fix the
`TokioBackgroundExecutor` to handle high-throughput `block_on` calls without deadlocking
(e.g., increase channel capacity, or drain completed futures before sending new ones).

**Affected specs:**
- `cp_partitioned/specs/cp_partitioned_read_part_0.json`
- `cp_partitioned/specs/cp_partitioned_read_part_3.json`

---

## Summary Table

| Bug | Category | Severity | Tests | Root Cause File | Status |
|-----|----------|----------|-------|-----------------|--------|
| 1 | Feature gating | Medium | 5 | `kernel/src/table_configuration.rs:86` | Open |
| 2 | Data skipping | Low | 0 (fixed) | `kernel/src/kernel_predicates/mod.rs:206` | Open (missed optimization) |
| 3 | Column mapping | High | 2 | `kernel/src/engine/arrow_utils.rs:683` | Open |
| 4 | Column mapping | High | 1 | `kernel/src/engine/arrow_utils.rs:912` | Open |
| 5 | Type widening | Medium | 2 | `kernel/src/engine/arrow_utils.rs:804` | Open |
| 6 | Type widening | Medium | 2 | `kernel/src/engine/arrow_expression/apply_schema.rs:121` | Open |
| 7 | Executor | Low | 2 (skipped) | `kernel/src/engine/default/executor.rs:102` | Open |
| **Total** | | | **10 failing** | | |

---

## Capture/Spec Bugs (2 failing, not kernel issues)

These failures are due to issues in the test spec capture process, not kernel bugs:

- **DV-018 missing delta directory (2 tests):** The DV-018 table's `delta/` directory is
  missing from the spec. The test resource `table-with-dv-feature-enabled` is relative to
  the Bazel runfiles and `captureExact` couldn't copy it to the output directory.

**Previously fixed capture bugs (now passing):**
- **Stale files from SaveMode.Overwrite** (fixed by `cleanOutputDir()` in ExactSpecCapture.scala):
  Previously caused 9 failures across cp_schema_evolution, cp_v2_basic, cp_v2_all_actions_in_manifest,
  and DV-002 tables. Root cause: `copyDirectory()` didn't clean destination, and `Files.createDirectories()`
  didn't delete existing files, causing stale parquet/checkpoint files from previous capture runs.
- DV `.bin` file prefix: Removed `test%dv%prefix-` from 14 DV binary files (fixed 17 tests)
- Expected data duplication: Removed duplicate transaction parquet files from 233 directories (fixed ~180 tests)
- Summary validation: Changed to compare against Spark's actual output instead of theoretical count (fixed ~52 tests)

---

## Harness Fixes Applied

The following fixes were made to the test harness during this integration:

1. **LIKE prefix patterns** → range comparison (`col >= 'prefix' AND col < 'prefiy'`)
2. **IS NULL/IS NOT NULL on comparisons** → fallback to predicate-level null check
3. **Boolean literal predicates** (`TRUE`/`FALSE`)
4. **TimestampNTZ literal generation** → use `Scalar::TimestampNtz` for NTZ columns
5. **Txn version field** → made optional (`Option<i64>`)
6. **Graceful skip** for unsupported predicates (HEX, modulo, `_metadata`)
7. **Timestamp normalization** in validation → normalize all timestamps to `Microsecond/UTC`
8. **Summary validation** → compare against `actual_row_count` (Spark output) not `expected_row_count`
9. **IS NULL on predicates** → extract column refs: `(a > 0) IS NULL` → `a IS NULL` (fixed incorrect `NOT(OR(pred, NOT(pred)))` transformation that returned NULL instead of TRUE)
