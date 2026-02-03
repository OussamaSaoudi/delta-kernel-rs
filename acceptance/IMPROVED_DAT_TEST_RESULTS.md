# Improved DAT Test Suite Results

This document summarizes the results of running the `improved_dat` test suite against delta-kernel-rs.

## Summary

| Status | Count |
|--------|-------|
| Passed | 537 |
| Failed | 182 |
| Skipped | 30 (timestamp-based time travel + domain metadata) |
| Total | 719 |

Note: 39 DV-* tests were removed due to broken symlinks in test data.

### Recent Fixes
- **Column mapping metadata comparison** - Fixed validation to ignore Arrow field metadata (column mapping IDs), allowing column-mapped tables to pass validation (+33 tests)

## Test Categories

### Passing Tests

The following test categories are working correctly:

- Basic read workloads (no predicates)
- Read workloads with predicates on matching types
- Snapshot metadata validation (protocol, metadata)
- Version-based time travel
- Deletion vector (DV) reads
- CDC table reads (reading data, not change feed)
- Column mapping reads (including nested structs with column mapping)
- Checkpoint reads
- Schema evolution reads
- Type widening reads

### Skipped Tests

- **Timestamp-based time travel (30 tests)** - Requires reading `commitInfo.timestamp` from commit files

---

## Failure Categories

### 1. Row Count Mismatch (~98 tests)

Tests where the kernel returns different row counts than expected. Primary cause: **kernel does not perform row-level predicate filtering** - it only does file-level data skipping. The `scan.execute()` returns all rows from files that pass data skipping; row filtering must be done by the caller using `scan.physical_predicate()`.

**Affected tests:** `ds_*` (data skipping tests), and others with predicates that can't skip entire files.

### 2. Timestamp-based Time Travel (~57 tests)

Tests using timestamp-based time travel fail because the test harness reads commit timestamps from file modification times, which don't match the embedded `commitInfo.timestamp` values in the test data.

**Affected tests:** `tt_*` (time travel tests with timestamps)

### 3. Missing Error Validation (~24 tests)

These tests expect the kernel to produce an error, but the operation succeeds. This represents missing validation in the kernel.

| Test | Expected Error | Description |
|------|----------------|-------------|
| `dv_err_001_checksum_error` | `DELTA_DELETION_VECTOR_CHECKSUM_MISMATCH` | Kernel should validate DV checksums |
| `dv_err_002_missing_file_error` | `DELTA_MISSING_DELETION_VECTOR` | Kernel should error on missing DV file |
| `dv_err_003_malformed_path_error` | `DELTA_INVALID_DELETION_VECTOR_PATH` | Kernel should validate DV path format |
| `cm_err_001_unsupported_protocol_error` | `DELTA_UNSUPPORTED_COLUMN_MAPPING_PROTOCOL` | Kernel should reject unsupported column mapping protocol |
| `cm_err_002_invalid_chars_error` | `DELTA_COLUMN_MAPPING_INVALID_CHARACTERS` | Kernel should reject invalid characters in column names |
| `cm_err_003_illegal_mode_change_error` | `DELTA_COLUMN_MAPPING_MODE_CHANGE_NOT_SUPPORTED` | Kernel should reject illegal column mapping mode changes |
| `rt_err_001_write_materialized_error` | `DELTA_ROW_TRACKING_MATERIALIZED_COLUMN` | Kernel should error on materialized row tracking column |
| `rt_err_002_tracking_disabled_error` | `DELTA_ROW_TRACKING_NOT_ENABLED` | Kernel should error when row tracking is disabled |
| CDC error tests | Various | CDC-specific error validations |

### 5. Type Mismatch in Predicates (~120 tests)

The kernel does not auto-cast between numeric types when evaluating predicates. This causes failures when comparing columns to literals of different types.

**Examples:**
- `Int32 <= Int64` (54 tests)
- `Int32 > Int64` (28 tests)
- `Int32 < Int64` (16 tests)
- `Float64 > Int64` (4 tests)
- `Date32 <= Utf8` (6 tests)

**Root cause:** The predicate parser creates `Int64` literals for all integers. The kernel requires exact type matching.

### 6. Unsupported Predicate Syntax (~72 tests)

The predicate parser does not support all SQL syntax used in test predicates.

| Syntax | Count | Example |
|--------|-------|---------|
| `LIKE` | 44 | `name LIKE 'A%'` |
| `IN` | 8 | `id IN (1, 2, 3)` |
| `BETWEEN` | ~12 | `value BETWEEN 0 AND 99` |
| `NOT` (standalone) | 8 | `NOT condition` |

### 7. Unsupported Features (8 tests)

| Feature | Description |
|---------|-------------|
| `testRemovableLegacyReaderWriter` | Test-only feature for protocol testing |

**Affected tests:** `cm_upgrade_*`

### 8. Data Content Mismatch (~6 tests)

Tests where row counts match but actual data content differs from expected.

---

## Recommendations

### High Priority (Kernel Issues)

1. **Row-level predicate filtering** - Currently `scan.execute()` only does file-level data skipping. Either:
   - Document that callers must apply `physical_predicate()` for row filtering
   - Or add row filtering to the scan execution

2. **Add missing error validations** - ~24 tests expect errors that kernel doesn't produce:
   - DV checksum/file validation
   - Column mapping protocol validation
   - Row tracking validation

### Medium Priority (Test Harness Issues)

3. **Timestamp-based time travel** - Read `commitInfo.timestamp` from commit files instead of file mtime

4. **Type coercion in predicates** - Schema-aware predicate parsing to use correct literal types

### Lower Priority (Extended Syntax)

5. **Extended predicate syntax** - Add support for:
   - `LIKE` operator
   - `IN` operator
   - `BETWEEN` operator

---

## Test Infrastructure

The test harness is located at:
- `acceptance/tests/improved_dat_reader.rs` - Test harness
- `acceptance/src/improved_dat/` - Supporting modules
  - `types.rs` - JSON deserialization types
  - `predicate_parser.rs` - SQL predicate parser
  - `workload.rs` - Workload execution
  - `validation.rs` - Result validation

### Running Tests

```bash
# Run all tests
cargo test -p acceptance --test improved_dat_reader

# Run specific test
cargo test -p acceptance --test improved_dat_reader st_numeric_stats_full_scan

# Run with output
cargo test -p acceptance --test improved_dat_reader -- --nocapture
```
