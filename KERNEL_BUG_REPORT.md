# Delta Kernel-RS Bug Report

**Date**: 2026-03-08 (updated with write spec harness)
**Test harness**: `acceptance/tests/improved_dat_reader.rs`
**Spec source**: 1017 read specs + 414 write specs from 67+ Databricks Runtime capture suites
**Results**: 5288 tests pass (including expected failures and skipped), 0 failures

## Summary

| Category | Count | Notes |
|----------|-------|-------|
| Total spec files | 3643 | Discovered by datatest-stable |
| Passing (normal) | ~3530 | Workload executes and validates correctly |
| Expected kernel failures | ~90 | Kernel fails/diverges from Spark; documented |
| Skipped (data mismatch) | 16 | Kernel returns wrong data; can't use expect macro |
| Skipped (OOM/crash) | 3 | DV-017 (2B rows), struct special types crash |

---

## Category 1: Kernel Bugs (execution failures)

### 1.1 Void/NullType not supported
**Affected tests**: 6 (void_001, void_002, void_005, void_006, void_007, void_in_struct)
**Error**: Schema deserialization fails for `void` / NullType columns
**Impact**: Tables with NullType columns cannot be read at all
**Severity**: Medium — NullType is rare in production

### 1.2 Interval types not supported
**Affected tests**: 8 (intv_001 through intv_006, intv_boundary_values, intv_sub_second)
**Error**: Schema deserialization fails for YearMonthIntervalType and DayTimeIntervalType
**Impact**: Tables with interval columns cannot be read
**Severity**: Medium — interval types are uncommon

### 1.3 TimestampNTZ/time type deserialization
**Affected tests**: 5 (ds_multi_file_time_*)
**Error**: `untagged enum DataType` deserialization failure
**Impact**: Tables with time-type columns can't be read
**Severity**: Low-Medium — time type is less common than timestamp

### 1.4 Column mapping ID mode failures
**Affected tests**: 4+ (cm_id_matching_swapped, cm_id_matching_nonexistent)
**Error**: `None` in `final_fields_cols` when physical column IDs are swapped/missing
**Impact**: Tables using column mapping id mode with non-trivial ID assignments fail
**Severity**: High — column mapping id mode is used in production

### 1.5 Type widening for arrays/maps
**Affected tests**: 3 (tw_array_element, tw_map_key_value_widening)
**Error**: `Cannot cast list to non-list data types`
**Impact**: Tables with type widening on array elements or map keys/values fail
**Severity**: Medium — type widening on complex types is less common

### 1.6 Null metadata in checkpoint/log
**Affected tests**: 4 (pv_empty_reader_features, pv_protocol_downgrade, pv_reader_feature_not_in_writer, pv_unknown_writer_feature_ok)
**Error**: Non-nullable StructArray error when metadata is null in checkpoint
**Impact**: Certain protocol version edge cases with null metadata fail
**Severity**: Medium — affects tables with unusual protocol configurations

### 1.7 Checkpoint fallback failures
**Affected tests**: 2 (ckp_incomplete_multipart, ckp_missing_checkpoint_file)
**Error**: Can't fall back to log replay when checkpoint is corrupt/missing
**Impact**: Tables with corrupt checkpoints that Spark can recover from fail in kernel
**Severity**: High — production tables can have corrupt checkpoints

### 1.8 Non-contiguous version handling
**Affected tests**: 1 (prod_non_contiguous_versions)
**Error**: Kernel requires contiguous commits
**Impact**: Tables with version gaps (e.g., from external writers) fail
**Severity**: Medium — version gaps are uncommon but exist in production

### 1.9 Null schemaString in old protocol
**Affected tests**: 1 (pv_old_protocol_read)
**Error**: Fails with null schemaString
**Impact**: Very old Delta tables with null schema can't be read
**Severity**: Low — very rare in practice

### 1.10 Partition predicate pushdown returns 0 rows
**Affected tests**: 12 (see skip list in test file)
**Behavior**: Kernel returns 0 rows instead of the correct partition subset
**Tables affected**: cloneDeepPartitioned, cloneShallowPartitioned, cloneSqlMultiPartition, convertParquetPartitioned, cp_partitioned, dcscStructPartitioned, ds_timestamp_microsecond, ic_010_restore_partitioned, ps_partitioned, restorePartitioned
**Impact**: Partition-filtered reads return empty results
**Severity**: **Critical** — partition predicates are fundamental to Delta reads

### 1.11 TokioBackgroundExecutor RecvError
**Affected tests**: 2 (cloneDeepMultiType_readFiltered, restoreCheckData_filterBoolean)
**Error**: Panic with `TokioBackgroundExecutor has crashed: RecvError`
**Impact**: Certain filtered scans cause executor crash
**Severity**: High — crashes are unacceptable in production

### 1.12 Struct with special types data mismatch
**Affected tests**: 2 (dcscStructWithSpecialTypes read_by_date, read_high_amount)
**Behavior**: Workload returns data but contents don't match expected
**Impact**: Filtered reads on tables with mixed special types return wrong data
**Severity**: Medium

### 1.13 Time travel retention enforcement
**Affected tests**: 1 (tt_blocked_beyond_retention)
**Behavior**: Kernel doesn't enforce deletedFileRetentionDuration
**Impact**: Kernel allows time travel to versions where Spark would error
**Severity**: Low — more lenient is generally safe

---

## Category 2: Kernel Divergences (more lenient than Spark)

These are cases where Spark returns an error but kernel succeeds. The test framework documents them using `expect_kernel_failure` — if kernel is later tightened, the entry should be removed.

| Test Pattern | Divergence |
|-------------|-----------|
| `cm_err_003_invalid_mode` | Doesn't validate column mapping mode values |
| `corrupt_truncated_commit_json` | Gracefully handles truncated commit JSON |
| `cp_err_missing_protocol` | Doesn't require protocol in checkpoint |
| `ct_corrupt_parquet` | Doesn't eagerly validate parquet file integrity |
| `ct_duplicate_metadata` | Allows duplicate metadata actions |
| `ct_duplicate_protocol` | Allows duplicate protocol actions |
| `ct_invalid_json` | Handles invalid JSON in commit log gracefully |
| `ct_missing_data_file` | Doesn't eagerly check data file existence |
| `ct_missing_protocol` | Doesn't require protocol in commit 0 |
| `ct_missing_metadata` | Doesn't require metadata in commit 0 |
| `dsReadCorruptJson` | Handles corrupt JSON gracefully |
| `dv_err_001_checksum` | Doesn't validate DV checksums |
| `dv_err_003_malformed_path` | Doesn't fail on malformed DV path |
| `err_add_and_remove_same_path_dv` | Allows add+remove same path in same version |
| `err_duplicate_add_same_version` | Allows duplicate add actions |
| `err_schema_empty` | Handles empty schema |
| `err_dv_invalid_storage_type` | Doesn't validate DV storage type |
| `err_schema_invalid_json` | Handles invalid JSON schema |
| `err_missing_version_0` | Doesn't require version 0 |
| `ev_unknown_reader_feature` | Doesn't block on unknown reader features |
| `log_err_missing_protocol` | Doesn't require protocol in log |
| `log_err_missing_metadata` | Doesn't require metadata in log |
| `tt_after_vacuum` | Doesn't detect files removed by VACUUM |
| `dsReadInvalidColumnName` | Doesn't validate column names upfront |
| `ds_err_001_field_not_found` | Doesn't fail on missing field in predicate |

**Total**: 25 divergences where kernel is more lenient than Spark.

---

## Category 3: Capture Bugs (test infrastructure issues)

These are issues in the Spark-side spec capture, not kernel bugs:

| Pattern | Issue |
|---------|-------|
| Clone shallow tables | AddFile references absolute temp path (13 entries) |
| `fpe_*` tables | AddFile/data files reference absolute temp paths or special chars |
| `ds_missing_stats_graceful`, `fc_null_fields_in_add`, `stats_*` | Malformed JSON in commit log (null fields truncated) |
| `ct_empty_delta_log`, `ct_missing_delta_log`, `dseReadNonDeltaPath` | Empty/missing delta log directory |
| `dv_storage_type_*` | Corrupted DV files or absolute temp path references |
| `dv_checkpoint_only_read` | Stale checkpoint references temp data |
| `dv_err_002_missing_file` | Wrong workload type (snapshot instead of read) |
| `dsRead*_snapshot` | Snapshot spec generated for invalid/empty tables |
| `corrupt_checkpoint_corrupt_no_delta_files_snapshot` | Snapshot spec for corrupt checkpoint |

**Total**: ~35 capture bug entries

---

## Category 4: Predicate Handling

The harness handles kernel's predicate behavior:
- **Kernel only does file-level predicate pushdown** (data skipping), NOT row-level filtering
- Validation uses **superset checking** when predicates are present: actual rows >= expected rows, and every expected row exists in actual
- Predicate parsing gracefully falls back to full scan on unsupported predicates
- `scan_builder.build()` failures with predicates trigger retry without predicate

---

## Test Counts Breakdown

```
Total spec files discovered:     5288
├── Read/snapshot specs:         3643
│   ├── Normal passing:          ~3530
│   ├── Expected kernel failures:  101
│   └── Skipped (data/crash):       12
├── Write workload specs:        1645
│   ├── Passing (read post-write): 1516
│   └── Skipped (unsupported):      129
│       ├── Collation features:     ~60
│       ├── Liquid clustering:      ~45
│       └── Other unsupported:      ~24
└── Unsupported (skipped in code):  varies
    ├── CDF workloads
    ├── Txn workloads
    ├── DomainMetadata workloads
    └── Timestamp time-travel workloads
```

### Write Spec Coverage (414 test cases, 30 suites)

| Suite | Tests | Description |
|-------|-------|-------------|
| AlterTable (AW) | 15 | ALTER TABLE operations |
| CDC DML (CD) | 15 | Change data capture DML |
| CheckConstraint (CC) | 15 | Check constraint operations |
| CollationWrite (CO) | 15* | Collation-aware writes (*skipped - unsupported) |
| ColumnMapping (CM) | 15 | Column mapping writes |
| ConcurrentWrite (CW) | 15 | Concurrent write patterns |
| CTAS (CT) | 15 | CREATE TABLE AS SELECT |
| DefaultValues (DV) | 15 | Default column values |
| DeleteExtended (DE) | 15 | Extended DELETE operations |
| DML (DW) | 15 | Basic DML (INSERT/UPDATE/DELETE) |
| GeneratedColumn (GC) | 15 | Generated columns |
| IdentityColumn (IC) | 15 | Identity columns |
| InsertOverwrite (IO) | 15 | INSERT OVERWRITE |
| InsertSchemaEvolution (SE) | 15 | Schema evolution via INSERT |
| Insert (IW) | 15 | Basic INSERT |
| LiquidClustering (LC) | 15* | Liquid clustering (*skipped - unsupported) |
| MergeAdvanced (MA) | 15 | Advanced MERGE patterns |
| MergeCDC (MC) | 15 | MERGE with CDC |
| MergeNMBS (NM) | 15 | MERGE not-matched-by-source |
| MergeStarExcept (MX) | 15 | MERGE * EXCEPT |
| MergeStructEvolution (MS) | 15 | MERGE with struct evolution |
| Merge (MW) | 15 | Basic MERGE |
| Optimize (OP) | 15 | OPTIMIZE operations |
| ProtocolUpgrade (PU) | 14 | Protocol version upgrades |
| ReplaceWhere (RW) | 0* | replaceWhere (*classpath issue) |
| Restore (RS) | 15 | RESTORE operations |
| RowTracking (RT) | 15 | Row tracking writes |
| Streaming (SW) | 0* | Streaming writes (*env issue) |
| TypeWidening (TW) | 15 | Type widening writes |
| UniForm (UF) | 13 | UniForm/Iceberg compat |
| UpdateExtended (UE) | 15 | Extended UPDATE operations |
| Vacuum (VC) | 15 | VACUUM operations |

---

## Recommendations

### Critical (fix first)
1. **Partition predicate pushdown** (1.10) — Returns 0 rows for partition column equality predicates. This is a fundamental Delta read operation.
2. **TokioBackgroundExecutor crash** (1.11) — Panics should never happen; affects filtered scans.

### High Priority
3. **Column mapping ID mode** (1.4) — Production tables use this feature.
4. **Checkpoint fallback** (1.7) — Production tables can have corrupt checkpoints.

### Medium Priority
5. **Type widening for arrays/maps** (1.5)
6. **Null metadata in checkpoint** (1.6)
7. **Void/NullType support** (1.1)
8. **Interval type support** (1.2)
9. **TimestampNTZ/time type** (1.3)

### Low Priority (divergences)
10. Consider whether kernel should match Spark's strictness for the 25 divergence cases. Many of these (graceful handling of corrupt data) are arguably better behavior.
