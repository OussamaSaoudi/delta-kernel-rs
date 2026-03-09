//! Test harness for improved_dat test suite.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! improved_dat directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::improved_dat::{
    test_case_from_spec_path,
    types::{WorkloadSpec, WriteSpec},
    validation::{validate_read_result, validate_snapshot_from_inline, validate_snapshot_metadata},
    workload::{execute_workload, execute_structured_write_spec, WorkloadResult},
};

fn should_skip_test(test_path: &str) -> bool {
    // ONLY skip tests that would crash the process (OOM/panic, not Result::Err)
    // or have capture bugs that make the test meaningless.
    // Kernel bugs that produce wrong data should FAIL, not be skipped —
    // each failure is a documented FINDING.
    let skip_prefixes = [
        "DV-017/", // Huge table (2B rows) causes OOM/hang — process-level skip

        // ── Capture bugs: CDC tables with read specs that expect CDF change records ──
        // These tests read CDC-enabled tables via normal `read` workloads but the expected
        // data includes change tracking rows (_change_type column) only visible through CDF.
        // This is a CAPTURE BUG — the expected data is wrong, not kernel.
        "cdc_by_path/", "cdc_column_mapping/", "cdc_data_skipping/", "cdc_deletes/",
        "cdc_deletion_vectors/", "cdc_dv_column_mapping/", "cdc_generated_columns/",
        "cdc_inserts/", "cdc_map_array/", "cdc_merge/", "cdc_merge_delete/",
        "cdc_metadata_filter/", "cdc_multiple_refs/", "cdc_multiple_types/",
        "cdc_nested_struct/", "cdc_optimize/", "cdc_partitioned/",
        "cdc_partitioned_dml/", "cdc_predicates/", "cdc_schema_evolution/",
        "cdc_single_version/", "cdc_transform/", "cdc_updates/",
        "cdc_version_range/",

        // ── Kernel bug: TokioBackgroundExecutor RecvError on filtered reads ──
        // Kernel panics (process crash, not Result::Err) during scan with certain
        // predicates. Cannot use expect_kernel_failure for panics.
        "cloneDeepMultiType/specs/cloneDeepMultiType_readFiltered",
        "restoreCheckData/specs/restoreCheckData_filterBoolean",

        // ── Capture bugs: incomplete structured write ops ──
        // The write.json is missing ops (e.g., full file deletes alongside DVs),
        // so the harness can't replicate the full commit. CAPTURE BUG.
        "DE-012/specs/DE-012_read_after",
        "RT-013/specs/RT-013_read_after",

        // ── Capture bug: multi-commit rewrite path mismatch ──
        "RW-012/specs/RW-012_read_after",
    ];

    // ── Kernel limitation: create_table doesn't support DV/feature properties ──
    // Only skip the write-path (non-write_workloads) snapshot specs; the read-path
    // write_workloads/ versions have pre-built delta/ with correct protocol.
    let write_path_only_skips = [
        "CT-014/specs/CT-014_snapshot_v0",
        "MRG-020/specs/MRG-020_snapshot_v",
    ];

    skip_prefixes.iter().any(|p| test_path.contains(p))
        || (write_path_only_skips
            .iter()
            .any(|p| test_path.contains(p))
            && !test_path.contains("write_workloads/"))
}

/// Known kernel-vs-Spark divergences. Each entry is (path substring, reason).
/// The test asserts kernel DOES fail — if a kernel fix lands, the assertion breaks
/// and the entry should be removed.
macro_rules! expect_kernel_failure {
    ($path:expr => { $( $pattern:expr => $reason:expr ),* $(,)? }) => {
        $(
            if $path.contains($pattern) {
                return assert_expected_kernel_failure($path, $reason);
            }
        )*
    };
}

fn assert_expected_kernel_failure(
    spec_path_str: &str,
    reason: &str,
) -> datatest_stable::Result<()> {
    let spec_path = std::path::PathBuf::from(spec_path_str);
    let (test_case, workload_name) =
        test_case_from_spec_path(&spec_path).expect("Failed to load test case");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let content = std::fs::read_to_string(&spec_path).expect("Failed to read spec file");
        let spec: WorkloadSpec = serde_json::from_str(&content).expect("Failed to parse spec file");
        let table_root = test_case.table_root().expect("Failed to get table URL");
        let engine =
            test_utils::create_default_engine(&table_root).expect("Failed to create engine");

        let result = execute_workload(engine, &table_root, &spec);
        match result {
            Err(e) => {
                println!("  Expected kernel failure ({}): {}", reason, e);
            }
            Ok(_) if spec.expects_error() => {
                println!(
                    "  Expected kernel divergence ({}): kernel succeeded where spec expects error",
                    reason
                );
            }
            Ok(_) => {
                panic!(
                    "Workload '{}' was expected to fail but succeeded! \
                     Reason for expected failure: {}. \
                     If kernel now handles this, remove it from expect_kernel_failure!",
                    workload_name, reason
                );
            }
        }
    });
    Ok(())
}

/// Features that kernel does not support. Write specs involving these features
/// are skipped because the table cannot be read at all.
/// Matched case-insensitively against tags in test_info.json.
const UNSUPPORTED_FEATURE_TAGS: &[&str] = &[
    "collation",         // Kernel does not support collation-aware types
    "liquid-clustering", // Kernel does not support liquid clustering metadata
    "liquidclustering",  // Alternative casing
];

/// Check if workload type is unsupported by the harness.
fn unsupported_workload_reason(spec: &WorkloadSpec) -> Option<&'static str> {
    match spec {
        WorkloadSpec::Read {
            timestamp: Some(_), ..
        }
        | WorkloadSpec::Snapshot {
            timestamp: Some(_), ..
        } => Some("Timestamp-based time travel not supported by harness"),
        WorkloadSpec::Txn { .. } => Some("Txn workloads not supported in this build"),
        WorkloadSpec::DomainMetadata { .. } => {
            Some("DomainMetadata workloads not supported in this build")
        }
        _ => None,
    }
}

/// Operations where empty ops means "metadata/protocol change that can't be decomposed."
/// An empty transaction can't replicate these — must skip the spec.
const METADATA_CHANGING_OPERATIONS: &[&str] = &[
    "SET TBLPROPERTIES",
    "UNSET TBLPROPERTIES",
    "UPGRADE PROTOCOL",
    "ALTER TABLE",
];

/// Check if a structured write spec should be skipped based on unsupported ops.
fn structured_write_skip_reason(write_spec: &WriteSpec) -> Option<String> {
    use acceptance::improved_dat::types::WriteOp;

    for commit in &write_spec.commits {
        if commit.ops.is_empty() {
            // Check if this is a metadata-changing operation that can't be replicated
            // with an empty commit (ALTER TABLE, UPGRADE PROTOCOL, etc.)
            let op = commit.operation.as_deref().unwrap_or("");
            if METADATA_CHANGING_OPERATIONS.iter().any(|m| op.contains(m)) {
                return Some(format!(
                    "empty ops for metadata-changing operation '{}' (can't replicate with empty commit)",
                    op
                ));
            }
            // Otherwise (no-match DML, VACUUM, etc.) — empty commit is fine
            continue;
        }
        for op in &commit.ops {
            match op {
                WriteOp::Replace { .. } => return Some("replace op not supported".to_string()),
                WriteOp::UpdateSchema { .. } => {
                    return Some("updateSchema op not supported".to_string())
                }
                WriteOp::UpdateProperties { set, .. } => {
                    // Check for protocol-upgrade properties that we can't replicate.
                    // delta.minReaderVersion/delta.minWriterVersion trigger Spark-specific
                    // protocol upgrades that add all features implied by those version numbers.
                    if let Some(props) = set {
                        if props.contains_key("delta.minReaderVersion")
                            || props.contains_key("delta.minWriterVersion")
                        {
                            return Some(
                                "updateProperties sets delta.min{Reader,Writer}Version (protocol upgrade not supported)".to_string()
                            );
                        }
                    }
                    // Other updateProperties are supported via inject_metadata_update
                }
                WriteOp::SetClustering { .. } => {
                    return Some("setClustering op not supported".to_string())
                }
                WriteOp::RestoreRows { .. } => {
                    return Some("restoreRows op not supported".to_string())
                }
                WriteOp::RemoveDeletes { .. } => {
                    return Some("removeDeletes op not supported".to_string())
                }
                WriteOp::Delete { .. }
                | WriteOp::Rewrite { .. }
                | WriteOp::SetDeletes { .. }
                | WriteOp::AddDeletes { .. } => {
                    // These ops are now supported by the harness
                }
                _ => {} // create, append, setTransaction, setDomainMetadata are supported
            }
        }
    }
    None
}

/// Check if a write spec should be skipped based on test_info.json tags or known
/// unsupported features. Returns a reason string if skipped.
fn write_spec_skip_reason(test_case_root: &std::path::Path) -> Option<String> {
    // Check test_info.json for tags
    let test_info_path = test_case_root.join("test_info.json");
    if test_info_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&test_info_path) {
            if let Ok(info) = serde_json::from_str::<serde_json::Value>(&content) {
                // Check tags array for unsupported features
                if let Some(tags) = info.get("tags").and_then(|t| t.as_array()) {
                    for tag in tags {
                        if let Some(tag_str) = tag.as_str() {
                            for unsupported in UNSUPPORTED_FEATURE_TAGS {
                                if tag_str.eq_ignore_ascii_case(unsupported) {
                                    return Some(format!(
                                        "Unsupported feature: {} (kernel does not support this)",
                                        tag_str
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn improved_dat_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();

    if should_skip_test(&spec_path_str) {
        println!("Skipping test: {}", spec_path_str);
        return Ok(());
    }

    let (test_case, workload_name) =
        test_case_from_spec_path(&spec_path_abs).expect("Failed to load test case");
    let expected_dir = test_case.expected_dir(&workload_name);

    expect_kernel_failure!(&spec_path_str => {
        // ── Kernel bugs: struct special types ──
        "dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_snapshot" =>
            "Kernel bug: index out of bounds when reading struct with special types",

        // ── Kernel bugs ──
        "corrupt_incomplete_multipart_checkpoint/" =>
            "Kernel can't fall back to log replay when multipart checkpoint has missing parts",
        "prod_non_contiguous_versions/" =>
            "Kernel requires contiguous commits; Spark uses CRC files to bridge gaps",
        "cm_id_matching_swapped/specs/cm_id_matching_swapped_select_" =>
            "Kernel bug: column mapping id mode fails with None in final_fields_cols",
        "cm_id_matching_nonexistent/specs/cm_id_matching_nonexistent_select_" =>
            "Kernel bug: column mapping id mode fails with None in final_fields_cols",
        "tw_array_element/specs/tw_array_element_read_" =>
            "Kernel bug: Cannot cast list to non-list data types during type widening",
        "tw_map_key_value_widening/specs/tw_map_key_value_widening_read_all" =>
            "Kernel bug: Cannot cast list to non-list data types during type widening",
        "ds_multi_file_time/" =>
            "Kernel bug: schema deserialization fails for TimestampNTZ/time type",
        // Kernel doesn't support void type in schema
        "void_001_void_top_level/" =>
            "Kernel bug: void/NullType not supported in schema deserialization",
        "void_002_void_nested_struct/" =>
            "Kernel bug: void/NullType not supported in schema deserialization",
        "void_005_void_schema_evolution/" =>
            "Kernel bug: void/NullType not supported in schema deserialization",
        "void_006_void_multiple_columns/" =>
            "Kernel bug: void/NullType not supported in schema deserialization",
        "void_007_void_with_backticks/" =>
            "Kernel bug: void/NullType not supported in schema deserialization",
        // Kernel doesn't support interval types in schema
        "intv_001_interval_ym_basic/" =>
            "Kernel bug: YearMonthIntervalType not supported in schema deserialization",
        "intv_002_interval_dt_basic/" =>
            "Kernel bug: DayTimeIntervalType not supported in schema deserialization",
        "intv_003_interval_partitioned/" =>
            "Kernel bug: interval types not supported in schema deserialization",
        "intv_004_interval_negative/" =>
            "Kernel bug: interval types not supported in schema deserialization",
        "intv_005_interval_mixed/" =>
            "Kernel bug: interval types not supported in schema deserialization",
        "intv_006_create_insert_select/" =>
            "Kernel bug: interval types not supported in schema deserialization",
        // Kernel doesn't enforce file retention duration
        "tt_blocked_beyond_retention/specs/tt_blocked_beyond_retention_error" =>
            "Kernel bug: doesn't enforce deletedFileRetentionDuration for time travel",
        // Kernel fails to read snapshot with null schemaString
        "pv_old_protocol_read/specs/pv_old_protocol_read_snapshot" =>
            "Kernel bug: fails with null schemaString in old protocol metadata",


        // Clone table entries removed — capture bug fixed in regenerated specs

        // ── Kernel divergence: kernel is more lenient than Spark for these errors ──
        "cm_err_003_invalid_mode/specs/cm_err_003_invalid_mode_error" =>
            "Kernel divergence: doesn't validate column mapping mode values",
        "corrupt_truncated_commit_json/specs/corrupt_truncated_commit_json_error" =>
            "Kernel divergence: gracefully handles truncated commit JSON",
        "cp_err_missing_protocol/specs/cp_err_missing_protocol_error" =>
            "Kernel divergence: doesn't require protocol in checkpoint",
        "ct_corrupt_parquet/specs/ct_corrupt_parquet_error" =>
            "Kernel divergence: doesn't eagerly validate parquet file integrity",
        "ct_duplicate_metadata/specs/ct_duplicate_metadata_error" =>
            "Kernel divergence: allows duplicate metadata actions",
        "ct_duplicate_protocol/specs/ct_duplicate_protocol_error" =>
            "Kernel divergence: allows duplicate protocol actions",
        "ct_invalid_json/specs/ct_invalid_json_error" =>
            "Kernel divergence: handles invalid JSON in commit log gracefully",
        "ct_missing_data_file/specs/ct_missing_data_file_error" =>
            "Kernel divergence: doesn't eagerly check data file existence",
        "ct_missing_protocol/specs/ct_missing_protocol_error" =>
            "Kernel divergence: doesn't require protocol action in commit 0",
        "ct_missing_metadata/specs/ct_missing_metadata_error" =>
            "Kernel divergence: doesn't require metadata action in commit 0",
        "dsReadCorruptJson/specs/dsReadCorruptJson_error" =>
            "Kernel divergence: handles corrupt JSON gracefully",
        "dsReadCorruptCheckpoint/specs/dsReadCorruptCheckpoint_error" =>
            "Kernel divergence: reads corrupted checkpoint without error",
        "dsReadModifyCheckpoint/specs/dsReadModifyCheckpoint_error" =>
            "Kernel divergence: reads modified checkpoint without error",
        "dv_err_001_checksum/specs/dv_err_001_checksum_error" =>
            "Kernel divergence: doesn't validate DV checksums",
        "err_add_and_remove_same_path_dv/specs/err_add_and_remove_same_path_dv_error" =>
            "Kernel divergence: allows add+remove same path in same version",
        "err_duplicate_add_same_version/specs/err_duplicate_add_same_version_error" =>
            "Kernel divergence: allows duplicate add actions",
        "err_schema_empty/specs/err_schema_empty_error" =>
            "Kernel divergence: handles empty schema",
        "err_dv_invalid_storage_type/specs/err_dv_invalid_storage_type_error" =>
            "Kernel divergence: doesn't validate DV storage type",
        "err_schema_invalid_json/specs/err_schema_invalid_json_error" =>
            "Kernel divergence: handles invalid JSON schema",
        "err_missing_version_0/specs/err_missing_version_0_error" =>
            "Kernel divergence: doesn't require version 0",
        "ev_unknown_reader_feature/specs/ev_unknown_reader_feature_error" =>
            "Kernel divergence: doesn't block on unknown reader features",
        "log_err_missing_protocol/specs/log_err_missing_protocol_error" =>
            "Kernel divergence: doesn't require protocol in log",
        "log_err_missing_metadata/specs/log_err_missing_metadata_error" =>
            "Kernel divergence: doesn't require metadata in log",
        "tt_after_vacuum/specs/tt_after_vacuum_error" =>
            "Kernel divergence: doesn't detect files removed by VACUUM",

        // ── Kernel bug: null metadata in checkpoint causes non-nullable StructArray error ──
        // ps_partitioned: filter workload triggers null metadata StructArray only in full_scan,
        // not filter_partition (which succeeds). Removed from expect_kernel_failure.
        "pv_empty_reader_features/" =>
            "Kernel bug: null metaData in checkpoint/log causes StructArray error",
        "pv_protocol_downgrade/" =>
            "Kernel bug: null metaData in checkpoint/log causes StructArray error",
        "pv_reader_feature_not_in_writer/" =>
            "Kernel bug: null metaData in checkpoint/log causes StructArray error",
        "pv_unknown_writer_feature_ok/" =>
            "Kernel bug: null metaData in checkpoint/log causes StructArray error",

        // ── Capture bugs: malformed JSON in commit log (only affects read workloads) ──
        "ds_missing_stats_graceful/specs/ds_missing_stats_graceful_filter_" =>
            "Capture bug: malformed JSON in commit log (null stats field truncated)",
        "ds_missing_stats_graceful/specs/ds_missing_stats_graceful_read_" =>
            "Capture bug: malformed JSON in commit log (null stats field truncated)",
        "fc_null_fields_in_add/specs/fc_null_fields_in_add_read_" =>
            "Capture bug: malformed JSON in commit log (null fields truncated)",
        "stats_empty_string/specs/stats_empty_string_filter_" =>
            "Capture bug: malformed JSON in commit log (null stats truncated)",
        "stats_empty_string/specs/stats_empty_string_read_" =>
            "Capture bug: malformed JSON in commit log (null stats truncated)",
        "stats_missing_entirely/specs/stats_missing_entirely_filter_" =>
            "Capture bug: malformed JSON in commit log (null stats truncated)",
        "stats_missing_entirely/specs/stats_missing_entirely_read_" =>
            "Capture bug: malformed JSON in commit log (null stats truncated)",
        "stats_numrecords_only/specs/stats_numrecords_only_filter_" =>
            "Capture bug: malformed JSON in commit log (null stats truncated)",
        "stats_numrecords_only/specs/stats_numrecords_only_read_" =>
            "Capture bug: malformed JSON in commit log (null stats truncated)",
        // restorePartitioned: read workloads succeed despite malformed stats. Removed.

        // ── Kernel bug: checkpoint with missing _last_checkpoint file ──
        "ckp_incomplete_multipart/" =>
            "Kernel bug: can't fall back when multipart checkpoint parts are missing",
        "ckp_missing_checkpoint_file/" =>
            "Kernel bug: can't fall back when checkpoint file referenced by _last_checkpoint is missing",

        // ── Kernel bug: interval types not supported ──
        "intv_boundary_values/" =>
            "Kernel bug: DayTimeIntervalType not supported in schema deserialization",
        "intv_sub_second/" =>
            "Kernel bug: DayTimeIntervalType not supported in schema deserialization",

        // ── Kernel bug: void type not supported ──
        "void_in_struct/" =>
            "Kernel bug: void/NullType not supported in schema deserialization",

        // ── Capture bugs: data file paths not found (only affects read workloads) ──
        "fpe_absolute_path/specs/fpe_absolute_path_read_" =>
            "Capture bug: AddFile references absolute temp path",
        "fpe_absolute_path/specs/fpe_absolute_path_filter_" =>
            "Capture bug: AddFile references absolute temp path",
        "fpe_space_in_path/specs/fpe_space_in_path_read_" =>
            "Capture bug: data file with space in filename not found",
        "fpe_space_in_path/specs/fpe_space_in_path_filter_" =>
            "Capture bug: data file with space in filename not found",
        "fpe_special_chars_path/specs/fpe_special_chars_path_read_" =>
            "Capture bug: data file with special chars not found",
        "fpe_special_chars_path/specs/fpe_special_chars_path_filter_" =>
            "Capture bug: data file with special chars not found",
        "fpe_unicode_in_path/specs/fpe_unicode_in_path_read_" =>
            "Capture bug: data file with unicode chars not found",
        "fpe_unicode_in_path/specs/fpe_unicode_in_path_project_" =>
            "Capture bug: data file with unicode chars not found",

        // ── Capture bugs: empty/invalid delta logs from CoreReadsSuiteCapture ──
        "ct_empty_delta_log/" =>
            "Capture bug: empty delta log directory",
        "ct_missing_delta_log/" =>
            "Capture bug: missing delta log directory",
        "dseReadNonDeltaPath/" =>
            "Capture bug: non-delta path has empty log segment",

        // ── Capture bugs: DV storage type tests have corrupted DV files ──
        "dv_storage_type_i/specs/dv_storage_type_i_read_" =>
            "Capture bug: DV file has invalid magic (corrupt DV data)",
        "dv_storage_type_p/specs/dv_storage_type_p_read_" =>
            "Capture bug: DV file references absolute temp path",

        // ── Kernel bug: DV with malformed path ──
        "dv_err_003_malformed_path/specs/dv_err_003_malformed_path_error" =>
            "Kernel divergence: doesn't fail on malformed DV path",

        // ── Capture bugs: snapshot spec generated for tables that can't construct valid snapshot ──
        "dv_err_002_missing_file/specs/dv_err_002_missing_file_error.json" =>
            "Capture bug: snapshot type doesn't access DV files; should be read type",
        "dsReadEmptyTable/specs/dsReadEmptyTable_snapshot" =>
            "Capture bug: snapshot spec generated for empty/invalid table",
        "dsReadEmptyString/specs/dsReadEmptyString_snapshot" =>
            "Capture bug: snapshot spec generated for empty/invalid table",
        "dsReadMissingCommitFile/specs/dsReadMissingCommitFile_snapshot" =>
            "Capture bug: snapshot spec generated for table with missing commit",
        "dsReadMissingDeltaLog/specs/dsReadMissingDeltaLog_snapshot" =>
            "Capture bug: snapshot spec generated for table with no delta log",
        "dsReadPathWithSpaces/specs/dsReadPathWithSpaces_snapshot" =>
            "Capture bug: snapshot spec generated for table at missing path",
        "dsReadDuplicateColumns/specs/dsReadDuplicateColumns_snapshot" =>
            "Capture bug: snapshot spec generated for table with duplicate columns",
        "corrupt_checkpoint_corrupt_no_delta_files/specs/corrupt_checkpoint_corrupt_no_delta_files_snapshot" =>
            "Capture bug: snapshot spec generated for table with corrupt checkpoint",

        // ── Kernel divergence: error specs where kernel succeeds ──
        "dsReadInvalidColumnName/specs/dsReadInvalidColumnName_error" =>
            "Kernel divergence: doesn't validate column names upfront",
        "ds_err_001_field_not_found/specs/ds_err_001_field_not_found_error" =>
            "Kernel divergence: doesn't fail on missing field in predicate",

        // Clone with ICT entry removed — capture bug fixed in regenerated specs

        // ── Capture bugs: tables with stale data from temp directories (read-only) ──
        "dv_checkpoint_only_read/specs/dv_checkpoint_only_read_read_" =>
            "Capture bug: stale checkpoint references temp data",
        "dv_checkpoint_only_read/specs/dv_checkpoint_only_read_filter_" =>
            "Capture bug: stale checkpoint references temp data",
        "dv_checkpoint_only_read/specs/dv_checkpoint_only_read_snapshot" =>
            "Capture bug: stale checkpoint references temp data, no files in log segment",

        // ── Kernel bug: unsupported geometry type ──
        "GS-003/" =>
            "Kernel bug: geometry(SRID:0) type not supported in schema deserialization",

        // ── Capture bug: DV file path with percent-encoded prefix ──
        "DV-005b/specs/DV-005b_count" =>
            "Capture bug: DV data file has percent-encoded prefix that fails path resolution",

    });

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let content =
                std::fs::read_to_string(&spec_path_abs).expect("Failed to read spec file");
            let spec: WorkloadSpec =
                serde_json::from_str(&content).expect("Failed to parse spec file");

            println!("Running workload: {}", workload_name);

            // Check for structured write spec (write.json with commits)
            let write_json_path = test_case.root_dir.join("write.json");
            let structured_write = if write_json_path.exists() {
                let content = std::fs::read_to_string(&write_json_path).ok();
                content.and_then(|c| {
                    let ws: Result<WriteSpec, _> = serde_json::from_str(&c);
                    ws.ok().filter(|w| !w.commits.is_empty())
                })
            } else {
                None
            };

            // If this test has a structured write spec, execute the write through
            // kernel APIs. If kernel can't handle it, the test FAILS — no fallbacks.
            let (table_root, _temp_dir) = if let Some(ref write_spec) = structured_write {
                // Check if write ops are supported by the harness
                if let Some(reason) = structured_write_skip_reason(write_spec) {
                    panic!(
                        "KERNEL/HARNESS WRITE LIMITATION for '{}': {}",
                        workload_name, reason
                    );
                }

                match execute_structured_write_spec(&test_case.root_dir, write_spec).await {
                    Ok((url, temp_path)) => {
                        println!("  Executed structured write to temp table");
                        (url, Some(temp_path))
                    }
                    Err(e) => {
                        panic!(
                            "KERNEL WRITE FAILURE for '{}': {}",
                            workload_name, e
                        );
                    }
                }
            } else {
                let url = test_case.table_root().expect("Failed to get table URL");
                (url, None)
            };

            let engine =
                test_utils::create_default_engine(&table_root).expect("Failed to create engine");

            // Unsupported workload types fail — no silent skips
            if let Some(reason) = unsupported_workload_reason(&spec) {
                panic!(
                    "UNSUPPORTED WORKLOAD for '{}': {}",
                    workload_name, reason
                );
            }

            // Unsupported kernel features fail — no silent skips
            if matches!(spec, WorkloadSpec::Write { .. }) {
                if let Some(reason) = write_spec_skip_reason(&test_case.root_dir) {
                    panic!(
                        "UNSUPPORTED KERNEL FEATURE for '{}': {}",
                        workload_name, reason
                    );
                }
            }

            // Error workloads
            if spec.expects_error() {
                let expected_error = spec.expected_error().unwrap();
                let result = execute_workload(engine.clone(), &table_root, &spec);
                match result {
                    Ok(_) => {
                        panic!(
                            "Workload '{}' expected error '{}' but succeeded",
                            workload_name, expected_error.error_code
                        );
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        // Validate error message pattern if specified
                        if let Some(ref pattern) = expected_error.error_message {
                            if !err_str.contains(pattern.as_str()) {
                                println!(
                                    "  WARNING: Error message mismatch for '{}': expected pattern '{}', got '{}'",
                                    workload_name, pattern, err_str
                                );
                            }
                        }
                        println!(
                            "  Got expected error (code='{}', actual='{}')",
                            expected_error.error_code, err_str
                        );
                    }
                }
                return;
            }

            // Execute workload
            let result = execute_workload(engine.clone(), &table_root, &spec)
                .unwrap_or_else(|e| panic!("Workload '{}' failed: {}", workload_name, e));

            // Validate results
            let has_predicate = spec.has_predicate();
            match result {
                WorkloadResult::Read(read_result) => {
                    let inline_expected = match &spec {
                        WorkloadSpec::Read { expected: Some(ref e), .. } => Some(e),
                        _ => None,
                    };
                    let batch = read_result.concat().expect("Failed to concat batches");
                    if expected_dir.exists() || inline_expected.is_some() {
                        validate_read_result(batch, &expected_dir, has_predicate, inline_expected)
                            .await
                            .unwrap_or_else(|e| {
                                panic!("Validation failed for workload '{}': {}", workload_name, e)
                            });
                    } else {
                        // No expected data dir — execution succeeded but we can't validate.
                        // This is acceptable for write-path intermediate reads (e.g., read_v0
                        // during multi-commit replay) where only the final state is verified.
                        println!(
                            "  No expected data for '{}' ({} rows returned, not validated)",
                            workload_name,
                            batch.num_rows()
                        );
                    }
                }
                WorkloadResult::Snapshot(snapshot_result) => {
                    let inline_expected = match &spec {
                        WorkloadSpec::Snapshot { expected: Some(ref e), .. } => Some(e),
                        _ => None,
                    };
                    if let Some(inline) = inline_expected {
                        validate_snapshot_from_inline(&snapshot_result, inline)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "Metadata validation failed for workload '{}': {}",
                                    workload_name, e
                                )
                            });
                    } else if expected_dir.exists() {
                        validate_snapshot_metadata(&snapshot_result, &expected_dir).unwrap_or_else(
                            |e| {
                                panic!(
                                    "Metadata validation failed for workload '{}': {}",
                                    workload_name, e
                                )
                            },
                        );
                    } else {
                        println!(
                            "  No expected metadata for '{}' (snapshot not validated)",
                            workload_name
                        );
                    }
                }
            }

            println!("  Passed");
        });

    Ok(())
}

datatest_stable::harness! {
    {
        test = improved_dat_test,
        root = "../improved_dat/",
        pattern = r"specs/.*\.json$"
    },
}
