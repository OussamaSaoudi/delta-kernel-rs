//! Canonical Full Snapshot Read (FSR) declarative plans — *window-on-commits + anti-join-on-checkpoint*.
//!
//! Mirrors Delta log-replay semantics (`kernel/src/action_reconciliation/log_replay.rs`,
//! `kernel/src/log_replay/deduplicator.rs`) by composing three or four declarative plans
//! that flow as one [`PhaseOperation::Plans`] step:
//!
//! 1. **commit_load** — `Values(commit metadata) → LoadSink(JSON, action schema, passthrough=[version])`
//!    materializes the raw per-commit action stream into [`FSR_COMMIT_RAW`]. The kernel cover
//!    over `ascending_commit_files ∪ ascending_compaction_files` is materialized verbatim so
//!    that downstream steps can `ORDER BY version DESC` to recover Delta's "newest action wins"
//!    semantics inside the commit tail.
//! 2. **commit_dedup** — `RelationRef(commit_raw) → Filter(has identity) → Project(action_cols + key) →
//!    Window(row_number PARTITION BY key ORDER BY version DESC) → Filter(__rn = 1) → Project(action_cols + key)`
//!    yields the *commit winners*, materialized into [`FSR_COMMIT_DEDUP`]. Commits supersede
//!    (and remove-tombstone) checkpoint state for any `(action_kind, identity)` pair they touch.
//! 3. **(only when `has_sidecars`) sidecar_load** — `Scan(top-level checkpoint, sidecar-only schema) →
//!    Filter(sidecar IS NOT NULL) → Project(sidecar.path, sidecar.sizeInBytes) →
//!    LoadSink(Parquet, action schema)` materializes each V2-multipart sidecar parquet's action
//!    rows into [`FSR_SIDECAR_ACTIONS`]. V1 / V2-inline checkpoints carry no sidecars; this plan
//!    is omitted entirely so the executor never opens them.
//! 4. **results** —
//!    `(Scan(top-level checkpoint) [∪ RelationRef(sidecar_actions)]) → Filter(has identity) →
//!    Project(action_cols + key) → LeftAntiJoin(probe=this, build=RelationRef(commit_dedup).project(key))`
//!    materializes the *checkpoint survivors* (rows the commit tail didn't touch). The plan
//!    completes with `Union(RelationRef(commit_dedup), survivors) → Filter(retention) →
//!    Filter(add.path IS NOT NULL) → Project(scan_row_schema) → into_results()` so the engine's
//!    `Results` consumer sees exactly the live add-action rows that classic kernel scan produces.
//!
//! The window applies only to the (typically-small) commit-tail stream; the (typically-large)
//! checkpoint stream goes through a single hash anti-join keyed on the dedup column. Compared
//! with windowing the union of commits and checkpoint, this avoids materializing per-key
//! orderings over the entire snapshot.
//!
//! ## Decision notes
//!
//! - **`dv_unique_id`**: Per-row deletion-vector identity, used only as a `partition_by` /
//!   join-key contribution to the dedup key. Implemented as
//!   `If(storageType IS NULL, NULL, ToJson(Array(storageType, pathOrInlineDv)))` —
//!   semantics-equivalent to [`crate::actions::deletion_vector::DeletionVectorDescriptor::unique_id_from_parts`]
//!   (matches [`crate::log_replay::deduplicator::Deduplicator::extract_dv_unique_id`]) for
//!   equality / non-equality but not byte-for-byte. The exact byte form is not protocol-stable,
//!   so the difference does not affect correctness; it only avoids overloading
//!   [`crate::expressions::BinaryExpressionOp::Plus`] with UTF-8 concat semantics. Note:
//!   `offset` is intentionally omitted (UUID DVs have no offset; inline DVs with the same
//!   `pathOrInlineDv` and distinct offsets would imply two distinct in-line DV byte payloads,
//!   which is not representable). A follow-up can include `offset` once kernel grows an
//!   int-to-string cast.
//! - **`single_action_schema`**: Matches [`crate::scan::scan_row_schema`] (`kernel/src/scan/log_replay.rs::SCAN_ROW_SCHEMA`),
//!   i.e. the canonical flattened rows [`Snapshot::scan`] exposes (see `kernel/src/scan/mod.rs`).
//! - **Retention thresholds**: Derived like checkpoint reconciliation via
//!   [`crate::action_reconciliation::deleted_file_retention_timestamp_with_time`] and
//!   [`crate::action_reconciliation::calculate_transaction_expiration_timestamp`] against
//!   [`crate::snapshot::Snapshot::table_properties`] (`kernel/src/table_properties/mod.rs`).
//! - **`CheckpointShape.file_format`**: Taken from the first checkpoint part's filename extension
//!   in the snapshot listing (`crate::path::ParsedLogPath::extension`), with `_last_checkpoint`
//!   schema falling back through [`crate::log_segment::LogSegment::checkpoint_schema`].

use std::sync::Arc;

use url::Url;

use crate::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, DOMAIN_METADATA_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::action_reconciliation::{
    calculate_transaction_expiration_timestamp, deleted_file_retention_timestamp_with_time,
};
use crate::expressions::{ColumnName, Expression, Predicate, Scalar, UnaryExpressionOp};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::nodes::{
    FileFormat, JoinHint, JoinNode, JoinType, LoadSink, OrderingSpec,     RelationHandle, ScanFileColumns,
    WindowFunction,
};
use crate::plans::ir::{DeclarativePlanNode, Plan};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::{PhaseOperation, SchemaQueryNode};
use crate::scan::scan_row_schema;
use crate::schema::{ArrayType, DataType, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::Snapshot;
use crate::utils::current_time_duration;
use crate::{delta_error, FileMeta, Version};

/// Raw per-commit action stream, materialized by [`build_commit_load_plan`] into a
/// [`LoadSink`] so the downstream [`build_commit_dedup_plan`] can window over it. Schema =
/// [`action_read_schema`] plus a passthrough `version` column.
pub const FSR_COMMIT_RAW: &str = "fsr.commit_raw";
/// Commit winners: the single newest action per `__fsr_join_k` partition produced by
/// [`build_commit_dedup_plan`]. Schema = [`action_read_schema`] plus the dedup-key column
/// [`FSR_JOIN_KEY_COL`].
pub const FSR_COMMIT_DEDUP: &str = "fsr.commit_dedup";
/// Sidecar action stream materialized by [`build_sidecar_load_plan`] for V2-multipart
/// checkpoints; absent (relation handle never created) for V1 / V2-inline. Schema =
/// [`action_read_schema`].
pub const FSR_SIDECAR_ACTIONS: &str = "fsr.sidecar_actions";

/// Synthetic column carrying `ToJson(fsr_dedup_key)` so hash joins only need top-level
/// column keys (`delta-kernel-datafusion-engine/src/compile/join.rs`).
pub const FSR_JOIN_KEY_COL: &str = "__fsr_join_k";

/// One literal row describing a Delta JSON commit file for [`build_commit_load_plan`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileMeta {
    pub path: String,
    pub size: i64,
    pub version: Version,
}

/// Resolved checkpoint encoding + schema hints. `actions_schema_subset` keys the top-level
/// checkpoint scan in [`build_results_plan`]; `has_sidecars` decides whether
/// [`build_sidecar_load_plan`] is appended to the plan vector.
#[derive(Clone, Debug)]
pub struct CheckpointShape {
    pub file_format: FileFormat,
    pub has_sidecars: bool,
    pub actions_schema_subset: SchemaRef,
}

/// Public entry: SM body from the FSR migration plan (`PhaseOperation::SchemaQuery` prelude optional).
pub fn full_state_sm(snapshot: Arc<Snapshot>) -> Result<CoroutineSM<()>, DeltaError> {
    let needs_schema_query = snapshot.log_segment().last_checkpoint_hint_summary().is_none()
        && snapshot_has_checkpoint_files(&snapshot);
    let maybe_checkpoint_url = needs_schema_query
        .then(|| first_checkpoint_url(snapshot.as_ref()))
        .transpose()?;

    CoroutineSM::new(move |mut co| async move {
        let mut phase = Phase(&mut co);

        let checkpoint_shape = if let Some(url) = maybe_checkpoint_url {
            let state = phase
                .execute(
                    PhaseOperation::SchemaQuery(SchemaQueryNode::new(url)),
                    "fsr.schema_query",
                )
                .await
                .map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "fsr::schema_query::execute",
                        detail = e.display_with_source_chain(),
                        source = e,
                    )
                })?;
            let schema = state.take_schema().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::schema_query::take_schema",
                    detail = "executor reported PhaseOperation::SchemaQuery success but did not submit a schema",
                )
            })?;
            checkpoint_shape_from_schema(&schema)?
        } else {
            checkpoint_shape_from_last_checkpoint(snapshot.as_ref())?
        };

        let plans = build_fsr_plans(snapshot.as_ref(), checkpoint_shape)?;
        let _state = phase
            .execute(PhaseOperation::Plans(plans), "fsr.full_state")
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::full_state::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        Ok(())
    })
}

pub fn snapshot_has_checkpoint_files(snapshot: &Snapshot) -> bool {
    !snapshot.log_segment().listed.checkpoint_parts.is_empty()
}

pub fn first_checkpoint_url(snapshot: &Snapshot) -> Result<String, DeltaError> {
    snapshot
        .log_segment()
        .listed
        .checkpoint_parts
        .first()
        .map(|p| p.location.location.as_str().to_string())
        .ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::first_checkpoint_url",
                detail = "snapshot has no checkpoint parts but schema prelude was requested",
            )
        })
}

pub fn checkpoint_shape_from_schema(schema: &SchemaRef) -> Result<CheckpointShape, DeltaError> {
    // SchemaQuery is executed against an on-disk checkpoint part today — treat as Parquet-shaped reads.
    let subset = checkpoint_actions_schema_projection(schema)?;
    Ok(CheckpointShape {
        file_format: FileFormat::Parquet,
        has_sidecars: schema.contains(SIDECAR_NAME),
        actions_schema_subset: subset,
    })
}

pub fn checkpoint_shape_from_last_checkpoint(snapshot: &Snapshot) -> Result<CheckpointShape, DeltaError> {
    let seg = snapshot.log_segment();
    let fmt = seg
        .listed
        .checkpoint_parts
        .first()
        .map(checkpoint_format_from_path)
        .unwrap_or(FileFormat::Json);

    let full_schema = seg.checkpoint_schema().unwrap_or_else(action_read_schema);
    let subset = checkpoint_actions_schema_projection(&full_schema)?;
    Ok(CheckpointShape {
        file_format: fmt,
        has_sidecars: full_schema.contains(SIDECAR_NAME),
        actions_schema_subset: subset,
    })
}

fn checkpoint_format_from_path(cp: &ParsedLogPath<FileMeta>) -> FileFormat {
    match cp.extension.as_str() {
        "json" => FileFormat::Json,
        _ => FileFormat::Parquet,
    }
}

fn checkpoint_actions_schema_projection(full: &SchemaRef) -> Result<SchemaRef, DeltaError> {
    const WANT: &[&str] = &[
        ADD_NAME,
        REMOVE_NAME,
        PROTOCOL_NAME,
        METADATA_NAME,
        DOMAIN_METADATA_NAME,
        SET_TRANSACTION_NAME,
    ];
    let names: Vec<_> = WANT.iter().copied().filter(|n| full.contains(*n)).collect();
    if names.is_empty() {
        Ok(action_read_schema())
    } else {
        full.project(names.as_slice())
            .map_err(|e| e.into_delta_default())
    }
}

/// Build the full FSR plan vector for `snapshot` in topological order:
///
/// `[commit_load, commit_dedup, (sidecar_load if has_sidecars), results]`.
///
/// - `commit_load` → `commit_dedup` is sequential by relation handle dependency
///   ([`FSR_COMMIT_RAW`]).
/// - `sidecar_load` (when present) → `results` is sequential via [`FSR_SIDECAR_ACTIONS`].
/// - `commit_dedup` → `results` is sequential via [`FSR_COMMIT_DEDUP`].
///
/// The single [`PhaseOperation::Plans`] yield wraps all of them; the executor walks them
/// in submitted order and the relation registry keeps each step's batches available to its
/// successors.
pub fn build_fsr_plans(snapshot: &Snapshot, shape: CheckpointShape) -> Result<Vec<Plan>, DeltaError> {
    let log_root = snapshot.log_segment().log_root.clone();
    let segment = snapshot.log_segment();

    let commits = commit_cover_rows(segment)?;
    let checkpoint_files: Vec<FileMeta> = segment
        .listed
        .checkpoint_parts
        .iter()
        .map(|p| p.location.clone())
        .collect();

    let commit_raw_schema =
        load_materialized_schema(&action_read_schema(), &path_size_version_schema(), &["version"])?;
    let commit_dedup_schema = augmented_action_schema()?;

    let commit_raw_handle = RelationHandle::fresh(FSR_COMMIT_RAW, commit_raw_schema);
    let commit_dedup_handle = RelationHandle::fresh(FSR_COMMIT_DEDUP, commit_dedup_schema);

    let now = current_time_duration().map_err(|e| e.into_delta_default())?;
    let min_file_ts = deleted_file_retention_timestamp_with_time(
        snapshot.table_properties().deleted_file_retention_duration,
        now,
    )
    .map_err(|e| e.into_delta_default())?;
    let txn_expiry = calculate_transaction_expiration_timestamp(snapshot.table_properties())
        .map_err(|e| e.into_delta_default())?;

    let mut plans = Vec::with_capacity(4);
    plans.push(build_commit_load_plan(&commits, &commit_raw_handle, &log_root)?);
    plans.push(build_commit_dedup_plan(&commit_raw_handle, &commit_dedup_handle)?);

    let sidecar_handle = if shape.has_sidecars {
        let handle = RelationHandle::fresh(FSR_SIDECAR_ACTIONS, action_read_schema());
        plans.push(build_sidecar_load_plan(
            checkpoint_files.clone(),
            &handle,
            &log_root,
            shape.file_format,
        )?);
        Some(handle)
    } else {
        None
    };

    plans.push(build_results_plan(
        &commit_dedup_handle,
        sidecar_handle.as_ref(),
        &checkpoint_files,
        shape.file_format,
        &shape.actions_schema_subset,
        min_file_ts,
        txn_expiry,
    )?);

    Ok(plans)
}

fn load_materialized_schema(
    file_schema: &SchemaRef,
    upstream: &SchemaRef,
    passthrough: &[&str],
) -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<StructField> = file_schema.fields().cloned().collect();
    let up = upstream.as_ref();
    for name in passthrough {
        let field = up
            .fields()
            .find(|f| f.name() == *name)
            .ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::load_materialized_schema",
                    detail = format!(
                        "upstream schema {:?} missing passthrough `{name}`",
                        upstream
                    ),
                )
            })?;
        fields.push(StructField::new(*name, field.data_type().clone(), field.is_nullable()));
    }
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

fn commit_cover_rows(seg: &crate::log_segment::LogSegment) -> Result<Vec<CommitFileMeta>, DeltaError> {
    let log_root = &seg.log_root;
    let mut rows = Vec::new();
    let merge = itertools::Itertools::merge_by(
        seg.listed.ascending_commit_files.iter(),
        seg.listed.ascending_compaction_files.iter(),
        |a, b| a.version <= b.version,
    );

    let mut last_pushed: Option<&ParsedLogPath<FileMeta>> = None;
    for next in merge {
        match last_pushed {
            Some(prev) if prev.version == next.version => {
                rows.pop();
            }
            Some(&ParsedLogPath {
                file_type: LogPathFileType::CompactedCommit { hi },
                ..
            }) if next.version <= hi => {
                continue;
            }
            _ => {}
        }
        last_pushed = Some(next);
        rows.push(CommitFileMeta {
            path: path_under_log_root(log_root, &next.location.location)?,
            size: next.location.size as i64,
            version: next.version,
        });
    }
    rows.reverse();
    Ok(rows)
}

fn path_under_log_root(log_root: &Url, file: &Url) -> Result<String, DeltaError> {
    let base = log_root.path().trim_end_matches('/');
    let full = file.path();
    let suffix = full.strip_prefix(base).unwrap_or(full);
    Ok(suffix.trim_start_matches('/').to_string())
}

fn action_read_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
    ]))
}

fn path_size_version_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
        StructField::not_null("version", DataType::LONG),
    ]))
}

fn path_size_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::not_null("path", DataType::STRING),
        StructField::not_null("size", DataType::LONG),
    ]))
}

fn sidecar_only_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        SIDECAR_NAME,
        Sidecar::to_schema(),
    )]))
}

fn single_action_schema() -> SchemaRef {
    scan_row_schema()
}

/// Tombstone / txn expiration predicate aligned with [`crate::action_reconciliation::log_replay::ActionReconciliationVisitor::is_expired_tombstone`]
/// and txn retention checks in the same visitor (`kernel/src/action_reconciliation/log_replay.rs`).
///
/// `txn_expiration_cutoff` is `None` when `delta.setTransactionRetentionDuration` is unset — txn rows are not filtered by age.
fn retention_filter(min_file_retention_timestamp: i64, txn_expiration_cutoff: Option<i64>) -> Predicate {
    let removal_ts = Expression::coalesce([
        Expression::column(["remove", "deletionTimestamp"]),
        Expression::literal(Scalar::Long(0)),
    ]);
    let remove_ok = Predicate::or(
        Predicate::is_null(Expression::column(["remove"])),
        Predicate::gt(removal_ts, Expression::literal(Scalar::Long(min_file_retention_timestamp))),
    );

    let txn_ok = match txn_expiration_cutoff {
        None => Predicate::literal(true),
        Some(cutoff) => Predicate::or_from([
            Predicate::is_null(Expression::column(["txn"])),
            Predicate::is_null(Expression::column(["txn", "lastUpdated"])),
            Predicate::gt(
                Expression::column(["txn", "lastUpdated"]),
                Expression::literal(Scalar::Long(cutoff)),
            ),
        ]),
    };

    Predicate::and(remove_ok, txn_ok)
}

fn action_identity_projection() -> Vec<Arc<Expression>> {
    action_read_schema()
        .fields()
        .map(|f| Arc::new(Expression::column([f.name().as_str()])))
        .collect()
}

fn single_action_projection() -> Vec<Arc<Expression>> {
    vec![
        Arc::new(Expression::column(["add", "path"])),
        Arc::new(Expression::column(["add", "size"])),
        Arc::new(Expression::column(["add", "modificationTime"])),
        Arc::new(Expression::column(["add", "stats"])),
        Arc::new(Expression::column(["add", "deletionVector"])),
        Arc::new(Expression::struct_from([
            Arc::new(Expression::column(["add", "partitionValues"])),
            Arc::new(Expression::column(["add", "baseRowId"])),
            Arc::new(Expression::column(["add", "defaultRowCommitVersion"])),
            Arc::new(Expression::column(["add", "tags"])),
            Arc::new(Expression::column(["add", "clusteringProvider"])),
        ])),
    ]
}

fn fsr_dedup_key() -> Expression {
    let file_arm_cond = Predicate::or(
        Predicate::is_not_null(Expression::column(["add", "path"])),
        Predicate::is_not_null(Expression::column(["remove", "path"])),
    );

    let path_coalesce = Expression::coalesce([
        Expression::column(["add", "path"]),
        Expression::column(["remove", "path"]),
    ]);
    let dv_coalesce = Expression::coalesce([
        dv_unique_id_for_prefix("add"),
        dv_unique_id_for_prefix("remove"),
    ]);

    let file_arm = Expression::array(vec![
        Expression::literal(Scalar::String("file".into())),
        path_coalesce,
        dv_coalesce,
    ]);

    let proto_arm = Expression::array(vec![Expression::literal(Scalar::String(
        PROTOCOL_NAME.into(),
    ))]);

    let meta_arm = Expression::array(vec![
        Expression::literal(Scalar::String(METADATA_NAME.into())),
        Expression::column(["metaData", "id"]),
    ]);

    let domain_arm = Expression::array(vec![
        Expression::column(["domainMetadata", "domain"]),
        Expression::column(["domainMetadata", "configuration"]),
    ]);

    let txn_arm = Expression::array(vec![Expression::column(["txn", "appId"])]);

    let null_list = Expression::literal(Scalar::Null(DataType::Array(Box::new(
        ArrayType::new(DataType::STRING, true),
    ))));

    Expression::case_when(
        vec![
            (file_arm_cond, file_arm),
            (
                Predicate::is_not_null(Expression::column(["protocol"])),
                proto_arm,
            ),
            (
                Predicate::is_not_null(Expression::column(["metaData"])),
                meta_arm,
            ),
            (
                Predicate::is_not_null(Expression::column(["domainMetadata"])),
                domain_arm,
            ),
            (Predicate::is_not_null(Expression::column(["txn"])), txn_arm),
        ],
        null_list,
    )
}

/// Per-row deletion-vector identity for an action with the given top-level prefix
/// (`"add"` or `"remove"`).
///
/// Returns `NULL` when the action carries no deletion vector (signalled by a `NULL`
/// `storageType`); otherwise returns the JSON-encoded form of the array
/// `[storageType, pathOrInlineDv]` produced by `ToJson(Array(...))`. Two rows whose deletion
/// vectors share both fields produce the same JSON string and therefore the same dedup-key
/// contribution; rows that differ in either field produce distinct strings. The exact byte
/// form is not protocol-stable — it is only used to drive Window's `partition_by` hashing and
/// the upstream LeftAnti-join key in Plan B; both consumers care only about per-row equality,
/// not about matching `DeletionVectorDescriptor::unique_id_from_parts` byte-for-byte.
///
/// `offset` is intentionally omitted (see module-level decision note); folding it in is a
/// follow-up once kernel grows an int-to-string cast. This also avoids string concatenation
/// (which would require overloading [`crate::expressions::BinaryExpressionOp::Plus`] semantics),
/// keeping `Plus` numeric-only.
fn dv_unique_id_for_prefix(prefix: &'static str) -> Expression {
    let storage_ty = Expression::column([prefix, "deletionVector", "storageType"]);
    let path_inline = Expression::column([prefix, "deletionVector", "pathOrInlineDv"]);

    // ToJson over a homogeneous array of UTF-8 columns (the inner expression evaluates to
    // List<Utf8>, which `evaluate_expression::to_json` encodes as a JSON array). Two DVs are
    // considered equal iff their (storageType, pathOrInlineDv) pair matches, which agrees with
    // [`crate::actions::deletion_vector::DeletionVectorDescriptor::unique_id_from_parts`] for the
    // overwhelmingly-common UUID storage type (offset is always None there). Inline DVs with the
    // same `pathOrInlineDv` but distinct `offset` are conceptually possible but unrepresentable
    // in real commit logs, since the inline bytes themselves carry the DV identity. A future
    // refinement can fold `offset` in once kernel grows an int-to-string cast (see follow-up).
    let dv_json = Expression::unary(
        UnaryExpressionOp::ToJson,
        Expression::array(vec![storage_ty.clone(), path_inline]),
    );

    Expression::if_then_else(
        Predicate::is_null(storage_ty),
        Expression::literal(Scalar::Null(DataType::STRING)),
        dv_json,
    )
}

/// Plan 1: materialize raw per-commit action rows into [`FSR_COMMIT_RAW`].
///
/// Each [`CommitFileMeta`] becomes one literal row; the [`LoadSink`] opens each commit JSON
/// at `<base_url>/<path>` with the action read schema and broadcasts the per-commit
/// `version` value onto every action row via `passthrough_columns`. Downstream
/// [`build_commit_dedup_plan`] uses that `version` column to `ORDER BY version DESC` inside
/// each `__fsr_join_k` partition.
fn build_commit_load_plan(
    commits: &[CommitFileMeta],
    commit_raw_handle: &RelationHandle,
    log_root: &Url,
) -> Result<Plan, DeltaError> {
    let rows: Vec<Vec<Scalar>> = commits
        .iter()
        .map(|c| {
            vec![
                Scalar::String(c.path.clone()),
                Scalar::Long(c.size),
                Scalar::Long(c.version as i64),
            ]
        })
        .collect();

    let literal_plan = DeclarativePlanNode::values(path_size_version_schema(), rows)
        .map_err(|e| e.into_delta_default())?;
    let sink = LoadSink {
        output_relation: commit_raw_handle.clone(),
        file_schema: action_read_schema(),
        base_url: Some(log_root.clone()),
        file_meta: ScanFileColumns {
            path: ColumnName::new(["path"]),
            size: Some(ColumnName::new(["size"])),
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: vec![ColumnName::new(["version"])],
        row_index_column: None,
        file_type: FileFormat::Json,
    };
    Ok(literal_plan.into_load(sink))
}

/// Plan 2: window-dedup raw commit actions into per-key winners ([`FSR_COMMIT_DEDUP`]).
///
/// Steps:
/// 1. `Filter(fsr_row_has_identity_predicate)` — drop rows that aren't a recognized action.
/// 2. `Project(action_cols + __fsr_join_k + version)` — materialize the dedup key as a
///    top-level column so `Window.partition_by` and the downstream LeftAnti can use a column
///    reference (kernel + DF window/join compilers reject non-column partition keys).
/// 3. `Window(row_number PARTITION BY __fsr_join_k ORDER BY version DESC)` — assign
///    a 1-based row number per `(action_kind, identity)`, newest commit first.
/// 4. `Filter(__rn = 1)` — keep just the newest action per key.
/// 5. `Project(action_cols + __fsr_join_k)` — drop `version` and `__rn`; the persisted
///    relation matches `augmented_action_schema()` so [`build_results_plan`]'s union
///    schema-checks line up.
fn build_commit_dedup_plan(
    commit_raw_handle: &RelationHandle,
    commit_dedup_handle: &RelationHandle,
) -> Result<Plan, DeltaError> {
    let dedup_expr = Arc::new(Expression::unary(
        UnaryExpressionOp::ToJson,
        fsr_dedup_key(),
    ));

    // Schema after the first project (action_cols + __fsr_join_k + version).
    let with_key_and_version_schema = augmented_action_schema_with_version()?;

    // First project keeps version so the window can ORDER BY version DESC.
    let project_with_key_and_version: Vec<Arc<Expression>> = action_identity_projection()
        .into_iter()
        .chain(std::iter::once(Arc::clone(&dedup_expr)))
        .chain(std::iter::once(Arc::new(Expression::column(["version"]))))
        .collect();

    let projected = DeclarativePlanNode::relation_ref(commit_raw_handle.clone())
        .filter(Arc::new(fsr_row_has_identity_predicate().into()))
        .project(project_with_key_and_version, with_key_and_version_schema);

    let windowed = projected
        .window(
            vec![WindowFunction {
                function_name: "row_number".into(),
                args: vec![],
                output_col: "__kernel_rn".into(),
            }],
            vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            vec![OrderingSpec::desc(ColumnName::new(["version"]))],
        )
        .map_err(|e| e.into_delta_default())?;

    let rn_one = Arc::new(
        Predicate::eq(
            Expression::column(["__kernel_rn"]),
            Expression::literal(Scalar::Long(1)),
        )
        .into(),
    );

    // Final project: drop `version` and `__rn`; keep action_cols + __fsr_join_k.
    let final_proj: Vec<Arc<Expression>> = action_identity_projection()
        .into_iter()
        .chain(std::iter::once(Arc::new(Expression::column([FSR_JOIN_KEY_COL]))))
        .collect();

    Ok(windowed
        .filter(rn_one)
        .project(final_proj, augmented_action_schema()?)
        .into_relation(commit_dedup_handle.clone()))
}

/// Plan 3 (only when `has_sidecars`): scan the top-level checkpoint for sidecar pointers and
/// materialize each referenced sidecar parquet's action rows into [`FSR_SIDECAR_ACTIONS`].
///
/// Top-level scan uses `sidecar_only_schema()` to read just the `sidecar` column; rows
/// without a sidecar pointer are dropped. The LoadSink reads each referenced parquet under
/// `<log_root>/<sidecar.path>` with the full action read schema; sidecars only carry
/// `add` / `remove` rows in practice, so the other columns will be NULL.
fn build_sidecar_load_plan(
    checkpoint_top_files: Vec<FileMeta>,
    sidecar_handle: &RelationHandle,
    log_root: &Url,
    format: FileFormat,
) -> Result<Plan, DeltaError> {
    let scan = DeclarativePlanNode::scan(format, checkpoint_top_files, sidecar_only_schema())
        .filter(Arc::new(
            Predicate::is_not_null(Expression::column([SIDECAR_NAME])).into(),
        ))
        .project(
            vec![
                Arc::new(Expression::column([SIDECAR_NAME, "path"])),
                Arc::new(Expression::column([SIDECAR_NAME, "sizeInBytes"])),
            ],
            path_size_schema(),
        );

    let sink = LoadSink {
        output_relation: sidecar_handle.clone(),
        file_schema: action_read_schema(),
        base_url: Some(log_root.clone()),
        file_meta: ScanFileColumns {
            path: ColumnName::new(["path"]),
            size: Some(ColumnName::new(["size"])),
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: vec![],
        row_index_column: None,
        file_type: FileFormat::Parquet,
    };

    Ok(scan.into_load(sink))
}

/// Plan 4 (terminal): assemble the live snapshot rows from commit winners + checkpoint
/// survivors and stream them to the [`SinkType::Results`](crate::plans::ir::nodes::SinkType::Results) consumer.
///
/// Inline shape:
///
/// ```text
/// checkpoint_full = top_scan [∪ relation_ref(sidecar_actions)]
/// checkpoint_keyed = checkpoint_full
///     | Filter(fsr_row_has_identity_predicate)
///     | Project(action_cols + __fsr_join_k)
///
/// commit_keys = relation_ref(commit_dedup) | Project([__fsr_join_k])
///
/// survivors = LeftAntiJoin(probe = checkpoint_keyed, build = commit_keys) on __fsr_join_k
///
/// Union(relation_ref(commit_dedup), survivors)
///     | Filter(retention)
///     | Filter(add.path IS NOT NULL)
///     | Project(single_action_projection, single_action_schema)
///     | into_results()
/// ```
///
/// The top-level checkpoint is read with the *full* action schema (missing fields
/// resolve to NULL); this keeps the union with the sidecar relation and the commit-dedup
/// relation schema-compatible without an explicit alignment project.
fn build_results_plan(
    commit_dedup_handle: &RelationHandle,
    sidecar_handle: Option<&RelationHandle>,
    checkpoint_top_files: &[FileMeta],
    checkpoint_top_format: FileFormat,
    _checkpoint_top_schema: &SchemaRef,
    min_file_retention_timestamp: i64,
    txn_expiration_cutoff: Option<i64>,
) -> Result<Plan, DeltaError> {
    let dedup_expr = Arc::new(Expression::unary(
        UnaryExpressionOp::ToJson,
        fsr_dedup_key(),
    ));
    let augmented_schema = augmented_action_schema()?;

    // Read top-level checkpoint with the full action schema so the union schema-check below
    // does not need an explicit alignment projection. The kernel reader fills missing fields
    // with NULL, which `fsr_row_has_identity_predicate` then drops.
    let top_scan = DeclarativePlanNode::scan(
        checkpoint_top_format,
        checkpoint_top_files.to_vec(),
        action_read_schema(),
    );

    let checkpoint_full = match sidecar_handle {
        Some(handle) => DeclarativePlanNode::union_unordered(vec![
            top_scan,
            DeclarativePlanNode::relation_ref(handle.clone()),
        ])
        .map_err(|e| e.into_delta_default())?,
        None => top_scan,
    };

    let checkpoint_keyed = checkpoint_full
        .filter(Arc::new(fsr_row_has_identity_predicate().into()))
        .project(
            action_identity_projection()
                .into_iter()
                .chain(std::iter::once(Arc::clone(&dedup_expr)))
                .collect(),
            augmented_schema.clone(),
        );

    // Build side: just the dedup keys from commit winners (one column wide). LeftAnti emits
    // probe rows whose key is NOT in the build set, mirroring the probe child's schema.
    let commit_keys = DeclarativePlanNode::relation_ref(commit_dedup_handle.clone()).project(
        vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
        join_key_only_schema()?,
    );

    let survivors = DeclarativePlanNode::join(
        JoinNode {
            build_keys: vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            probe_keys: vec![Arc::new(Expression::column([FSR_JOIN_KEY_COL]))],
            join_type: JoinType::LeftAnti,
            hint: JoinHint::Hash,
        },
        commit_keys,
        checkpoint_keyed,
    );

    let everything = DeclarativePlanNode::union_unordered(vec![
        DeclarativePlanNode::relation_ref(commit_dedup_handle.clone()),
        survivors,
    ])
    .map_err(|e| e.into_delta_default())?;

    Ok(everything
        .filter(Arc::new(
            retention_filter(min_file_retention_timestamp, txn_expiration_cutoff).into(),
        ))
        .filter(add_path_is_not_null())
        .project(single_action_projection(), single_action_schema())
        .into_results())
}

/// Schema = [`action_read_schema`] plus the dedup-key column [`FSR_JOIN_KEY_COL`]. Used by
/// the [`FSR_COMMIT_DEDUP`] relation handle and by the projection that materializes
/// the dedup key on the checkpoint side of the LeftAnti.
fn augmented_action_schema() -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<_> = action_read_schema().fields().cloned().collect();
    fields.push(StructField::nullable(FSR_JOIN_KEY_COL, DataType::STRING));
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

/// Schema = [`augmented_action_schema`] plus the per-commit `version` column. Carried only
/// inside [`build_commit_dedup_plan`] so the row_number window can `ORDER BY version DESC`;
/// the version column is dropped by the plan's final project.
fn augmented_action_schema_with_version() -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<_> = action_read_schema().fields().cloned().collect();
    fields.push(StructField::nullable(FSR_JOIN_KEY_COL, DataType::STRING));
    fields.push(StructField::not_null("version", DataType::LONG));
    StructType::try_new(fields)
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

/// Schema = single column `__fsr_join_k: STRING`. Build-side schema for the LeftAnti hash
/// join in [`build_results_plan`].
fn join_key_only_schema() -> Result<SchemaRef, DeltaError> {
    StructType::try_new([StructField::nullable(FSR_JOIN_KEY_COL, DataType::STRING)])
        .map(Arc::new)
        .map_err(|e| e.into_delta_default())
}

/// Predicate: `add.path IS NOT NULL`. The terminal scan-row filter retains only live add
/// rows; protocol / metaData / domainMetadata / setTransaction rows pass through earlier
/// stages so the dedup machinery sees them but are dropped here because
/// [`scan_row_schema`](crate::scan::scan_row_schema) describes file actions only.
fn add_path_is_not_null() -> Arc<Expression> {
    Arc::new(Predicate::is_not_null(Expression::column(["add", "path"])).into())
}

fn fsr_row_has_identity_predicate() -> Predicate {
    Predicate::or_from([
        Predicate::is_not_null(Expression::column(["add", "path"])),
        Predicate::is_not_null(Expression::column(["remove", "path"])),
        Predicate::is_not_null(Expression::column(["protocol"])),
        Predicate::is_not_null(Expression::column(["metaData"])),
        Predicate::is_not_null(Expression::column(["domainMetadata"])),
        Predicate::is_not_null(Expression::column(["txn"])),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
    use crate::engine::sync::SyncEngine;
    use crate::plans::ir::nodes::SinkType;
    use crate::utils::test_utils::{load_test_table, string_array_to_engine_data};
    use crate::Engine;

    /// `app-txn-no-checkpoint` has only commit JSONs (no `_last_checkpoint`, no checkpoint
    /// parts, no sidecars), so the planner must emit exactly three plans:
    /// `[commit_load, commit_dedup, results]`. The terminal `results` plan must reference
    /// `commit_dedup` (twice — once directly, once via the LeftAnti build side) and must NOT
    /// reference `sidecar_actions` (the planner must skip the sidecar load entirely).
    #[test]
    fn fsr_plan_shape_app_txn_no_checkpoint() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-no-checkpoint").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(&snapshot).unwrap();
        let plans = build_fsr_plans(&snapshot, shape).unwrap();
        assert_eq!(
            plans.len(),
            3,
            "no-checkpoint table should produce [commit_load, commit_dedup, results]"
        );

        assert!(matches!(
            plan_sink_kind(&plans[0]),
            PlanTerminalKind::Load(FSR_COMMIT_RAW)
        ));
        assert!(matches!(
            plan_sink_kind(&plans[1]),
            PlanTerminalKind::Relation(FSR_COMMIT_DEDUP)
        ));
        assert!(matches!(plan_sink_kind(&plans[2]), PlanTerminalKind::Results));

        assert!(
            plan_reads_relation_named(&plans[1], FSR_COMMIT_RAW),
            "commit_dedup plan must read FSR_COMMIT_RAW"
        );
        assert!(
            plan_reads_relation_named(&plans[2], FSR_COMMIT_DEDUP),
            "results plan must read FSR_COMMIT_DEDUP (used both as a Union arm and as the LeftAnti build side)"
        );
        assert!(
            !plan_reads_relation_named(&plans[2], FSR_SIDECAR_ACTIONS),
            "no-sidecar table must not reference FSR_SIDECAR_ACTIONS"
        );

        assert!(
            commit_dedup_window_has_version_desc_order(&plans[1]),
            "commit_dedup plan must carry Window(row_number ORDER BY version DESC)"
        );
    }

    /// `app-txn-checkpoint` carries a single classic-V1 `.checkpoint.parquet` (no sidecars),
    /// so the planner must still emit exactly three plans (no `sidecar_load`). The terminal
    /// `results` plan carries an embedded `Scan(Parquet, [<checkpoint.parquet>], action_read_schema())`
    /// that the LeftAnti probes against the commit-dedup key set.
    #[test]
    fn fsr_plan_shape_v1_checkpoint_no_sidecars() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-checkpoint").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(&snapshot).unwrap();
        assert!(
            !shape.has_sidecars,
            "app-txn-checkpoint is V1 classic; CheckpointShape.has_sidecars must be false"
        );
        let plans = build_fsr_plans(&snapshot, shape).unwrap();
        assert_eq!(
            plans.len(),
            3,
            "V1-no-sidecars table should produce [commit_load, commit_dedup, results]"
        );

        assert!(matches!(
            plan_sink_kind(&plans[0]),
            PlanTerminalKind::Load(FSR_COMMIT_RAW)
        ));
        assert!(matches!(
            plan_sink_kind(&plans[1]),
            PlanTerminalKind::Relation(FSR_COMMIT_DEDUP)
        ));
        assert!(matches!(plan_sink_kind(&plans[2]), PlanTerminalKind::Results));

        assert!(plan_reads_relation_named(&plans[1], FSR_COMMIT_RAW));
        assert!(plan_reads_relation_named(&plans[2], FSR_COMMIT_DEDUP));
        assert!(
            !plan_reads_relation_named(&plans[2], FSR_SIDECAR_ACTIONS),
            "no-sidecar shape must not reference FSR_SIDECAR_ACTIONS"
        );
        assert!(
            results_plan_carries_anti_join_against_commit_dedup(&plans[2]),
            "results plan must carry a LeftAnti(probe=checkpoint, build=commit_dedup) on __fsr_join_k"
        );
    }

    /// `v2-parquet-sidecars-struct-stats-only` carries a V2-multipart checkpoint with a
    /// `_sidecars/` directory referenced by the checkpoint manifest. The planner must emit
    /// four plans (`commit_load, commit_dedup, sidecar_load, results`); the `results` plan
    /// must reference both `FSR_COMMIT_DEDUP` (build side of the LeftAnti) and
    /// `FSR_SIDECAR_ACTIONS` (one arm of the union that feeds the LeftAnti probe).
    #[test]
    fn fsr_plan_shape_v2_multipart_with_sidecars() {
        let (_engine, snapshot, _tmp) =
            load_test_table("v2-parquet-sidecars-struct-stats-only").unwrap();
        let shape = checkpoint_shape_from_last_checkpoint(&snapshot).unwrap();
        assert!(
            shape.has_sidecars,
            "v2-parquet-sidecars-struct-stats-only is V2 multipart; CheckpointShape.has_sidecars must be true"
        );
        let plans = build_fsr_plans(&snapshot, shape).unwrap();
        assert_eq!(
            plans.len(),
            4,
            "V2-with-sidecars table should produce [commit_load, commit_dedup, sidecar_load, results]"
        );

        assert!(matches!(
            plan_sink_kind(&plans[0]),
            PlanTerminalKind::Load(FSR_COMMIT_RAW)
        ));
        assert!(matches!(
            plan_sink_kind(&plans[1]),
            PlanTerminalKind::Relation(FSR_COMMIT_DEDUP)
        ));
        assert!(matches!(
            plan_sink_kind(&plans[2]),
            PlanTerminalKind::Load(FSR_SIDECAR_ACTIONS)
        ));
        assert!(matches!(plan_sink_kind(&plans[3]), PlanTerminalKind::Results));

        assert!(plan_reads_relation_named(&plans[1], FSR_COMMIT_RAW));
        assert!(plan_reads_relation_named(&plans[3], FSR_COMMIT_DEDUP));
        assert!(
            plan_reads_relation_named(&plans[3], FSR_SIDECAR_ACTIONS),
            "results plan must reference FSR_SIDECAR_ACTIONS in the union under the LeftAnti probe"
        );
        assert!(
            results_plan_carries_anti_join_against_commit_dedup(&plans[3]),
            "results plan must carry a LeftAnti(probe=checkpoint∪sidecars, build=commit_dedup) on __fsr_join_k"
        );
    }

    #[test]
    fn fsr_dedup_key_eval_add_row_carries_path_and_dv_components() {
        // dv_unique_id is now `ToJson(Array(storageType, pathOrInlineDv))` rather than a
        // Plus-concatenated `unique_id_from_parts` string. Assert the encoded JSON carries the
        // path and DV identity components verbatim (any consumer that reduces over equality of
        // full JSON strings still dedups identical DVs).
        let line = r#"{"add":{"path":"p1.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"dvpath","offset":7,"sizeInBytes":1,"cardinality":2}}}"#;
        let engine = SyncEngine::new();
        let parsed = engine
            .json_handler()
            .parse_json(
                string_array_to_engine_data(StringArray::from(vec![line])),
                action_read_schema(),
            )
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let out = evaluate_expression(
            &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
            batch,
            Some(&DataType::STRING),
        )
        .unwrap();
        let s = out.as_string::<i32>().value(0);
        // Outer Array(["file", path_coalesce, dv_coalesce]) JSON-encodes to a JSON array; the dv
        // arm is itself a JSON-encoded array string `["u","dvpath"]` embedded as a JSON string.
        assert!(s.contains("p1.parquet"), "expected `p1.parquet` in json={s}");
        assert!(s.contains("dvpath"), "expected `dvpath` in json={s}");
        // The Plus-as-concat byte form must not appear (regression guard).
        let stringy_concat = DeletionVectorDescriptor::unique_id_from_parts("u", "dvpath", Some(7));
        assert!(
            !s.contains(&stringy_concat),
            "json contains the legacy concat form `{stringy_concat}` -- dv_unique_id must use \
             ToJson(Array(...)), not Plus-as-string-concat: json={s}"
        );
    }

    #[test]
    fn fsr_dedup_key_debug_string_is_non_trivial() {
        let dbg = format!("{:?}", fsr_dedup_key());
        assert!(dbg.len() > 40, "{dbg}");
    }

    #[test]
    fn fsr_dedup_key_eval_various_action_types() {
        let engine = SyncEngine::new();
        let schema = action_read_schema();
        let check = |line: &str, subs: &[&str]| {
            let parsed = engine
                .json_handler()
                .parse_json(
                    string_array_to_engine_data(StringArray::from(vec![line])),
                    Arc::clone(&schema),
                )
                .unwrap();
            let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
            let batch = arrow.record_batch();
            let out = evaluate_expression(
                &Expression::unary(UnaryExpressionOp::ToJson, fsr_dedup_key()),
                batch,
                Some(&DataType::STRING),
            )
            .unwrap();
            let s = out.as_string::<i32>().value(0);
            for sub in subs {
                assert!(s.contains(sub), "line={line} json={s} missing {sub}");
            }
        };

        check(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            &["protocol"],
        );
        check(
            r#"{"metaData":{"id":"mid-9","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
            &["metaData", "mid-9"],
        );
        check(
            r#"{"domainMetadata":{"domain":"dom-z","configuration":"conf-z","removed":false}}"#,
            &["dom-z", "conf-z"],
        );
        check(
            r#"{"txn":{"appId":"app-z","version":1,"lastUpdated":100}}"#,
            &["app-z"],
        );
        check(
            r#"{"remove":{"path":"r1.parquet","deletionTimestamp":1,"dataChange":true,"partitionValues":{}}}"#,
            &["r1.parquet"],
        );
    }

    #[test]
    fn retention_filter_keeps_and_drops_tombstones() {
        let p = retention_filter(100, None);
        let engine = SyncEngine::new();
        let schema = Arc::new(
            StructType::try_new([
                StructField::nullable(REMOVE_NAME, Remove::to_schema()),
                StructField::nullable("txn", SetTransaction::to_schema()),
            ])
            .unwrap(),
        );
        let rows = StringArray::from(vec![
            r#"{"remove":{"path":"old","deletionTimestamp":101,"dataChange":true,"partitionValues":{}}}"#
                .to_string(),
            r#"{"remove":{"path":"gone","deletionTimestamp":99,"dataChange":true,"partitionValues":{}}}"#
                .to_string(),
        ]);
        let parsed = engine
            .json_handler()
            .parse_json(string_array_to_engine_data(rows), schema)
            .unwrap();
        let arrow = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch = arrow.record_batch();

        let arr = evaluate_expression(&p.into(), batch, Some(&DataType::BOOLEAN)).unwrap();
        let b = arr.as_boolean();
        assert!(b.value(0));
        assert!(!b.value(1));
    }

    #[derive(Debug, PartialEq, Eq)]
    enum PlanTerminalKind {
        Load(&'static str),
        Relation(&'static str),
        Results,
    }

    fn plan_sink_kind(plan: &Plan) -> PlanTerminalKind {
        match &plan.sink.sink_type {
            SinkType::Load(l) => {
                let n = l.output_relation.name.as_str();
                if n == FSR_COMMIT_RAW {
                    PlanTerminalKind::Load(FSR_COMMIT_RAW)
                } else if n == FSR_SIDECAR_ACTIONS {
                    PlanTerminalKind::Load(FSR_SIDECAR_ACTIONS)
                } else {
                    panic!("unexpected load relation {n}")
                }
            }
            SinkType::Relation(h) => {
                let n = h.name.as_str();
                if n == FSR_COMMIT_DEDUP {
                    PlanTerminalKind::Relation(FSR_COMMIT_DEDUP)
                } else {
                    panic!("unexpected relation sink {n}")
                }
            }
            SinkType::Results => PlanTerminalKind::Results,
            other => panic!("unexpected sink {other:?}"),
        }
    }

    fn plan_reads_relation_named(plan: &Plan, name: &str) -> bool {
        fn walk(node: &DeclarativePlanNode, target: &str) -> bool {
            match node {
                DeclarativePlanNode::RelationRef(h) => h.name == target,
                DeclarativePlanNode::Filter { child, .. }
                | DeclarativePlanNode::Project { child, .. }
                | DeclarativePlanNode::Window { child, .. } => walk(child, target),
                DeclarativePlanNode::Union { children, .. } => {
                    children.iter().any(|c| walk(c, target))
                }
                DeclarativePlanNode::Join { build, probe, .. } => {
                    walk(build, target) || walk(probe, target)
                }
                _ => false,
            }
        }
        walk(&plan.root, name)
    }

    /// Walks `plan.root` for a `Join { node: { join_type: LeftAnti, .. }, build, probe }`
    /// where the build subtree references [`FSR_COMMIT_DEDUP`] and the probe-side build_keys
    /// / probe_keys are both `__fsr_join_k`. This is the structural invariant of every
    /// `results` plan, regardless of sidecar presence.
    fn results_plan_carries_anti_join_against_commit_dedup(plan: &Plan) -> bool {
        fn walk(node: &DeclarativePlanNode) -> bool {
            match node {
                DeclarativePlanNode::Join { node: jn, build, probe } => {
                    let is_left_anti = matches!(jn.join_type, JoinType::LeftAnti);
                    let build_reads_dedup = subtree_references(build, FSR_COMMIT_DEDUP);
                    let key_is_join_k = jn
                        .build_keys
                        .iter()
                        .all(|e| matches!(e.as_ref(), Expression::Column(c) if c.first() == Some(&FSR_JOIN_KEY_COL.to_string())))
                        && jn
                            .probe_keys
                            .iter()
                            .all(|e| matches!(e.as_ref(), Expression::Column(c) if c.first() == Some(&FSR_JOIN_KEY_COL.to_string())));
                    if is_left_anti && build_reads_dedup && key_is_join_k {
                        return true;
                    }
                    walk(build) || walk(probe)
                }
                DeclarativePlanNode::Filter { child, .. }
                | DeclarativePlanNode::Project { child, .. }
                | DeclarativePlanNode::Window { child, .. } => walk(child),
                DeclarativePlanNode::Union { children, .. } => children.iter().any(walk),
                _ => false,
            }
        }
        fn subtree_references(node: &DeclarativePlanNode, name: &str) -> bool {
            match node {
                DeclarativePlanNode::RelationRef(h) => h.name == name,
                DeclarativePlanNode::Filter { child, .. }
                | DeclarativePlanNode::Project { child, .. }
                | DeclarativePlanNode::Window { child, .. } => subtree_references(child, name),
                DeclarativePlanNode::Union { children, .. } => {
                    children.iter().any(|c| subtree_references(c, name))
                }
                DeclarativePlanNode::Join { build, probe, .. } => {
                    subtree_references(build, name) || subtree_references(probe, name)
                }
                _ => false,
            }
        }
        walk(&plan.root)
    }

    fn commit_dedup_window_has_version_desc_order(plan: &Plan) -> bool {
        fn find_window(node: &DeclarativePlanNode) -> Option<&crate::plans::ir::nodes::WindowNode> {
            match node {
                DeclarativePlanNode::Window { node, .. } => Some(node),
                DeclarativePlanNode::Filter { child, .. }
                | DeclarativePlanNode::Project { child, .. } => find_window(child),
                DeclarativePlanNode::Union { children, .. } => {
                    children.iter().find_map(find_window)
                }
                _ => None,
            }
        }
        let w = find_window(&plan.root).expect("window");
        !w.order_by.is_empty()
            && w.order_by[0].column == ColumnName::new(["version"])
            && w.order_by[0].descending
    }
}
