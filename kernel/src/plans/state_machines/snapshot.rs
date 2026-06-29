//! Snapshot-construction state machine.
//!
//! Resolves the table's protocol + metadata by driving an engine [`EngineRequest::Reduce`] over the
//! log (commits + checkpoint manifest) with [`MetadataProtocolReader`], then assembles the
//! [`Snapshot`]. The caller lists the log eagerly (`LogSegment::for_snapshot`) and hands the segment
//! in — this SM drives only the P&M read through the engine, mirroring how the scan SMs take a
//! pre-built snapshot.
//!
//! [`EngineRequest::Reduce`]: super::framework::state_machine::EngineRequest::Reduce

use std::sync::Arc;

use url::Url;

use super::framework::coroutine::CoroutineSM;
use super::framework::plan_context::{Context, PlanBuilder};
use super::scan::reconciliation::{
    commit_load_schema, log_files_to_rows, FSR_BASE,
};
use crate::crc::LazyCrc;
use crate::expressions::{col, column_expr, lit, Expression};
use crate::log_segment::LogSegment;
use crate::metrics::MetricId;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::{delta_error, Engine};
use crate::plans::ir::nodes::{FileType, LoadColumnInfo, LoadNode};
use crate::plans::kernel_reducers::MetadataProtocolReader;
use crate::schema::{ColumnName, DataType, StructField};
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::FileMeta;

/// Build the snapshot-construction SM for a table root, listing the log eagerly via the engine's
/// storage handler. Convenience entry point for engines (e.g. the DuckDB FFI) that have a table
/// root + optional time-travel version and an [`Engine`] for the (cheap) directory listing; the
/// returned SM then drives the P&M read through whatever executor the engine provides.
pub fn snapshot_state_machine_for(
    table_root: Url,
    version: Option<u64>,
    engine: &dyn Engine,
) -> Result<CoroutineSM<Snapshot>, DeltaError> {
    let log_root = table_root.join("_delta_log/").map_err(|e| {
        delta_error!(
            DeltaErrorCode::DeltaStateRecoverError,
            "snapshot_state_machine_for: join _delta_log url: {e}",
        )
    })?;
    let operation_id = MetricId::new();
    let log_segment = LogSegment::for_snapshot(
        engine.storage_handler().as_ref(),
        log_root,
        vec![],
        version,
        operation_id,
    )
    .map_err(|e| e.into_delta_default())?;
    snapshot_state_machine(table_root, log_segment)
}

pub fn snapshot_state_machine(
    location: Url,
    log_segment: LogSegment,
) -> Result<CoroutineSM<Snapshot>, DeltaError> {
    CoroutineSM::new(
        "snapshot",
        move |mut engine, _sm_id| async move {
            let ctx = Context::new();
            let base = FSR_BASE.clone();
            let log_root = log_segment.log_root.clone();

            // Commits (JSON), newest-first cover; each may carry a protocol / metaData action.
            let commit_rows = log_files_to_rows(&log_root, log_segment.find_commit_cover())?;
            let commit_row_count = commit_rows.len();
            let commits = ctx.values(commit_load_schema(), commit_rows)?.load(LoadNode {
                file_schema: Arc::clone(&base),
                file_type: FileType::Json,
                base_url: Some(log_root.clone()),
                metadata_derived_columns: vec![ColumnName::new(["version"])],
                file_meta: LoadColumnInfo {
                    path_column: ColumnName::new(["path"]),
                    file_size_column: Some(ColumnName::new(["size"])),
                    num_records_column: None,
                },
                dv_ref: None,
                version: None,
            })?;
            let version_field = StructField::nullable("version", DataType::LONG);
            let cp_version =
                log_segment.checkpoint_version.unwrap_or(log_segment.end_version) as i64;

            // Checkpoint manifest, if present — protocol / metaData live in the manifest (not in
            // sidecars), so no sidecar chase is needed for P&M. A checkpoint may be parquet or (V2)
            // JSON; partition the parts by format and scan each with the same FSR base schema as the
            // commits so a union's input schemas match.
            let cp_parquet: Vec<FileMeta> = log_segment
                .listed
                .checkpoint_parts
                .iter()
                .filter(|p| p.extension != "json")
                .map(|p| p.location.clone())
                .collect();
            let cp_json: Vec<FileMeta> = log_segment
                .listed
                .checkpoint_parts
                .iter()
                .filter(|p| p.extension == "json")
                .map(|p| p.location.clone())
                .collect();

            // Combine only the NON-EMPTY arms — unioning an empty arm drops the others' streams.
            let mut arms: Vec<PlanBuilder> = Vec::new();
            if commit_row_count > 0 {
                arms.push(commits);
            }
            if !cp_parquet.is_empty() {
                arms.push(
                    ctx.scan_parquet(cp_parquet, Arc::clone(&base))?
                        .append_col_typed(version_field.clone(), lit(cp_version))?,
                );
            }
            if !cp_json.is_empty() {
                arms.push(
                    ctx.scan_json(cp_json, Arc::clone(&base))?
                        .append_col_typed(version_field.clone(), lit(cp_version))?,
                );
            }
            let mut arms = arms.into_iter();
            let source = arms.next().ok_or_else(|| {
                delta_error!(
                    DeltaErrorCode::DeltaStateRecoverError,
                    "snapshot_state_machine: log segment has no commits or checkpoint to read P&M from",
                )
            })?;
            let rest: Vec<PlanBuilder> = arms.collect();
            let source = if rest.is_empty() {
                source
            } else {
                source.union_all(&rest)?
            };

            // Max-by-version: keep the latest protocol and latest metaData (PARTITION BY a scalar
            // action kind, ORDER BY version DESC, rn=1). kind passed as the group-by expression
            // directly (no appended column).
            let value_columns: Vec<String> = base.fields().map(|f| f.name().clone()).collect();
            let kind_expr = Expression::case_when(
                vec![
                    (col(["protocol", "minReaderVersion"]).is_not_null(), lit(1i64)),
                    (col(["metaData", "id"]).is_not_null(), lit(2i64)),
                ],
                lit(0i64),
            );
            let source =
                source.max_by_version([kind_expr], column_expr!("version"), value_columns)?;

            // Engine reads the deduped (latest-per-kind) actions and drains P&M into the reducer.
            let (protocol, metadata) = ctx
                .reduce(
                    &mut engine,
                    source,
                    MetadataProtocolReader::default(),
                    "read_protocol_metadata",
                )
                .await?;

            let end_version = log_segment.end_version;
            let lazy_crc = Arc::new(LazyCrc::new(log_segment.listed.latest_crc_file.clone()));
            let table_configuration =
                TableConfiguration::try_new(metadata, protocol, location, end_version)
                    .map_err(|e| e.into_delta_default())?;
            Ok(Snapshot::new_with_crc(
                log_segment,
                table_configuration,
                lazy_crc,
            ))
        },
    )
}
