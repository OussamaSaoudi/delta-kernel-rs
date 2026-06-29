//! State-machine SDK FFI: drive kernel state machines to completion from C++, executing **every**
//! step in DuckDB.
//!
//! [`kdf_sm_scan_open`] drives the snapshot-construction SM and then the scan SM through a single
//! generic [`drive_via_duckdb`] loop and hands back the finalized data-stage SQL via
//! [`kdf_sm_result_sql`]. Each [`EngineRequest::Reduce`] is lowered to SQL and run **in DuckDB**
//! through the engine-provided [`KdfExecSqlFn`] callback (the Arrow result is drained through the
//! kernel reducer); each [`EngineRequest::SchemaQuery`] is a parquet-footer read served by the
//! kernel engine's storage/parquet handlers. The file-list reconciliation that finalizes the scan
//! plan is likewise executed in DuckDB (`finalize_result_plan_to_sql_duckdb`). No DataFusion.
//!
//! # Threading
//! [`KdfSM`] is opaque and owns only the finalized SQL string after `open` returns. The driving
//! itself happens entirely inside `kdf_sm_scan_open` on the calling thread (the SM coroutines are
//! `!Send`), so there is no cross-call cursor state to serialize.

use std::ffi::{c_char, c_void, CString};
use std::ptr;

use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{RecordBatch, StructArray};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::ir::plan::Plan;
use delta_kernel::plans::kernel_reducers::KdfControl;
use delta_kernel::plans::state_machines::framework::state_machine::{
    EngineRequest, EngineResponse, NextStep, StateMachine,
};
use delta_kernel::plans::state_machines::snapshot::snapshot_state_machine_for;
use delta_kernel::Engine;
use url::Url;

use super::plan_to_sql::result_plan_to_sql_until;
use super::{build_local_engine, finalize_result_plan_to_sql_duckdb, table_url, write_err};

/// C callback that executes `sql` in DuckDB and hands the *entire* result back as one Arrow C Data
/// batch, moved into `*out_array` / `*out_schema`. Returns 0 on success; nonzero leaves the
/// out-params untouched and fails the driving SM. This is how DuckDB — not the kernel's DataFusion
/// executor — runs every plan the state machines emit.
///
/// # Safety
/// `sql_ptr` points to `sql_len` valid UTF-8 bytes; `out_array`/`out_schema` are writable and
/// uninitialized (the callback initializes them via Arrow's C Data export). `ctx` is opaque to the
/// kernel and passed through verbatim.
pub type KdfExecSqlFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    sql_ptr: *const c_char,
    sql_len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32;

/// A DuckDB query executor: the engine-provided callback plus its opaque context. Runs a SQL
/// string and imports the result as a single Arrow [`RecordBatch`]. `Copy` so it threads cheaply
/// through the driver and the plan finalizer.
#[derive(Clone, Copy)]
pub(crate) struct DuckdbExec {
    exec_sql: KdfExecSqlFn,
    ctx: *mut c_void,
}

impl DuckdbExec {
    pub(crate) fn new(exec_sql: KdfExecSqlFn, ctx: *mut c_void) -> Self {
        Self { exec_sql, ctx }
    }

    /// Run `sql` in DuckDB and import the whole result as one Arrow batch.
    pub(crate) fn run_sql(&self, sql: &str) -> Result<RecordBatch, String> {
        let csql = CString::new(sql).map_err(|_| "SQL contains an interior NUL".to_string())?;
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let rc = unsafe {
            (self.exec_sql)(
                self.ctx,
                csql.as_ptr(),
                csql.as_bytes().len(),
                &mut out_array,
                &mut out_schema,
            )
        };
        if rc != 0 {
            return Err(format!("DuckDB exec failed (rc={rc})"));
        }
        let array_data = unsafe { from_ffi(out_array, &out_schema) }
            .map_err(|e| format!("import result from Arrow C Data: {e}"))?;
        Ok(StructArray::from(array_data).into())
    }
}

/// Execute one [`EngineRequest`] and produce the [`EngineResponse`] the SM expects:
/// - [`EngineRequest::Reduce`] → lower the plan to SQL, run it in DuckDB, and drain the Arrow rows
///   through the kernel reducer (mirrors the DataFusion executor's `drain_reduce_sink`, but DuckDB
///   does the compute).
/// - [`EngineRequest::SchemaQuery`] → read the parquet footer schema via the kernel engine's
///   storage + parquet handlers (no compute engine; this is a footer read, not a plan).
fn execute_request_via_duckdb(
    req: EngineRequest,
    engine: &dyn Engine,
    exec: DuckdbExec,
) -> Result<EngineResponse, String> {
    match req {
        EngineRequest::Reduce {
            nodes,
            terminal,
            sink,
        } => {
            let plan = Plan { nodes };
            let sql = result_plan_to_sql_until(&plan, terminal)
                .map_err(|e| format!("lower reduce plan to SQL: {e}"))?;
            let batch = exec.run_sql(&sql)?;
            // Drain the single batch through the reducer.
            let mut handle = sink.new_handle();
            let engine_data = ArrowEngineData::new(batch);
            match handle
                .apply(&engine_data)
                .map_err(|e| format!("reducer apply: {e}"))?
            {
                KdfControl::Continue | KdfControl::Break => {}
            }
            Ok(EngineResponse::Reducer(handle.finish()))
        }
        EngineRequest::SchemaQuery(q) => {
            let url = resolve_schema_query_url(&q.file_path)?;
            let meta = engine
                .storage_handler()
                .head(&url)
                .map_err(|e| format!("schema query: head {url}: {e}"))?;
            let footer = engine
                .parquet_handler()
                .read_parquet_footer(&meta)
                .map_err(|e| format!("schema query: read parquet footer {url}: {e}"))?;
            Ok(EngineResponse::Schema(footer.schema))
        }
    }
}

/// Drive any kernel [`StateMachine`] to completion, executing each step in DuckDB (Reduce) or via
/// the kernel engine (SchemaQuery). The SM coroutines are `!Send`; this runs them step-by-step on
/// the calling thread.
fn drive_via_duckdb<S: StateMachine>(
    mut sm: S,
    engine: &dyn Engine,
    exec: DuckdbExec,
) -> Result<S::Result, String> {
    loop {
        let response = match sm.get_step() {
            Ok(req) => execute_request_via_duckdb(req, engine, exec)?,
            // A zero-yield/terminal SM returns Err from get_step; prime the trampoline with Empty.
            Err(_) => EngineResponse::Empty,
        };
        match sm
            .submit(Ok(response))
            .map_err(|e| format!("SM submit: {e}"))?
        {
            NextStep::Continue => {}
            NextStep::Done(result) => return Ok(result),
        }
    }
}

/// Parse a schema-query location string into a URL (absolute URL, else a local file path).
fn resolve_schema_query_url(path: &str) -> Result<Url, String> {
    Url::parse(path).or_else(|_| {
        Url::from_file_path(std::path::Path::new(path))
            .map_err(|_| format!("invalid schema-query location string: {path}"))
    })
}

#[cfg(test)]
mod pm_sql_dump {
    //! Diagnostic: lower the snapshot SM's first (P&M) reduce request to DuckDB SQL so it can be
    //! run directly in DuckDB. `DUMP_PM_TABLE=<table> cargo test -p delta_kernel_ffi --features
    //! duckdb pm_sql -- --nocapture`.
    use super::*;

    #[test]
    fn dump_pm_sql() {
        let Ok(path) = std::env::var("DUMP_PM_TABLE") else {
            return;
        };
        let engine = build_local_engine();
        let url = table_url(&path).expect("table_url");
        let mut sm = snapshot_state_machine_for(url, None, engine.as_ref()).expect("snapshot SM");
        match sm.get_step().expect("get_step") {
            EngineRequest::Reduce { nodes, terminal, .. } => {
                let plan = Plan { nodes };
                let sql = result_plan_to_sql_until(&plan, terminal).expect("lower P&M plan to SQL");
                println!("\n===PM_SQL_BEGIN===\n{sql}\n===PM_SQL_END===\n");
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }
}

/// Opaque scan state-machine handle. After [`kdf_sm_scan_open`] it owns only the finalized
/// data-stage DuckDB SQL. Freed with [`kdf_sm_free`].
pub struct KdfSM {
    result_sql: String,
}

impl KdfSM {
    fn open(
        path: &str,
        version: i64,
        exec_sql: KdfExecSqlFn,
        exec_ctx: *mut c_void,
    ) -> Result<KdfSM, String> {
        let engine = build_local_engine();
        let url = table_url(path)?;
        let exec = DuckdbExec::new(exec_sql, exec_ctx);
        let version_opt = if version >= 0 { Some(version as u64) } else { None };

        // 1) Snapshot construction SM: resolve protocol+metadata via a Reduce executed in DuckDB,
        //    then assemble the Snapshot. (Eager log listing happens in `snapshot_state_machine_for`.)
        let snapshot_sm = snapshot_state_machine_for(url, version_opt, engine.as_ref())
            .map_err(|e| format!("build snapshot SM: {e}"))?;
        let snapshot = std::sync::Arc::new(drive_via_duckdb(snapshot_sm, engine.as_ref(), exec)?);

        // 2) Scan SM: resolve the scan shape (sidecar Reduce + footer SchemaQueries, all in DuckDB)
        //    into a ResultPlan.
        let scan = snapshot
            .scan_builder()
            .build()
            .map_err(|e| format!("build scan: {e}"))?;
        let scan_sm = scan
            .scan_state_machine()
            .map_err(|e| format!("build scan SM: {e}"))?;
        let result_plan = drive_via_duckdb(scan_sm, engine.as_ref(), exec)?;

        // 3) Finalize: materialize the runtime file-list Load in DuckDB and lower the whole plan to
        //    the data-stage SQL.
        let result_sql = finalize_result_plan_to_sql_duckdb(result_plan, exec)?;
        Ok(KdfSM { result_sql })
    }
}

/// Drive the snapshot + scan state machines for the Delta table at `path` (optionally at `version`,
/// or `-1` for latest), executing every step in DuckDB via `exec_sql`, and return an owned
/// [`KdfSM`] holding the finalized data-stage SQL. Returns null on error (`*out_err` set, free with
/// `kdf_string_free`).
///
/// # Safety
/// `path_ptr` must point to `path_len` valid UTF-8 bytes. `exec_sql` must be a valid function
/// pointer and `exec_ctx` valid for the duration of the call. `out_err`, if non-null, must point to
/// a writable `*mut c_char`. Free the result exactly once with [`kdf_sm_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_scan_open(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    exec_sql: KdfExecSqlFn,
    exec_ctx: *mut c_void,
    out_err: *mut *mut c_char,
) -> *mut KdfSM {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_sm_scan_open: null path pointer") };
        return ptr::null_mut();
    }
    let exec_ctx_addr = exec_ctx as usize;
    let outcome = std::panic::catch_unwind(move || {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        KdfSM::open(path, version, exec_sql, exec_ctx_addr as *mut c_void)
    });
    match outcome {
        Ok(Ok(sm)) => Box::into_raw(Box::new(sm)),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_scan_open: panic while driving the SM") };
            ptr::null_mut()
        }
    }
}

/// Free a [`KdfSM`].
///
/// # Safety
/// `sm` must be null or a pointer returned by [`kdf_sm_scan_open`], freed exactly once.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_free(sm: *mut KdfSM) {
    if !sm.is_null() {
        drop(unsafe { Box::from_raw(sm) });
    }
}

/// Return the finalized data-stage DuckDB SQL produced by [`kdf_sm_scan_open`]. Returns a malloc'd
/// C string (free with `kdf_string_free`), or null on error (`*out_err` set).
///
/// # Safety
/// `sm` must be a valid [`KdfSM`]. `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_result_sql(sm: *mut KdfSM, out_err: *mut *mut c_char) -> *mut c_char {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if sm.is_null() {
        unsafe { write_err(out_err, "kdf_sm_result_sql: null handle") };
        return ptr::null_mut();
    }
    let sm = unsafe { &*sm };
    match CString::new(sm.result_sql.clone()) {
        Ok(c) => c.into_raw(),
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_result_sql: SQL contains an interior NUL") };
            ptr::null_mut()
        }
    }
}
