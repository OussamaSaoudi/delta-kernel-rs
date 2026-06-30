//! State-machine SDK FFI: **DuckDB drives the kernel scan state machine.**
//!
//! The kernel hands DuckDB a steppable state machine ([`kdf_scan_open`]); DuckDB owns the loop:
//! pull the next step ([`kdf_sm_get_step`]), execute it *in DuckDB* (a [`Reduce`] becomes a SQL
//! string via [`kdf_sm_reduce_sql`] that DuckDB runs; the Arrow result is handed back via
//! [`kdf_sm_submit_reduce`]), and repeat until `Done`. Then [`kdf_sm_result_sql`] lowers the
//! terminal `ResultPlan` to the SQL DuckDB executes for the scan. **No DuckDB callback is ever
//! passed into the kernel** — the kernel is a passive state machine, the engine drives it.
//!
//! Snapshot construction (protocol+metadata) and scan reconciliation are two kernel SMs run in
//! sequence; the handle sequences them internally (a phase enum) so DuckDB just sees one
//! get_step/submit stream. `metadata_only` selects the kernel's metadata-only scan SM (terminates
//! at the surviving-file list) vs the full data+metadata SM.
//!
//! [`Reduce`]: EngineRequest::Reduce
//!
//! # Threading
//! [`KdfSM`] wraps `!Send` coroutine SMs; it is a single-owner cursor. Never call two `kdf_sm_*`
//! entry points for one handle concurrently.

use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{RecordBatch, StructArray};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::ir::nodes::ReduceSink;
use delta_kernel::plans::ir::plan::{Plan, ResultPlan};
use delta_kernel::plans::kernel_reducers::KdfControl;
use delta_kernel::plans::state_machines::framework::coroutine::CoroutineSM;
use delta_kernel::plans::state_machines::framework::state_machine::{
    EngineRequest, EngineResponse, NextStep, StateMachine,
};
use delta_kernel::plans::state_machines::snapshot::snapshot_state_machine_for;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Engine;
use url::Url;

use super::plan_to_sql::result_plan_to_sql_until;
use super::{build_engine_for_url, table_url, write_err};
use crate::expressions::kernel_visitor::{unwrap_kernel_predicate, KernelExpressionVisitorState};
use crate::scan::EnginePredicate;

/// `kdf_sm_get_step` result: the kernel needs DuckDB to run a Reduce's plan.
pub const KDF_STEP_REDUCE: i32 = 1;
/// `kdf_sm_get_step` result: the state machine is finished; call `kdf_sm_result_sql`.
pub const KDF_STEP_DONE: i32 = 0;

/// Build a data-skipping predicate from `DELTA_SM_PREDICATE` ("col OP literal"; OP one of
/// `>= <= != > < =`; literal parsed i32 → i64 → f64 → quoted string). `None` ⇒ no skipping.
fn predicate_from_env() -> Option<delta_kernel::expressions::Predicate> {
    use delta_kernel::expressions::{Expression, Scalar};
    let spec = std::env::var("DELTA_SM_PREDICATE").ok()?;
    let spec = spec.trim();
    for sym in [">=", "<=", "!=", ">", "<", "="] {
        let Some(idx) = spec.find(sym) else { continue };
        let col = spec[..idx].trim();
        let rhs = spec[idx + sym.len()..].trim();
        if col.is_empty() || rhs.is_empty() {
            return None;
        }
        let scalar: Scalar = if let Ok(i) = rhs.parse::<i32>() {
            i.into()
        } else if let Ok(i) = rhs.parse::<i64>() {
            i.into()
        } else if let Ok(f) = rhs.parse::<f64>() {
            f.into()
        } else {
            rhs.trim_matches(|c| c == '\'' || c == '"').to_string().into()
        };
        let col_expr = Expression::column([col]);
        let lit = Expression::literal(scalar);
        return Some(match sym {
            ">=" => col_expr.ge(lit),
            "<=" => col_expr.le(lit),
            "!=" => col_expr.ne(lit),
            ">" => col_expr.gt(lit),
            "<" => col_expr.lt(lit),
            _ => col_expr.eq(lit),
        });
    }
    None
}

/// Decode an [`EnginePredicate`] into a kernel [`Predicate`] using the SAME visitor logic
/// `apply_predicate` uses (run the engine's visitor, then `unwrap_kernel_predicate`). The engine
/// state behind the predicate must remain valid for the duration of this call.
///
/// # Safety
/// `predicate` is a valid, non-null `EnginePredicate` whose `visitor`/`predicate` fields are safe to
/// call and read.
unsafe fn decode_engine_predicate(
    predicate: &mut EnginePredicate,
) -> Result<delta_kernel::expressions::Predicate, String> {
    let mut visitor_state = KernelExpressionVisitorState::default();
    let pred_id = (predicate.visitor)(predicate.predicate, &mut visitor_state);
    unwrap_kernel_predicate(&mut visitor_state, pred_id)
        .ok_or_else(|| "engine predicate visitor returned an invalid expression ID".to_string())
}

/// Resolve a schema-query location string to a URL (absolute URL, else a local file path).
fn resolve_schema_query_url(path: &str) -> Result<Url, String> {
    Url::parse(path).or_else(|_| {
        Url::from_file_path(std::path::Path::new(path))
            .map_err(|_| format!("invalid schema-query location string: {path}"))
    })
}

/// Resolve a `SchemaQuery` (a parquet footer read) via the kernel engine's storage/parquet
/// handlers — a file metadata read, not a compute plan, so it stays kernel-side.
fn footer_schema(engine: &dyn Engine, path: &str) -> Result<SchemaRef, String> {
    let url = resolve_schema_query_url(path)?;
    let meta = engine
        .storage_handler()
        .head(&url)
        .map_err(|e| format!("schema query head {url}: {e}"))?;
    let footer = engine
        .parquet_handler()
        .read_parquet_footer(&meta)
        .map_err(|e| format!("schema query footer {url}: {e}"))?;
    Ok(footer.schema)
}

/// Which kernel SM the handle is currently driving. The snapshot SM resolves protocol+metadata,
/// then the handle transparently builds and switches to the scan SM; `Done` holds the terminal plan.
enum Phase {
    Snapshot(CoroutineSM<Snapshot>),
    Scan(CoroutineSM<ResultPlan>),
    Done(ResultPlan),
    Poisoned,
}

/// Opaque, steppable scan state machine that DuckDB drives. Freed with [`kdf_sm_free`].
pub struct KdfSM {
    engine: Arc<dyn Engine>,
    /// Select the kernel's metadata-only scan SM (file-list terminal) vs the data+metadata SM.
    metadata_only: bool,
    phase: Phase,
    /// Data-skipping predicate supplied by the engine at `open` (visited once there, while the
    /// engine state it borrows is alive). Applied to the scan builder when the snapshot SM finishes.
    /// `None` ⇒ fall back to `predicate_from_env()` (debug escape hatch).
    predicate: Option<Arc<delta_kernel::expressions::Predicate>>,
    /// The pending `Reduce`'s sink, set when `get_step` returns `KDF_STEP_REDUCE`; consumed by the
    /// matching `submit_reduce` to drain the kernel reducer.
    pending_sink: Option<ReduceSink>,
    /// Lowered SQL for the pending `Reduce` (handed to DuckDB via `kdf_sm_reduce_sql`).
    pending_sql: Option<String>,
}

impl KdfSM {
    fn open(
        path: &str,
        version: i64,
        metadata_only: bool,
        predicate: Option<Arc<delta_kernel::expressions::Predicate>>,
    ) -> Result<KdfSM, String> {
        let url = table_url(path)?;
        // Scheme-aware engine: local for file://, S3 for s3:// (AWS_* env creds). The kernel uses
        // it to list the log / read footers; DuckDB does the reduce + data I/O.
        let engine = build_engine_for_url(&url)?;
        let version_opt = if version >= 0 { Some(version as u64) } else { None };
        let snapshot_sm = snapshot_state_machine_for(url, version_opt, engine.as_ref())
            .map_err(|e| format!("build snapshot SM: {e}"))?;
        Ok(KdfSM {
            engine,
            metadata_only,
            phase: Phase::Snapshot(snapshot_sm),
            predicate,
            pending_sink: None,
            pending_sql: None,
        })
    }

    /// Submit an engine response to the SM in the current phase, advancing it. When the snapshot SM
    /// finishes, build and switch to the scan SM (metadata-only or full per the flag); when the scan
    /// SM finishes, capture the terminal `ResultPlan`.
    fn submit_response(&mut self, resp: EngineResponse) -> Result<(), String> {
        self.phase = match std::mem::replace(&mut self.phase, Phase::Poisoned) {
            Phase::Snapshot(mut sm) => match sm.submit(Ok(resp)).map_err(|e| format!("snapshot SM submit: {e}"))? {
                NextStep::Continue => Phase::Snapshot(sm),
                NextStep::Done(snapshot) => {
                    let mut sb = Arc::new(snapshot).scan_builder();
                    // Prefer the engine-supplied predicate (visited at `open`); fall back to the
                    // `DELTA_SM_PREDICATE` env var as a debug escape hatch when none was supplied.
                    if let Some(pred) = self.predicate.clone() {
                        sb = sb.with_predicate(Some(pred));
                    } else if let Some(pred) = predicate_from_env() {
                        sb = sb.with_predicate(Some(Arc::new(pred)));
                    }
                    let scan = sb.build().map_err(|e| format!("build scan: {e}"))?;
                    let scan_sm = if self.metadata_only {
                        scan.scan_metadata_state_machine()
                    } else {
                        scan.scan_state_machine()
                    }
                    .map_err(|e| format!("build scan SM: {e}"))?;
                    Phase::Scan(scan_sm)
                }
            },
            Phase::Scan(mut sm) => match sm.submit(Ok(resp)).map_err(|e| format!("scan SM submit: {e}"))? {
                NextStep::Continue => Phase::Scan(sm),
                NextStep::Done(rp) => Phase::Done(rp),
            },
            Phase::Done(rp) => Phase::Done(rp),
            Phase::Poisoned => return Err("SM poisoned by a prior error".into()),
        };
        Ok(())
    }

    /// Advance the SM until it needs DuckDB to run a `Reduce` (`KDF_STEP_REDUCE`) or is finished
    /// (`KDF_STEP_DONE`). `SchemaQuery`s (footer reads) and zero-yield/terminal transitions are
    /// handled internally, so DuckDB only ever has to execute reduces.
    fn get_step(&mut self) -> Result<i32, String> {
        loop {
            let req = match &mut self.phase {
                Phase::Done(_) => return Ok(KDF_STEP_DONE),
                Phase::Snapshot(sm) => sm.get_step(),
                Phase::Scan(sm) => sm.get_step(),
                Phase::Poisoned => return Err("SM poisoned by a prior error".into()),
            };
            match req {
                Ok(EngineRequest::Reduce { nodes, terminal, sink }) => {
                    self.pending_sql = Some(
                        result_plan_to_sql_until(&Plan { nodes }, terminal)
                            .map_err(|e| format!("lower reduce plan to SQL: {e}"))?,
                    );
                    self.pending_sink = Some(sink);
                    return Ok(KDF_STEP_REDUCE);
                }
                // SchemaQuery is a footer read (storage, not compute) — resolve it kernel-side and
                // keep advancing; DuckDB never sees it.
                Ok(EngineRequest::SchemaQuery(q)) => {
                    let schema = footer_schema(self.engine.as_ref(), &q.file_path)?;
                    self.submit_response(EngineResponse::Schema(schema))?;
                }
                // Zero-yield / terminal: prime the trampoline; submit handles the Done transition.
                Err(_) => self.submit_response(EngineResponse::Empty)?,
            }
        }
    }

    /// Drain the DuckDB-produced Arrow batch for the pending `Reduce` through the kernel reducer and
    /// submit the result, advancing the SM.
    ///
    /// # Safety
    /// `array`/`schema` are valid, freshly-exported Arrow C Data structs (ownership moves in).
    unsafe fn submit_reduce(
        &mut self,
        array: FFI_ArrowArray,
        schema: FFI_ArrowSchema,
    ) -> Result<(), String> {
        let sink = self.pending_sink.take().ok_or("submit_reduce without a pending reduce")?;
        self.pending_sql = None;
        let array_data =
            unsafe { from_ffi(array, &schema) }.map_err(|e| format!("import reduce result: {e}"))?;
        let batch: RecordBatch = StructArray::from(array_data).into();
        let mut handle = sink.new_handle();
        match handle.apply(&ArrowEngineData::new(batch)).map_err(|e| format!("reducer apply: {e}"))? {
            KdfControl::Continue | KdfControl::Break => {}
        }
        self.submit_response(EngineResponse::Reducer(handle.finish()))
    }

    /// Lower the terminal `ResultPlan` to the DuckDB SQL DuckDB executes for the scan (file-list
    /// reconciliation for a metadata-only SM, or the full plan incl. the `delta_load` data read).
    fn result_sql(&self) -> Result<String, String> {
        match &self.phase {
            Phase::Done(rp) => {
                let sql = result_plan_to_sql_until(&rp.plan, rp.result)
                    .map_err(|e| format!("lower result plan to SQL: {e}"))?;
                if std::env::var("KDF_DUMP_SQL").is_ok() {
                    eprintln!("==== KDF result SQL ====\n{sql}\n========================");
                }
                Ok(sql)
            }
            _ => Err("kdf_sm_result_sql: SM not finished (drive get_step/submit until DONE)".into()),
        }
    }
}

/// Open a steppable scan state machine over the Delta table at `path` (`version` < 0 = latest).
/// `metadata_only` selects the kernel's metadata-only scan SM (terminates at the surviving-file
/// list) vs the full data+metadata SM. Returns an owned [`KdfSM`] DuckDB drives, or null on error.
///
/// `predicate`, if non-null, is a data-skipping predicate the engine wants applied to the scan
/// (used to emit the kernel's stats-based file-skip filter). It is visited ONCE here — the engine
/// state it borrows need only outlive this (synchronous) call — and stashed on the handle for the
/// scan builder. When null, the SM falls back to the `DELTA_SM_PREDICATE` env var (debug only).
///
/// # Safety
/// `path_ptr` points to `path_len` valid UTF-8 bytes. `out_err`, if non-null, is writable.
/// `predicate`, if non-null, is a valid [`EnginePredicate`] whose `visitor`/`predicate` fields are
/// safe to call and read for the duration of this call. Free the result once with [`kdf_sm_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_open(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    metadata_only: bool,
    predicate: *mut EnginePredicate,
    out_err: *mut *mut c_char,
) -> *mut KdfSM {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_scan_open: null path pointer") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        // Visit the engine predicate ONCE, here, while the engine state it borrows is alive.
        let predicate_ref = unsafe { predicate.as_mut() };
        let predicate = match predicate_ref {
            Some(p) => {
                let decoded = unsafe { decode_engine_predicate(p) }?;
                Some(Arc::new(decoded))
            }
            None => None,
        };
        KdfSM::open(path, version, metadata_only, predicate)
    }));
    match outcome {
        Ok(Ok(sm)) => Box::into_raw(Box::new(sm)),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_open: panic while opening the scan SM") };
            ptr::null_mut()
        }
    }
}

/// Free a [`KdfSM`].
///
/// # Safety
/// `sm` is null or a pointer from [`kdf_scan_open`], freed exactly once.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_free(sm: *mut KdfSM) {
    if !sm.is_null() {
        drop(unsafe { Box::from_raw(sm) });
    }
}

/// Advance the SM. Returns [`KDF_STEP_REDUCE`] (DuckDB must run the SQL from [`kdf_sm_reduce_sql`]
/// then call [`kdf_sm_submit_reduce`]), [`KDF_STEP_DONE`] (finished — call [`kdf_sm_result_sql`]),
/// or `-1` on error (`*out_err` set).
///
/// # Safety
/// `sm` is a valid [`KdfSM`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_get_step(sm: *mut KdfSM, out_err: *mut *mut c_char) -> i32 {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if sm.is_null() {
        unsafe { write_err(out_err, "kdf_sm_get_step: null handle") };
        return -1;
    }
    let sm = unsafe { &mut *sm };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sm.get_step())) {
        Ok(Ok(kind)) => kind,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_get_step: panic") };
            -1
        }
    }
}

/// The lowered SQL for the pending `Reduce` (valid after `kdf_sm_get_step` returned
/// [`KDF_STEP_REDUCE`]). DuckDB runs it and hands the Arrow result to [`kdf_sm_submit_reduce`].
/// Returns a malloc'd C string (free with `kdf_string_free`), or null on error.
///
/// # Safety
/// `sm` is a valid [`KdfSM`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_reduce_sql(sm: *mut KdfSM, out_err: *mut *mut c_char) -> *mut c_char {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if sm.is_null() {
        unsafe { write_err(out_err, "kdf_sm_reduce_sql: null handle") };
        return ptr::null_mut();
    }
    let sm = unsafe { &*sm };
    match sm.pending_sql.as_deref() {
        Some(s) => match CString::new(s) {
            Ok(c) => c.into_raw(),
            Err(_) => {
                unsafe { write_err(out_err, "kdf_sm_reduce_sql: SQL contains an interior NUL") };
                ptr::null_mut()
            }
        },
        None => {
            unsafe { write_err(out_err, "kdf_sm_reduce_sql: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// Hand the pending `Reduce`'s result back to the kernel as one Arrow C Data batch (moved in); the
/// kernel drains it through its reducer and advances the SM. Returns 0 on success, `-1` on error.
///
/// # Safety
/// `sm` is a valid [`KdfSM`]; `array`/`schema` are valid Arrow C Data structs (ownership moves in);
/// `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_submit_reduce(
    sm: *mut KdfSM,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out_err: *mut *mut c_char,
) -> i32 {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if sm.is_null() || array.is_null() || schema.is_null() {
        unsafe { write_err(out_err, "kdf_sm_submit_reduce: null argument") };
        return -1;
    }
    let sm = unsafe { &mut *sm };
    // Move the Arrow structs out of the caller's storage (replacing with empties).
    let array = std::mem::replace(unsafe { &mut *array }, FFI_ArrowArray::empty());
    let schema = std::mem::replace(unsafe { &mut *schema }, FFI_ArrowSchema::empty());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        sm.submit_reduce(array, schema)
    })) {
        Ok(Ok(())) => 0,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_submit_reduce: panic") };
            -1
        }
    }
}

/// Lower the finished SM's terminal `ResultPlan` to the data-stage DuckDB SQL. Only valid after
/// `kdf_sm_get_step` returned [`KDF_STEP_DONE`]. Returns a malloc'd C string (free with
/// `kdf_string_free`), or null on error.
///
/// # Safety
/// `sm` is a valid [`KdfSM`]; `out_err`, if non-null, writable.
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
    match sm.result_sql() {
        Ok(sql) => match CString::new(sql) {
            Ok(c) => c.into_raw(),
            Err(_) => {
                unsafe { write_err(out_err, "kdf_sm_result_sql: SQL contains an interior NUL") };
                ptr::null_mut()
            }
        },
        Err(msg) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
    }
}
