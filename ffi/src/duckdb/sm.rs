//! State-machine SDK FFI: **DuckDB drives the kernel scan state machines.**
//!
//! The kernel exposes the read path as two steppable state machines the engine drives in sequence,
//! mirroring the kernel's own domain model:
//!
//! 1. [`KdfSnapshot`] — opened with [`kdf_snapshot_open`], driven to a built point-in-time
//!    [`Snapshot`] (protocol + metadata reconciliation). Then [`kdf_snapshot_version`] reports its
//!    version and [`kdf_snapshot_scan`] builds a scan off it.
//! 2. [`KdfScan`] — built from a snapshot with [`kdf_snapshot_scan`], driven to a terminal
//!    [`ResultPlan`] (the scan's file-list reconciliation, or the full data+metadata plan).
//!
//! Both are driven the same way: pull the next step (`*_get_step`); when the kernel needs a Reduce
//! computed, it hands the engine a SQL string (`*_reduce_sql`) or the IR as proto bytes
//! (`*_reduce_plan`); the engine runs it and hands the Arrow result back (`*_submit_reduce`); repeat
//! until the step is `DONE`. A snapshot's terminal is the built `Snapshot` (consumed by
//! `kdf_snapshot_scan`); a scan's terminal is the `ResultPlan` (`kdf_scan_result_sql` /
//! `kdf_scan_result_plan`). **No engine callback is ever passed into the kernel** — the kernel is a
//! passive state machine, the engine drives it.
//!
//! `SchemaQuery`s (parquet footer reads) are storage, not compute, so the handle resolves them
//! kernel-side and keeps advancing; the engine only ever has to execute reduces.
//!
//! # Threading & error contract (applies to every `kdf_snapshot_*` / `kdf_scan_*` export)
//! Each handle wraps `!Send` coroutine SMs; it is a **single-owner cursor**. Never call two entry
//! points for one handle concurrently, and never share a handle across threads without external
//! synchronization — there is no internal locking.
//!
//! Each handle is **single-shot on error**: any export that signals failure (a `-1` / null return
//! with `out_err` set) leaves the handle poisoned. Do not call further entry points on a poisoned
//! handle (they just return the poison error); free it with the matching `*_free`.

use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{RecordBatch, StructArray};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::ir::nodes::ReduceSink;
use delta_kernel::plans::ir::plan::{Plan, ResultPlan};
use delta_kernel::plans::kernel_reducers::{FinishedHandle, KdfControl, ReducerHandle};
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

/// `*_get_step` result: the kernel needs the engine to run a Reduce's plan.
pub const KDF_STEP_REDUCE: i32 = 1;
/// `*_get_step` result: the state machine is finished; fetch its terminal.
pub const KDF_STEP_DONE: i32 = 0;

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

//===----------------------------------------------------------------------===//
// Generic reduce driver
//
// Both handles drive a `CoroutineSM<R>` the same way: advance to the next Reduce (resolving
// SchemaQuery footer reads internally), hand the engine the pending reduce as SQL or proto, take the
// Arrow result back through the kernel reducer, and repeat until the SM yields its terminal `R`. That
// machinery lives here once, parameterized by the terminal type; the two handles differ only in what
// `R` is and what they expose once it's produced.
//===----------------------------------------------------------------------===//

/// The lifecycle of the SM inside a driver: still running, finished (holding its terminal `R`), or
/// poisoned by a prior error. `R: 'static` because `CoroutineSM<R>` requires it.
enum DriverPhase<R: 'static> {
    Running(CoroutineSM<R>),
    Done(R),
    Poisoned,
}

/// Drives one `CoroutineSM<R>` to its terminal `R`, brokering Reduce steps to the engine.
struct ReduceDriver<R: 'static> {
    engine: Arc<dyn Engine>,
    phase: DriverPhase<R>,
    /// The pending `Reduce`'s sink, set when `get_step` returns `KDF_STEP_REDUCE`; consumed by the
    /// matching `submit_reduce` to drain the kernel reducer.
    pending_sink: Option<ReduceSink>,
    /// Lowered SQL for the pending `Reduce` (handed to the engine via `*_reduce_sql`).
    pending_sql: Option<String>,
    /// The pending `Reduce`'s subplan serialized to protobuf bytes (handed to the engine via
    /// `*_reduce_plan`) — the engine-neutral analogue of `pending_sql`. Set alongside it in
    /// `get_step`, cleared in `submit_reduce`. Additive so the SQL and proto forms coexist while the
    /// engine migrates from one to the other.
    pending_reduce_proto: Option<Vec<u8>>,
}

impl<R: 'static> ReduceDriver<R> {
    fn new(engine: Arc<dyn Engine>, sm: CoroutineSM<R>) -> Self {
        ReduceDriver {
            engine,
            phase: DriverPhase::Running(sm),
            pending_sink: None,
            pending_sql: None,
            pending_reduce_proto: None,
        }
    }

    /// Submit an engine response to the SM, advancing it. On terminal, capture `R`.
    fn submit_response(&mut self, resp: EngineResponse) -> Result<(), String> {
        self.phase = match std::mem::replace(&mut self.phase, DriverPhase::Poisoned) {
            DriverPhase::Running(mut sm) => {
                match sm.submit(Ok(resp)).map_err(|e| format!("SM submit: {e}"))? {
                    NextStep::Continue => DriverPhase::Running(sm),
                    NextStep::Done(r) => DriverPhase::Done(r),
                }
            }
            DriverPhase::Done(r) => DriverPhase::Done(r),
            DriverPhase::Poisoned => return Err("SM poisoned by a prior error".into()),
        };
        Ok(())
    }

    /// Advance the SM until it needs the engine to run a `Reduce` (`KDF_STEP_REDUCE`) or is finished
    /// (`KDF_STEP_DONE`). `SchemaQuery`s (footer reads) and zero-yield/terminal transitions are
    /// handled internally, so the engine only ever has to execute reduces.
    fn get_step(&mut self) -> Result<i32, String> {
        loop {
            let req = match &mut self.phase {
                DriverPhase::Done(_) => return Ok(KDF_STEP_DONE),
                DriverPhase::Running(sm) => sm.get_step(),
                DriverPhase::Poisoned => return Err("SM poisoned by a prior error".into()),
            };
            match req {
                Ok(EngineRequest::Reduce { nodes, terminal, sink }) => {
                    // A Reduce subplan has the same shape as a terminal ResultPlan ({plan, result}).
                    // Build it once, then emit BOTH forms: SQL (legacy path) and proto bytes (IR
                    // transport). The engine picks whichever it drives with; they describe the same
                    // computation.
                    let rp = ResultPlan { plan: Plan { nodes }, result: terminal };
                    self.pending_sql = Some(
                        result_plan_to_sql_until(&rp.plan, rp.result)
                            .map_err(|e| format!("lower reduce plan to SQL: {e}"))?,
                    );
                    self.pending_reduce_proto = Some({
                        use prost::Message;
                        super::proto_convert::result_plan_to_proto(&rp)
                            .map_err(|e| format!("serialize reduce plan to proto: {e}"))?
                            .encode_to_vec()
                    });
                    self.pending_sink = Some(sink);
                    return Ok(KDF_STEP_REDUCE);
                }
                // SchemaQuery is a footer read (storage, not compute) — resolve it kernel-side and
                // keep advancing; the engine never sees it.
                Ok(EngineRequest::SchemaQuery(q)) => {
                    let schema = footer_schema(self.engine.as_ref(), &q.file_path)?;
                    self.submit_response(EngineResponse::Schema(schema))?;
                }
                // A `CoroutineSM` returns `Err` from `get_step` only at a zero-yield boundary — it has
                // no pending request, so its terminal result is delivered on the next `submit`. We
                // prime that submit with `Empty` and let it drive the SM to `Done`. If `submit` itself
                // errors, that is a genuine SM failure and propagates via `?` (we do NOT mask it).
                Err(_) => self.submit_response(EngineResponse::Empty)?,
            }
        }
    }

    /// The lowered SQL for the pending `Reduce` (valid after `get_step` returned `KDF_STEP_REDUCE`).
    fn reduce_sql(&self) -> Option<&str> {
        self.pending_sql.as_deref()
    }

    /// The pending `Reduce`'s subplan proto bytes (valid after `get_step` returned `KDF_STEP_REDUCE`).
    fn reduce_plan(&self) -> Option<&[u8]> {
        self.pending_reduce_proto.as_deref()
    }

    /// Drain the engine-produced Arrow batch for the pending `Reduce` through the kernel reducer and
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
        self.pending_reduce_proto = None;
        let array_data =
            unsafe { from_ffi(array, &schema) }.map_err(|e| format!("import reduce result: {e}"))?;
        let batch: RecordBatch = StructArray::from(array_data).into();
        let mut handle = sink.new_handle();
        // Contract: the engine runs the whole Reduce query and hands back its result as exactly ONE
        // Arrow batch, so we apply once and finish. `KdfControl::Break` ("stop pulling input") is
        // therefore a no-op here — there is no further input to withhold. If this ever fed a reducer
        // more than one batch per Reduce, Break would have to short-circuit before applying the rest.
        match handle.apply(&ArrowEngineData::new(batch)).map_err(|e| format!("reducer apply: {e}"))? {
            KdfControl::Continue | KdfControl::Break => {}
        }
        self.submit_response(EngineResponse::Reducer(handle.finish()))
    }

    /// Borrow the finished terminal, or `None` if the SM has not reached `Done`.
    fn terminal(&self) -> Option<&R> {
        match &self.phase {
            DriverPhase::Done(r) => Some(r),
            _ => None,
        }
    }

    /// Take the finished terminal, leaving the driver poisoned. `Err` if not finished.
    fn take_terminal(&mut self) -> Result<R, String> {
        match std::mem::replace(&mut self.phase, DriverPhase::Poisoned) {
            DriverPhase::Done(r) => Ok(r),
            other => {
                self.phase = other;
                Err("SM not finished (drive get_step/submit until DONE)".into())
            }
        }
    }
}

//===----------------------------------------------------------------------===//
// KdfSnapshot — drive the snapshot SM, then hold the built Snapshot
//===----------------------------------------------------------------------===//

/// Opaque, steppable snapshot state machine that DuckDB drives to a built [`Snapshot`], then holds
/// that snapshot (so scans can be built off it). Freed with [`kdf_snapshot_free`].
pub struct KdfSnapshot {
    driver: ReduceDriver<Snapshot>,
    /// The built snapshot, moved out of the driver once its SM reaches `Done`. `Arc` so
    /// `scan_builder(self: Arc<Self>)` can build one or more scans without consuming it.
    snapshot: Option<Arc<Snapshot>>,
}

impl KdfSnapshot {
    fn open(path: &str, version: i64) -> Result<KdfSnapshot, String> {
        let url = table_url(path)?;
        // Scheme-aware engine: local for file://, S3 for s3:// (AWS_* env creds). The kernel uses it
        // to list the log / read footers; the engine does the reduce + data I/O.
        let engine = build_engine_for_url(&url)?;
        let version_opt = if version >= 0 { Some(version as u64) } else { None };
        let snapshot_sm = snapshot_state_machine_for(url, version_opt, engine.as_ref())
            .map_err(|e| format!("build snapshot SM: {e}"))?;
        Ok(KdfSnapshot { driver: ReduceDriver::new(engine, snapshot_sm), snapshot: None })
    }

    /// The built snapshot, materializing it out of the driver on first access. `Err` if the SM has
    /// not been driven to `Done`.
    fn snapshot(&mut self) -> Result<Arc<Snapshot>, String> {
        if self.snapshot.is_none() {
            let snap = self.driver.take_terminal()?;
            self.snapshot = Some(Arc::new(snap));
        }
        Ok(self.snapshot.clone().expect("snapshot set above"))
    }

    /// Build a scan off the finished snapshot (its data+metadata or metadata-only SM), applying an
    /// optional data-skipping predicate. `Err` if the snapshot SM has not finished.
    fn build_scan(
        &mut self,
        metadata_only: bool,
        predicate: Option<Arc<delta_kernel::expressions::Predicate>>,
    ) -> Result<KdfScan, String> {
        let snapshot = self.snapshot()?;
        let engine = self.driver.engine.clone();
        let mut sb = snapshot.scan_builder();
        if let Some(pred) = predicate {
            sb = sb.with_predicate(Some(pred));
        }
        let scan = sb.build().map_err(|e| format!("build scan: {e}"))?;
        let scan_sm = if metadata_only {
            scan.scan_metadata_state_machine()
        } else {
            scan.scan_state_machine()
        }
        .map_err(|e| format!("build scan SM: {e}"))?;
        Ok(KdfScan { driver: ReduceDriver::new(engine, scan_sm) })
    }
}

//===----------------------------------------------------------------------===//
// KdfScan — drive the scan SM to a terminal ResultPlan
//===----------------------------------------------------------------------===//

/// Opaque, steppable scan state machine that DuckDB drives to a terminal [`ResultPlan`]. Built from a
/// [`KdfSnapshot`] via [`kdf_snapshot_scan`]; freed with [`kdf_scan_free`].
pub struct KdfScan {
    driver: ReduceDriver<ResultPlan>,
}

impl KdfScan {
    /// Lower the terminal `ResultPlan` to the DuckDB SQL the engine executes for the scan (file-list
    /// reconciliation for a metadata-only SM, or the full plan incl. the `delta_load` data read).
    fn result_sql(&self) -> Result<String, String> {
        let rp = self.driver.terminal().ok_or(
            "kdf_scan_result_sql: SM not finished (drive get_step/submit until DONE)".to_string(),
        )?;
        result_plan_to_sql_until(&rp.plan, rp.result).map_err(|e| format!("lower result plan to SQL: {e}"))
    }

    /// Serialize the terminal `ResultPlan` (the SSA IR DAG) to protobuf bytes — the engine-neutral
    /// plan transport. The engine decodes these into the kernel-generated proto structs and lowers
    /// them itself; the kernel emits no engine dialect.
    fn result_plan_proto(&self) -> Result<Vec<u8>, String> {
        use prost::Message;
        let rp = self.driver.terminal().ok_or(
            "kdf_scan_result_plan: SM not finished (drive get_step/submit until DONE)".to_string(),
        )?;
        let proto = super::proto_convert::result_plan_to_proto(rp)
            .map_err(|e| format!("serialize result plan to proto: {e}"))?;
        Ok(proto.encode_to_vec())
    }
}

//===----------------------------------------------------------------------===//
// Shared FFI helpers
//===----------------------------------------------------------------------===//

/// Reset `out_err`/`out_len` slots to their empty state at the start of an export.
macro_rules! init_out {
    ($out_err:expr) => {
        if !$out_err.is_null() {
            unsafe { *$out_err = ptr::null_mut() };
        }
    };
    ($out_err:expr, $out_len:expr) => {
        init_out!($out_err);
        if !$out_len.is_null() {
            unsafe { *$out_len = 0 };
        }
    };
}

/// Move a `Vec<u8>` out to a malloc'd `(ptr, len)` the caller frees with [`kdf_bytes_free`].
fn leak_bytes(mut buf: Vec<u8>, out_len: *mut usize) -> *mut u8 {
    buf.shrink_to_fit();
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    unsafe { *out_len = len };
    ptr
}

/// Turn an owned `String` into a malloc'd C string, or null + `out_err` if it has an interior NUL.
fn leak_cstring(s: &str, what: &str, out_err: *mut *mut c_char) -> *mut c_char {
    match CString::new(s) {
        Ok(c) => c.into_raw(),
        Err(_) => {
            unsafe { write_err(out_err, &format!("{what}: string contains an interior NUL")) };
            ptr::null_mut()
        }
    }
}

//===----------------------------------------------------------------------===//
// KdfSnapshot FFI
//===----------------------------------------------------------------------===//

/// Open a steppable snapshot state machine over the Delta table at `path` (`version` < 0 = latest).
/// Returns an owned [`KdfSnapshot`] DuckDB drives to a built snapshot, or null on error.
///
/// # Safety
/// `path_ptr` points to `path_len` valid UTF-8 bytes. `out_err`, if non-null, is writable. Free the
/// result once with [`kdf_snapshot_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_open(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    out_err: *mut *mut c_char,
) -> *mut KdfSnapshot {
    init_out!(out_err);
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_open: null path pointer") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        KdfSnapshot::open(path, version)
    }));
    match outcome {
        Ok(Ok(snap)) => Box::into_raw(Box::new(snap)),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_snapshot_open: panic while opening the snapshot SM") };
            ptr::null_mut()
        }
    }
}

/// Free a [`KdfSnapshot`].
///
/// # Safety
/// `snap` is null or a pointer from [`kdf_snapshot_open`], freed exactly once.
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_free(snap: *mut KdfSnapshot) {
    if !snap.is_null() {
        drop(unsafe { Box::from_raw(snap) });
    }
}

/// Advance the snapshot SM to the next Reduce (`KDF_STEP_REDUCE`) or to `KDF_STEP_DONE`. Returns
/// `-1` on error (with `out_err` set).
///
/// # Safety
/// `snap` is a valid [`KdfSnapshot`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_get_step(snap: *mut KdfSnapshot, out_err: *mut *mut c_char) -> i32 {
    init_out!(out_err);
    if snap.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_get_step: null handle") };
        return -1;
    }
    let snap = unsafe { &mut *snap };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| snap.driver.get_step())) {
        Ok(Ok(kind)) => kind,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_snapshot_get_step: panic") };
            -1
        }
    }
}

/// The pending snapshot `Reduce` lowered to DuckDB SQL (valid after `kdf_snapshot_get_step` returned
/// [`KDF_STEP_REDUCE`]). Malloc'd C string (free with `kdf_string_free`), or null on error.
///
/// # Safety
/// `snap` is a valid [`KdfSnapshot`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_reduce_sql(
    snap: *mut KdfSnapshot,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    init_out!(out_err);
    if snap.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_reduce_sql: null handle") };
        return ptr::null_mut();
    }
    match unsafe { &*snap }.driver.reduce_sql() {
        Some(s) => leak_cstring(s, "kdf_snapshot_reduce_sql", out_err),
        None => {
            unsafe { write_err(out_err, "kdf_snapshot_reduce_sql: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// The pending snapshot `Reduce`'s subplan as proto bytes (IR transport). Writes the byte length to
/// `*out_len`; returns a malloc'd buffer freed with [`kdf_bytes_free`], or null on error.
///
/// # Safety
/// `snap` is a valid [`KdfSnapshot`]; `out_len` and `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_reduce_plan(
    snap: *mut KdfSnapshot,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    init_out!(out_err, out_len);
    if snap.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_reduce_plan: null pointer argument") };
        return ptr::null_mut();
    }
    match unsafe { &*snap }.driver.reduce_plan() {
        Some(buf) => leak_bytes(buf.to_vec(), out_len),
        None => {
            unsafe { write_err(out_err, "kdf_snapshot_reduce_plan: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// Hand the pending snapshot `Reduce`'s result back as one Arrow C Data batch (ownership moves in;
/// the structs are emptied). Returns 0 on success, `-1` on error.
///
/// # Safety
/// `snap` is a valid [`KdfSnapshot`]; `array`/`schema` are valid Arrow C Data structs (ownership
/// moves in); `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_submit_reduce(
    snap: *mut KdfSnapshot,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out_err: *mut *mut c_char,
) -> i32 {
    init_out!(out_err);
    if snap.is_null() || array.is_null() || schema.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_submit_reduce: null argument") };
        return -1;
    }
    let snap = unsafe { &mut *snap };
    let array = std::mem::replace(unsafe { &mut *array }, FFI_ArrowArray::empty());
    let schema = std::mem::replace(unsafe { &mut *schema }, FFI_ArrowSchema::empty());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        snap.driver.submit_reduce(array, schema)
    })) {
        Ok(Ok(())) => 0,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_snapshot_submit_reduce: panic") };
            -1
        }
    }
}

/// The finished snapshot's version. Returns `-1` on error (incl. SM not finished), with `out_err` set.
///
/// # Safety
/// `snap` is a valid [`KdfSnapshot`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_version(snap: *mut KdfSnapshot, out_err: *mut *mut c_char) -> i64 {
    init_out!(out_err);
    if snap.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_version: null handle") };
        return -1;
    }
    let snap = unsafe { &mut *snap };
    match snap.snapshot() {
        Ok(s) => s.version() as i64,
        Err(msg) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
    }
}

/// Build a scan off the finished snapshot. `metadata_only` selects the file-list-terminal SM.
/// `predicate`, if non-null, is a data-skipping [`EnginePredicate`] visited ONCE here (its borrowed
/// engine state need only outlive this call). Returns an owned [`KdfScan`] the engine drives, or null
/// on error. The snapshot handle is unchanged and can build further scans.
///
/// # Safety
/// `snap` is a valid, finished [`KdfSnapshot`]; `predicate`, if non-null, is a valid
/// [`EnginePredicate`] safe to call/read for this call; `out_err`, if non-null, writable. Free the
/// result once with [`kdf_scan_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_snapshot_scan(
    snap: *mut KdfSnapshot,
    metadata_only: bool,
    predicate: *mut EnginePredicate,
    out_err: *mut *mut c_char,
) -> *mut KdfScan {
    init_out!(out_err);
    if snap.is_null() {
        unsafe { write_err(out_err, "kdf_snapshot_scan: null handle") };
        return ptr::null_mut();
    }
    let snap = unsafe { &mut *snap };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Visit the engine predicate ONCE, here, while the engine state it borrows is alive.
        let predicate = match unsafe { predicate.as_mut() } {
            Some(p) => Some(Arc::new(unsafe { decode_engine_predicate(p) }?)),
            None => None,
        };
        snap.build_scan(metadata_only, predicate)
    }));
    match outcome {
        Ok(Ok(scan)) => Box::into_raw(Box::new(scan)),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_snapshot_scan: panic while building the scan") };
            ptr::null_mut()
        }
    }
}

//===----------------------------------------------------------------------===//
// KdfScan FFI
//===----------------------------------------------------------------------===//

/// Free a [`KdfScan`].
///
/// # Safety
/// `scan` is null or a pointer from [`kdf_snapshot_scan`], freed exactly once.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_free(scan: *mut KdfScan) {
    if !scan.is_null() {
        drop(unsafe { Box::from_raw(scan) });
    }
}

/// Advance the scan SM to the next Reduce (`KDF_STEP_REDUCE`) or to `KDF_STEP_DONE`. Returns `-1` on
/// error (with `out_err` set).
///
/// # Safety
/// `scan` is a valid [`KdfScan`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_get_step(scan: *mut KdfScan, out_err: *mut *mut c_char) -> i32 {
    init_out!(out_err);
    if scan.is_null() {
        unsafe { write_err(out_err, "kdf_scan_get_step: null handle") };
        return -1;
    }
    let scan = unsafe { &mut *scan };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scan.driver.get_step())) {
        Ok(Ok(kind)) => kind,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_get_step: panic") };
            -1
        }
    }
}

/// The pending scan `Reduce` lowered to DuckDB SQL (valid after `kdf_scan_get_step` returned
/// [`KDF_STEP_REDUCE`]). Malloc'd C string (free with `kdf_string_free`), or null on error.
///
/// # Safety
/// `scan` is a valid [`KdfScan`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_reduce_sql(scan: *mut KdfScan, out_err: *mut *mut c_char) -> *mut c_char {
    init_out!(out_err);
    if scan.is_null() {
        unsafe { write_err(out_err, "kdf_scan_reduce_sql: null handle") };
        return ptr::null_mut();
    }
    match unsafe { &*scan }.driver.reduce_sql() {
        Some(s) => leak_cstring(s, "kdf_scan_reduce_sql", out_err),
        None => {
            unsafe { write_err(out_err, "kdf_scan_reduce_sql: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// The pending scan `Reduce`'s subplan as proto bytes (IR transport). Writes the byte length to
/// `*out_len`; returns a malloc'd buffer freed with [`kdf_bytes_free`], or null on error.
///
/// # Safety
/// `scan` is a valid [`KdfScan`]; `out_len` and `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_reduce_plan(
    scan: *mut KdfScan,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    init_out!(out_err, out_len);
    if scan.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "kdf_scan_reduce_plan: null pointer argument") };
        return ptr::null_mut();
    }
    match unsafe { &*scan }.driver.reduce_plan() {
        Some(buf) => leak_bytes(buf.to_vec(), out_len),
        None => {
            unsafe { write_err(out_err, "kdf_scan_reduce_plan: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// Hand the pending scan `Reduce`'s result back as one Arrow C Data batch (ownership moves in; the
/// structs are emptied). Returns 0 on success, `-1` on error.
///
/// # Safety
/// `scan` is a valid [`KdfScan`]; `array`/`schema` are valid Arrow C Data structs (ownership moves
/// in); `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_submit_reduce(
    scan: *mut KdfScan,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out_err: *mut *mut c_char,
) -> i32 {
    init_out!(out_err);
    if scan.is_null() || array.is_null() || schema.is_null() {
        unsafe { write_err(out_err, "kdf_scan_submit_reduce: null argument") };
        return -1;
    }
    let scan = unsafe { &mut *scan };
    let array = std::mem::replace(unsafe { &mut *array }, FFI_ArrowArray::empty());
    let schema = std::mem::replace(unsafe { &mut *schema }, FFI_ArrowSchema::empty());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        scan.driver.submit_reduce(array, schema)
    })) {
        Ok(Ok(())) => 0,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_submit_reduce: panic") };
            -1
        }
    }
}

/// Lower the finished scan's terminal `ResultPlan` to DuckDB SQL. Only valid after
/// `kdf_scan_get_step` returned [`KDF_STEP_DONE`]. Malloc'd C string (free with `kdf_string_free`),
/// or null on error.
///
/// # Safety
/// `scan` is a valid [`KdfScan`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_result_sql(scan: *mut KdfScan, out_err: *mut *mut c_char) -> *mut c_char {
    init_out!(out_err);
    if scan.is_null() {
        unsafe { write_err(out_err, "kdf_scan_result_sql: null handle") };
        return ptr::null_mut();
    }
    match unsafe { &*scan }.result_sql() {
        Ok(sql) => leak_cstring(&sql, "kdf_scan_result_sql", out_err),
        Err(msg) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
    }
}

/// Serialize the finished scan's terminal `ResultPlan` to protobuf bytes (the engine-neutral plan IR
/// transport). Only valid after `kdf_scan_get_step` returned [`KDF_STEP_DONE`]. Writes the byte
/// length to `*out_len`; returns a malloc'd buffer freed with [`kdf_bytes_free`], or null on error.
///
/// # Safety
/// `scan` is a valid [`KdfScan`]; `out_len` and `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_result_plan(
    scan: *mut KdfScan,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    init_out!(out_err, out_len);
    if scan.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "kdf_scan_result_plan: null pointer argument") };
        return ptr::null_mut();
    }
    match unsafe { &*scan }.result_plan_proto() {
        Ok(buf) => leak_bytes(buf, out_len),
        Err(msg) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
    }
}

/// Free a byte buffer returned by a `*_reduce_plan` / `*_result_plan` emitter.
///
/// # Safety
/// `ptr`/`len` are exactly what such an emitter returned (or `ptr` is null). Call at most once.
#[no_mangle]
pub unsafe extern "C" fn kdf_bytes_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe { drop(Vec::from_raw_parts(ptr, len, len)) };
    }
}

//===============================================================================================
// delta_* — the proto-only SDK ABI (Phase B of SDK_IMPLEMENTATION_PLAN.md)
//
// The engine-neutral surface the C++ SDK (delta_kernel_sdk.hpp) sits on. Additive: it reuses the
// existing KdfSnapshot/KdfScan internals (driver + snapshot-holding + build_scan) unchanged, and
// coexists with the legacy `kdf_*`/`_sql` exports above until Stage 4 deletes them.
//
// One opaque state-machine handle (`DeltaSm`, a runtime-tagged enum recovering the type C erases),
// value handles (`DeltaSnapshot` = Arc<Snapshot>, `DeltaScan` = ResultPlan), and the reducer handles.
// `next`/`submit` are terminal-agnostic (2-arm forward); `build_*` are terminal-specific and assert
// the arm. Proto-only: `_plan` bytes, no `_sql`. Errors via null-return + `out_err` string (the SDK
// premise is `open(path, version)` with no engine handle, so we keep the self-contained out_err
// idiom rather than ExternResult, which would need an engine param for its allocator).
//===============================================================================================

/// `delta_sm_next` result: the kernel needs the engine to run a Reduce's plan.
pub const DELTA_STEP_REDUCE: i32 = 1;
/// `delta_sm_next` result: the state machine is finished; call the matching `delta_sm_build_*`.
pub const DELTA_STEP_DONE: i32 = 0;

/// The uniform opaque state-machine handle. A runtime tag recovering the terminal type the C ABI
/// erases — the arms wrap the existing per-terminal handles (no logic duplication, no `dyn`, no
/// terminal-mapping union). `next`/`submit` forward to the shared `ReduceDriver` on either arm;
/// `build_snapshot`/`build_scan` consume the enum and assert the matching arm.
pub enum DeltaSm {
    Snapshot(KdfSnapshot),
    Scan(KdfScan),
}

impl DeltaSm {
    fn driver_get_step(&mut self) -> Result<i32, String> {
        match self {
            DeltaSm::Snapshot(s) => s.driver.get_step(),
            DeltaSm::Scan(s) => s.driver.get_step(),
        }
    }
    fn driver_reduce_plan(&self) -> Option<&[u8]> {
        match self {
            DeltaSm::Snapshot(s) => s.driver.reduce_plan(),
            DeltaSm::Scan(s) => s.driver.reduce_plan(),
        }
    }
    fn driver_take_sink(&mut self) -> Option<ReduceSink> {
        match self {
            DeltaSm::Snapshot(s) => s.driver.pending_sink.take(),
            DeltaSm::Scan(s) => s.driver.pending_sink.take(),
        }
    }
    /// Submit an already-finished reducer handle back to the SM, advancing it.
    fn driver_submit_finished(&mut self, finished: FinishedHandle) -> Result<(), String> {
        match self {
            DeltaSm::Snapshot(s) => s.driver.submit_response(EngineResponse::Reducer(finished)),
            DeltaSm::Scan(s) => s.driver.submit_response(EngineResponse::Reducer(finished)),
        }
    }
}

/// A finished reducer handle, moved out of the SM by the engine (via `delta_sm_take_reducer`), fed
/// Arrow batches (`delta_reducer_apply`), finished (`delta_reducer_finish`), and handed back
/// (`delta_sm_submit`). Wraps the kernel [`ReducerHandle`] and the finished [`FinishedHandle`].
pub struct DeltaReducer {
    handle: Option<ReducerHandle>,
}
/// The finished reducer produced by `delta_reducer_finish`; consumed by `delta_sm_submit`.
pub struct DeltaFinishedReducer {
    finished: FinishedHandle,
}

/// A built snapshot value (inert): version + schema, and a scan-SM factory.
pub struct DeltaSnapshotValue {
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
}
/// A built scan value (inert): the terminal `ResultPlan`, lowered to proto on demand.
pub struct DeltaScanValue {
    plan: ResultPlan,
}

// ── entry ──────────────────────────────────────────────────────────────────────────────────

/// Open a steppable snapshot state machine over the Delta table at `path` (`version` < 0 = latest).
/// Returns an owned [`DeltaSm`] (Snapshot arm) the engine drives, or null on error.
///
/// # Safety
/// `path_ptr` points to `path_len` valid UTF-8 bytes. `out_err`, if non-null, is writable. Free the
/// result once with [`delta_sm_free`].
#[no_mangle]
pub unsafe extern "C" fn delta_open_snapshot(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    out_err: *mut *mut c_char,
) -> *mut DeltaSm {
    init_out!(out_err);
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "delta_open_snapshot: null path pointer") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        KdfSnapshot::open(path, version)
    }));
    match outcome {
        Ok(Ok(snap)) => Box::into_raw(Box::new(DeltaSm::Snapshot(snap))),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "delta_open_snapshot: panic while opening the snapshot SM") };
            ptr::null_mut()
        }
    }
}

// ── state machine: next / reduce_plan / take_reducer / submit / submit_error / build / free ──

/// Advance the SM to the next Reduce ([`DELTA_STEP_REDUCE`]) or to [`DELTA_STEP_DONE`]. Returns `-1`
/// on error (with `out_err` set).
///
/// # Safety
/// `sm` is a valid [`DeltaSm`]; `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_sm_next(sm: *mut DeltaSm, out_err: *mut *mut c_char) -> i32 {
    init_out!(out_err);
    if sm.is_null() {
        unsafe { write_err(out_err, "delta_sm_next: null handle") };
        return -1;
    }
    let sm = unsafe { &mut *sm };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sm.driver_get_step())) {
        Ok(Ok(kind)) => kind,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "delta_sm_next: panic") };
            -1
        }
    }
}

/// The pending Reduce's subplan as proto bytes (valid after `delta_sm_next` returned
/// [`DELTA_STEP_REDUCE`]). Writes the byte length to `*out_len`; returns a malloc'd buffer freed with
/// [`delta_bytes_free`], or null on error.
///
/// # Safety
/// `sm` is a valid [`DeltaSm`]; `out_len` and `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_sm_reduce_plan(
    sm: *mut DeltaSm,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    init_out!(out_err, out_len);
    if sm.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "delta_sm_reduce_plan: null pointer argument") };
        return ptr::null_mut();
    }
    match unsafe { &*sm }.driver_reduce_plan() {
        Some(buf) => leak_bytes(buf.to_vec(), out_len),
        None => {
            unsafe { write_err(out_err, "delta_sm_reduce_plan: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// Move the pending Reduce's reducer out of the SM (valid after `delta_sm_next` returned
/// [`DELTA_STEP_REDUCE`]). The engine owns driving it (`delta_reducer_apply`/`_finish`) and hands the
/// finished reducer back via [`delta_sm_submit`]. Returns an owned [`DeltaReducer`], or null on error.
///
/// # Safety
/// `sm` is a valid [`DeltaSm`]; `out_err`, if non-null, writable. Free the result with
/// [`delta_reducer_free`] (or consume it via `delta_reducer_finish` + `delta_sm_submit`).
#[no_mangle]
pub unsafe extern "C" fn delta_sm_take_reducer(
    sm: *mut DeltaSm,
    out_err: *mut *mut c_char,
) -> *mut DeltaReducer {
    init_out!(out_err);
    if sm.is_null() {
        unsafe { write_err(out_err, "delta_sm_take_reducer: null handle") };
        return ptr::null_mut();
    }
    let sm = unsafe { &mut *sm };
    match sm.driver_take_sink() {
        Some(sink) => Box::into_raw(Box::new(DeltaReducer { handle: Some(sink.new_handle()) })),
        None => {
            unsafe { write_err(out_err, "delta_sm_take_reducer: no pending reduce") };
            ptr::null_mut()
        }
    }
}

/// Hand a finished reducer (from [`delta_reducer_finish`]) back to the SM, advancing it. Consumes
/// `finished`. Returns 0 on success, `-1` on error.
///
/// # Safety
/// `sm` is a valid [`DeltaSm`]; `finished` is a valid [`DeltaFinishedReducer`] (consumed here);
/// `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_sm_submit(
    sm: *mut DeltaSm,
    finished: *mut DeltaFinishedReducer,
    out_err: *mut *mut c_char,
) -> i32 {
    init_out!(out_err);
    if sm.is_null() || finished.is_null() {
        unsafe { write_err(out_err, "delta_sm_submit: null argument") };
        return -1;
    }
    let sm = unsafe { &mut *sm };
    let finished = unsafe { Box::from_raw(finished) }.finished;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sm.driver_submit_finished(finished))) {
        Ok(Ok(())) => 0,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "delta_sm_submit: panic") };
            -1
        }
    }
}

/// Report to the kernel that the engine FAILED to execute the pending Reduce's plan (the
/// engine→kernel error direction). `msg_ptr`/`msg_len` is the engine's error message. The SM is
/// poisoned; the failure surfaces on the next `delta_sm_*` call. Returns 0 on success, `-1` on error.
///
/// # Safety
/// `sm` is a valid [`DeltaSm`]; `msg_ptr` points to `msg_len` valid UTF-8 bytes (or is null);
/// `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_sm_submit_error(
    sm: *mut DeltaSm,
    msg_ptr: *const c_char,
    msg_len: usize,
    out_err: *mut *mut c_char,
) -> i32 {
    init_out!(out_err);
    if sm.is_null() {
        unsafe { write_err(out_err, "delta_sm_submit_error: null handle") };
        return -1;
    }
    let sm = unsafe { &mut *sm };
    let msg = if msg_ptr.is_null() {
        "engine failed to execute reduce plan".to_string()
    } else {
        let bytes = unsafe { std::slice::from_raw_parts(msg_ptr as *const u8, msg_len) };
        String::from_utf8_lossy(bytes).into_owned()
    };
    // Poison the SM so subsequent calls surface the engine failure. (The kernel's typed
    // EngineError path is richer; this MVP records the message and stops the drive.)
    match sm {
        DeltaSm::Snapshot(s) => s.driver.phase = DriverPhase::Poisoned,
        DeltaSm::Scan(s) => s.driver.phase = DriverPhase::Poisoned,
    }
    unsafe { write_err(out_err, &format!("engine reduce error: {msg}")) };
    // Return -1: the reduce could not be satisfied. The engine already knows; out_err echoes it.
    let _ = msg;
    -1
}

/// Consume a finished snapshot SM and return the built [`DeltaSnapshotValue`]. Errors (`null`) if the
/// SM is not a snapshot SM, or has not been driven to [`DELTA_STEP_DONE`].
///
/// # Safety
/// `sm` is a valid [`DeltaSm`] (consumed here); `out_err`, if non-null, writable. Free the result
/// with [`delta_snapshot_free`].
#[no_mangle]
pub unsafe extern "C" fn delta_sm_build_snapshot(
    sm: *mut DeltaSm,
    out_err: *mut *mut c_char,
) -> *mut DeltaSnapshotValue {
    init_out!(out_err);
    if sm.is_null() {
        unsafe { write_err(out_err, "delta_sm_build_snapshot: null handle") };
        return ptr::null_mut();
    }
    let boxed = unsafe { Box::from_raw(sm) };
    match *boxed {
        DeltaSm::Snapshot(mut s) => {
            let engine = s.driver.engine.clone();
            match s.snapshot() {
                Ok(snapshot) => Box::into_raw(Box::new(DeltaSnapshotValue { snapshot, engine })),
                Err(msg) => {
                    unsafe { write_err(out_err, &msg) };
                    ptr::null_mut()
                }
            }
        }
        DeltaSm::Scan(_) => {
            unsafe { write_err(out_err, "delta_sm_build_snapshot: state machine is a scan, not a snapshot") };
            ptr::null_mut()
        }
    }
}

/// Consume a finished scan SM and return the built [`DeltaScanValue`]. Errors (`null`) if the SM is
/// not a scan SM, or has not been driven to [`DELTA_STEP_DONE`].
///
/// # Safety
/// `sm` is a valid [`DeltaSm`] (consumed here); `out_err`, if non-null, writable. Free the result
/// with [`delta_scan_free`].
#[no_mangle]
pub unsafe extern "C" fn delta_sm_build_scan(
    sm: *mut DeltaSm,
    out_err: *mut *mut c_char,
) -> *mut DeltaScanValue {
    init_out!(out_err);
    if sm.is_null() {
        unsafe { write_err(out_err, "delta_sm_build_scan: null handle") };
        return ptr::null_mut();
    }
    let boxed = unsafe { Box::from_raw(sm) };
    match *boxed {
        DeltaSm::Scan(mut s) => match s.driver.take_terminal() {
            Ok(plan) => Box::into_raw(Box::new(DeltaScanValue { plan })),
            Err(msg) => {
                unsafe { write_err(out_err, &msg) };
                ptr::null_mut()
            }
        },
        DeltaSm::Snapshot(_) => {
            unsafe { write_err(out_err, "delta_sm_build_scan: state machine is a snapshot, not a scan") };
            ptr::null_mut()
        }
    }
}

/// Free a [`DeltaSm`].
///
/// # Safety
/// `sm` is null or a pointer from [`delta_open_snapshot`]/[`delta_snapshot_scan_sm`], freed once.
#[no_mangle]
pub unsafe extern "C" fn delta_sm_free(sm: *mut DeltaSm) {
    if !sm.is_null() {
        drop(unsafe { Box::from_raw(sm) });
    }
}

// ── reducer: apply / finish / free ───────────────────────────────────────────────────────────

/// Feed one Arrow C Data batch to the reducer (ownership moves in; the structs are emptied). Returns
/// 0 for `Continue` (feed more), 1 for `Break` (reducer wants no more input), `-1` on error.
///
/// # Safety
/// `reducer` is a valid [`DeltaReducer`]; `array`/`schema` are valid Arrow C Data structs (ownership
/// moves in); `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_reducer_apply(
    reducer: *mut DeltaReducer,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out_err: *mut *mut c_char,
) -> i32 {
    init_out!(out_err);
    if reducer.is_null() || array.is_null() || schema.is_null() {
        unsafe { write_err(out_err, "delta_reducer_apply: null argument") };
        return -1;
    }
    let reducer = unsafe { &mut *reducer };
    let array = std::mem::replace(unsafe { &mut *array }, FFI_ArrowArray::empty());
    let schema = std::mem::replace(unsafe { &mut *schema }, FFI_ArrowSchema::empty());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let handle = reducer
            .handle
            .as_mut()
            .ok_or("delta_reducer_apply: reducer already finished")?;
        let array_data =
            unsafe { from_ffi(array, &schema) }.map_err(|e| format!("import reduce batch: {e}"))?;
        let batch: RecordBatch = StructArray::from(array_data).into();
        handle
            .apply(&ArrowEngineData::new(batch))
            .map_err(|e| format!("reducer apply: {e}"))
    }));
    match outcome {
        Ok(Ok(KdfControl::Continue)) => 0,
        Ok(Ok(KdfControl::Break)) => 1,
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "delta_reducer_apply: panic") };
            -1
        }
    }
}

/// Finish the reducer, consuming it and producing a [`DeltaFinishedReducer`] to hand back via
/// [`delta_sm_submit`]. Returns null on error.
///
/// # Safety
/// `reducer` is a valid [`DeltaReducer`] (consumed here); `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_reducer_finish(
    reducer: *mut DeltaReducer,
    out_err: *mut *mut c_char,
) -> *mut DeltaFinishedReducer {
    init_out!(out_err);
    if reducer.is_null() {
        unsafe { write_err(out_err, "delta_reducer_finish: null handle") };
        return ptr::null_mut();
    }
    let mut reducer = unsafe { Box::from_raw(reducer) };
    match reducer.handle.take() {
        Some(handle) => Box::into_raw(Box::new(DeltaFinishedReducer { finished: handle.finish() })),
        None => {
            unsafe { write_err(out_err, "delta_reducer_finish: reducer already finished") };
            ptr::null_mut()
        }
    }
}

/// Free a [`DeltaReducer`] without finishing it (e.g. on the error path).
///
/// # Safety
/// `reducer` is null or a pointer from [`delta_sm_take_reducer`] not yet consumed by
/// [`delta_reducer_finish`], freed once.
#[no_mangle]
pub unsafe extern "C" fn delta_reducer_free(reducer: *mut DeltaReducer) {
    if !reducer.is_null() {
        drop(unsafe { Box::from_raw(reducer) });
    }
}

// ── snapshot value: version / schema / scan_sm / free ─────────────────────────────────────────

/// The finished snapshot's version.
///
/// # Safety
/// `snap` is a valid [`DeltaSnapshotValue`].
#[no_mangle]
pub unsafe extern "C" fn delta_snapshot_version(snap: *mut DeltaSnapshotValue) -> i64 {
    if snap.is_null() {
        return -1;
    }
    unsafe { &*snap }.snapshot.version() as i64
}

/// The finished snapshot's logical schema as proto bytes. Writes the byte length to `*out_len`;
/// returns a malloc'd buffer freed with [`delta_bytes_free`], or null on error.
///
/// # Safety
/// `snap` is a valid [`DeltaSnapshotValue`]; `out_len` and `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_snapshot_schema(
    snap: *mut DeltaSnapshotValue,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    init_out!(out_err, out_len);
    if snap.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "delta_snapshot_schema: null pointer argument") };
        return ptr::null_mut();
    }
    let snap = unsafe { &*snap };
    match super::proto_convert::schema_to_proto(&snap.snapshot.schema()) {
        // NOTE: schema() returns SchemaRef (Arc<StructType>); &... derefs to &StructType via the
        // Deref coercion in schema_to_proto's &StructType parameter.
        Ok(proto) => {
            use prost::Message;
            leak_bytes(proto.encode_to_vec(), out_len)
        }
        Err(msg) => {
            unsafe { write_err(out_err, &format!("serialize schema to proto: {msg}")) };
            ptr::null_mut()
        }
    }
}

/// Build a scan state machine off the finished snapshot. `scan_kind`: 0 = metadata-only (file-list
/// terminal), 1 = data (full plan). `predicate`, if non-null, is a data-skipping [`EnginePredicate`]
/// visited ONCE here. Returns an owned [`DeltaSm`] (Scan arm), or null on error. The snapshot is
/// unchanged and can build more scans.
///
/// # Safety
/// `snap` is a valid [`DeltaSnapshotValue`]; `predicate`, if non-null, is a valid [`EnginePredicate`]
/// safe to call/read for this call; `out_err`, if non-null, writable. Free the result with
/// [`delta_sm_free`].
#[no_mangle]
pub unsafe extern "C" fn delta_snapshot_scan_sm(
    snap: *mut DeltaSnapshotValue,
    scan_kind: i32,
    predicate: *mut EnginePredicate,
    out_err: *mut *mut c_char,
) -> *mut DeltaSm {
    init_out!(out_err);
    if snap.is_null() {
        unsafe { write_err(out_err, "delta_snapshot_scan_sm: null handle") };
        return ptr::null_mut();
    }
    let snap = unsafe { &*snap };
    let metadata_only = scan_kind == 0;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Visit the engine predicate ONCE, here, while the engine state it borrows is alive.
        let predicate = match unsafe { predicate.as_mut() } {
            Some(p) => Some(Arc::new(unsafe { decode_engine_predicate(p) }?)),
            None => None,
        };
        let mut sb = snap.snapshot.clone().scan_builder();
        if let Some(pred) = predicate {
            sb = sb.with_predicate(Some(pred));
        }
        let scan = sb.build().map_err(|e| format!("build scan: {e}"))?;
        let scan_sm = if metadata_only {
            scan.scan_metadata_state_machine()
        } else {
            scan.scan_state_machine()
        }
        .map_err(|e| format!("build scan SM: {e}"))?;
        Ok::<_, String>(KdfScan { driver: ReduceDriver::new(snap.engine.clone(), scan_sm) })
    }));
    match outcome {
        Ok(Ok(scan)) => Box::into_raw(Box::new(DeltaSm::Scan(scan))),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "delta_snapshot_scan_sm: panic while building the scan") };
            ptr::null_mut()
        }
    }
}

/// Free a [`DeltaSnapshotValue`].
///
/// # Safety
/// `snap` is null or a pointer from [`delta_sm_build_snapshot`], freed once.
#[no_mangle]
pub unsafe extern "C" fn delta_snapshot_free(snap: *mut DeltaSnapshotValue) {
    if !snap.is_null() {
        drop(unsafe { Box::from_raw(snap) });
    }
}

// ── scan value: plan / free ────────────────────────────────────────────────────────────────

/// The built scan's terminal `ResultPlan` as proto bytes (the plan the engine faithfully executes).
/// Writes the byte length to `*out_len`; returns a malloc'd buffer freed with [`delta_bytes_free`],
/// or null on error.
///
/// # Safety
/// `scan` is a valid [`DeltaScanValue`]; `out_len` and `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn delta_scan_plan(
    scan: *mut DeltaScanValue,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    init_out!(out_err, out_len);
    if scan.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "delta_scan_plan: null pointer argument") };
        return ptr::null_mut();
    }
    let scan = unsafe { &*scan };
    match super::proto_convert::result_plan_to_proto(&scan.plan) {
        Ok(proto) => {
            use prost::Message;
            leak_bytes(proto.encode_to_vec(), out_len)
        }
        Err(msg) => {
            unsafe { write_err(out_err, &format!("serialize result plan to proto: {msg}")) };
            ptr::null_mut()
        }
    }
}

/// Free a [`DeltaScanValue`].
///
/// # Safety
/// `scan` is null or a pointer from [`delta_sm_build_scan`], freed once.
#[no_mangle]
pub unsafe extern "C" fn delta_scan_free(scan: *mut DeltaScanValue) {
    if !scan.is_null() {
        drop(unsafe { Box::from_raw(scan) });
    }
}

// ── free helpers for the delta_* ABI (self-contained; no kdf_* leakage) ────────────────────────

/// Free a proto byte buffer returned by a `delta_*_plan` / `delta_*_schema` emitter.
///
/// # Safety
/// `ptr`/`len` are exactly what such an emitter returned (or `ptr` is null). Call at most once.
#[no_mangle]
pub unsafe extern "C" fn delta_bytes_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe { drop(Vec::from_raw_parts(ptr, len, len)) };
    }
}

/// Free an error string written to a `delta_*` export's `out_err` slot.
///
/// # Safety
/// `s` is null or a string produced by a `delta_*` `out_err` path, freed at most once.
#[no_mangle]
pub unsafe extern "C" fn delta_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

// ============================================================================
// Phase B structural tests for the delta_* ABI (SDK_IMPLEMENTATION_PLAN.md B7).
//
// Validates the ABI SHAPE + proto emission + arm-guards WITHOUT executing reduces
// (the full drive-to-Done is the C++ harness's job in Phase C, where DuckDB runs
// the reduces). Here we open a real snapshot SM, advance one step, and assert:
//   - open returns a non-null handle;
//   - next() returns a valid step code;
//   - on REDUCE, reduce_plan bytes decode into a ResultPlan proto, take_reducer is non-null;
//   - build_scan on a snapshot SM is rejected (wrong-arm guard);
//   - free/lifecycle is sound.
// ============================================================================
#[cfg(test)]
mod delta_abi_tests {
    use super::*;

    fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
        std::fs::create_dir_all(dst).unwrap();
        for e in std::fs::read_dir(src).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            let t = dst.join(e.file_name());
            if p.is_dir() {
                copy_dir(&p, &t);
            } else {
                std::fs::copy(&p, &t).unwrap();
            }
        }
    }

    /// Copy the committed fixture to a temp dir and return its `file://` path string.
    fn fixture_table() -> (tempfile::TempDir, String) {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../kernel/tests/data/table-without-dv-small");
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("t");
        copy_dir(&src, &table);
        let url =
            Url::from_directory_path(table.canonicalize().unwrap()).unwrap();
        (tmp, url.to_string())
    }

    #[test]
    fn delta_abi_shape_and_proto_emission() {
        let (_tmp, path) = fixture_table();
        let mut err: *mut c_char = ptr::null_mut();

        // open → non-null snapshot SM
        let sm = unsafe {
            delta_open_snapshot(path.as_ptr() as *const c_char, path.len(), -1, &mut err)
        };
        assert!(!sm.is_null(), "delta_open_snapshot returned null");
        assert!(err.is_null());

        // next → a valid step code (REDUCE or DONE), never -1
        let step = unsafe { delta_sm_next(sm, &mut err) };
        assert!(step == DELTA_STEP_REDUCE || step == DELTA_STEP_DONE, "next returned {step}");
        assert!(err.is_null(), "next set an error");

        if step == DELTA_STEP_REDUCE {
            // reduce_plan bytes must decode into a ResultPlan proto (the IR transport works)
            let mut len: usize = 0;
            let buf = unsafe { delta_sm_reduce_plan(sm, &mut len, &mut err) };
            assert!(!buf.is_null() && len > 0, "reduce_plan returned empty");
            let bytes = unsafe { std::slice::from_raw_parts(buf, len) };
            let decoded = <super::super::proto::plan::ResultPlan as prost::Message>::decode(bytes);
            assert!(decoded.is_ok(), "reduce_plan bytes did not decode as ResultPlan proto");
            unsafe { super::delta_bytes_free(buf, len) };

            // take_reducer → non-null
            let reducer = unsafe { delta_sm_take_reducer(sm, &mut err) };
            assert!(!reducer.is_null(), "take_reducer returned null");
            unsafe { super::delta_reducer_free(reducer) };
        }

        // wrong-arm guard: build_scan on a snapshot SM must fail (null + err), not misbehave.
        // (build_* consume the handle, so this also frees `sm`.)
        let scan_val = unsafe { delta_sm_build_scan(sm, &mut err) };
        assert!(scan_val.is_null(), "build_scan on a snapshot SM should be null");
        assert!(!err.is_null(), "build_scan wrong-arm should set an error");
        unsafe { super::delta_string_free(err) };
    }
}
