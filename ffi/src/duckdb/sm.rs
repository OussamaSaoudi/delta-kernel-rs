//! State-machine SDK FFI: drive a kernel scan state machine step-by-step from C++.
//!
//! Where [`super::kdf_scan_result_plan_sql`] drives the scan SM to completion *inside* the kernel
//! and hands back only the final SQL, this module exposes the SM as an opaque handle the C++ side
//! drives: pull the next [`EngineRequest`] (`kdf_sm_get_step`), execute it, submit the outcome
//! (`kdf_sm_submit_*`), loop until done (`kdf_sm_result_sql`). The C++ side owns the loop.
//!
//! M0 ships the loop with execution still delegated to the kernel's DataFusion executor
//! (`kdf_sm_submit_default`); later milestones add `kdf_sm_submit_schema` / `kdf_sm_submit_reducer`
//! so DuckDB executes the requests itself.
//!
//! # Threading
//! [`KdfSM`] wraps a `!Send` `CoroutineSM` (genawaiter `rc::Gen` over an `Rc<RefCell>` `Context`).
//! It is a single-owner cursor: **never call two `kdf_sm_*` entry points for the same handle
//! concurrently.** Between `get_step` and the matching submit the SM touches nothing; one request
//! is in flight at a time. Callers that move the handle across threads must serialize access (a
//! per-handle mutex) — the mutex is also the happens-before fence for the non-atomic refcounts.

use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use delta_kernel::plans::ir::plan::ResultPlan;
use delta_kernel::plans::state_machines::framework::coroutine::CoroutineSM;
use delta_kernel::plans::state_machines::framework::state_machine::{
    EngineRequest, EngineResponse, NextStep, StateMachine,
};
use delta_kernel::plans::state_machines::snapshot::snapshot_state_machine_for;
use delta_kernel::Engine;
use delta_kernel_datafusion_engine::DataFusionExecutor;

use super::{build_local_engine, finalize_result_plan_to_sql, table_url, write_err};

/// Request kind returned by [`kdf_sm_get_step`].
pub const KDF_REQ_SCHEMA_QUERY: i32 = 0;
pub const KDF_REQ_REDUCE: i32 = 1;
/// The SM yielded nothing (zero-yield / priming): the next submit hands back the terminal value.
pub const KDF_REQ_NONE: i32 = 2;

/// [`kdf_sm_submit_*`] outcome.
pub const KDF_NEXT_CONTINUE: i32 = 0;
pub const KDF_NEXT_DONE: i32 = 1;

/// Opaque scan state-machine handle. Owns everything needed to drive the SM and finalize its
/// `ResultPlan` into DuckDB SQL. Freed with [`kdf_sm_free`].
pub struct KdfSM {
    // Kept alive for the executor + SM captures.
    _engine: Arc<dyn Engine>,
    executor: DataFusionExecutor,
    runtime: tokio::runtime::Runtime,
    sm: CoroutineSM<ResultPlan>,
    /// The request from the most recent [`kdf_sm_get_step`], consumed by the next submit. `None`
    /// means the last `get_step` produced no step (submit will pass `EngineResponse::Empty`).
    pending: Option<EngineRequest>,
    /// Terminal `ResultPlan`, set once the SM reports [`NextStep::Done`].
    result: Option<ResultPlan>,
    done: bool,
    /// Scratch C strings handed back as borrowed pointers (valid until the next call that rewrites them).
    step_name_buf: CString,
    schema_path_buf: CString,
}

impl KdfSM {
    fn open(path: &str, version: i64) -> Result<KdfSM, String> {
        let engine = build_local_engine();
        let url = table_url(path)?;
        let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
            .map_err(|e| format!("build datafusion executor: {e}"))?;
        // The SM futures are `!Send`; `block_on` drives them on the calling thread (one step at a
        // time), while DataFusion's `Send` work spawns to the runtime's worker pool.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("build tokio runtime: {e}"))?;
        // Snapshot construction is itself driven through a state machine: the snapshot SM resolves
        // protocol+metadata via an engine Reduce over the log (the eager directory listing happens
        // in `snapshot_state_machine_for`), and the scan SM is then built from that snapshot.
        let version_opt = if version >= 0 { Some(version as u64) } else { None };
        let snapshot_sm = snapshot_state_machine_for(url, version_opt, engine.as_ref())
            .map_err(|e| format!("build snapshot SM: {e}"))?;
        let snapshot = Arc::new(
            runtime
                .block_on(executor.drive_to_completion(snapshot_sm))
                .map_err(|e| format!("drive snapshot SM: {e}"))?,
        );
        let scan = snapshot
            .scan_builder()
            .build()
            .map_err(|e| format!("build scan: {e}"))?;
        let sm = scan
            .scan_state_machine()
            .map_err(|e| format!("build scan SM: {e}"))?;
        Ok(KdfSM {
            _engine: engine,
            executor,
            runtime,
            sm,
            pending: None,
            result: None,
            done: false,
            step_name_buf: CString::default(),
            schema_path_buf: CString::default(),
        })
    }
}

/// Open a scan state machine over the Delta table at `path` (optionally at `version`, or `-1` for
/// latest). Returns an owned [`KdfSM`] or null on error (`*out_err` set, free with `kdf_string_free`).
///
/// # Safety
/// `path_ptr` must point to `path_len` valid UTF-8 bytes. `out_err`, if non-null, must point to a
/// writable `*mut c_char`. Free the result exactly once with [`kdf_sm_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_scan_open(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    out_err: *mut *mut c_char,
) -> *mut KdfSM {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_sm_scan_open: null path pointer") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        KdfSM::open(path, version)
    });
    match outcome {
        Ok(Ok(sm)) => Box::into_raw(Box::new(sm)),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_scan_open: panic while opening scan SM") };
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

/// True once the SM has reported [`NextStep::Done`].
///
/// # Safety
/// `sm` must be null or a valid [`KdfSM`].
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_is_done(sm: *const KdfSM) -> bool {
    !sm.is_null() && unsafe { (*sm).done }
}

/// Borrowed label for the SM's current step (diagnostics). Valid until the next `kdf_sm_*` call.
///
/// # Safety
/// `sm` must be a valid [`KdfSM`].
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_step_name(sm: *mut KdfSM) -> *const c_char {
    if sm.is_null() {
        return ptr::null();
    }
    let sm = unsafe { &mut *sm };
    sm.step_name_buf = CString::new(sm.sm.step_name()).unwrap_or_default();
    sm.step_name_buf.as_ptr()
}

/// Pull the next step. Returns the request kind (`KDF_REQ_*`), or `-1` on error (`*out_err` set).
/// Stores the request internally for the next `kdf_sm_submit_*`. For `KDF_REQ_SCHEMA_QUERY` the
/// file path is available via [`kdf_sm_schema_query_path`].
///
/// # Safety
/// `sm` must be a valid [`KdfSM`]. `out_err`, if non-null, writable.
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
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sm.sm.get_step()));
    match outcome {
        // A zero-yield SM (or terminal) returns Err from get_step; the driver treats that as "no
        // step" and the next submit hands back the terminal value via `EngineResponse::Empty`.
        Ok(Err(_)) => {
            sm.pending = None;
            KDF_REQ_NONE
        }
        Ok(Ok(req)) => {
            let kind = match &req {
                EngineRequest::SchemaQuery(q) => {
                    sm.schema_path_buf = CString::new(q.file_path.as_str()).unwrap_or_default();
                    KDF_REQ_SCHEMA_QUERY
                }
                EngineRequest::Reduce { .. } => KDF_REQ_REDUCE,
            };
            sm.pending = Some(req);
            kind
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_get_step: panic") };
            -1
        }
    }
}

/// Borrowed file path of the pending `SchemaQuery` (after `kdf_sm_get_step` returned
/// `KDF_REQ_SCHEMA_QUERY`). Valid until the next `kdf_sm_*` call. Empty otherwise.
///
/// # Safety
/// `sm` must be a valid [`KdfSM`].
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_schema_query_path(sm: *const KdfSM) -> *const c_char {
    if sm.is_null() {
        return ptr::null();
    }
    unsafe { (*sm).schema_path_buf.as_ptr() }
}

/// Execute the pending step via the kernel's DataFusion executor and submit the outcome (M0 bridge
/// — DuckDB executes the request itself once `kdf_sm_submit_schema`/`_reducer` land). Returns the
/// next-step code (`KDF_NEXT_*`) or `-1` on error (`*out_err` set). On `KDF_NEXT_DONE` the terminal
/// `ResultPlan` is stored for [`kdf_sm_result_sql`].
///
/// # Safety
/// `sm` must be a valid [`KdfSM`]. `out_err`, if non-null, writable.
#[no_mangle]
pub unsafe extern "C" fn kdf_sm_submit_default(sm: *mut KdfSM, out_err: *mut *mut c_char) -> i32 {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if sm.is_null() {
        unsafe { write_err(out_err, "kdf_sm_submit_default: null handle") };
        return -1;
    }
    let sm = unsafe { &mut *sm };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Execute the pending request (if any) on the runtime, then submit its outcome.
        let phase_result = match sm.pending.take() {
            Some(op) => sm.runtime.block_on(sm.executor.execute_step(op)),
            None => Ok(EngineResponse::Empty),
        };
        sm.sm.submit(phase_result)
    }));
    match outcome {
        Ok(Ok(NextStep::Continue)) => KDF_NEXT_CONTINUE,
        Ok(Ok(NextStep::Done(rp))) => {
            sm.result = Some(rp);
            sm.done = true;
            KDF_NEXT_DONE
        }
        Ok(Err(e)) => {
            unsafe { write_err(out_err, &format!("kdf_sm_submit_default: {e}")) };
            -1
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_submit_default: panic") };
            -1
        }
    }
}

/// Finalize the terminal `ResultPlan` into the data-stage DuckDB SQL (the same lowering the legacy
/// `kdf_scan_result_plan_sql` produces). Only valid after a submit returned `KDF_NEXT_DONE`.
/// Returns a malloc'd C string (free with `kdf_string_free`), or null on error (`*out_err` set).
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
    let sm = unsafe { &mut *sm };
    let Some(rp) = sm.result.take() else {
        unsafe { write_err(out_err, "kdf_sm_result_sql: SM not done (no ResultPlan)") };
        return ptr::null_mut();
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finalize_result_plan_to_sql(rp, &sm.executor, &sm.runtime)
    }));
    match outcome {
        Ok(Ok(sql)) => match CString::new(sql) {
            Ok(c) => c.into_raw(),
            Err(_) => {
                unsafe { write_err(out_err, "kdf_sm_result_sql: SQL contains interior NUL") };
                ptr::null_mut()
            }
        },
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_sm_result_sql: panic during finalize") };
            ptr::null_mut()
        }
    }
}
