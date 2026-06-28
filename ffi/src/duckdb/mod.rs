//! Phase-1 DuckDB FFI shim: plan-driven scan file enumeration.
//!
//! Drives the kernel's declarative scan-metadata state machine
//! ([`Scan::scan_metadata_state_machine`]) through the DataFusion engine, collects the flat
//! `scan_file_row` output (`{ path, size, deletionVector?, fileConstantValues? }`), and exposes
//! the surviving data-file list (path + size) to the `duckdb-delta` C++ extension.
//!
//! This is the Phase-1 ("Approach C") beachhead from `DUCKDB_SCAN_PLAN.md`: it re-sources the
//! `delta_scan` file list from the plan-based scan SM, replacing the legacy `scan_metadata`
//! iterator, while DuckDB's existing read path reads the parquet files. The SM driver, plan
//! execution, and file enumeration all happen here in Rust; only the resolved `path`/`size`
//! cross the FFI boundary in M1.
//!
//! [`Scan::scan_metadata_state_machine`]: delta_kernel::scan::Scan::scan_metadata_state_machine

use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use url::Url;

pub mod plan_to_sql;
pub mod proto;
pub mod proto_convert;

/// One surviving data file produced by the plan-driven scan.
struct KdfFileRow {
    /// Table-relative (or absolute) data-file path, exactly as the `scan_file_row.path` column
    /// carries it. Stored as a C string so `kdf_scan_file_path` can hand back a borrowed pointer.
    path: CString,
    /// File size in bytes (`scan_file_row.size`); `0` when the column was null.
    size: i64,
}

/// Opaque handle returned to the C++ extension. Owns the fully-enumerated file list; the C++
/// side iterates it by index. Freed with [`kdf_scan_free`].
pub struct KdfScan {
    files: Vec<KdfFileRow>,
}

/// Build a local-filesystem-backed kernel default engine. M1 targets on-disk tables; cloud
/// credential threading (reusing duckdb-delta's `CreateBuilder`) lands in a later milestone.
fn build_local_engine() -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

/// Resolve a user-supplied table location to a `Url`. Accepts an existing URL (e.g. `file://`,
/// `s3://`) or a local filesystem path, which is canonicalized into a `file://` directory URL.
fn table_url(path: &str) -> Result<Url, String> {
    if let Ok(url) = Url::parse(path) {
        // Treat single-character "schemes" as Windows drive letters, not URL schemes.
        if url.scheme().len() > 1 {
            return Ok(url);
        }
    }
    let abs = std::path::Path::new(path)
        .canonicalize()
        .map_err(|e| format!("canonicalize table path {path}: {e}"))?;
    Url::from_directory_path(&abs)
        .map_err(|()| format!("cannot build a file:// url from {}", abs.display()))
}

/// Drive the plan-based scan-metadata SM and collect the surviving file list.
fn enumerate_files(path: &str) -> Result<Vec<KdfFileRow>, String> {
    let engine = build_local_engine();
    let url = table_url(path)?;
    let snapshot = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .map_err(|e| format!("build snapshot: {e}"))?;
    let scan = snapshot
        .scan_builder()
        .build()
        .map_err(|e| format!("build scan: {e}"))?;
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
        .map_err(|e| format!("build datafusion executor: {e}"))?;

    // The scan SM future is `!Send` (CPU-only sequencer). `block_on` drives it on the calling
    // thread; DataFusion's internal `Send` task spawns go to the runtime's worker pool.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;

    let batches: Vec<RecordBatch> = runtime.block_on(async {
        let df = executor
            .scan_metadata(&scan)
            .await
            .map_err(|e| format!("drive scan_metadata SM: {e}"))?;
        df.collect()
            .await
            .map_err(|e| format!("collect scan_metadata: {e}"))
    })?;

    // Terminal `scan_file_row` shape: column 0 = path (Utf8, non-null), column 1 = size (Int64).
    let mut files = Vec::new();
    for batch in &batches {
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "scan_file_row column 0 (path) is not a Utf8 array".to_string())?;
        let sizes = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "scan_file_row column 1 (size) is not an Int64 array".to_string())?;
        for row in 0..batch.num_rows() {
            if paths.is_null(row) {
                continue;
            }
            let path = CString::new(paths.value(row))
                .map_err(|e| format!("data-file path contains an interior NUL: {e}"))?;
            let size = if sizes.is_null(row) { 0 } else { sizes.value(row) };
            files.push(KdfFileRow { path, size });
        }
    }
    Ok(files)
}

/// Write `msg` as a freshly-allocated C string into `*out_err` (when `out_err` is non-null).
/// The caller frees it with [`kdf_string_free`].
///
/// # Safety
/// `out_err`, if non-null, must point to a writable `*mut c_char`.
unsafe fn write_err(out_err: *mut *mut c_char, msg: &str) {
    if out_err.is_null() {
        return;
    }
    let c = CString::new(msg).unwrap_or_default();
    unsafe { *out_err = c.into_raw() };
}

/// Open a plan-driven scan over the Delta table at `path` and enumerate its surviving data files.
///
/// Returns an owned [`KdfScan`] handle on success, or null on error. On error, when `out_err` is
/// non-null, `*out_err` is set to a malloc'd C string describing the failure (free it with
/// [`kdf_string_free`]); on success `*out_err` is set to null.
///
/// # Safety
/// `path_ptr` must point to `path_len` valid UTF-8 bytes. `out_err`, if non-null, must point to a
/// writable `*mut c_char`. The returned pointer must be freed exactly once with [`kdf_scan_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_open(
    path_ptr: *const c_char,
    path_len: usize,
    out_err: *mut *mut c_char,
) -> *mut KdfScan {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_scan_open: null path pointer") };
        return ptr::null_mut();
    }
    // Guard the FFI boundary against unwinding into C++.
    let outcome = std::panic::catch_unwind(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        enumerate_files(path)
    });
    match outcome {
        Ok(Ok(files)) => Box::into_raw(Box::new(KdfScan { files })),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_open: panic while enumerating scan files") };
            ptr::null_mut()
        }
    }
}

/// Number of surviving data files in the scan.
///
/// # Safety
/// `scan` must be null or a valid pointer returned by [`kdf_scan_open`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_file_count(scan: *const KdfScan) -> u64 {
    if scan.is_null() {
        return 0;
    }
    unsafe { (*scan).files.len() as u64 }
}

/// Borrowed pointer to the NUL-terminated path of the `i`-th data file, or null if out of range.
/// Valid until the owning [`KdfScan`] is freed.
///
/// # Safety
/// `scan` must be null or a valid pointer returned by [`kdf_scan_open`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_file_path(scan: *const KdfScan, i: u64) -> *const c_char {
    if scan.is_null() {
        return ptr::null();
    }
    match unsafe { &(*scan).files }.get(i as usize) {
        Some(file) => file.path.as_ptr(),
        None => ptr::null(),
    }
}

/// Size in bytes of the `i`-th data file, or `-1` if out of range.
///
/// # Safety
/// `scan` must be null or a valid pointer returned by [`kdf_scan_open`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_file_size(scan: *const KdfScan, i: u64) -> i64 {
    if scan.is_null() {
        return -1;
    }
    unsafe { &(*scan).files }
        .get(i as usize)
        .map(|file| file.size)
        .unwrap_or(-1)
}

/// Free a [`KdfScan`] returned by [`kdf_scan_open`].
///
/// # Safety
/// `scan` must be null or a pointer returned by [`kdf_scan_open`], freed at most once.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_free(scan: *mut KdfScan) {
    if !scan.is_null() {
        drop(unsafe { Box::from_raw(scan) });
    }
}

// ============================================================================
// Deletion-vector resolution (for the plan-SQL data read)
// ============================================================================

unsafe fn cstr_to_str<'a>(ptr: *const c_char, len: usize) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
    std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {e}"))
}

fn resolve_dv_impl(
    root: &str,
    storage_type: &str,
    path_or_inline: &str,
    has_offset: bool,
    offset: i32,
    size_in_bytes: i32,
    cardinality: i64,
) -> Result<Vec<bool>, String> {
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    let engine = build_local_engine();
    let table_root = table_url(root)?;
    let storage_type: DeletionVectorStorageType = storage_type
        .parse()
        .map_err(|_| format!("unrecognized DV storageType: {storage_type:?}"))?;
    let descriptor = DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv: path_or_inline.to_string(),
        offset: if has_offset { Some(offset) } else { None },
        size_in_bytes,
        cardinality,
    };
    delta_kernel::scan::selection_vector(engine.as_ref(), &descriptor, &table_root)
        .map_err(|e| format!("resolve deletion vector: {e}"))
}

/// Resolve a Delta deletion-vector descriptor into a row-selection bool slice (true = keep) that
/// the C++ `DeltaDeleteFilter` applies during the parquet read. Returns an empty slice on error
/// (writes `*out_err`).
///
/// # Safety
/// All `*_ptr`/`*_len` pairs must describe valid UTF-8 byte ranges. `out_err`, if non-null, must
/// be a writable `*mut *mut c_char`. The returned slice's `ptr` (if non-null) must be freed once
/// with `free_bool_slice`.
#[no_mangle]
pub unsafe extern "C" fn kdf_resolve_dv(
    table_root_ptr: *const c_char,
    table_root_len: usize,
    storage_type_ptr: *const c_char,
    storage_type_len: usize,
    path_or_inline_ptr: *const c_char,
    path_or_inline_len: usize,
    has_offset: bool,
    offset: i32,
    size_in_bytes: i32,
    cardinality: i64,
    out_err: *mut *mut c_char,
) -> crate::KernelBoolSlice {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    let outcome = std::panic::catch_unwind(|| {
        let root = unsafe { cstr_to_str(table_root_ptr, table_root_len) }?;
        let storage_type = unsafe { cstr_to_str(storage_type_ptr, storage_type_len) }?;
        let path_or_inline = unsafe { cstr_to_str(path_or_inline_ptr, path_or_inline_len) }?;
        resolve_dv_impl(
            root,
            storage_type,
            path_or_inline,
            has_offset,
            offset,
            size_in_bytes,
            cardinality,
        )
    });
    match outcome {
        Ok(Ok(bools)) => crate::KernelBoolSlice::from(bools),
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            crate::KernelBoolSlice::empty()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_resolve_dv: panic during DV resolution") };
            crate::KernelBoolSlice::empty()
        }
    }
}

/// Free an error string produced by [`kdf_scan_open`].
///
/// # Safety
/// `s` must be null or a pointer produced by this crate's `*out_err` path, freed at most once.
#[no_mangle]
pub unsafe extern "C" fn kdf_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

// ============================================================================
// Phase 2: proto-encoded ResultPlan over FFI
// ============================================================================

/// Drive the scan-metadata SM to its terminal [`ResultPlan`] (without executing it) and return the
/// plan, proto-encoded, as bytes.
fn result_plan_proto_bytes(path: &str) -> Result<Vec<u8>, String> {
    use prost::Message;
    let engine = build_local_engine();
    let url = table_url(path)?;
    let snapshot = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .map_err(|e| format!("build snapshot: {e}"))?;
    let scan = snapshot
        .scan_builder()
        .build()
        .map_err(|e| format!("build scan: {e}"))?;
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
        .map_err(|e| format!("build datafusion executor: {e}"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;
    let sm = scan
        .scan_metadata_state_machine()
        .map_err(|e| format!("build scan-metadata SM: {e}"))?;
    // Drives the SM's shape-resolution probes (SchemaQuery/Reduce) and returns the terminal
    // ResultPlan without executing its dataflow.
    let result_plan = runtime
        .block_on(executor.drive_to_completion(sm))
        .map_err(|e| format!("drive scan-metadata SM to ResultPlan: {e}"))?;
    let proto = super::duckdb::proto_convert::result_plan_to_proto(&result_plan)?;
    Ok(proto.encode_to_vec())
}

/// Drive the scan-metadata SM for the Delta table at `path` to its terminal `ResultPlan` and
/// return the plan as proto-encoded bytes (`delta.kernel.plan.ResultPlan`).
///
/// On success returns a malloc'd byte buffer and writes its length to `*out_len`; free it with
/// [`kdf_bytes_free`]. On error returns null and (when `out_err` is non-null) writes a malloc'd
/// error C string (free with [`kdf_string_free`]).
///
/// # Safety
/// `path_ptr` must point to `path_len` valid UTF-8 bytes. `out_len` must be a writable `*mut usize`.
/// `out_err`, if non-null, must be a writable `*mut *mut c_char`. The returned buffer must be freed
/// exactly once with [`kdf_bytes_free`] using the same length written to `*out_len`.
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_result_plan_proto(
    path_ptr: *const c_char,
    path_len: usize,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if !out_len.is_null() {
        unsafe { *out_len = 0 };
    }
    if path_ptr.is_null() || out_len.is_null() {
        unsafe { write_err(out_err, "kdf_scan_result_plan_proto: null pointer argument") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        result_plan_proto_bytes(path)
    });
    match outcome {
        Ok(Ok(mut buf)) => {
            buf.shrink_to_fit();
            let len = buf.len();
            let ptr = buf.as_mut_ptr();
            std::mem::forget(buf);
            unsafe { *out_len = len };
            ptr
        }
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_result_plan_proto: panic during plan serialization") };
            ptr::null_mut()
        }
    }
}

/// Free a byte buffer returned by [`kdf_scan_result_plan_proto`].
///
/// # Safety
/// `ptr`/`len` must be the exact pointer and length returned by [`kdf_scan_result_plan_proto`],
/// freed at most once.
#[no_mangle]
pub unsafe extern "C" fn kdf_bytes_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
    }
}

/// Drive the scan-metadata SM to its terminal `ResultPlan` and lower it to a single DuckDB SQL
/// statement that, when executed by DuckDB, produces the scan's `scan_file_row` output.
/// `version` selects a snapshot version for time travel; a negative value means latest.
fn result_plan_sql(path: &str, version: i64) -> Result<String, String> {
    let engine = build_local_engine();
    let url = table_url(path)?;
    let mut builder = Snapshot::builder_for(url);
    if version >= 0 {
        builder = builder.at_version(version as u64);
    }
    let snapshot = builder
        .build(engine.as_ref())
        .map_err(|e| format!("build snapshot: {e}"))?;
    let scan = snapshot
        .scan_builder()
        .build()
        .map_err(|e| format!("build scan: {e}"))?;
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
        .map_err(|e| format!("build datafusion executor: {e}"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;
    // Drive the DATA-stage SM (with_data=true): its `scan_file_row` carries the full partition
    // schema (`partitionValues_parsed`) and `deletionVector`, which the metadata-only scan omits.
    let sm = scan
        .scan_state_machine()
        .map_err(|e| format!("build scan SM: {e}"))?;
    let rp = runtime
        .block_on(executor.drive_to_completion(sm))
        .map_err(|e| format!("drive scan SM to ResultPlan: {e}"))?;

    // Peel the data stage: terminal `Project` <- data `Load` <- `scan_file_row`. Lower up to
    // `scan_file_row`; DuckDB then reads the parquet and the C++ MultiFileReader applies DV +
    // partitions per file (the data `Load` + final logical projection are NOT lowered to SQL --
    // DV row-masking can't be expressed in SQL).
    use delta_kernel::plans::ir::nodes::NodeKind;
    let nodes = &rp.plan.nodes;
    let term = nodes
        .get(rp.result.0 as usize)
        .ok_or("missing terminal node")?;
    if !matches!(term.kind, NodeKind::Project(_)) {
        return Err(format!("expected data-stage terminal Project, got {}", term.kind));
    }
    let load_ref = *term.inputs.first().ok_or("terminal Project has no input")?;
    let load = nodes.get(load_ref.0 as usize).ok_or("missing data Load node")?;
    if !matches!(load.kind, NodeKind::Load(_)) {
        return Err(format!("expected data-stage Load, got {}", load.kind));
    }
    let scan_file_row_ref = *load.inputs.first().ok_or("data Load has no input")?;

    // No peel: lower the FULL plan. Sidecar/manifest runtime Loads (indices below scan_file_row, in
    // the reconciliation) are still materialized to static read_parquet; the terminal data Load
    // (above scan_file_row) keeps its runtime `scan_file_row` input and lowers to the `delta_load`
    // table function (delta_load_sql), which streams it and applies DV + partitions per file.
    let result_ref = rp.result;
    let mut plan = rp.plan;
    materialize_runtime_loads(&mut plan, &executor, &runtime, scan_file_row_ref)?;
    super::duckdb::plan_to_sql::result_plan_to_sql_until(&plan, result_ref)
}

/// Minimal JSON string escaping for the load-node params payload.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Serialize a single `LoadNode` (at plan-node index `idx`) to a JSON object. `dv` is non-null only
/// for the data-stage Load; metadata Loads (commit/checkpoint/sidecar) have `dv: null`.
fn load_node_to_json(idx: usize, ln: &delta_kernel::plans::ir::nodes::LoadNode) -> String {
    use delta_kernel::plans::ir::nodes::{DvKind, FileType};
    let file_type = match ln.file_type {
        FileType::Parquet => "parquet",
        FileType::Json => "json",
    };
    let path_column = ln.file_meta.path_column.path().join(".");
    let mdc: Vec<String> = ln
        .metadata_derived_columns
        .iter()
        .map(|c| json_escape(&c.path().join(".")))
        .collect();
    let dv = match &ln.dv_ref {
        Some(d) => {
            let kind = match d.kind {
                DvKind::Bytes => "bytes",
                DvKind::Descriptor => "descriptor",
            };
            format!(
                "{{\"column\":{},\"kind\":\"{}\"}}",
                json_escape(&d.column.path().join(".")),
                kind
            )
        }
        None => "null".to_string(),
    };
    let base_url = match &ln.base_url {
        Some(u) => json_escape(u.as_str()),
        None => "null".to_string(),
    };
    format!(
        "{{\"node_index\":{},\"file_type\":\"{}\",\"path_column\":{},\"metadata_derived_columns\":[{}],\"dv\":{},\"base_url\":{}}}",
        idx,
        file_type,
        json_escape(&path_column),
        mdc.join(","),
        dv,
        base_url
    )
}

/// Drive the scan-metadata SM to its terminal `ResultPlan` and return **every `LoadNode` in the
/// plan** as a JSON array, so the C++ side is driven by the real kernel `Load` nodes (file_type, the
/// explicit metadata_derived_columns broadcast set, the deletion-vector reference + kind, base_url,
/// path_column) instead of re-inferring them. A scan plan has an arbitrary number of Loads — commit
/// (JSON), checkpoint, sidecar (Parquet), and the data Load (Parquet, with `dv`); this reports all of
/// them generically, indexed by plan-node position.
fn scan_load_nodes_json(path: &str, version: i64) -> Result<String, String> {
    use delta_kernel::plans::ir::nodes::NodeKind;
    let engine = build_local_engine();
    let url = table_url(path)?;
    let mut builder = Snapshot::builder_for(url);
    if version >= 0 {
        builder = builder.at_version(version as u64);
    }
    let snapshot = builder
        .build(engine.as_ref())
        .map_err(|e| format!("build snapshot: {e}"))?;
    let scan = snapshot
        .scan_builder()
        .build()
        .map_err(|e| format!("build scan: {e}"))?;
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
        .map_err(|e| format!("build datafusion executor: {e}"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;
    let sm = scan
        .scan_state_machine()
        .map_err(|e| format!("build scan SM: {e}"))?;
    let rp = runtime
        .block_on(executor.drive_to_completion(sm))
        .map_err(|e| format!("drive scan SM to ResultPlan: {e}"))?;

    // Enumerate EVERY Load node in the plan (commit/checkpoint/sidecar/data) — an arbitrary number —
    // and emit each generically, indexed by plan-node position.
    let loads: Vec<String> = rp
        .plan
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(i, node)| match &node.kind {
            NodeKind::Load(ln) => Some(load_node_to_json(i, ln)),
            _ => None,
        })
        .collect();
    Ok(format!("[{}]", loads.join(",")))
}

/// A `ColumnName` reduced to its single top-level component (errors if nested).
fn single_col(c: &delta_kernel::expressions::ColumnName) -> Result<String, String> {
    match c.path() {
        [one] => Ok(one.clone()),
        other => Err(format!("expected a top-level column, got {other:?}")),
    }
}

/// Convert one Arrow array cell to a kernel `Scalar` (covers the file-list column types:
/// path strings, sizes, versions).
fn arrow_to_scalar(
    array: &dyn delta_kernel::arrow::array::Array,
    row: usize,
) -> Result<delta_kernel::expressions::Scalar, String> {
    use delta_kernel::arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
    use delta_kernel::expressions::Scalar;
    if array.is_null(row) {
        // Type the null by the array's arrow type.
        return Ok(match array.data_type() {
            ArrowDataType::Utf8 => Scalar::Null(delta_kernel::schema::DataType::STRING),
            ArrowDataType::Int64 => Scalar::Null(delta_kernel::schema::DataType::LONG),
            ArrowDataType::Int32 => Scalar::Null(delta_kernel::schema::DataType::INTEGER),
            other => return Err(format!("materialize: unsupported null arrow type {other:?}")),
        });
    }
    match array.data_type() {
        ArrowDataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("materialize: not a StringArray")?;
            Ok(Scalar::String(a.value(row).to_string()))
        }
        ArrowDataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("materialize: not an Int64Array")?;
            Ok(Scalar::Long(a.value(row)))
        }
        ArrowDataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("materialize: not an Int32Array")?;
            Ok(Scalar::Integer(a.value(row)))
        }
        other => Err(format!("materialize: unsupported arrow type {other:?}")),
    }
}

/// Map an Arrow type to the kernel `DataType` used for a materialized Values column.
fn arrow_type_to_kernel(t: &delta_kernel::arrow::datatypes::DataType) -> Result<delta_kernel::schema::DataType, String> {
    use delta_kernel::arrow::datatypes::DataType as A;
    use delta_kernel::schema::DataType as K;
    Ok(match t {
        A::Utf8 => K::STRING,
        A::Int64 => K::LONG,
        A::Int32 => K::INTEGER,
        other => return Err(format!("materialize: unsupported column type {other:?}")),
    })
}

/// Rewrite every runtime-file-list `Load` input (in nodes `0..=up_to`) into a concrete `Values`
/// node by executing that input subplan via DataFusion and collecting the file-list columns.
pub fn materialize_runtime_loads(
    plan: &mut delta_kernel::plans::ir::plan::Plan,
    executor: &DataFusionExecutor,
    runtime: &tokio::runtime::Runtime,
    up_to: delta_kernel::plans::ir::plan::RefId,
) -> Result<(), String> {
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::plans::ir::nodes::{NodeKind, ValuesNode};
    use delta_kernel::plans::ir::plan::ResultPlan;
    use delta_kernel::schema::{StructField, StructType};

    let last = (up_to.0 as usize).min(plan.nodes.len().saturating_sub(1));
    for i in 0..=last {
        let NodeKind::Load(load) = plan.nodes[i].kind.clone() else {
            continue;
        };
        let Some(&in_ref) = plan.nodes[i].inputs.first() else {
            continue;
        };
        if matches!(plan.nodes[in_ref.0 as usize].kind, NodeKind::Values(_)) {
            continue; // already a static file list
        }

        // Columns load_sql reads from the input relation: path, size, and broadcast columns.
        let mut col_names = vec![single_col(&load.file_meta.path_column)?];
        if let Some(c) = &load.file_meta.file_size_column {
            col_names.push(single_col(c)?);
        }
        if let Some(c) = &load.file_meta.num_records_column {
            col_names.push(single_col(c)?);
        }
        for c in &load.metadata_derived_columns {
            col_names.push(single_col(c)?);
        }

        // Execute the input subplan to resolve the concrete file list.
        let sub = ResultPlan {
            plan: plan.clone(),
            result: in_ref,
        };
        let df = executor
            .result_plan_to_dataframe(&sub)
            .map_err(|e| format!("materialize Load input: compile: {e}"))?;
        let batches: Vec<RecordBatch> = runtime
            .block_on(async { df.collect().await })
            .map_err(|e| format!("materialize Load input: collect: {e}"))?;

        // Determine column types from the first batch (or default to string-typed empties).
        let mut fields: Vec<StructField> = Vec::new();
        if let Some(b) = batches.first() {
            for name in &col_names {
                let col = b
                    .column_by_name(name)
                    .ok_or_else(|| format!("materialize: column {name} missing in subplan output"))?;
                fields.push(StructField::nullable(
                    name.clone(),
                    arrow_type_to_kernel(col.data_type())?,
                ));
            }
        } else {
            for name in &col_names {
                fields.push(StructField::nullable(name.clone(), delta_kernel::schema::DataType::STRING));
            }
        }
        let schema = std::sync::Arc::new(StructType::new_unchecked(fields));

        let mut rows = Vec::new();
        for b in &batches {
            let cols: Vec<&std::sync::Arc<dyn delta_kernel::arrow::array::Array>> = col_names
                .iter()
                .map(|name| b.column_by_name(name).ok_or_else(|| format!("materialize: column {name} missing")))
                .collect::<Result<Vec<_>, _>>()?;
            for r in 0..b.num_rows() {
                let mut row = Vec::with_capacity(cols.len());
                for col in &cols {
                    row.push(arrow_to_scalar(col.as_ref(), r)?);
                }
                rows.push(row);
            }
        }

        plan.nodes[in_ref.0 as usize].kind = NodeKind::Values(ValuesNode { schema, rows });
        plan.nodes[in_ref.0 as usize].inputs = Vec::new();
    }
    Ok(())
}

/// Drive the scan-metadata SM for the Delta table at `path` and return the DuckDB SQL that
/// executes the resulting plan (NUL-terminated C string; free with [`kdf_string_free`]).
///
/// On error returns null and (when `out_err` is non-null) writes a malloc'd error C string.
///
/// # Safety
/// `path_ptr` must point to `path_len` valid UTF-8 bytes. `out_err`, if non-null, must be a
/// writable `*mut *mut c_char`. The returned string must be freed with [`kdf_string_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_result_plan_sql(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_scan_result_plan_sql: null path pointer") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        result_plan_sql(path, version)
    });
    match outcome {
        Ok(Ok(sql)) => match CString::new(sql) {
            Ok(c) => c.into_raw(),
            Err(_) => {
                unsafe { write_err(out_err, "generated SQL contains an interior NUL") };
                ptr::null_mut()
            }
        },
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_result_plan_sql: panic during plan lowering") };
            ptr::null_mut()
        }
    }
}

/// Emit the data-stage `LoadNode` params (as JSON) for the scan of the Delta table at `path`,
/// so the C++ `PhysicalDeltaLoad` operator is driven by the real kernel `Load` node. `version`
/// selects a snapshot version for time travel; a negative value means latest.
///
/// # Safety
/// `path_ptr` must point to `path_len` valid UTF-8 bytes. `out_err`, if non-null, must be a
/// writable `*mut *mut c_char`. The returned string must be freed with [`kdf_string_free`].
#[no_mangle]
pub unsafe extern "C" fn kdf_scan_data_load_node(
    path_ptr: *const c_char,
    path_len: usize,
    version: i64,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    if !out_err.is_null() {
        unsafe { *out_err = ptr::null_mut() };
    }
    if path_ptr.is_null() {
        unsafe { write_err(out_err, "kdf_scan_data_load_node: null path pointer") };
        return ptr::null_mut();
    }
    let outcome = std::panic::catch_unwind(|| {
        let bytes = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len) };
        let path = std::str::from_utf8(bytes).map_err(|e| format!("table path is not UTF-8: {e}"))?;
        scan_load_nodes_json(path, version)
    });
    match outcome {
        Ok(Ok(json)) => match CString::new(json) {
            Ok(c) => c.into_raw(),
            Err(_) => {
                unsafe { write_err(out_err, "load-node JSON contains an interior NUL") };
                ptr::null_mut()
            }
        },
        Ok(Err(msg)) => {
            unsafe { write_err(out_err, &msg) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { write_err(out_err, "kdf_scan_data_load_node: panic during load-node extraction") };
            ptr::null_mut()
        }
    }
}
