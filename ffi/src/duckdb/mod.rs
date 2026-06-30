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

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::Engine;
use url::Url;

pub mod plan_to_sql;
pub mod proto;
pub mod sm;
pub mod proto_convert;

/// Build a local-filesystem-backed kernel default engine.
pub(crate) fn build_local_engine() -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

/// Build a kernel default engine whose object store is chosen by the table URL's scheme — local
/// for `file://`, S3 for `s3://`/`s3a://` (creds read from the standard `AWS_*` env vars and passed
/// as object_store options). Used by the SM driver so it can list the log / read footers on cloud
/// tables; DuckDB (httpfs) handles the data + reduce-query I/O separately.
pub(crate) fn build_engine_for_url(url: &Url) -> Result<Arc<dyn Engine>, String> {
    use delta_kernel::engine::default::storage::store_from_url_opts;
    let mut opts: Vec<(String, String)> = Vec::new();
    if matches!(url.scheme(), "s3" | "s3a") {
        if let Ok(v) = std::env::var("AWS_REGION").or_else(|_| std::env::var("AWS_DEFAULT_REGION")) {
            opts.push(("region".into(), v));
        }
        if let Ok(v) = std::env::var("AWS_ACCESS_KEY_ID") {
            opts.push(("access_key_id".into(), v));
        }
        if let Ok(v) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            opts.push(("secret_access_key".into(), v));
        }
        if let Ok(v) = std::env::var("AWS_SESSION_TOKEN") {
            opts.push(("session_token".into(), v));
        }
    }
    let store = store_from_url_opts(url, opts)
        .map_err(|e| format!("build object store for {url}: {e}"))?;
    Ok(Arc::new(DefaultEngineBuilder::new(store).build()))
}

/// Resolve a user-supplied table location to a `Url`. Accepts an existing URL (e.g. `file://`,
/// `s3://`) or a local filesystem path, which is canonicalized into a `file://` directory URL.
pub(crate) fn table_url(path: &str) -> Result<Url, String> {
    if let Ok(mut url) = Url::parse(path) {
        // Treat single-character "schemes" as Windows drive letters, not URL schemes.
        if url.scheme().len() > 1 {
            // The table root is a directory: ensure a trailing slash so the kernel's
            // `Url::join("_delta_log/")` appends rather than replacing the last path segment
            // (without it, `s3://b/a/tbl` + `_delta_log/` resolves to `s3://b/a/_delta_log/`).
            if !url.path().ends_with('/') {
                let with_slash = format!("{}/", url.path());
                url.set_path(&with_slash);
            }
            return Ok(url);
        }
    }
    let abs = std::path::Path::new(path)
        .canonicalize()
        .map_err(|e| format!("canonicalize table path {path}: {e}"))?;
    Url::from_directory_path(&abs)
        .map_err(|()| format!("cannot build a file:// url from {}", abs.display()))
}

/// Write `msg` as a freshly-allocated C string into `*out_err` (when `out_err` is non-null).
/// The caller frees it with [`kdf_string_free`].
///
/// # Safety
/// `out_err`, if non-null, must point to a writable `*mut c_char`.
pub(crate) unsafe fn write_err(out_err: *mut *mut c_char, msg: &str) {
    if out_err.is_null() {
        return;
    }
    let c = CString::new(msg).unwrap_or_default();
    unsafe { *out_err = c.into_raw() };
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

/// A `ColumnName` reduced to its single top-level component (errors if nested).
pub(crate) fn single_col(c: &delta_kernel::expressions::ColumnName) -> Result<String, String> {
    match c.path() {
        [one] => Ok(one.clone()),
        other => Err(format!("expected a top-level column, got {other:?}")),
    }
}

/// Convert one Arrow array cell to a kernel `Scalar` (covers the file-list column types:
/// path strings, sizes, versions).
pub(crate) fn arrow_to_scalar(
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
pub(crate) fn arrow_type_to_kernel(t: &delta_kernel::arrow::datatypes::DataType) -> Result<delta_kernel::schema::DataType, String> {
    use delta_kernel::arrow::datatypes::DataType as A;
    use delta_kernel::schema::DataType as K;
    Ok(match t {
        A::Utf8 => K::STRING,
        A::Int64 => K::LONG,
        A::Int32 => K::INTEGER,
        other => return Err(format!("materialize: unsupported column type {other:?}")),
    })
}

