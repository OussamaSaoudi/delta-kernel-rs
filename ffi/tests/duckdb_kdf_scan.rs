//! Direct test of the Phase-1 DuckDB plan-driven scan FFI (`kdf_*`).
//!
//! Exercises the exact C ABI the `duckdb-delta` extension calls: drive the kernel's declarative
//! scan-metadata state machine over a real on-disk Delta table and enumerate the surviving files.
//! Run with: `cargo test -p delta_kernel_ffi --features duckdb --test duckdb_kdf_scan`.
#![cfg(feature = "duckdb")]

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use delta_kernel_ffi::duckdb::{
    kdf_scan_file_count, kdf_scan_file_path, kdf_scan_file_size, kdf_scan_free, kdf_scan_open,
    kdf_string_free,
};

/// Build a tiny Delta table fixture by copying the kernel's `table-without-dv-small` test table
/// into a temp dir (so the test is hermetic and doesn't depend on /tmp/m1_smoke).
fn fixture() -> tempfile::TempDir {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data/table-without-dv-small");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dst = tmp.path().join("table-without-dv-small");
    copy_dir(&src, &dst);
    tmp
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for entry in std::fs::read_dir(src).expect("read_dir") {
        let entry = entry.expect("entry");
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if path.is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).expect("copy");
        }
    }
}

#[test]
fn kdf_scan_enumerates_files_via_plan() {
    let tmp = fixture();
    let table = tmp.path().join("table-without-dv-small");
    let table_str = table.to_str().expect("utf8 path");

    let c_path = CString::new(table_str).expect("cstring");
    let mut err: *mut c_char = ptr::null_mut();
    let scan = unsafe { kdf_scan_open(c_path.as_ptr(), table_str.len(), &mut err) };

    if scan.is_null() {
        let msg = if err.is_null() {
            "<null>".to_string()
        } else {
            unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned()
        };
        unsafe { kdf_string_free(err) };
        panic!("kdf_scan_open failed: {msg}");
    }
    assert!(err.is_null(), "out_err should be null on success");

    let count = unsafe { kdf_scan_file_count(scan) };
    assert!(count > 0, "plan-driven scan should enumerate at least one file");

    for i in 0..count {
        let path_ptr = unsafe { kdf_scan_file_path(scan, i) };
        assert!(!path_ptr.is_null(), "file {i} path is null");
        let path = unsafe { CStr::from_ptr(path_ptr) }.to_string_lossy().into_owned();
        assert!(
            path.ends_with(".parquet"),
            "file {i} should be a parquet data file, got {path}"
        );
        let size = unsafe { kdf_scan_file_size(scan, i) };
        assert!(size > 0, "file {i} size should be positive, got {size}");
        println!("plan-driven scan file {i}: {path} ({size} bytes)");
    }

    // Out-of-range queries are safe.
    assert!(unsafe { kdf_scan_file_path(scan, count) }.is_null());
    assert_eq!(unsafe { kdf_scan_file_size(scan, count) }, -1);

    unsafe { kdf_scan_free(scan) };
}

#[test]
fn kdf_scan_open_reports_error_for_missing_table() {
    let bad = "/nonexistent/delta/table/path";
    let c_path = CString::new(bad).expect("cstring");
    let mut err: *mut c_char = ptr::null_mut();
    let scan = unsafe { kdf_scan_open(c_path.as_ptr(), bad.len(), &mut err) };
    assert!(scan.is_null(), "scan over a missing table must fail");
    assert!(!err.is_null(), "error string must be populated on failure");
    unsafe { kdf_string_free(err) };
}
