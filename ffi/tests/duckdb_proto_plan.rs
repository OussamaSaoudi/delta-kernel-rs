//! Phase-2 round-trip test: drive the scan-metadata state machine to its terminal `ResultPlan`,
//! serialize it to proto over the FFI (`kdf_scan_result_plan_proto`), decode the bytes back with
//! prost, and validate the plan structure. Proves the kernel IR -> proto bridge end-to-end.
//!
//! Run with: `cargo test -p delta_kernel_ffi --features duckdb --test duckdb_proto_plan`.
#![cfg(feature = "duckdb")]

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use delta_kernel_ffi::duckdb::proto::plan::{operator::Op, ResultPlan};
use delta_kernel_ffi::duckdb::proto::Message;
use delta_kernel_ffi::duckdb::{kdf_bytes_free, kdf_scan_result_plan_proto, kdf_string_free};

fn fixture() -> tempfile::TempDir {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data/table-without-dv-small");
    let tmp = tempfile::tempdir().expect("tempdir");
    copy_dir(&src, &tmp.path().join("table-without-dv-small"));
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

fn op_name(node: &delta_kernel_ffi::duckdb::proto::plan::PlanNode) -> &'static str {
    match node.op.as_ref().and_then(|o| o.op.as_ref()) {
        Some(Op::ListFiles(_)) => "ListFiles",
        Some(Op::ScanParquet(_)) => "ScanParquet",
        Some(Op::ScanJson(_)) => "ScanJson",
        Some(Op::Values(_)) => "Values",
        Some(Op::Project(_)) => "Project",
        Some(Op::Filter(_)) => "Filter",
        Some(Op::Load(_)) => "Load",
        Some(Op::MaxByVersion(_)) => "MaxByVersion",
        Some(Op::EquiJoin(_)) => "EquiJoin",
        Some(Op::UnionAll(_)) => "UnionAll",
        None => "<none>",
    }
}

#[test]
fn scan_metadata_result_plan_round_trips_through_proto() {
    let tmp = fixture();
    let table = tmp.path().join("table-without-dv-small");
    let table_str = table.to_str().expect("utf8 path");

    let c_path = CString::new(table_str).expect("cstring");
    let mut out_len: usize = 0;
    let mut err: *mut c_char = ptr::null_mut();
    let buf = unsafe {
        kdf_scan_result_plan_proto(c_path.as_ptr(), table_str.len(), &mut out_len, &mut err)
    };
    if buf.is_null() {
        let msg = if err.is_null() {
            "<null>".to_string()
        } else {
            unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned()
        };
        unsafe { kdf_string_free(err) };
        panic!("kdf_scan_result_plan_proto failed: {msg}");
    }
    assert!(out_len > 0, "proto bytes should be non-empty");

    let bytes = unsafe { std::slice::from_raw_parts(buf, out_len) }.to_vec();
    unsafe { kdf_bytes_free(buf, out_len) };

    // Decode the proto bytes back into the generated ResultPlan and validate structure.
    let plan = ResultPlan::decode(&bytes[..]).expect("decode ResultPlan proto");
    let inner = plan.plan.expect("plan present");
    assert!(!inner.nodes.is_empty(), "plan should have nodes");
    assert!(
        (plan.result as usize) < inner.nodes.len(),
        "result index {} out of range (len {})",
        plan.result,
        inner.nodes.len()
    );

    for (i, n) in inner.nodes.iter().enumerate() {
        println!("node {i}: {} inputs={:?} output={}", op_name(n), n.inputs, n.output);
    }

    // The scan-metadata terminal is the `project_scan_file_row` projection.
    let terminal = &inner.nodes[plan.result as usize];
    assert_eq!(
        op_name(terminal),
        "Project",
        "scan-metadata terminal should be a Project (scan_file_row)"
    );

    // Every node's inputs must reference earlier nodes (SSA / topo order).
    for (i, n) in inner.nodes.iter().enumerate() {
        for &input in &n.inputs {
            assert!(
                (input as usize) < i,
                "node {i} input {input} is not strictly earlier"
            );
        }
    }

    println!(
        "round-trip OK: {} nodes, terminal=Project, {} proto bytes",
        inner.nodes.len(),
        out_len
    );
}
