extern crate cbindgen;

use std::env;
use std::path::{Path, PathBuf};

use cbindgen::{Config, Language};

fn get_target_dir(manifest_dir: &str) -> PathBuf {
    if let Ok(target) = env::var("CARGO_TARGET_DIR") {
        // allow a CARGO_TARGET_DIR var that could be set via e.g. cmake to override where we put
        // the header files
        PathBuf::from(target)
    } else {
        let mut manifest_dir = PathBuf::from(manifest_dir);
        manifest_dir.pop(); // go up, since we're a sub-crate
        manifest_dir.join("target").join("ffi-headers")
    }
}

fn main() {
    // Re-run (and thus regenerate the cbindgen headers) whenever the crate source changes. Without
    // this, emitting any `rerun-if-changed` below would pin build.rs to only those paths and leave
    // the generated headers stale after src edits.
    println!("cargo:rerun-if-changed=src");

    // Phase-2 (DuckDB plan-based scan): compile the plan-IR proto schema into Rust types when the
    // `duckdb` feature is enabled. Requires `protoc` on PATH. Generated files land in OUT_DIR and
    // are included by `src/duckdb/proto.rs`.
    if env::var("CARGO_FEATURE_DUCKDB").is_ok() {
        println!("cargo:rerun-if-changed=proto/plan.proto");
        println!("cargo:rerun-if-changed=proto/expressions.proto");
        println!("cargo:rerun-if-changed=proto/schema.proto");
        prost_build::compile_protos(
            &[
                "proto/plan.proto",
                "proto/expressions.proto",
                "proto/schema.proto",
            ],
            &["proto"],
        )
        .expect("prost-build: failed to compile plan-IR proto schema");

        // Also generate the C++ proto structs so the plan IR is a KERNEL-owned SDK artifact (single
        // source of truth, no encoder/decoder version skew). They land in target/ffi-headers next to
        // the cbindgen header; the DuckDB engine consumes them transitively (include + link
        // libprotobuf) and never runs protoc itself. Uses the same `protoc` prost-build resolves.
        let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set");
        let cpp_out = get_target_dir(&crate_dir).join("proto-cpp");
        std::fs::create_dir_all(&cpp_out).expect("create proto-cpp out dir");
        // Same protoc prost-build uses: honor $PROTOC, else fall back to PATH.
        let protoc = env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
        let status = std::process::Command::new(&protoc)
            .arg(format!("--cpp_out={}", cpp_out.display()))
            .arg("--proto_path=proto")
            .args(["proto/plan.proto", "proto/expressions.proto", "proto/schema.proto"])
            .status()
            .expect("failed to run protoc for C++ codegen");
        assert!(status.success(), "protoc --cpp_out failed with {status}");
    }

    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set");
    let package_name = env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME should be set");
    let target_dir = get_target_dir(crate_dir.as_str());
    let cbindgen_toml = Path::new(&crate_dir).join("cbindgen.toml");
    let mut config = Config::from_file(&cbindgen_toml)
        .unwrap_or_else(|_| panic!("Couldn't find {}", cbindgen_toml.display()));

    // generate cxx bindings
    let output_file_hpp = target_dir
        .join(format!("{package_name}.hpp"))
        .display()
        .to_string();
    let mut config_hpp = config.clone();
    config_hpp.language = Language::Cxx;
    cbindgen::generate_with_config(&crate_dir, config_hpp)
        .expect("generate_with_config should have worked for Cxx")
        .write_to_file(output_file_hpp);

    // generate c bindings
    let output_file_h = target_dir
        .join(format!("{package_name}.h"))
        .display()
        .to_string();
    config.language = Language::C;
    cbindgen::generate_with_config(&crate_dir, config)
        .expect("generate_with_config should have worked for C")
        .write_to_file(output_file_h);

    // Ship the hand-written RAII C++ SDK header alongside the generated bindings. It layers typed,
    // move-only owners (ScanStateMachine, KernelString, KernelBytes) over the flat `kdf_*` C ABI so
    // the engine drives the scan SM without hand-managing owning pointers or error strings. Copied
    // (not generated) — it's authored source under ffi/include, and the kernel is its single owner.
    let sdk_src = Path::new(&crate_dir).join("include").join("delta_kernel_sdk.hpp");
    println!("cargo:rerun-if-changed={}", sdk_src.display());
    std::fs::copy(&sdk_src, target_dir.join("delta_kernel_sdk.hpp"))
        .expect("copy delta_kernel_sdk.hpp into ffi-headers");
}
