use rustc_version::{version_meta, Channel};

fn main() {
    println!("cargo::rustc-check-cfg=cfg(NIGHTLY_CHANNEL)");
    // note if we're on the nightly channel so we can enable doc_auto_cfg if so
    if let Channel::Nightly = version_meta().unwrap().channel {
        println!("cargo:rustc-cfg=NIGHTLY_CHANNEL");
    }

    // Compile protobuf definitions for plan/state machine communication
    compile_protos();
}

fn compile_protos() {
    let proto_dir = "../proto";
    let protos = &[
        format!("{}/schema.proto", proto_dir),
        format!("{}/function_registry.proto", proto_dir),
        format!("{}/plan.proto", proto_dir),
        format!("{}/state_machine.proto", proto_dir),
    ];

    // Tell cargo to recompile if proto files change
    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto);
    }

    // Compile to OUT_DIR (standard prost approach)
    prost_build::Config::new()
        .compile_protos(protos, &[proto_dir])
        .expect("Failed to compile protobuf files");
}
