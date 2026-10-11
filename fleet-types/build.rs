fn main() -> Result<(), Box<dyn std::error::Error>> {
    // protoc is provided by the protoc-bin-vendored crate so no system install
    // is needed (offline/test machines have no protoc on PATH).
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: single-threaded build-script context; no other threads read env.
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/fleet.proto"], &["proto"])?;
    Ok(())
}
