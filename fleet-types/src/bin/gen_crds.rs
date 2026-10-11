//! Generate `deploy/crds/*.yaml` from the `CustomResource` derives.
//!
//! Usage: `cargo run -p fleet-types --bin gen_crds [-- --out <dir>]`
//! Default output: `<workspace>/deploy/crds/` (created if missing).

use fleet_types::crds::{BotRollout, Proxy, XenomitBot};
use kube::core::CustomResourceExt;
use std::path::PathBuf;

fn main() {
    let mut args = std::env::args().skip(1);
    let out = match (args.next().as_deref(), args.next()) {
        (Some("--out"), Some(dir)) => PathBuf::from(dir),
        _ => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("fleet-types has a workspace parent")
            .join("deploy")
            .join("crds"),
    };
    std::fs::create_dir_all(&out).expect("create deploy/crds");

    for (name, crd) in [
        ("xenomitbots", XenomitBot::crd()),
        ("botrollouts", BotRollout::crd()),
        ("proxies", Proxy::crd()),
    ] {
        let yaml = serde_saphyr::to_string(&crd).expect("serialize CRD to YAML");
        let path = out.join(format!("{name}.fleet.xenomit.io.yaml"));
        std::fs::write(&path, yaml).expect("write CRD YAML");
        eprintln!("wrote {}", path.display());
    }
}
