use serde_json::json;
use sha2::{Digest, Sha256};
use std::{env, error::Error, fs, path::PathBuf, process::Command};

fn source_identity() -> Result<serde_json::Value, Box<dyn Error>> {
    let revision = env::var("BIGBROTR_SOURCE_REVISION").ok();
    let tree = env::var("BIGBROTR_SOURCE_TREE").ok();
    let manifest = env::var("BIGBROTR_SOURCE_MANIFEST_SHA256").ok();
    for name in [
        "BIGBROTR_SOURCE_REVISION",
        "BIGBROTR_SOURCE_TREE",
        "BIGBROTR_SOURCE_MANIFEST_SHA256",
    ] {
        println!("cargo::rerun-if-env-changed={name}");
    }
    match (revision, tree, manifest) {
        (None, None, None) => Ok(json!({"state": "unrecorded", "revision": null, "tree": null, "dirty": null, "manifest_sha256": null})),
        (Some(revision), Some(tree), Some(manifest))
            if [&revision, &tree].iter().all(|value| {
                value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }) && manifest.len() == 64 && manifest.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            // These are builder assertions, not an independent authenticity check.
            Ok(json!({"state": "builder-supplied", "revision": revision, "tree": tree, "dirty": false, "manifest_sha256": manifest}))
        }
        _ => Err("provide BIGBROTR_SOURCE_REVISION/TREE as lowercase Git SHA-1 and BIGBROTR_SOURCE_MANIFEST_SHA256 as lowercase SHA-256, or none of them".into()),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("missing manifest path")?)
        .join("../..");
    let mut hashes = serde_json::Map::new();
    for name in [
        "Cargo.lock",
        "rust-toolchain.toml",
        "contracts/release/dependencies.json",
        "contracts/protocol/versions.json",
        "contracts/protocol/nip01-event.json",
    ] {
        let path = root.join(name);
        println!("cargo::rerun-if-changed={}", path.display());
        let hash: String = Sha256::digest(fs::read(path)?)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        hashes.insert(name.into(), hash.into());
    }
    println!("cargo::rerun-if-changed=build.rs");
    let versions: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("contracts/protocol/versions.json"))?)?;
    let nonempty_text = |value: &serde_json::Value| value.as_str().is_some_and(|s| !s.is_empty());
    if versions["format"] != "bigbrotr-protocol-versions/1"
        || !versions["capabilities"]
            .as_array()
            .is_some_and(|values| values.iter().all(nonempty_text))
        || !["schemas", "protocols"].iter().all(|name| {
            versions[name].as_object().is_some_and(|values| {
                values
                    .iter()
                    .all(|(key, value)| !key.is_empty() && nonempty_text(value))
            })
        })
        || !nonempty_text(&versions["scope"])
    {
        return Err("invalid protocol version registry format or fields".into());
    }
    let compiler = Command::new(env::var_os("RUSTC").ok_or("missing compiler")?)
        .arg("-vV")
        .output()?;
    if !compiler.status.success() {
        return Err("could not identify Rust compiler".into());
    }
    let target = env::var("TARGET")?;
    println!("cargo::rerun-if-env-changed=BIGBROTR_BUILD_PROFILE");
    let profile = env::var("BIGBROTR_BUILD_PROFILE").ok();
    if profile
        .as_deref()
        .is_some_and(|profile| !matches!(profile, "dev" | "release"))
    {
        return Err("unsupported BIGBROTR_BUILD_PROFILE assertion".into());
    }
    let info = json!({
        "format": "bigbrotr-build/1",
        "product": "BigBrotr",
        "version": env::var("CARGO_PKG_VERSION")?,
        "source": source_identity()?,
        "rustc": String::from_utf8(compiler.stdout)?.trim(),
        "target": target,
        "profile_assertion": profile,
        "optimization": env::var("OPT_LEVEL")?,
        "debug_info": env::var("DEBUG")?,
        "inputs_sha256": hashes,
        "capabilities": versions["capabilities"],
        "protocol_registry_format": versions["format"],
        "capability_scope": versions["scope"],
        "schemas": versions["schemas"],
        "protocols": versions["protocols"]
    });
    let output = PathBuf::from(env::var_os("OUT_DIR").ok_or("missing build output path")?);
    fs::write(
        output.join("build-info.json"),
        serde_json::to_vec_pretty(&info)?,
    )?;
    println!("cargo::rustc-env=BIGBROTR_BUILD_TARGET={target}");
    Ok(())
}
