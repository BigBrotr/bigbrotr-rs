use sha2::{Digest, Sha256};
use std::{env, error::Error, fs, io::Read, path::Path};

fn verify(path: &Path, expected: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() != expected["bytes"].as_u64().ok_or("asset size")? {
        return Err("native archive build input has wrong file type or size".into());
    }
    let mut input = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut block = [0; 64 * 1024];
    let mut total = 0u64;
    loop {
        let count = input.read(&mut block)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .filter(|size| *size <= metadata.len())
            .ok_or("native input grew beyond pinned size")?;
        hash.update(&block[..count]);
    }
    let actual: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if actual != expected["sha256"].as_str().ok_or("asset hash")? {
        return Err("native archive build input hash mismatch".into());
    }
    println!("cargo::rerun-if-changed={}", path.display());
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let manifest = Path::new("../../contracts/release/archive-engine.json");
    println!("cargo::rerun-if-changed={}", manifest.display());
    println!("cargo::rerun-if-env-changed=DUCKDB_LIB_DIR");
    println!("cargo::rerun-if-env-changed=DUCKDB_INCLUDE_DIR");
    for variable in ["DUCKDB_DOWNLOAD_LIB", "DUCKDB_STATIC"] {
        println!("cargo::rerun-if-env-changed={variable}");
        if env::var_os(variable).is_some() {
            return Err(
                "implicit DuckDB download/static override is not a selected build input".into(),
            );
        }
    }
    let registry: serde_json::Value = serde_json::from_slice(&fs::read(manifest)?)?;
    let target = env::var("TARGET")?;
    let selected = registry["platforms"]
        .get(&target)
        .ok_or("no pinned archive engine assets for target")?;
    let library_dir = env::var_os("DUCKDB_LIB_DIR")
        .ok_or("set DUCKDB_LIB_DIR to the verified archive bundle's lib directory")?;
    let library_dir = Path::new(&library_dir);
    if !library_dir.is_absolute() {
        return Err("DUCKDB_LIB_DIR must be absolute".into());
    }
    let include = env::var_os("DUCKDB_INCLUDE_DIR");
    let include_dir = include.as_deref().map(Path::new).unwrap_or(library_dir);
    for (directory, name) in [(library_dir, "library"), (include_dir, "header")] {
        let asset = &selected[name];
        verify(
            &directory.join(asset["name"].as_str().ok_or("asset name")?),
            asset,
        )?;
    }
    let output = env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?;
    let spec = serde_json::json!({
        "target": target, "engine_version": registry["engine_version"],
        "engine_source_id": registry["engine_source_id"],
        "library": selected["library"], "extensions": selected["extensions"]
    });
    fs::write(
        Path::new(&output).join("archive-bundle.json"),
        serde_json::to_vec(&spec)?,
    )?;
    // Installed workers resolve only the separately packaged library next to
    // their executable. Development tests set an explicit loader path.
    let runpath = if target.ends_with("apple-darwin") {
        "@loader_path/lib"
    } else {
        "$ORIGIN/lib"
    };
    println!("cargo::rustc-link-arg=-Wl,-rpath,{runpath}");
    Ok(())
}
