//! Isolated dependency experiment; never a production catalog launcher.
//! The lab verifies immutable native assets before exec and owns kill/reap.
//! Relations and barriers are deliberately synthetic and confer no archive ACK.
use duckdb::{Config, Connection};
use std::{
    fs,
    io::{self, Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    process::ExitCode,
};

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn emit(value: serde_json::Value) -> Result<(), &'static str> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, &value).map_err(|_| "output")?;
    output.write_all(b"\n").map_err(|_| "output")?;
    output.flush().map_err(|_| "output")
}

fn barrier<const N: usize>(expected: &[u8; N]) -> Result<(), &'static str> {
    let mut bytes = [0; N];
    io::stdin()
        .lock()
        .read_exact(&mut bytes)
        .map_err(|_| "barrier")?;
    if &bytes != expected {
        return Err("barrier");
    }
    Ok(())
}

fn counts(connection: &Connection, batch: u64) -> Result<[u64; 3], &'static str> {
    let mut counts = [0; 3];
    for (i, table) in ["facts", "witnesses", "receipts"].iter().enumerate() {
        counts[i] = connection
            .query_row(
                &format!("SELECT count(*)::UBIGINT FROM lake.{table} WHERE batch=?"),
                [batch],
                |row| row.get(0),
            )
            .map_err(|_| "count")?;
    }
    Ok(counts)
}

fn run() -> Result<(), &'static str> {
    let args: Vec<_> = std::env::args().take(8).collect();
    if args.len() != 7 || !matches!(args[1].as_str(), "init" | "write" | "read" | "lost-ack") {
        return Err("arguments");
    }
    let mode = &args[1];
    let bundle = Path::new(&args[2]);
    let temp = Path::new(&args[3]);
    let data = Path::new(&args[4]);
    for path in [bundle, temp, data] {
        if !path.is_absolute() || path.canonicalize().ok().as_deref() != Some(path) {
            return Err("path");
        }
    }
    if fs::read_dir(temp)
        .map_err(|_| "temporary-root")?
        .next()
        .is_some()
    {
        return Err("temporary-root");
    }
    let credentials = Path::new(&args[5]);
    if fs::metadata(credentials).map_err(|_| "credentials")?.len() > 16384 {
        return Err("credentials");
    }
    let dsn = fs::read_to_string(credentials).map_err(|_| "credentials")?;
    if dsn.contains(['\0', '\n', '\r']) {
        return Err("credentials");
    }
    let batch: u64 = args[6].parse().map_err(|_| "batch")?;
    if !(1..=16).contains(&batch) {
        return Err("batch");
    }
    let config = Config::default()
        .enable_autoload_extension(false)
        .and_then(|c| c.threads(1))
        .and_then(|c| c.max_memory("128MiB"))
        .and_then(|c| c.with("temp_directory", &args[3]))
        .and_then(|c| c.with("home_directory", &args[3]))
        .and_then(|c| c.with("secret_directory", &args[3]))
        .and_then(|c| c.with("max_temp_directory_size", "64MiB"))
        .and_then(|c| c.with("allow_unsigned_extensions", "false"))
        .and_then(|c| c.with("allow_community_extensions", "false"))
        .and_then(|c| c.with("allow_persistent_secrets", "false"))
        .map_err(|_| "configuration")?;
    let connection = Connection::open_in_memory_with_flags(config).map_err(|_| "engine")?;
    for name in ["ducklake", "postgres_scanner", "httpfs"] {
        let path = bundle
            .join("extensions")
            .join(format!("{name}.duckdb_extension"));
        connection
            .execute_batch(&format!("LOAD {}", literal(path.to_str().ok_or("path")?)))
            .map_err(|_| "extension")?;
    }
    connection.execute_batch(
        "SET enable_progress_bar=false; SET pg_pool_enable_reaper_thread=false; SET pg_use_ctid_scan=false; SET pg_pool_max_connections=8; SET pg_pool_acquire_mode='try'",
    ).map_err(|_| "settings")?;
    let version: String = connection
        .query_row("SELECT library_version FROM pragma_version()", [], |r| {
            r.get(0)
        })
        .map_err(|_| "version")?;
    if version != "v1.5.5" {
        return Err("version");
    }
    let options = format!(
        "DATA_PATH {}, DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS {}, META_SECRET_STORAGE_TABLE ''{}",
        literal(&args[4]),
        mode == "init",
        if mode == "read" { ", READ_ONLY" } else { "" }
    );
    connection
        .execute_batch(&format!(
            "ATTACH {} AS lake ({options})",
            literal(&format!("ducklake:postgres:{dsn}"))
        ))
        .map_err(|error| {
            // The isolated lab needs the native cause, but native diagnostics
            // may repeat connection secrets. Never put it on either pipe.
            let mut diagnostic = error.to_string().replace(&dsn, "<lab-connection>");
            for secret in dsn
                .split_whitespace()
                .filter_map(|s| s.strip_prefix("password="))
            {
                diagnostic = diagnostic.replace(secret, "<redacted>");
            }
            if let Ok(mut file) = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(temp.join("attach-error.private"))
            {
                let _ = file.write_all(diagnostic.as_bytes());
            }
            "attach"
        })?;
    if mode == "init" {
        connection.execute_batch(
            "BEGIN; CREATE TABLE lake.facts(batch UBIGINT, body VARCHAR); CREATE TABLE lake.witnesses(batch UBIGINT, endpoint VARCHAR); CREATE TABLE lake.receipts(batch UBIGINT, digest BLOB); COMMIT",
        ).map_err(|_| "initialize")?;
        return emit(serde_json::json!({"stage":"initialized","version":version}));
    }
    connection.execute_batch("BEGIN").map_err(|_| "begin")?;
    let before = counts(&connection, batch)?;
    if mode == "read" {
        connection
            .execute_batch("COMMIT")
            .map_err(|_| "read-commit")?;
        return emit(serde_json::json!({"stage":"read","counts":before}));
    }
    emit(serde_json::json!({"stage":"ready","counts":before}))?;
    barrier(b"publish\n")?;
    connection
        .execute("INSERT INTO lake.facts VALUES (?, 'canary-ß')", [batch])
        .map_err(|_| "fact")?;
    connection
        .execute(
            "INSERT INTO lake.witnesses VALUES (?, 'wss://example.invalid')",
            [batch],
        )
        .map_err(|_| "witness")?;
    connection
        .execute(
            "INSERT INTO lake.receipts VALUES (?, from_hex('00ff'))",
            [batch],
        )
        .map_err(|_| "receipt")?;
    connection.execute_batch("COMMIT").map_err(|_| "commit")?;
    if mode == "lost-ack" {
        // Test-only observation, deliberately before the caller-facing result.
        emit(serde_json::json!({"stage":"before-ack"}))?;
        barrier(b"ack\n")?;
    }
    emit(serde_json::json!({"stage":"committed"}))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(phase) => {
            // Native errors can embed the entire DSN: expose only fixed phases.
            eprintln!("catalog fence experiment failed: {phase}");
            ExitCode::FAILURE
        }
    }
}
