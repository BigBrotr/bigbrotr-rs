//! Isolated native DDL negative controls. Never an application migration API.
use duckdb::{Config, Connection};
use std::{fs, path::Path};
fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 2);
    let (database, mode) = (&args[0], &args[1]);
    assert!(
        database.starts_with("bbcat_")
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_')
    );
    let temp = tempfile::tempdir_in("/out").unwrap();
    let config = Config::default()
        .enable_autoload_extension(false)
        .unwrap()
        .threads(1)
        .unwrap()
        .max_memory("128MiB")
        .unwrap()
        .with("allow_persistent_secrets", "false")
        .unwrap()
        .with("allow_unsigned_extensions", "false")
        .unwrap()
        .with("temp_directory", temp.path().to_str().unwrap())
        .unwrap();
    let connection = Connection::open_in_memory_with_flags(config).unwrap();
    for extension in ["httpfs", "postgres_scanner", "ducklake"] {
        let path = format!("/out/archive-worker/extensions/{extension}.duckdb_extension");
        assert!(Path::new(&path).is_file());
        connection.execute_batch(&format!("LOAD '{path}'")).unwrap();
    }
    let password = fs::read_to_string("/secrets/BootstrapMixed").unwrap();
    assert!(password.bytes().all(|b| b.is_ascii_hexdigit()));
    let credentials = format!(
        "CREATE TEMPORARY SECRET fixture (TYPE postgres, URI 'host=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={password} sslmode=disable');"
    );
    assert!(
        connection.execute_batch(&credentials).is_ok(),
        "private secret configuration"
    );
    connection.execute_batch("SET pg_pool_enable_reaper_thread=false; SET pg_use_ctid_scan=false; SET pg_pool_max_connections=8; SET pg_pool_acquire_mode='try'").unwrap();
    let attach = format!(
        "ATTACH 'ducklake:postgres:' AS archive (DATA_PATH '/out/schema_{database}/', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS false, META_SECRET_STORAGE_TABLE '', META_SECRET 'fixture')"
    );
    assert!(connection.execute_batch(&attach).is_ok(), "native attach");
    let ddl = match mode.as_str() {
        "add" => "ALTER TABLE archive.main.events ADD COLUMN canary STRUCT(x UBIGINT)",
        "rollback" => {
            "BEGIN; ALTER TABLE archive.main.events ADD COLUMN canary STRUCT(x UBIGINT); ROLLBACK"
        }
        "drop" => "DROP TABLE archive.main.archive_commits",
        _ => panic!("unknown DDL control"),
    };
    connection.execute_batch(ddl).unwrap();
    connection
        .execute_batch("DETACH archive; DROP SECRET fixture")
        .unwrap();
    println!("{}", serde_json::json!({"status":"PASS","case":mode}));
}
