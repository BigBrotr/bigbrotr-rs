use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, FlushOptions,
    IteratorMode, Options, WriteBatch, WriteBufferManager, WriteOptions,
};
use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    thread,
    time::Duration,
};

const COLUMN_FAMILIES: [&str; 3] = ["default", "facts", "receipts"];
const CACHE_BYTES: usize = 32 * 1024 * 1024;
const WRITE_BUFFER_MANAGER_BYTES: usize = 16 * 1024 * 1024;
const WRITE_BUFFER_BYTES: usize = 2 * 1024 * 1024;
const MAX_TOTAL_WAL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_OPEN_FILES: i32 = 64;
const MAX_BACKGROUND_JOBS: i32 = 2;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn registry() -> &'static Mutex<HashSet<PathBuf>> {
    static REGISTRY: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashSet::new()))
}

struct OwnerDb {
    db: Option<DB>,
    canonical_path: PathBuf,
}

impl OwnerDb {
    fn create(path: &Path) -> Result<Self> {
        if path.exists() {
            return Err(format!(
                "refusing to initialize an existing database path: {}",
                path.display()
            )
            .into());
        }
        fs::create_dir_all(path)?;
        Self::open(path, true)
    }

    fn reopen(path: &Path) -> Result<Self> {
        if !path.join("CURRENT").is_file() {
            return Err(format!(
                "refusing to reopen database without CURRENT: {}",
                path.display()
            )
            .into());
        }
        Self::open(path, false)
    }

    fn open(path: &Path, create: bool) -> Result<Self> {
        let canonical_path = fs::canonicalize(path)?;
        {
            let mut owned = registry().lock().map_err(|_| "owner registry poisoned")?;
            if !owned.insert(canonical_path.clone()) {
                return Err(format!(
                    "canonical database directory already owned in this process: {}",
                    canonical_path.display()
                )
                .into());
            }
        }

        match Self::open_claimed(&canonical_path, create) {
            Ok(db) => Ok(Self {
                db: Some(db),
                canonical_path,
            }),
            Err(error) => {
                registry()
                    .lock()
                    .map_err(|_| "owner registry poisoned")?
                    .remove(&canonical_path);
                Err(error)
            }
        }
    }

    fn open_claimed(path: &Path, create: bool) -> Result<DB> {
        if !create {
            let mut actual = DB::list_cf(&Options::default(), path)?;
            actual.sort();
            let mut required = COLUMN_FAMILIES.map(str::to_owned).to_vec();
            required.sort();
            if actual != required {
                return Err(format!(
                    "column families mismatch: expected {required:?}, observed {actual:?}"
                )
                .into());
            }
        }

        let cache = Cache::new_lru_cache(CACHE_BYTES);
        let write_buffers = WriteBufferManager::new_write_buffer_manager_with_cache(
            WRITE_BUFFER_MANAGER_BYTES,
            true,
            cache.clone(),
        );
        let descriptors = COLUMN_FAMILIES.map(|name| {
            ColumnFamilyDescriptor::new(name, column_family_options(&cache, &write_buffers))
        });
        let mut db_options = Options::default();
        db_options.create_if_missing(create);
        db_options.create_missing_column_families(create);
        db_options.set_error_if_exists(create);
        db_options.set_use_fsync(true);
        db_options.set_atomic_flush(true);
        db_options.set_max_total_wal_size(MAX_TOTAL_WAL_BYTES);
        db_options.set_max_open_files(MAX_OPEN_FILES);
        db_options.set_max_background_jobs(MAX_BACKGROUND_JOBS);
        DB::open_cf_descriptors(&db_options, path, descriptors).map_err(Into::into)
    }

    fn db(&self) -> &DB {
        self.db
            .as_ref()
            .expect("database exists until OwnerDb drop")
    }

    fn put_token(&self, token: &str) -> Result<()> {
        let default = self.handle("default")?;
        let facts = self.handle("facts")?;
        let receipts = self.handle("receipts")?;
        let mut batch = WriteBatch::default();
        batch.put_cf(default, token.as_bytes(), format!("sequence:{token}"));
        batch.put_cf(facts, token.as_bytes(), format!("fact:{token}"));
        batch.put_cf(receipts, token.as_bytes(), format!("receipt:{token}"));
        self.write_synced(batch)
    }

    fn write_synced(&self, batch: WriteBatch) -> Result<()> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        options.disable_wal(false);
        self.db().write_opt(batch, &options).map_err(Into::into)
    }

    fn values(&self, token: &str) -> Result<[Option<Vec<u8>>; 3]> {
        Ok([
            self.db().get_cf(self.handle("default")?, token)?,
            self.db().get_cf(self.handle("facts")?, token)?,
            self.db().get_cf(self.handle("receipts")?, token)?,
        ])
    }

    fn mask(&self, token: &str) -> Result<String> {
        Ok(self
            .values(token)?
            .iter()
            .map(|value| if value.is_some() { '1' } else { '0' })
            .collect())
    }

    fn handle(&self, name: &str) -> Result<&rocksdb::ColumnFamily> {
        self.db()
            .cf_handle(name)
            .ok_or_else(|| format!("required private column-family handle missing: {name}").into())
    }
}

impl Drop for OwnerDb {
    fn drop(&mut self) {
        drop(self.db.take());
        if let Ok(mut owned) = registry().lock() {
            owned.remove(&self.canonical_path);
        }
    }
}

fn column_family_options(cache: &Cache, write_buffers: &WriteBufferManager) -> Options {
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_block_size(16 * 1024);
    let mut options = Options::default();
    options.set_block_based_table_factory(&table);
    options.set_write_buffer_manager(write_buffers);
    options.set_write_buffer_size(WRITE_BUFFER_BYTES);
    options.set_max_write_buffer_number(2);
    options.set_compression_type(DBCompressionType::Zstd);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);
    options
}

fn barrier(text: &str) -> Result<()> {
    println!("{text}");
    io::stdout().flush()?;
    Ok(())
}

fn command_write_barrier(path: &Path, token: &str, phase: &str) -> Result<()> {
    let db = OwnerDb::create(path)?;
    match phase {
        "before-call" => barrier("BEFORE_CALL")?,
        "after-ok" => {
            db.put_token(token)?;
            barrier("AFTER_OK_UNKNOWN")?;
        }
        _ => return Err(format!("unknown barrier phase: {phase}").into()),
    }
    thread::sleep(Duration::from_secs(60));
    Ok(())
}

fn command_write_loop(path: &Path, prefix: &str) -> Result<()> {
    let db = OwnerDb::create(path)?;
    barrier("LOOP_READY")?;
    for index in 0_u64.. {
        db.put_token(&format!("{prefix}{index:016}"))?;
    }
    unreachable!()
}

fn command_inspect(path: &Path, token: &str) -> Result<()> {
    let db = OwnerDb::reopen(path)?;
    let values = db.values(token)?;
    let display = |value: &Option<Vec<u8>>| {
        value
            .as_deref()
            .map(String::from_utf8_lossy)
            .map(|value| value.into_owned())
            .unwrap_or_else(|| "-".to_owned())
    };
    println!(
        "{}\t{}\t{}\t{}",
        db.mask(token)?,
        display(&values[0]),
        display(&values[1]),
        display(&values[2])
    );
    Ok(())
}

fn command_scan(path: &Path, prefix: &str) -> Result<()> {
    let db = OwnerDb::reopen(path)?;
    let mut observed: BTreeMap<Vec<u8>, [Option<Vec<u8>>; 3]> = BTreeMap::new();
    for (index, name) in COLUMN_FAMILIES.iter().enumerate() {
        for item in db.db().iterator_cf(db.handle(name)?, IteratorMode::Start) {
            let (key, value) = item?;
            if key.starts_with(prefix.as_bytes()) {
                observed.entry(key.into_vec()).or_default()[index] = Some(value.into_vec());
            }
        }
    }
    for (token, values) in observed {
        let token = String::from_utf8(token)?;
        let mask: String = values
            .iter()
            .map(|value| if value.is_some() { '1' } else { '0' })
            .collect();
        let display = |value: Option<Vec<u8>>| -> Result<String> {
            value.map_or_else(|| Ok("-".to_owned()), |bytes| Ok(String::from_utf8(bytes)?))
        };
        let [default, facts, receipts] = values;
        println!(
            "{token}\t{mask}\t{}\t{}\t{}",
            display(default)?,
            display(facts)?,
            display(receipts)?
        );
    }
    Ok(())
}

fn assert_values(values: [Option<Vec<u8>>; 3], token: &str, present: bool) -> Result<()> {
    let expected = [
        format!("sequence:{token}").into_bytes(),
        format!("fact:{token}").into_bytes(),
        format!("receipt:{token}").into_bytes(),
    ];
    for (actual, expected) in values.iter().zip(expected) {
        if present {
            if actual.as_deref() != Some(expected.as_slice()) {
                return Err("snapshot/current value differs from literal expected value".into());
            }
        } else if actual.is_some() {
            return Err("snapshot unexpectedly observed later value".into());
        }
    }
    Ok(())
}

fn command_snapshot(path: &Path) -> Result<()> {
    {
        let db = OwnerDb::create(path)?;
        db.put_token("snapshot-before")?;
        let snapshot = db.db().snapshot();
        db.put_token("snapshot-after")?;
        let snapshot_values = |token: &str| -> Result<[Option<Vec<u8>>; 3]> {
            Ok([
                snapshot.get_cf(db.handle("default")?, token)?,
                snapshot.get_cf(db.handle("facts")?, token)?,
                snapshot.get_cf(db.handle("receipts")?, token)?,
            ])
        };
        assert_values(snapshot_values("snapshot-before")?, "snapshot-before", true)?;
        assert_values(snapshot_values("snapshot-after")?, "snapshot-after", false)?;
        assert_values(db.values("snapshot-after")?, "snapshot-after", true)?;
    }
    let reopened = OwnerDb::reopen(path)?;
    assert_values(reopened.values("snapshot-before")?, "snapshot-before", true)?;
    assert_values(reopened.values("snapshot-after")?, "snapshot-after", true)?;
    println!("SNAPSHOT_BEFORE_AFTER_REOPEN_OK");
    Ok(())
}

fn command_hold(path: &Path) -> Result<()> {
    let _db = OwnerDb::create(path)?;
    barrier("LOCK_HELD")?;
    thread::sleep(Duration::from_secs(60));
    Ok(())
}

fn command_alias_control(path: &Path, alias: &Path) -> Result<()> {
    let _db = OwnerDb::reopen(path)?;
    let error = match OwnerDb::reopen(alias) {
        Ok(_) => return Err("canonical alias unexpectedly acquired a second owner".into()),
        Err(error) => error,
    };
    if !error.to_string().contains("already owned in this process") {
        return Err(format!("alias refusal came from unexpected boundary: {error}").into());
    }
    println!("CANONICAL_ALIAS_REFUSED");
    Ok(())
}

fn command_seed_schema(path: &Path, layout: &str) -> Result<()> {
    let names: &[&str] = match layout {
        "expected" => &COLUMN_FAMILIES,
        "missing" => &["default", "facts"],
        "unknown" => &["default", "facts", "receipts", "unexpected"],
        _ => return Err(format!("unknown schema layout: {layout}").into()),
    };
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let descriptors = names
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()));
    drop(DB::open_cf_descriptors(&options, path, descriptors)?);
    Ok(())
}

fn command_compression(path: &Path) -> Result<()> {
    let db = OwnerDb::create(path)?;
    let facts = db.handle("facts")?;
    let receipts = db.handle("receipts")?;
    let payload = vec![b'z'; 16 * 1024];
    let mut batch = WriteBatch::default();
    for index in 0..128_u32 {
        let key = format!("compression-{index:08}");
        batch.put_cf(facts, key.as_bytes(), &payload);
        batch.put_cf(receipts, key.as_bytes(), &payload);
    }
    db.write_synced(batch)?;
    let mut flush = FlushOptions::default();
    flush.set_wait(true);
    db.db().flush_cfs_opt(&[facts, receipts], &flush)?;
    let live = db.db().live_files()?;
    let mut fact_files = 0;
    let mut receipt_files = 0;
    for file in &live {
        let counter = match file.column_family_name.as_str() {
            "facts" => &mut fact_files,
            "receipts" => &mut receipt_files,
            _ => continue,
        };
        *counter += 1;
        let path = db.canonical_path.join(file.name.trim_start_matches('/'));
        let bytes = fs::read(&path)?;
        // RocksDB 11.8.1 stores the exact per-SST compression-name property as
        // BuiltinV2;<hex compression types>;. CompressionType kZSTD is 0x07.
        if !bytes
            .windows(b"BuiltinV2;07;".len())
            .any(|window| window == b"BuiltinV2;07;")
        {
            return Err(format!(
                "live SST {} lacks the exact RocksDB 11.8.1 ZSTD metadata marker",
                path.display()
            )
            .into());
        }
    }
    if fact_files == 0 || receipt_files == 0 {
        return Err("forced flush produced no live SST for a required CF".into());
    }
    println!(
        "ZSTD_SST_OK\tfact_files={fact_files}\treceipt_files={receipt_files}\tcache_bytes={CACHE_BYTES}\twrite_buffer_manager_bytes={WRITE_BUFFER_MANAGER_BYTES}\twrite_buffer_bytes_per_cf={WRITE_BUFFER_BYTES}\tmax_total_wal_bytes={MAX_TOTAL_WAL_BYTES}\tmax_open_files={MAX_OPEN_FILES}\tmax_background_jobs={MAX_BACKGROUND_JOBS}"
    );
    Ok(())
}

fn command_identity() {
    println!("rocksdb-crate=0.25.0");
    println!("librocksdb-sys=0.19.0+11.8.1");
    println!("bundled-rocksdb=11.8.1");
    println!("features=bindgen-runtime,zstd,static-implicit");
    println!("sync=true;disable_wal=false;use_fsync=true;atomic_flush=true");
}

fn required<'a>(args: &'a [String], index: usize, name: &str) -> Result<&'a str> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| format!("missing {name}").into())
}

fn execute(args: &[String]) -> Result<()> {
    match required(args, 1, "command")? {
        "write-barrier" => command_write_barrier(
            Path::new(required(args, 2, "database path")?),
            required(args, 3, "token")?,
            required(args, 4, "phase")?,
        ),
        "write-loop" => command_write_loop(
            Path::new(required(args, 2, "database path")?),
            required(args, 3, "prefix")?,
        ),
        "inspect" => command_inspect(
            Path::new(required(args, 2, "database path")?),
            required(args, 3, "token")?,
        ),
        "scan" => command_scan(
            Path::new(required(args, 2, "database path")?),
            required(args, 3, "prefix")?,
        ),
        "snapshot" => command_snapshot(Path::new(required(args, 2, "database path")?)),
        "hold" => command_hold(Path::new(required(args, 2, "database path")?)),
        "validate" => {
            let _db = OwnerDb::reopen(Path::new(required(args, 2, "database path")?))?;
            Ok(())
        }
        "alias-control" => command_alias_control(
            Path::new(required(args, 2, "database path")?),
            Path::new(required(args, 3, "alias path")?),
        ),
        "seed-schema" => command_seed_schema(
            Path::new(required(args, 2, "database path")?),
            required(args, 3, "layout")?,
        ),
        "compression" => command_compression(Path::new(required(args, 2, "database path")?)),
        "identity" => {
            command_identity();
            Ok(())
        }
        command => Err(format!("unknown command: {command}").into()),
    }
}

fn main() {
    if let Err(error) = execute(&std::env::args().collect::<Vec<_>>()) {
        eprintln!("ERROR: {error}");
        std::process::exit(2);
    }
}
