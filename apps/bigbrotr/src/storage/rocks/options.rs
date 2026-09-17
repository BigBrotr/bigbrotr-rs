//! Native option profile 1. These controls are not a process RSS or disk ceiling.
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Options,
    WriteBufferManager,
};
use std::fmt;

pub const NATIVE_OPTION_PROFILE_VERSION: u16 = 1;
pub(crate) const INGEST_COLUMN_FAMILIES: [&str; 3] = ["default", "facts", "receipts"];
const WRITE_BUFFERS_PER_CF: i32 = 2;
const BLOCK_BYTES: usize = 16 * 1024;
const MIN_WRITE_BUFFER_BYTES: u64 = 64 * 1024;
const MAX_WRITE_BUFFER_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Explicit targets selected within the owner's live native reservation.
/// Construction and parsing alone do not acquire that reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksNativeSettings {
    pub block_cache_bytes: u64,
    pub write_buffer_manager_bytes: u64,
    pub per_cf_write_buffer_bytes: u64,
    pub max_total_wal_bytes: u64,
    pub max_open_files: u32,
    pub max_background_jobs: u32,
    pub native_allowance_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeLimitError {
    Zero,
    NativeRange,
    CacheCoupling,
    WriteBufferCoupling,
    ArithmeticOverflow,
    AllowanceTooSmall,
}

impl fmt::Display for NativeLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Zero => "native resource targets and allowance must be nonzero",
            Self::NativeRange => "native resource target is outside the supported engine range",
            Self::CacheCoupling => {
                "block cache must exceed the write buffer manager by at least one block"
            }
            Self::WriteBufferCoupling => {
                "write buffer manager must cover one per-family write buffer target"
            }
            Self::ArithmeticOverflow => {
                "native resource accounting or engine arithmetic would overflow"
            }
            Self::AllowanceTooSmall => {
                "native allowance is smaller than the accounted control targets"
            }
        })
    }
}

impl std::error::Error for NativeLimitError {}

/// Validated C integer conversions and conservative control-target accounting.
/// Values stay immutable after validation; native handles are never exposed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksNativeLimits {
    block_cache_bytes: usize,
    write_buffer_manager_bytes: usize,
    per_cf_write_buffer_bytes: usize,
    max_total_wal_bytes: u64,
    max_open_files: i32,
    max_background_jobs: i32,
    native_allowance_bytes: u64,
    accounted_target_bytes: u64,
}

impl RocksNativeLimits {
    pub fn new(input: RocksNativeSettings) -> Result<Self, NativeLimitError> {
        if input.block_cache_bytes == 0
            || input.write_buffer_manager_bytes == 0
            || input.per_cf_write_buffer_bytes == 0
            || input.max_total_wal_bytes == 0
            || input.max_open_files == 0
            || input.max_background_jobs == 0
            || input.native_allowance_bytes == 0
        {
            return Err(NativeLimitError::Zero);
        }
        // RocksDB clamps smaller memtables to 64 KiB and larger ones to
        // min(size_t::MAX, 64 GiB). Refuse targets the engine would increase.
        if !(MIN_WRITE_BUFFER_BYTES..=MAX_WRITE_BUFFER_BYTES)
            .contains(&input.per_cf_write_buffer_bytes)
            || input.max_open_files < 20
            // The native scheduler always permits at least one flush and one
            // compaction even when max_background_jobs is supplied as one.
            || input.max_background_jobs < 2
        {
            return Err(NativeLimitError::NativeRange);
        }
        let block_cache_bytes =
            usize::try_from(input.block_cache_bytes).map_err(|_| NativeLimitError::NativeRange)?;
        let write_buffer_manager_bytes = usize::try_from(input.write_buffer_manager_bytes)
            .map_err(|_| NativeLimitError::NativeRange)?;
        let per_cf_write_buffer_bytes = usize::try_from(input.per_cf_write_buffer_bytes)
            .map_err(|_| NativeLimitError::NativeRange)?;
        let max_open_files =
            i32::try_from(input.max_open_files).map_err(|_| NativeLimitError::NativeRange)?;
        let max_background_jobs =
            i32::try_from(input.max_background_jobs).map_err(|_| NativeLimitError::NativeRange)?;
        usize::try_from(input.native_allowance_bytes).map_err(|_| NativeLimitError::NativeRange)?;

        // Native 11.8.1 computes its mutable threshold as size_t * 7 / 8.
        // Checked Rust conversions alone would not prevent that multiplication
        // from wrapping inside the C++ constructor.
        write_buffer_manager_bytes
            .checked_mul(7)
            .ok_or(NativeLimitError::ArithmeticOverflow)?;
        let cache_minimum = write_buffer_manager_bytes
            .checked_add(BLOCK_BYTES)
            .ok_or(NativeLimitError::ArithmeticOverflow)?;
        if block_cache_bytes < cache_minimum {
            return Err(NativeLimitError::CacheCoupling);
        }
        if write_buffer_manager_bytes < per_cf_write_buffer_bytes {
            return Err(NativeLimitError::WriteBufferCoupling);
        }
        // Deliberately count the cache, WBM and six CF-buffer targets separately,
        // despite WBM charging memtables to that cache. This is a reservation
        // floor, not an upper bound on native allocations or process RSS.
        let cf_buffers = input
            .per_cf_write_buffer_bytes
            .checked_mul(6)
            .ok_or(NativeLimitError::ArithmeticOverflow)?;
        let accounted_target_bytes = input
            .block_cache_bytes
            .checked_add(input.write_buffer_manager_bytes)
            .and_then(|sum| sum.checked_add(cf_buffers))
            .ok_or(NativeLimitError::ArithmeticOverflow)?;
        if accounted_target_bytes > input.native_allowance_bytes {
            return Err(NativeLimitError::AllowanceTooSmall);
        }
        Ok(Self {
            block_cache_bytes,
            write_buffer_manager_bytes,
            per_cf_write_buffer_bytes,
            max_total_wal_bytes: input.max_total_wal_bytes,
            max_open_files,
            max_background_jobs,
            native_allowance_bytes: input.native_allowance_bytes,
            accounted_target_bytes,
        })
    }

    pub const fn native_allowance_bytes(self) -> u64 {
        self.native_allowance_bytes
    }

    pub const fn accounted_target_bytes(self) -> u64 {
        self.accounted_target_bytes
    }
}

/// Snapshots of independently sampled native counters. They overlap: WBM
/// charges memtables to the cache, and pinned cache bytes are part of cache bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksNativeUsage {
    pub cache_bytes: usize,
    pub pinned_cache_bytes: usize,
    pub memtable_bytes: usize,
}

/// Retain beside the DB and the owner's native reservation; no mutating handle
/// escapes. RocksDB also retains shared references through its opened Options.
pub(crate) struct RocksNativeResources {
    cache: Cache,
    write_buffers: WriteBufferManager,
    limits: RocksNativeLimits,
}

impl RocksNativeResources {
    pub(crate) fn usage(&self) -> RocksNativeUsage {
        RocksNativeUsage {
            cache_bytes: self.cache.get_usage(),
            pinned_cache_bytes: self.cache.get_pinned_usage(),
            memtable_bytes: self.write_buffers.get_usage(),
        }
    }

    pub(crate) const fn limits(&self) -> RocksNativeLimits {
        self.limits
    }
}

/// Open inputs only. The store owner alone authorizes creation/reopen, checks
/// the manifest/CF set and imposes WAL-enabled synchronized write options.
pub(crate) struct RocksNativeProfile {
    pub(crate) db_options: Options,
    pub(crate) column_families: [ColumnFamilyDescriptor; 3],
    pub(crate) resources: RocksNativeResources,
}

impl RocksNativeProfile {
    pub(crate) fn build(limits: RocksNativeLimits) -> Self {
        let cache = Cache::new_lru_cache(limits.block_cache_bytes);
        let write_buffers = WriteBufferManager::new_write_buffer_manager_with_cache(
            limits.write_buffer_manager_bytes,
            true,
            cache.clone(),
        );
        let column_families = INGEST_COLUMN_FAMILIES.map(|name| {
            let mut table = BlockBasedOptions::default();
            table.set_block_cache(&cache);
            table.set_block_size(BLOCK_BYTES);
            table.set_cache_index_and_filter_blocks(true);
            let mut options = Options::default();
            options.set_block_based_table_factory(&table);
            options.set_write_buffer_size(limits.per_cf_write_buffer_bytes);
            options.set_max_write_buffer_number(WRITE_BUFFERS_PER_CF);
            options.set_max_write_buffer_size_to_maintain(0);
            options.set_compression_type(DBCompressionType::Zstd);
            options.set_bottommost_compression_type(DBCompressionType::Zstd);
            ColumnFamilyDescriptor::new(name, options)
        });
        let mut db_options = Options::default();
        db_options.create_if_missing(false);
        db_options.create_missing_column_families(false);
        db_options.set_error_if_exists(false);
        // This belongs to DBOptions. The native CF descriptor conversion drops
        // a manager assigned only to per-family Options, silently leaving it idle.
        db_options.set_write_buffer_manager(&write_buffers);
        db_options.set_use_fsync(true);
        db_options.set_atomic_flush(true);
        db_options.set_paranoid_checks(true);
        db_options.set_manual_wal_flush(false);
        db_options.set_max_total_wal_size(limits.max_total_wal_bytes);
        db_options.set_max_open_files(limits.max_open_files);
        db_options.set_max_background_jobs(limits.max_background_jobs);
        db_options.set_max_subcompactions(1);
        db_options.set_max_file_opening_threads(1);
        db_options.set_allow_mmap_reads(false);
        db_options.set_allow_mmap_writes(false);
        db_options.set_max_log_file_size(1024 * 1024);
        db_options.set_keep_log_file_num(4);
        Self {
            db_options,
            column_families,
            resources: RocksNativeResources {
                cache,
                write_buffers,
                limits,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super as options;

    use options::{RocksNativeLimits, RocksNativeProfile, RocksNativeSettings};
    use rocksdb::{
        BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Options, WriteBufferManager,
        WriteOptions,
    };

    const MIB: u64 = 1024 * 1024;

    fn settings() -> RocksNativeSettings {
        RocksNativeSettings {
            block_cache_bytes: 32 * MIB,
            write_buffer_manager_bytes: 16 * MIB,
            per_cf_write_buffer_bytes: 2 * MIB,
            max_total_wal_bytes: 16 * MIB,
            max_open_files: 64,
            max_background_jobs: 2,
            native_allowance_bytes: 128 * MIB,
        }
    }

    #[test]
    fn checked_profile_accounts_targets_separately_from_native_allowance() {
        assert_eq!(options::NATIVE_OPTION_PROFILE_VERSION, 1);
        let limits = RocksNativeLimits::new(settings()).unwrap();
        assert_eq!(limits.accounted_target_bytes(), 60 * MIB);
        assert_eq!(limits.native_allowance_bytes(), 128 * MIB);
        assert!(
            RocksNativeLimits::new(RocksNativeSettings {
                native_allowance_bytes: 60 * MIB - 1,
                ..settings()
            })
            .is_err()
        );
        assert!(
            RocksNativeLimits::new(RocksNativeSettings {
                native_allowance_bytes: 60 * MIB,
                ..settings()
            })
            .is_ok()
        );
    }

    #[test]
    fn zero_unlimited_narrowed_or_native_clamped_limits_are_refused() {
        let invalid = [
            RocksNativeSettings {
                block_cache_bytes: 0,
                ..settings()
            },
            RocksNativeSettings {
                write_buffer_manager_bytes: 0,
                ..settings()
            },
            RocksNativeSettings {
                per_cf_write_buffer_bytes: 0,
                ..settings()
            },
            RocksNativeSettings {
                max_total_wal_bytes: 0,
                ..settings()
            },
            RocksNativeSettings {
                max_open_files: 0,
                ..settings()
            },
            RocksNativeSettings {
                max_background_jobs: 0,
                ..settings()
            },
            RocksNativeSettings {
                native_allowance_bytes: 0,
                ..settings()
            },
            RocksNativeSettings {
                max_open_files: 19,
                ..settings()
            },
            RocksNativeSettings {
                max_open_files: u32::MAX,
                ..settings()
            },
            RocksNativeSettings {
                max_background_jobs: 1,
                ..settings()
            },
            RocksNativeSettings {
                max_background_jobs: u32::MAX,
                ..settings()
            },
            RocksNativeSettings {
                per_cf_write_buffer_bytes: 65535,
                ..settings()
            },
            RocksNativeSettings {
                per_cf_write_buffer_bytes: 64 * 1024 * MIB + 1,
                ..settings()
            },
            RocksNativeSettings {
                block_cache_bytes: 16 * MIB,
                ..settings()
            },
            RocksNativeSettings {
                write_buffer_manager_bytes: MIB,
                ..settings()
            },
            RocksNativeSettings {
                block_cache_bytes: u64::MAX,
                write_buffer_manager_bytes: u64::MAX / 7 + 1,
                native_allowance_bytes: u64::MAX,
                ..settings()
            },
            RocksNativeSettings {
                block_cache_bytes: u64::MAX,
                native_allowance_bytes: u64::MAX,
                ..settings()
            },
        ];
        for input in invalid {
            assert!(RocksNativeLimits::new(input).is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn profile_does_not_authorize_implicit_creation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("not-initialized");
        let profile = RocksNativeProfile::build(RocksNativeLimits::new(settings()).unwrap());
        let names: Vec<_> = profile.column_families.iter().map(|cf| cf.name()).collect();
        assert_eq!(names, ["default", "facts", "receipts"]);
        assert!(
            DB::open_cf_descriptors(&profile.db_options, &path, profile.column_families).is_err()
        );
        assert!(!path.join("CURRENT").exists());
    }

    fn synced() -> WriteOptions {
        let mut write = WriteOptions::default();
        write.set_sync(true);
        write.disable_wal(false);
        write
    }

    #[test]
    fn actual_database_options_bind_shared_wbm_and_cache_to_every_family() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("positive");
        let mut profile = RocksNativeProfile::build(RocksNativeLimits::new(settings()).unwrap());
        profile.db_options.create_if_missing(true);
        profile.db_options.create_missing_column_families(true);
        profile.db_options.set_error_if_exists(true);
        let db =
            DB::open_cf_descriptors(&profile.db_options, &path, profile.column_families).unwrap();
        drop(profile.db_options);
        let value = vec![0x39; 512 * 1024];
        for name in ["default", "facts", "receipts"] {
            let before = profile.resources.usage();
            let cf = db.cf_handle(name).unwrap();
            db.put_cf_opt(cf, b"independent-key", &value, &synced())
                .unwrap();
            assert_eq!(
                db.get_cf(cf, b"independent-key").unwrap().as_deref(),
                Some(value.as_slice())
            );
            let after = profile.resources.usage();
            println!("native usage {name}: before={before:?}, after={after:?}");
            assert!(
                after.memtable_bytes > before.memtable_bytes,
                "{name}: WBM was not charged"
            );
            assert!(
                after.cache_bytes > before.cache_bytes,
                "{name}: cache was not charged"
            );
            assert!(
                after.pinned_cache_bytes > before.pinned_cache_bytes,
                "{name}: WBM cache reservation missing"
            );
            assert!(
                after.memtable_bytes < 16 * MIB as usize,
                "test unexpectedly reached WBM flush threshold"
            );
        }
        assert_eq!(
            profile.resources.limits().native_allowance_bytes(),
            128 * MIB
        );
        // Native persisted options provide an independent observation of the actual
        // DB/CF configuration, rather than a mirror of the builder's Rust fields.
        let latest = std::fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("OPTIONS-")
            })
            .max()
            .unwrap();
        let text = std::fs::read_to_string(latest).unwrap();
        let lines: Vec<_> = text.lines().map(str::trim).collect();
        for (line, count) in [
            ("rocksdb_version=11.8.1", 1),
            ("max_open_files=64", 1),
            ("max_background_jobs=2", 1),
            ("max_total_wal_size=16777216", 1),
            ("use_fsync=true", 1),
            ("atomic_flush=true", 1),
            ("write_buffer_size=2097152", 3),
            ("max_write_buffer_number=2", 3),
            ("block_size=16384", 3),
            ("compression=kZSTD", 3),
            ("bottommost_compression=kZSTD", 3),
            ("cache_index_and_filter_blocks=true", 3),
        ] {
            assert_eq!(
                lines.iter().filter(|value| **value == line).count(),
                count,
                "native option {line}"
            );
        }
        drop(db);
    }

    #[test]
    fn negative_control_wbm_attached_only_to_cf_options_remains_unused() {
        let root = tempfile::tempdir().unwrap();
        let cache = Cache::new_lru_cache((32 * MIB) as usize);
        let manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            (16 * MIB) as usize,
            true,
            cache.clone(),
        );
        let descriptors = ["default", "facts", "receipts"].map(|name| {
            let mut table = BlockBasedOptions::default();
            table.set_block_cache(&cache);
            let mut cf = Options::default();
            cf.set_block_based_table_factory(&table);
            cf.set_write_buffer_size((2 * MIB) as usize);
            cf.set_max_write_buffer_number(2);
            // Intentional old-experiment mistake: this is a DBOptions member and
            // is discarded by the native ColumnFamilyOptions conversion at open.
            cf.set_write_buffer_manager(&manager);
            ColumnFamilyDescriptor::new(name, cf)
        });
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.set_max_open_files(64);
        db_options.set_max_background_jobs(2);
        db_options.set_max_total_wal_size(16 * MIB);
        let db = DB::open_cf_descriptors(&db_options, root.path().join("negative"), descriptors)
            .unwrap();
        let before = manager.get_usage();
        assert_eq!(before, 0);
        for name in ["default", "facts", "receipts"] {
            let cf = db.cf_handle(name).unwrap();
            db.put_cf_opt(cf, b"independent-key", vec![0x39; 512 * 1024], &synced())
                .unwrap();
            assert!(
                db.property_int_value_cf(cf, "rocksdb.cur-size-all-mem-tables")
                    .unwrap()
                    .unwrap()
                    >= 512 * 1024
            );
        }
        println!(
            "CF-only WBM usage: before={before}, after={}",
            manager.get_usage()
        );
        assert_eq!(
            manager.get_usage(),
            before,
            "negative control unexpectedly attached its WBM"
        );
    }
}
