use duckdb::{Config, Connection};
use std::{fmt, path::Path};

/// Engine accounting is only one part of the enclosing process grant. DuckDB's
/// memory limit does not bound Arrow, extension allocations or total process RSS.
#[derive(Debug, Clone, Copy)]
pub struct EngineLimits {
    pub memory_bytes: u64,
    pub temp_bytes: u64,
    pub threads: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineError {
    InvalidLimits,
    InvalidTempRoot,
    Configuration,
    Startup,
    IncompatibleEngine,
}
impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Native diagnostics can contain credential-bearing paths or queries.
        write!(f, "archive engine failed: {self:?}")
    }
}
impl std::error::Error for EngineError {}

pub struct Engine {
    pub(crate) connection: Connection,
}
impl Engine {
    /// The launcher must own this empty temporary directory and retain the whole
    /// native grant until the worker has exited. Opening a connection grants no
    /// catalog, scientific publication, scope or durability authority.
    pub fn open(temp: &Path, limits: EngineLimits) -> Result<Self, EngineError> {
        if limits.memory_bytes == 0 || limits.temp_bytes == 0 || limits.threads == 0 {
            return Err(EngineError::InvalidLimits);
        }
        let text = temp
            .to_str()
            .filter(|s| !s.contains('\0'))
            .ok_or(EngineError::InvalidTempRoot)?;
        if !temp.is_absolute()
            || temp.canonicalize().ok().as_deref() != Some(temp)
            || std::fs::read_dir(temp)
                .map_err(|_| EngineError::InvalidTempRoot)?
                .next()
                .is_some()
        {
            return Err(EngineError::InvalidTempRoot);
        }
        let configure = || -> duckdb::Result<Config> {
            Config::default()
                .enable_autoload_extension(false)?
                .threads(i64::from(limits.threads))?
                .max_memory(&format!("{}B", limits.memory_bytes))?
                .with("home_directory", text)?
                .with("secret_directory", text)?
                .with("temp_directory", text)?
                .with("max_temp_directory_size", format!("{}B", limits.temp_bytes))?
                .with("allow_unsigned_extensions", "false")?
                .with("allow_community_extensions", "false")?
                .with("allow_persistent_secrets", "false")
        };
        let config = configure().map_err(|_| EngineError::Configuration)?;
        let connection =
            Connection::open_in_memory_with_flags(config).map_err(|_| EngineError::Startup)?;
        // This is a connection-local setting and is rejected by duckdb_set_config.
        connection
            .execute_batch("SET enable_progress_bar = false")
            .map_err(|_| EngineError::Configuration)?;
        let (version, source): (String, String) = connection
            .query_row(
                "SELECT library_version, source_id FROM pragma_version()",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| EngineError::IncompatibleEngine)?;
        let spec = crate::bundle::specification().map_err(|_| EngineError::IncompatibleEngine)?;
        if version != spec.engine_version || source != spec.engine_source_id {
            return Err(EngineError::IncompatibleEngine);
        }
        Ok(Self { connection })
    }

    /// Bounded inspection of this process, not a publication or release receipt.
    pub fn inspection(&self) -> Result<serde_json::Value, EngineError> {
        let spec = crate::bundle::specification().map_err(|_| EngineError::IncompatibleEngine)?;
        let mut settings = serde_json::Map::new();
        for name in [
            "autoinstall_known_extensions",
            "autoload_known_extensions",
            "allow_unsigned_extensions",
            "allow_community_extensions",
            "allow_persistent_secrets",
            "enable_progress_bar",
            "threads",
            "memory_limit",
            "max_temp_directory_size",
        ] {
            let value: String = self
                .connection
                .query_row(
                    "SELECT value FROM duckdb_settings() WHERE name = ?",
                    [name],
                    |row| row.get(0),
                )
                .map_err(|_| EngineError::Configuration)?;
            settings.insert(name.into(), value.into());
        }
        let mut extensions = Vec::with_capacity(spec.extensions.len());
        for asset in spec.extensions {
            let (loaded, version): (bool, String) = self.connection.query_row(
                "SELECT loaded, extension_version FROM duckdb_extensions() WHERE extension_name = ?",
                [&asset.name], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|_| EngineError::Configuration)?;
            extensions.push(serde_json::json!({"name": asset.name, "loaded": loaded,
                "version": version, "sha256": asset.sha256}));
        }
        Ok(
            serde_json::json!({"format": "bigbrotr-archive-engine-inspection/1",
            "target": spec.target, "version": spec.engine_version,
            "source_id": spec.engine_source_id, "library_sha256": spec.library.sha256,
            "settings": settings, "extensions": extensions}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_does_not_install_or_autoload_extensions() {
        let temp = tempfile::tempdir().unwrap();
        let temp_path = temp.path().canonicalize().unwrap();
        let engine = Engine::open(
            &temp_path,
            EngineLimits {
                memory_bytes: 128 * 1024 * 1024,
                temp_bytes: 64 * 1024 * 1024,
                threads: 1,
            },
        )
        .unwrap();
        let connection = &engine.connection;
        for name in [
            "autoinstall_known_extensions",
            "autoload_known_extensions",
            "allow_unsigned_extensions",
            "allow_community_extensions",
        ] {
            let value: bool = connection
                .query_row(
                    "SELECT value::BOOLEAN FROM duckdb_settings() WHERE name = ?",
                    [name],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(!value, "{name} must be disabled");
        }
    }

    #[test]
    fn zero_limits_and_invalid_temp_roots_refuse() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap();
        let limits = EngineLimits {
            memory_bytes: 128 * 1024 * 1024,
            temp_bytes: 64 * 1024 * 1024,
            threads: 1,
        };
        for value in [
            EngineLimits {
                memory_bytes: 0,
                ..limits
            },
            EngineLimits {
                temp_bytes: 0,
                ..limits
            },
            EngineLimits {
                threads: 0,
                ..limits
            },
        ] {
            assert!(matches!(
                Engine::open(&path, value),
                Err(EngineError::InvalidLimits)
            ));
        }
        for path in [Path::new("relative"), Path::new("/tmp/invalid\0path")] {
            assert!(matches!(
                Engine::open(path, limits),
                Err(EngineError::InvalidTempRoot)
            ));
        }
        std::fs::write(path.join("owned-marker"), b"preserve").unwrap();
        assert!(matches!(
            Engine::open(&path, limits),
            Err(EngineError::InvalidTempRoot)
        ));
        assert_eq!(
            std::fs::read(path.join("owned-marker")).unwrap(),
            b"preserve"
        );
    }

    #[test]
    fn actual_memory_threads_and_temp_settings_match_the_request() {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(
            &temp.path().canonicalize().unwrap(),
            EngineLimits {
                memory_bytes: 128 * 1024 * 1024,
                temp_bytes: 64 * 1024 * 1024,
                threads: 1,
            },
        )
        .unwrap();
        let report = engine.inspection().unwrap();
        assert_eq!(report["settings"]["memory_limit"], "128.0 MiB");
        assert_eq!(report["settings"]["max_temp_directory_size"], "64.0 MiB");
        assert_eq!(report["settings"]["threads"], "1");
        assert_eq!(report["settings"]["allow_persistent_secrets"], "false");
        assert_eq!(report["settings"]["enable_progress_bar"], "false");
        let actual: String = engine
            .connection
            .query_row("SELECT current_setting('temp_directory')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(Path::new(&actual), temp.path().canonicalize().unwrap());
        for setting in ["home_directory", "secret_directory"] {
            let actual: String = engine
                .connection
                .query_row(
                    "SELECT value FROM duckdb_settings() WHERE name = ?",
                    [setting],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(Path::new(&actual), temp.path().canonicalize().unwrap());
        }
    }
}
