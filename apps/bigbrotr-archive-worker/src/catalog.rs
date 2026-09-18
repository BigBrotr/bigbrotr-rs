//! Private native catalog bootstrap. The parent selects a trusted infrastructure
//! route/TLS policy and supplies only the dedicated bootstrap login on stdin.
//! This finite operation must exit/reap before privileged guard installation.
use crate::engine::Engine;
use serde::Deserialize;
use std::{fmt, io::Read};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogError {
    InvalidInput,
    Input,
    Configuration,
    Bootstrap,
    ExistingCatalog,
    Initialize,
    InvalidDataRoot,
}
impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive catalog bootstrap failed: {self:?}")
    }
}
impl std::error::Error for CatalogError {}

// Deliberately no Debug or serialization: credentials cannot enter diagnostics.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapRequest {
    dsn: String,
    data_root: String,
}
impl BootstrapRequest {
    pub fn read(input: impl Read) -> Result<Self, CatalogError> {
        let mut bytes = Vec::new();
        input
            .take(32769)
            .read_to_end(&mut bytes)
            .map_err(|_| CatalogError::Input)?;
        if bytes.len() > 32768 {
            return Err(CatalogError::InvalidInput);
        }
        let request: Self =
            serde_json::from_slice(&bytes).map_err(|_| CatalogError::InvalidInput)?;
        request.validate()?;
        Ok(request)
    }
    pub(crate) fn validate(&self) -> Result<(), CatalogError> {
        if self.dsn.is_empty()
            || self.dsn.len() > 16384
            || self.dsn.contains(['\0', '\n', '\r'])
            || !(2..=4096).contains(&self.data_root.len())
            || !self.data_root.starts_with('/')
            || !self.data_root.ends_with('/')
            || self.data_root.contains(['\0', '\n', '\r'])
            // A singleton add_data_files argument still expands native globs.
            || self.data_root.contains(['*', '?', '['])
            || self.data_root.contains("//")
            || self.data_root.split('/').any(|p| matches!(p, "." | ".."))
        {
            return Err(CatalogError::InvalidInput);
        }
        Ok(())
    }
    pub fn data_root(&self) -> &str {
        &self.data_root
    }
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
impl Engine {
    pub(crate) fn attach_existing_catalog(
        &self,
        request: &BootstrapRequest,
    ) -> Result<(), CatalogError> {
        let root = std::path::Path::new(request.data_root());
        if root.canonicalize().ok().as_deref() != Some(root) || !root.is_dir() {
            return Err(CatalogError::InvalidDataRoot);
        }
        self.configure_catalog(request)?;
        self.connection.execute_batch(&format!("ATTACH 'ducklake:postgres:' AS archive (DATA_PATH {}, DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS false, META_SECRET_STORAGE_TABLE '', META_SECRET 'bigbrotr_bootstrap')",literal(request.data_root())))
            .map_err(|_| CatalogError::Bootstrap)
    }
    /// Native initialization only. Existing catalog identity/freshness and
    /// privileges are checked by the separate PG guard installer; this response
    /// alone cannot authorize publication or adoption of an existing catalog.
    /// Consuming the engine closes its connections before success is returned.
    pub fn bootstrap_catalog(self, request: BootstrapRequest) -> Result<(), CatalogError> {
        self.attach_catalog(&request)?;
        self.close_catalog()
    }

    pub(crate) fn configure_catalog(&self, request: &BootstrapRequest) -> Result<(), CatalogError> {
        self.connection.execute_batch("SET pg_pool_enable_reaper_thread=false; SET pg_use_ctid_scan=false; SET pg_pool_max_connections=8; SET pg_pool_acquire_mode='try'")
            .map_err(|_|CatalogError::Configuration)?;
        self.connection
            .execute_batch(&format!(
                "CREATE TEMPORARY SECRET bigbrotr_bootstrap (TYPE postgres, URI {})",
                literal(&request.dsn)
            ))
            .map_err(|_| CatalogError::Configuration)?;
        let memories:u64=self.connection.query_row("SELECT count(*)::UBIGINT FROM duckdb_secrets() WHERE name='bigbrotr_bootstrap' AND NOT persistent AND storage='memory'",[],|r|r.get(0)).map_err(|_|CatalogError::Configuration)?;
        if memories != 1 {
            return Err(CatalogError::Configuration);
        }
        Ok(())
    }

    fn attach_catalog(&self, request: &BootstrapRequest) -> Result<(), CatalogError> {
        self.configure_catalog(request)?;
        self.attach_configured_catalog(request)
    }

    fn attach_configured_catalog(&self, request: &BootstrapRequest) -> Result<(), CatalogError> {
        self.connection.execute_batch(&format!("ATTACH 'ducklake:postgres:' AS archive (DATA_PATH {}, DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS true, META_SECRET_STORAGE_TABLE '', META_SECRET 'bigbrotr_bootstrap')",literal(&request.data_root)))
            .map_err(|_|CatalogError::Bootstrap)
    }

    fn close_catalog(&self) -> Result<(), CatalogError> {
        self.connection
            .execute_batch("DETACH archive; DROP SECRET bigbrotr_bootstrap")
            .map_err(|_| CatalogError::Bootstrap)?;
        Ok(())
    }
}

impl Engine {
    /// Fresh-only explicit initialization. The operator must first reserve this
    /// dedicated empty PostgreSQL database and root and exclude other clients.
    /// A retry after any native creation is refused; a fresh PG inspection must
    /// resolve it. This response is not guard installation or publication.
    pub fn initialize_catalog(self, request: BootstrapRequest) -> Result<(), CatalogError> {
        let root = std::path::Path::new(&request.data_root);
        if root.canonicalize().ok().as_deref() != Some(root)
            || std::fs::read_dir(root)
                .map_err(|_| CatalogError::InvalidDataRoot)?
                .next()
                .is_some()
        {
            return Err(CatalogError::InvalidDataRoot);
        }
        self.configure_catalog(&request)?;
        self.connection.execute_batch("ATTACH '' AS bigbrotr_init (TYPE postgres, SECRET 'bigbrotr_bootstrap', SECRET_STORAGE_TABLE '', READ_ONLY)")
            .map_err(|_| CatalogError::Configuration)?;
        // Inspect before DuckLake ATTACH can initialize or migrate anything.
        // Extension-owned or unfamiliar objects also refuse. The catalog is an
        // exclusive operator boundary; this read is not a distributed lock.
        let preflight = r"SELECT
NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname NOT IN ('information_schema','public'))
AND NOT EXISTS (SELECT 1 FROM pg_depend d JOIN pg_namespace n ON d.refclassid='pg_namespace'::regclass AND d.refobjid=n.oid WHERE n.nspname='public')
AND NOT EXISTS (SELECT 1 FROM pg_largeobject_metadata)
AND NOT EXISTS (SELECT 1 FROM pg_event_trigger)
AND NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname<>'plpgsql')
AND NOT EXISTS (SELECT 1 FROM pg_foreign_data_wrapper)
AND NOT EXISTS (SELECT 1 FROM pg_foreign_server)
AND NOT EXISTS (SELECT 1 FROM pg_publication)
AND NOT EXISTS (SELECT 1 FROM pg_subscription)
AS empty";
        let empty: bool = self
            .connection
            .query_row(
                &format!(
                    "SELECT empty FROM postgres_query('bigbrotr_init', {})",
                    literal(preflight)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(|_| CatalogError::Configuration)?;
        if !empty {
            return Err(CatalogError::ExistingCatalog);
        }
        self.connection
            .execute_batch("DETACH bigbrotr_init")
            .map_err(|_| CatalogError::Configuration)?;
        self.attach_configured_catalog(&request)?;
        let pristine: bool = self.connection.query_row(
            "SELECT count(*)=1 AND count(*) FILTER (WHERE snapshot_id=0 AND schema_version=0)=1 FROM ducklake_snapshots('archive')",
            [], |row| row.get(0)).map_err(|_| CatalogError::Initialize)?;
        if !pristine {
            return Err(CatalogError::ExistingCatalog);
        }
        crate::schema::initialize(&self.connection).map_err(|_| CatalogError::Initialize)?;
        self.close_catalog()
    }
}
