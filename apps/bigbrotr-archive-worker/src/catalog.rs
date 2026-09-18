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
        if request.dsn.is_empty()
            || request.dsn.len() > 16384
            || request.dsn.contains(['\0', '\n', '\r'])
            || !(2..=4096).contains(&request.data_root.len())
            || !request.data_root.starts_with('/')
            || !request.data_root.ends_with('/')
            || request.data_root.contains(['\0', '\n', '\r'])
            || request.data_root.contains("//")
            || request
                .data_root
                .split('/')
                .any(|p| matches!(p, "." | ".."))
        {
            return Err(CatalogError::InvalidInput);
        }
        Ok(request)
    }
    pub fn data_root(&self) -> &str {
        &self.data_root
    }
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
impl Engine {
    /// Native initialization only. Existing catalog identity/freshness and
    /// privileges are checked by the separate PG guard installer; this response
    /// alone cannot authorize publication or adoption of an existing catalog.
    /// Consuming the engine closes its connections before success is returned.
    pub fn bootstrap_catalog(self, request: BootstrapRequest) -> Result<(), CatalogError> {
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
        self.connection.execute_batch(&format!("ATTACH 'ducklake:postgres:' AS archive (DATA_PATH {}, DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS true, META_SECRET_STORAGE_TABLE '', META_SECRET 'bigbrotr_bootstrap')",literal(&request.data_root)))
            .map_err(|_|CatalogError::Bootstrap)?;
        self.connection
            .execute_batch("DETACH archive; DROP SECRET bigbrotr_bootstrap")
            .map_err(|_| CatalogError::Bootstrap)?;
        Ok(())
    }
}
