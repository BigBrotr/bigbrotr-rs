//! Consuming fresh credential handoff to one supervised finite publisher.
use super::{Preparation, PreparationError, ProcessError};
use crate::{
    runtime::ResourceBudget,
    storage::{
        postgres::{
            catalog_guard::{CatalogIdentity, FreshWriter},
            connection_settings::PlaintextBoundary,
        },
        rocks::store::PendingBatch,
    },
};
use bigbrotr_model::{ArchiveBatch, Digest256, codec::FactDecodeLimits};
use bigbrotr_protocol::archive_batch::Limits;
use serde::Serialize;
use std::{
    io::{self, Write},
    net::IpAddr,
    path::Path,
};
use zeroize::Zeroizing;

/// Operator-validated and already resolved infrastructure address. Native libpq
/// receives hostaddr so it cannot repeat DNS after route policy validation.
pub struct CatalogRoute<'a> {
    pub address: IpAddr,
    pub port: u16,
    pub database: &'a str,
    pub tls: CatalogTls<'a>,
}
pub enum CatalogTls<'a> {
    LocalPlaintext(PlaintextBoundary),
    VerifyFull {
        server_name: &'a str,
        ca_file: &'a Path,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationError {
    InvalidInput,
    Identity,
    Preparation(PreparationError),
    /// A launched child may have committed even without a valid response.
    /// Positively exclude old work and inspect a fresh receipt before any retry.
    OutcomeUnknown,
    TerminationUnknown,
}
#[derive(Debug)]
pub struct PublicationResult {
    pub batch_digest: Digest256,
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}
impl CatalogRoute<'_> {
    pub(super) fn dsn(
        &self,
        login: &str,
        password: &str,
        application_name: &str,
    ) -> Result<Zeroizing<String>, PublicationError> {
        let valid = |s: &str| !s.is_empty() && s.len() <= 4096 && !s.chars().any(char::is_control);
        if self.port == 0 || !valid(self.database) {
            return Err(PublicationError::InvalidInput);
        }
        let address = self.address.to_canonical();
        let tls = match &self.tls {
            CatalogTls::LocalPlaintext(boundary) => {
                let allowed = match boundary {
                    PlaintextBoundary::Loopback => address.is_loopback(),
                    PlaintextBoundary::PrivateBridge => match address {
                        IpAddr::V4(ip) => ip.is_private(),
                        IpAddr::V6(ip) => ip.is_unique_local(),
                    },
                };
                if !allowed {
                    return Err(PublicationError::InvalidInput);
                }
                "sslmode=disable".to_owned()
            }
            CatalogTls::VerifyFull {
                server_name,
                ca_file,
            } => {
                let ca = ca_file
                    .to_str()
                    .filter(|s| valid(s))
                    .ok_or(PublicationError::InvalidInput)?;
                if !valid(server_name) || !ca_file.is_absolute() {
                    return Err(PublicationError::InvalidInput);
                }
                format!(
                    "sslmode=verify-full host={} sslrootcert={}",
                    literal(server_name),
                    literal(ca)
                )
            }
        };
        Ok(Zeroizing::new(format!(
            "hostaddr={} port={} dbname={} user={} password={} connect_timeout=5 application_name={} {tls}",
            address,
            self.port,
            literal(self.database),
            literal(login),
            literal(password),
            literal(application_name)
        )))
    }
}

#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    topology_generation: bigbrotr_model::TopologyGeneration,
    batch_digest: Digest256,
    attempt_id: &'a [u8; 16],
    publication_ordinal: u64,
}
struct Size(u32);
impl Write for Size {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(
                u32::try_from(bytes.len())
                    .map_err(|_| io::Error::other("publication input limit"))?,
            )
            .filter(|n| *n <= 32768)
            .ok_or_else(|| io::Error::other("publication input limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Preparation<'_> {
    /// Known native completion only, never protection or pending ACK. The caller
    /// must retain source/topology authority and the durable attempt across parent
    /// loss. This move consumes even a refused fresh credential; do not reissue it.
    pub fn publish_pending(
        self,
        pending: &PendingBatch,
        writer: FreshWriter,
        route: &CatalogRoute<'_>,
        budget: &ResourceBudget,
    ) -> Result<PublicationResult, PublicationError> {
        let request = writer.request().clone();
        let identity = writer.identity();
        let science = CatalogIdentity::local(request.dataset(), request.catalog(), identity.root())
            .map_err(|_| PublicationError::Identity)?;
        let archive = ArchiveBatch::new(
            1,
            request.topology_generation(),
            std::slice::from_ref(pending.batch()),
        )
        .map_err(|_| PublicationError::Identity)?;
        if identity != &science
            || archive.dataset_id() != request.dataset()
            || archive.digest() != request.batch_digest()
        {
            return Err(PublicationError::Identity);
        }
        let root = identity.root().to_owned();
        let transfer = Limits {
            max_records: self.limits.max_records,
            max_encoded_bytes: self.limits.max_encoded_bytes,
            fact: FactDecodeLimits::default(),
        };
        let floor = transfer
            .required_data_bytes()
            .map_err(|_| PublicationError::InvalidInput)?;
        if self.limits.native_reserve_bytes <= floor as u64 {
            return Err(PublicationError::InvalidInput);
        }
        writer.with_login(|login, password| {
            let dsn = route.dsn(login, password, "bigbrotr-archive-publisher")?;
            let input = Input {
                catalog: super::bootstrap::Input {
                    dsn: &dsn,
                    data_root: &root,
                },
                topology_generation: request.topology_generation(),
                batch_digest: request.batch_digest(),
                attempt_id: request.attempt_id(),
                publication_ordinal: request.ordinal(),
            };
            input
                .catalog
                .validate()
                .map_err(PublicationError::Preparation)?;
            let mut size = Size(0);
            serde_json::to_writer(&mut size, &input).map_err(|_| PublicationError::InvalidInput)?;
            let mut process = self
                .start(
                    "publish-batch",
                    &[
                        self.limits.max_records.to_string(),
                        self.limits.max_encoded_bytes.to_string(),
                    ],
                    budget,
                )
                .map_err(PublicationError::Preparation)?;
            let write = process.write_all(&size.0.to_be_bytes()).and_then(|()| {
                serde_json::to_writer(&mut process, &input).map_err(io::Error::other)
            });
            let transferred = write.is_ok()
                && pending
                    .write_archive_batch(&mut process, transfer, self.cancelled)
                    .is_ok();
            let output = process.finish();
            if matches!(output, Err(ProcessError::TerminationUnknown)) {
                return Err(PublicationError::TerminationUnknown);
            }
            if !transferred {
                return Err(PublicationError::OutcomeUnknown);
            }
            let value: serde_json::Value =
                serde_json::from_slice(output.map_err(|_| PublicationError::OutcomeUnknown)?)
                    .map_err(|_| PublicationError::OutcomeUnknown)?;
            if value.as_object().map(|v| v.len()) != Some(2)
                || value["format"] != "bigbrotr-native-publication/1"
                || value["batch_digest"]
                    != serde_json::to_value(request.batch_digest()).expect("fixed digest")
            {
                return Err(PublicationError::OutcomeUnknown);
            }
            Ok(PublicationResult {
                batch_digest: request.batch_digest(),
            })
        })
    }
}
