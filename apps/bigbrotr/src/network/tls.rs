use super::TransportError;
use crate::runtime::OwnedPermit;
use rustls::{ClientConfig, RootCertStore, client::Resumption, pki_types::CertificateDer};
use std::{path::Path, sync::Arc};

const MAX_ROOT_BYTES: usize = 1024 * 1024;

/// Verified server TLS only. Callers supply extra DER roots, never a verifier,
/// ambient client identity, global provider or verification-disable switch.
#[derive(Clone)]
pub struct TlsPolicy {
    pub(super) connector: tokio_rustls::TlsConnector,
    // Destroy all connector/config copies before returning their shared credit.
    _permit: Option<Arc<OwnedPermit>>,
}
impl TlsPolicy {
    /// One bounded scratch input, owned anchors and public-root/config overhead.
    /// This is a conservative logical reservation, not measured allocator/RSS usage.
    pub const FILE_ROOTS_BYTES: u64 = 4 * 1024 * 1024;

    /// Each operator-selected path contains exactly one DER certificate. Read
    /// sequentially and immediately extract anchors: retaining each reader's
    /// maximum-capacity Vec would multiply memory by the number of tiny files.
    /// No network I/O; callers run startup filesystem work under supervision.
    pub fn from_der_files(paths: &[&Path], permit: OwnedPermit) -> Result<Self, TransportError> {
        if permit.request().units().data_bytes() < Self::FILE_ROOTS_BYTES
            || permit.request().units().items() == 0
        {
            return Err(TransportError::Budget);
        }
        if paths.len() > 128 {
            return Err(TransportError::TlsConfiguration);
        }
        let mut roots = Self::public_roots();
        let mut remaining = MAX_ROOT_BYTES;
        for path in paths {
            let bytes = crate::local_file::read(path, remaining)
                .map_err(|_| TransportError::TlsConfiguration)?;
            remaining -= bytes.len();
            roots
                .add(CertificateDer::from(bytes))
                .map_err(|_| TransportError::TlsConfiguration)?;
        }
        Self::with_roots(roots, Some(Arc::new(permit)))
    }

    pub fn new(extra: &[CertificateDer<'_>]) -> Result<Self, TransportError> {
        if extra.len() > 128
            || extra
                .iter()
                .try_fold(0usize, |n, cert| n.checked_add(cert.len()))
                .is_none_or(|n| n > MAX_ROOT_BYTES)
        {
            return Err(TransportError::TlsConfiguration);
        }
        let mut roots = Self::public_roots();
        for certificate in extra {
            roots
                .add(certificate.clone())
                .map_err(|_| TransportError::TlsConfiguration)?;
        }
        Self::with_roots(roots, None)
    }

    fn public_roots() -> RootCertStore {
        RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
    }

    fn with_roots(
        roots: RootCertStore,
        permit: Option<Arc<OwnedPermit>>,
    ) -> Result<Self, TransportError> {
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|_| TransportError::TlsConfiguration)?
                .with_root_certificates(roots)
                .with_no_client_auth();
        // A finite connection has no shared ticket cache or replayable 0-RTT.
        config.resumption = Resumption::disabled();
        config.enable_early_data = false;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            connector: Arc::new(config).into(),
            _permit: permit,
        })
    }
}
