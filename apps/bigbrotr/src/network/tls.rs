use super::TransportError;
use rustls::{ClientConfig, RootCertStore, client::Resumption, pki_types::CertificateDer};
use std::sync::Arc;

/// Verified server TLS only. Callers supply extra DER roots, never a verifier,
/// ambient client identity, global provider or verification-disable switch.
#[derive(Clone)]
pub struct TlsPolicy(pub(super) tokio_rustls::TlsConnector);
impl TlsPolicy {
    pub fn new(extra: &[CertificateDer<'_>]) -> Result<Self, TransportError> {
        if extra.len() > 128
            || extra
                .iter()
                .try_fold(0usize, |n, cert| n.checked_add(cert.len()))
                .is_none_or(|n| n > 1024 * 1024)
        {
            return Err(TransportError::TlsConfiguration);
        }
        let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        for certificate in extra {
            roots
                .add(certificate.clone())
                .map_err(|_| TransportError::TlsConfiguration)?;
        }
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
        Ok(Self(Arc::new(config).into()))
    }
}
