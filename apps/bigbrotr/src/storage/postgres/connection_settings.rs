//! Preparation of bounded PostgreSQL credentials and explicit transport policy.
//!
//! This module performs no network I/O and owns no connection task. The caller
//! keeps exclusive ownership of the returned configuration/connector and must
//! retain resource charges until every driver copy and operation has terminated.

use crate::secrets::SecretFile;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};
use std::{fmt, net::IpAddr, path::Path, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{Config, config::SslMode, tls::MakeTlsConnect};
use tokio_postgres_rustls::MakeRustlsConnect;
use url::Url;

pub const PASSWORD_MAX_BYTES: usize = 64 * 1024;
pub const CA_MAX_BYTES: usize = 1024 * 1024;
pub const CLIENT_CERTIFICATE_MAX_BYTES: usize = 1024 * 1024;
pub const CLIENT_PRIVATE_KEY_MAX_BYTES: usize = 256 * 1024;
pub const CONNECT_TIMEOUT_MAX: Duration = Duration::from_secs(60);

const NAME_MAX_BYTES: usize = 1024;
const ENDPOINT_MAX_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionError {
    Endpoint,
    Database,
    User,
    Password,
    ConnectTimeout,
    PlaintextBoundary,
    ServerName,
    CaOpen,
    CaNotRegular,
    CaTooLarge,
    CaRead,
    CaCertificate,
    ClientIdentityPair,
    ClientCertificate,
    ClientPrivateKey,
    ClientIdentity,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Endpoint => "invalid bounded PostgreSQL endpoint",
            Self::Database => "invalid bounded PostgreSQL database name",
            Self::User => "invalid bounded PostgreSQL user name",
            Self::Password => "invalid protected PostgreSQL password input",
            Self::ConnectTimeout => "invalid bounded PostgreSQL connect timeout",
            Self::PlaintextBoundary => {
                "PostgreSQL plaintext route is outside its declared local boundary"
            }
            Self::ServerName => "invalid explicit PostgreSQL TLS server name",
            Self::CaOpen => "cannot open PostgreSQL CA descriptor",
            Self::CaNotRegular => "PostgreSQL CA descriptor must be a regular file",
            Self::CaTooLarge => "PostgreSQL CA input exceeds its byte limit",
            Self::CaRead => "cannot read bounded PostgreSQL CA input",
            Self::CaCertificate => "invalid PostgreSQL CA certificate input",
            Self::ClientIdentityPair => "PostgreSQL client certificate and key must be paired",
            Self::ClientCertificate => "invalid protected PostgreSQL client certificate input",
            Self::ClientPrivateKey => "invalid protected PostgreSQL client private key input",
            Self::ClientIdentity => {
                "PostgreSQL client certificate and private key do not form a usable identity"
            }
        })
    }
}

impl std::error::Error for ConnectionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaintextBoundary {
    Loopback,
    PrivateBridge,
}

pub enum TlsSettings<'a> {
    VerifyFull {
        ca_file: &'a Path,
        server_name: &'a str,
        client_certificate: Option<SecretFile<'a>>,
        client_private_key: Option<SecretFile<'a>>,
    },
    LocalPlaintext {
        boundary: PlaintextBoundary,
    },
}

/// Borrowed, already selected connection settings. This type does not expose a
/// `Debug` implementation because it contains protected references.
pub struct ConnectionSettings<'a> {
    pub endpoint: &'a str,
    pub database: &'a str,
    pub user: &'a str,
    pub password: SecretFile<'a>,
    pub connect_timeout: Duration,
    pub tls: TlsSettings<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionSecurity {
    VerifyFull { client_auth: bool },
    LocalPlaintext,
}

pub struct VerifyFullConnector {
    connector: MakeRustlsConnect,
    expected_server_name: String,
}

impl fmt::Debug for VerifyFullConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifyFullConnector([redacted])")
    }
}

impl<S> MakeTlsConnect<S> for VerifyFullConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = <MakeRustlsConnect as MakeTlsConnect<S>>::Stream;
    type TlsConnect = <MakeRustlsConnect as MakeTlsConnect<S>>::TlsConnect;
    type Error = <MakeRustlsConnect as MakeTlsConnect<S>>::Error;

    fn make_tls_connect(&mut self, _hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        <MakeRustlsConnect as MakeTlsConnect<S>>::make_tls_connect(
            &mut self.connector,
            &self.expected_server_name,
        )
    }
}

pub enum PreparedTransport {
    /// The connected owner must resolve, vet and pin an address inside this
    /// boundary before connecting. Lexical preparation alone cannot prove the
    /// route selected by DNS.
    LocalPlaintext(PlaintextBoundary),
    VerifyFull(VerifyFullConnector),
}

impl fmt::Debug for PreparedTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalPlaintext(boundary) => {
                f.debug_tuple("LocalPlaintext").field(boundary).finish()
            }
            Self::VerifyFull(_) => f.write_str("VerifyFull([redacted])"),
        }
    }
}

pub struct PreparedConnection {
    config: Config,
    transport: PreparedTransport,
    security: ConnectionSecurity,
    preparation_input_bytes: usize,
}

impl fmt::Debug for PreparedConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedConnection")
            .field("security", &self.security)
            .field("preparation_input_bytes", &self.preparation_input_bytes)
            .finish_non_exhaustive()
    }
}

impl PreparedConnection {
    pub fn security(&self) -> ConnectionSecurity {
        self.security
    }

    /// Sum of source bytes read while preparing this value. It is useful for
    /// diagnostics, but is not an allocation measurement and excludes driver,
    /// rustls, vector-capacity and allocator overhead copies.
    pub fn preparation_input_bytes(&self) -> usize {
        self.preparation_input_bytes
    }

    pub fn into_parts(self) -> (Config, PreparedTransport) {
        (self.config, self.transport)
    }
}

impl ConnectionSettings<'_> {
    /// Upper bound for the logical `limit + 1` source buffers used by `prepare`.
    /// Consumers reserve this before preparation and separately allow for
    /// `Config`, rustls and allocator copies retained after this call.
    pub fn preparation_input_reservation_bytes(&self) -> usize {
        let mut bytes = PASSWORD_MAX_BYTES + 1;
        if let TlsSettings::VerifyFull {
            client_certificate,
            client_private_key,
            ..
        } = &self.tls
        {
            bytes += CA_MAX_BYTES + 1;
            if client_certificate.is_some() || client_private_key.is_some() {
                bytes += CLIENT_CERTIFICATE_MAX_BYTES + 1;
                bytes += CLIENT_PRIVATE_KEY_MAX_BYTES + 1;
            }
        }
        bytes
    }

    pub fn prepare(self) -> Result<PreparedConnection, ConnectionError> {
        let endpoint = parse_endpoint(self.endpoint)?;
        if !valid_name(self.database) {
            return Err(ConnectionError::Database);
        }
        if !valid_name(self.user) {
            return Err(ConnectionError::User);
        }
        if self.connect_timeout.is_zero() || self.connect_timeout > CONNECT_TIMEOUT_MAX {
            return Err(ConnectionError::ConnectTimeout);
        }
        if let TlsSettings::VerifyFull {
            client_certificate,
            client_private_key,
            ..
        } = &self.tls
            && client_certificate.is_some() != client_private_key.is_some()
        {
            return Err(ConnectionError::ClientIdentityPair);
        }

        let password = self
            .password
            .read(PASSWORD_MAX_BYTES)
            .map_err(|_| ConnectionError::Password)?;
        if password.as_bytes().is_empty() {
            return Err(ConnectionError::Password);
        }
        let mut preparation_input_bytes = password.as_bytes().len();
        let mut config = Config::new();
        config
            .host(&endpoint.host)
            .port(endpoint.port)
            .dbname(self.database)
            .user(self.user)
            .password(password.as_bytes())
            .connect_timeout(self.connect_timeout);

        let (transport, security) = match self.tls {
            TlsSettings::LocalPlaintext { boundary } => {
                validate_plaintext_boundary(&endpoint.host, boundary)?;
                config.ssl_mode(SslMode::Disable);
                (
                    PreparedTransport::LocalPlaintext(boundary),
                    ConnectionSecurity::LocalPlaintext,
                )
            }
            TlsSettings::VerifyFull {
                ca_file,
                server_name,
                client_certificate,
                client_private_key,
            } => {
                ServerName::try_from(server_name.to_owned())
                    .map_err(|_| ConnectionError::ServerName)?;
                let ca_bytes = read_public_file(ca_file, CA_MAX_BYTES)?;
                preparation_input_bytes = preparation_input_bytes
                    .checked_add(ca_bytes.len())
                    .ok_or(ConnectionError::CaTooLarge)?;
                let roots = root_store(&ca_bytes)?;
                let provider = rustls::crypto::ring::default_provider();
                let builder = ClientConfig::builder_with_provider(provider.into())
                    .with_safe_default_protocol_versions()
                    .map_err(|_| ConnectionError::CaCertificate)?
                    .with_root_certificates(roots);

                let (client_config, client_auth) = match (client_certificate, client_private_key) {
                    (None, None) => (builder.with_no_client_auth(), false),
                    (Some(certificate), Some(private_key)) => {
                        let certificate = certificate
                            .read(CLIENT_CERTIFICATE_MAX_BYTES)
                            .map_err(|_| ConnectionError::ClientCertificate)?;
                        let private_key = private_key
                            .read(CLIENT_PRIVATE_KEY_MAX_BYTES)
                            .map_err(|_| ConnectionError::ClientPrivateKey)?;
                        preparation_input_bytes = preparation_input_bytes
                            .checked_add(certificate.as_bytes().len())
                            .and_then(|bytes| bytes.checked_add(private_key.as_bytes().len()))
                            .ok_or(ConnectionError::ClientIdentity)?;
                        let certificates = certificate_chain(certificate.as_bytes())?;
                        let private_key = one_private_key(private_key.as_bytes())?;
                        (
                            builder
                                .with_client_auth_cert(certificates, private_key)
                                .map_err(|_| ConnectionError::ClientIdentity)?,
                            true,
                        )
                    }
                    _ => return Err(ConnectionError::ClientIdentityPair),
                };
                config.ssl_mode(SslMode::Require);
                (
                    PreparedTransport::VerifyFull(VerifyFullConnector {
                        connector: MakeRustlsConnect::new(client_config),
                        expected_server_name: server_name.to_owned(),
                    }),
                    ConnectionSecurity::VerifyFull { client_auth },
                )
            }
        };

        Ok(PreparedConnection {
            config,
            transport,
            security,
            preparation_input_bytes,
        })
    }
}

struct Endpoint {
    host: String,
    port: u16,
}

fn parse_endpoint(raw: &str) -> Result<Endpoint, ConnectionError> {
    if raw.is_empty()
        || raw.len() > ENDPOINT_MAX_BYTES
        || raw
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || raw.contains(['/', '\\', '@', '?', '#'])
    {
        return Err(ConnectionError::Endpoint);
    }
    let parsed = Url::parse(&format!("https://{raw}")).map_err(|_| ConnectionError::Endpoint)?;
    let host = parsed.host_str().ok_or(ConnectionError::Endpoint)?;
    // HTTPS supplies authority parsing only. Its default-port normalization must
    // neither discard PostgreSQL's explicit 443 nor invent a missing port.
    let port = raw
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .ok_or(ConnectionError::Endpoint)?;
    if parsed.path() != "/" || port == 0 {
        return Err(ConnectionError::Endpoint);
    }
    Ok(Endpoint {
        // URL syntax brackets IPv6; the PostgreSQL driver and IpAddr expect
        // the address itself, including when TLS uses a separate server name.
        host: host.trim_matches(['[', ']']).to_owned(),
        port,
    })
}

fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= NAME_MAX_BYTES && !value.chars().any(char::is_control)
}

fn validate_plaintext_boundary(
    host: &str,
    boundary: PlaintextBoundary,
) -> Result<(), ConnectionError> {
    let valid = match boundary {
        PlaintextBoundary::Loopback => {
            host == "localhost"
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        }
        PlaintextBoundary::PrivateBridge => !host.contains(['.', ':']),
    };
    valid
        .then_some(())
        .ok_or(ConnectionError::PlaintextBoundary)
}

fn read_public_file(path: &Path, limit: usize) -> Result<Vec<u8>, ConnectionError> {
    crate::local_file::read(path, limit).map_err(|error| match error {
        crate::local_file::FileError::Open => ConnectionError::CaOpen,
        crate::local_file::FileError::NotRegular => ConnectionError::CaNotRegular,
        crate::local_file::FileError::TooLarge => ConnectionError::CaTooLarge,
        crate::local_file::FileError::Read => ConnectionError::CaRead,
    })
}

fn root_store(bytes: &[u8]) -> Result<RootCertStore, ConnectionError> {
    validate_pem_blocks(bytes, &["CERTIFICATE"]).map_err(|_| ConnectionError::CaCertificate)?;
    let certificates = CertificateDer::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ConnectionError::CaCertificate)?;
    if certificates.is_empty() {
        return Err(ConnectionError::CaCertificate);
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|_| ConnectionError::CaCertificate)?;
    }
    Ok(roots)
}

pub(crate) fn certificate_chain(
    bytes: &[u8],
) -> Result<Vec<CertificateDer<'static>>, ConnectionError> {
    validate_pem_blocks(bytes, &["CERTIFICATE"]).map_err(|_| ConnectionError::ClientCertificate)?;
    let certificates = CertificateDer::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ConnectionError::ClientCertificate)?;
    if certificates.is_empty() {
        return Err(ConnectionError::ClientCertificate);
    }
    Ok(certificates)
}

pub(crate) fn one_private_key(bytes: &[u8]) -> Result<PrivateKeyDer<'static>, ConnectionError> {
    validate_pem_blocks(bytes, &["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"])
        .map_err(|_| ConnectionError::ClientPrivateKey)?;
    let mut keys = PrivateKeyDer::pem_slice_iter(bytes);
    let key = keys
        .next()
        .ok_or(ConnectionError::ClientPrivateKey)?
        .map_err(|_| ConnectionError::ClientPrivateKey)?;
    if keys.next().is_some() {
        return Err(ConnectionError::ClientPrivateKey);
    }
    Ok(key)
}

fn validate_pem_blocks(bytes: &[u8], labels: &[&str]) -> Result<(), ()> {
    let text = std::str::from_utf8(bytes).map_err(|_| ())?;
    let mut open_label = None;
    let mut blocks = 0_usize;
    for raw_line in text.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if let Some(label) = line
            .strip_prefix("-----BEGIN ")
            .and_then(|line| line.strip_suffix("-----"))
        {
            if open_label.is_some() || !labels.contains(&label) {
                return Err(());
            }
            open_label = Some(label);
        } else if let Some(label) = line
            .strip_prefix("-----END ")
            .and_then(|line| line.strip_suffix("-----"))
        {
            if open_label != Some(label) {
                return Err(());
            }
            open_label = None;
            blocks = blocks.checked_add(1).ok_or(())?;
        } else if open_label.is_none() && !line.trim().is_empty() {
            return Err(());
        }
    }
    if open_label.is_some() || blocks == 0 {
        return Err(());
    }
    Ok(())
}
