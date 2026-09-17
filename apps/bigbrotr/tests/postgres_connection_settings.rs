mod secrets {
    pub use bigbrotr::secrets::*;
}

#[path = "../src/storage/postgres/connection_settings.rs"]
mod connection_settings;

use connection_settings::{
    ConnectionError, ConnectionSecurity, ConnectionSettings, PlaintextBoundary, PreparedTransport,
    TlsSettings,
};
use secrets::SecretFile;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

struct PkiFixture {
    _directory: tempfile::TempDir,
    ca: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
    other_key: PathBuf,
}

impl PkiFixture {
    fn generate() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let ca_key = root.join("ca.key");
        let ca = root.join("ca.crt");
        generate_self_signed(&ca_key, &ca, "BigBrotr test CA", true);
        let client_key = root.join("client.key");
        let client_cert = root.join("client.crt");
        generate_self_signed(&client_key, &client_cert, "BigBrotr test client", false);
        let other_key = root.join("other.key");
        let other_cert = root.join("other.crt");
        generate_self_signed(&other_key, &other_cert, "BigBrotr other client", false);
        for path in [&client_cert, &client_key, &other_key] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        fs::set_permissions(&ca, fs::Permissions::from_mode(0o644)).unwrap();
        Self {
            _directory: directory,
            ca,
            client_cert,
            client_key,
            other_key,
        }
    }
}

fn generate_self_signed(key: &Path, certificate: &Path, common_name: &str, ca: bool) {
    let config = certificate.with_extension("cnf");
    let constraints = if ca {
        "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign"
    } else {
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth"
    };
    fs::write(
        &config,
        format!(
            "[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN={common_name}\n[ext]\n{constraints}\nsubjectKeyIdentifier=hash\n"
        ),
    )
    .unwrap();
    let status = Command::new("openssl")
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config",
        ])
        .arg(&config)
        .arg("-extensions")
        .arg("ext")
        .arg("-keyout")
        .arg(key)
        .arg("-out")
        .arg(certificate)
        .output()
        .expect("openssl is required to generate ephemeral test credentials");
    assert!(
        status.status.success(),
        "openssl failed to generate an ephemeral test identity"
    );
}

fn secret_reference(path: &Path) -> String {
    format!("file:{}", path.display())
}

fn password_file(directory: &Path, bytes: &[u8]) -> PathBuf {
    let path = directory.canonicalize().unwrap().join("password");
    fs::write(&path, bytes).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

fn verify_full<'a>(
    password: &'a str,
    ca: &'a Path,
    certificate: Option<&'a str>,
    private_key: Option<&'a str>,
) -> ConnectionSettings<'a> {
    ConnectionSettings {
        endpoint: "postgres.internal:5432",
        database: "bigbrotr",
        user: "migration_owner",
        password: SecretFile::parse(password).unwrap(),
        connect_timeout: Duration::from_secs(7),
        tls: TlsSettings::VerifyFull {
            ca_file: ca,
            server_name: "postgres.service.example",
            client_certificate: certificate.map(|value| SecretFile::parse(value).unwrap()),
            client_private_key: private_key.map(|value| SecretFile::parse(value).unwrap()),
        },
    }
}

#[test]
fn prepares_explicit_ca_name_and_matching_client_identity_without_network_io() {
    let fixture = PkiFixture::generate();
    let password_path = password_file(fixture._directory.path(), b"exact\0password\n");
    let password = secret_reference(&password_path);
    let certificate = secret_reference(&fixture.client_cert);
    let private_key = secret_reference(&fixture.client_key);
    let settings = verify_full(
        &password,
        &fixture.ca,
        Some(&certificate),
        Some(&private_key),
    );
    assert_eq!(
        settings.preparation_input_reservation_bytes(),
        connection_settings::PASSWORD_MAX_BYTES
            + connection_settings::CA_MAX_BYTES
            + connection_settings::CLIENT_CERTIFICATE_MAX_BYTES
            + connection_settings::CLIENT_PRIVATE_KEY_MAX_BYTES
            + 4
    );
    let prepared = settings.prepare().unwrap();

    assert_eq!(
        prepared.security(),
        ConnectionSecurity::VerifyFull { client_auth: true }
    );
    assert!(prepared.preparation_input_bytes() > b"exact\0password\n".len());
    assert!(!format!("{prepared:?}").contains("migration_owner"));
    assert!(!format!("{prepared:?}").contains(fixture.ca.to_str().unwrap()));
    let (config, transport) = prepared.into_parts();
    assert_eq!(config.get_hosts().len(), 1);
    assert_eq!(config.get_ports(), &[5432]);
    assert_eq!(config.get_dbname(), Some("bigbrotr"));
    assert_eq!(config.get_user(), Some("migration_owner"));
    assert_eq!(config.get_password(), Some(b"exact\0password\n".as_slice()));
    assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(7)));
    assert_eq!(
        config.get_ssl_mode(),
        tokio_postgres::config::SslMode::Require
    );
    let PreparedTransport::VerifyFull(connector) = transport else {
        panic!("verify-full settings produced plaintext transport")
    };
    assert_eq!(format!("{connector:?}"), "VerifyFullConnector([redacted])");
}

#[test]
fn verify_full_refuses_unpaired_malformed_or_mismatched_client_material() {
    let fixture = PkiFixture::generate();
    let password_path = password_file(fixture._directory.path(), b"secret");
    let password = secret_reference(&password_path);
    let certificate = secret_reference(&fixture.client_cert);
    let private_key = secret_reference(&fixture.client_key);
    let other_key = secret_reference(&fixture.other_key);
    let missing_password = secret_reference(&fixture._directory.path().join("missing-password"));

    assert_eq!(
        verify_full(&missing_password, &fixture.ca, Some(&certificate), None)
            .prepare()
            .unwrap_err(),
        ConnectionError::ClientIdentityPair
    );
    assert_eq!(
        verify_full(&password, &fixture.ca, Some(&certificate), Some(&other_key))
            .prepare()
            .unwrap_err(),
        ConnectionError::ClientIdentity
    );

    fs::write(&fixture.client_cert, b"not a certificate").unwrap();
    assert_eq!(
        verify_full(
            &password,
            &fixture.ca,
            Some(&certificate),
            Some(&private_key)
        )
        .prepare()
        .unwrap_err(),
        ConnectionError::ClientCertificate
    );
}

#[test]
fn verify_full_requires_one_valid_key_and_nonempty_valid_ca() {
    let fixture = PkiFixture::generate();
    let password_path = password_file(fixture._directory.path(), b"secret");
    let password = secret_reference(&password_path);
    let certificate = secret_reference(&fixture.client_cert);
    let private_key = secret_reference(&fixture.client_key);

    let original_key = fs::read(&fixture.client_key).unwrap();
    let mut two_keys = original_key.clone();
    two_keys.extend_from_slice(&original_key);
    fs::write(&fixture.client_key, two_keys).unwrap();
    assert_eq!(
        verify_full(
            &password,
            &fixture.ca,
            Some(&certificate),
            Some(&private_key)
        )
        .prepare()
        .unwrap_err(),
        ConnectionError::ClientPrivateKey
    );

    fs::write(&fixture.client_key, original_key).unwrap();
    fs::write(&fixture.ca, b"").unwrap();
    assert_eq!(
        verify_full(&password, &fixture.ca, None, None)
            .prepare()
            .unwrap_err(),
        ConnectionError::CaCertificate
    );
    fs::write(&fixture.ca, b"-----BEGIN CERTIFICATE-----\ninvalid\n").unwrap();
    assert_eq!(
        verify_full(&password, &fixture.ca, None, None)
            .prepare()
            .unwrap_err(),
        ConnectionError::CaCertificate
    );
}

#[test]
fn public_ca_is_bounded_regular_and_never_follows_a_named_alias() {
    let fixture = PkiFixture::generate();
    let password_path = password_file(fixture._directory.path(), b"secret");
    let password = secret_reference(&password_path);
    let alias = fixture._directory.path().join("ca-alias.crt");
    symlink(&fixture.ca, &alias).unwrap();
    assert_eq!(
        verify_full(&password, &alias, None, None)
            .prepare()
            .unwrap_err(),
        ConnectionError::CaOpen
    );

    let real_parent = fixture
        ._directory
        .path()
        .canonicalize()
        .unwrap()
        .join("real-ca");
    fs::create_dir(&real_parent).unwrap();
    fs::copy(&fixture.ca, real_parent.join("ca.crt")).unwrap();
    let parent_alias = fixture
        ._directory
        .path()
        .canonicalize()
        .unwrap()
        .join("ca-directory-alias");
    symlink(&real_parent, &parent_alias).unwrap();
    assert_eq!(
        verify_full(&password, &parent_alias.join("ca.crt"), None, None)
            .prepare()
            .unwrap_err(),
        ConnectionError::CaOpen
    );

    fs::write(
        &fixture.ca,
        vec![b'x'; connection_settings::CA_MAX_BYTES + 1],
    )
    .unwrap();
    assert_eq!(
        verify_full(&password, &fixture.ca, None, None)
            .prepare()
            .unwrap_err(),
        ConnectionError::CaTooLarge
    );
}

#[test]
fn plaintext_is_only_prepared_for_the_declared_local_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let password_path = password_file(directory.path(), b"secret");
    let password = secret_reference(&password_path);
    let settings = |endpoint, boundary| ConnectionSettings {
        endpoint,
        database: "bigbrotr",
        user: "application_user",
        password: SecretFile::parse(&password).unwrap(),
        connect_timeout: Duration::from_secs(3),
        tls: TlsSettings::LocalPlaintext { boundary },
    };

    let prepared = settings("127.0.0.1:5432", PlaintextBoundary::Loopback)
        .prepare()
        .unwrap();
    assert_eq!(prepared.security(), ConnectionSecurity::LocalPlaintext);
    let (config, transport) = prepared.into_parts();
    assert_eq!(
        config.get_ssl_mode(),
        tokio_postgres::config::SslMode::Disable
    );
    assert!(matches!(
        transport,
        PreparedTransport::LocalPlaintext(PlaintextBoundary::Loopback)
    ));

    assert_eq!(
        settings("10.0.0.5:5432", PlaintextBoundary::Loopback)
            .prepare()
            .unwrap_err(),
        ConnectionError::PlaintextBoundary
    );
    assert_eq!(
        settings("10.0.0.5:5432", PlaintextBoundary::PrivateBridge)
            .prepare()
            .unwrap_err(),
        ConnectionError::PlaintextBoundary
    );
    settings("postgres:5432", PlaintextBoundary::PrivateBridge)
        .prepare()
        .unwrap();
}

#[test]
fn empty_password_zero_timeout_and_redacted_errors_fail_before_connection() {
    let directory = tempfile::tempdir().unwrap();
    let password_path = password_file(directory.path(), b"");
    let canary_path = secret_reference(&password_path);
    let settings = ConnectionSettings {
        endpoint: "localhost:5432",
        database: "bigbrotr",
        user: "application_user",
        password: SecretFile::parse(&canary_path).unwrap(),
        connect_timeout: Duration::from_secs(2),
        tls: TlsSettings::LocalPlaintext {
            boundary: PlaintextBoundary::Loopback,
        },
    };
    let error = settings.prepare().unwrap_err();
    assert_eq!(error, ConnectionError::Password);
    assert!(!format!("{error:?} {error}").contains(password_path.to_str().unwrap()));

    fs::write(&password_path, b"secret").unwrap();
    let settings = ConnectionSettings {
        endpoint: "localhost:5432",
        database: "bigbrotr",
        user: "application_user",
        password: SecretFile::parse(&canary_path).unwrap(),
        connect_timeout: Duration::ZERO,
        tls: TlsSettings::LocalPlaintext {
            boundary: PlaintextBoundary::Loopback,
        },
    };
    assert_eq!(
        settings.prepare().unwrap_err(),
        ConnectionError::ConnectTimeout
    );
}
