//! Bounded explicit client identity for the native libpq process, never a path
//! that libpq can reopen after operator rotation. No named secret copies.
use crate::{
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    secrets::SecretFile,
    storage::postgres::connection_settings::{
        CLIENT_CERTIFICATE_MAX_BYTES, CLIENT_PRIVATE_KEY_MAX_BYTES, certificate_chain,
        one_private_key,
    },
};
use command_fds::CommandFdExt;
use std::{
    fmt,
    fs::File,
    os::fd::{AsRawFd, OwnedFd},
    process::Command,
    sync::Arc,
};

/// Conservative preparation/source/memfd allowance; native TLS allocations are
/// also inside the worker's existing total grant. This is not an RSS guarantee.
const RESERVATION: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIdentityError {
    ResourceLimit,
    Certificate,
    PrivateKey,
    Identity,
    AnonymousFile,
    UnsupportedPlatform,
}

pub struct NativeClientIdentity(Arc<Material>);
struct Material {
    certificate: File,
    private_key: File,
    _grant: OwnedPermit,
}
impl fmt::Debug for NativeClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeClientIdentity([redacted])")
    }
}
impl NativeClientIdentity {
    /// Load once using protected descriptors, validate PEM and the matching key,
    /// and freeze exact bytes before any permanent bootstrap reservation.
    pub fn load(
        certificate: SecretFile<'_>,
        private_key: SecretFile<'_>,
        budget: &ResourceBudget,
    ) -> Result<Self, NativeIdentityError> {
        let grant = budget
            .try_acquire(ResourceRequest::data(RESERVATION, 1, 0).unwrap())
            .map_err(|_| NativeIdentityError::ResourceLimit)?;
        let certificate = certificate
            .read(CLIENT_CERTIFICATE_MAX_BYTES)
            .map_err(|_| NativeIdentityError::Certificate)?;
        let private_key = private_key
            .read(CLIENT_PRIVATE_KEY_MAX_BYTES)
            .map_err(|_| NativeIdentityError::PrivateKey)?;
        let chain = certificate_chain(certificate.as_bytes())
            .map_err(|_| NativeIdentityError::Certificate)?;
        let key =
            one_private_key(private_key.as_bytes()).map_err(|_| NativeIdentityError::PrivateKey)?;
        let checked = rustls::sign::CertifiedKey::from_der(
            chain,
            key,
            &rustls::crypto::ring::default_provider(),
        )
        .map_err(|_| NativeIdentityError::Identity)?;
        checked
            .keys_match()
            .map_err(|_| NativeIdentityError::Identity)?;
        drop(checked);
        Ok(Self(Arc::new(Material {
            certificate: sealed(certificate.as_bytes())?,
            private_key: sealed(private_key.as_bytes())?,
            _grant: grant,
        })))
    }

    pub(super) fn handoff(&self) -> Result<NativeTlsHandoff, NativeIdentityError> {
        let duplicate = |file: &File| {
            rustix::io::fcntl_dupfd_cloexec(file, 3).map_err(|_| NativeIdentityError::AnonymousFile)
        };
        Ok(NativeTlsHandoff {
            certificate: duplicate(&self.0.certificate)?,
            private_key: duplicate(&self.0.private_key)?,
            retention: NativeTlsRetention {
                _material: Arc::clone(&self.0),
            },
        })
    }
}

#[cfg(target_os = "linux")]
fn sealed(bytes: &[u8]) -> Result<File, NativeIdentityError> {
    use rustix::fs::{MemfdFlags, Mode, SealFlags, fchmod, fcntl_add_seals, memfd_create};
    use std::io::Write;
    let create = || -> std::io::Result<File> {
        let mut file = File::from(memfd_create(
            "bigbrotr-catalog-identity",
            MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING,
        )?);
        fchmod(&file, Mode::RUSR)?;
        file.write_all(bytes)?;
        fcntl_add_seals(
            &file,
            SealFlags::WRITE | SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL,
        )?;
        Ok(file)
    };
    create().map_err(|_| NativeIdentityError::AnonymousFile)
}
#[cfg(not(target_os = "linux"))]
fn sealed(_: &[u8]) -> Result<File, NativeIdentityError> {
    Err(NativeIdentityError::UnsupportedPlatform)
}

pub(super) struct NativeTlsHandoff {
    certificate: OwnedFd,
    private_key: OwnedFd,
    retention: NativeTlsRetention,
}
/// Retain the source allowance through actual child reaping, including a
/// permanently charged unknown-termination outcome in Process::drop.
pub(super) struct NativeTlsRetention {
    _material: Arc<Material>,
}
impl NativeTlsHandoff {
    /// Later handoffs can allocate different descriptor numbers. Two positive
    /// signed 32-bit descriptors need at most ten decimal digits each.
    pub(super) fn maximum_parameter_growth(&self) -> usize {
        "sslcertmode=require sslcert='/proc/self/fd/2147483647' sslkey='/proc/self/fd/2147483647'"
            .len()
            - self.parameters().len()
    }
    pub(super) fn parameters(&self) -> String {
        format!(
            "sslcertmode=require sslcert='/proc/self/fd/{}' sslkey='/proc/self/fd/{}'",
            self.certificate.as_raw_fd(),
            self.private_key.as_raw_fd()
        )
    }
    pub(super) fn inherit(self, command: &mut Command) -> NativeTlsRetention {
        // No fixed-FD remapping: these live descriptors cannot collide with the
        // separately preserved custody FD or Command's stdin/exec-error pipes.
        command.preserved_fds(vec![self.certificate, self.private_key]);
        self.retention
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits};
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
    };

    fn budget(bytes: u64) -> ResourceBudget {
        ResourceBudget::new(
            ProcessEnvelope::new(
                bytes,
                0,
                ResourceLimits::new(bytes, 1, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        )
    }
    fn generate(root: &Path, name: &str) -> (PathBuf, PathBuf) {
        let cert = root.join(format!("{name}.crt"));
        let key = root.join(format!("{name}.key"));
        let output = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=BigBrotr test client",
                "-keyout",
            ])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .output()
            .expect("openssl fixture dependency");
        assert!(
            output.status.success(),
            "ephemeral identity generation failed"
        );
        for path in [&cert, &key] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        (cert, key)
    }
    fn load(
        cert: &Path,
        key: &Path,
        budget: &ResourceBudget,
    ) -> Result<NativeClientIdentity, NativeIdentityError> {
        let c = format!("file:{}", cert.display());
        let k = format!("file:{}", key.display());
        NativeClientIdentity::load(
            SecretFile::parse(&c).unwrap(),
            SecretFile::parse(&k).unwrap(),
            budget,
        )
    }

    #[test]
    fn refused_identity_releases_capacity_and_checks_limits_before_files() {
        assert!(matches!(
            load(
                Path::new("/absent/cert"),
                Path::new("/absent/key"),
                &budget(RESERVATION - 1)
            ),
            Err(NativeIdentityError::ResourceLimit)
        ));
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let (cert, key) = generate(&root, "first");
        let (_, other_key) = generate(&root, "other");
        let budget = budget(RESERVATION);
        assert!(matches!(
            load(&cert, &other_key, &budget),
            Err(NativeIdentityError::Identity)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
        fs::write(&cert, vec![b'x'; CLIENT_CERTIFICATE_MAX_BYTES + 1]).unwrap();
        assert!(matches!(
            load(&cert, &key, &budget),
            Err(NativeIdentityError::Certificate)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
        fs::write(&cert, "not a certificate").unwrap();
        assert!(matches!(
            load(&cert, &key, &budget),
            Err(NativeIdentityError::Certificate)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sealed_identity_survives_rotation_and_inherits_only_explicit_descriptors() {
        use rustix::fs::{SealFlags, fcntl_get_seals};
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let (cert, key) = generate(&root, "first");
        let expected = fs::read(&cert).unwrap();
        let budget = budget(RESERVATION);
        let identity = load(&cert, &key, &budget).unwrap();
        assert_eq!(
            identity
                .0
                .private_key
                .metadata()
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
        assert_eq!(
            fcntl_get_seals(&identity.0.private_key).unwrap(),
            SealFlags::WRITE | SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL
        );
        assert!((&identity.0.private_key).write_all(b"changed").is_err());
        assert!(identity.0.private_key.set_len(0).is_err());
        fs::write(&cert, "rotated").unwrap();
        fs::remove_file(&key).unwrap();
        let handoff = identity.handoff().unwrap();
        let descriptor = handoff.certificate.as_raw_fd();
        let mut ambient = Command::new("cat");
        ambient.arg(format!("/proc/self/fd/{descriptor}"));
        assert!(!ambient.output().unwrap().status.success());
        let mut child = Command::new("cat");
        child.arg(format!("/proc/self/fd/{descriptor}"));
        let retention = handoff.inherit(&mut child);
        drop(identity);
        assert_eq!(budget.snapshot().active_permits(), 1);
        let result = child.output().unwrap();
        assert!(result.status.success());
        assert_eq!(result.stdout, expected);
        drop(child);
        assert_eq!(budget.snapshot().active_permits(), 1);
        drop(retention);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mutual_tls_route_keeps_scram_and_uses_only_the_handed_off_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let (cert, key) = generate(&root, "client");
        let budget = budget(RESERVATION);
        let identity = load(&cert, &key, &budget).unwrap();
        let route = super::super::CatalogRoute {
            address: "127.0.0.1".parse().unwrap(),
            port: 5432,
            database: "catalog",
            tls: super::super::CatalogTls::VerifyFullWithClient {
                server_name: "catalog.example",
                ca_file: Path::new("/ca.pem"),
                identity: &identity,
            },
        };
        let (dsn, handoff) = route
            .connection("worker", "password", "native-test")
            .unwrap();
        let handoff = handoff.unwrap();
        assert!(dsn.contains("require_auth=scram-sha-256 sslcertmode=require"));
        assert!(dsn.contains("sslmode=verify-full host='catalog.example' sslrootcert='/ca.pem'"));
        assert!(dsn.contains(&handoff.parameters()));
        assert!(!dsn.contains(cert.to_str().unwrap()) && !dsn.contains(key.to_str().unwrap()));
    }
}
