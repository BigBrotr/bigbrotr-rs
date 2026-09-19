use super::{Result, required};
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    secrets::SecretFile,
    storage::{
        archive::{
            CatalogRoute, CatalogTls, NativeClientIdentity, Preparation, PreparationLimits,
            WorkerInstallation,
        },
        postgres::{
            catalog_guard::CatalogIdentity, connection_settings::PlaintextBoundary,
            transport::PinnedRoute,
        },
    },
};
use std::{
    fs,
    net::IpAddr,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

pub(super) fn budget(config: &crate::config::Resolved) -> Result<ResourceBudget> {
    let envelope = |process: &str| {
        let n = |field: &str| config.number(&format!("runtime.processes.{process}.{field}"));
        ProcessEnvelope::new(
            n("memory_max_bytes"),
            n("baseline_bytes"),
            ResourceLimits::new(
                n("payload_bytes"),
                2,
                n("native_bytes"),
                n("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .map_err(|_| (3, "invalid archive command process envelope".into()))
    };
    let app = envelope("application")?;
    let native = envelope("native")?;
    let a = app.resources();
    let n = native.resources();
    let sum = |values: &[u64]| {
        values
            .iter()
            .try_fold(0u64, |total, v| total.checked_add(*v))
            .ok_or_else(|| (3, "archive command envelope overflow".into()))
    };
    // Two validated process envelopes, fixed disjoint lanes. Unused allowances
    // remain reserved baseline; aggregate headroom cannot move between processes.
    let combined = ProcessEnvelope::new(
        sum(&[app.memory_max_bytes(), native.memory_max_bytes()])?,
        sum(&[
            app.baseline_bytes(),
            native.baseline_bytes(),
            a.native_bytes(),
            n.data_bytes(),
            n.control_bytes(),
        ])?,
        ResourceLimits::new(a.data_bytes(), 2, n.native_bytes(), a.control_bytes()),
        WaiterLimits::new(0, 0),
    )
    .map_err(|_| (3, "invalid combined archive command envelope".into()))?;
    Ok(ResourceBudget::new(combined))
}

pub(super) fn native_tls(
    config: &crate::config::Resolved,
    budget: &ResourceBudget,
) -> Result<Option<NativeClientIdentity>> {
    let certificate = config.text("postgres.catalog.tls.client_cert_ref");
    let key = config.text("postgres.catalog.tls.client_key_ref");
    let refused = || {
        (
            3,
            "native catalog client identity refused; no reservation was created".into(),
        )
    };
    match (certificate, key) {
        (None, None) => Ok(None),
        (Some(certificate), Some(key))
            if config.text("postgres.catalog.tls.mode") == Some("verify-full") =>
        {
            NativeClientIdentity::load(
                SecretFile::parse(certificate).map_err(|_| refused())?,
                SecretFile::parse(key).map_err(|_| refused())?,
                budget,
            )
            .map(Some)
            .map_err(|_| refused())
        }
        _ => Err(refused()),
    }
}

pub(super) fn limits(config: &crate::config::Resolved) -> Result<PreparationLimits> {
    let total = config.number("archive.worker_memory_bytes");
    if total < 512 * 1024 * 1024 {
        return Err((
            3,
            "native initialization requires a worker allowance of at least 512MiB".into(),
        ));
    }
    // The qualified initial empty-schema profile reserves three quarters for
    // startup/native overhead. It never adds overhead outside the worker total.
    Ok(PreparationLimits {
        memory_bytes: total / 4,
        native_reserve_bytes: total - total / 4,
        temp_bytes: 64 * 1024 * 1024,
        threads: 1,
        max_records: 0,
        max_encoded_bytes: 0,
    })
}

pub(super) fn worker(
    config: &crate::config::Resolved,
    identity: &CatalogIdentity,
    args: &clap::ArgMatches,
) -> Result<(WorkerInstallation, PathBuf, [u8; 16])> {
    let root = args
        .get_one::<PathBuf>("worker-root")
        .cloned()
        .or_else(|| {
            std::env::current_exe()
                .ok()?
                .parent()
                .map(|p| p.join("archive-worker"))
        })
        .ok_or_else(|| (3, "cannot locate installed worker bundle".into()))?;
    let installation = WorkerInstallation::packaged(root)
        .map_err(|_| (3, "main executable has no embedded worker identity".into()))?;
    let root = Path::new(required(config, "storage.scratch_root")?);
    let data = Path::new(identity.root());
    let metadata =
        fs::symlink_metadata(root).map_err(|_| (3, "cannot inspect scratch root".into()))?;
    if !root.is_absolute()
        || root.canonicalize().ok().as_deref() != Some(root)
        || !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
        || data.canonicalize().ok().as_deref() != Some(data)
        || overlaps(root, data)
        || overlaps(root, &installation.root)
    {
        return Err((
            3,
            "scratch must be canonical, private, owned and disjoint from archive data and bundle"
                .into(),
        ));
    }
    let cancelled = AtomicBool::new(false);
    let deadline = Instant::now() + Duration::from_secs(30);
    installation
        .verify(deadline, &cancelled)
        .map_err(|_| (3, "installed archive worker verification refused".into()))?;
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|_| (1, "initialization entropy unavailable".into()))?;
    if nonce == [0; 16] {
        return Err((1, "invalid initialization entropy".into()));
    }
    let name = nonce.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let scratch = root.join(format!("initialize-{name}"));
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&scratch)
        .map_err(|_| (1, "cannot exclusively create initialization scratch".into()))?;
    Preparation {
        installation: &installation,
        temp_root: &scratch,
        limits: limits(config)?,
        deadline,
        cancelled: &cancelled,
    }
    .validate_temporary_root()
    .map_err(|_| (3, "invalid initialization scratch".into()))?;
    // Retain this owned directory on every outcome. Cleanup must follow positive
    // process exclusion and an explicit lifecycle decision, never TempDir drop.
    Ok((installation, scratch, nonce))
}

fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

pub(super) struct NativeRoute {
    address: IpAddr,
    port: u16,
    database: String,
    tls: NativeTls,
}
enum NativeTls {
    Plain(PlaintextBoundary),
    Full {
        server: String,
        ca: PathBuf,
        identity: Option<NativeClientIdentity>,
    },
}
impl NativeRoute {
    pub(super) fn new(
        config: &crate::config::Resolved,
        pinned: &PinnedRoute,
        identity: Option<NativeClientIdentity>,
    ) -> Result<Self> {
        let tls = match required(config, "postgres.catalog.tls.mode")? {
            "verify-full" => NativeTls::Full {
                server: required(config, "postgres.catalog.tls.server_name")?.into(),
                ca: required(config, "postgres.catalog.tls.ca_file")?.into(),
                identity,
            },
            "local-plaintext" => NativeTls::Plain(
                match required(config, "postgres.catalog.tls.local_boundary")? {
                    "loopback" => PlaintextBoundary::Loopback,
                    "private-bridge" => PlaintextBoundary::PrivateBridge,
                    _ => return Err((3, "invalid catalog plaintext boundary".into())),
                },
            ),
            _ => return Err((3, "invalid catalog TLS mode".into())),
        };
        let value = Self {
            address: pinned.address(),
            port: pinned.port(),
            database: required(config, "postgres.catalog.database")?.into(),
            tls,
        };
        value.borrowed().validate().map_err(|_| {
            (
                3,
                "native catalog route exceeds its supported bounds".into(),
            )
        })?;
        Ok(value)
    }
    pub(super) fn borrowed(&self) -> CatalogRoute<'_> {
        CatalogRoute {
            address: self.address,
            port: self.port,
            database: &self.database,
            tls: match &self.tls {
                NativeTls::Plain(boundary) => CatalogTls::LocalPlaintext(*boundary),
                NativeTls::Full {
                    server,
                    ca,
                    identity: None,
                } => CatalogTls::VerifyFull {
                    server_name: server,
                    ca_file: ca,
                },
                NativeTls::Full {
                    server,
                    ca,
                    identity: Some(identity),
                } => CatalogTls::VerifyFullWithClient {
                    server_name: server,
                    ca_file: ca,
                    identity,
                },
            },
        }
    }
}

pub(super) fn admit_native(
    config: &crate::config::Resolved,
    budget: &ResourceBudget,
) -> Result<()> {
    let _native = budget
        .try_acquire(
            ResourceRequest::data(0, 1, config.number("archive.worker_memory_bytes")).unwrap(),
        )
        .map_err(|_| {
            (
                3,
                "native initialization does not fit its declared grant".into(),
            )
        })?;
    let _control = budget
        .try_acquire(ResourceRequest::control(128 * 1024).unwrap())
        .map_err(|_| {
            (
                3,
                "native supervision does not fit its declared control grant".into(),
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_roots_are_refused_in_both_directions() {
        for (a, b) in [
            ("/data", "/data/scratch"),
            ("/data/scratch", "/data"),
            ("/data", "/data/"),
        ] {
            assert!(overlaps(Path::new(a), Path::new(b)));
        }
        assert!(!overlaps(Path::new("/data"), Path::new("/database")));
    }
    #[test]
    fn aggregate_budget_keeps_process_lanes_separate() {
        let config = crate::config::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/recipes/archive-only.toml"
        )))
        .unwrap();
        let budget = budget(&config).unwrap();
        let heap = config.number("runtime.processes.application.payload_bytes");
        let native = config.number("runtime.processes.native.native_bytes");
        assert!(
            budget
                .try_acquire(ResourceRequest::data(heap + 1, 1, 0).unwrap())
                .is_err()
        );
        assert!(
            budget
                .try_acquire(ResourceRequest::data(0, 1, native + 1).unwrap())
                .is_err()
        );
        let _all = budget
            .try_acquire(ResourceRequest::data(heap, 1, native).unwrap())
            .unwrap();
        let _control = budget
            .try_acquire(ResourceRequest::control(128 * 1024).unwrap())
            .unwrap();
    }
}
