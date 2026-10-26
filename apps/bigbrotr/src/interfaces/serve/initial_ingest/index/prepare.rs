//! Deployment input only, on the parent's supervised, joined blocking preparer.
//! No client, journal, database or authority is created here. Accepted policy
//! supplies provenance later, immediately before actual custody/installation.
use crate::config::Resolved;
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    secrets::SecretFile,
    storage::{
        clickhouse::adapter::IndexConnection,
        indexer::state::IndexState,
        postgres::{connection_settings::PlaintextBoundary, transport::select_plaintext_address},
    },
};
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::Arc,
};

type Result<T> = std::result::Result<T, (u8, String)>;

/// Conservative initial preparation/retained-input allowance, not measured RSS
/// or a bound on libc resolver allocations. No new thread/runtime starts here.
/// The existing parent supervisor must retain and join this preparer on timeout.
pub(super) const PREPARATION_BYTES: u64 = 16 * 1024 * 1024;
pub(super) const ITEMS: u64 = 1;
const PASSWORD_BYTES: usize = 8192;

/// Keep this value until every adapter/operation using its connections has
/// joined and dropped. Callers may clone the connection Arcs for adapters, but
/// must retain this owner through those users. There is no into_parts or raw
/// credential API; the service owns the final lifetime and drop ordering.
pub(super) struct PreparedIndex {
    writer: Arc<IndexConnection>,
    initializer: Option<Arc<IndexConnection>>,
    database: String,
    state_root: PathBuf,
    engine: ResourceBudget,
    // Last field: credential/configuration allocations drop before their grant.
    _permit: OwnedPermit,
}
impl PreparedIndex {
    pub(super) fn writer(&self) -> &Arc<IndexConnection> {
        &self.writer
    }
    pub(super) fn initializer(&self) -> Option<&Arc<IndexConnection>> {
        self.initializer.as_ref()
    }
    pub(super) fn database(&self) -> &str {
        &self.database
    }
    pub(super) fn state_root(&self) -> &Path {
        &self.state_root
    }
    pub(super) fn engine(&self) -> &ResourceBudget {
        &self.engine
    }
}

fn refused(reason: &'static str) -> (u8, String) {
    (3, format!("initial Index preparation refused: {reason}"))
}
fn required<'a>(root: &'a Resolved, key: &str) -> Result<&'a str> {
    root.text(key)
        .ok_or_else(|| refused("deployment input missing"))
}

pub(super) fn prepare(
    root: &Resolved,
    application: &ResourceBudget,
    initialize: bool,
) -> Result<PreparedIndex> {
    prepare_with(root, application, initialize, |host, port| {
        (host, port).to_socket_addrs()
    })
}

// The test seam supplies DNS answers only. SecretFile, resource admission,
// numeric address policy, directory inspection and connections remain actual.
fn prepare_with<R, Addresses>(
    root: &Resolved,
    application: &ResourceBudget,
    initialize: bool,
    resolve: R,
) -> Result<PreparedIndex>
where
    R: FnOnce(&str, u16) -> std::io::Result<Addresses>,
    Addresses: IntoIterator<Item = SocketAddr>,
{
    let permit = application
        .try_acquire(ResourceRequest::data(PREPARATION_BYTES, ITEMS, 0).expect("fixed reservation"))
        .map_err(|_| refused("application reservation unavailable"))?;
    if !root.local("index")
        || !root.flag("runtime.processes.clickhouse.enabled")
        || root.text("clickhouse.placement") != Some("local")
        || root.text("clickhouse.tls.mode") != Some("local-plaintext")
    {
        return Err(refused("unsupported deployment profile"));
    }
    let boundary = match root.text("clickhouse.tls.local_boundary") {
        Some("loopback") => PlaintextBoundary::Loopback,
        Some("private-bridge") => PlaintextBoundary::PrivateBridge,
        _ => return Err(refused("plaintext boundary missing")),
    };
    // Resolved already rejects ineffective TLS settings; preserve the refusal
    // even when a caller passes a subsequently modified root.
    for key in [
        "ca_file",
        "server_name",
        "client_cert_ref",
        "client_key_ref",
    ] {
        if root.text(&format!("clickhouse.tls.{key}")).is_some() {
            return Err(refused("TLS settings cannot be downgraded"));
        }
    }
    let n = |field: &str| root.number(&format!("runtime.processes.clickhouse.{field}"));
    let engine = ResourceBudget::new(
        ProcessEnvelope::new(
            n("memory_max_bytes"),
            n("baseline_bytes"),
            ResourceLimits::new(
                n("payload_bytes"),
                1,
                n("native_bytes"),
                n("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .map_err(|_| refused("ClickHouse process envelope invalid"))?,
    );
    // Only observe this existing root. The native Ingest root is deliberately
    // absent during fresh preparation; actual cross-store/mount disjointness is
    // checked after it exists, before IndexState creation and any remote DDL.
    let state_root = existing_root(required(root, "storage.index_root")?)?;
    let database = required(root, "clickhouse.database")?.to_owned();
    let writer_user = required(root, "clickhouse.write_user")?;
    let writer_ref = required(root, "clickhouse.write_password_ref")?;
    let initializer = if initialize {
        let user = required(root, "clickhouse.initialize_user")?;
        let reference = required(root, "clickhouse.initialize_password_ref")?;
        if user == writer_user
            || Some(user) == root.text("clickhouse.read_user")
            || user == "default"
        {
            return Err(refused("initializer must use a separate account"));
        }
        Some((user, reference))
    } else {
        // Do not open or otherwise inspect the staged initialization credential.
        None
    };
    let raw = required(root, "clickhouse.endpoint")?;
    if raw.len() > 4096 {
        return Err(refused("endpoint exceeds the selected bound"));
    }
    let mut endpoint = url::Url::parse(raw).map_err(|_| refused("endpoint invalid"))?;
    if endpoint.scheme() != "http"
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.port() == Some(0)
    {
        return Err(refused("endpoint outside the selected plaintext profile"));
    }
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| refused("endpoint port missing"))?;
    let address = match endpoint
        .host()
        .ok_or_else(|| refused("endpoint host missing"))?
    {
        url::Host::Ipv4(ip) => {
            select_plaintext_address([SocketAddr::new(IpAddr::V4(ip), port)], boundary)
        }
        url::Host::Ipv6(ip) => {
            select_plaintext_address([SocketAddr::new(IpAddr::V6(ip), port)], boundary)
        }
        url::Host::Domain(host) => {
            let addresses =
                resolve(host, port).map_err(|_| refused("route resolution unavailable"))?;
            select_plaintext_address(addresses, boundary)
        }
    }
    .map_err(|_| refused("resolved route outside the declared boundary"))?;
    endpoint
        .set_ip_host(address)
        .map_err(|_| refused("numeric route invalid"))?;
    let connection = |user: &str, reference: &str| -> Result<Arc<IndexConnection>> {
        let password = SecretFile::parse(reference)
            .and_then(|file| file.read(PASSWORD_BYTES))
            .map_err(|_| refused("protected credential unavailable"))?;
        IndexConnection::from_local_root(endpoint.to_string(), boundary, user.to_owned(), password)
            .map(Arc::new)
            .map_err(|_| refused("connection input outside the selected profile"))
    };
    let writer = connection(writer_user, writer_ref)?;
    let initializer = initializer
        .map(|(user, reference)| connection(user, reference))
        .transpose()?;
    Ok(PreparedIndex {
        writer,
        initializer,
        database,
        state_root,
        engine,
        _permit: permit,
    })
}

fn existing_root(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    IndexState::validate_root(path)
        .map_err(|_| refused("state root must be existing, canonical, private and owned"))?;
    // This read-only observation is not a retained store capability. The actual
    // custody owner reopens and binds descriptors after accepted policy.
    Ok(path.to_owned())
}

#[cfg(test)]
#[path = "prepare/tests.rs"]
mod tests;
