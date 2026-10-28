//! One service-owned remote slot and one completely joined local executor.
//! An uncertain remote query parks the slot; dropping a caller's result does not
//! return its engine credit. The service owns this slot for its entire lifetime.
use super::{PhysicalError, PhysicalLimits};
use crate::{
    runtime::OwnedPermit,
    secrets::SecretBytes,
    storage::{
        clickhouse::lane::{JoinedIndexLane, LaneError, OwnedIndexLane},
        indexer::plan::PlannedBatch,
        postgres::{connection_settings::PlaintextBoundary, transport::select_plaintext_address},
    },
};
use bigbrotr_model::ProcessIncarnation;
use hyper_util::client::legacy::connect::HttpConnector;
use std::{
    net::{IpAddr, SocketAddr},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

/// Fresh process incarnation plus a never-reused operation counter. SQL query
/// attempts add a bounded monotonically increasing suffix inside this operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexOperationId {
    incarnation: ProcessIncarnation,
    sequence: NonZeroU64,
}
impl IndexOperationId {
    pub fn new(incarnation: ProcessIncarnation, sequence: NonZeroU64) -> Self {
        Self {
            incarnation,
            sequence,
        }
    }
    /// Diagnostic identity only; it carries no dispatch or remote authority.
    pub fn incarnation(self) -> ProcessIncarnation {
        self.incarnation
    }
    pub fn sequence(self) -> NonZeroU64 {
        self.sequence
    }
    fn query(self, ordinal: u16) -> String {
        let incarnation: String = self
            .incarnation
            .as_bytes()
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect();
        format!("bigbrotr-index-{incarnation}-{}-{ordinal}", self.sequence)
    }
}

struct Reservation {
    operation: IndexOperationId,
    query: Option<String>,
    joined: bool,
    _permit: OwnedPermit,
}

/// Exactly one of these belongs to one stable runtime writer account. Creating
/// replacement users/slots is not an uncertainty-resolution procedure. The
/// server's persistent per-user limit remains required across process restart.
#[derive(Default)]
pub struct IndexEngineSlot {
    state: Mutex<Option<Reservation>>,
}
impl IndexEngineSlot {
    pub fn new() -> Self {
        Self::default()
    }
    /// Diagnostic identity only; absence while a local operation runs is not a
    /// remote cessation proof and cannot clear an unresolved reservation.
    pub fn unresolved_query(&self) -> Option<String> {
        self.state
            .lock()
            .expect("Index engine slot poisoned")
            .as_ref()
            .filter(|state| state.joined)
            .and_then(|state| state.query.clone())
    }
    pub(super) fn reserve(
        &self,
        operation: IndexOperationId,
        permit: OwnedPermit,
        limits: PhysicalLimits,
    ) -> Result<(), PhysicalError> {
        let units = permit.request().units();
        if units.data_bytes() < limits.query_memory_bytes
            || units.items() == 0
            || units.control_bytes() != 0
        {
            return Err(PhysicalError::InsufficientReservation);
        }
        let mut state = self.state.lock().map_err(|_| PhysicalError::Unavailable)?;
        if state.is_some() {
            return Err(PhysicalError::RemoteUnresolved);
        }
        *state = Some(Reservation {
            operation,
            query: None,
            joined: false,
            _permit: permit,
        });
        Ok(())
    }
    fn begin_query(&self, operation: IndexOperationId, query: String) -> Result<(), PhysicalError> {
        let mut state = self.state.lock().map_err(|_| PhysicalError::Unavailable)?;
        let retained = state.as_mut().ok_or(PhysicalError::Unavailable)?;
        if retained.operation != operation || retained.query.is_some() || retained.joined {
            return Err(PhysicalError::RemoteUnresolved);
        }
        retained.query = Some(query);
        Ok(())
    }
    fn completed_query(
        &self,
        operation: IndexOperationId,
        query: &str,
    ) -> Result<(), PhysicalError> {
        let mut state = self.state.lock().map_err(|_| PhysicalError::Unavailable)?;
        let retained = state.as_mut().ok_or(PhysicalError::Unavailable)?;
        if retained.operation != operation
            || retained.query.as_deref() != Some(query)
            || retained.joined
        {
            return Err(PhysicalError::RemoteUnresolved);
        }
        retained.query = None;
        Ok(())
    }
    pub(super) fn joined(&self, operation: IndexOperationId) {
        let mut state = self.state.lock().expect("Index engine slot poisoned");
        let Some(retained) = state.as_mut() else {
            return;
        };
        assert_eq!(
            retained.operation, operation,
            "Index operation cannot release another reservation"
        );
        retained.joined = true;
        if retained.query.is_none() {
            *state = None;
        }
        // Otherwise the service-owned slot keeps the actual engine permit. No
        // generic reset/timeout/empty-process-list API can discard uncertainty.
    }
}

/// Validated local plaintext deployment root plus bounded protected credentials.
/// The configuration owner must have accepted its loopback/container-network
/// boundary and resolved all names before constructing this numeric endpoint.
/// This adapter refuses DNS names and HTTPS rather than resolving or downgrading.
/// It neither reads a secret file nor prints the supplied credentials.
pub struct IndexConnection {
    endpoint: String,
    user: String,
    password: SecretBytes,
}
impl IndexConnection {
    pub fn from_local_root(
        endpoint: String,
        boundary: PlaintextBoundary,
        user: String,
        password: SecretBytes,
    ) -> Result<Self, PhysicalError> {
        let mut parsed = url::Url::parse(&endpoint).map_err(|_| PhysicalError::InvalidInput)?;
        if endpoint.len() > 4096
            || parsed.scheme() != "http"
            || parsed.host_str().is_none()
            || parsed.port() == Some(0)
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != "/"
            || user.is_empty()
            || user.len() > 128
            || user == "default"
            || !user
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'_' | b'-'))
        {
            return Err(PhysicalError::InvalidInput);
        }
        let address = match parsed.host() {
            Some(url::Host::Ipv4(ip)) => IpAddr::V4(ip),
            Some(url::Host::Ipv6(ip)) => IpAddr::V6(ip),
            _ => return Err(PhysicalError::InvalidInput),
        }
        .to_canonical();
        let address = select_plaintext_address(
            [SocketAddr::new(
                address,
                parsed
                    .port_or_known_default()
                    .ok_or(PhysicalError::InvalidInput)?,
            )],
            boundary,
        )
        .map_err(|_| PhysicalError::InvalidInput)?;
        parsed
            .set_ip_host(address)
            .map_err(|_| PhysicalError::InvalidInput)?;
        let text =
            std::str::from_utf8(password.as_bytes()).map_err(|_| PhysicalError::InvalidInput)?;
        if text.is_empty() || text.len() > 8192 || text.chars().any(char::is_control) {
            return Err(PhysicalError::InvalidInput);
        }
        Ok(Self {
            endpoint: parsed.into(),
            user,
            password,
        })
    }
    pub(super) fn client(&self, database: &str, limits: PhysicalLimits) -> clickhouse::Client {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(limits.query_timeout));
        crate::storage::clickhouse::transport::bounded_client(connector, limits.response_bytes)
            .with_url(&self.endpoint)
            .with_database(database)
            .with_user(&self.user)
            .with_password(
                std::str::from_utf8(self.password.as_bytes()).expect("validated credential"),
            )
            .with_setting("async_insert", "0")
            .with_setting("wait_for_async_insert", "1")
            .with_setting("max_memory_usage", limits.query_memory_bytes.to_string())
            .with_setting("max_threads", "1")
            .with_setting(
                "max_execution_time",
                limits.query_timeout.as_secs().to_string(),
            )
            .with_setting("timeout_overflow_mode", "throw")
            .with_setting("max_result_rows", limits.max_rows.to_string())
            .with_setting("result_overflow_mode", "throw")
            .with_setting("max_result_bytes", limits.response_bytes.get().to_string())
            .with_setting("max_query_size", super::MAX_QUERY_BYTES.to_string())
            .with_setting("max_block_size", "1")
            .with_setting("enable_http_compression", "0")
            .with_setting("insert_deduplicate", "0")
    }
    pub(super) fn user(&self) -> &str {
        &self.user
    }
}

pub(super) struct QueryOwner {
    pub client: clickhouse::Client,
    slot: Arc<IndexEngineSlot>,
    operation: IndexOperationId,
    ordinal: u16,
}
impl QueryOwner {
    pub fn new(
        client: clickhouse::Client,
        slot: Arc<IndexEngineSlot>,
        operation: IndexOperationId,
    ) -> Self {
        Self {
            client,
            slot,
            operation,
            ordinal: 0,
        }
    }
    pub fn begin(&mut self) -> Result<String, PhysicalError> {
        self.ordinal = self.ordinal.checked_add(1).ok_or(PhysicalError::Limit)?;
        let query = self.operation.query(self.ordinal);
        self.slot.begin_query(self.operation, query.clone())?;
        Ok(query)
    }
    /// Only after complete successful response consumption. An HTTP/driver
    /// error can itself be truncated by our caps and remains conservative unknown.
    pub fn completed(&self, query: &str) -> Result<(), PhysicalError> {
        self.slot.completed_query(self.operation, query)
    }
    pub fn failed(&self, query: &str, error: clickhouse::error::Error) -> PhysicalError {
        // The selected no-idle-pool HTTP/1 connector failed before handing a
        // connection to Hyper's try_send_request. No request bytes were sent for
        // this query. Other errors, including partial responses, remain unknown.
        if let clickhouse::error::Error::Network(inner) = &error
            && inner
                .downcast_ref::<hyper_util::client::legacy::Error>()
                .is_some_and(|error| error.is_connect())
            && self.completed(query).is_ok()
        {
            return PhysicalError::Unavailable;
        }
        PhysicalError::Query
    }
    pub async fn execute(&mut self, sql: &str) -> Result<(), PhysicalError> {
        if sql.len() > super::MAX_QUERY_BYTES {
            return Err(PhysicalError::Limit);
        }
        let query = self.begin()?;
        self.client
            .query(sql)
            .with_setting("query_id", &query)
            .execute()
            .await
            .map_err(|error| self.failed(&query, error))?;
        self.completed(&query)
    }
}

/// Input/decoder reservations live outside the request future and remain alive
/// through cancellation, complete executor destruction and the real OS join.
/// Field/drop order does not return engine credit for unresolved remote work.
pub struct IndexOperation<T: Send + 'static> {
    lane: Option<OwnedIndexLane<Result<T, PhysicalError>>>,
    _source: Option<Arc<PlannedBatch>>,
    _metadata: Option<Arc<OwnedPermit>>,
    slot: Arc<IndexEngineSlot>,
    operation: IndexOperationId,
}
impl<T: Send + 'static> IndexOperation<T> {
    /// Identity of this actual retained operation, independent of actor cursors.
    pub fn identity(&self) -> IndexOperationId {
        self.operation
    }
    pub(super) fn new(
        lane: OwnedIndexLane<Result<T, PhysicalError>>,
        source: Option<Arc<PlannedBatch>>,
        metadata: Option<Arc<OwnedPermit>>,
        slot: Arc<IndexEngineSlot>,
        operation: IndexOperationId,
    ) -> Self {
        Self {
            lane: Some(lane),
            _source: source,
            _metadata: metadata,
            slot,
            operation,
        }
    }
    pub fn request_cancel(&self) {
        if let Some(lane) = &self.lane {
            lane.request_cancel();
        }
    }
    pub fn try_join(
        &mut self,
    ) -> Result<Option<JoinedIndexLane<Result<T, PhysicalError>>>, LaneError> {
        let result = self
            .lane
            .as_mut()
            .ok_or(LaneError::AlreadyJoined)?
            .try_join()?;
        if result.is_some() {
            drop(self.lane.take());
            self.slot.joined(self.operation);
            drop(self._source.take());
            drop(self._metadata.take());
        }
        Ok(result)
    }
    pub fn join(mut self) -> Result<JoinedIndexLane<Result<T, PhysicalError>>, LaneError> {
        let result = self.lane.take().ok_or(LaneError::AlreadyJoined)?.join();
        self.slot.joined(self.operation);
        result
    }
}
impl<T: Send + 'static> Drop for IndexOperation<T> {
    fn drop(&mut self) {
        // OwnedIndexLane::Drop cancels then actually joins. The source and remote
        // slot remain fields throughout that wait; no timeout frees them early.
        if let Some(lane) = self.lane.take() {
            drop(lane);
            self.slot.joined(self.operation);
        }
    }
}

#[cfg(test)]
#[path = "operation/tests.rs"]
mod tests;
