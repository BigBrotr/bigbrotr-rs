//! Finite no-auth source operation. Policy and enrollment locks outlive every
//! socket poll; neither a prepared visibility mapping nor copied IDs grant work.
use super::{ControlSession, OwnerError, backend, require, snapshot, transaction_error};
use crate::{
    acquisition::{
        job::AcquisitionJobBinding,
        relay_source::{RelaySourceBinding, source_visibility_allowed},
    },
    network::{
        DirectAttempt, DirectConnector, DirectPolicy, DirectStream, RouteSelection, Target,
        TransportError,
    },
    runtime::{
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
        policy_document::PolicyDocumentVersion,
    },
    storage::{
        postgres::{
            initialization::application_transaction,
            producer_sources::{self, SourceBindingError},
        },
        rocks::store::StoredVisibility,
    },
};
use bigbrotr_model::{DatasetId, PolicyEpoch};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{io, time::Duration};
use tokio::time::{Instant, timeout_at};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelaySourceError {
    Control(OwnerError),
    Source(SourceBindingError),
    Visibility,
    Transport(TransportError),
}
impl std::fmt::Display for RelaySourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "finite relay source operation unavailable: {self:?}")
    }
}
impl std::error::Error for RelaySourceError {}
impl From<OwnerError> for RelaySourceError {
    fn from(value: OwnerError) -> Self {
        Self::Control(value)
    }
}

pub struct RelaySourceRequest<'a> {
    pub source: RelaySourceBinding,
    /// Required at schema24; the original job and scope are checked under locks.
    pub acquisition: Option<AcquisitionJobBinding>,
    /// Previously resolved through actual native admission; borrowing retains
    /// its memory permit. The locked access context must name this exact mapping.
    pub visibility: &'a StoredVisibility,
    pub policy_ttl: Duration,
    pub deadline: Instant,
}

impl ControlSession {
    /// The trusted validator must restore BOTH complete accepted documents under
    /// actual roots and enforce selected service, fixed membership and bans for
    /// this exact endpoint/route. It must not use a staged seed or browser grant.
    /// The caller owns policy/response memory and the connection driver until
    /// joined; the callback owns bounded protocol work and cannot retain a socket.
    /// Returned data still needs current admission checks with `input.source`.
    pub async fn run_relay_source<T>(
        &mut self,
        input: RelaySourceRequest<'_>,
        connector: &DirectConnector,
        mut authorize: impl FnMut(
            &str,
            [u8; 32],
            &Endpoint,
            RouteSelection,
        ) -> Result<DirectPolicy, OwnerError>,
        operation: impl for<'a, 'owner> AsyncFnOnce(&'a mut DirectStream<'owner>) -> io::Result<T>,
    ) -> Result<T, RelaySourceError> {
        let ttl = PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?;
        let deadline = input.deadline.min(Instant::now() + input.policy_ttl);
        if Instant::now() >= deadline {
            return Err(RelaySourceError::Transport(TransportError::Deadline));
        }
        // Start before even schema/migration lock I/O. A slow valid response
        // consumes the original lease instead of starting another full window.
        let mut lease = PolicyLeaseTracker::new(
            self.request.dataset,
            self.request.incarnation,
            PolicyEpoch::new(self.validated_policy.policy_revision),
            ttl,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        timeout_at(deadline, async {
            let tx = application_transaction(&mut self.client, 20)
                .await
                .map_err(transaction_error)?;
            let result = async {
                require(&tx, self.request.dataset, self.identity).await?;
                let job_remaining = producer_sources::require_job(
                    &tx,
                    self.request.dataset,
                    input.source,
                    input.acquisition,
                )
                .await
                .map_err(RelaySourceError::Source)?;
                producer_sources::require_current_enrollment(
                    &tx,
                    self.request.dataset,
                    input.source,
                    Some(input.visibility.entry().reference()),
                )
                .await
                .map_err(RelaySourceError::Source)?;
                let (endpoint, route) =
                    load_destination(&tx, self.request.dataset, input.source).await?;
                if !source_visibility_allowed(&endpoint, route, input.visibility.entry()) {
                    return Err(RelaySourceError::Visibility);
                }
                let mut destination = None;
                // require_current_enrollment already owns this dataset lock.
                // snapshot rechecks deployment identity and both documents; a
                // refusal of either never leaves the first decision usable.
                let policy = snapshot(&tx, &self.request, &mut |document, digest| {
                    if PolicyDocumentVersion::from_document(document)
                        .map_err(|_| OwnerError::InvalidPolicy)?
                        != PolicyDocumentVersion::V2
                    {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    destination = Some(authorize(document, digest, &endpoint, route)?);
                    Ok(())
                })
                .await?;
                let remaining: i64 = tx
                    .query_one(
                        REMAINING,
                        &[
                            &&self.identity.resource_id.as_bytes()[..],
                            &self.identity.epoch.get().to_string(),
                        ],
                    )
                    .await
                    .map_err(backend)?
                    .try_get(0)
                    .map_err(backend)?;
                let remaining = u64::try_from(remaining)
                    .ok()
                    .filter(|&v| v > 0)
                    .ok_or(OwnerError::Unavailable)?;
                let epoch = PolicyEpoch::new(policy.policy_revision);
                lease
                    .complete_refresh(
                        &ticket,
                        LeaseResponse {
                            dataset: self.request.dataset,
                            process_incarnation: self.request.incarnation,
                            policy_epoch: epoch,
                            ttl: input
                                .policy_ttl
                                .min(Duration::from_micros(remaining))
                                .min(job_remaining.unwrap_or(input.policy_ttl)),
                        },
                    )
                    .map_err(|_| OwnerError::Unavailable)?;
                let window = lease
                    .begin_action(epoch)
                    .and_then(|action| action.complete(input.policy_ttl))
                    .map_err(|_| OwnerError::Unavailable)?;
                let (attempt, _cancel) = DirectAttempt::new(
                    Target::new(endpoint).map_err(|_| OwnerError::InvalidMetadata)?,
                    destination.ok_or(OwnerError::InvalidPolicy)?,
                    window,
                    deadline,
                );
                // PostgreSQL may release its locks after backend loss. Observed
                // loss must refuse even already decrypted TLS data immediately;
                // an undetected partition remains bounded by the finite lease.
                let live = || !tx.client().is_closed();
                connector
                    .run(attempt.with_liveness_check(&live), operation)
                    .await
                    .map(|value| (value, epoch))
                    .map_err(RelaySourceError::Transport)
            }
            .await;
            // The connector has dropped socket/work before any lock is released.
            // Timeout/cancellation or failed rollback keeps this session poisoned.
            tx.rollback().await.map_err(backend)?;
            if self.client.is_closed() {
                return Err(OwnerError::Unavailable.into());
            }
            self.available = true;
            let (value, epoch) = result?;
            // Cleanup may outlast a shorter Control remainder. Never deliver a
            // response under an expired lease just because rollback succeeded.
            if Instant::now() >= deadline {
                return Err(RelaySourceError::Transport(TransportError::Deadline));
            }
            lease
                .check_delivery(epoch)
                .map_err(|_| OwnerError::Unavailable)?;
            Ok(value)
        })
        .await
        .unwrap_or(Err(RelaySourceError::Transport(TransportError::Deadline)))
    }
}

const REMAINING: &str = "
    SELECT lease_expires_at_us - (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint
    FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric";
const DESTINATION: &str = "
    SELECT substring(e.url_bytes FROM 1 FOR 8193), substring(r.route_bytes FROM 1 FOR 20)
    FROM control.relay_endpoints e JOIN control.network_routes r
        ON r.dataset_id=e.dataset_id AND r.endpoint_id=e.endpoint_id
    WHERE e.dataset_id=$1::bytea AND e.endpoint_id=$2::bytea AND r.route_id=$3::bytea
        AND e.normalization_version=1 AND r.route_version=1";

pub(super) async fn load_destination(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    source: RelaySourceBinding,
) -> Result<(Endpoint, RouteSelection), OwnerError> {
    let row = tx
        .query_opt(
            DESTINATION,
            &[
                &&dataset.as_bytes()[..],
                &&source.endpoint_id.as_bytes()[..],
                &&source.route_id.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?
        .ok_or(OwnerError::InvalidMetadata)?;
    let url = std::str::from_utf8(row.try_get::<_, &[u8]>(0).map_err(backend)?)
        .map_err(|_| OwnerError::InvalidMetadata)?;
    let endpoint = Endpoint::parse(url).map_err(|_| OwnerError::InvalidMetadata)?;
    if endpoint.as_str() != url {
        return Err(OwnerError::InvalidMetadata);
    }
    let route = RouteSelection::from_canonical_bytes(row.try_get(1).map_err(backend)?)
        .map_err(|_| OwnerError::InvalidMetadata)?;
    Ok((endpoint, route))
}
