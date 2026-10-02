//! Finite initial acquisition, serialized by the living native/Control owner.
//! Scanning is scheduling only: neither EOSE nor a pass claims durable coverage.
mod observation;
mod receive;
use super::*;
use crate::config::acquisition::{SyncArrivalPolicy, authorize_sync_collection};
use bigbrotr::{
    acquisition::{
        job::{AcquisitionJobBinding, AcquisitionScan},
        observation::StoredObservation,
        receive_clock::ReceiveClock,
        relay_source::{StoredRelaySource, source_visibility_allowed},
        scope::{AcquisitionEventContext, StoredAcquisitionScope},
    },
    management::sessions::SessionError,
    network::{DirectConnector, DnsResolver, TlsPolicy, TransportError},
    storage::{
        postgres::{
            control_owner::{
                InitialCertificationRequest, InitialEventRequest, InitialVisibilityRequest,
                RelaySourceError, RelaySourceRequest,
            },
            producer_sources::SourceBindingError,
        },
        rocks::store::{
            AdmissionLimits, AdmissionOutcome, CertificationLimits, CertificationOutcome,
            PreparedCertification, PreparedEvent,
        },
    },
};
use bigbrotr_model::{EventIntent, ProducerEpoch, ProducerRequestId, RequestSequence};
use bigbrotr_protocol::{EventLimits, decode_event_json};
use std::time::Duration;

// TLS, scope, source, visibility, observation, dictionary preparation, batch,
// three retained frames, two session items, WebSocket, Direct, DNS and one
// effect input. Preparation/socket/effect grants do not all overlap. Control,
// retained producer inputs and native memory already have separate base slots.
pub(super) const ITEMS: u64 = 16;
pub(super) const INTERVAL: Duration = Duration::from_millis(250);

pub(super) struct Sync {
    connector: DirectConnector,
    scan: Option<AcquisitionScan>,
    next_request: u64,
    profile: Option<(usize, bool)>,
}

fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (4, format!("initial Sync unavailable: {error:?}"))
}

impl Sync {
    pub(super) fn prepare(root: &Resolved, budget: &ResourceBudget) -> Result<Self> {
        if !root.local("sync") {
            return Err((3, "initial Sync requires local placement".into()));
        }
        let resolver = required(root, "network.dns.resolver")?
            .parse()
            .map_err(|_| (3, "initial Sync DNS resolver is invalid".into()))?;
        let paths: Vec<_> = root
            .value("network.tls.extra_roots")
            .as_array()
            .expect("validated roots")
            .iter()
            .map(|path| Path::new(path.as_str().expect("validated path")))
            .collect();
        let tls = TlsPolicy::from_der_files(&paths, grant(budget, TlsPolicy::FILE_ROOTS_BYTES, 0)?)
            .map_err(error)?;
        Ok(Self {
            connector: DirectConnector::new(
                DnsResolver::new(resolver, budget.clone()).map_err(error)?,
                tls,
                budget.clone(),
            ),
            scan: None,
            next_request: 3,
            profile: None,
        })
    }

    pub(super) fn accept(&mut self, accepted: &Resolved) -> std::result::Result<(), OwnerError> {
        let bytes = usize::try_from(accepted.number("ingest.max_event_bytes"))
            .map_err(|_| OwnerError::InvalidPolicy)?;
        // This explicit initial profile uses the already qualified finite wire
        // and decoder bounds. Larger configured events need the later scheduler.
        if !accepted.local("sync")
            || !accepted.flag("sync.enabled")
            || accepted.number("ingest.producer_outstanding_requests") < 4
            || bytes == 0
            || bytes > EventLimits::default().event_bytes
        {
            return Err(OwnerError::InvalidPolicy);
        }
        let profile = (
            bytes,
            accepted.text("sync.provenance") == Some("local_first_witness"),
        );
        if self.profile.is_some_and(|old| old != profile) {
            return Err(OwnerError::InvalidPolicy);
        }
        self.profile = Some(profile);
        Ok(())
    }

    pub(super) async fn dispatch(&mut self, mut work: Dispatch<'_>) -> Result<()> {
        let candidate = work
            .owner
            .nominate_acquisition(
                self.scan,
                grant(work.budget, AcquisitionScan::REQUIRED_BYTES, 0)?,
            )
            .await
            .map_err(error)?;
        self.scan = (!candidate.scan.complete()).then_some(candidate.scan);
        if work.stopped() {
            return Ok(());
        }
        let Some(job) = candidate.job else {
            return Ok(());
        };
        let job = match work.owner.claim_acquisition(job, &Cell::new(false)).await {
            Ok(job) => job,
            // These completed rollbacks/explicit COMMIT rejections leave the
            // owner reusable. Unknown outcomes never advance to a new effect.
            Err(SessionError::Forbidden | SessionError::Conflict) => return Ok(()),
            Err(e) => return Err(error(e)),
        };
        if work.stopped() {
            return Ok(());
        }
        let Some(scope) = work
            .owner
            .read_acquisition_scope(
                job.scope,
                grant(work.budget, EventLimits::default().parse_bytes as u64, 0)?,
            )
            .await
            .map_err(error)?
        else {
            return Err(error("claimed acquisition scope missing"));
        };
        if work.stopped() {
            return Ok(());
        }
        let Some(source) = work
            .owner
            .read_acquisition_source(
                &scope,
                grant(work.budget, StoredRelaySource::REQUIRED_BYTES, 0)?,
            )
            .await
            .map_err(error)?
        else {
            return Ok(());
        };
        if work.stopped() {
            return Ok(());
        }
        let limits = work.certification_limits();
        let path = Path::new(
            work.root
                .text("storage.ingest_root")
                .expect("validated path"),
        );
        let ttl = work.remaining()?;
        let visible = work
            .owner
            .read_initial_visibility(
                work.store,
                InitialVisibilityRequest {
                    ingest_root: path,
                    registration: work.spec,
                    reference: scope.scope().visibility(),
                    limits,
                    policy_ttl: ttl,
                    cancelled: &work.cancel.requested,
                },
                grant(work.budget, limits.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate(work.root, d, h),
            )
            .await
            .map_err(error)?
            .ok_or_else(|| error("acquisition visibility is not committed"))?;
        // Attribution itself is scientific data. Refuse an unsafe envelope
        // before persisting a private/query-bearing URL, not merely at dial.
        // The source adapter repeats this restriction under fresh source locks.
        if work.stopped()
            || !source_visibility_allowed(source.endpoint(), source.route(), visible.entry())
        {
            return Ok(());
        }
        let (max_event_bytes, provenance) = self.profile.expect("accepted Sync profile");
        let attribution = work
            .observation(&scope, &source, provenance, &mut self.next_request)
            .await?;
        if work.stopped() {
            return Ok(());
        }
        let hard = work.deadline;
        // Compute before SQL/connect, not when a slow peer finally handshakes.
        // Normal idle drops the poisoned protocol session inside a successful
        // callback, leaving the source adapter time to destroy socket/rollback.
        let soft = Instant::now() + (work.remaining()? / 2).min(Duration::from_millis(500));
        let ttl = work.remaining()?;
        let arrivals = work
            .owner
            .run_relay_source(
                RelaySourceRequest {
                    source: source.binding(),
                    acquisition: Some(AcquisitionJobBinding {
                        job: job.id,
                        scope: scope.identity(),
                    }),
                    visibility: &visible,
                    policy_ttl: ttl,
                    deadline: hard,
                },
                &self.connector,
                |d, h, endpoint, route| {
                    if endpoint != source.endpoint() || route != source.route() {
                        return Err(OwnerError::InvalidMetadata);
                    }
                    authorize_sync_collection(work.root, d, h, endpoint, route, scope.selector())
                },
                async |stream| {
                    receive::collect(
                        stream,
                        work.budget,
                        &scope,
                        &attribution,
                        source.route(),
                        max_event_bytes,
                        soft,
                        work.cancel,
                    )
                    .await
                },
            )
            .await;
        let arrivals = match arrivals {
            Ok(arrivals) => arrivals,
            // run_relay_source has completed its rollback before these known
            // pre-socket refusals. No adapter timeout/transport error is hidden.
            Err(
                RelaySourceError::Source(
                    SourceBindingError::Conflict | SourceBindingError::Unavailable,
                )
                | RelaySourceError::Visibility
                | RelaySourceError::Control(OwnerError::InvalidPolicy),
            ) => return Ok(()),
            // These errors can only return after the source adapter finished
            // rollback. A refused/offline/malformed peer yields this candidate;
            // it cannot stop HTTP or block the next admission in the scan.
            Err(RelaySourceError::Transport(
                TransportError::Budget
                | TransportError::Dns(_)
                | TransportError::Policy(_)
                | TransportError::Connect
                | TransportError::Tls
                | TransportError::Operation,
            )) => return Ok(()),
            Err(e) => return Err(error(e)),
        };
        if work.stopped() {
            return Ok(());
        }
        for (captured, observation) in &arrivals.events {
            if work.stopped() {
                return Ok(());
            }
            let context =
                AcquisitionEventContext::for_job(job.id, &scope, captured.request().filters())
                    .map_err(error)?;
            let policy = SyncArrivalPolicy {
                root: work.root,
                endpoint: source.endpoint(),
                route: source.route(),
                selector: scope.selector(),
                visibility: scope.scope().visibility(),
                observation: *observation,
                provenance_enabled: provenance,
            };
            // Reserve before decoding. Parser work plus event/preimage/canonical
            // encoding overlap; the existing prepared-input constructor checks
            // actual retained capacities against this same owned reservation.
            let prepared_bytes = captured.limits().parse_bytes as u64 + 4 * max_event_bytes as u64;
            let prepare = || -> Result<_> {
                let permit = grant(work.budget, prepared_bytes, 0)?;
                let Ok(decoded) = decode_event_json(captured.json(), captured.limits()) else {
                    return Ok(None);
                };
                // Peer data outside the exact intent is a local refusal before
                // any SQL/native operation, never a poisoned Control owner.
                if context
                    .check(source.binding(), scope.scope().visibility(), &decoded)
                    .is_err()
                {
                    return Ok(None);
                }
                let intent =
                    EventIntent::network(1, scope.scope().visibility(), decoded, *observation)
                        .map_err(error)?;
                Ok(Some((intent, permit)))
            };
            let Some((intent, permit)) = prepare()? else {
                continue;
            };
            let prepared =
                PreparedCertification::new(intent, max_event_bytes, permit).map_err(error)?;
            let ttl = work.remaining()?;
            let certified = work
                .owner
                .certify_initial_event(
                    work.store,
                    InitialCertificationRequest {
                        ingest_root: path,
                        registration: work.spec,
                        event: prepared,
                        relay_source: Some(source.binding()),
                        acquisition: Some(context),
                        provenance_enabled: provenance,
                        limits,
                        policy_ttl: ttl,
                        cancelled: &work.cancel.requested,
                    },
                    grant(work.budget, limits.required_data_bytes().map_err(error)?, 0)?,
                    |d, h, e| policy.certification(d, h, e),
                )
                .await
                .map_err(error)?;
            let needs_admission =
                matches!(certified.outcome(), CertificationOutcome::NeedsAdmission);
            drop(certified);
            if !needs_admission {
                continue;
            }
            if self.next_request > u64::from(work.spec.outstanding_request_limit) {
                // No ACK or slot retirement is implied by copying receipts.
                // This owner can keep serving/certifying without new mutations.
                return Ok(());
            }
            let (intent, permit) = prepare()?.ok_or_else(|| error("retained input changed"))?;
            let event = PreparedEvent::new(
                ProducerRequestId {
                    producer_slot_id: work.spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).unwrap(),
                    request_sequence: RequestSequence::new(self.next_request).map_err(error)?,
                },
                intent,
                max_event_bytes,
                permit,
            )
            .map_err(error)?;
            let admission = work.admission_limits();
            let ttl = work.remaining()?;
            let outcome = work
                .owner
                .admit_initial_event(
                    work.store,
                    InitialEventRequest {
                        ingest_root: path,
                        registration: work.spec,
                        event,
                        relay_source: Some(source.binding()),
                        acquisition: Some(context),
                        provenance_enabled: provenance,
                        recorded_at: recorded()?,
                        limits: admission,
                        policy_ttl: ttl,
                        cancelled: &work.cancel.requested,
                    },
                    grant(
                        work.budget,
                        admission.required_data_bytes().map_err(error)?,
                        admission.required_native_bytes().map_err(error)?,
                    )?,
                    |d, h, e| policy.admission(d, h, e),
                )
                .await
                .map_err(error)?;
            if !matches!(outcome.outcome(), AdmissionOutcome::Durable(_)) {
                return Err(error("certified event changed before admission"));
            }
            self.next_request += 1;
        }
        Ok(())
    }
}

pub(super) struct Dispatch<'a> {
    pub owner: &'a mut ControlSession,
    pub store: &'a mut IngestStore,
    pub spec: &'a InitialRegistration,
    pub root: &'a Resolved,
    pub budget: &'a ResourceBudget,
    pub cancel: &'a Cancellation,
    pub deadline: Instant,
}
impl Dispatch<'_> {
    fn stopped(&self) -> bool {
        self.cancel.requested.load(Ordering::Acquire)
    }
    fn remaining(&self) -> Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| error("dispatch deadline exhausted"))
    }
    fn admission_limits(&self) -> AdmissionLimits {
        AdmissionLimits {
            max_outstanding_requests: u64::from(self.spec.outstanding_request_limit),
            ..AdmissionLimits::default()
        }
    }
    fn certification_limits(&self) -> CertificationLimits {
        CertificationLimits::default()
    }
}
fn recorded() -> Result<bigbrotr_model::ObservedTime> {
    Ok(ReceiveClock::new()
        .map_err(error)?
        .sample()
        .map_err(error)?
        .received_at())
}
