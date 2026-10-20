//! Finite initial acquisition, serialized by the living native/Control owner.
//! Scanning is scheduling only: neither EOSE nor a pass claims durable coverage.
mod observation;
mod receive;
use super::owner::RuntimeContext;
use super::sync_diagnostic::{Stage, Trace};
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
                LiveSuccessorCertificationRequest, LiveSuccessorEventRequest,
                LiveSuccessorVisibilityRequest, RelaySourceError, RelaySourceRequest,
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

/// Preclaim capacity check for the complete fixed finite profile. TLS roots are
/// already retained by Sync::prepare. Conservatively overlap formation, wire
/// and admission scratch although those phases normally release some grants.
pub(super) fn preflight(budget: &ResourceBudget) -> Result<()> {
    use bigbrotr::acquisition::session::{ActiveRequest, RelaySession, RetainedRelayEvent};
    use bigbrotr_model::LocalObservation;
    let wire = EventLimits::default();
    let read = CertificationLimits::default();
    let observation = CertificationLimits {
        max_fact_bytes: 64 * 1024,
        max_dependencies: 8,
    };
    let dictionaries = AdmissionLimits {
        max_batch_bytes: 64 * 1024,
        max_dependencies: 8,
        max_outstanding_requests: 64,
    };
    let event = AdmissionLimits::default();
    let session = wire.wire_bytes
        + wire.parse_bytes
        + size_of::<ActiveRequest<'_>>()
        + size_of::<RelaySession<'_, '_, '_>>();
    let arrivals = 3
        * (wire.wire_bytes + size_of::<RetainedRelayEvent<'_>>() + size_of::<LocalObservation>())
        + size_of::<receive::Arrivals<'_>>()
        + bigbrotr_protocol::endpoint::MAX_ENDPOINT_BYTES;
    let data = AcquisitionScan::REQUIRED_BYTES + StoredRelaySource::REQUIRED_BYTES
        + wire.parse_bytes as u64
        + 2 * read.required_data_bytes().map_err(error)?
        + observation.required_data_bytes().map_err(error)?
        + dictionaries.required_dictionary_preparation_data_bytes().map_err(error)?
        + dictionaries.required_dictionary_data_bytes().map_err(error)?
        + event.required_data_bytes().map_err(error)?
        + (wire.parse_bytes + 4 * wire.event_bytes + session + arrivals) as u64
        // Current DirectConnector and DNS each charge 4MiB. RelayWebSocket
        // charges eight maximum frames plus 1MiB handshake/buffer overhead.
        + 8 * MIB + 8 * wire.wire_bytes as u64 + MIB;
    drop(
        budget
            .try_acquire(
                ResourceRequest::data(
                    data,
                    ITEMS,
                    event.required_native_bytes().map_err(error)?
                        + dictionaries.required_native_bytes().map_err(error)?,
                )
                .map_err(error)?,
            )
            .map_err(error)?,
    );
    Ok(())
}

pub(super) struct Sync {
    connector: DirectConnector,
    scan: Option<AcquisitionScan>,
    next_request: u64,
    lifecycle: Profile,
    profile: Option<(usize, bool)>,
}

fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (4, format!("initial Sync unavailable: {error:?}"))
}

impl Sync {
    pub(super) fn prepare(
        root: &Resolved,
        budget: &ResourceBudget,
        lifecycle: Profile,
    ) -> Result<Self> {
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
            next_request: if lifecycle == Profile::Initial { 3 } else { 1 },
            lifecycle,
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
            || accepted.number("ingest.producer_outstanding_requests")
                < self.lifecycle.minimum(true)
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
            .trace
            .wait(
                Stage::Nominate,
                None,
                None,
                work.owner.nominate_acquisition(
                    self.scan,
                    grant(work.budget, AcquisitionScan::REQUIRED_BYTES, 0)?,
                ),
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
        let job = match work
            .trace
            .wait(
                Stage::Claim,
                None,
                None,
                work.owner.claim_acquisition(job, &Cell::new(false)),
            )
            .await
        {
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
            .trace
            .wait(
                Stage::Scope,
                None,
                None,
                work.owner.read_acquisition_scope(
                    job.scope,
                    grant(work.budget, EventLimits::default().parse_bytes as u64, 0)?,
                ),
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
            .trace
            .wait(
                Stage::Source,
                None,
                None,
                work.owner.read_acquisition_source(
                    &scope,
                    grant(work.budget, StoredRelaySource::REQUIRED_BYTES, 0)?,
                ),
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
        let Some(visible) = work
            .trace
            .wait(
                Stage::Visibility,
                None,
                None,
                work.writer.visibility(
                    work.owner,
                    work.context,
                    LiveSuccessorVisibilityRequest {
                        ingest_root: path,
                        reference: scope.scope().visibility(),
                        limits,
                        policy_ttl: ttl,
                        cancelled: &work.cancel.requested,
                    },
                    grant(work.budget, limits.required_data_bytes().map_err(error)?, 0)?,
                    |d, h| validate_sync(work.root, self.lifecycle, d, h),
                ),
            )
            .await
            .map_err(error)?
        else {
            // Command acceptance checks identity, not native allocation. The
            // completed read rollback leaves this owner usable for other jobs.
            return Ok(());
        };
        // Attribution itself is scientific data. Refuse an unsafe envelope
        // before persisting a private/query-bearing URL, not merely at dial.
        // The source adapter repeats this restriction under fresh source locks.
        if work.stopped()
            || !source_visibility_allowed(source.endpoint(), source.route(), visible.entry())
        {
            return Ok(());
        }
        let (max_event_bytes, provenance) = self.profile.expect("accepted Sync profile");
        let Some(attribution) = work
            .observation(&scope, &source, provenance, &mut self.next_request)
            .await?
        else {
            return Ok(());
        };
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
            .trace
            .wait(
                Stage::RelayCollection,
                None,
                None,
                work.owner.run_relay_source(
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
                        authorize_sync_collection(
                            work.root,
                            d,
                            h,
                            endpoint,
                            route,
                            scope.selector(),
                        )
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
                ),
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
        for (frame, (captured, observation)) in arrivals.events.iter().enumerate() {
            if work.stopped() {
                return Ok(());
            }
            let frame = Some(u8::try_from(frame).expect("at most three retained frames"));
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
            let prepared =
                work.trace
                    .run(Stage::EventPreparation, frame, None, || -> Result<_> {
                        let Some((intent, permit)) = prepare()? else {
                            return Ok(None);
                        };
                        PreparedCertification::new(intent, max_event_bytes, permit)
                            .map(Some)
                            .map_err(error)
                    })?;
            let Some(prepared) = prepared else {
                continue;
            };
            let ttl = work.remaining()?;
            let certified = work
                .trace
                .wait(
                    Stage::Certification,
                    frame,
                    None,
                    work.writer.certify(
                        work.owner,
                        work.context,
                        LiveSuccessorCertificationRequest {
                            ingest_root: path,
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
                    ),
                )
                .await
                .map_err(error)?;
            let needs_admission =
                matches!(certified.outcome(), CertificationOutcome::NeedsAdmission);
            drop(certified);
            if !needs_admission {
                continue;
            }
            if self.next_request > u64::from(work.writer.limit(work.context)?) {
                // No ACK or slot retirement is implied by copying receipts.
                // This owner can keep serving/certifying without new mutations.
                return Ok(());
            }
            let event = work.trace.run(
                Stage::AdmissionPreparation,
                frame,
                Some(self.next_request),
                || -> Result<_> {
                    let (intent, permit) =
                        prepare()?.ok_or_else(|| error("retained input changed"))?;
                    PreparedEvent::new(
                        ProducerRequestId {
                            producer_slot_id: work.writer.slot(work.context)?,
                            producer_epoch: ProducerEpoch::new(work.writer.epoch()?)
                                .map_err(error)?,
                            request_sequence: RequestSequence::new(self.next_request)
                                .map_err(error)?,
                        },
                        intent,
                        max_event_bytes,
                        permit,
                    )
                    .map_err(error)
                },
            )?;
            let admission = work.admission_limits()?;
            let ttl = work.remaining()?;
            let outcome = work
                .trace
                .wait(
                    Stage::Admission,
                    frame,
                    Some(self.next_request),
                    work.writer.admit(
                        work.owner,
                        work.context,
                        LiveSuccessorEventRequest {
                            ingest_root: path,
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
                    ),
                )
                .await
                .map_err(error)?;
            if !matches!(outcome.outcome(), AdmissionOutcome::Durable(_)) {
                return Err(error("certified event changed before admission"));
            }
            work.trace.delivered_admission(self.next_request);
            self.next_request += 1;
        }
        Ok(())
    }
}

pub(super) struct Dispatch<'a> {
    pub owner: &'a mut ControlSession,
    pub writer: &'a mut NativeOwner,
    pub context: RuntimeContext<'a>,
    pub root: &'a Resolved,
    pub budget: &'a ResourceBudget,
    pub cancel: &'a Cancellation,
    pub deadline: Instant,
    pub trace: Trace<'a>,
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
    fn admission_limits(&self) -> Result<AdmissionLimits> {
        Ok(AdmissionLimits {
            max_outstanding_requests: u64::from(self.writer.limit(self.context)?),
            ..AdmissionLimits::default()
        })
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

fn validate_sync(
    root: &Resolved,
    profile: Profile,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let restored = super::super::accepted_policy(root, document, digest)?;
    accepted_owner_profile(&restored, profile)
}
