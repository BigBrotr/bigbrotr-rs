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
        job::{AcquisitionCandidate, AcquisitionJobBinding, AcquisitionScan},
        observation::StoredObservation,
        preparation::{CapturedEventInput, CapturedInputError},
        receive_clock::ReceiveClock,
        relay_source::{StoredRelaySource, source_visibility_allowed},
        scope::{AcquisitionEventContext, ScopeEventError, StoredAcquisitionScope},
    },
    management::sessions::SessionError,
    network::{DirectConnector, DnsResolver, TlsPolicy, TransportError},
    storage::{
        postgres::{
            control_owner::{
                LiveSuccessorCertificationRequest, LiveSuccessorEventRequest,
                LiveSuccessorVisibilityRequest, ProgressGrant, RelaySourceError,
                RelaySourceRequest, SyncProgress,
            },
            producer_sources::SourceBindingError,
        },
        rocks::store::{
            AdmissionLimits, AdmissionOutcome, CertificationLimits, CertificationOutcome,
            PreparedCertification, PreparedEvent,
        },
    },
};
use bigbrotr_model::{JobId, ProducerEpoch, ProducerRequestId, RequestSequence};
use bigbrotr_protocol::{EventError, EventLimits};
use std::time::Duration;

// TLS, scope, source, visibility, observation, dictionary preparation, batch,
// three retained frames, two session items, WebSocket, Direct, DNS and one
// effect input and progress transaction/ACK. These grants do not all overlap.
// Control, retained producer inputs and native memory have separate base slots.
// Safe-point renewal overlaps the retained arrivals/prepared input/progress,
// using that same permanently held Control/policy grant, with no new socket,
// input copy or native worker. Preflight already overlaps the complete Sync set.
pub(super) const ITEMS: u64 = 17;
pub(super) const INTERVAL: Duration = Duration::from_millis(250);

/// Preclaim capacity check for the complete fixed finite profile. TLS roots are
/// already retained by Sync::prepare. Conservatively overlap formation, wire
/// and admission scratch although those phases normally release some grants.
pub(super) fn requirements() -> Result<ResourceLimits> {
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
        + ProgressGrant::REQUIRED_DATA_BYTES
        + (wire.parse_bytes + 4 * wire.event_bytes + session + arrivals) as u64
        // Current DirectConnector and DNS each charge 4MiB. RelayWebSocket
        // charges eight maximum frames plus 1MiB handshake/buffer overhead.
        + 8 * MIB + 8 * wire.wire_bytes as u64 + MIB;
    Ok(ResourceLimits::new(
        data,
        ITEMS,
        event.required_native_bytes().map_err(error)?
            + dictionaries.required_native_bytes().map_err(error)?
            + ProgressGrant::REQUIRED_NATIVE_BYTES,
        0,
    ))
}

pub(super) fn preflight(budget: &ResourceBudget) -> Result<()> {
    let needed = requirements()?;
    drop(budget.try_reserve(needed).map_err(error)?);
    Ok(())
}

pub(super) struct Sync {
    connector: DirectConnector,
    nomination: Nomination,
    next_request: u64,
    lifecycle: Profile,
    profile: Option<(usize, bool)>,
}

/// Inert scheduling metadata, not a claim or authority. Keep the exact selected
/// job across capacity deferral; advancing a scan cannot silently skip it.
#[derive(Default)]
struct Nomination {
    scan: Option<AcquisitionScan>,
    job: Option<JobId>,
}
impl Nomination {
    fn retain(&mut self, candidate: AcquisitionCandidate) {
        assert!(
            self.job.is_none(),
            "selected job must be consumed before another scan"
        );
        self.scan = (!candidate.scan.complete()).then_some(candidate.scan);
        self.job = candidate.job;
    }
    fn reserve(
        &self,
        budget: &ResourceBudget,
    ) -> Result<Option<admission::Admission<ReservedBudget>>> {
        self.job
            .map(|_| admission::reserve(budget, requirements()?, "application"))
            .transpose()
    }
    fn take_for_claim(&mut self, cancelled: &AtomicBool) -> Result<Option<JobId>> {
        if cancelled.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.job
            .take()
            .map(Some)
            .ok_or_else(|| error("dispatch without nomination"))
    }
}

fn scan_grant(budget: &ResourceBudget) -> Result<Option<OwnedPermit>> {
    match budget
        .try_acquire(ResourceRequest::data(AcquisitionScan::REQUIRED_BYTES, 1, 0).map_err(error)?)
    {
        Ok(permit) => Ok(Some(permit)),
        Err(bigbrotr::runtime::AcquireError::Unavailable) => Ok(None),
        Err(failure) => Err(error(failure)),
    }
}

fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (4, format!("initial Sync unavailable: {error:?}"))
}

/// Preparation refusals have no durable classification in this first slice.
/// Later valid events retain mutation handoff and credit, but cannot advance
/// the classified acquisition prefix past that hole in this actual REQ.
struct ClassifiedPrefix {
    continuous: bool,
}
impl ClassifiedPrefix {
    fn prepared<T>(
        &mut self,
        input: std::result::Result<T, CapturedInputError>,
    ) -> Result<Option<T>> {
        let refusal = match input {
            Ok(input) => return Ok(Some(input)),
            Err(refusal) => refusal,
        };
        let peer_refusal = match refusal {
            CapturedInputError::Decode(reason) => reason != EventError::InvalidLimits,
            CapturedInputError::Scope(
                ScopeEventError::OutsideCollection
                | ScopeEventError::EphemeralExcluded
                | ScopeEventError::OutsideRequest,
            ) => true,
            _ => false,
        };
        if !peer_refusal {
            return Err(error(refusal));
        }
        self.continuous = false;
        Ok(None)
    }
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
            nomination: Nomination::default(),
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

    /// One bounded read-only scan before reserving a complete round. No job is
    /// claimed and no source action is issued. Temporary scan-grant pressure
    /// simply leaves the scan unchanged; it cannot establish full-work debt.
    pub(super) async fn nominate(&mut self, mut work: Dispatch<'_, '_>) -> Result<()> {
        if self.nomination.job.is_some() {
            return Ok(());
        }
        let Some(permit) = scan_grant(work.budget)? else {
            return Ok(());
        };
        if !work.safe_point(self, false).await? {
            return Ok(());
        }
        let candidate = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::Nominate,
            None,
            None,
            work.commands
                .owner
                .nominate_acquisition(self.nomination.scan, permit),
        )
        .await?
        .map_err(error)?;
        self.nomination.retain(candidate);
        Ok(())
    }

    pub(super) fn reserve(
        &self,
        budget: &ResourceBudget,
    ) -> Result<Option<admission::Admission<ReservedBudget>>> {
        self.nomination.reserve(budget)
    }

    pub(super) async fn dispatch(
        &mut self,
        mut work: Dispatch<'_, '_>,
        reservation: &ReservedBudget,
    ) -> Result<()> {
        let connector = self.connector.within(reservation);
        if !work.safe_point(self, false).await? {
            return Ok(());
        }
        let Some(job) = self.nomination.take_for_claim(&work.cancel.requested)? else {
            return Ok(());
        };
        let job = match commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::Claim,
            None,
            None,
            work.commands
                .owner
                .claim_acquisition(job, &Cell::new(false)),
        )
        .await?
        {
            Ok(job) => job,
            // These completed rollbacks/explicit COMMIT rejections leave the
            // owner reusable. Unknown outcomes never advance to a new effect.
            Err(SessionError::Forbidden | SessionError::Conflict) => return Ok(()),
            Err(e) => return Err(error(e)),
        };
        if !work.safe_point(self, false).await? {
            return Ok(());
        }
        let Some(scope) = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::Scope,
            None,
            None,
            work.commands.owner.read_acquisition_scope(
                job.scope,
                grant(work.budget, EventLimits::default().parse_bytes as u64, 0)?,
            ),
        )
        .await?
        .map_err(error)?
        else {
            return Err(error("claimed acquisition scope missing"));
        };
        if !work.safe_point(self, false).await? {
            return Ok(());
        }
        let Some(source) = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::Source,
            None,
            None,
            work.commands.owner.read_acquisition_source(
                &scope,
                grant(work.budget, StoredRelaySource::REQUIRED_BYTES, 0)?,
            ),
        )
        .await?
        .map_err(error)?
        else {
            return Ok(());
        };
        if !work.safe_point(self, false).await? {
            return Ok(());
        }
        let limits = work.certification_limits();
        let path = Path::new(
            work.root
                .text("storage.ingest_root")
                .expect("validated path"),
        );
        let ttl = work.remaining()?;
        let Some(visible) = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::Visibility,
            None,
            None,
            work.writer.visibility(
                work.commands.owner,
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
        .await?
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
        let Some(attribution) = self
            .observation(&mut work, &scope, &source, provenance)
            .await?
        else {
            return Ok(());
        };
        // No socket exists here and attribution has completed its handoff.
        // Start the wire phase with an actual renewal, not a residual slice
        // left by nomination/claim/dictionary operations. The receive cap and
        // every deadline once dispatched remain unchanged.
        if !work.safe_point(self, true).await? {
            return Ok(());
        }
        let hard = work.deadline;
        // Compute before SQL/connect, not when a slow peer finally handshakes.
        // Normal idle drops the poisoned protocol session inside a successful
        // callback, leaving the source adapter time to destroy socket/rollback.
        let soft = Instant::now() + (work.remaining()? / 2).min(Duration::from_millis(500));
        let ttl = work.remaining()?;
        let arrivals = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::RelayCollection,
            None,
            None,
            work.commands.owner.run_relay_source(
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
                &connector,
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
            ),
        )
        .await?;
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
        let mut prefix = ClassifiedPrefix { continuous: true };
        for (frame, (captured, observation)) in arrivals.events.iter().enumerate() {
            if work.stopped() {
                return Ok(());
            }
            let ordinal = u16::try_from(frame + 1).expect("at most three retained frames");
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
            let budget = work.budget;
            let prepare = || -> Result<_> {
                let permit = grant(budget, prepared_bytes, 0)?;
                Ok(CapturedEventInput::prepare(
                    captured,
                    job.id,
                    &scope,
                    &source,
                    &attribution,
                    permit,
                ))
            };
            let prepared =
                work.trace
                    .run(Stage::EventPreparation, frame, None, || -> Result<_> {
                        let Some(input) = prefix.prepared(prepare()?)? else {
                            return Ok(None);
                        };
                        PreparedCertification::from_captured(input, max_event_bytes)
                            .map(Some)
                            .map_err(error)
                    })?;
            let Some(prepared) = prepared else {
                continue;
            };
            let checkpoint = SyncProgress::capture(
                captured,
                ordinal,
                ProgressGrant::new(grant(
                    work.budget,
                    ProgressGrant::REQUIRED_DATA_BYTES,
                    ProgressGrant::REQUIRED_NATIVE_BYTES,
                )?)
                .map_err(error)?,
            )
            .map_err(error)?;
            let checkpoint = if prefix.continuous {
                checkpoint
            } else {
                checkpoint.mutation_only()
            };
            if !work.safe_point(self, false).await? {
                return Ok(());
            }
            let ttl = work.remaining()?;
            let certified = commands::sync_operation(
                work.trace,
                work.deadline,
                &work.cancel.requested,
                Stage::Certification,
                frame,
                None,
                work.writer.certify(
                    work.commands.owner,
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
                    Some(&checkpoint),
                    |d, h, e| policy.certification(d, h, e),
                ),
            )
            .await?
            .map_err(error)?;
            let needs_admission =
                matches!(certified.outcome(), CertificationOutcome::NeedsAdmission);
            drop(certified);
            if !needs_admission {
                continue;
            }
            if !work.writer.can_reserve(work.context, self.next_request)? {
                // A PG checkpoint alone returns no slot. Certification remains
                // available when native credit or retained history is full.
                return Ok(());
            }
            let event = work.trace.run(
                Stage::AdmissionPreparation,
                frame,
                Some(self.next_request),
                || -> Result<_> {
                    let input = prefix
                        .prepared(prepare()?)?
                        .ok_or_else(|| error("retained input changed"))?;
                    PreparedEvent::from_captured(
                        ProducerRequestId {
                            producer_slot_id: work.writer.slot(work.context)?,
                            producer_epoch: ProducerEpoch::new(work.writer.epoch()?)
                                .map_err(error)?,
                            request_sequence: RequestSequence::new(self.next_request)
                                .map_err(error)?,
                        },
                        input,
                        max_event_bytes,
                    )
                    .map_err(error)
                },
            )?;
            let admission = work.admission_limits()?;
            if !work.safe_point(self, false).await? {
                return Ok(());
            }
            let ttl = work.remaining()?;
            let outcome = commands::sync_operation(
                work.trace,
                work.deadline,
                &work.cancel.requested,
                Stage::Admission,
                frame,
                Some(self.next_request),
                work.writer.admit(
                    work.commands.owner,
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
                    Some(&checkpoint),
                    |d, h, e| policy.admission(d, h, e),
                ),
            )
            .await?
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

pub(super) struct Dispatch<'a, 'owner> {
    pub commands: &'a mut commands::CommandOwner<'owner>,
    pub writer: &'a mut NativeOwner,
    pub context: RuntimeContext<'a>,
    pub root: &'a Resolved,
    pub budget: &'a ResourceBudget,
    pub cancel: &'a Cancellation,
    pub management: &'a mut Option<management::Management>,
    pub due: &'a mut Instant,
    pub deadline: Instant,
    pub trace: Trace<'a>,
}
impl Dispatch<'_, '_> {
    async fn safe_point(&mut self, selected_sync: &mut Sync, wire_phase: bool) -> Result<bool> {
        self.trace.pause();
        let Some(window) = self
            .commands
            .sync_window(
                self.writer,
                selected_sync,
                self.management,
                self.due,
                wire_phase,
            )
            .await?
        else {
            return Ok(false);
        };
        self.deadline = window.dispatch_deadline;
        self.trace.begin(window);
        Ok(true)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_nomination_creates_no_full_work_debt_and_selected_job_survives_waits() {
        let needed = requirements().unwrap();
        let parent = ResourceBudget::new(
            ProcessEnvelope::new(
                needed.data_bytes() + needed.native_bytes(),
                0,
                needed,
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let held = parent
            .try_acquire(ResourceRequest::data(needed.data_bytes(), 1, 0).unwrap())
            .unwrap();
        let mut selected = Nomination {
            scan: Some(AcquisitionScan {
                admission_cut: 40,
                after_ordinal: 32,
            }),
            ..Nomination::default()
        };
        // An unavailable read is not proof of an empty scan. It cannot advance
        // the position, establish work debt or request the full round.
        let scan = selected.scan;
        assert!(scan_grant(&parent).unwrap().is_none());
        assert_eq!(selected.scan, scan);
        assert!(selected.reserve(&parent).unwrap().is_none());
        assert_eq!(parent.snapshot().active_permits(), 1);
        let job: JobId = [17; 16].try_into().unwrap();
        selected.retain(AcquisitionCandidate {
            scan: AcquisitionScan {
                admission_cut: 40,
                after_ordinal: 40,
            },
            job: Some(job),
        });
        assert!(selected.scan.is_none()); // Final scan position cannot drop this candidate.
        let start = Instant::now();
        let mut waiting = admission::Deferred::default();
        for seconds in [0, 15, 29] {
            assert!(matches!(
                selected.reserve(&parent).unwrap(),
                Some(admission::Admission::Deferred(_))
            ));
            waiting.waiting(admission::Role::Sync, start + Duration::from_secs(seconds));
            waiting.check(start + Duration::from_secs(seconds)).unwrap();
            assert_eq!(selected.job, Some(job));
        }
        assert!(waiting.check(start + Duration::from_secs(30)).is_err());
        assert_eq!(selected.job, Some(job));
        drop(held);
        let Some(admission::Admission::Granted(round)) = selected.reserve(&parent).unwrap() else {
            panic!("released physical credit must admit the selected round")
        };
        assert!(
            selected
                .take_for_claim(&AtomicBool::new(true))
                .unwrap()
                .is_none()
        );
        assert_eq!(selected.job, Some(job));
        assert_eq!(
            selected.take_for_claim(&AtomicBool::new(false)).unwrap(),
            Some(job)
        );
        assert!(selected.take_for_claim(&AtomicBool::new(false)).is_err());
        assert!(selected.reserve(&parent).unwrap().is_none());
        drop(round);
        parent.close();
        assert!(scan_grant(&parent).is_err()); // Only temporary Unavailable was suppressed.
    }

    #[test]
    fn funded_round_keeps_progress_ack_credit_when_parent_is_saturated() {
        let needed = requirements().unwrap();
        eprintln!(
            "{}",
            json!({"format":"bigbrotr-initial-budget-requirements/1","role":"sync",
            "scope":"compiled-finite-profile-not-a-capacity-claim",
            "application":{"data_bytes":needed.data_bytes(),"items":needed.items(),"native_bytes":needed.native_bytes(),"control_bytes":needed.control_bytes()},
            "retained_tls_data_bytes":TlsPolicy::FILE_ROOTS_BYTES})
        );
        let parent = ResourceBudget::new(
            ProcessEnvelope::new(
                needed.data_bytes() + needed.native_bytes(),
                0,
                needed,
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let scope = parent.try_reserve(needed).unwrap();
        let progress = ProgressGrant::new(grant(scope.budget(), 2 * MIB, 4096).unwrap()).unwrap();
        assert!(
            parent
                .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
                .is_err()
        );
        let scratch = grant(scope.budget(), EventLimits::default().parse_bytes as u64, 0).unwrap();
        drop(scratch);
        drop(scope);
        assert_eq!(
            parent.snapshot().used(),
            ResourceLimits::new(2 * MIB, 1, 4096, 0)
        );
        drop(progress);
        assert_eq!(parent.snapshot().active_permits(), 0);
    }

    #[test]
    fn a_preparation_hole_keeps_later_admission_eligible_without_reopening_progress() {
        for refusal in [
            CapturedInputError::Decode(EventError::InvalidJson),
            CapturedInputError::Decode(EventError::ArenaLimit),
            CapturedInputError::Scope(ScopeEventError::OutsideCollection),
            CapturedInputError::Scope(ScopeEventError::EphemeralExcluded),
            CapturedInputError::Scope(ScopeEventError::OutsideRequest),
        ] {
            let mut prefix = ClassifiedPrefix { continuous: true };
            assert_eq!(prefix.prepared(Ok(1)).unwrap(), Some(1));
            assert!(prefix.continuous);
            assert_eq!(prefix.prepared::<u8>(Err(refusal)).unwrap(), None);
            assert!(!prefix.continuous);
            assert_eq!(prefix.prepared(Ok(3)).unwrap(), Some(3));
            assert!(!prefix.continuous);
        }
    }

    #[test]
    fn resource_and_context_failures_cannot_be_hidden_as_peer_refusals() {
        for failure in [
            CapturedInputError::Budget,
            CapturedInputError::Decode(EventError::InvalidLimits),
            CapturedInputError::Scope(ScopeEventError::InvalidRequest),
            CapturedInputError::Scope(ScopeEventError::SourceMismatch),
            CapturedInputError::Observation(
                bigbrotr::acquisition::observation::ObservationError::SourceMismatch,
            ),
            CapturedInputError::Observation(
                bigbrotr::acquisition::observation::ObservationError::ClockMismatch,
            ),
            CapturedInputError::Intent,
        ] {
            let mut prefix = ClassifiedPrefix { continuous: true };
            assert!(prefix.prepared::<u8>(Err(failure)).is_err());
        }
    }
}
