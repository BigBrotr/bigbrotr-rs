//! One explicitly armed passive source, serialized by the original Native owner.
//! This local trigger grants no socket or admission authority and is not a fleet
//! scheduler. Compact failed captures remain owned until this lifetime stops.
mod selection;
use super::{owner::RuntimeContext, *};
use bigbrotr::{
    acquisition::{
        monitor::{AttemptSpec, CaptureFailure, CapturedMeasurement, PreparedPassiveRead},
        relay_source::{RelaySourceBinding, StoredRelaySource, source_visibility_allowed},
    },
    network::{DirectConnector, DirectPolicy, DnsResolver, RouteSelection, TlsPolicy},
    storage::{
        postgres::control_owner::{
            InitialMeasurementRequest, LiveMeasurementRequest,
            LiveSuccessorDictionaryAppendRequest, LiveSuccessorVisibilityRequest, MonitorCreation,
            MonitorEvidenceBinding, MonitorIntent, MonitorNoIoProof, MonitorSourceError,
            ProgressGrant,
        },
        rocks::store::{AdmissionLimits, CertificationLimits},
    },
};
use bigbrotr_model::{
    AccessClass, AccessScopeDescriptor, ClockQuality, ClockSource, DescriptorLabel,
    DictionaryDescriptor, DirectRouteClass, DirectRouteDescriptor, DurationUs, EndpointDescriptor,
    MeasurementAttemptId, MeasurementEvidenceIntent, ObserverDescriptor, QualifiedRef,
    RequestSequence, RouteClass,
    measurement::{MeasurementContext, MeasurementSubject, PassiveReadProfile},
};
use bigbrotr_protocol::endpoint::Endpoint;
pub(super) use selection::SelectionPath;
use selection::{Selection, Visibility};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const ITEMS: u64 = 20;
pub(super) const INTERVAL: Duration = Duration::from_millis(250);
const FIXED_BYTES: u64 = 64 * 1024;
const REQUEST_DICTIONARIES: u64 = 3;
const REQUEST_MEASUREMENT: u64 = 4;

fn failure(message: &str) -> (u8, String) {
    (4, format!("initial Monitor once: {message}"))
}
fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (4, format!("initial Monitor once refused: {error:?}"))
}

pub(super) fn accept(root: &Resolved) -> std::result::Result<(), OwnerError> {
    let exact = |key| {
        root.value(key)
            .as_array()
            .is_some_and(|values| values.len() == 2 && values[0] == "open" && values[1] == "read")
    };
    if !root.local("monitor")
        || !root.flag("monitor.enabled")
        || !exact("monitor.checks")
        || !exact("monitor.store_checks")
        || root.number("monitor.check_timeout") != 15_000_000
        || root.number("monitor.retries") != 1
        || root.number("network.connect_timeout") != 10_000_000
        || root.number("network.max_message_bytes") != 2 * MIB
        || root.number("pipeline.parse_arena_bytes") != 8 * MIB
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

fn validate(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let accepted = super::super::accepted_policy(root, document, digest)?;
    super::accepted_profile(&accepted)?;
    accept(&accepted)
}
fn authorize(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
    endpoint: &Endpoint,
    route: RouteSelection,
) -> std::result::Result<DirectPolicy, OwnerError> {
    validate(root, document, digest)?;
    crate::config::acquisition::authorize_monitor_source(root, document, digest, endpoint, route)
}
fn limits(outstanding: u64) -> AdmissionLimits {
    AdmissionLimits {
        max_batch_bytes: 64 * 1024,
        max_dependencies: 8,
        max_outstanding_requests: outstanding,
    }
}
fn reads() -> CertificationLimits {
    CertificationLimits {
        max_fact_bytes: 64 * 1024,
        max_dependencies: 8,
    }
}

fn requirements() -> Result<ResourceLimits> {
    let limits = limits(2);
    Ok(ResourceLimits::new(
        PreparedPassiveRead::REQUIRED_DATA_BYTES
            + StoredRelaySource::REQUIRED_BYTES
            + reads().required_data_bytes().map_err(error)?
            + limits
                .required_dictionary_preparation_data_bytes()
                .map_err(error)?
            + limits.required_dictionary_data_bytes().map_err(error)?
            + limits.required_dictionary_data_bytes().map_err(error)?
            + ProgressGrant::REQUIRED_DATA_BYTES
            + FIXED_BYTES,
        ITEMS - 2,
        limits.required_native_bytes().map_err(error)? * 2 + ProgressGrant::REQUIRED_NATIVE_BYTES,
        0,
    ))
}

/// The original process budget admits the driver's ONE funded reservation.
/// Metadata/dictionary work has its ordinary grants; it creates no child scope.
/// Acquire the shared evidence kernel's full permit before that reservation and
/// before dispatch COMMIT/IO. Either preparation failure releases both owners.
fn fund_attempt(
    parent: &ResourceBudget,
    endpoint: &Endpoint,
    spec: AttemptSpec,
    limits: AdmissionLimits,
) -> Result<(PreparedPassiveRead, OwnedPermit)> {
    let admission = grant(
        parent,
        limits.required_dictionary_data_bytes().map_err(error)?,
        limits.required_native_bytes().map_err(error)?,
    )?;
    let capture = PreparedPassiveRead::prepare(parent, endpoint, spec).map_err(error)?;
    Ok((capture, admission))
}

/// Only fixed measurements and private no-I/O proofs survive a source error.
/// Raw EVENT bodies, AUTH challenges and borrowed streams never enter this state.
#[allow(
    clippy::large_enum_variant,
    reason = "fixed observations stay inline without an allocation on failure"
)]
enum Retained {
    Capture(CaptureFailure),
    Measured(CapturedMeasurement),
    Evidence {
        intent: MeasurementEvidenceIntent,
        _grant: OwnedPermit,
        _binding: MonitorEvidenceBinding,
        recorded_at: bigbrotr_model::ObservedTime,
        request_sequence: RequestSequence,
    },
    NoIo(MonitorNoIoProof),
}

pub(super) struct Monitor {
    path: SelectionPath,
    connector: DirectConnector,
    armed: bool,
    status: Value,
    retained: Option<Retained>,
    // Covers selection decoding, exact intent wrapper, fixed reports and state.
    _fixed: OwnedPermit,
}
impl Monitor {
    pub(super) fn prepare(
        root: &Resolved,
        budget: &ResourceBudget,
        path: SelectionPath,
    ) -> Result<Self> {
        accept(root).map_err(|_| (3,"--monitor-once requires local enabled Monitor with checks/store_checks exactly [open,read], check_timeout=15s, retries=1, network.connect_timeout=10s, network.max_message_bytes=2MiB and pipeline.parse_arena_bytes=8MiB; additional checks or changed fixed limits are unsupported by this command".into()))?;
        let fixed = grant(budget, FIXED_BYTES, 0)?;
        let resolver = required(root, "network.dns.resolver")?
            .parse()
            .map_err(|_| failure("invalid DNS resolver"))?;
        let paths: Vec<_> = root
            .value("network.tls.extra_roots")
            .as_array()
            .expect("validated roots")
            .iter()
            .map(|path| Path::new(path.as_str().expect("validated path")))
            .collect();
        let tls = TlsPolicy::from_der_files(&paths, grant(budget, TlsPolicy::FILE_ROOTS_BYTES, 0)?)
            .map_err(error)?;
        // Reserve the complete finite peak before any source can be selected.
        // The actual run retains this same capacity before dispatch COMMIT/IO.
        drop(budget.try_reserve(requirements()?).map_err(error)?);
        Ok(Self {
            path,
            connector: DirectConnector::new(
                DnsResolver::new(resolver, budget.clone()).map_err(error)?,
                tls,
                budget.clone(),
            ),
            armed: true,
            status: json!({"format":"bigbrotr-monitor-once/1","state":"armed","profile":1,"ready":false}),
            retained: None,
            _fixed: fixed,
        })
    }
    pub(super) fn status(&self) -> Value {
        self.status.clone()
    }
    pub(super) fn armed(&self) -> bool {
        self.armed
    }

    pub(super) async fn poll(
        &mut self,
        commands: &mut commands::CommandOwner<'_>,
        native: &mut NativeOwner,
        runtime: RuntimeContext<'_>,
        budget: &ResourceBudget,
        management: &mut Option<management::Management>,
        due: &mut Instant,
    ) -> Result<()> {
        if !self.armed {
            return Ok(());
        }
        let selection = match self.path.poll() {
            Ok(None) => return Ok(()),
            Ok(Some(selected)) => selected,
            Err(refusal) => {
                self.armed = false;
                self.park("selection-refused");
                self.publish(commands.progress());
                return Err(refusal);
            }
        };
        self.armed = false;
        let result = self
            .run(
                selection, commands, native, runtime, budget, management, due,
            )
            .await;
        if result.is_err() && self.status["state"] != "parked" {
            self.park("operation-refused-or-unknown");
        }
        self.publish(commands.progress());
        result
    }

    fn park(&mut self, reason: &str) {
        self.status["state"] = json!("parked");
        self.status["reason"] = json!(reason);
        if let Some(Retained::Capture(failure)) = &self.retained {
            let retained = failure.observations();
            self.status["retained_capture"] = json!({"candidates":retained.read_prefix().event_candidates,
                "first_valid_event_observed":retained.read_prefix().first_valid_event.is_some(),
                "terminal_observed":retained.terminal().is_some(),"first_io_observed":retained.transport().first_io.is_some(),
                "reserved_bytes":retained.reserved_bytes(),"reserved_items":retained.reserved_items()});
        }
    }
    fn publish(&self, progress: &watch::Sender<Progress>) {
        progress.send_modify(|value| {
            if let Some(report) = value.initialized.as_mut() {
                report["monitor_once"] = self.status();
            }
        });
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "borrow the actual serial Native/Control owner and existing supervisor without a second executor"
    )]
    async fn run(
        &mut self,
        selection: Selection,
        commands: &mut commands::CommandOwner<'_>,
        native: &mut NativeOwner,
        runtime: RuntimeContext<'_>,
        budget: &ResourceBudget,
        management: &mut Option<management::Management>,
        due: &mut Instant,
    ) -> Result<()> {
        let registration = runtime.initial().map_err(owner_error)?;
        let root = commands.root;
        let maximum = commands.monitor_window(native, management, due).await?;
        let source = bounded(maximum, commands.cancellation, async {
            commands
                .owner
                .read_monitor_source(
                    selection.source,
                    grant(budget, StoredRelaySource::REQUIRED_BYTES, 0)?,
                )
                .await
                .map_err(error)
        })
        .await?;
        let maximum = commands.monitor_window(native, management, due).await?;
        // An outstanding attempt is an explicit recovery gate. Absence of an
        // evidence reservation never licenses a replacement probe.
        let pending = bounded(maximum, commands.cancellation, async {
            commands
                .owner
                .pending_monitor_dispatch(
                    selection.source.endpoint_id,
                    selection.source.access_scope_id,
                )
                .await
                .map_err(error)
        })
        .await?;
        if let Some(attempt) = pending {
            self.status["attempt_id"] = json!(attempt);
            self.park("pending-dispatch-needs-recovery");
            return Ok(());
        }
        let visible = visibility(native, selection.visibility)?;
        let limits = limits(u64::from(native.limit(runtime)?));
        let maximum = commands.monitor_window(native, management, due).await?;
        let visibility = bounded(maximum, commands.cancellation, async {
            native
                .visibility(
                    commands.owner,
                    runtime,
                    LiveSuccessorVisibilityRequest {
                        ingest_root: Path::new(required(root, "storage.ingest_root")?),
                        reference: visible,
                        limits: reads(),
                        policy_ttl: maximum,
                        cancelled: &commands.cancellation.requested,
                    },
                    grant(budget, reads().required_data_bytes().map_err(error)?, 0)?,
                    |d, h| authorize(root, d, h, source.endpoint(), source.route()).map(|_| ()),
                )
                .await
                .map_err(error)?
                .ok_or_else(|| failure("committed visibility unavailable"))
        })
        .await?;
        if !source_visibility_allowed(source.endpoint(), source.route(), visibility.entry()) {
            return Err(failure("source visibility refused"));
        }
        let (prepared, context, clock) = declarations(
            native,
            visible,
            &source,
            limits,
            grant(
                budget,
                limits
                    .required_dictionary_preparation_data_bytes()
                    .map_err(error)?,
                0,
            )?,
            &commands.cancellation.requested,
        )?;
        let checkpoint = ProgressGrant::new(grant(
            budget,
            ProgressGrant::REQUIRED_DATA_BYTES,
            ProgressGrant::REQUIRED_NATIVE_BYTES,
        )?)
        .map_err(error)?;
        let maximum = commands.monitor_window(native, management, due).await?;
        if !native.can_reserve(runtime, REQUEST_DICTIONARIES)? {
            return Err(failure("dictionary producer slot is not available"));
        }
        let dictionaries_recorded_at = recorded()?;
        bounded(maximum, commands.cancellation, async {
            native
                .append(
                    commands.owner,
                    runtime,
                    LiveSuccessorDictionaryAppendRequest {
                        ingest_root: Path::new(required(root, "storage.ingest_root")?),
                        request_sequence: RequestSequence::new(REQUEST_DICTIONARIES)
                            .map_err(error)?,
                        intent: prepared.intent(),
                        recorded_at: dictionaries_recorded_at,
                        limits,
                        policy_ttl: maximum,
                        cancelled: &commands.cancellation.requested,
                    },
                    grant(
                        budget,
                        limits.required_dictionary_data_bytes().map_err(error)?,
                        limits.required_native_bytes().map_err(error)?,
                    )?,
                    &checkpoint,
                    |d, h| authorize(root, d, h, source.endpoint(), source.route()).map(|_| ()),
                )
                .await
                .map_err(error)
        })
        .await?;
        drop(prepared);
        // Prepare the exact capture and ALL subsequent admission scratch before
        // creating the only runnable token. No grant is acquired during IO except
        // the existing driver's dedicated child allocations.
        let maximum = commands
            .monitor_window(native, management, due)
            .await?
            .min(Duration::from_secs(5));
        let authority = DurationUs::new(u64::try_from(maximum.as_micros()).map_err(error)?);
        let attempt: MeasurementAttemptId = (*super::super::fresh_process_incarnation()
            .map_err(error)?
            .as_bytes())
        .try_into()
        .map_err(error)?;
        let (capture, admission) = fund_attempt(
            budget,
            source.endpoint(),
            AttemptSpec {
                attempt_id: attempt,
                context,
                clock_quality: ClockQuality::new(clock, DurationUs::new(1), None, 0)
                    .map_err(error)?,
                effective_authority_us: authority,
            },
            limits,
        )?;
        let profile = PassiveReadProfile::first(authority).map_err(error)?;
        let intent =
            MonitorIntent::new(attempt, selection.source, context, clock, visible, profile)
                .map_err(error)?;
        self.status = json!({"format":"bigbrotr-monitor-once/1","state":"selected","profile":1,"ready":false,"attempt_id":attempt,
            "source":{"endpoint_id":selection.source.endpoint_id,"access_scope_id":selection.source.access_scope_id,
                "enrollment_generation":selection.source.enrollment_generation.to_string(),"route_id":selection.source.route_id},
            "context":{"endpoint_id":context.endpoint_id(),"observer_id":context.observer_id,"route_id":context.route_id,
                "access_scope_id":context.access_scope_id,"method_id":context.method_id,"clock_id":clock,"visibility_scope_id":visible},
            "effective_authority_us":authority.get().to_string()});
        self.publish(commands.progress());
        let deadline = commands.progress().borrow().deadline;
        let created = bounded(
            deadline.saturating_duration_since(Instant::now()),
            commands.cancellation,
            async {
                commands
                    .owner
                    .create_monitor_dispatch(intent, &visibility, deadline, |d, h, e, r| {
                        authorize(root, d, h, e, r)
                    })
                    .await
                    .map_err(error)
            },
        )
        .await?;
        let token = match created {
            MonitorCreation::Created(token) => token,
            MonitorCreation::Existing(_) => {
                self.park("existing-dispatch-is-history-only");
                return Ok(());
            }
        };
        let binding = token.evidence_binding();
        let connector = capture.connector(&self.connector);
        // The borrowed source future is awaited/dropped on this sole Native
        // owner stack. Its connector destroys the actual socket before return.
        let (result, proof) = if commands.cancellation.requested.load(Ordering::Acquire) {
            (None, Some(token.refuse()))
        } else {
            tokio::select! {
                biased;
                _=commands.cancellation.wait()=>(None,None),
                outcome=commands.owner.run_monitor_source(token,&visibility,&connector,capture.transport_trace(),
                    |d,h,e,r|authorize(root,d,h,e,r),async |stream|capture.read(stream).await)=> {
                    let (result,proof)=outcome.into_parts();(Some(result),proof)
                }
            }
        };
        drop(connector);
        let captured = match result {
            Some(Ok(())) => capture.finish(Ok(())),
            Some(Err(MonitorSourceError::Transport(error))) => capture.finish(Err(error)),
            _ => capture.finish_unclassified(),
        };
        drop(source);
        drop(visibility);
        // Finalization has closed the driver's sole reservation, releasing
        // unused wire/verifier/DNS capacity. Only its compact capture grant and
        // the already-held evidence permit survive; no ambient grant is needed.
        let captured = match captured {
            Err(failure) => {
                self.retained = Some(Retained::Capture(failure));
                self.park("capture-retained-without-invented-terminal");
                return Ok(());
            }
            Ok(value) => value,
        };
        let Some(captured) = captured else {
            if let Some(proof) = proof {
                self.retained = Some(Retained::NoIo(proof));
                let maximum = commands.monitor_window(native, management, due).await?;
                let Some(Retained::NoIo(proof)) = &self.retained else {
                    unreachable!()
                };
                bounded(maximum, commands.cancellation, async {
                    commands
                        .owner
                        .record_monitor_pre_io_refusal(proof)
                        .await
                        .map_err(error)
                })
                .await?;
                self.retained = None;
                self.status["state"] = json!("no-io");
                self.status["result"] = json!("operational-refusal-recorded");
            } else {
                self.park("no-positive-no-io-proof-needs-recovery");
            }
            return Ok(());
        };
        // Retain the original across the consuming checked wrapper constructor.
        // The capture grant funds both fixed measurement allocations at this
        // handoff, including a constructor/resource refusal.
        self.retained = Some(Retained::Measured(captured));
        let Some(Retained::Measured(captured)) = &self.retained else {
            unreachable!()
        };
        let evidence = MeasurementEvidenceIntent::new(
            1,
            visible,
            captured.measurement().clone(),
            limits.max_batch_bytes,
        )
        .map_err(error)?;
        let recorded_at = recorded()?;
        let request_sequence = RequestSequence::new(REQUEST_MEASUREMENT).map_err(error)?;
        let Some(Retained::Measured(captured)) = self.retained.take() else {
            unreachable!()
        };
        let (_, grant) = captured.into_parts();
        self.retained = Some(Retained::Evidence {
            intent: evidence,
            _grant: grant,
            _binding: binding,
            recorded_at,
            request_sequence,
        });
        // Source authority has ended. Historical admission gets fresh current
        // Control/Native authority; no probe or request is repeated on error.
        let maximum = commands.monitor_window(native, management, due).await?;
        if !native.can_reserve(runtime, REQUEST_MEASUREMENT)? {
            return Err(failure("measurement producer slot is not available"));
        }
        let Some(Retained::Evidence {
            intent,
            recorded_at,
            request_sequence,
            ..
        }) = &self.retained
        else {
            unreachable!()
        };
        let receipt = bounded(maximum, commands.cancellation, async {
            commands
                .owner
                .append_initial_measurement_with_progress(
                    native.initial_mut()?,
                    InitialMeasurementRequest {
                        registration,
                        evidence: LiveMeasurementRequest {
                            ingest_root: Path::new(required(root, "storage.ingest_root")?),
                            binding,
                            request_sequence: *request_sequence,
                            intent,
                            recorded_at: *recorded_at,
                            limits,
                            policy_ttl: maximum,
                            cancelled: &commands.cancellation.requested,
                        },
                    },
                    admission,
                    &checkpoint,
                    |d, h| validate(root, d, h),
                )
                .await
                .map_err(error)
        })
        .await?;
        let batch = receipt
            .receipt()
            .batch()
            .ok_or_else(|| failure("fresh measurement returned no emitted Fact"))?;
        let [reference] = batch.records() else {
            return Err(failure(
                "fresh measurement receipt must contain exactly one Fact",
            ));
        };
        let id = reference.source_record_id;
        self.status["state"] = json!("completed");
        self.status["result"] = json!("admitted");
        self.status["request_id"] = json!(receipt.receipt().request_id());
        self.status["request_digest"] = json!(receipt.receipt().request_digest());
        self.status["measurement"] = json!({"dataset_id":id.dataset_id,"shard_id":id.shard_id,"owner_epoch":id.owner_epoch,
            "sequence":id.sequence,"payload_digest":reference.payload_digest});
        self.retained = None;
        Ok(())
    }
}

fn visibility(native: &NativeOwner, selection: Visibility) -> Result<QualifiedRef> {
    let manifest = native.initial()?.manifest();
    Ok(QualifiedRef {
        dataset_id: manifest.identity().dataset_id,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: manifest
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .ok_or_else(|| failure("missing visibility namespace"))?,
        ordinal: bigbrotr_model::Ordinal::new(match selection {
            Visibility::Public => 1,
            Visibility::Private => 3,
        })
        .map_err(error)?,
    })
}
fn declarations(
    native: &NativeOwner,
    visibility: QualifiedRef,
    source: &StoredRelaySource,
    limits: AdmissionLimits,
    permit: OwnedPermit,
    cancelled: &AtomicBool,
) -> Result<(
    bigbrotr::storage::rocks::store::PreparedDictionaries,
    MeasurementContext,
    QualifiedRef,
)> {
    let mut prepared = native
        .prepare_dictionaries(visibility, limits, permit, cancelled)
        .map_err(error)?;
    let method = prepared
        .intern(DictionaryDescriptor::PassiveWsRead)
        .map_err(error)?;
    let endpoint = prepared
        .intern(DictionaryDescriptor::Endpoint(
            EndpointDescriptor::new(source.endpoint().as_str().into()).map_err(error)?,
        ))
        .map_err(error)?;
    let (class, route_class, label) = match source.route() {
        RouteSelection::Public {} => (
            DirectRouteClass::Public,
            RouteClass::Public,
            "direct-public",
        ),
        RouteSelection::Private { .. } => (
            DirectRouteClass::Private,
            RouteClass::Private,
            "direct-private",
        ),
    };
    let route = prepared
        .intern(DictionaryDescriptor::DirectRoute(DirectRouteDescriptor {
            class,
            label: DescriptorLabel::new(label.into()).map_err(error)?,
        }))
        .map_err(error)?;
    let access = prepared
        .intern(DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
            class: AccessClass::Public,
            label: None,
            visibility_scope_id: visibility,
        }))
        .map_err(error)?;
    let observer = prepared
        .intern(DictionaryDescriptor::Observer(observer_descriptor(
            method,
            None,
            Some(route_class),
        )))
        .map_err(error)?;
    let clock = prepared
        .intern(DictionaryDescriptor::Observer(observer_descriptor(
            method,
            Some(ClockSource::SystemUtc),
            None,
        )))
        .map_err(error)?;
    let prepared = prepared.finish().map_err(error)?;
    Ok((
        prepared,
        MeasurementContext {
            subject: MeasurementSubject::RelayEndpoint(endpoint),
            observer_id: observer,
            route_id: route,
            access_scope_id: access,
            method_id: method,
        },
        clock,
    ))
}
fn observer_descriptor(
    method: QualifiedRef,
    clock: Option<ClockSource>,
    route_class: Option<RouteClass>,
) -> ObserverDescriptor {
    ObserverDescriptor {
        clock,
        vantage: None,
        software: None,
        method_id: Some(method),
        public_key: None,
        route_class,
        software_version: None,
    }
}
fn recorded() -> Result<bigbrotr_model::ObservedTime> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(error)?
        .as_micros();
    Ok(bigbrotr_model::ObservedTime::new(
        i64::try_from(micros).map_err(error)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt_spec() -> AttemptSpec {
        let reference = |dictionary_kind, ordinal| QualifiedRef {
            dataset_id: [1; 16].try_into().unwrap(),
            dictionary_kind,
            allocation_domain: [2; 16].try_into().unwrap(),
            ordinal: bigbrotr_model::Ordinal::new(ordinal).unwrap(),
        };
        AttemptSpec {
            attempt_id: [3; 16].try_into().unwrap(),
            context: MeasurementContext {
                subject: MeasurementSubject::RelayEndpoint(reference(DictionaryKind::Endpoint, 1)),
                observer_id: reference(DictionaryKind::Observer, 1),
                route_id: reference(DictionaryKind::Route, 1),
                access_scope_id: reference(DictionaryKind::AccessScope, 1),
                method_id: reference(DictionaryKind::Method, 1),
            },
            clock_quality: ClockQuality::new(
                reference(DictionaryKind::Observer, 2),
                DurationUs::new(1),
                None,
                0,
            )
            .unwrap(),
            effective_authority_us: DurationUs::new(5_000_000),
        }
    }

    fn attempt_budget(data: u64, items: u64, native: u64) -> ResourceBudget {
        ResourceBudget::new(
            ProcessEnvelope::new(
                data + native,
                0,
                ResourceLimits::new(data, items, native, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        )
    }

    #[test]
    fn one_capture_reservation_and_full_evidence_permit_are_funded_before_dispatch() {
        let limits = limits(2);
        let evidence = limits.required_dictionary_data_bytes().unwrap();
        assert!(evidence > limits.required_data_bytes().unwrap());
        let native = limits.required_native_bytes().unwrap();
        let bytes = PreparedPassiveRead::REQUIRED_DATA_BYTES + evidence;
        let items = PreparedPassiveRead::REQUIRED_ITEMS + 1;
        let parent = attempt_budget(bytes, items, native);
        let endpoint = Endpoint::parse("wss://relay.example/").unwrap();
        // Exercise the exact production helper, including actual driver's
        // try_reserve, checked template, verifier and capture grant acquisition.
        let (capture, admission) =
            fund_attempt(&parent, &endpoint, attempt_spec(), limits).unwrap();
        assert!(capture.transport_trace().snapshot().first_io.is_none());
        assert_eq!(admission.request().units().data_bytes(), evidence);
        assert_eq!(admission.request().units().native_bytes(), native);
        assert_eq!(
            parent.snapshot().used(),
            ResourceLimits::new(bytes, items, native, 0)
        );
        assert_eq!(parent.snapshot().active_permits(), 2);
        // Finalization closes only the driver's scope. The separately funded
        // exact evidence permit remains usable without late ambient capacity.
        assert!(capture.finish_unclassified().unwrap().is_none());
        assert_eq!(
            parent.snapshot().used(),
            ResourceLimits::new(evidence, 1, native, 0)
        );
        assert_eq!(parent.snapshot().active_permits(), 1);
        drop(admission);
        assert_eq!(parent.snapshot().used(), ResourceLimits::default());
        assert_eq!(parent.snapshot().active_permits(), 0);
    }

    #[test]
    fn incomplete_combined_attempt_capacity_refuses_before_io_and_releases_grants() {
        let limits = limits(2);
        let evidence = limits.required_dictionary_data_bytes().unwrap();
        let native = limits.required_native_bytes().unwrap();
        let bytes = PreparedPassiveRead::REQUIRED_DATA_BYTES + evidence;
        let items = PreparedPassiveRead::REQUIRED_ITEMS + 1;
        let endpoint = Endpoint::parse("wss://relay.example/").unwrap();
        for (data, item_limit, native_limit) in [
            (bytes - 1, items, native),
            (bytes, items - 1, native),
            (bytes, items, native - 1),
            (
                PreparedPassiveRead::REQUIRED_DATA_BYTES + limits.required_data_bytes().unwrap(),
                items,
                native,
            ),
        ] {
            let parent = attempt_budget(data, item_limit, native_limit);
            assert!(fund_attempt(&parent, &endpoint, attempt_spec(), limits).is_err());
            assert_eq!(parent.snapshot().used(), ResourceLimits::default());
            assert_eq!(parent.snapshot().active_permits(), 0);
        }
        // Also unwind a checked profile refusal after the complete permit and
        // driver reservation have actually been admitted, not merely calculated.
        let parent = attempt_budget(bytes, items, native);
        let mut invalid = attempt_spec();
        invalid.effective_authority_us = DurationUs::new(0);
        assert!(fund_attempt(&parent, &endpoint, invalid, limits).is_err());
        assert_eq!(parent.snapshot().used(), ResourceLimits::default());
        assert_eq!(parent.snapshot().active_permits(), 0);
    }

    #[test]
    fn checkpoint_capacity_is_retained_beside_the_capture_and_admission_grants() {
        let limits = limits(2);
        let evidence = limits.required_dictionary_data_bytes().unwrap();
        let native = limits.required_native_bytes().unwrap();
        let bytes = PreparedPassiveRead::REQUIRED_DATA_BYTES
            + evidence
            + ProgressGrant::REQUIRED_DATA_BYTES;
        let items = PreparedPassiveRead::REQUIRED_ITEMS + 2;
        let endpoint = Endpoint::parse("wss://relay.example/").unwrap();
        for short in [false, true] {
            let parent = attempt_budget(
                bytes - u64::from(short),
                items,
                native + ProgressGrant::REQUIRED_NATIVE_BYTES,
            );
            let checkpoint = ProgressGrant::new(
                grant(
                    &parent,
                    ProgressGrant::REQUIRED_DATA_BYTES,
                    ProgressGrant::REQUIRED_NATIVE_BYTES,
                )
                .unwrap(),
            )
            .unwrap();
            let attempt = fund_attempt(&parent, &endpoint, attempt_spec(), limits);
            if short {
                assert!(attempt.is_err());
            } else {
                let (capture, admission) = attempt.unwrap();
                assert_eq!(
                    parent.snapshot().used(),
                    ResourceLimits::new(
                        bytes,
                        items,
                        native + ProgressGrant::REQUIRED_NATIVE_BYTES,
                        0
                    )
                );
                assert!(capture.transport_trace().snapshot().first_io.is_none());
                assert!(capture.finish_unclassified().unwrap().is_none());
                drop(admission);
            }
            assert_eq!(
                parent.snapshot().used(),
                ResourceLimits::new(
                    ProgressGrant::REQUIRED_DATA_BYTES,
                    1,
                    ProgressGrant::REQUIRED_NATIVE_BYTES,
                    0
                )
            );
            drop(checkpoint);
            assert_eq!(parent.snapshot().used(), ResourceLimits::default());
        }
    }

    #[test]
    fn once_profile_refuses_the_unimplemented_default_checks_and_parameter_changes() {
        let mut root = crate::config::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/recipes/archive-only.toml"),
        )
        .unwrap();
        assert!(accept(&root).is_err());
        root.values.insert("monitor.enabled".into(), json!(true));
        root.values
            .insert("monitor.checks".into(), json!(["open", "read"]));
        root.values
            .insert("monitor.store_checks".into(), json!(["open", "read"]));
        // Placement is validated by Resolved::local, independently of role names.
        root.local_roles.insert("monitor".into());
        let original = root.clone();
        assert!(accept(&root).is_ok());
        for (key, value) in [
            ("monitor.checks", json!(["open", "read", "dns"])),
            ("monitor.store_checks", json!(["read"])),
            ("monitor.check_timeout", json!(14_999_999)),
            ("monitor.retries", json!(2)),
            ("network.connect_timeout", json!(1_000_000)),
            ("network.max_message_bytes", json!(1024 * 1024)),
            ("pipeline.parse_arena_bytes", json!(4 * 1024 * 1024)),
        ] {
            let mut changed = original.clone();
            changed.values.insert(key.into(), value);
            assert!(accept(&changed).is_err(), "{key}");
        }
    }
    #[test]
    fn passive_observer_and_clock_are_distinct_and_both_name_the_passive_method() {
        let method = QualifiedRef {
            dataset_id: [1; 16].try_into().unwrap(),
            dictionary_kind: DictionaryKind::Method,
            allocation_domain: [2; 16].try_into().unwrap(),
            ordinal: bigbrotr_model::Ordinal::new(1).unwrap(),
        };
        let observer = observer_descriptor(method, None, Some(RouteClass::Private));
        let clock = super::observer_descriptor(method, Some(ClockSource::SystemUtc), None);
        assert_ne!(observer, clock);
        assert_eq!(observer.method_id, Some(method));
        assert_eq!(clock.method_id, Some(method));
        assert_eq!(observer.clock, None);
        assert_eq!(clock.clock, Some(ClockSource::SystemUtc));
    }
}
