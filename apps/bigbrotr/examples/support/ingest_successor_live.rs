//! Finite writer consumer. The declared import inputs are an exact laboratory
//! artifact, not an installed import job or online acquisition authority.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        LiveSuccessorCertificationRequest, LiveSuccessorDictionaryAppendRequest,
        LiveSuccessorEventRequest, LiveSuccessorVisibilityRequest, LiveSuccessorWriter,
        SuccessorActivation,
    },
    rocks::store::{
        AdmissionOutcome, CertificationLimits, CertificationOutcome, PreparedCertification,
        PreparedEvent,
    },
};
use bigbrotr_model::{
    DatasetLineageDescriptor, EventIntent, EventOrigin, ProducerEpoch, RequestSequence,
    SignedEvent,
    codec::request::{MutationIntent, decode_request},
};
use sha2::{Digest, Sha256};
use std::io::Read;

#[derive(Default)]
pub(super) struct LiveCustody {
    activation: Option<SuccessorActivation>,
    writer: Option<LiveSuccessorWriter>,
}
impl LiveCustody {
    pub(super) fn activated(&self) -> bool {
        self.writer.is_some()
    }
}

const MAX_INPUT: usize = 8192;
const MAX_EVENT: usize = 256 * 1024;
const WINDOW: u64 = 32;

fn admission_limits() -> AdmissionLimits {
    AdmissionLimits {
        max_outstanding_requests: WINDOW,
        max_dependencies: 256,
        ..AdmissionLimits::default()
    }
}
fn read_limits() -> CertificationLimits {
    CertificationLimits {
        max_fact_bytes: MAX_INPUT,
        max_dependencies: 256,
    }
}
pub(super) fn preflight(budget: &ResourceBudget) {
    // The prepared event and dispatch/result grants overlap the retained
    // lineage, native, transport, observation and preparation grants before claim.
    let _input = grant(budget, MIB, 0);
    let limits = admission_limits();
    let data = limits
        .required_data_bytes()
        .unwrap()
        .max(limits.required_dictionary_data_bytes().unwrap())
        .max(read_limits().required_data_bytes().unwrap());
    let _operation = grant(budget, data, limits.required_native_bytes().unwrap());
    emit(
        serde_json::json!({"event":"lineage-live-resources","prepared_input_bytes":MIB,
        "operation_data_bytes":data,"operation_native_bytes":limits.required_native_bytes().unwrap(),
        "max_input_bytes":MAX_INPUT,"max_event_bytes":MAX_EVENT,"max_dependencies":256,
        "outstanding_request_limit":WINDOW,"provenance":false}),
    );
}

struct Fixture {
    scope: QualifiedRef,
    lineage: QualifiedRef,
    old: SignedEvent,
    fresh: SignedEvent,
    dictionary: EvidenceIntent,
}
impl Fixture {
    fn load(probe: &Probe, writer: &LiveSuccessorWriter) -> Self {
        let name = format!(
            "{}-0000000000000001-0000000000000004.cbor",
            writer.producer_slot_id()
        );
        let mut bytes = Vec::new();
        std::fs::File::open(probe.path.join("producer-inputs-v1").join(&name))
            .unwrap()
            .take((MAX_INPUT + 1) as u64)
            .read_to_end(&mut bytes)
            .unwrap();
        assert!(bytes.len() <= MAX_INPUT);
        let digest = Digest256::from_bytes(Sha256::digest(&bytes).into());
        assert_eq!(
            digest.to_string(),
            "19820e2cea792c8780de0d8542baa76dabbdb611ab96237eed2230a228d82977"
        );
        let (original, input) = decode_request(
            &bytes,
            digest,
            RequestDecodeLimits {
                values: FactDecodeLimits {
                    max_encoded_bytes: MAX_INPUT,
                    max_allocation_bytes: 32 * 1024,
                    max_tag_nodes: 32,
                    max_string_bytes: MAX_INPUT,
                },
                max_dictionaries: 4,
            },
        )
        .unwrap();
        assert_eq!(original.producer_slot_id, writer.producer_slot_id());
        assert_eq!(
            (
                original.producer_epoch.get(),
                original.request_sequence.get()
            ),
            (1, 4)
        );
        let MutationIntent::Event(input) = input else {
            panic!("frozen original Event input")
        };
        let old: SignedEvent = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ingest/import-event.jsonl"
        ))
        .unwrap();
        assert_eq!(input.event(), &old);
        assert_eq!(input.origin(), EventOrigin::Network);
        let scope = input.visibility_scope_id();
        assert_eq!(scope.dataset_id, writer.manifest().identity().dataset_id);
        assert_eq!(scope.dictionary_kind, DictionaryKind::VisibilityScope);
        assert_eq!(scope.ordinal.get(), 3);
        let signed: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../../../experiments/identity-contract/signed-fixture.json"
        ))
        .unwrap();
        let fresh: SignedEvent =
            serde_json::from_value(signed["events"]["B_note"].clone()).unwrap();
        assert_ne!(fresh.id, old.id);
        // Retain the exact finite artifact in the output. Its descriptor names
        // both signed inputs; it never falsely attributes B_note to the old file.
        let artifact = serde_json::to_vec(&[&old, &fresh]).unwrap();
        assert!(artifact.len() <= 2 * MAX_INPUT);
        let artifact_sha256 = Digest256::from_bytes(Sha256::digest(&artifact).into());
        let lineage = QualifiedRef {
            dictionary_kind: DictionaryKind::DatasetLineage,
            allocation_domain: writer
                .manifest()
                .allocation_namespaces()
                .domain(DictionaryKind::DatasetLineage)
                .unwrap(),
            ordinal: Ordinal::new(1).unwrap(),
            ..scope
        };
        let dictionary = EvidenceIntent::new(
            1,
            scope,
            vec![
                DictionaryEntry::new(
                    lineage,
                    DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                        format: DescriptorLabel::new("bigbrotr-finite-successor-events".into())
                            .unwrap(),
                        format_version: 1,
                        source_dataset_id: None,
                        source_artifact_digest: artifact_sha256,
                    }),
                )
                .unwrap(),
            ],
            EvidenceIntentLimits {
                max_entries: 1,
                max_encoded_intent_bytes: MAX_INPUT,
                max_validation_scratch_bytes: 4096,
            },
        )
        .unwrap();
        emit(
            serde_json::json!({"event":"lineage-live-fixture","original_name":name,
            "original_bytes":hex(&bytes),"original_sha256":digest.to_string(),"scope":scope,"lineage":lineage,
            "artifact_bytes":hex(&artifact),"artifact_sha256":artifact_sha256.to_string(),
            "old_event":old,"new_event":fresh,"provenance":false,"installed_import":false}),
        );
        Self {
            scope,
            lineage,
            old,
            fresh,
            dictionary,
        }
    }
    fn intent(&self, event: &SignedEvent) -> EventIntent {
        EventIntent::import(1, self.scope, event.clone(), self.lineage).unwrap()
    }
}

impl Probe {
    pub(super) async fn live(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        mutation: OwnedPermit,
        new_event: bool,
    ) -> Result<serde_json::Value, Failure> {
        let target = self
            .activate_live(owner, native, metadata, mutation)
            .await?;
        let writer = native.live.writer.as_mut().unwrap();
        let fixture = Fixture::load(self, writer);
        self.dictionary(owner, writer, &fixture).await?;
        self.event_pair(owner, writer, &fixture, &fixture.old, 2, "already-durable")
            .await?;
        if new_event {
            self.event_pair(owner, writer, &fixture, &fixture.fresh, 3, "new-event")
                .await?;
        }
        self.visibility(owner, writer, fixture.scope).await?;
        self.certification(owner, writer, &fixture, &fixture.old, "historical")
            .await?;
        if new_event {
            self.certification(owner, writer, &fixture, &fixture.fresh, "current")
                .await?;
        }
        Ok(
            serde_json::json!({"target_epoch":target,"requests":if new_event {3}else{2},
            "writer_delivered":true,"new_event":new_event,"read_only_checks":if new_event {3}else{2}}),
        )
    }
    // Shared only inside the finite consumer: candidate/preparation/activation
    // remain in Native custody before every observer await. Existing live modes
    // keep the exact calls, event order and refresh/renew sequence.
    pub(super) async fn activate_live(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        mutation: OwnedPermit,
    ) -> Result<u64, Failure> {
        self.transition(owner, native, metadata, Some(mutation), "prepare", false)
            .await?;
        native.live.activation = Some(SuccessorActivation::new(native.preparation.take().unwrap()));
        let activation = native.live.activation.as_mut().unwrap();
        let target = activation
            .record()
            .fields()
            .next_manifest
            .identity()
            .owner_epoch
            .get();
        emit(
            serde_json::json!({"event":"lineage-live-activation-start","target_epoch":target,
            "control_epoch":owner.identity().epoch.get()}),
        );
        let result = self
            .bounded(
                "activate",
                owner.activate_successor_writer(activation, self.request(), |d, h| {
                    self.validate(d, h)
                }),
            )
            .await;
        // Install successful native custody before any observer await or assertion.
        match result {
            Ok(writer) => native.live.writer = Some(writer),
            Err(error) => {
                emit(
                    serde_json::json!({"event":"lineage-live-activation-result","writer":false,
                    "target_epoch":target,"error":format!("{error:?}")}),
                );
                self.snapshot("activation-returned").await;
                return Err(Failure::at("activate", Some(target), None, error));
            }
        }
        let writer = native.live.writer.as_mut().unwrap();
        emit(
            serde_json::json!({"event":"lineage-live-activation-result","writer":true,
            "target_epoch":target,"activated_at_us":writer.activated_at_us().to_string(),
            "manifest":hex(writer.manifest().encode().as_bytes()),"role":hex(&writer.role_instance_id()),
            "producer_slot":hex(writer.producer_slot_id().as_bytes()),"error":null}),
        );
        self.snapshot("activated-zero").await;
        assert_eq!(writer.manifest().identity().owner_epoch.get(), target);
        assert_eq!(u64::from(writer.outstanding_request_limit()), WINDOW);
        let policy = self
            .bounded("live-refresh", owner.refresh(|d, h| self.validate(d, h)))
            .await
            .map_err(|e| Failure::at("live-refresh", Some(target), None, e))?;
        self.policy(owner, "live-refreshed", Some(target), None, policy);
        self.snapshot("live-control-refreshed").await;
        let started = Cell::new(false);
        let result = self
            .bounded(
                "live-renew",
                owner.renew_live_successor_ingest(
                    writer,
                    self.request().policy_ttl,
                    |d, h| self.validate(d, h),
                    &started,
                ),
            )
            .await;
        emit(
            serde_json::json!({"event":"lineage-live-renewal","commit_started":started.get(),
            "error":result.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        self.snapshot("live-renew-returned").await;
        result.map_err(|e| Failure::at("live-renew", Some(target), None, e))?;
        assert!(started.get());
        Ok(target)
    }
    async fn dictionary(
        &self,
        owner: &mut ControlSession,
        writer: &mut LiveSuccessorWriter,
        fixture: &Fixture,
    ) -> Result<(), Failure> {
        let request = request(writer, 1);
        let limits = admission_limits();
        let preimage = fixture
            .dictionary
            .encode_request_preimage(request, MAX_INPUT)
            .unwrap();
        let digest = fixture.dictionary.request_digest(request);
        let mut original = None;
        for (stage, time) in [
            ("dictionary", 900_000_001),
            ("dictionary-retry", 900_000_002),
        ] {
            emit_input(stage, request, &preimage, digest, time, None);
            self.snapshot(&format!("before-{stage}")).await;
            let result = self
                .bounded(
                    stage,
                    owner.append_live_successor_dictionaries(
                        writer,
                        LiveSuccessorDictionaryAppendRequest {
                            ingest_root: &self.path,
                            request_sequence: request.request_sequence,
                            intent: &fixture.dictionary,
                            recorded_at: ObservedTime::new(time),
                            limits,
                            policy_ttl: self.request().policy_ttl,
                            cancelled: &self.cancelled,
                        },
                        grant(
                            &self.budget,
                            limits.required_dictionary_data_bytes().unwrap(),
                            limits.required_native_bytes().unwrap(),
                        ),
                        |d, h| self.validate(d, h),
                    ),
                )
                .await;
            self.snapshot(&format!("after-{stage}")).await;
            let receipt = result.map_err(|e| {
                Failure::at("dictionary", Some(request.producer_epoch.get()), Some(1), e)
            })?;
            let encoded = receipt.receipt().encode_persistent(65536).unwrap();
            emit_receipt(stage, request, &encoded);
            let batch = receipt.receipt().batch().expect("new target dictionary");
            assert_eq!(
                (
                    batch.first().owner_epoch.get(),
                    batch.first().sequence.get(),
                    batch.records().len()
                ),
                (request.producer_epoch.get(), 1, 1)
            );
            if let Some(previous) = &original {
                assert_eq!(previous, &encoded);
            } else {
                original = Some(encoded);
            }
        }
        Ok(())
    }
    async fn event_pair(
        &self,
        owner: &mut ControlSession,
        writer: &mut LiveSuccessorWriter,
        fixture: &Fixture,
        event: &SignedEvent,
        sequence: u64,
        kind: &str,
    ) -> Result<(), Failure> {
        let request = request(writer, sequence);
        let limits = admission_limits();
        let mut original = None;
        for retry in [false, true] {
            let stage = if retry {
                format!("{kind}-retry")
            } else {
                kind.into()
            };
            let time = 900_000_003 + i64::try_from(sequence - 2).unwrap() * 2 + i64::from(retry);
            let intent = fixture.intent(event);
            let preimage = intent.encode_request_preimage(request, MAX_INPUT).unwrap();
            emit_input(
                &stage,
                request,
                &preimage,
                intent.request_digest(request),
                time,
                Some(event),
            );
            let prepared =
                PreparedEvent::new(request, intent, MAX_EVENT, grant(&self.budget, MIB, 0))
                    .map_err(|e| {
                        Failure::at(
                            "event-input",
                            Some(request.producer_epoch.get()),
                            Some(sequence),
                            e,
                        )
                    })?;
            self.snapshot(&format!("before-{stage}")).await;
            let result = self
                .bounded(
                    &stage,
                    owner.admit_live_successor_event(
                        writer,
                        LiveSuccessorEventRequest {
                            ingest_root: &self.path,
                            event: prepared,
                            relay_source: None,
                            acquisition: None,
                            provenance_enabled: false,
                            recorded_at: ObservedTime::new(time),
                            limits,
                            policy_ttl: self.request().policy_ttl,
                            cancelled: &self.cancelled,
                        },
                        grant(
                            &self.budget,
                            limits.required_data_bytes().unwrap(),
                            limits.required_native_bytes().unwrap(),
                        ),
                        |d, h, candidate| {
                            self.validate(d, h)?;
                            if candidate.request_id() != request
                                || candidate.verification_limit_bytes() != MAX_EVENT
                                || candidate.visibility_scope_id() != fixture.scope
                                || candidate.origin() != EventOrigin::Import(fixture.lineage)
                                || candidate.observation().is_some()
                                || candidate
                                    .verified_event()
                                    .is_none_or(|v| v.event() != event)
                            {
                                return Err(OwnerError::InvalidPolicy);
                            }
                            Ok(())
                        },
                    ),
                )
                .await;
            self.snapshot(&format!("after-{stage}")).await;
            let outcome = result.map_err(|e| {
                Failure::at(
                    "event",
                    Some(request.producer_epoch.get()),
                    Some(sequence),
                    e,
                )
            })?;
            let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
                panic!("frozen signed event durable")
            };
            let encoded = receipt.encode_persistent(65536).unwrap();
            emit_receipt(&stage, request, &encoded);
            if sequence == 2 {
                assert!(receipt.batch().is_none());
            } else {
                let batch = receipt.batch().expect("new signed B_note facts");
                assert_eq!(
                    (
                        batch.first().owner_epoch.get(),
                        batch.first().sequence.get()
                    ),
                    (request.producer_epoch.get(), 2)
                );
            }
            assert!(
                receipt.dependencies().records().iter().any(|fact| fact
                    .source_record_id
                    .owner_epoch
                    .get()
                    == 1)
            );
            if let Some(previous) = &original {
                assert_eq!(previous, &encoded);
            } else {
                original = Some(encoded);
            }
        }
        Ok(())
    }
    async fn visibility(
        &self,
        owner: &mut ControlSession,
        writer: &mut LiveSuccessorWriter,
        reference: QualifiedRef,
    ) -> Result<(), Failure> {
        self.snapshot("before-visibility").await;
        let limits = read_limits();
        let result = self
            .bounded(
                "visibility",
                owner.read_live_successor_visibility(
                    writer,
                    LiveSuccessorVisibilityRequest {
                        ingest_root: &self.path,
                        reference,
                        limits,
                        policy_ttl: self.request().policy_ttl,
                        cancelled: &self.cancelled,
                    },
                    grant(&self.budget, limits.required_data_bytes().unwrap(), 0),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("after-visibility").await;
        let visible = result
            .map_err(|e| Failure::at("visibility", None, None, e))?
            .expect("retained private scope");
        let entry = visible.entry();
        let bytes = entry.encode_preimage(MAX_INPUT).unwrap();
        let DictionaryDescriptor::Visibility { class } = entry.descriptor() else {
            panic!("visibility descriptor")
        };
        emit(
            serde_json::json!({"event":"lineage-live-visibility","reference":entry.reference(),
            "class":class.as_str(),"bytes":hex(&bytes),"sha256":hex(&Sha256::digest(&bytes))}),
        );
        assert_eq!(visible.entry().reference(), reference);
        Ok(())
    }
    async fn certification(
        &self,
        owner: &mut ControlSession,
        writer: &mut LiveSuccessorWriter,
        fixture: &Fixture,
        event: &SignedEvent,
        kind: &str,
    ) -> Result<(), Failure> {
        let intent = fixture.intent(event);
        let expected_digest = intent.certification_digest();
        let input = PreparedCertification::new(intent, MAX_EVENT, grant(&self.budget, MIB, 0))
            .map_err(|e| Failure::at("certification-input", None, None, e))?;
        let stage = format!("certify-{kind}");
        self.snapshot(&format!("before-{stage}")).await;
        let limits = read_limits();
        let result = self
            .bounded(
                &stage,
                owner.certify_live_successor_event(
                    writer,
                    LiveSuccessorCertificationRequest {
                        ingest_root: &self.path,
                        event: input,
                        relay_source: None,
                        acquisition: None,
                        provenance_enabled: false,
                        limits,
                        policy_ttl: self.request().policy_ttl,
                        cancelled: &self.cancelled,
                    },
                    grant(&self.budget, limits.required_data_bytes().unwrap(), 0),
                    |d, h, candidate| {
                        self.validate(d, h)?;
                        if candidate.intent_digest() != expected_digest
                            || candidate.verification_limit_bytes() != MAX_EVENT
                            || candidate.visibility_scope_id() != fixture.scope
                            || candidate.origin() != EventOrigin::Import(fixture.lineage)
                            || candidate.observation().is_some()
                            || candidate
                                .verified_event()
                                .is_none_or(|v| v.event() != event)
                        {
                            return Err(OwnerError::InvalidPolicy);
                        }
                        Ok(())
                    },
                ),
            )
            .await;
        self.snapshot(&format!("after-{stage}")).await;
        let result = result.map_err(|e| Failure::at("certification", None, None, e))?;
        let CertificationOutcome::Existing(proof) = result.outcome() else {
            panic!("existing signed effects")
        };
        emit(
            serde_json::json!({"event":"lineage-live-certification","kind":kind,"event_id":event.id,
            "context":proof.protection_context(),"intent_digest":proof.intent_digest(),
            "dependencies":proof.dependencies().records(),"proof_digest":proof.digest()}),
        );
        assert_eq!(proof.intent_digest(), expected_digest);
        assert!(
            proof.dependencies().records().iter().any(|fact| fact
                .source_record_id
                .owner_epoch
                .get()
                == 1)
        );
        Ok(())
    }
}

fn request(writer: &LiveSuccessorWriter, sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: writer.producer_slot_id(),
        producer_epoch: ProducerEpoch::new(writer.manifest().identity().owner_epoch.get()).unwrap(),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}
fn emit_input(
    stage: &str,
    request: ProducerRequestId,
    preimage: &[u8],
    digest: Digest256,
    time: i64,
    event: Option<&SignedEvent>,
) {
    emit(
        serde_json::json!({"event":"lineage-live-input","stage":stage,"request":request,
        "preimage":hex(preimage),"request_digest":digest,"recorded_at_us":time.to_string(),
        "signed_event":event,"provenance":false}),
    );
}
fn emit_receipt(stage: &str, request: ProducerRequestId, bytes: &[u8]) {
    emit(
        serde_json::json!({"event":"lineage-live-receipt","stage":stage,"request":request,
        "receipt":hex(bytes),"sha256":hex(&Sha256::digest(bytes))}),
    );
}
