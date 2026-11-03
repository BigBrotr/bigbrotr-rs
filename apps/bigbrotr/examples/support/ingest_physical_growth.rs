//! Bounded physical-growth diagnostic under the existing absolute64 ceiling.
//! This does not qualify a reserve coefficient, OPEN65, acquisition or reclaim.
use super::*;
use bigbrotr::{
    acquisition::import::{
        JsonlReader, RecordPosition, ScratchSpace, SourceIdentity, SourceSnapshot,
    },
    storage::{
        postgres::control_owner::{InitialEventRequest, InitialPendingQuotaRequest, ProgressGrant},
        rocks::store::{AdmissionLimits, AdmissionOutcome, NativeOutcomeKind, PreparedEvent},
    },
};
use bigbrotr_model::{EventIntent, ProducerEpoch, ProducerRequestId, RequestSequence, SignedEvent};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, io::Read, mem::size_of, os::fd::AsFd, time::Instant};

const MODE: &str = "physical-growth-diagnostic";
const EVENT_BYTES: usize = 256 * 1024;
const INPUT_DATA_BYTES: u64 = 24 * MIB;
const MAX_EVENTS: usize = 63;
pub const LEASE_SECONDS: u64 = 180;

pub fn limits() -> RocksNativeLimits {
    // Match the installed initial owner's complete native profile, while the
    // older finite probe modes retain their existing32-file setting.
    limits_with_open_files(64)
}

pub struct Fixture {
    pub identity: SourceIdentity,
    pub budget: ResourceBudget,
    records: Vec<(SignedEvent, RecordPosition)>,
    retained_input_upper_bound: u64,
    _inputs: OwnedPermit,
}

fn request(sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: [66; 16].try_into().unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

/// Closed mode/arity, source identity, signed size, count, distinctness and
/// current finite policy/spool funding are checked before any engine effects.
pub fn preflight(args: &[String]) -> Fixture {
    assert_eq!(
        args.len(),
        9,
        "physical diagnostic requires exactly eight arguments"
    );
    assert_eq!(args[1], MODE, "unknown physical diagnostic mode");
    let canonical_decimal = |text: &str| {
        let value: u64 = text.parse().expect("unsigned decimal diagnostic input");
        assert_eq!(
            value.to_string(),
            text,
            "canonical decimal diagnostic input"
        );
        value
    };
    let count = usize::try_from(canonical_decimal(&args[8])).unwrap();
    assert!(
        (1..=MAX_EVENTS).contains(&count),
        "one bootstrap plus at most63 events"
    );
    let bytes = canonical_decimal(&args[7]);
    assert_eq!(bytes, count as u64 * (EVENT_BYTES as u64 + 1));
    assert!(
        args[6].len() == 64
            && args[6]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "lowercase source SHA-256"
    );
    let digest: Digest256 = args[6].parse().expect("source SHA-256");
    let identity = SourceIdentity {
        bytes,
        sha256: *digest.as_bytes(),
    };
    let path = Path::new(&args[4]);
    assert!(path.is_absolute() && Path::new(&args[5]).is_absolute());
    assert!(
        matches!(std::fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound),
        "fresh isolated native root required"
    );
    let parent = path.parent().expect("native root parent");
    assert!(parent.is_dir());
    let document: serde_json::Value = serde_json::from_str(&fixture_document()).unwrap();
    let policy = fixture_intake_policy();
    assert_eq!(
        document["settings"]["ingest.producer_outstanding_requests"],
        64
    );
    assert_eq!(document["settings"]["ingest.max_event_bytes"], EVENT_BYTES);
    assert_eq!(document["settings"]["import.max_event_bytes"], EVENT_BYTES);
    assert_eq!(policy.request_receipt_retention(), 64);
    assert!(policy.max_pending_unit() >= AdmissionLimits::default().max_batch_bytes as u64);
    assert!(policy.pending_high() >= 64 * policy.max_pending_unit());

    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            241 * MIB,
            0,
            ResourceLimits::new(96 * MIB, 8, 144 * MIB, MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let inputs = grant(&budget, INPUT_DATA_BYTES, 0);
    let scratch_root = tempfile::tempdir_in(parent).unwrap();
    let scratch = ScratchSpace::new(scratch_root.path(), bytes, 1).unwrap();
    let cancelled = AtomicBool::new(false);
    let snapshot =
        SourceSnapshot::capture(Path::new(&args[5]), &scratch, Some(identity), &cancelled).unwrap();
    let mut reader = JsonlReader::new(
        snapshot,
        bigbrotr_protocol::EventLimits::default(),
        grant(&budget, 11 * MIB, 0),
    )
    .unwrap();
    let mut records = Vec::with_capacity(count);
    let mut ids = BTreeSet::new();
    let mut allocation_bytes = 64 * 1024usize;
    // Bootstrap's existing max_encoded_intent_bytes is8192; the fixed request
    // framing fits the remaining8192. Each event's actual canonical request is
    // measured below. These are logical original bytes, never physical d/R.
    let mut retained_input_upper_bound = 16 * 1024u64;
    let scope = QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: [64; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    let lineage = QualifiedRef {
        dictionary_kind: DictionaryKind::DatasetLineage,
        allocation_domain: [69; 16].try_into().unwrap(),
        ..scope
    };
    while let Some(record) = reader.next_record(&cancelled).unwrap() {
        assert!(records.len() < count, "extra source record");
        let verified = record
            .result()
            .as_ref()
            .expect("every diagnostic event must verify");
        assert_eq!(
            verified.normalized_bytes(),
            EVENT_BYTES,
            "complete signed event size"
        );
        let position = record.position();
        assert_eq!(position.line, records.len() as u64 + 1);
        assert_eq!(
            position.byte_end - position.byte_start,
            EVENT_BYTES as u64 + 1
        );
        let event = verified.event();
        assert!(ids.insert(event.id), "distinct event identities required");
        let event_allocations = event.content.len()
            + event.tags.len() * size_of::<Vec<String>>()
            + event
                .tags
                .iter()
                .map(|tag| {
                    tag.len() * size_of::<String>() + tag.iter().map(String::len).sum::<usize>()
                })
                .sum::<usize>()
            + size_of::<SignedEvent>()
            + size_of::<RecordPosition>();
        allocation_bytes = allocation_bytes.checked_add(event_allocations).unwrap();
        assert!(
            allocation_bytes as u64 <= INPUT_DATA_BYTES,
            "retained input grant"
        );
        let _preimage_scratch = grant(&budget, MIB, 0);
        let intent = EventIntent::import(1, scope, event.clone(), lineage).unwrap();
        let preimage = intent
            .encode_request_preimage(request(records.len() as u64 + 2), MIB as usize)
            .unwrap();
        assert!(
            event_allocations + preimage.capacity() + size_of::<PreparedEvent>() <= MIB as usize,
            "each prepared event fits its existing1MiB grant"
        );
        retained_input_upper_bound += preimage.len() as u64;
        records.push((event.clone(), position));
    }
    assert_eq!(records.len(), count, "complete pinned source count");
    assert!(
        retained_input_upper_bound <= 32 * MIB,
        "existing original-input aggregate guard"
    );
    drop(reader);
    assert_eq!(scratch.used_bytes(), 0);
    emit(
        serde_json::json!({"event":"physical-input-validated","source_bytes":bytes,"source_sha256":hex(&identity.sha256),"events":count,"signed_event_bytes":EVENT_BYTES,"retained_input_upper_bound":retained_input_upper_bound,"absolute_request_limit":64,"pending_high":policy.pending_high(),"max_pending_unit":policy.max_pending_unit(),"outstanding_request_limit":64,"event_acknowledgement":"NOT_RUN-unbound-import"}),
    );
    Fixture {
        identity,
        budget,
        records,
        retained_input_upper_bound,
        _inputs: inputs,
    }
}

struct Gates {
    deadline: Instant,
    original_flags: rustix::fs::OFlags,
}
impl Gates {
    fn new() -> Self {
        let input = std::io::stdin();
        let original_flags = rustix::fs::fcntl_getfl(input.as_fd()).unwrap();
        rustix::fs::fcntl_setfl(input.as_fd(), original_flags | rustix::fs::OFlags::NONBLOCK)
            .unwrap();
        Self {
            deadline: Instant::now() + Duration::from_secs(120),
            original_flags,
        }
    }
    async fn wait(&self, command: &[u8]) {
        let deadline = self.deadline.min(Instant::now() + Duration::from_secs(45));
        for expected in command {
            loop {
                assert!(
                    Instant::now() < deadline,
                    "physical diagnostic gate deadline expired"
                );
                let mut byte = [0];
                match std::io::stdin().read(&mut byte) {
                    Ok(1) => {
                        assert_eq!(byte[0], *expected, "unexpected physical gate command");
                        break;
                    }
                    Ok(_) => panic!("physical diagnostic controller closed before completion"),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => panic!(
                        "physical diagnostic controller read failed: {}",
                        error.kind()
                    ),
                }
            }
        }
    }
}
impl Drop for Gates {
    fn drop(&mut self) {
        let _ = rustix::fs::fcntl_setfl(std::io::stdin().as_fd(), self.original_flags);
    }
}

fn credit(store: &IngestStore, spec: &InitialRegistration) -> serde_json::Value {
    let credit = store.producer_credit(spec.producer_slot_id).unwrap();
    serde_json::json!({"issued":credit.issued_through(),"acknowledged":credit.acknowledged_through(),"outstanding":credit.outstanding_requests(),"retired":credit.retired_through()})
}

fn observed_at_unix_us() -> u64 {
    // Diagnostic overlap observation only; this never supplies event time,
    // source identity, scheduling authority or a policy lease.
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros(),
    )
    .unwrap()
}

fn pending(
    store: &IngestStore,
    first: bigbrotr_model::SourceRecordId,
    budget: &ResourceBudget,
) -> serde_json::Value {
    let batch = store
        .read_pending_batch(
            first,
            PendingReadLimits {
                max_records: 8,
                max_encoded_bytes: MIB as usize,
                fact: bigbrotr_model::codec::FactDecodeLimits {
                    max_encoded_bytes: MIB as usize,
                    max_allocation_bytes: MIB as usize,
                    max_tag_nodes: 65536,
                    max_string_bytes: MIB as usize,
                },
            },
            grant(budget, 3 * MIB, 0),
            &AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();
    serde_json::json!({"batch_digest":batch.batch().digest().to_string(),"facts":batch.records().iter().map(|bytes| hex(&Sha256::digest(bytes))).collect::<Vec<_>>(),"pending_bytes":batch.pending_bytes()})
}

#[allow(
    clippy::too_many_arguments,
    reason = "finite diagnostic shares existing isolated owners and readers"
)]
pub async fn run(
    mut owner: ControlSession,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    intent: &EvidenceIntent,
    path: &Path,
    budget: &ResourceBudget,
    scope: QualifiedRef,
    fixture: Fixture,
) {
    assert_eq!(spec.dataset_id, [1; 16].try_into().unwrap());
    assert_eq!(spec.instance_id, [2; 16]);
    assert_eq!(store.native_limits(), limits());
    let gates = Gates::new();
    let cancelled = AtomicBool::new(false);
    let manifest = *store.manifest();
    let progress = ProgressGrant::new(grant(
        budget,
        ProgressGrant::REQUIRED_DATA_BYTES,
        ProgressGrant::REQUIRED_NATIVE_BYTES,
    ))
    .unwrap();
    let bootstrap = owner
        .bootstrap_initial_dictionaries_with_progress(
            &mut store,
            InitialBatchRequest {
                ingest_root: path,
                registration: spec,
                intent,
                recorded_at: ObservedTime::new(123),
                limits: BootstrapLimits {
                    max_mappings: 8,
                    max_batch_bytes: MIB as usize,
                },
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, 8 * MIB, 4 * MIB),
            &progress,
            validate,
        )
        .await
        .unwrap();
    let first = bootstrap.receipt().batch().unwrap().first();
    assert_eq!(bootstrap.receipt().batch().unwrap().records().len(), 3);
    let bootstrap_bytes = bootstrap.receipt().encode_persistent(8192).unwrap();
    drop(bootstrap);
    drop(progress);
    let mut expected_batches = vec![(first, pending(&store, first, budget))];
    let bootstrap_copy = owner
        .read_producer_outcome(request(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bootstrap_copy.kind, NativeOutcomeKind::NewFacts);
    assert_eq!(
        bootstrap_copy.outcome_sha256,
        <[u8; 32]>::from(Sha256::digest(&bootstrap_bytes))
    );
    assert_eq!(credit(&store, spec)["acknowledged"], 1);
    emit(
        serde_json::json!({"event":"physical-ready","events":fixture.records.len(),"source_bytes":fixture.identity.bytes,"source_sha256":hex(&fixture.identity.sha256),"retained_input_upper_bound":fixture.retained_input_upper_bound,"lease_seconds":LEASE_SECONDS,"gate_deadline_seconds":120,"receipt":hex(&bootstrap_bytes),"pending":expected_batches[0].1,"credit":credit(&store,spec),"profile":{"block_cache_bytes":32*MIB,"write_buffer_manager_bytes":16*MIB,"per_cf_write_buffer_bytes":2*MIB,"max_total_wal_bytes":16*MIB,"max_open_files":64,"max_background_jobs":2,"native_allowance_bytes":128*MIB}}),
    );
    gates.wait(b"load\n").await;
    let lineage = QualifiedRef {
        dictionary_kind: DictionaryKind::DatasetLineage,
        allocation_domain: [69; 16].try_into().unwrap(),
        ..scope
    };
    let count = fixture.records.len();
    let checkpoint_after = 31.min(count.saturating_sub(1));
    let Fixture {
        records, _inputs, ..
    } = fixture;
    for (index, (event, position)) in records.into_iter().enumerate() {
        if index == checkpoint_after {
            emit(
                serde_json::json!({"event":"physical-checkpoint-ready","admitted_events":index,"credit":credit(&store,spec)}),
            );
            gates.wait(b"checkpoint-started\n").await;
        }
        let id = event.id;
        let prepared = PreparedEvent::new(
            request(index as u64 + 2),
            EventIntent::import(1, scope, event, lineage).unwrap(),
            EVENT_BYTES,
            grant(budget, MIB, 0),
        )
        .unwrap();
        let request_digest = prepared.request_digest();
        let admission_started_at_unix_us = observed_at_unix_us();
        let stored = owner
            .admit_initial_event(
                &mut store,
                InitialEventRequest {
                    ingest_root: path,
                    registration: spec,
                    event: prepared,
                    relay_source: None,
                    acquisition: None,
                    provenance_enabled: true,
                    recorded_at: ObservedTime::new(456),
                    limits: AdmissionLimits::default(),
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
                },
                grant(budget, 40 * MIB, 10 * MIB),
                |document, digest, candidate| {
                    validate(document, digest)?;
                    if candidate.verification_limit_bytes() != EVENT_BYTES
                        || candidate.visibility_scope_id() != scope
                        || candidate.origin() != bigbrotr_model::EventOrigin::Import(lineage)
                        || candidate.observation().is_some()
                        || candidate.verified_event().is_none_or(|verified| {
                            verified.event().id != id || verified.normalized_bytes() != EVENT_BYTES
                        })
                    {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        let observed_at_unix_us = observed_at_unix_us();
        let AdmissionOutcome::Durable(receipt) = stored.outcome() else {
            panic!("verified distinct diagnostic event refused")
        };
        assert_eq!(receipt.batch().unwrap().records().len(), 3);
        let bytes = receipt.encode_persistent(8192).unwrap();
        let first = receipt.batch().unwrap().first();
        drop(stored);
        let copied = owner
            .read_producer_outcome(request(index as u64 + 2))
            .await
            .unwrap()
            .unwrap();
        let reservation = owner
            .read_producer_reservation(request(index as u64 + 2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(copied.kind, NativeOutcomeKind::NewFacts);
        assert_eq!(copied.request_digest, request_digest);
        assert_eq!(reservation.digest, request_digest);
        assert_eq!(
            copied.outcome_sha256,
            <[u8; 32]>::from(Sha256::digest(&bytes))
        );
        assert_eq!(copied.outcome_bytes as usize, bytes.len());
        assert_eq!(copied.policy_epoch.get(), 1);
        let expected = pending(&store, first, budget);
        emit(
            serde_json::json!({"event":"physical-admitted","index":index+1,"request_sequence":index+2,"event_id":id.to_string(),"recorded_at":456,"admission_started_at_unix_us":admission_started_at_unix_us,"observed_at_unix_us":observed_at_unix_us,"request_digest":request_digest.to_string(),"input_bytes":reservation.input_bytes,"line":position.line,"byte_start":position.byte_start,"byte_end":position.byte_end,"record_sha256":hex(&position.sha256),"receipt":hex(&bytes),"outcome_sha256":hex(&copied.outcome_sha256),"pending":expected,"credit":credit(&store,spec)}),
        );
        expected_batches.push((first, expected));
        assert_eq!(credit(&store, spec)["acknowledged"], 1);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(_inputs);
    let final_credit = credit(&store, spec);
    assert_eq!(final_credit["issued"], count + 1);
    assert_eq!(final_credit["outstanding"], count);
    emit(
        serde_json::json!({"event":"physical-load-complete","admitted_events":count,"credit":final_credit,"event_acknowledgement":"NOT_RUN-unbound-import","reclamation":"NOT_RUN","reserve_qualification":"NOT_RUN"}),
    );
    gates.wait(b"close\n").await;
    drop(store);
    let state = {
        let _snapshot = grant(budget, 80 * MIB, 0);
        ingest_snapshot::state(observer, path).await
    };
    emit(serde_json::json!({"event":"physical-closed","state":state}));
    gates.wait(b"reopen\n").await;
    let mut store = IngestStore::reopen(
        path,
        manifest,
        limits(),
        ingest_owner::grant(budget, 128 * MIB),
    )
    .unwrap();
    assert_eq!(store.native_limits(), limits());
    ingest_owner::reconcile_initial_pending_quota(
        &mut owner,
        &mut store,
        InitialPendingQuotaRequest {
            ingest_root: path,
            registration: spec,
            limits: ingest_recovery::initial_limits(AdmissionLimits::default()),
            policy_ttl: Duration::from_secs(2),
            cancelled: &cancelled,
        },
        budget,
        validate,
    )
    .await
    .unwrap();
    for (first, expected) in expected_batches {
        assert_eq!(
            pending(&store, first, budget),
            expected,
            "ordinary reopen preserved exact pending science"
        );
    }
    assert_eq!(credit(&store, spec), final_credit);
    drop(store);
    let reopened = {
        let _snapshot = grant(budget, 80 * MIB, 0);
        ingest_snapshot::state(observer, path).await
    };
    assert_eq!(
        reopened, state,
        "ordinary reopen preserved native state, originals and all control rows/xmins"
    );
    emit(
        serde_json::json!({"event":"physical-reopened","state":reopened,"credit":final_credit,"same_native_originals_outcomes_control":true}),
    );
    gates.wait(b"finish\n").await;
    owner.release().await.unwrap();
    emit(
        serde_json::json!({"event":"physical-complete","admitted_events":count,"credit":final_credit,"source_bound_event_acknowledgement":"NOT_RUN","reclamation":"NOT_RUN","reserve_qualification":"NOT_RUN","open65":"NOT_RUN"}),
    );
}
