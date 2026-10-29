//! Independent native component fixtures, not connected activation evidence.
//! The private activation call below supplies synthetic PostgreSQL authority.
//! Expected new input/fact/receipt/index bytes use the closed byte contracts,
//! never the production codecs, reducers, or a second execution as an oracle.
use super::*;
use crate::storage::rocks::store::{ActiveInitialWriter, InitialWriterCandidate};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, sync::atomic::AtomicBool};

#[path = "activated_recovery_tests.rs"]
mod activated_recovery_tests;

type Snapshot = (Rows, u64, Vec<(String, Vec<u8>)>);

fn positive<'a>(fixture: &Fixture, recovery: &'a IngestRecovery) -> ResolvedSource<'a> {
    let closed = recovery
        .observe_closed_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&fixture.budget, 4096, 4096),
        )
        .unwrap();
    let mut source = closed.source_resolution(recovery, 4).unwrap();
    for (index, original) in fixture.originals.iter().enumerate() {
        let resolved = recovery
            .resolve_original_request(
                request(7, index as u64 + 1),
                input_digest(original),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(resolved.native_outcome().bytes(), original.outcome);
        source.record(&resolved).unwrap();
    }
    let source = source.finish().unwrap();
    assert_eq!(source.digest(), fixture.closure);
    assert_eq!(source.last_complete_sequence(), 9);
    source
}

fn plan(fixture: &Fixture, candidate: &InitialWriterCandidate) -> InitialHandoff {
    candidate
        .handoff(
            &positive(fixture, candidate.recovery()),
            PolicyEpoch::new(1),
        )
        .unwrap()
}

fn active(fixture: &Fixture) -> ActiveInitialWriter {
    let mut candidate = InitialWriterCandidate::new(fixture.reopen()).unwrap();
    let handoff = plan(fixture, &candidate);
    assert_eq!(candidate.require_handoff(&handoff), Ok(()));
    assert_eq!(
        candidate.require_prepared_handoff(&handoff),
        Err(BootstrapError::StateMismatch)
    );
    let role = candidate.role_instance_id();
    let incarnation = candidate.process_incarnation();
    let sequence = candidate.recovery().store.db.latest_sequence_number();
    drop(
        candidate
            .recovery_mut()
            .apply_initial_handoff(&handoff, grant(&fixture.budget, 8192, 8192), || Ok(()))
            .unwrap(),
    );
    assert_eq!(
        candidate.recovery().store.db.latest_sequence_number(),
        sequence + 4
    );
    assert_eq!(candidate.require_prepared_handoff(&handoff), Ok(()));
    // No database/activation receipt is involved in this native-only fixture.
    let writer = candidate.activate(handoff).unwrap();
    assert_eq!(writer.role_instance_id(), role);
    assert_eq!(
        writer.manifest().identity().process_incarnation,
        incarnation
    );
    assert_eq!(writer.producer_slot_id(), request(7, 1).producer_slot_id);
    assert_eq!(writer.handoff().next_manifest(), writer.manifest());
    originals(fixture, writer.store());
    writer
}

fn next_request(sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_epoch: ProducerEpoch::new(2).unwrap(),
        ..request(7, sequence)
    }
}

fn scope(writer: &ActiveInitialWriter) -> QualifiedRef {
    QualifiedRef {
        allocation_domain: writer
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap(),
        ..reference(DictionaryKind::VisibilityScope, 1)
    }
}

fn dictionary(scope: QualifiedRef, class: &str) -> EvidenceIntent {
    EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                scope,
                DictionaryDescriptor::Visibility {
                    class: label(class),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap()
}

fn append_permit(fixture: &Fixture) -> OwnedPermit {
    let limits = AdmissionLimits::default();
    grant(
        &fixture.budget,
        limits.required_dictionary_data_bytes().unwrap(),
        limits.required_native_bytes().unwrap(),
    )
}

fn originals(fixture: &Fixture, store: &IngestStore) {
    let current = kv(store);
    for row in &fixture.closed_rows {
        if row.1 != [1, 1] {
            assert!(
                current.contains(row),
                "old row changed: {} {:?}",
                row.0,
                row.1
            );
        }
    }
    let current = inputs(&fixture.path());
    for (index, original) in fixture.originals.iter().enumerate() {
        assert!(current.contains(&(input_name(1, index as u64 + 1), original.input.clone())));
    }
}

fn unchanged(fixture: &Fixture, store: &IngestStore, before: &Snapshot) {
    assert_eq!(kv(store), before.0);
    assert_eq!(store.db.latest_sequence_number(), before.1);
    assert_eq!(inputs(&fixture.path()), before.2);
}

fn capture(fixture: &Fixture, store: &IngestStore) -> Snapshot {
    (
        kv(store),
        store.db.latest_sequence_number(),
        inputs(&fixture.path()),
    )
}

// Only the primitive CBOR forms used by this fixed independent recipe.
fn head(major: u8, value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    match value {
        0..=23 => result.push(major << 5 | value as u8),
        24..=255 => result.extend([major << 5 | 24, value as u8]),
        256..=65535 => {
            result.push(major << 5 | 25);
            result.extend_from_slice(&(value as u16).to_be_bytes());
        }
        65536..=0xffff_ffff => {
            result.push(major << 5 | 26);
            result.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            result.push(major << 5 | 27);
            result.extend_from_slice(&value.to_be_bytes());
        }
    }
    result
}
fn uint(value: u64) -> Vec<u8> {
    head(0, value)
}
fn bytes(value: &[u8]) -> Vec<u8> {
    [head(2, value.len() as u64), value.to_vec()].concat()
}
fn text(value: &str) -> Vec<u8> {
    [head(3, value.len() as u64), value.as_bytes().to_vec()].concat()
}
fn array(values: &[Vec<u8>]) -> Vec<u8> {
    [head(4, values.len() as u64), values.concat()].concat()
}
fn digest(value: &[u8]) -> Vec<u8> {
    Sha256::digest(value).to_vec()
}
fn qualified(domain: &[u8; 16]) -> Vec<u8> {
    array(&[bytes(&[1; 16]), uint(4), bytes(domain), uint(1)])
}
fn source(epoch: u64, sequence: u64) -> Vec<u8> {
    array(&[bytes(&[1; 16]), uint(0), uint(epoch), uint(sequence)])
}
fn raw_source(epoch: u64, sequence: Option<u64>) -> Vec<u8> {
    let mut value = [
        vec![1; 16],
        0_u32.to_be_bytes().to_vec(),
        epoch.to_be_bytes().to_vec(),
    ]
    .concat();
    if let Some(sequence) = sequence {
        value.extend_from_slice(&sequence.to_be_bytes());
    }
    value
}
fn raw_request(sequence: u64) -> Vec<u8> {
    [
        vec![7; 16],
        2_u64.to_be_bytes().to_vec(),
        sequence.to_be_bytes().to_vec(),
    ]
    .concat()
}
fn marker(issued: u64) -> Vec<u8> {
    [vec![1, 49], issued.to_be_bytes().to_vec(), vec![0; 16]].concat()
}

struct Expected {
    input: Vec<u8>,
    receipt: Vec<u8>,
    writes: Rows,
}
fn expected(domain: &[u8; 16], request_sequence: u64) -> Expected {
    let reference = qualified(domain);
    let descriptor_fields = [
        uint(1),
        text("visibility"),
        uint(1),
        [vec![0xa1], text("class"), text("writer-public")].concat(),
    ];
    let descriptor = array(&descriptor_fields);
    let payload = array(&[vec![reference.clone()], descriptor_fields.to_vec()].concat());
    let input = [
        b"bigbrotr/producer-request/1\0".to_vec(),
        array(&[
            array(&[bytes(&[7; 16]), uint(2), uint(request_sequence)]),
            text("admit-evidence"),
            uint(1),
            array(&[
                uint(1),
                reference.clone(),
                array(&[array(&[text("dictionary"), uint(1), payload.clone()])]),
            ]),
        ]),
    ]
    .concat();
    let fact = [
        b"bigbrotr/fact/1\0".to_vec(),
        array(&[
            source(2, 1),
            text("dictionary"),
            uint(1),
            reference,
            uint(77),
            payload,
        ]),
    ]
    .concat();
    let fact_digest = digest(&fact);
    let record = array(&[source(2, 1), bytes(&fact_digest)]);
    let records = array(&[record]);
    let receipt = [
        b"bigbrotr/receipt/1\0".to_vec(),
        array(&[
            uint(1),
            array(&[bytes(&[7; 16]), uint(2), uint(request_sequence)]),
            bytes(&digest(&input)),
            array(&[bytes(&[1; 16]), uint(2), uint(1), bytes(&[6; 16]), uint(1)]),
            records.clone(),
            records.clone(),
        ]),
    ]
    .concat();
    let batch = [
        b"bigbrotr/admission-batch/1\0".to_vec(),
        array(&[bytes(&[1; 16]), uint(0), uint(2), uint(1), uint(1), records]),
    ]
    .concat();
    let qualified_raw = [
        vec![1; 16],
        4_u16.to_be_bytes().to_vec(),
        domain.to_vec(),
        1_u64.to_be_bytes().to_vec(),
    ]
    .concat();
    let mut writes = Vec::new();
    let mut put =
        |cf: &str, key: Vec<u8>, value: Vec<u8>| writes.push((cf.to_string(), key, value));
    put(
        "facts",
        [vec![1, 32], raw_source(2, Some(1))].concat(),
        [vec![1, 32], fact_digest.clone(), fact].concat(),
    );
    put(
        "facts",
        [vec![1, 33], raw_source(2, Some(1))].concat(),
        [vec![1, 33], 1_u64.to_be_bytes().to_vec(), digest(&batch)].concat(),
    );
    put(
        "default",
        [vec![1, 4], qualified_raw.clone()].concat(),
        [vec![1, 4], raw_source(2, Some(1)), fact_digest].concat(),
    );
    put(
        "default",
        [
            vec![1, 5],
            vec![1; 16],
            4_u16.to_be_bytes().to_vec(),
            digest(&descriptor),
            qualified_raw,
        ]
        .concat(),
        [vec![1, 5], descriptor].concat(),
    );
    put(
        "default",
        [
            vec![1, 3],
            vec![1; 16],
            4_u16.to_be_bytes().to_vec(),
            domain.to_vec(),
        ]
        .concat(),
        [vec![1, 3], 1_u64.to_be_bytes().to_vec()].concat(),
    );
    put(
        "default",
        [vec![1, 2], raw_source(2, None)].concat(),
        [vec![1, 2], 1_u64.to_be_bytes().to_vec()].concat(),
    );
    put(
        "receipts",
        [vec![1, 48], raw_request(request_sequence)].concat(),
        receipt.clone(),
    );
    put(
        "receipts",
        [vec![1, 49], vec![7; 16], 2_u64.to_be_bytes().to_vec()].concat(),
        marker(request_sequence),
    );
    Expected {
        input,
        receipt,
        writes,
    }
}

fn assert_append(fixture: &Fixture, writer: &mut ActiveInitialWriter, sequence: u64) {
    let scope = scope(writer);
    let expected = expected(scope.allocation_domain.as_bytes(), sequence);
    let before = capture(fixture, writer.store());
    let intent = dictionary(scope, "writer-public");
    let result = writer
        .store_mut()
        .append_dictionaries(
            next_request(sequence),
            &intent,
            ObservedTime::new(77),
            AdmissionLimits::default(),
            append_permit(fixture),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        result.receipt().encode_persistent(8192).unwrap(),
        expected.receipt
    );
    drop(result);
    let mut rows: BTreeMap<_, _> = before
        .0
        .iter()
        .map(|(cf, key, value)| ((cf.clone(), key.clone()), value.clone()))
        .collect();
    for (cf, key, value) in expected.writes {
        rows.insert((cf, key), value);
    }
    assert_eq!(
        kv(writer.store()),
        rows.into_iter()
            .map(|((cf, key), value)| (cf, key, value))
            .collect::<Rows>()
    );
    assert_eq!(writer.store().db.latest_sequence_number(), before.1 + 8);
    let mut input_rows = before.2;
    input_rows.push((input_name(2, sequence), expected.input));
    input_rows.sort();
    assert_eq!(inputs(&fixture.path()), input_rows);
    originals(fixture, writer.store());
    let before = capture(fixture, writer.store());
    let replay = writer
        .store_mut()
        .append_dictionaries(
            next_request(sequence),
            &intent,
            ObservedTime::new(999),
            AdmissionLimits::default(),
            append_permit(fixture),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        replay.receipt().encode_persistent(8192).unwrap(),
        expected.receipt
    );
    drop(replay);
    unchanged(fixture, writer.store(), &before);
    let conflict = writer.store_mut().append_dictionaries(
        next_request(sequence),
        &dictionary(scope, "conflicting"),
        ObservedTime::new(77),
        AdmissionLimits::default(),
        append_permit(fixture),
        || Ok(()),
    );
    assert!(matches!(conflict, Err(AdmissionError::RequestConflict)));
    unchanged(fixture, writer.store(), &before);
}

#[test]
fn local_writer_first_dictionary_request_is_an_append_and_not_bootstrap() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    let before = capture(&fixture, writer.store());
    let intent = dictionary(scope(&writer), "writer-public");
    assert!(matches!(
        writer.store_mut().bootstrap_dictionaries(
            next_request(1),
            &intent,
            ObservedTime::new(77),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: 256 * 1024
            },
            grant(&fixture.budget, 4 * MIB, MIB),
            || Ok(()),
        ),
        Err(BootstrapError::InvalidIntent)
    ));
    unchanged(&fixture, writer.store(), &before);
    assert_append(&fixture, &mut writer, 1);
}

fn event(fixture: &Fixture, request_sequence: u64, rejected: bool) -> PreparedEvent {
    let intent = if rejected {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/ingest/signed-event-admission.json"
        ))
        .unwrap();
        let mut input = fixture["scenarios"][0]["operations"][4]["input"].clone();
        input["observation"]["witness"] = serde_json::Value::Null;
        fixture_intent(&input)
    } else {
        intent("E_private", 2, 10_000_000)
    };
    PreparedEvent::new(
        next_request(request_sequence),
        intent,
        256 * 1024,
        grant(&fixture.budget, MIB, 0),
    )
    .unwrap()
}

#[test]
fn rejected_or_already_durable_request_one_keeps_zero_source_then_append_two_starts_one() {
    for rejected in [true, false] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        let before = capture(&fixture, writer.store());
        let outcome = writer
            .store_mut()
            .admit_event(
                event(&fixture, 1, rejected),
                ObservedTime::new(77),
                false,
                AdmissionLimits::default(),
                grant(&fixture.budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        if rejected {
            assert!(matches!(
                outcome.outcome(),
                AdmissionOutcome::Rejected(RejectionReason::Signature)
            ));
        } else {
            let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
                panic!("signed duplicate")
            };
            assert!(receipt.batch().is_none());
            assert_eq!(receipt.request_id(), next_request(1));
            assert_eq!(receipt.protection_context().topology_generation.get(), 2);
            assert_eq!(
                receipt
                    .dependencies()
                    .records()
                    .iter()
                    .map(|r| (
                        r.source_record_id.owner_epoch.get(),
                        r.source_record_id.sequence.get()
                    ))
                    .collect::<Vec<_>>(),
                [(1, 1), (1, 2), (1, 3), (1, 6), (1, 7), (1, 8), (1, 9)]
            );
        }
        drop(outcome);
        assert_eq!(
            value(writer.store(), source_key(2)),
            [vec![1, 2], vec![0; 8]].concat()
        );
        assert_eq!(writer.store().db.latest_sequence_number(), before.1 + 2);
        let rows = kv(writer.store());
        let changes: Vec<_> = rows.iter().filter(|row| !before.0.contains(row)).collect();
        assert_eq!(changes.len(), 2);
        assert!(
            changes
                .iter()
                .all(|row| row.0 == "receipts" && matches!(row.1[1], 48 | 49))
        );
        assert!(
            before
                .0
                .iter()
                .filter(|row| !(row.0 == "receipts"
                    && row.1[1] == 49
                    && row.1.ends_with(&2_u64.to_be_bytes())))
                .all(|row| rows.contains(row))
        );
        assert_eq!(inputs(&fixture.path()).len(), before.2.len() + 1);
        let retained = capture(&fixture, writer.store());
        drop(
            writer
                .store_mut()
                .admit_event(
                    event(&fixture, 1, rejected),
                    ObservedTime::new(999),
                    false,
                    AdmissionLimits::default(),
                    grant(&fixture.budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap(),
        );
        unchanged(&fixture, writer.store(), &retained);
        assert_append(&fixture, &mut writer, 2);
    }
}

#[test]
fn observed_successor_cannot_recreate_live_writer_custody() {
    let fixture = closed_fixture();
    let writer = active(&fixture);
    let target = *writer.manifest();
    let intent = dictionary(scope(&writer), "writer-public");
    let before = capture(&fixture, writer.store());
    drop(writer);
    let mut observed = IngestStore::reopen(
        &fixture.path(),
        target,
        fixture.native,
        grant(&fixture.budget, 0, 128 * MIB),
    )
    .unwrap();
    assert!(matches!(
        observed.append_dictionaries(
            next_request(1),
            &intent,
            ObservedTime::new(77),
            AdmissionLimits::default(),
            append_permit(&fixture),
            || Ok(()),
        ),
        Err(AdmissionError::InvalidInput)
    ));
    assert!(matches!(
        observed.admit_event(
            event(&fixture, 1, false),
            ObservedTime::new(77),
            false,
            AdmissionLimits::default(),
            grant(&fixture.budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        ),
        Err(AdmissionError::InvalidInput)
    ));
    unchanged(&fixture, &observed, &before);
    drop(observed);
    assert!(matches!(
        InitialWriterCandidate::new(fixture.reopen()),
        Err(BootstrapError::InvalidIntent)
    ));
    let recovery = fixture.reopen();
    unchanged(&fixture, &recovery.store, &before);
}

#[test]
fn candidate_requires_its_minted_target_local_root_current_proof_and_no_sidecars() {
    let fixture = closed_fixture();
    let other_fixture = closed_fixture();
    let candidate = InitialWriterCandidate::new(fixture.reopen()).unwrap();
    let other = InitialWriterCandidate::new(other_fixture.reopen()).unwrap();
    assert_eq!(kv(&candidate.recovery().store), kv(&other.recovery().store));
    assert_eq!(
        candidate.recovery().store.db.latest_sequence_number(),
        other.recovery().store.db.latest_sequence_number()
    );
    let own = positive(&fixture, candidate.recovery());
    let foreign = positive(&other_fixture, other.recovery());
    assert!(matches!(
        candidate.handoff(&foreign, PolicyEpoch::new(1)),
        Err(BootstrapError::StateMismatch)
    ));
    let plan = candidate.handoff(&own, PolicyEpoch::new(1)).unwrap();
    assert_eq!(
        other.require_handoff(&plan),
        Err(BootstrapError::StateMismatch)
    );
    let caller_selected = InitialHandoff::from_source(&own, fixture.next).unwrap();
    assert_eq!(
        candidate.require_handoff(&caller_selected),
        Err(BootstrapError::StateMismatch)
    );
    let before = capture(&fixture, &candidate.recovery().store);
    drop(own);
    drop(foreign);
    assert!(matches!(
        candidate.activate(plan),
        Err(BootstrapError::StateMismatch)
    ));
    let recovery = fixture.reopen();
    unchanged(&fixture, &recovery.store, &before);
    drop(recovery);
    drop(other);

    for fault in [
        "foreign-root",
        "same-bytes",
        "sidecar-before",
        "sidecar-after",
        "sidecar-completed",
    ] {
        let mut candidate = InitialWriterCandidate::new(fixture.reopen()).unwrap();
        let sidecar = fixture
            .path()
            .join("producer-inputs-v1")
            .join(input_name(2, 1));
        if fault == "sidecar-before" {
            std::fs::write(&sidecar, b"unrelated target input").unwrap();
            // Reach the target-epoch conflict rather than the private-file mode guard.
            std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let source = positive(&fixture, candidate.recovery());
        let result = candidate.handoff(&source, PolicyEpoch::new(1));
        drop(source);
        if fault == "sidecar-before" {
            // Portable data can be formed; local preflight must still refuse.
            let plan = result.unwrap();
            let before = capture(&fixture, &candidate.recovery().store);
            assert_eq!(
                candidate.require_handoff(&plan),
                Err(BootstrapError::RequestInput(RequestInputError::Conflict))
            );
            assert!(
                candidate
                    .recovery_mut()
                    .apply_initial_handoff(&plan, grant(&fixture.budget, 8192, 8192), || Ok(()))
                    .is_err()
            );
            unchanged(&fixture, &candidate.recovery().store, &before);
            drop(candidate);
            std::fs::remove_file(&sidecar).unwrap();
            continue;
        }
        let plan = result.unwrap();
        if fault == "foreign-root" {
            let foreign = other_fixture.reopen();
            let before = capture(&other_fixture, &foreign.store);
            // Isolate root identity from state/sequence disagreement.
            assert_eq!(kv(&candidate.recovery().store), before.0);
            assert_eq!(
                candidate.recovery().store.db.latest_sequence_number(),
                before.1
            );
            assert_eq!(
                foreign.validate_initial_handoff(&plan),
                Err(BootstrapError::StateMismatch)
            );
            unchanged(&other_fixture, &foreign.store, &before);
            continue;
        }
        if fault == "sidecar-completed" {
            drop(
                candidate
                    .recovery_mut()
                    .apply_initial_handoff(&plan, grant(&fixture.budget, 8192, 8192), || Ok(()))
                    .unwrap(),
            );
        }
        if fault == "same-bytes" {
            let bytes = value(&candidate.recovery().store, source_key(1));
            put(&candidate.recovery().store, source_key(1), &bytes);
        } else {
            std::fs::write(&sidecar, b"unrelated target input").unwrap();
            // Reach the target-epoch conflict rather than the private-file mode guard.
            std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let before = capture(&fixture, &candidate.recovery().store);
        assert!(candidate.require_handoff(&plan).is_err(), "{fault}");
        assert!(
            candidate
                .recovery_mut()
                .apply_initial_handoff(&plan, grant(&fixture.budget, 8192, 8192), || Ok(()))
                .is_err(),
            "{fault}"
        );
        unchanged(&fixture, &candidate.recovery().store, &before);
        if fault == "sidecar-completed" {
            assert!(matches!(
                candidate.activate(plan),
                Err(BootstrapError::RequestInput(RequestInputError::Conflict))
            ));
        } else {
            drop(candidate);
        }
        if sidecar.exists() {
            std::fs::remove_file(&sidecar).unwrap();
        }
    }
}

#[test]
fn active_zero_source_is_explicit_and_rejects_stray_facts_pending_or_allocation_zero() {
    for fault in [
        "source-missing",
        "source-malformed",
        "fact",
        "pending",
        "allocation-zero",
        "event-allocation-zero",
        "producer-missing",
    ] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        let scope = scope(&writer);
        match fault {
            "source-missing" => delete(writer.store(), source_key(2)),
            "source-malformed" => put(writer.store(), source_key(2), &[1, 2]),
            "fact" => put(
                writer.store(),
                IngestKey::Fact {
                    source_record_id: fact_id(2, 7),
                },
                b"stray",
            ),
            "pending" => put(
                writer.store(),
                IngestKey::PendingBatch {
                    first: fact_id(2, 7),
                },
                b"stray",
            ),
            "allocation-zero" => put(
                writer.store(),
                IngestKey::AllocationStream {
                    dataset_id: scope.dataset_id,
                    dictionary_kind: scope.dictionary_kind,
                    allocation_domain: scope.allocation_domain,
                },
                &[1, 3, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            "event-allocation-zero" => put(
                writer.store(),
                IngestKey::AllocationStream {
                    dataset_id: scope.dataset_id,
                    dictionary_kind: DictionaryKind::Event,
                    allocation_domain: writer
                        .manifest()
                        .allocation_namespaces()
                        .domain(DictionaryKind::Event)
                        .unwrap(),
                },
                &[1, 3, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            "producer-missing" => delete(
                writer.store(),
                IngestKey::ProducerState {
                    producer_slot_id: next_request(1).producer_slot_id,
                    producer_epoch: next_request(1).producer_epoch,
                },
            ),
            _ => unreachable!(),
        }
        let before = capture(&fixture, writer.store());
        if fault == "event-allocation-zero" {
            // This ID is absent from the retained E_private fixture: a duplicate
            // would not consult the new Event allocation counter at all.
            assert_ne!(fixture_event("B_note").id, fixture_event("E_private").id);
            let input = PreparedEvent::new(
                next_request(1),
                intent("B_note", 1, 10_000_000),
                256 * 1024,
                grant(&fixture.budget, MIB, 0),
            )
            .unwrap();
            assert!(matches!(
                writer.store_mut().admit_event(
                    input,
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    grant(&fixture.budget, 40 * MIB, 10 * MIB),
                    || Ok(())
                ),
                Err(AdmissionError::StateMismatch)
            ));
            unchanged(&fixture, writer.store(), &before);
            // Remove only the injected zero: the same signed new ID must then
            // reach Event ordinal1 and source1, proving the refusal's boundary.
            delete(
                writer.store(),
                IngestKey::AllocationStream {
                    dataset_id: scope.dataset_id,
                    dictionary_kind: DictionaryKind::Event,
                    allocation_domain: writer
                        .manifest()
                        .allocation_namespaces()
                        .domain(DictionaryKind::Event)
                        .unwrap(),
                },
            );
            let input = PreparedEvent::new(
                next_request(1),
                intent("B_note", 1, 10_000_000),
                256 * 1024,
                grant(&fixture.budget, MIB, 0),
            )
            .unwrap();
            let outcome = writer
                .store_mut()
                .admit_event(
                    input,
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    grant(&fixture.budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap();
            let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
                panic!("signed B_note")
            };
            let batch = receipt.batch().unwrap();
            assert_eq!(batch.first(), fact_id(2, 1));
            assert_eq!(batch.last(), fact_id(2, 4));
            drop(outcome);
            let all = facts(writer.store());
            let new: Vec<_> = all
                .iter()
                .filter(|fact| fact.source_record_id().owner_epoch.get() == 2)
                .collect();
            assert_eq!(new.len(), 4);
            assert!(matches!(new[0].payload(), FactPayload::Dictionary(entry)
                if entry.reference().ordinal.get() == 1
                && entry.reference().allocation_domain == writer.manifest().allocation_namespaces().domain(DictionaryKind::Event).unwrap()
                && entry.descriptor() == &DictionaryDescriptor::Event(fixture_event("B_note").id)));
            assert!(
                matches!(new[1].payload(), FactPayload::EventPayload(payload)
                if payload.body == EventBody::from(fixture_event("B_note")))
            );
            assert!(
                matches!(new[2].payload(), FactPayload::EventEligibility(eligibility)
                if eligibility.sig == fixture_event("B_note").sig)
            );
            assert!(
                matches!(new[3].payload(), FactPayload::EventReceive(receive)
                if receive.received_at.get() == 10_000_000)
            );
            originals(&fixture, writer.store());
            continue;
        }
        assert!(
            matches!(
                writer.store_mut().append_dictionaries(
                    next_request(1),
                    &dictionary(scope, "writer-public"),
                    ObservedTime::new(77),
                    AdmissionLimits::default(),
                    append_permit(&fixture),
                    || Ok(()),
                ),
                Err(AdmissionError::StateMismatch)
            ),
            "{fault}"
        );
        unchanged(&fixture, writer.store(), &before);
        if fault != "allocation-zero" {
            for rejected in [false, true] {
                assert!(
                    matches!(
                        writer.store_mut().admit_event(
                            event(&fixture, 1, rejected),
                            ObservedTime::new(77),
                            false,
                            AdmissionLimits::default(),
                            grant(&fixture.budget, 40 * MIB, 10 * MIB),
                            || Ok(()),
                        ),
                        Err(AdmissionError::StateMismatch)
                    ),
                    "{fault}/{rejected}"
                );
                unchanged(&fixture, writer.store(), &before);
            }
        }
    }
}

#[test]
fn dictionary_grants_and_authority_refuse_before_write_without_losing_exact_retry() {
    for fault in [
        "data",
        "native",
        "items",
        "authority1",
        "authority2",
        "authority3",
    ] {
        let fixture = closed_fixture();
        let mut writer = active(&fixture);
        let scope = scope(&writer);
        let intent = dictionary(scope, "writer-public");
        let limits = AdmissionLimits::default();
        let data = limits.required_dictionary_data_bytes().unwrap();
        let native = limits.required_native_bytes().unwrap();
        let permit = fixture
            .budget
            .try_acquire(
                ResourceRequest::data(
                    data - u64::from(fault == "data"),
                    u64::from(fault != "items"),
                    native - u64::from(fault == "native"),
                )
                .unwrap(),
            )
            .unwrap();
        let before = capture(&fixture, writer.store());
        let mut calls = 0;
        let result = writer.store_mut().append_dictionaries(
            next_request(1),
            &intent,
            ObservedTime::new(77),
            limits,
            permit,
            || {
                calls += 1;
                if fault == format!("authority{calls}") {
                    Err(AdmissionError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        if fault.starts_with("authority") {
            assert_eq!(
                calls,
                fault
                    .strip_prefix("authority")
                    .unwrap()
                    .parse::<u32>()
                    .unwrap()
            );
            assert!(
                matches!(result, Err(AdmissionError::AuthorityUnavailable)),
                "{fault}"
            );
        } else {
            assert!(
                matches!(result, Err(AdmissionError::InsufficientReservation)),
                "{fault}"
            );
            assert_eq!(calls, 0);
        }
        assert_eq!(kv(writer.store()), before.0);
        assert_eq!(writer.store().db.latest_sequence_number(), before.1);
        if fault == "authority3" {
            // The final native prewrite veto follows immutable input staging.
            let mut expected_inputs = before.2.clone();
            expected_inputs.push((
                input_name(2, 1),
                expected(scope.allocation_domain.as_bytes(), 1).input,
            ));
            expected_inputs.sort();
            assert_eq!(inputs(&fixture.path()), expected_inputs);
        } else {
            unchanged(&fixture, writer.store(), &before);
        }
        // The same owned candidate, request and bytes remain usable after a
        // prewrite refusal. No changed authority is inferred from observations.
        let result = writer
            .store_mut()
            .append_dictionaries(
                next_request(1),
                &intent,
                ObservedTime::new(77),
                limits,
                append_permit(&fixture),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(
            result.receipt().encode_persistent(8192).unwrap(),
            expected(scope.allocation_domain.as_bytes(), 1).receipt
        );
        drop(result);
        originals(&fixture, writer.store());
    }
}

#[test]
fn borrowed_live_pending_keeps_owned_bytes_across_head_advance_and_refuses_other_streams() {
    // Native component only: active() supplies the existing synthetic connected
    // activation boundary. Public PG selection/authority is qualified separately.
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    assert_append(&fixture, &mut writer, 1);
    let limits = PendingReadLimits {
        max_records: 64,
        max_encoded_bytes: MIB as usize,
        fact: bigbrotr_model::codec::FactDecodeLimits::default(),
    };
    let permit = || grant(&fixture.budget, limits.required_data_bytes().unwrap(), 0);
    let cancel = AtomicBool::new(false);
    let source = writer.index_source().unwrap();
    let first = fact_id(2, 1);
    let head = source.observe(&cancel).unwrap();
    let pending = source
        .read_pending_batch(first, limits, permit(), &cancel)
        .unwrap()
        .unwrap();
    let expected = expected(scope(&writer).allocation_domain.as_bytes(), 1);
    let fact = expected
        .writes
        .iter()
        .find(|(cf, key, _)| cf == "facts" && key[1] == 32)
        .unwrap();
    assert_eq!(pending.records(), [fact.2[34..].to_vec()]);
    let used = fixture.budget.snapshot().active_permits();
    for foreign in [
        SourceRecordId {
            dataset_id: [99; 16].try_into().unwrap(),
            ..first
        },
        SourceRecordId {
            shard_id: 1,
            ..first
        },
        SourceRecordId {
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            ..first
        },
        SourceRecordId {
            sequence: Sequence::new(3).unwrap(),
            ..first
        },
    ] {
        assert!(matches!(
            source.read_pending_batch(foreign, limits, permit(), &cancel),
            Err(PendingError::IncompleteBatch)
        ));
        assert_eq!(fixture.budget.snapshot().active_permits(), used);
    }
    assert!(matches!(
        source.read_pending_batch(first, limits, grant(&fixture.budget, 1, 0), &cancel),
        Err(PendingError::InsufficientReservation)
    ));
    cancel.store(true, std::sync::atomic::Ordering::Release);
    assert!(matches!(
        source.read_pending_batch(first, limits, permit(), &cancel),
        Err(PendingError::Cancelled)
    ));
    cancel.store(false, std::sync::atomic::Ordering::Release);
    let input = PreparedEvent::new(
        next_request(2),
        intent("B_note", 1, 10_000_000),
        256 * 1024,
        grant(&fixture.budget, MIB, 0),
    )
    .unwrap();
    drop(
        writer
            .store_mut()
            .admit_event(
                input,
                ObservedTime::new(77),
                false,
                AdmissionLimits::default(),
                grant(&fixture.budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let source = writer.index_source().unwrap();
    let after = source.observe(&cancel).unwrap();
    assert!(after.through() > head.through());
    assert_eq!(source.handoff().last_complete_sequence(), 9);
    let replay = source
        .read_pending_batch(first, limits, permit(), &cancel)
        .unwrap()
        .unwrap();
    assert_eq!(replay.batch(), pending.batch());
    assert_eq!(replay.records(), pending.records());
    assert_eq!(source.retained_requests().unwrap(), 6); // Four old, two new; facts differ.
    drop(replay);
    drop(pending);
    assert_eq!(fixture.budget.snapshot().active_permits(), used - 1);
    originals(&fixture, writer.store());
}

#[test]
fn live_request_census_counts_receipt_only_requests_without_changing_producer_admission() {
    let fixture = closed_fixture();
    let mut writer = active(&fixture);
    let intent = dictionary(scope(&writer), "writer-public");
    let limits = AdmissionLimits::default();
    // The shared retained spool is literally64 inputs, across epochs. Four
    // original predecessor inputs leave60 new ones; the producer's independent
    // per-epoch window is still64. No reclamation or65-file state is fabricated.
    assert_eq!(fixture.originals.len(), 4);
    assert_eq!(inputs(&fixture.path()).len(), 4);
    assert_eq!(limits.max_outstanding_requests, 64);
    for sequence in 1..=60 {
        let outcome = writer
            .store_mut()
            .append_dictionaries(
                next_request(sequence),
                &intent,
                ObservedTime::new(77),
                limits,
                append_permit(&fixture),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(outcome.receipt().request_id(), next_request(sequence));
        assert_eq!(outcome.receipt().batch().is_some(), sequence == 1);
        drop(outcome);
        let source = writer.index_source().unwrap();
        assert_eq!(source.retained_requests().unwrap(), 4 + sequence);
        assert_eq!(
            source
                .observe(&AtomicBool::new(false))
                .unwrap()
                .through()
                .get(),
            1
        );
    }
    assert_eq!(writer.producer_credit().unwrap().issued_through(), 60);
    assert_eq!(
        writer.index_source().unwrap().retained_requests().unwrap(),
        64
    );
    let mut expected_inputs: Vec<_> = fixture
        .originals
        .iter()
        .enumerate()
        .map(|(i, original)| (input_name(1, i as u64 + 1), original.input.clone()))
        .collect();
    expected_inputs.extend((1..=60).map(|sequence| {
        (
            input_name(2, sequence),
            expected(scope(&writer).allocation_domain.as_bytes(), sequence).input,
        )
    }));
    expected_inputs.sort();
    assert_eq!(expected_inputs.len(), 64);
    assert!(expected_inputs.iter().all(|(_, bytes)| bytes.len() <= 8192));
    assert!(
        expected_inputs
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>()
            < 32 * MIB as usize
    );
    assert_eq!(inputs(&fixture.path()), expected_inputs);
    // Request61 still fits the current producer window. The existing shared
    // input envelope, before native commit, is the discriminating refusal.
    writer
        .store()
        .require_request_capacity(next_request(61), limits.max_outstanding_requests)
        .unwrap();
    let before = capture(&fixture, writer.store());
    let used = fixture.budget.snapshot().used();
    let permits = fixture.budget.snapshot().active_permits();
    assert!(matches!(
        writer.store_mut().append_dictionaries(
            next_request(61),
            &intent,
            ObservedTime::new(77),
            limits,
            append_permit(&fixture),
            || Ok(())
        ),
        Err(AdmissionError::RequestInput(RequestInputError::Limit))
    ));
    unchanged(&fixture, writer.store(), &before);
    assert_eq!(fixture.budget.snapshot().used(), used);
    assert_eq!(fixture.budget.snapshot().active_permits(), permits);
    assert_eq!(writer.producer_credit().unwrap().issued_through(), 60);
    assert_eq!(
        writer.index_source().unwrap().retained_requests().unwrap(),
        64
    );
    let replay = writer
        .store_mut()
        .append_dictionaries(
            next_request(60),
            &intent,
            ObservedTime::new(999),
            limits,
            append_permit(&fixture),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(replay.receipt().request_id(), next_request(60));
    assert!(replay.receipt().batch().is_none());
    assert_eq!(
        replay.receipt().encode_persistent(8192).unwrap(),
        value(
            writer.store(),
            IngestKey::Request {
                request_id: next_request(60)
            }
        )
    );
    drop(replay);
    unchanged(&fixture, writer.store(), &before);
    assert_eq!(fixture.budget.snapshot().used(), used);
    assert_eq!(fixture.budget.snapshot().active_permits(), permits);
    originals(&fixture, writer.store());
}
