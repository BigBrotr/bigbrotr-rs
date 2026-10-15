//! Component fixtures with an explicit synthetic confirmed-PG premise. No SQL
//! activation or process replacement is inferred from these native-only calls.
//! New dictionary bytes below are literal CBOR/framing, independent of codecs.
use super::*;
use crate::storage::rocks::store::{ActiveSuccessorWriter, NativeSuccessorActivation};
use std::collections::BTreeMap;

#[path = "successor_recovery_tests.rs"]
mod successor_recovery_tests;

type Snapshot = (Rows, u64, Vec<(String, Vec<u8>)>);
fn snapshot(f: &Fixture, store: &IngestStore) -> Snapshot {
    (
        kv(store),
        store.db.latest_sequence_number(),
        inputs(&f.path()),
    )
}
fn envelope(
    f: &Fixture,
    later: bool,
    kind: PredecessorTerminalKind,
    complete: bool,
) -> (
    NativeSuccessorActivation,
    *const crate::storage::rocks::store::RetainedLineageEntry,
) {
    let r = if later {
        let r = valid_empty_third(f);
        put(&r.store, producer_key(3), &marker(true, 0));
        r
    } else {
        terminal_second(f)
    };
    let mut candidate = SuccessorCandidate::new(r).unwrap();
    let (lineage, handoff) = local_plan(f, &candidate, kind);
    let allocation = lineage.entries().as_ptr();
    if complete {
        candidate
            .apply_handoff(
                &lineage,
                &handoff,
                grant(&f.budget, 8192, 8192),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    (
        NativeSuccessorActivation::new(candidate, lineage, handoff),
        allocation,
    )
}
fn active(f: &Fixture, later: bool) -> ActiveSuccessorWriter {
    let (mut owner, pointer) = envelope(f, later, PredecessorTerminalKind::UnactivatedEmpty, true);
    let before = snapshot(f, &owner.recovery().unwrap().store);
    let reserved = f.budget.snapshot().used();
    let writer = owner.activate(&NOT_CANCELLED, || Ok(())).unwrap();
    assert_eq!(f.budget.snapshot().used(), reserved);
    assert!(owner.recovery().is_none());
    assert!(matches!(
        owner.activate(&NOT_CANCELLED, || Ok(())),
        Err(BootstrapError::AuthorityUnavailable)
    ));
    assert_eq!(snapshot(f, writer.store()), before);
    let (actual, capacity) = writer
        .store()
        .live_successor
        .as_ref()
        .unwrap()
        .entries_identity();
    assert_eq!(actual, pointer);
    assert!(capacity >= writer.manifest().identity().owner_epoch.get() as usize - 1);
    writer.validate(&NOT_CANCELLED).unwrap();
    writer
}
fn scope(writer: &ActiveSuccessorWriter) -> QualifiedRef {
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
fn operation(f: &Fixture) -> OwnedPermit {
    let limits = AdmissionLimits::default();
    grant(
        &f.budget,
        limits.required_dictionary_data_bytes().unwrap(),
        limits.required_native_bytes().unwrap(),
    )
}
fn event(f: &Fixture, epoch: u64, n: u64, rejected: bool) -> PreparedEvent {
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
    PreparedEvent::new(at(epoch, n), intent, 256 * 1024, grant(&f.budget, MIB, 0)).unwrap()
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
fn raw_request(epoch: u64, sequence: u64) -> Vec<u8> {
    [
        vec![7; 16],
        epoch.to_be_bytes().to_vec(),
        sequence.to_be_bytes().to_vec(),
    ]
    .concat()
}
fn open_marker(issued: u64) -> Vec<u8> {
    [vec![1, 49], issued.to_be_bytes().to_vec(), vec![0; 16]].concat()
}

struct Expected {
    input: Vec<u8>,
    receipt: Vec<u8>,
    writes: Rows,
}
fn expected(epoch: u64, domain: &[u8; 16], request_sequence: u64) -> Expected {
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
            array(&[bytes(&[7; 16]), uint(epoch), uint(request_sequence)]),
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
            source(epoch, 1),
            text("dictionary"),
            uint(1),
            reference,
            uint(77),
            payload,
        ]),
    ]
    .concat();
    let fact_digest = digest(&fact);
    let record = array(&[source(epoch, 1), bytes(&fact_digest)]);
    let records = array(&[record]);
    let receipt = [
        b"bigbrotr/receipt/1\0".to_vec(),
        array(&[
            uint(1),
            array(&[bytes(&[7; 16]), uint(epoch), uint(request_sequence)]),
            bytes(&digest(&input)),
            array(&[
                bytes(&[1; 16]),
                uint(epoch),
                uint(1),
                bytes(&[6; 16]),
                uint(7),
            ]),
            records.clone(),
            records.clone(),
        ]),
    ]
    .concat();
    let batch = [
        b"bigbrotr/admission-batch/1\0".to_vec(),
        array(&[
            bytes(&[1; 16]),
            uint(0),
            uint(epoch),
            uint(1),
            uint(1),
            records,
        ]),
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
        [vec![1, 32], raw_source(epoch, Some(1))].concat(),
        [vec![1, 32], fact_digest.clone(), fact].concat(),
    );
    put(
        "facts",
        [vec![1, 33], raw_source(epoch, Some(1))].concat(),
        [vec![1, 33], 1_u64.to_be_bytes().to_vec(), digest(&batch)].concat(),
    );
    put(
        "default",
        [vec![1, 4], qualified_raw.clone()].concat(),
        [vec![1, 4], raw_source(epoch, Some(1)), fact_digest].concat(),
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
        [vec![1, 2], raw_source(epoch, None)].concat(),
        [vec![1, 2], 1_u64.to_be_bytes().to_vec()].concat(),
    );
    put(
        "receipts",
        [vec![1, 48], raw_request(epoch, request_sequence)].concat(),
        receipt.clone(),
    );
    put(
        "receipts",
        [vec![1, 49], vec![7; 16], epoch.to_be_bytes().to_vec()].concat(),
        open_marker(request_sequence),
    );
    Expected {
        input,
        receipt,
        writes,
    }
}

fn append_and_check(f: &Fixture, writer: &mut ActiveSuccessorWriter, n: u64) {
    let epoch = writer.manifest().identity().owner_epoch.get();
    let reference = scope(writer);
    let expected = expected(epoch, reference.allocation_domain.as_bytes(), n);
    let intent = dictionary(reference, "writer-public");
    let before = snapshot(f, writer.store());
    let permit = operation(f);
    let staged = writer
        .stage_dictionary_append(
            at(epoch, n),
            &intent,
            ObservedTime::new(77),
            AdmissionLimits::default(),
            &permit,
            &NOT_CANCELLED,
        )
        .unwrap();
    assert_eq!(staged.request, at(epoch, n));
    assert_eq!(staged.input_bytes as usize, expected.input.len());
    assert_eq!(
        staged.digest.as_bytes(),
        &<[u8; 32]>::from(Sha256::digest(&expected.input))
    );
    assert_eq!(kv(writer.store()), before.0);
    assert_eq!(writer.store().db.latest_sequence_number(), before.1);
    let mut wanted_inputs = before.2.clone();
    wanted_inputs.push((input_name(epoch, n), expected.input));
    wanted_inputs.sort();
    assert_eq!(inputs(&f.path()), wanted_inputs);
    let result = writer
        .append_dictionaries(
            at(epoch, n),
            &intent,
            ObservedTime::new(77),
            AdmissionLimits::default(),
            permit,
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        result.receipt().encode_persistent(8192).unwrap(),
        expected.receipt
    );
    drop(result);
    let mut wanted: BTreeMap<_, _> = before
        .0
        .into_iter()
        .map(|(cf, k, v)| ((cf, k), v))
        .collect();
    for (cf, k, v) in expected.writes {
        wanted.insert((cf, k), v);
    }
    assert_eq!(
        kv(writer.store()),
        wanted
            .into_iter()
            .map(|((cf, k), v)| (cf, k, v))
            .collect::<Rows>()
    );
    assert_eq!(writer.store().db.latest_sequence_number(), before.1 + 8);
    assert_eq!(inputs(&f.path()), wanted_inputs);
    let completed = snapshot(f, writer.store());
    drop(
        writer
            .append_dictionaries(
                at(epoch, n),
                &intent,
                ObservedTime::new(999),
                AdmissionLimits::default(),
                operation(f),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap(),
    );
    assert_eq!(snapshot(f, writer.store()), completed);
    assert!(matches!(
        writer.append_dictionaries(
            at(epoch, n),
            &dictionary(reference, "conflict"),
            ObservedTime::new(77),
            AdmissionLimits::default(),
            operation(f),
            &NOT_CANCELLED,
            || Ok(())
        ),
        Err(AdmissionError::RequestConflict)
    ));
    assert_eq!(snapshot(f, writer.store()), completed);
    writer.validate(&NOT_CANCELLED).unwrap();
}

#[test]
fn successor_writer_moves_one_lineage_and_appends_literal_request_one_at_three_and_four() {
    for later in [false, true] {
        let f = prepared_fixture();
        let mut writer = active(&f, later);
        assert_eq!(writer.manifest().key_schema(), 3);
        assert_eq!(writer.record().fields().next_manifest, *writer.manifest());
        assert_ne!(writer.role_instance_id(), [0; 16]);
        assert_eq!(writer.producer_slot_id(), request(7, 1).producer_slot_id);
        assert!(writer.matches_configured_root(&f.path()));
        append_and_check(&f, &mut writer, 1);
    }
    // Activated-zero and empty-zero predecessor labels are distinct proof
    // premises; either can feed the exact same one-way local transfer.
    let f = prepared_fixture();
    let (mut owner, pointer) = envelope(&f, false, PredecessorTerminalKind::Activated, true);
    let writer = owner.activate(&NOT_CANCELLED, || Ok(())).unwrap();
    assert_eq!(
        writer
            .store()
            .live_successor
            .as_ref()
            .unwrap()
            .entries_identity()
            .0,
        pointer
    );
}

#[test]
fn successor_writer_requires_completed_local_handoff_and_retains_failed_promotion_custody() {
    for fault in 0..5 {
        let f = prepared_fixture();
        let (mut owner, _) = envelope(
            &f,
            false,
            PredecessorTerminalKind::UnactivatedEmpty,
            fault != 0,
        );
        if fault == 3 {
            let r = owner.recovery().unwrap();
            let m = r.manifest().encode();
            put(&r.store, IngestKey::Manifest, m.as_bytes()); // same bytes, unknown sequence
        }
        if fault == 4 {
            stored_input(&f, 3, 1, b"unadmitted");
        }
        let before = snapshot(&f, &owner.recovery().unwrap().store);
        let cancelled = AtomicBool::new(fault == 2);
        let calls = std::cell::Cell::new(0);
        let result = owner.activate(&cancelled, || {
            calls.set(calls.get() + 1);
            if fault == 1 {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert_eq!(calls.get(), usize::from(fault == 1));
        assert_eq!(snapshot(&f, &owner.recovery().unwrap().store), before);
        assert!(matches!(
            owner.activate(&NOT_CANCELLED, || Ok(())),
            Err(BootstrapError::AuthorityUnavailable)
        ));
        assert_eq!(snapshot(&f, &owner.recovery().unwrap().store), before);
    }
}

#[test]
fn successor_writer_no_fact_request_one_uses_older_than_predecessor_frontier() {
    for rejected in [true, false] {
        let f = prepared_fixture();
        let mut writer = active(&f, false);
        let before = snapshot(&f, writer.store());
        let input = event(&f, 3, 1, rejected);
        let permit = grant(&f.budget, 40 * MIB, 10 * MIB);
        writer
            .stage_event(&input, AdmissionLimits::default(), &permit, &NOT_CANCELLED)
            .unwrap();
        let result = writer
            .admit_event(
                input,
                ObservedTime::new(77),
                false,
                AdmissionLimits::default(),
                permit,
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
        if rejected {
            assert!(matches!(
                result.outcome(),
                AdmissionOutcome::Rejected(RejectionReason::Signature)
            ));
        } else {
            let AdmissionOutcome::Durable(receipt) = result.outcome() else {
                panic!("retained signed duplicate")
            };
            assert!(receipt.batch().is_none());
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
            assert_eq!(receipt.protection_context().topology_generation.get(), 3);
            assert_eq!(receipt.protection_context().policy_epoch.get(), 7);
        }
        drop(result);
        let mut wanted = before.0.clone();
        replace(&mut wanted, producer_key(3), &marker(false, 1));
        // Literal outcome framing: hash the retained input, never the emitted
        // outcome. Dependency hashes come from unchanged old signed fact bytes.
        let retained =
            std::fs::read(f.path().join("producer-inputs-v1").join(input_name(3, 1))).unwrap();
        let input_hash = digest(&retained);
        let outcome = if rejected {
            [
                b"bigbrotr/rejection/1\0".to_vec(),
                raw_request(3, 1),
                input_hash,
                vec![1; 16],
                7_u64.to_be_bytes().to_vec(),
                vec![4],
            ]
            .concat()
        } else {
            let dependencies: Vec<_> = [1, 2, 3, 6, 7, 8, 9]
                .into_iter()
                .map(|n| {
                    let key = [vec![1, 32], raw_source(1, Some(n))].concat();
                    let (_, _, fact) = before
                        .0
                        .iter()
                        .find(|(cf, k, _)| cf == "facts" && *k == key)
                        .unwrap();
                    assert_eq!(&fact[2..34], digest(&fact[34..]));
                    array(&[source(1, n), bytes(&fact[2..34])])
                })
                .collect();
            [
                b"bigbrotr/receipt/1\0".to_vec(),
                array(&[
                    uint(2),
                    array(&[bytes(&[7; 16]), uint(3), uint(1)]),
                    bytes(&input_hash),
                    array(&[bytes(&[1; 16]), uint(3), uint(1), bytes(&[6; 16]), uint(7)]),
                    vec![0xf6],
                    array(&dependencies),
                ]),
            ]
            .concat()
        };
        replace(
            &mut wanted,
            IngestKey::Request {
                request_id: at(3, 1),
            },
            &outcome,
        );
        assert_eq!(kv(writer.store()), wanted);
        assert_eq!(
            value(writer.store(), source_key(3)),
            [vec![1, 2], vec![0; 8]].concat()
        );
        assert_eq!(writer.store().db.latest_sequence_number(), before.1 + 2);
        let complete = snapshot(&f, writer.store());
        drop(
            writer
                .admit_event(
                    event(&f, 3, 1, rejected),
                    ObservedTime::new(999),
                    false,
                    AdmissionLimits::default(),
                    grant(&f.budget, 40 * MIB, 10 * MIB),
                    &NOT_CANCELLED,
                    || Ok(()),
                )
                .unwrap(),
        );
        assert_eq!(snapshot(&f, writer.store()), complete);
        append_and_check(&f, &mut writer, 2);
    }
}

#[test]
fn successor_writer_refuses_reopening_and_ordinary_store_entrypoints() {
    let f = prepared_fixture();
    let writer = active(&f, false);
    let target = *writer.manifest();
    let intent = dictionary(scope(&writer), "writer-public");
    let before = snapshot(&f, writer.store());
    // No private mutable store extraction exists on the writer. Reopening gets
    // an observation only, even with the exact target manifest and retained rows.
    drop(writer);
    let mut reopened =
        IngestStore::reopen(&f.path(), target, f.native, grant(&f.budget, 0, 128 * MIB)).unwrap();
    assert!(matches!(
        reopened.append_dictionaries(
            at(3, 1),
            &intent,
            ObservedTime::new(77),
            AdmissionLimits::default(),
            operation(&f),
            || Ok(())
        ),
        Err(AdmissionError::InvalidInput)
    ));
    assert!(matches!(
        reopened.stage_successor_dictionaries(
            at(3, 1),
            &intent,
            ObservedTime::new(77),
            AdmissionLimits::default(),
            &operation(&f),
            &NOT_CANCELLED
        ),
        Err(AdmissionError::InvalidInput)
    ));
    assert!(matches!(
        event(&f, 3, 1, true).stage_successor(
            &mut reopened,
            AdmissionLimits::default(),
            &grant(&f.budget, 40 * MIB, 10 * MIB),
            &NOT_CANCELLED
        ),
        Err(AdmissionError::InvalidInput)
    ));
    assert_eq!(snapshot(&f, &reopened), before);
    drop(reopened);
    let candidate = SuccessorCandidate::new(f.reopen()).unwrap();
    let lineage = inspect(&f, candidate.recovery(), None).unwrap();
    assert!(
        replay(
            &f,
            candidate.recovery(),
            &lineage,
            3,
            Some(PredecessorTerminalKind::Activated)
        )
        .is_err()
    );
    assert_eq!(snapshot(&f, &candidate.recovery().store), before);
}

#[test]
fn successor_writer_staging_census_refuses_unknown_native_and_sidecar_changes() {
    for fault in 0..5 {
        let f = prepared_fixture();
        let mut writer = active(&f, false);
        let input = event(&f, 3, 1, true);
        let permit = grant(&f.budget, 40 * MIB, 10 * MIB);
        writer
            .stage_event(&input, AdmissionLimits::default(), &permit, &NOT_CANCELLED)
            .unwrap();
        match fault {
            0 => {
                let m = writer.manifest().encode();
                put(writer.store(), IngestKey::Manifest, m.as_bytes());
            }
            1 => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            2 => {
                let path = f.path().join("producer-inputs-v1").join(input_name(3, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            3 => stored_input(&f, 3, 2, b"unknown later request"),
            _ => stored_input(&f, 4, 1, b"unknown future lifetime"),
        }
        let changed = snapshot(&f, writer.store());
        assert!(
            writer
                .admit_event(
                    input,
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    permit,
                    &NOT_CANCELLED,
                    || Ok(())
                )
                .is_err()
        );
        assert_eq!(snapshot(&f, writer.store()), changed);
        assert_eq!(
            writer.validate(&NOT_CANCELLED),
            Err(AdmissionError::AuthorityUnavailable)
        );
    }
}

#[test]
fn successor_writer_bounds_fact_frontier_and_cancelled_staging_without_writes() {
    for fault in 0..4 {
        let f = prepared_fixture();
        let mut writer = active(&f, false);
        let before = snapshot(&f, writer.store());
        if fault == 0 {
            let cancelled = AtomicBool::new(true);
            assert!(
                writer
                    .stage_event(
                        &event(&f, 3, 1, true),
                        AdmissionLimits::default(),
                        &grant(&f.budget, 40 * MIB, 10 * MIB),
                        &cancelled
                    )
                    .is_err()
            );
        } else {
            let mut id = SourceRecordId {
                dataset_id: f.previous.identity().dataset_id,
                shard_id: f.previous.identity().shard_id,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
                sequence: Sequence::new(9).unwrap(),
            };
            writer
                .store()
                .live_successor
                .as_ref()
                .unwrap()
                .require_fact(writer.store(), id)
                .unwrap();
            match fault {
                1 => id.sequence = Sequence::new(10).unwrap(),
                2 => id.owner_epoch = OwnerEpoch::new(2).unwrap(),
                _ => id.owner_epoch = OwnerEpoch::new(3).unwrap(),
            }
            assert_eq!(
                writer
                    .store()
                    .live_successor
                    .as_ref()
                    .unwrap()
                    .require_fact(writer.store(), id),
                Err(AdmissionError::StateMismatch)
            );
        }
        assert_eq!(snapshot(&f, writer.store()), before);
    }
}

#[test]
fn successor_writer_preserves_full_spool_and_refuses_sixty_fifth_input() {
    let f = zero_fixture(64);
    let mut writer = active(&f, false);
    let before = snapshot(&f, writer.store());
    assert_eq!(before.2.len(), 64);
    assert!(matches!(
        writer.stage_event(
            &event(&f, 3, 1, true),
            AdmissionLimits::default(),
            &grant(&f.budget, 40 * MIB, 10 * MIB),
            &NOT_CANCELLED
        ),
        Err(AdmissionError::Limit)
    ));
    assert_eq!(snapshot(&f, writer.store()), before);
    writer.validate(&NOT_CANCELLED).unwrap();
}

#[test]
fn successor_writer_new_event_extends_current_frontier_and_certifies_both_generations() {
    let f = prepared_fixture();
    let mut writer = active(&f, false);
    let before = snapshot(&f, writer.store());
    let supplied = intent("B_note", 2, 10_000_000);
    assert_ne!(fixture_event("B_note").id, fixture_event("E_private").id);
    let prepared =
        PreparedEvent::new(at(3, 1), supplied, 256 * 1024, grant(&f.budget, MIB, 0)).unwrap();
    let result = writer
        .admit_event(
            prepared,
            ObservedTime::new(77),
            false,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 10 * MIB),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    let AdmissionOutcome::Durable(receipt) = result.outcome() else {
        panic!("new signed event")
    };
    let batch = receipt.batch().unwrap();
    assert_eq!(
        (
            batch.first().owner_epoch.get(),
            batch.first().sequence.get(),
            batch.last().sequence.get()
        ),
        (3, 1, 4)
    );
    let expected_receipt = receipt.encode_persistent(8192).unwrap();
    drop(result);
    assert_eq!(
        value(writer.store(), source_key(3)),
        [vec![1, 2], 4_u64.to_be_bytes().to_vec()].concat()
    );
    assert_eq!(value(writer.store(), producer_key(3)), marker(false, 1));
    for row in before.0.iter().filter(|row| {
        row.1 != source_key(3).encode().unwrap().as_bytes()
            && row.1 != producer_key(3).encode().unwrap().as_bytes()
    }) {
        assert!(kv(writer.store()).contains(row));
    }
    let after = snapshot(&f, writer.store());
    let repeated = PreparedEvent::new(
        at(3, 1),
        intent("B_note", 2, 10_000_000),
        256 * 1024,
        grant(&f.budget, MIB, 0),
    )
    .unwrap();
    let repeated = writer
        .admit_event(
            repeated,
            ObservedTime::new(999),
            false,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 10 * MIB),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    let AdmissionOutcome::Durable(receipt) = repeated.outcome() else {
        panic!("same event receipt")
    };
    assert_eq!(receipt.encode_persistent(8192).unwrap(), expected_receipt);
    drop(repeated);
    for name in ["E_private", "B_note"] {
        let prepared = crate::storage::rocks::store::PreparedCertification::new(
            intent(name, 2, 10_000_000),
            256 * 1024,
            grant(&f.budget, MIB, 0),
        )
        .unwrap();
        let limits = crate::storage::rocks::store::CertificationLimits::default();
        let result = writer
            .certify_event(
                prepared,
                false,
                limits,
                grant(&f.budget, limits.required_data_bytes().unwrap(), 0),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
        assert!(matches!(
            result.outcome(),
            crate::storage::rocks::store::CertificationOutcome::Existing(_)
        ));
    }
    assert_eq!(snapshot(&f, writer.store()), after);
}

#[test]
fn successor_writer_borrowed_preparation_rechecks_same_sequence_input_content() {
    let f = prepared_fixture();
    let mut writer = active(&f, false);
    let reference = reference(DictionaryKind::VisibilityScope, 1);
    let limits = crate::storage::rocks::store::CertificationLimits::default();
    let visibility = writer
        .read_visibility(
            reference,
            limits,
            grant(&f.budget, limits.required_data_bytes().unwrap(), 0),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(visibility.entry().reference(), reference);
    drop(visibility);
    let limits = AdmissionLimits::default();
    let descriptor = || {
        DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
            format: label("successor-component"),
            format_version: 1,
            source_dataset_id: None,
            source_artifact_digest: Digest256::from_bytes([0x63; 32]),
        })
    };
    let mut preparation = writer
        .prepare_dictionaries(
            reference,
            limits,
            grant(
                &f.budget,
                limits.required_dictionary_preparation_data_bytes().unwrap(),
                0,
            ),
            &NOT_CANCELLED,
        )
        .unwrap();
    let selected = preparation.intern(descriptor()).unwrap();
    assert_eq!(
        selected.allocation_domain,
        writer
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::DatasetLineage)
            .unwrap()
    );
    assert_eq!(selected.ordinal.get(), 1);
    drop(preparation.finish().unwrap());
    let mut preparation = writer
        .prepare_dictionaries(
            reference,
            limits,
            grant(
                &f.budget,
                limits.required_dictionary_preparation_data_bytes().unwrap(),
                0,
            ),
            &NOT_CANCELLED,
        )
        .unwrap();
    preparation.intern(descriptor()).unwrap();
    let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let changed = snapshot(&f, writer.store());
    assert!(preparation.finish().is_err());
    assert_eq!(snapshot(&f, writer.store()), changed);
    assert_eq!(
        writer.validate(&NOT_CANCELLED),
        Err(AdmissionError::AuthorityUnavailable)
    );
}
