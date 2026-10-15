//! Structural observations only. The test-only later handoffs deliberately do
//! not establish SQL terminal authority or a positively replayed frontier.
use super::*;
use crate::storage::rocks::store::{RetainedLineage, RetainedTerminalKind};
use crate::storage::rocks::succession::{
    PredecessorTerminalKind, SuccessionFields, SuccessionProfile, SuccessionRecord,
};
use std::sync::atomic::AtomicBool;

fn inspect(
    f: &Fixture,
    r: &IngestRecovery,
    pending: Option<u64>,
) -> Result<RetainedLineage, BootstrapError> {
    RetainedLineage::inspect(
        r,
        pending,
        grant(&f.budget, 256 * 1024, 0),
        &AtomicBool::new(false),
    )
}

fn producer_key(epoch: u64) -> IngestKey {
    IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(epoch).unwrap(),
    }
}

fn marker(closing: bool, issued: u64) -> Vec<u8> {
    let mut bytes = vec![if closing { 2 } else { 1 }, 0x31];
    bytes.extend_from_slice(&issued.to_be_bytes());
    bytes.extend_from_slice(&[0; 16]);
    if closing {
        bytes.push(1);
    }
    bytes
}

fn later_manifest(previous: IngestManifest, epoch: u64) -> IngestManifest {
    let mut identity = *previous.identity();
    identity.owner_epoch = OwnerEpoch::new(epoch).unwrap();
    identity.topology_generation = TopologyGeneration::new(epoch);
    let mut incarnation = [0; 16];
    incarnation[..8].copy_from_slice(&epoch.to_be_bytes());
    incarnation[15] = 0xf0;
    identity.process_incarnation = incarnation.try_into().unwrap();
    let namespaces: Vec<_> = (1..=9)
        .filter_map(|n| {
            let kind = DictionaryKind::try_from(n).unwrap();
            previous.allocation_namespaces().domain(kind).map(|_| {
                let mut domain = [0; 16];
                domain[..8].copy_from_slice(&epoch.to_be_bytes());
                domain[15] = n as u8;
                (kind, domain.try_into().unwrap())
            })
        })
        .collect();
    IngestManifest::for_repeatable_successor(
        identity,
        AllocationNamespaces::from_entries(&namespaces).unwrap(),
    )
}

fn extend_to(f: &Fixture, last: u64) -> IngestRecovery {
    let r = f.reopen();
    let mut previous = *r.manifest();
    let mut incoming: [u8; 32] = Sha256::digest(value(&r.store, handoff_key())).into();
    for epoch in 3..=last {
        let next = later_manifest(previous, epoch);
        let record = SuccessionRecord::new(SuccessionFields {
            terminal_kind: PredecessorTerminalKind::UnactivatedEmpty,
            producer_slot_id: request(7, 1).producer_slot_id,
            predecessor_epoch: ProducerEpoch::new(epoch - 1).unwrap(),
            issued_through: 0,
            last_complete_sequence: 0,
            closure_digest: [0x7a; 32], // a claim, deliberately not a positive proof
            incoming_handoff_sha256: incoming,
            previous_manifest: previous,
            next_manifest: next,
        })
        .unwrap();
        put(&r.store, producer_key(epoch - 1), &marker(true, 0));
        put(&r.store, record.key(), record.encode().as_bytes());
        put(&r.store, producer_key(epoch), &marker(false, 0));
        put(&r.store, source_key(epoch), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        put(&r.store, IngestKey::Manifest, next.encode().as_bytes());
        incoming = Sha256::digest(record.encode().as_bytes()).into();
        previous = next;
    }
    drop(r);
    f.reopen()
}

fn refusal_preserves(f: &Fixture, r: &IngestRecovery) {
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    assert!(inspect(f, r, None).is_err());
    assert_eq!(kv(&r.store), before);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&f.path()), originals);
}

#[test]
fn lineage_observes_exact_genesis_and_pending_target_without_authority() {
    let f = prepared_fixture();
    let r = f.reopen();
    let before = kv(&r.store);
    let originals = inputs(&f.path());
    let sequence = r.store.db.latest_sequence_number();
    let lineage = inspect(&f, &r, Some(3)).unwrap();
    assert_eq!(lineage.profile().registered_epochs(), 3);
    assert_eq!(lineage.pending_target(), Some(3));
    assert_eq!(lineage.entries().len(), 2);
    assert!(lineage.entry(0).is_none() && lineage.entry(3).is_none());
    let old = lineage.entry(1).unwrap();
    assert_eq!(old.manifest(), &f.previous);
    assert_eq!((old.issued_through(), old.source_through()), (4, 9));
    assert_eq!(old.incoming_handoff_sha256(), [0; 32]);
    assert_eq!(
        old.terminal_claim(),
        Some((RetainedTerminalKind::Initial, f.closure))
    );
    assert!(old.is_closing());
    let head = lineage.native_head();
    assert_eq!(head.manifest(), &f.next);
    assert_eq!((head.issued_through(), head.source_through()), (0, 0));
    assert_eq!(
        head.incoming_handoff_sha256(),
        <[u8; 32]>::from(Sha256::digest(value(&r.store, handoff_key())))
    );
    assert_eq!(head.terminal_claim(), None);
    assert!(!head.is_closing());
    lineage.validate(&r, &AtomicBool::new(false)).unwrap();
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
    let old_only = SuccessionProfile::for_recovery(2, None)
        .unwrap()
        .required_metadata_bytes();
    assert!(matches!(
        RetainedLineage::inspect(
            &r,
            Some(3),
            grant(&f.budget, old_only, 0),
            &AtomicBool::new(false)
        ),
        Err(BootstrapError::InsufficientReservation)
    ));
}

#[test]
fn lineage_follows_all_parent_bytes_and_rejects_older_namespace_reuse() {
    let f = prepared_fixture();
    let r = extend_to(&f, 4);
    let lineage = inspect(&f, &r, None).unwrap();
    assert_eq!(lineage.entries().len(), 4);
    assert_eq!(lineage.entry(1).unwrap().source_through(), 9);
    assert_eq!(
        lineage.entry(2).unwrap().terminal_claim(),
        Some((RetainedTerminalKind::UnactivatedEmpty, [0x7a; 32]))
    );
    drop(lineage);
    let key = IngestKey::OwnerHandoff {
        dataset_id: f.previous.identity().dataset_id,
        owner_resource_id: f.previous.identity().owner_resource_id,
        predecessor_epoch: OwnerEpoch::new(3).unwrap(),
    };
    let bytes = value(&r.store, key);
    let record = SuccessionRecord::decode(&bytes).unwrap();
    let mut fields = *record.fields();
    fields.incoming_handoff_sha256[0] ^= 1;
    put(
        &r.store,
        key,
        SuccessionRecord::new(fields).unwrap().encode().as_bytes(),
    );
    refusal_preserves(&f, &r);
    put(&r.store, key, &bytes);
    drop(r);
    let r = f.reopen();
    let mut fields = *record.fields();
    fields.next_manifest = IngestManifest::for_repeatable_successor(
        *fields.next_manifest.identity(),
        *f.previous.allocation_namespaces(),
    );
    // Locally fresh relative to epoch3; illegally aliases epoch1 globally.
    let altered = SuccessionRecord::new(fields).unwrap();
    put(&r.store, key, altered.encode().as_bytes());
    put(
        &r.store,
        IngestKey::Manifest,
        fields.next_manifest.encode().as_bytes(),
    );
    drop(r);
    let r = f.reopen();
    refusal_preserves(&f, &r);
}

#[test]
fn lineage_census_refuses_foreign_markers_holes_and_missing_physical_counters() {
    for fault in 0..8 {
        let f = prepared_fixture();
        let r = f.reopen();
        match fault {
            0 => delete(&r.store, source_key(1)),
            1 => delete(&r.store, source_key(2)),
            2 => put(&r.store, source_key(3), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0]),
            3 => put(
                &r.store,
                IngestKey::ProducerState {
                    producer_slot_id: request(8, 1).producer_slot_id,
                    producer_epoch: ProducerEpoch::new(2).unwrap(),
                },
                &marker(false, 0),
            ),
            4 => delete(
                &r.store,
                IngestKey::Fact {
                    source_record_id: fact_id(1, 5),
                },
            ),
            5 => delete(
                &r.store,
                IngestKey::PendingBatch {
                    first: fact_id(1, 5),
                },
            ),
            6 => delete(
                &r.store,
                IngestKey::Request {
                    request_id: request(7, 2),
                },
            ),
            7 => {
                r.store.db.put([0xff, 0xff], [1]).unwrap();
            }
            _ => unreachable!(),
        }
        refusal_preserves(&f, &r);
    }
}

#[test]
fn lineage_binds_root_sequence_and_sidecar_contents_separately() {
    let a = prepared_fixture();
    let b = prepared_fixture();
    let ra = a.reopen();
    let rb = b.reopen();
    assert_eq!(kv(&ra.store), kv(&rb.store));
    assert_eq!(
        ra.store.db.latest_sequence_number(),
        rb.store.db.latest_sequence_number()
    );
    let lineage = inspect(&a, &ra, None).unwrap();
    assert!(lineage.validate(&rb, &AtomicBool::new(false)).is_err());
    let path = a.path().join("producer-inputs-v1").join(input_name(1, 4));
    let mut altered = std::fs::read(&path).unwrap();
    let old_sequence = ra.store.db.latest_sequence_number();
    altered[0] ^= 1;
    std::fs::write(&path, &altered).unwrap();
    assert_eq!(ra.store.db.latest_sequence_number(), old_sequence);
    assert!(lineage.validate(&ra, &AtomicBool::new(false)).is_err());
    // A fresh observation can bind even unresolved/corrupt scientific bytes;
    // inspection alone has no positive replay conversion.
    drop(inspect(&a, &ra, None).unwrap());
    std::fs::write(&path, &a.originals[3].input).unwrap();
    let fresh = inspect(&a, &ra, None).unwrap();
    put(&ra.store, source_key(2), &value(&ra.store, source_key(2)));
    assert!(fresh.validate(&ra, &AtomicBool::new(false)).is_err());
}

#[test]
fn lineage_rejects_missing_foreign_and_future_original_files() {
    for fault in 0..4 {
        let f = prepared_fixture();
        let r = f.reopen();
        let directory = f.path().join("producer-inputs-v1");
        let old = directory.join(input_name(1, 4));
        match fault {
            0 => std::fs::remove_file(old).unwrap(),
            1 => {
                std::fs::copy(old, directory.join(input_name(2, 1))).unwrap();
            }
            2 => {
                std::fs::rename(
                    old,
                    directory.join(input_name(1, 4).replacen("07", "08", 1)),
                )
                .unwrap();
            }
            3 => {
                // Each file is admitted by the old per-file bound, but the
                // complete directory exceeds the independent 32MiB envelope.
                for request in [1, 2] {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(directory.join(input_name(1, request)))
                        .unwrap()
                        .set_len(16 * MIB)
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
        refusal_preserves(&f, &r);
    }
}

fn zero_fixture(issued: u64) -> Fixture {
    let (_unused, old, budget, previous, native) = setup(false);
    drop(old);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let mut store =
        IngestStore::initialize(&path, previous, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let mut raw = corpus["scenarios"][0]["operations"][4]["input"].clone();
    raw["observation"]["witness"] = serde_json::Value::Null;
    let mut originals = Vec::new();
    for sequence in 1..=issued {
        let event = fixture_intent(&raw);
        let input = event
            .encode_request_preimage(request(7, sequence), 8192)
            .unwrap();
        let prepared = PreparedEvent::new(
            request(7, sequence),
            event,
            256 * 1024,
            grant(&budget, MIB, 0),
        )
        .unwrap();
        let outcome = store
            .admit_event(
                prepared,
                ObservedTime::new(10_000_000),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        assert!(matches!(
            outcome.outcome(),
            AdmissionOutcome::Rejected(RejectionReason::Signature)
        ));
        drop(outcome);
        originals.push(Original {
            input,
            outcome: value(
                &store,
                IngestKey::Request {
                    request_id: request(7, sequence),
                },
            ),
        });
    }
    assert!(
        store
            .db
            .get(source_key(1).encode().unwrap().as_bytes())
            .unwrap()
            .is_none()
    );
    drop(store);
    let mut r =
        IngestRecovery::open(&path, previous, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let closed = r
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let mut source = closed.source_resolution(&r, issued).unwrap();
    for (index, original) in originals.iter().enumerate() {
        let resolved = r
            .resolve_original_request(
                request(7, index as u64 + 1),
                input_digest(original),
                recovery_limits(),
                grant(&budget, 3 * MIB, 0),
                grant(&budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(resolved.native_outcome().bytes(), original.outcome);
        source.record(&resolved).unwrap();
    }
    let proof = source.finish().unwrap();
    assert_eq!(proof.last_complete_sequence(), 0);
    let mut transcript = b"bigbrotr/initial-source-closure/1\0".to_vec();
    transcript.extend_from_slice(&Sha256::digest(previous.encode().as_bytes()));
    transcript.extend_from_slice(&[7; 16]);
    transcript.extend_from_slice(&1_u64.to_be_bytes());
    transcript.extend_from_slice(&issued.to_be_bytes());
    for (index, original) in originals.iter().enumerate() {
        transcript.extend_from_slice(&(index as u64 + 1).to_be_bytes());
        transcript.extend_from_slice(&Sha256::digest(&original.input));
        transcript.extend_from_slice(&Sha256::digest(&original.outcome));
    }
    transcript.extend_from_slice(&0_u64.to_be_bytes());
    let closure: [u8; 32] = Sha256::digest(transcript).into();
    assert_eq!(proof.digest(), closure);
    let next = successor(previous);
    let handoff = InitialHandoff::from_source(&proof, next).unwrap();
    drop(proof);
    let closed_rows = kv(&r.store);
    drop(
        r.apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || Ok(()))
            .unwrap(),
    );
    drop(r);
    Fixture {
        root,
        budget,
        native,
        previous,
        next,
        originals,
        closed_rows,
        closure,
    }
}

#[test]
fn lineage_preserves_legacy_zero_with_rejected_originals_and_full_spool() {
    for issued in [0, 1, 64] {
        let f = zero_fixture(issued);
        let r = f.reopen();
        let lineage = inspect(&f, &r, None).unwrap();
        let old = lineage.entry(1).unwrap();
        assert_eq!((old.issued_through(), old.source_through()), (issued, 0));
        assert_eq!(
            old.terminal_claim(),
            Some((RetainedTerminalKind::Initial, f.closure))
        );
        assert!(
            r.store
                .db
                .get(source_key(1).encode().unwrap().as_bytes())
                .unwrap()
                .is_none()
        );
        drop(lineage);
        if issued == 0 {
            let key = IngestKey::Fact {
                source_record_id: fact_id(1, 1),
            };
            put(&r.store, key, &[1, 0x20]);
            let before = kv(&r.store);
            assert!(inspect(&f, &r, None).is_err());
            assert_eq!(kv(&r.store), before);
            delete(&r.store, key);
        }
        if issued > 0 {
            let directory = f.path().join("producer-inputs-v1");
            std::fs::copy(
                directory.join(input_name(1, 1)),
                directory.join(input_name(2, 1)),
            )
            .unwrap();
            refusal_preserves(&f, &r);
        }
        delete(&r.store, source_key(2));
        assert!(inspect(&f, &r, None).is_err());
    }
}

#[test]
fn lineage_admits_actual_compact_255_entries_and_accounts_pending_before_reads() {
    let f = prepared_fixture();
    let r = extend_to(&f, 255);
    let lineage = inspect(&f, &r, None).unwrap();
    assert_eq!(lineage.entries().len(), 255);
    assert_eq!(lineage.profile().required_metadata_bytes(), 261_632);
    assert!(lineage.pending_target().is_none());
    drop(lineage);
    let before = kv(&r.store);
    assert!(matches!(
        inspect(&f, &r, Some(256)),
        Err(BootstrapError::InsufficientReservation)
    ));
    let bytes = SuccessionProfile::for_recovery(255, None)
        .unwrap()
        .required_metadata_bytes();
    assert!(matches!(
        RetainedLineage::inspect(
            &r,
            None,
            grant(&f.budget, bytes - 1, 0),
            &AtomicBool::new(false)
        ),
        Err(BootstrapError::InsufficientReservation)
    ));
    assert!(matches!(
        RetainedLineage::inspect(&r, None, grant(&f.budget, bytes, 0), &AtomicBool::new(true)),
        Err(BootstrapError::AuthorityUnavailable)
    ));
    assert_eq!(kv(&r.store), before);
}
