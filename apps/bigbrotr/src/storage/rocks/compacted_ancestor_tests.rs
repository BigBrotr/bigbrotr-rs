//! Synthetic reclamation over existing real native fixtures, not a GC writer.
use super::*;
use crate::storage::rocks::{
    keys::{ColumnFamily, decode_key},
    producer_floor::ClosedProducerFloor,
    store::{
        CompactedInitialAncestor, CompactedOutcomeRow, NativeOutcomeKind, NativeSuccessorRecovery,
        VerifiedCompactedPrefix,
    },
};

#[path = "compacted_history_tests.rs"]
mod compacted_history_tests;

fn floor_bytes(f: &Fixture) -> [u8; 139] {
    let mut bytes = [0; 139];
    bytes[..2].copy_from_slice(&[3, 0x31]);
    for at in [2, 10, 18] {
        bytes[at..at + 8].copy_from_slice(&4_u64.to_be_bytes());
    }
    bytes[26] = 1;
    bytes[27..59].copy_from_slice(&Sha256::digest(f.previous.encode().as_bytes()));
    bytes[59..67].copy_from_slice(&9_u64.to_be_bytes());
    let digest = expected_closure(f.previous.encode().as_bytes(), &f.originals);
    assert_eq!(digest, f.closure);
    bytes[67..99].copy_from_slice(&digest);
    bytes[99..107].copy_from_slice(&5_u64.to_be_bytes());
    bytes[107..].fill(0x45); // retained PG identity is a separate obligation
    bytes
}

fn install_floor(f: &Fixture, r: &IngestRecovery) {
    let cf = r.store.db.cf_handle("receipts").unwrap();
    let mut batch = rocksdb::WriteBatch::default();
    batch.put_cf(
        cf,
        producer_key(1).encode().unwrap().as_bytes(),
        floor_bytes(f),
    );
    for sequence in 1..=4 {
        batch.delete_cf(
            cf,
            IngestKey::Request {
                request_id: at(1, sequence),
            }
            .encode()
            .unwrap()
            .as_bytes(),
        );
    }
    r.store.db.write(batch).unwrap();
}

fn fixture() -> (Fixture, IngestRecovery) {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    // Reuse the later dictionary fixture: all indices now include real epoch3
    // state, so a singleton/through1 semantic scan would wrongly reject it.
    let scope = target_scope(&r);
    let entry = DictionaryEntry::new(
        scope,
        DictionaryDescriptor::Visibility {
            class: label("public"),
        },
    )
    .unwrap();
    let input = EvidenceIntent::new(
        1,
        scope,
        vec![entry.clone()],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    let fact = dictionary_bytes(&r, entry, scope, 1);
    let batch = AdmissionBatch::new(vec![fact]).unwrap();
    let dependencies = SourceDependencies::new(scope.dataset_id, vec![fact]).unwrap();
    for sequence in [1, 2] {
        let id = at(3, sequence);
        let receipt = if sequence == 1 {
            Receipt::new_facts(
                id,
                input.request_digest(id),
                context(*r.manifest()),
                batch.clone(),
                dependencies.clone(),
            )
        } else {
            Receipt::already_durable(
                id,
                input.request_digest(id),
                context(*r.manifest()),
                dependencies.clone(),
            )
        }
        .unwrap();
        put(
            &r.store,
            IngestKey::Request { request_id: id },
            &receipt.encode_persistent(8192).unwrap(),
        );
        stored_input(
            &f,
            3,
            sequence,
            &input.encode_request_preimage(id, 8192).unwrap(),
        );
    }
    put(
        &r.store,
        IngestKey::PendingBatch {
            first: fact_id(3, 1),
        },
        &PendingBatchHeader::from_batch(&batch).encode(),
    );
    put(&r.store, source_key(3), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 1]);
    put(&r.store, producer_key(3), &marker(true, 2));
    install_floor(&f, &r);
    (f, r)
}

fn inspect_compacted(f: &Fixture, r: &IngestRecovery) -> Result<RetainedLineage, BootstrapError> {
    RetainedLineage::inspect_compacted_initial(
        r,
        None,
        grant(&f.budget, 256 * 1024, 0),
        &NOT_CANCELLED,
    )
}
fn ancestor<'a>(
    f: &Fixture,
    r: &'a IngestRecovery,
    lineage: &'a RetainedLineage,
) -> Result<CompactedInitialAncestor<'a>, AdmissionError> {
    lineage
        .compacted_initial_ancestor(
            r,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 0),
            &NOT_CANCELLED,
        )
        .map(|view| view.expect("compacted fixture"))
}
fn row(f: &Fixture, index: usize) -> CompactedOutcomeRow<'_> {
    let original = &f.originals[index];
    CompactedOutcomeRow {
        request: at(1, index as u64 + 1),
        digest: input_digest(original),
        kind: if index == 3 {
            NativeOutcomeKind::AlreadyDurable
        } else {
            NativeOutcomeKind::NewFacts
        },
        policy_epoch: PolicyEpoch::new(1),
        bytes: &original.outcome,
        input_bytes: original.input.len() as u32,
    }
}
fn resolve<'a>(
    f: &Fixture,
    view: &CompactedInitialAncestor<'a>,
) -> Result<VerifiedCompactedPrefix<'a>, AdmissionError> {
    let mut resolution = view.resolution(&NOT_CANCELLED)?;
    let permit = grant(&f.budget, 40 * MIB, 0);
    for index in 0..4 {
        resolution.record(
            row(f, index),
            AdmissionLimits::default(),
            &permit,
            &NOT_CANCELLED,
            || Ok(()),
        )?;
    }
    resolution.finish(&NOT_CANCELLED)
}
fn remove(r: &IngestRecovery, key: IngestKey) {
    let encoded = key.encode().unwrap();
    r.store
        .db
        .delete_cf(
            r.store
                .db
                .cf_handle(encoded.column_family().name())
                .unwrap(),
            encoded.as_bytes(),
        )
        .unwrap();
}

#[test]
fn compacted_ancestor_reads_full_later_census_and_all_residual_shapes_without_writes() {
    let (f, r) = fixture();
    for remaining in [4, 2, 0] {
        for sequence in remaining + 1..=4 {
            let path = f
                .path()
                .join("producer-inputs-v1")
                .join(input_name(1, sequence));
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        let before = kv(&r.store);
        let originals = inputs(&f.path());
        let sequence = r.store.db.latest_sequence_number();
        let lineage = inspect_compacted(&f, &r).unwrap();
        assert_eq!(lineage.entries().len(), 3);
        assert_eq!(lineage.native_head().source_through(), 1);
        let view = ancestor(&f, &r, &lineage).unwrap();
        assert_eq!(view.manifest(), &f.previous);
        assert_ne!(view.manifest(), r.manifest());
        assert_eq!(
            view.floor(),
            &ClosedProducerFloor::decode(&floor_bytes(&f)).unwrap()
        );
        assert_eq!(view.producer_slot_id(), request(7, 1).producer_slot_id);
        let verified = resolve(&f, &view).unwrap();
        assert_eq!(verified.manifest(), &f.previous);
        verified.validate(&r, &NOT_CANCELLED).unwrap();
        // No original replay, successor plan, ACK or writer follows this view.
        assert!(
            lineage
                .resolution(
                    &r,
                    3,
                    Some(PredecessorTerminalKind::Activated),
                    &NOT_CANCELLED
                )
                .is_err()
        );
        assert!(inspect(&f, &r, None).is_err());
        assert!(
            NativeSuccessorRecovery::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED)
                .is_err()
        );
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&f.path()), originals);
    }
}

#[test]
fn compacted_ancestor_requires_exact_initial_anchor_and_refuses_later_floors() {
    for fault in 0..8 {
        let (f, r) = fixture();
        let mut floor = floor_bytes(&f);
        match fault {
            0 => floor[27..59].copy_from_slice(&Sha256::digest(r.manifest().encode().as_bytes())),
            1 => floor[67] ^= 1,
            2 => floor[66] ^= 1,
            3 => {
                let mut bytes = value(&r.store, handoff_key());
                bytes[42] ^= 1; // initial handoff closure digest
                put(&r.store, handoff_key(), &bytes);
            }
            4 | 5 => put(&r.store, producer_key((fault - 2) as u64), &floor),
            6 => {
                floor[9] = 0;
                floor[17] = 0;
                floor[25] = 0;
            }
            7 => {
                floor[9] = 3;
                floor[17] = 3;
                floor[25] = 3;
            }
            _ => unreachable!(),
        }
        if matches!(fault, 0..=2 | 6 | 7) {
            put(&r.store, producer_key(1), &floor);
        }
        let before = kv(&r.store);
        assert!(inspect_compacted(&f, &r).is_err());
        assert_eq!(kv(&r.store), before);
    }
    let f = prepared_fixture();
    let r = f.reopen();
    assert!(inspect_compacted(&f, &r).is_err()); // absence is not compacted proof
    inspect(&f, &r, None).unwrap();
}

#[test]
fn compacted_ancestor_census_covers_epoch_two_and_later_correction_ancestry() {
    let f = prepared_fixture();
    let r = f.reopen();
    install_floor(&f, &r);
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    let lineage = inspect_compacted(&f, &r).unwrap();
    assert_eq!(lineage.entries().len(), 2);
    resolve(&f, &ancestor(&f, &r, &lineage).unwrap()).unwrap();
    assert_eq!(kv(&r.store), before);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&f.path()), originals);

    for linked in [true, false] {
        let f = prepared_fixture();
        let r = valid_empty_third(&f);
        correction_with_link(&f, &r, false, linked);
        install_floor(&f, &r);
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let originals = inputs(&f.path());
        let lineage = inspect_compacted(&f, &r).unwrap();
        if linked {
            // Old canonical receipts still point at the original receive fact;
            // the full current census establishes its later correction chain.
            resolve(&f, &ancestor(&f, &r, &lineage).unwrap()).unwrap();
        } else {
            assert!(ancestor(&f, &r, &lineage).is_err());
        }
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&f.path()), originals);
    }
}

#[test]
fn compacted_ancestor_requires_pending_science_and_later_originals() {
    for fault in 0..5 {
        let (f, r) = fixture();
        match fault {
            0 => remove(
                &r,
                IngestKey::PendingBatch {
                    first: fact_id(1, 1),
                },
            ),
            1 => remove(
                &r,
                IngestKey::PendingBatch {
                    first: fact_id(3, 1),
                },
            ),
            2 => remove(
                &r,
                IngestKey::Dictionary {
                    reference: target_scope(&r),
                },
            ),
            3 => remove(
                &r,
                IngestKey::Fact {
                    source_record_id: fact_id(3, 1),
                },
            ),
            4 => std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(3, 1)))
                .unwrap(),
            _ => unreachable!(),
        }
        let before = kv(&r.store);
        let originals = inputs(&f.path());
        assert!(inspect_compacted(&f, &r).is_err());
        assert_eq!(kv(&r.store), before);
        assert_eq!(inputs(&f.path()), originals);
    }
    let (f, r) = fixture();
    let rows = kv(&r.store);
    let (_, index, mut pointer) = rows
        .iter()
        .find(|(cf, key, _)| {
            cf == "default"
                && matches!(
                    decode_key(ColumnFamily::Default, key),
                    Ok(IngestKey::EventPayload { .. })
                )
        })
        .unwrap()
        .clone();
    let dictionary = rows
        .iter()
        .find(|(cf, key, _)| {
            cf == "default"
                && matches!(
                    decode_key(ColumnFamily::Default, key),
                    Ok(IngestKey::Dictionary { .. })
                )
        })
        .unwrap();
    pointer[2..].copy_from_slice(&dictionary.2[2..]);
    r.store.db.put(&index, pointer).unwrap();
    let before = kv(&r.store);
    let lineage = inspect_compacted(&f, &r).unwrap(); // structure still closes
    assert!(ancestor(&f, &r, &lineage).is_err()); // semantic index subject does not
    assert_eq!(kv(&r.store), before);
}

#[test]
fn compacted_ancestor_binds_root_sequence_sidecars_and_native_uncertainty() {
    let (f, mut r) = fixture();
    let (_other, other) = fixture();
    let lineage = inspect_compacted(&f, &r).unwrap();
    let view = ancestor(&f, &r, &lineage).unwrap();
    assert_eq!(kv(&r.store), kv(&other.store));
    assert_eq!(
        r.store.db.latest_sequence_number(),
        other.store.db.latest_sequence_number()
    );
    assert!(view.validate(&other, &NOT_CANCELLED).is_err());
    assert_eq!(
        view.validate(&r, &AtomicBool::new(true)),
        Err(AdmissionError::AuthorityUnavailable)
    );
    let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(view.validate(&r, &NOT_CANCELLED).is_err());
    let fresh = inspect_compacted(&f, &r).unwrap();
    assert!(resolve(&f, &ancestor(&f, &r, &fresh).unwrap()).is_err());
    bytes[0] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    let fresh = inspect_compacted(&f, &r).unwrap();
    fresh.validate(&r, &NOT_CANCELLED).unwrap();
    put(&r.store, producer_key(3), &marker(true, 2)); // equal bytes, new sequence
    assert!(fresh.validate(&r, &NOT_CANCELLED).is_err());
    r.store.write_uncertain = true;
    assert!(matches!(
        inspect_compacted(&f, &r),
        Err(BootstrapError::CommitUnknown)
    ));
}

#[test]
fn compacted_ancestor_requires_grants_cancellation_and_complete_ordered_rows() {
    let (f, r) = fixture();
    assert!(matches!(
        RetainedLineage::inspect_compacted_initial(
            &r,
            None,
            grant(&f.budget, 4096, 0),
            &NOT_CANCELLED
        ),
        Err(BootstrapError::InsufficientReservation)
    ));
    assert!(matches!(
        RetainedLineage::inspect_compacted_initial(
            &r,
            None,
            grant(&f.budget, 256 * 1024, 0),
            &AtomicBool::new(true)
        ),
        Err(BootstrapError::AuthorityUnavailable)
    ));
    let lineage = inspect_compacted(&f, &r).unwrap();
    assert!(matches!(
        lineage.compacted_initial_ancestor(
            &r,
            AdmissionLimits::default(),
            grant(&f.budget, 4096, 0),
            &NOT_CANCELLED
        ),
        Err(AdmissionError::InsufficientReservation)
    ));
    assert!(matches!(
        lineage.compacted_initial_ancestor(
            &r,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 0),
            &AtomicBool::new(true)
        ),
        Err(AdmissionError::AuthorityUnavailable)
    ));
    let view = ancestor(&f, &r, &lineage).unwrap();
    assert!(
        view.resolution(&NOT_CANCELLED)
            .unwrap()
            .finish(&NOT_CANCELLED)
            .is_err()
    );
    let mut cursor = view.resolution(&NOT_CANCELLED).unwrap();
    assert_eq!(
        cursor.record(
            row(&f, 0),
            AdmissionLimits::default(),
            &grant(&f.budget, 4096, 0),
            &NOT_CANCELLED,
            || Ok(())
        ),
        Err(AdmissionError::InsufficientReservation)
    );
    let permit = grant(&f.budget, 40 * MIB, 0);
    assert_eq!(
        cursor.record(
            row(&f, 0),
            AdmissionLimits::default(),
            &permit,
            &NOT_CANCELLED,
            || Err(AdmissionError::AuthorityUnavailable)
        ),
        Err(AdmissionError::AuthorityUnavailable)
    );
    assert!(
        cursor
            .record(
                row(&f, 1),
                AdmissionLimits::default(),
                &permit,
                &NOT_CANCELLED,
                || Ok(())
            )
            .is_err()
    );
    for index in 0..4 {
        cursor
            .record(
                row(&f, index),
                AdmissionLimits::default(),
                &permit,
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    cursor.finish(&NOT_CANCELLED).unwrap();
}

#[test]
fn compacted_later_ancestor_cannot_mint_initial_handoff() {
    let (f, r) = fixture();
    let lineage = inspect_compacted(&f, &r).unwrap();
    let view = ancestor(&f, &r, &lineage).unwrap();
    let verified = resolve(&f, &view).unwrap();
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    assert!(InitialHandoff::from_compacted(&verified, f.next, &NOT_CANCELLED).is_err());
    assert_eq!(kv(&r.store), before);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}
