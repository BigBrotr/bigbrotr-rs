//! Synthetic floors exercise readers; the child kernel controls separately
//! exercise real reclamation mechanics without claiming PostgreSQL authority.
use super::*;
use crate::storage::rocks::{
    keys::IngestKey,
    producer_floor::{ClosedProducerFloor, ClosedProducerFloorFields},
};
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicBool;

struct Row {
    digest: Digest256,
    bytes: Vec<u8>,
    kind: NativeOutcomeKind,
    input_bytes: u32,
}
struct Fixture {
    recovery: IngestRecovery,
    budget: ResourceBudget,
    rows: Vec<Row>,
    floor: ClosedProducerFloor,
    root: tempfile::TempDir,
}
fn producer_key() -> IngestKey {
    IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
    }
}
fn put(recovery: &IngestRecovery, key: IngestKey, bytes: &[u8]) {
    let key = key.encode().unwrap();
    recovery
        .store
        .db
        .put_cf(
            recovery
                .store
                .db
                .cf_handle(key.column_family().name())
                .unwrap(),
            key.as_bytes(),
            bytes,
        )
        .unwrap();
}
fn delete(recovery: &IngestRecovery, key: IngestKey) {
    let key = key.encode().unwrap();
    recovery
        .store
        .db
        .delete_cf(
            recovery
                .store
                .db
                .cf_handle(key.column_family().name())
                .unwrap(),
            key.as_bytes(),
        )
        .unwrap();
}
fn input_path(root: &std::path::Path, n: u64) -> std::path::PathBuf {
    root.join("db/producer-inputs-v1").join(format!(
        "{}-{:016x}-{:016x}.cbor",
        "07".repeat(16),
        1,
        n
    ))
}
fn fixture() -> Fixture {
    let (root, mut store, budget, manifest, native) = setup(false);
    for n in 2..=4 {
        let input = if n == 4 {
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
        let result = store
            .admit_event(
                PreparedEvent::new(request(7, n), input, 256 * 1024, grant(&budget, MIB, 0))
                    .unwrap(),
                ObservedTime::new(10_000_000),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        if n == 4 {
            assert_eq!(
                result.outcome(),
                &AdmissionOutcome::Rejected(RejectionReason::Signature)
            );
        }
    }
    let kinds = [
        NativeOutcomeKind::NewFacts,
        NativeOutcomeKind::NewFacts,
        NativeOutcomeKind::AlreadyDurable,
        NativeOutcomeKind::Rejected,
    ];
    let rows = (1..=4)
        .zip(kinds)
        .map(|(n, kind)| {
            let input = std::fs::read(input_path(root.path(), n)).unwrap();
            let key = IngestKey::Request {
                request_id: request(7, n),
            }
            .encode()
            .unwrap();
            Row {
                digest: Digest256::from_bytes(Sha256::digest(&input).into()),
                bytes: store
                    .db
                    .get_cf(store.db.cf_handle("receipts").unwrap(), key.as_bytes())
                    .unwrap()
                    .unwrap(),
                kind,
                input_bytes: input.len() as u32,
            }
        })
        .collect::<Vec<_>>();
    // Independent literal closure grammar, not SourceResolution or the new reducer.
    let manifest_hash: [u8; 32] = Sha256::digest(manifest.encode().as_bytes()).into();
    let mut digest = Sha256::new();
    digest.update(b"bigbrotr/initial-source-closure/1\0");
    digest.update(manifest_hash);
    digest.update([7; 16]);
    digest.update(1_u64.to_be_bytes());
    digest.update(4_u64.to_be_bytes());
    for (index, row) in rows.iter().enumerate() {
        digest.update((index as u64 + 1).to_be_bytes());
        digest.update(row.digest.as_bytes());
        digest.update(Sha256::digest(&row.bytes));
    }
    digest.update(8_u64.to_be_bytes());
    let floor = ClosedProducerFloor::new(ClosedProducerFloorFields {
        issued_through: 4,
        manifest_sha256: manifest_hash,
        source_through: 8,
        closure_digest: digest.finalize().into(),
        checkpoint_revision: 5,
        checkpoint_digest: [0x45; 32],
    })
    .unwrap();
    drop(store);
    let recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    assert_eq!(
        recovery
            .closed_producer_floor(request(7, 1).producer_slot_id)
            .unwrap(),
        None
    );
    // Privileged reader-fixture construction only; this deliberately bypasses
    // the separately tested product reclamation authority/kernel.
    let cf = recovery.store.db.cf_handle("receipts").unwrap();
    let mut batch = rocksdb::WriteBatch::default();
    batch.put_cf(
        cf,
        producer_key().encode().unwrap().as_bytes(),
        floor.encode(),
    );
    for n in 1..=4 {
        batch.delete_cf(
            cf,
            IngestKey::Request {
                request_id: request(7, n),
            }
            .encode()
            .unwrap()
            .as_bytes(),
        );
    }
    recovery.store.db.write(batch).unwrap();
    Fixture {
        recovery,
        budget,
        rows,
        floor,
        root,
    }
}
fn observe(f: &Fixture) -> Result<RetainedCompactedPrefix, AdmissionError> {
    RetainedCompactedPrefix::inspect_initial(
        &f.recovery,
        request(7, 1).producer_slot_id,
        AdmissionLimits::default(),
        grant(&f.budget, COMPACTED_PREFIX_METADATA_BYTES, 0),
        grant(&f.budget, 40 * MIB, 0),
        &AtomicBool::new(false),
    )
}
fn row(f: &Fixture, index: usize) -> CompactedOutcomeRow<'_> {
    let row = &f.rows[index];
    CompactedOutcomeRow {
        request: request(7, index as u64 + 1),
        digest: row.digest,
        kind: row.kind,
        policy_epoch: PolicyEpoch::new(1),
        bytes: &row.bytes,
        input_bytes: row.input_bytes,
    }
}
fn resolve<'a>(
    f: &'a Fixture,
    prefix: &'a RetainedCompactedPrefix,
) -> Result<VerifiedCompactedPrefix<'a>, AdmissionError> {
    let cancelled = AtomicBool::new(false);
    let permit = grant(&f.budget, 40 * MIB, 0);
    let mut resolution = prefix.resolution(&f.recovery, &cancelled)?;
    for index in 0..4 {
        resolution.record(
            row(f, index),
            AdmissionLimits::default(),
            &permit,
            &cancelled,
            || Ok(()),
        )?;
    }
    resolution.finish(&cancelled)
}

#[test]
fn compacted_floor_codec_has_exact_bytes_and_legacy_refusal() {
    let fields = ClosedProducerFloorFields {
        issued_through: 7,
        manifest_sha256: [0x11; 32],
        source_through: 27,
        closure_digest: [0x22; 32],
        checkpoint_revision: 8,
        checkpoint_digest: [0x33; 32],
    };
    let literal = concat!(
        "033100000000000000070000000000000007000000000000000701",
        "1111111111111111111111111111111111111111111111111111111111111111",
        "000000000000001b",
        "2222222222222222222222222222222222222222222222222222222222222222",
        "0000000000000008",
        "3333333333333333333333333333333333333333333333333333333333333333"
    );
    let expected = (0..literal.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&literal[at..at + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let floor = ClosedProducerFloor::new(fields).unwrap();
    assert_eq!(floor.encode().as_slice(), expected);
    assert_eq!(ClosedProducerFloor::decode(&expected).unwrap(), floor);
    assert!(super::super::producer_closing::ProducerState::decode(&expected).is_err());
    for (offset, byte) in [
        (0, 2),
        (1, 0x30),
        (9, 0),
        (17, 6),
        (25, 8),
        (26, 0),
        (106, 0),
    ] {
        let mut bad = expected.clone();
        bad[offset] = byte;
        assert!(ClosedProducerFloor::decode(&bad).is_err());
    }
    for end in 0..139 {
        assert!(ClosedProducerFloor::decode(&expected[..end]).is_err());
    }
    let mut longer = expected;
    longer.push(0);
    assert!(ClosedProducerFloor::decode(&longer).is_err());
    for issued in [0, 65, u64::MAX] {
        assert!(
            ClosedProducerFloor::new(ClosedProducerFloorFields {
                issued_through: issued,
                ..fields
            })
            .is_err()
        );
    }
}

#[test]
fn compacted_prefix_validates_all_outcome_kinds_and_residual_originals_without_writes() {
    let f = fixture();
    for remaining in [4, 2, 0] {
        for n in remaining + 1..=4 {
            let path = input_path(f.root.path(), n);
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        let before = kv(&f.recovery.store);
        let sequence = f.recovery.store.db.latest_sequence_number();
        assert_eq!(
            f.recovery
                .closed_producer_floor(request(7, 1).producer_slot_id)
                .unwrap(),
            Some(f.floor)
        );
        let prefix = observe(&f).unwrap();
        let verified = resolve(&f, &prefix).unwrap();
        assert_eq!(verified.floor(), &f.floor);
        verified
            .validate(&f.recovery, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(kv(&f.recovery.store), before);
        assert_eq!(f.recovery.store.db.latest_sequence_number(), sequence);
        assert!(
            f.recovery
                .observe_closed_producer(
                    request(7, 1).producer_slot_id,
                    ProducerEpoch::new(1).unwrap(),
                    grant(&f.budget, 4096, 4096)
                )
                .is_err()
        );
    }
}

#[test]
fn compacted_prefix_requires_pending_indices_and_complete_scientific_census() {
    for fault in 0..7 {
        let f = fixture();
        observe(&f).unwrap();
        let rows = kv(&f.recovery.store);
        match fault {
            0..=3 => {
                let domain = [0x21, 0x04, 0x05, 0x20][fault];
                let (cf, key, _) = rows
                    .iter()
                    .find(|(_, key, _)| key[..2] == [1, domain])
                    .unwrap();
                f.recovery
                    .store
                    .db
                    .delete_cf(f.recovery.store.db.cf_handle(cf).unwrap(), key)
                    .unwrap();
            }
            4 => put(
                &f.recovery,
                IngestKey::Request {
                    request_id: request(7, 1),
                },
                &f.rows[0].bytes,
            ),
            6 => {
                let (cf, key, _) = rows
                    .iter()
                    .find(|(_, key, _)| key[..2] == [1, 0x03])
                    .unwrap();
                f.recovery
                    .store
                    .db
                    .delete_cf(f.recovery.store.db.cf_handle(cf).unwrap(), key)
                    .unwrap();
            }
            5 => {
                let (_, key, original) = rows
                    .iter()
                    .find(|(cf, key, _)| cf == "default" && key[..2] == [1, 0x04])
                    .unwrap();
                let other = rows
                    .iter()
                    .find(|(cf, k, v)| cf == "default" && k[..2] == [1, 0x04] && v != original)
                    .unwrap();
                f.recovery.store.db.put(key, &other.2).unwrap();
            }
            _ => unreachable!(),
        }
        let before = kv(&f.recovery.store);
        assert!(observe(&f).is_err());
        assert_eq!(kv(&f.recovery.store), before);
    }
}

#[test]
fn compacted_outcome_reducer_refuses_changed_rows_and_incomplete_closure() {
    let f = fixture();
    let prefix = observe(&f).unwrap();
    let cancelled = AtomicBool::new(false);
    let permit = grant(&f.budget, 40 * MIB, 0);
    assert!(
        prefix
            .resolution(&f.recovery, &cancelled)
            .unwrap()
            .finish(&cancelled)
            .is_err()
    );
    for fault in 0..5 {
        let mut resolution = prefix.resolution(&f.recovery, &cancelled).unwrap();
        let mut changed = row(&f, 0);
        match fault {
            0 => changed.kind = NativeOutcomeKind::AlreadyDurable,
            1 => changed.policy_epoch = PolicyEpoch::new(2),
            2 => changed.digest = Digest256::from_bytes([0x55; 32]),
            3 => changed.request = request(7, 2),
            4 => changed.input_bytes += 1,
            _ => unreachable!(),
        }
        assert!(
            resolution
                .record(
                    changed,
                    AdmissionLimits::default(),
                    &permit,
                    &cancelled,
                    || Ok(())
                )
                .is_err()
        );
        // Refusal cannot consume the slot: the exact valid row remains next.
        for index in 0..4 {
            resolution
                .record(
                    row(&f, index),
                    AdmissionLimits::default(),
                    &permit,
                    &cancelled,
                    || Ok(()),
                )
                .unwrap();
        }
        resolution.finish(&cancelled).unwrap();
    }
    let mut fields = *f.floor.fields();
    fields.closure_digest[0] ^= 1;
    put(
        &f.recovery,
        producer_key(),
        &ClosedProducerFloor::new(fields).unwrap().encode(),
    );
    let prefix = observe(&f).unwrap();
    assert!(resolve(&f, &prefix).is_err());
}

#[test]
fn compacted_prefix_binds_custody_inputs_cancellation_and_grants() {
    let f = fixture();
    let other = fixture();
    let prefix = observe(&f).unwrap();
    let cancelled = AtomicBool::new(false);
    assert!(prefix.validate(&other.recovery, &cancelled).is_err());
    assert_eq!(
        prefix.validate(&f.recovery, &AtomicBool::new(true)),
        Err(AdmissionError::AuthorityUnavailable)
    );
    assert!(matches!(
        RetainedCompactedPrefix::inspect_initial(
            &f.recovery,
            request(7, 1).producer_slot_id,
            AdmissionLimits::default(),
            grant(&f.budget, 4096, 0),
            grant(&f.budget, 40 * MIB, 0),
            &cancelled
        ),
        Err(AdmissionError::InsufficientReservation)
    ));
    let mut resolution = prefix.resolution(&f.recovery, &cancelled).unwrap();
    assert_eq!(
        resolution.record(
            row(&f, 0),
            AdmissionLimits::default(),
            &grant(&f.budget, 4096, 0),
            &cancelled,
            || Ok(())
        ),
        Err(AdmissionError::InsufficientReservation)
    );
    assert_eq!(
        resolution.record(
            row(&f, 0),
            AdmissionLimits::default(),
            &grant(&f.budget, 40 * MIB, 0),
            &cancelled,
            || Err(AdmissionError::AuthorityUnavailable)
        ),
        Err(AdmissionError::AuthorityUnavailable)
    );
    let path = input_path(f.root.path(), 1);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(prefix.validate(&f.recovery, &cancelled).is_err());
    // A new structural observation cannot authenticate the changed original.
    let prefix = observe(&f).unwrap();
    assert!(resolve(&f, &prefix).is_err());
    delete(&f.recovery, producer_key());
    assert!(prefix.validate(&f.recovery, &cancelled).is_err());
}

#[test]
fn compacted_outcomes_check_protection_batch_digest_and_residual_names() {
    let f = fixture();
    let prefix = observe(&f).unwrap();
    resolve(&f, &prefix).unwrap();
    let cancelled = AtomicBool::new(false);
    let permit = grant(&f.budget, 40 * MIB, 0);
    let receipt = Receipt::decode_persistent(
        &f.rows[0].bytes,
        bigbrotr_model::receipt_codec::ReceiptDecodeLimits::default(),
    )
    .unwrap();
    let mut protection = *receipt.protection_context();
    protection.topology_generation = TopologyGeneration::new(2);
    let wrong = Receipt::new_facts(
        receipt.request_id(),
        receipt.request_digest(),
        protection,
        receipt.batch().unwrap().clone(),
        receipt.dependencies().clone(),
    )
    .unwrap()
    .encode_persistent(65536)
    .unwrap();
    let mut changed = row(&f, 0);
    changed.bytes = &wrong;
    assert!(
        prefix
            .resolution(&f.recovery, &cancelled)
            .unwrap()
            .record(
                changed,
                AdmissionLimits::default(),
                &permit,
                &cancelled,
                || Ok(())
            )
            .is_err()
    );
    drop(prefix);
    let first = receipt.batch().unwrap().first();
    let mut header = PendingBatchHeader::from_batch(receipt.batch().unwrap()).encode();
    header[10] ^= 1;
    put(&f.recovery, IngestKey::PendingBatch { first }, &header);
    // Structural partition and all Fact bytes survive; exact batch binding does not.
    let prefix = observe(&f).unwrap();
    assert!(resolve(&f, &prefix).is_err());
    put(
        &f.recovery,
        IngestKey::PendingBatch { first },
        &PendingBatchHeader::from_batch(receipt.batch().unwrap()).encode(),
    );
    for name in [
        "unknown".to_owned(),
        format!("{}-{:016x}-{:016x}.cbor", "07".repeat(16), 1, 5),
    ] {
        let path = f.root.path().join("db/producer-inputs-v1").join(name);
        std::fs::copy(input_path(f.root.path(), 1), &path).unwrap();
        assert!(observe(&f).is_err());
        std::fs::remove_file(path).unwrap();
    }
    observe(&f).unwrap();
    for n in 1..=4 {
        std::fs::remove_file(input_path(f.root.path(), n)).unwrap();
    }
    observe(&f).unwrap();
    std::fs::remove_dir(f.root.path().join("db/producer-inputs-v1")).unwrap();
    assert!(observe(&f).is_err());
}

#[test]
fn compacted_outcomes_preserve_full_width_historical_policy_and_reject_row_mismatch() {
    let mut f = fixture();
    let original = observe(&f).unwrap();
    resolve(&f, &original).unwrap();
    drop(original);
    // As in the existing original-recovery controls, establish historical
    // canonical bytes under a policy distinct from the unchanged manifest.
    for index in [1, 2] {
        let receipt = Receipt::decode_persistent(
            &f.rows[index].bytes,
            bigbrotr_model::receipt_codec::ReceiptDecodeLimits::default(),
        )
        .unwrap();
        let mut context = *receipt.protection_context();
        context.policy_epoch = PolicyEpoch::new(u64::MAX);
        let historic = match receipt.batch() {
            Some(batch) => Receipt::new_facts(
                receipt.request_id(),
                receipt.request_digest(),
                context,
                batch.clone(),
                receipt.dependencies().clone(),
            ),
            None => Receipt::already_durable(
                receipt.request_id(),
                receipt.request_digest(),
                context,
                receipt.dependencies().clone(),
            ),
        }
        .unwrap();
        f.rows[index].bytes = historic.encode_persistent(65536).unwrap();
    }
    let rejection = &mut f.rows[3].bytes;
    let end = rejection.len();
    rejection[end - 9..end - 1].copy_from_slice(&u64::MAX.to_be_bytes());
    // Bind these exact historical bytes using the independent closure grammar.
    let mut digest = Sha256::new();
    digest.update(b"bigbrotr/initial-source-closure/1\0");
    digest.update(f.floor.fields().manifest_sha256);
    digest.update([7; 16]);
    digest.update(1_u64.to_be_bytes());
    digest.update(4_u64.to_be_bytes());
    for (index, row) in f.rows.iter().enumerate() {
        digest.update((index as u64 + 1).to_be_bytes());
        digest.update(row.digest.as_bytes());
        digest.update(Sha256::digest(&row.bytes));
    }
    digest.update(8_u64.to_be_bytes());
    f.floor = ClosedProducerFloor::new(ClosedProducerFloorFields {
        closure_digest: digest.finalize().into(),
        ..*f.floor.fields()
    })
    .unwrap();
    put(&f.recovery, producer_key(), &f.floor.encode());
    let before = kv(&f.recovery.store);
    let sequence = f.recovery.store.db.latest_sequence_number();
    let prefix = observe(&f).unwrap();
    assert_eq!(
        prefix.manifest().identity().policy_epoch,
        PolicyEpoch::new(1)
    );
    let cancelled = AtomicBool::new(false);
    let permit = grant(&f.budget, 40 * MIB, 0);
    let mut resolution = prefix.resolution(&f.recovery, &cancelled).unwrap();
    resolution
        .record(
            row(&f, 0),
            AdmissionLimits::default(),
            &permit,
            &cancelled,
            || Ok(()),
        )
        .unwrap();
    for index in 1..4 {
        // Only row metadata differs. Bytes, closure and native state stay exact.
        assert_eq!(
            resolution.record(
                row(&f, index),
                AdmissionLimits::default(),
                &permit,
                &cancelled,
                || Ok(())
            ),
            Err(AdmissionError::StateMismatch)
        );
        let mut historic = row(&f, index);
        historic.policy_epoch = PolicyEpoch::new(u64::MAX);
        resolution
            .record(
                historic,
                AdmissionLimits::default(),
                &permit,
                &cancelled,
                || Ok(()),
            )
            .unwrap();
    }
    resolution
        .finish(&cancelled)
        .unwrap()
        .validate(&f.recovery, &cancelled)
        .unwrap();
    assert_eq!(kv(&f.recovery.store), before);
    assert_eq!(f.recovery.store.db.latest_sequence_number(), sequence);
}

#[path = "closed_reclamation_tests.rs"]
mod closed_reclamation_tests;
