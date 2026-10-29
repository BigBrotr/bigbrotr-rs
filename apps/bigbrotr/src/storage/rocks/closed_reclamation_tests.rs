//! Real native mechanics under synthetic SQL authority. These controls do not
//! qualify PostgreSQL retirement, COMMIT completion or installed reclamation.
use super::*;
use crate::storage::rocks::store::ClosedProducerReclamation;

fn mutation(f: &Fixture) -> OwnedPermit {
    grant(
        &f.budget,
        ClosedProducerReclamation::DATA_BYTES,
        ClosedProducerReclamation::NATIVE_BYTES,
    )
}
fn compacted_plan(f: &Fixture) -> ClosedProducerReclamation {
    let prefix = observe(f).unwrap();
    let proof = resolve(f, &prefix).unwrap();
    ClosedProducerReclamation::initial_compacted(
        &f.recovery,
        &proof,
        mutation(f),
        &AtomicBool::new(false),
    )
    .unwrap()
}
fn originals(f: &Fixture) {
    let mut marker = vec![2, 0x31];
    for n in [4_u64, 4, 0] {
        marker.extend_from_slice(&n.to_be_bytes());
    }
    marker.push(1);
    put(&f.recovery, producer_key(), &marker);
    for (index, row) in f.rows.iter().enumerate() {
        put(
            &f.recovery,
            IngestKey::Request {
                request_id: request(7, index as u64 + 1),
            },
            &row.bytes,
        );
    }
}
fn original_plan(f: &Fixture) -> ClosedProducerReclamation {
    originals(f);
    let closing = f
        .recovery
        .observe_closed_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&f.budget, 4096, 4096),
        )
        .unwrap();
    let mut resolution = closing.source_resolution(&f.recovery, 4).unwrap();
    for (index, row) in f.rows.iter().enumerate() {
        let resolved = f
            .recovery
            .resolve_original_request(
                request(7, index as u64 + 1),
                row.digest,
                super::super::producer_recovery_tests::recovery_limits(),
                grant(&f.budget, 3 * MIB, 0),
                grant(&f.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        resolution.record(&resolved).unwrap();
    }
    let source = resolution.finish().unwrap();
    ClosedProducerReclamation::initial_original(
        &source,
        f.floor,
        AdmissionLimits::default(),
        mutation(f),
        &grant(&f.budget, 40 * MIB, 0),
        &AtomicBool::new(false),
    )
    .unwrap()
}
fn remaining(f: &Fixture) -> usize {
    std::fs::read_dir(f.root.path().join("db/producer-inputs-v1"))
        .unwrap()
        .count()
}

#[test]
fn closed_reclamation_installs_exact_batch_then_preserves_science_and_restarts() {
    let mut f = fixture();
    let plan = original_plan(&f);
    let before = kv(&f.recovery.store);
    let sequence = f.recovery.store.db.latest_sequence_number();
    let receipt = plan
        .test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(()))
        .unwrap();
    assert_eq!(receipt.retired_requests(), 4);
    assert_eq!(receipt.removed_input_files(), 4);
    assert_eq!(receipt.native_sequence(), sequence + 5);
    assert_eq!(remaining(&f), 0);
    let after = kv(&f.recovery.store);
    let mut expected = before;
    let marker = producer_key().encode().unwrap();
    expected.retain(|(cf, key, _)| !(cf == "receipts" && key.starts_with(&[1, 0x30])));
    let entry = expected
        .iter_mut()
        .find(|(cf, key, _)| cf == "receipts" && key == marker.as_bytes())
        .unwrap();
    entry.2 = f.floor.encode().to_vec();
    assert_eq!(after, expected);
    let manifest = *f.recovery.manifest();
    let limits = f.recovery.store.resources.limits();
    drop(f.recovery);
    f.recovery = IngestRecovery::open(
        &f.root.path().join("db"),
        manifest,
        limits,
        grant(&f.budget, 0, 128 * MIB),
    )
    .unwrap();
    let plan = compacted_plan(&f);
    let sequence = f.recovery.store.db.latest_sequence_number();
    let receipt = plan
        .test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(()))
        .unwrap();
    assert_eq!(receipt.removed_input_files(), 0);
    assert_eq!(receipt.native_sequence(), sequence);
    assert_eq!(kv(&f.recovery.store), after);
}
#[test]
fn closed_reclamation_refuses_stale_native_or_inventory_before_unlink() {
    for changed_native in [false, true] {
        let mut f = fixture();
        let plan = compacted_plan(&f);
        if changed_native {
            put(&f.recovery, producer_key(), &f.floor.encode());
        } else {
            std::fs::remove_file(input_path(f.root.path(), 1)).unwrap();
        }
        let before = kv(&f.recovery.store);
        let count = remaining(&f);
        assert!(matches!(
            plan.test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(())),
            Err(AdmissionError::StateMismatch) | Err(AdmissionError::RequestInput(_))
        ));
        assert_eq!(remaining(&f), count);
        assert_eq!(kv(&f.recovery.store), before);
    }
    // The same root, manifest, sequence and byte-exact inputs after reopening
    // cannot reuse authority authenticated under an earlier native exclusion.
    // Both original and compacted confirmations must obtain a fresh proof.
    for compacted in [false, true] {
        let mut f = fixture();
        let plan = if compacted {
            compacted_plan(&f)
        } else {
            original_plan(&f)
        };
        let before = kv(&f.recovery.store);
        let sequence = f.recovery.store.db.latest_sequence_number();
        let manifest = *f.recovery.manifest();
        let limits = f.recovery.store.resources.limits();
        drop(f.recovery);
        f.recovery = IngestRecovery::open(
            &f.root.path().join("db"),
            manifest,
            limits,
            grant(&f.budget, 0, 128 * MIB),
        )
        .unwrap();
        assert_eq!(f.recovery.store.db.latest_sequence_number(), sequence);
        assert!(matches!(
            plan.test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(())),
            Err(AdmissionError::StateMismatch)
        ));
        assert_eq!(remaining(&f), 4);
        assert_eq!(kv(&f.recovery.store), before);
    }
}
#[test]
fn closed_reclamation_cancellation_after_one_unlink_requires_fresh_proof() {
    let mut f = fixture();
    let plan = compacted_plan(&f);
    let before = kv(&f.recovery.store);
    let sequence = f.recovery.store.db.latest_sequence_number();
    let mut calls = 0;
    assert!(
        plan.test_apply(&mut f.recovery, &AtomicBool::new(false), || {
            calls += 1;
            if calls == 5 {
                Err(AdmissionError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        })
        .is_err()
    );
    assert_eq!(remaining(&f), 3);
    assert_eq!(kv(&f.recovery.store), before);
    let plan = compacted_plan(&f);
    let receipt = plan
        .test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(()))
        .unwrap();
    assert_eq!(receipt.removed_input_files(), 3);
    assert_eq!(remaining(&f), 0);
    assert_eq!(receipt.native_sequence(), sequence);
    assert_eq!(kv(&f.recovery.store), before);
}
#[test]
fn closed_reclamation_unknown_native_and_cancelled_authority_never_unlink() {
    for cancelled in [false, true] {
        let mut f = fixture();
        let plan = compacted_plan(&f);
        f.recovery.store.write_uncertain = !cancelled;
        assert!(
            plan.test_apply(&mut f.recovery, &AtomicBool::new(cancelled), || Ok(()))
                .is_err()
        );
        assert_eq!(remaining(&f), 4);
    }
}
#[test]
fn closed_reclamation_rechecks_corrupt_residuals_against_canonical_outcomes() {
    let f = fixture();
    let file = input_path(f.root.path(), 2);
    let mut bytes = std::fs::read(&file).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&file, bytes).unwrap();
    let prefix = observe(&f).unwrap();
    assert!(resolve(&f, &prefix).is_err());
    assert_eq!(remaining(&f), 4);
}

#[test]
fn reclaimed_resubmissions_return_retired_without_input_or_digest_reconstruction() {
    let mut f = fixture();
    let plan = compacted_plan(&f);
    plan.test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(()))
        .unwrap();
    let before = kv(&f.recovery.store);
    for digest in [f.rows[1].digest, Digest256::from_bytes([0x99; 32])] {
        assert!(matches!(
            f.recovery.resolve_original_request(
                request(7, 2),
                digest,
                super::super::producer_recovery_tests::recovery_limits(),
                grant(&f.budget, 3 * MIB, 0),
                grant(&f.budget, 40 * MIB, 0),
                || Ok(())
            ),
            Err(ProducerRecoveryError::Native(
                AdmissionError::RetiredRequest
            ))
        ));
    }
    let input = PreparedEvent::new(
        request(7, 2),
        intent("E_private", 2, 11_000_000),
        256 * 1024,
        grant(&f.budget, MIB, 0),
    )
    .unwrap();
    assert!(matches!(
        f.recovery.store.admit_event(
            input,
            ObservedTime::new(11_000_000),
            false,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 10 * MIB),
            || Ok(())
        ),
        Err(AdmissionError::RetiredRequest)
    ));
    assert_eq!(remaining(&f), 0);
    assert_eq!(kv(&f.recovery.store), before);
}

#[test]
fn closed_reclamation_empty_retry_binds_the_actual_spool_directory() {
    use std::os::unix::fs::PermissionsExt;
    let mut f = fixture();
    let plan = compacted_plan(&f);
    plan.test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(()))
        .unwrap();
    let plan = compacted_plan(&f);
    let spool = f.root.path().join("db/producer-inputs-v1");
    std::fs::rename(&spool, f.root.path().join("quarantined-empty-spool")).unwrap();
    std::fs::create_dir(&spool).unwrap();
    std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(
        plan.test_apply(&mut f.recovery, &AtomicBool::new(false), || Ok(())),
        Err(AdmissionError::StateMismatch)
    ));
    assert_eq!(remaining(&f), 0);
}
