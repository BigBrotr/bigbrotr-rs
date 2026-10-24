//! Literal floor/portable bytes over the existing real initial fixture. These
//! native tests supply synthetic authority, not connected SQL/activation proof.
use super::*;
use crate::storage::rocks::store::{
    COMPACTED_HANDOFF_DATA_BYTES, COMPACTED_PREFIX_METADATA_BYTES, CompactedOutcomeRow,
    CompactedPrefixResolution, InitialHandoffRecord, InitialWriterCandidate, NativeOutcomeKind,
    RetainedCompactedPrefix, RetainedLineage, VerifiedCompactedPrefix,
};
use std::{os::unix::fs::PermissionsExt, sync::atomic::AtomicBool};

static READY: AtomicBool = AtomicBool::new(false);

#[path = "compacted_empty_tests.rs"]
mod compacted_empty_tests;

fn floor_bytes(f: &Fixture) -> [u8; 139] {
    let mut floor = [0; 139];
    floor[..2].copy_from_slice(&[3, 0x31]);
    for at in [2, 10, 18] {
        floor[at..at + 8].copy_from_slice(&4_u64.to_be_bytes());
    }
    floor[26] = 1;
    floor[27..59].copy_from_slice(&Sha256::digest(f.previous.encode().as_bytes()));
    floor[59..67].copy_from_slice(&9_u64.to_be_bytes());
    floor[67..99].copy_from_slice(&expected_closure(
        f.previous.encode().as_bytes(),
        &f.originals,
    ));
    floor[99..107].copy_from_slice(&5_u64.to_be_bytes());
    floor[107..].fill(0x45);
    floor
}
fn producer_key(epoch: u64) -> IngestKey {
    IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(epoch).unwrap(),
    }
}
fn install(f: &Fixture, r: &IngestRecovery, remaining: u64) {
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
                request_id: request(7, sequence),
            }
            .encode()
            .unwrap()
            .as_bytes(),
        );
        if sequence > remaining {
            std::fs::remove_file(
                f.path()
                    .join("producer-inputs-v1")
                    .join(input_name(1, sequence)),
            )
            .unwrap();
        }
    }
    r.store.db.write(batch).unwrap();
}
fn scratch(f: &Fixture) -> OwnedPermit {
    grant(&f.budget, COMPACTED_HANDOFF_DATA_BYTES, 8192)
}
fn observe(f: &Fixture, r: &IngestRecovery) -> Result<RetainedCompactedPrefix, AdmissionError> {
    RetainedCompactedPrefix::inspect_initial(
        r,
        request(7, 1).producer_slot_id,
        AdmissionLimits::default(),
        grant(&f.budget, COMPACTED_PREFIX_METADATA_BYTES, 0),
        grant(&f.budget, 40 * MIB, 0),
        &READY,
    )
}
fn resolve<'a>(
    f: &Fixture,
    mut cursor: CompactedPrefixResolution<'a>,
) -> Result<VerifiedCompactedPrefix<'a>, AdmissionError> {
    let permit = grant(&f.budget, 40 * MIB, 0);
    for (index, original) in f.originals.iter().enumerate() {
        cursor.record(
            CompactedOutcomeRow {
                request: request(7, index as u64 + 1),
                digest: input_digest(original),
                kind: if index == 3 {
                    NativeOutcomeKind::AlreadyDurable
                } else {
                    NativeOutcomeKind::NewFacts
                },
                policy_epoch: PolicyEpoch::new(1),
                bytes: &original.outcome,
                input_bytes: original.input.len() as u32,
            },
            AdmissionLimits::default(),
            &permit,
            &READY,
            || Ok(()),
        )?;
    }
    cursor.finish(&READY)
}
fn plan(f: &Fixture, r: &IngestRecovery) -> InitialHandoff {
    let prefix = observe(f, r).unwrap();
    let proof = resolve(f, prefix.resolution(r, &READY).unwrap()).unwrap();
    InitialHandoff::from_compacted(&proof, f.next, &READY).unwrap()
}
fn lineage(f: &Fixture, r: &IngestRecovery, pending: Option<u64>) -> RetainedLineage {
    RetainedLineage::inspect_compacted_initial(r, pending, grant(&f.budget, 256 * 1024, 0), &READY)
        .unwrap()
}
fn prepared_plan(f: &Fixture, r: &IngestRecovery) -> Result<InitialHandoff, AdmissionError> {
    let lineage = lineage(f, r, None);
    let ancestor = lineage
        .compacted_initial_ancestor(
            r,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 0),
            &READY,
        )?
        .unwrap();
    let proof = resolve(f, ancestor.resolution(&READY)?)?;
    let historical = r.historical_initial(&literal_handoff(f))?;
    historical.reconstruct_compacted_handoff(&proof, &READY)
}
fn expected_compacted_rows(f: &Fixture) -> Rows {
    let mut rows = expected_rows(f);
    let key = producer_key(1).encode().unwrap();
    rows.iter_mut()
        .find(|(cf, k, _)| cf == "receipts" && k == key.as_bytes())
        .unwrap()
        .2 = floor_bytes(f).to_vec();
    for sequence in 1..=4 {
        let key = IngestKey::Request {
            request_id: request(7, sequence),
        }
        .encode()
        .unwrap();
        rows.retain(|(cf, k, _)| cf != "receipts" || k != key.as_bytes());
    }
    rows
}

#[test]
fn compacted_handoff_exact_four_writes_and_zero_write_prepared_retry() {
    for remaining in [4, 2, 0] {
        let f = closed_fixture();
        let mut r = f.reopen();
        install(&f, &r, remaining);
        let before = kv(&r.store);
        let original_inputs = inputs(&f.path());
        let sequence = r.store.db.latest_sequence_number();
        let handoff = plan(&f, &r);
        assert_eq!(handoff.encode().as_bytes(), literal_handoff(&f));
        assert_eq!(handoff.issued_through(), 4);
        assert_eq!(handoff.last_complete_sequence(), 9);
        assert_eq!(kv(&r.store), before);
        let permit = scratch(&f);
        assert_eq!(
            r.validate_initial_handoff(&handoff),
            Err(BootstrapError::StateMismatch)
        );
        assert!(
            r.apply_initial_handoff(&handoff, grant(&f.budget, 8192, 8192), || Ok(()))
                .is_err()
        );
        {
            let view = handoff
                .compacted_predecessor(&r, &permit, &READY)
                .unwrap()
                .unwrap();
            let verified = resolve(&f, view.resolution(&READY).unwrap()).unwrap();
            assert_eq!(verified.floor().encode(), floor_bytes(&f));
            // A plan-derived recheck cannot silently renew the retained stamp.
            assert!(InitialHandoff::from_compacted(&verified, f.next, &READY).is_err());
        }
        drop(
            r.apply_compacted_initial_handoff(&handoff, scratch(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
        assert_eq!(kv(&r.store), expected_compacted_rows(&f));
        assert_eq!(inputs(&f.path()), original_inputs);
        drop(
            r.apply_compacted_initial_handoff(&handoff, scratch(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
        {
            let view = handoff
                .compacted_predecessor(&r, &permit, &READY)
                .unwrap()
                .unwrap();
            resolve(&f, view.resolution(&READY).unwrap()).unwrap();
        }
        drop(r);
        // Portable bytes alone have no application conversion. Reopen performs
        // a fresh complete two-entry census and canonical resolution.
        let mut r = f.reopen();
        let before = kv(&r.store);
        let reconstructed = prepared_plan(&f, &r).unwrap();
        assert_eq!(reconstructed.encode().as_bytes(), literal_handoff(&f));
        let historical = r.historical_initial(&literal_handoff(&f)).unwrap();
        assert!(
            historical
                .observe_closed_producer(grant(&f.budget, 4096, 4096))
                .is_err()
        );
        assert!(super::super::reconstruct(&f, &r).is_err());
        assert!(
            crate::storage::rocks::store::ActivatedInitial::inspect(
                &r,
                InitialHandoffRecord::decode(&literal_handoff(&f)).unwrap(),
            )
            .is_err()
        );
        drop(
            r.apply_compacted_initial_handoff(&reconstructed, scratch(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
        assert_eq!(inputs(&f.path()), original_inputs);
    }
}

#[test]
fn compacted_handoff_refuses_stale_custody_inputs_and_dispatch_without_writes() {
    for fault in [
        "grant",
        "native-grant",
        "cancel",
        "authority",
        "dispatch-authority",
        "sequence",
        "after-four",
        "uncertain",
        "input-inode",
        "input-content",
        "extra-input",
        "floor",
        "target",
    ] {
        let f = closed_fixture();
        let mut r = f.reopen();
        install(&f, &r, 4);
        let handoff = plan(&f, &r);
        let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
        match fault {
            "sequence" => r
                .store
                .db
                .put([1, 1], f.previous.encode().as_bytes())
                .unwrap(),
            "after-four" => {
                drop(
                    r.apply_compacted_initial_handoff(&handoff, scratch(&f), &READY, || Ok(()))
                        .unwrap(),
                );
                r.store.db.put([1, 1], f.next.encode().as_bytes()).unwrap();
            }
            "uncertain" => r.store.write_uncertain = true,
            "input-inode" => {
                let old = std::fs::read(&path).unwrap();
                let replacement = f.root.path().join("replacement");
                std::fs::write(&replacement, old).unwrap();
                std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600))
                    .unwrap();
                std::fs::rename(replacement, &path).unwrap();
            }
            "input-content" => {
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(&path, bytes).unwrap();
            }
            "extra-input" => {
                let extra = f.path().join("producer-inputs-v1").join(input_name(2, 1));
                std::fs::write(&extra, b"extra").unwrap();
                std::fs::set_permissions(extra, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            "floor" => {
                let mut bytes = floor_bytes(&f);
                bytes[107] ^= 1;
                let key = producer_key(1).encode().unwrap();
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        key.as_bytes(),
                        bytes,
                    )
                    .unwrap();
            }
            "target" => {
                let key = source_key(2).encode().unwrap();
                r.store
                    .db
                    .put(key.as_bytes(), [1, 2, 0, 0, 0, 0, 0, 0, 0, 0])
                    .unwrap();
            }
            _ => (),
        }
        let before = kv(&r.store);
        let files = inputs(&f.path());
        let sequence = r.store.db.latest_sequence_number();
        let cancelled = AtomicBool::new(fault == "cancel");
        let data = if fault == "grant" {
            COMPACTED_HANDOFF_DATA_BYTES - 1
        } else {
            COMPACTED_HANDOFF_DATA_BYTES
        };
        let native = if fault == "native-grant" { 8191 } else { 8192 };
        let mut calls = 0;
        let error = r
            .apply_compacted_initial_handoff(
                &handoff,
                grant(&f.budget, data, native),
                &cancelled,
                || {
                    calls += 1;
                    if fault == "authority" || (fault == "dispatch-authority" && calls == 2) {
                        Err(BootstrapError::AuthorityUnavailable)
                    } else {
                        Ok(())
                    }
                },
            )
            .err()
            .unwrap();
        if matches!(fault, "grant" | "native-grant") {
            assert_eq!(error, BootstrapError::InsufficientReservation);
        }
        if matches!(fault, "cancel" | "authority" | "dispatch-authority") {
            assert_eq!(error, BootstrapError::AuthorityUnavailable);
        }
        if fault == "uncertain" {
            assert_eq!(error, BootstrapError::CommitUnknown);
        }
        assert_eq!(kv(&r.store), before, "{fault}");
        assert_eq!(r.store.db.latest_sequence_number(), sequence, "{fault}");
        assert_eq!(inputs(&f.path()), files, "{fault}");
    }
}

#[test]
fn compacted_handoff_requires_full_census_original_manifest_cut_and_digest() {
    for fault in [
        "manifest",
        "cut",
        "digest",
        "pending",
        "dictionary-index",
        "corrupt-residual",
    ] {
        let f = closed_fixture();
        let r = f.reopen();
        install(&f, &r, 4);
        match fault {
            "manifest" | "cut" | "digest" => {
                let mut bytes = floor_bytes(&f);
                bytes[match fault {
                    "manifest" => 27,
                    "cut" => 66,
                    _ => 67,
                }] ^= 1;
                let key = producer_key(1).encode().unwrap();
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        key.as_bytes(),
                        bytes,
                    )
                    .unwrap();
            }
            "pending" | "dictionary-index" => {
                let domain = if fault == "pending" { 0x21 } else { 0x04 };
                let rows = kv(&r.store);
                let (cf, key, _) = rows
                    .iter()
                    .find(|(_, key, _)| key[..2] == [1, domain])
                    .unwrap();
                r.store
                    .db
                    .delete_cf(r.store.db.cf_handle(cf).unwrap(), key)
                    .unwrap();
            }
            "corrupt-residual" => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            _ => unreachable!(),
        }
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let result = observe(&f, &r).and_then(|prefix| {
            let proof = resolve(&f, prefix.resolution(&r, &READY)?)?;
            InitialHandoff::from_compacted(&proof, f.next, &READY)
        });
        assert!(result.is_err(), "{fault}");
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn compacted_handoff_wrong_owner_and_prepared_pending_context_refuse() {
    let f = prepared_fixture();
    let r = f.reopen();
    install(&f, &r, 0);
    let pending = lineage(&f, &r, Some(3));
    let ancestor = pending
        .compacted_initial_ancestor(
            &r,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 0),
            &READY,
        )
        .unwrap()
        .unwrap();
    let proof = resolve(&f, ancestor.resolution(&READY).unwrap()).unwrap();
    let historical = r.historical_initial(&literal_handoff(&f)).unwrap();
    assert!(
        historical
            .reconstruct_compacted_handoff(&proof, &READY)
            .is_err()
    );
    let plain = lineage(&f, &r, None);
    let ancestor = plain
        .compacted_initial_ancestor(
            &r,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 0),
            &READY,
        )
        .unwrap()
        .unwrap();
    let proof = resolve(&f, ancestor.resolution(&READY).unwrap()).unwrap();
    let other = prepared_fixture();
    let other_r = other.reopen();
    install(&other, &other_r, 0);
    let other_history = other_r
        .historical_initial(&literal_handoff(&other))
        .unwrap();
    assert!(
        other_history
            .reconstruct_compacted_handoff(&proof, &READY)
            .is_err()
    );
    let handoff = historical
        .reconstruct_compacted_handoff(&proof, &READY)
        .unwrap();
    assert!(
        other_r
            .validate_compacted_initial_handoff(&handoff, &scratch(&other), &READY)
            .is_err()
    );
    assert!(
        historical
            .reconstruct_compacted_handoff(&proof, &AtomicBool::new(true))
            .is_err()
    );
    assert!(InitialHandoff::from_compacted(&proof, f.next, &READY).is_err());
    let permit = scratch(&f);
    let view = handoff
        .compacted_predecessor(&r, &permit, &READY)
        .unwrap()
        .unwrap();
    assert!(view.validate(&other_r, &READY).is_err());
    let mut changed = literal_handoff(&f);
    changed[45] ^= 1;
    assert!(r.historical_initial(&changed).is_err());
}

#[test]
fn compacted_handoff_prepared_target_must_be_empty_open_and_census_complete() {
    for fault in [
        "closing",
        "target-input",
        "target-source",
        "missing-pending",
        "later-floor",
    ] {
        let f = prepared_fixture();
        let r = f.reopen();
        install(&f, &r, 0);
        match fault {
            "closing" => {
                let mut bytes = [0; 27];
                bytes[..2].copy_from_slice(&[2, 0x31]);
                bytes[26] = 1;
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        producer_key(2).encode().unwrap().as_bytes(),
                        bytes,
                    )
                    .unwrap();
            }
            "target-input" => {
                let path = f.path().join("producer-inputs-v1").join(input_name(2, 1));
                std::fs::write(&path, b"unexpected").unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            "target-source" => r
                .store
                .db
                .put(
                    source_key(2).encode().unwrap().as_bytes(),
                    [1, 2, 0, 0, 0, 0, 0, 0, 0, 1],
                )
                .unwrap(),
            "missing-pending" => {
                let rows = kv(&r.store);
                let (cf, key, _) = rows
                    .iter()
                    .find(|(_, key, _)| key[..2] == [1, 0x21])
                    .unwrap();
                r.store
                    .db
                    .delete_cf(r.store.db.cf_handle(cf).unwrap(), key)
                    .unwrap();
            }
            "later-floor" => r
                .store
                .db
                .put_cf(
                    r.store.db.cf_handle("receipts").unwrap(),
                    producer_key(2).encode().unwrap().as_bytes(),
                    floor_bytes(&f),
                )
                .unwrap(),
            _ => unreachable!(),
        }
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let result = RetainedLineage::inspect_compacted_initial(
            &r,
            None,
            grant(&f.budget, 256 * 1024, 0),
            &READY,
        )
        .map_err(AdmissionError::from)
        .and_then(|lineage| {
            let ancestor = lineage
                .compacted_initial_ancestor(
                    &r,
                    AdmissionLimits::default(),
                    grant(&f.budget, 40 * MIB, 0),
                    &READY,
                )?
                .unwrap();
            let proof = resolve(&f, ancestor.resolution(&READY)?)?;
            r.historical_initial(&literal_handoff(&f))?
                .reconstruct_compacted_handoff(&proof, &READY)
        });
        assert!(result.is_err(), "{fault}");
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn compacted_candidate_binds_fresh_target_and_explicit_activation_only() {
    let f = closed_fixture();
    let r = f.reopen();
    install(&f, &r, 0);
    let mut candidate = InitialWriterCandidate::new(r).unwrap();
    let handoff = {
        let prefix = observe(&f, candidate.recovery()).unwrap();
        let proof = resolve(&f, prefix.resolution(candidate.recovery(), &READY).unwrap()).unwrap();
        let other = closed_fixture();
        let other_candidate = InitialWriterCandidate::new(other.reopen()).unwrap();
        assert!(
            other_candidate
                .handoff_compacted(&proof, PolicyEpoch::new(1), &READY)
                .is_err()
        );
        assert!(
            candidate
                .handoff_compacted(&proof, PolicyEpoch::new(1), &AtomicBool::new(true))
                .is_err()
        );
        candidate
            .handoff_compacted(&proof, PolicyEpoch::new(1), &READY)
            .unwrap()
    };
    let permit = scratch(&f);
    assert_eq!(
        handoff.next_manifest().identity().process_incarnation,
        candidate.process_incarnation()
    );
    assert_eq!(
        handoff.next_manifest().allocation_namespaces(),
        candidate.allocation_namespaces()
    );
    assert!(candidate.require_handoff(&handoff).is_err());
    assert!(candidate.require_prepared_handoff(&handoff).is_err());
    assert!(
        candidate
            .require_prepared_compacted_handoff(&handoff, &permit, &READY)
            .is_err()
    );
    candidate
        .require_compacted_handoff(&handoff, &permit, &READY)
        .unwrap();
    let sequence = candidate.recovery().store.db.latest_sequence_number();
    drop(
        candidate
            .recovery_mut()
            .apply_compacted_initial_handoff(&handoff, scratch(&f), &READY, || Ok(()))
            .unwrap(),
    );
    assert_eq!(
        candidate.recovery().store.db.latest_sequence_number(),
        sequence + 4
    );
    assert!(candidate.require_prepared_handoff(&handoff).is_err());
    candidate
        .require_prepared_compacted_handoff(&handoff, &permit, &READY)
        .unwrap();
    let before = kv(&candidate.recovery().store);
    // This private call supplies synthetic connected authority only.
    let writer = candidate
        .activate_compacted(handoff, &permit, &READY)
        .unwrap();
    assert_eq!(kv(writer.store()), before);
    assert_eq!(writer.store().db.latest_sequence_number(), sequence + 4);
    assert_eq!(writer.manifest(), writer.handoff().next_manifest());
    assert!(inputs(&f.path()).is_empty());
}

#[test]
fn compacted_handoff_postwrite_authority_refusal_retains_exact_retry() {
    let f = closed_fixture();
    let mut r = f.reopen();
    install(&f, &r, 0);
    let handoff = plan(&f, &r);
    let sequence = r.store.db.latest_sequence_number();
    let mut calls = 0;
    let refused = r.apply_compacted_initial_handoff(&handoff, scratch(&f), &READY, || {
        calls += 1;
        if calls == 3 {
            Err(BootstrapError::AuthorityUnavailable)
        } else {
            Ok(())
        }
    });
    assert!(matches!(refused, Err(BootstrapError::AuthorityUnavailable)));
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(kv(&r.store), expected_compacted_rows(&f));
    drop(
        r.apply_compacted_initial_handoff(&handoff, scratch(&f), &READY, || Ok(()))
            .unwrap(),
    );
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(kv(&r.store), expected_compacted_rows(&f));
}

#[test]
fn initial_preparation_selector_accepts_original_and_compacted_without_writes() {
    for compacted in [false, true] {
        let f = prepared_fixture();
        let r = f.reopen();
        if compacted {
            install(&f, &r, 0);
        }
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let spool = inputs(&f.path());
        let observed = RetainedLineage::inspect_initial_preparation(
            &r,
            grant(&f.budget, 128 * 1024 + 2 * 512, 0),
            &READY,
        )
        .unwrap();
        assert_eq!(observed.has_compacted_initial(), compacted);
        assert_eq!(observed.entries().len(), 2);
        assert_eq!(observed.pending_target(), None);
        assert_eq!(observed.entry(1).unwrap().issued_through(), 4);
        assert_eq!(observed.native_head().issued_through(), 0);
        assert_eq!(observed.native_head().source_through(), 0);
        assert!(!observed.native_head().is_closing());
        if compacted {
            // Selecting the structural reader cannot enable ordinary replay.
            assert!(
                RetainedLineage::inspect(&r, None, grant(&f.budget, 256 * 1024, 0), &READY,)
                    .is_err()
            );
        }
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&f.path()), spool);
    }
}

#[test]
fn initial_preparation_selector_refuses_incompatible_head_and_census() {
    for (compacted, fault) in [
        (false, "closing"),
        (true, "closing"),
        (false, "issued"),
        (true, "issued"),
        (false, "source"),
        (true, "source"),
        (false, "target-input"),
        (true, "target-input"),
        (false, "missing-original"),
        (true, "malformed-floor"),
    ] {
        let f = prepared_fixture();
        let r = f.reopen();
        if compacted {
            install(&f, &r, 0);
        }
        match fault {
            "closing" | "issued" => {
                let mut bytes = [0; 27];
                bytes[..2].copy_from_slice(&[2, 0x31]);
                if fault == "closing" {
                    bytes[26] = 1;
                } else {
                    bytes[9] = 1;
                }
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        producer_key(2).encode().unwrap().as_bytes(),
                        bytes,
                    )
                    .unwrap();
            }
            "source" => r
                .store
                .db
                .put(
                    source_key(2).encode().unwrap().as_bytes(),
                    [1, 2, 0, 0, 0, 0, 0, 0, 0, 1],
                )
                .unwrap(),
            "target-input" => {
                let path = f.path().join("producer-inputs-v1").join(input_name(2, 1));
                std::fs::write(&path, b"unexpected").unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            "missing-original" => {
                std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(1, 1)))
                    .unwrap()
            }
            "malformed-floor" => {
                let mut bytes = floor_bytes(&f);
                bytes[25] = 3; // R differs from the closed acknowledged/issued cut.
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        producer_key(1).encode().unwrap().as_bytes(),
                        bytes,
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let spool = inputs(&f.path());
        assert!(
            RetainedLineage::inspect_initial_preparation(
                &r,
                grant(&f.budget, 256 * 1024, 0),
                &READY,
            )
            .is_err(),
            "{compacted}/{fault}"
        );
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&f.path()), spool);
    }
}

#[test]
fn initial_preparation_selector_requires_epoch_two_budget_and_cancellation() {
    let old = closed_fixture();
    let r = old.reopen();
    assert!(RetainedLineage::inspect_initial_preparation(
        &r, grant(&old.budget, 256 * 1024, 0), &READY,
    ).is_err());
    drop(r);
    let f = prepared_fixture();
    let r = f.reopen();
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    for (bytes, cancelled) in [(128 * 1024 + 2 * 512 - 1, false), (256 * 1024, true)] {
        assert!(
            RetainedLineage::inspect_initial_preparation(
                &r,
                grant(&f.budget, bytes, 0),
                &AtomicBool::new(cancelled),
            )
            .is_err()
        );
    }
    assert_eq!(kv(&r.store), before);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}
