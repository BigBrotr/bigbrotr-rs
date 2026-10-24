//! Real signed predecessor fixture, literal empty-target effects and synthetic
//! native authority only. These tests do not qualify SQL lifecycle or custody.
use super::*;
use crate::storage::rocks::store::{COMPACTED_EMPTY_DATA_BYTES, CompactedEmptyInitialRetirement};
use std::sync::atomic::Ordering;

fn empty_lineage(f: &Fixture, r: &IngestRecovery) -> Result<RetainedLineage, BootstrapError> {
    RetainedLineage::inspect_initial_empty_attempt(
        r,
        grant(&f.budget, 128 * 1024 + 2 * 512, 0),
        &READY,
    )
}
fn empty_plan(
    f: &Fixture,
    r: &IngestRecovery,
) -> Result<(CompactedEmptyInitialRetirement, OwnedPermit), AdmissionError> {
    let lineage = empty_lineage(f, r)?;
    let plan = {
        let context = r.compacted_empty_initial_attempt(&literal_handoff(f), &lineage, &READY)?;
        let effects = grant(&f.budget, 40 * MIB, 0);
        let ancestor =
            context.compacted_initial_ancestor(AdmissionLimits::default(), &effects, &READY)?;
        let proof = resolve(f, ancestor.resolution(&READY)?)?;
        context.reconstruct_retirement(&proof, &READY)?
    };
    Ok((plan, lineage.into_metadata_permit()))
}
fn close_grant(f: &Fixture) -> OwnedPermit {
    grant(&f.budget, COMPACTED_EMPTY_DATA_BYTES, 4096)
}
fn literal_closing() -> [u8; 27] {
    let mut marker = [0; 27];
    marker[..2].copy_from_slice(&[2, 49]);
    marker[26] = 1;
    marker
}
fn closed_rows(before: &Rows) -> Rows {
    let mut rows = before.clone();
    let mut key = vec![1, 49];
    key.extend_from_slice(&[7; 16]);
    key.extend_from_slice(&2_u64.to_be_bytes());
    let row = rows
        .iter_mut()
        .find(|(cf, k, _)| cf == "receipts" && *k == key)
        .unwrap();
    assert_eq!(row.2, [vec![1, 49], vec![0; 24]].concat());
    row.2 = literal_closing().to_vec();
    rows
}
fn empty_digest(f: &Fixture) -> [u8; 32] {
    let mut transcript = b"bigbrotr/initial-empty-attempt-closure/1\0".to_vec();
    transcript.extend_from_slice(&Sha256::digest(literal_handoff(f)));
    transcript.extend_from_slice(&Sha256::digest(literal_manifests().1));
    transcript.extend_from_slice(&[7; 16]);
    for n in [2_u64, 0, 0] {
        transcript.extend_from_slice(&n.to_be_bytes());
    }
    Sha256::digest(transcript).into()
}
fn set_marker(r: &IngestRecovery, bytes: &[u8]) {
    r.store
        .db
        .put_cf(
            r.store.db.cf_handle("receipts").unwrap(),
            producer_key(2).encode().unwrap().as_bytes(),
            bytes,
        )
        .unwrap();
}

#[test]
fn compacted_empty_exact_one_write_preserves_science_and_fresh_closing_retry() {
    for remaining in [4, 2, 0] {
        let f = prepared_fixture();
        let mut r = f.reopen();
        install(&f, &r, remaining);
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let files = inputs(&f.path());
        let (plan, metadata) = empty_plan(&f, &r).unwrap();
        assert_eq!(plan.handoff().as_bytes(), literal_handoff(&f));
        assert_eq!(
            (plan.issued_through(), plan.last_complete_sequence()),
            (4, 9)
        );
        assert_eq!(plan.original_closure_digest(), f.closure);
        assert_eq!(plan.closure_digest(), empty_digest(&f));
        assert!(!plan.initially_closing());
        assert!(
            !r.validate_compacted_empty_initial_retirement(&plan, &metadata, &READY)
                .unwrap()
        );
        assert!(
            r.empty_initial_attempt(&literal_handoff(&f))
                .unwrap()
                .historical()
                .observe_closed_producer(grant(&f.budget, 4096, 4096))
                .is_err()
        );
        drop(
            r.close_compacted_empty_initial_attempt(&plan, close_grant(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert!(
            r.validate_compacted_empty_initial_retirement(&plan, &metadata, &READY)
                .unwrap()
        );
        assert_eq!(kv(&r.store), closed_rows(&before));
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
        assert_eq!(inputs(&f.path()), files);
        {
            let view = plan.predecessor(&r, &metadata, &READY).unwrap();
            let proof = resolve(&f, view.resolution(&READY).unwrap()).unwrap();
            assert!(InitialHandoff::from_compacted(&proof, f.next, &READY).is_err());
            let lineage = empty_lineage(&f, &r).unwrap();
            let context = r
                .compacted_empty_initial_attempt(&literal_handoff(&f), &lineage, &READY)
                .unwrap();
            assert!(context.reconstruct_retirement(&proof, &READY).is_err());
        }
        drop(
            r.close_compacted_empty_initial_attempt(&plan, close_grant(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
        assert!(r.historical_initial(&literal_handoff(&f)).is_err());
        assert!(
            RetainedLineage::inspect_initial_preparation(
                &r,
                grant(&f.budget, 256 * 1024, 0),
                &READY
            )
            .is_err()
        );
        drop(r);
        let mut r = f.reopen();
        let (closing, retained) = empty_plan(&f, &r).unwrap();
        assert!(closing.initially_closing());
        drop(
            r.close_compacted_empty_initial_attempt(&closing, close_grant(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert!(
            r.validate_compacted_empty_initial_retirement(&closing, &retained, &READY)
                .unwrap()
        );
        assert_eq!(kv(&r.store), closed_rows(&before));
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
        assert_eq!(inputs(&f.path()), files);
    }
}

#[test]
fn compacted_empty_requires_exact_canonical_origin_and_empty_two_entry_shape() {
    let f = prepared_fixture();
    let r = f.reopen();
    let original = empty_lineage(&f, &r).unwrap();
    assert!(!original.has_compacted_initial());
    assert!(
        r.compacted_empty_initial_attempt(&literal_handoff(&f), &original, &READY)
            .is_err()
    );
    drop(original);
    install(&f, &r, 0);
    let pending = lineage(&f, &r, Some(3));
    assert_eq!(pending.entries().len(), 2);
    assert!(
        r.compacted_empty_initial_attempt(&literal_handoff(&f), &pending, &READY)
            .is_err()
    );
    let a = empty_lineage(&f, &r).unwrap();
    let b = empty_lineage(&f, &r).unwrap();
    let effects = grant(&f.budget, 40 * MIB, 0);
    let context = r
        .compacted_empty_initial_attempt(&literal_handoff(&f), &a, &READY)
        .unwrap();
    let other = r
        .compacted_empty_initial_attempt(&literal_handoff(&f), &b, &READY)
        .unwrap();
    let ancestor = context
        .compacted_initial_ancestor(AdmissionLimits::default(), &effects, &READY)
        .unwrap();
    let proof = resolve(&f, ancestor.resolution(&READY).unwrap()).unwrap();
    assert!(other.reconstruct_retirement(&proof, &READY).is_err());
    let plan = context.reconstruct_retirement(&proof, &READY).unwrap();
    // A real native +1 close invalidates the old canonical construction origin;
    // only the already sealed plan may recognize its exact one-write completion.
    set_marker(&r, &literal_closing());
    assert!(context.reconstruct_retirement(&proof, &READY).is_err());
    assert!(
        r.validate_compacted_empty_initial_retirement(&plan, &close_grant(&f), &READY)
            .unwrap()
    );
    let (closing, _metadata) = empty_plan(&f, &r).unwrap();
    set_marker(&r, &[vec![1, 49], vec![0; 24]].concat());
    assert!(
        r.validate_compacted_empty_initial_retirement(&closing, &close_grant(&f), &READY)
            .is_err()
    );
}

#[test]
fn compacted_empty_rejects_foreign_root_and_changed_census_without_writes() {
    for fault in [
        "root",
        "same-bytes-plus-one",
        "floor",
        "input-inode",
        "input-content",
        "target-input",
        "unknown",
    ] {
        let f = prepared_fixture();
        let mut r = f.reopen();
        install(&f, &r, 4);
        let (mut plan, _metadata) = empty_plan(&f, &r).unwrap();
        let foreign = prepared_fixture();
        let other = foreign.reopen();
        install(&foreign, &other, 4);
        let foreign_plan = (fault == "root").then(|| empty_plan(&foreign, &other).unwrap());
        match fault {
            "root" => {
                assert_eq!(kv(&r.store), kv(&other.store));
                assert_eq!(
                    r.store.db.latest_sequence_number(),
                    other.store.db.latest_sequence_number()
                );
                plan = foreign_plan.as_ref().unwrap().0.clone();
            }
            "same-bytes-plus-one" => set_marker(&r, &value(&r.store, producer_key(2))),
            "floor" => {
                let mut bytes = floor_bytes(&f);
                bytes[107] ^= 1;
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        producer_key(1).encode().unwrap().as_bytes(),
                        bytes,
                    )
                    .unwrap();
            }
            "input-inode" | "input-content" => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                if fault == "input-content" {
                    bytes[0] ^= 1;
                    std::fs::write(path, bytes).unwrap();
                } else {
                    let replacement = f.root.path().join("replacement");
                    std::fs::write(&replacement, bytes).unwrap();
                    std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    std::fs::rename(replacement, path).unwrap();
                }
            }
            "target-input" => {
                let path = f.path().join("producer-inputs-v1").join(input_name(2, 1));
                std::fs::write(&path, b"unbound target").unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            "unknown" => r.store.write_uncertain = true,
            _ => unreachable!(),
        }
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let files = inputs(&f.path());
        assert!(
            r.close_compacted_empty_initial_attempt(&plan, close_grant(&f), &READY, || Ok(()))
                .is_err(),
            "{fault}"
        );
        assert_eq!(kv(&r.store), before, "{fault}");
        assert_eq!(r.store.db.latest_sequence_number(), sequence, "{fault}");
        assert_eq!(inputs(&f.path()), files, "{fault}");
    }
}

#[test]
fn compacted_empty_full_effect_census_and_zero_target_are_required() {
    for fault in [
        "missing-pending",
        "missing-index",
        "target-issued",
        "target-source",
        "malformed-floor",
        "corrupt-residual",
    ] {
        let f = prepared_fixture();
        let r = f.reopen();
        install(&f, &r, 4);
        match fault {
            "missing-pending" | "missing-index" => {
                let prefix = if fault == "missing-pending" {
                    0x21
                } else {
                    0x04
                };
                let rows = kv(&r.store);
                let (cf, key, _) = rows
                    .iter()
                    .find(|(_, key, _)| key[..2] == [1, prefix])
                    .unwrap();
                r.store
                    .db
                    .delete_cf(r.store.db.cf_handle(cf).unwrap(), key)
                    .unwrap();
            }
            "target-issued" => {
                let mut marker = [0; 26];
                marker[..2].copy_from_slice(&[1, 49]);
                marker[9] = 1;
                set_marker(&r, &marker);
            }
            "target-source" => {
                r.store
                    .db
                    .put(
                        source_key(2).encode().unwrap().as_bytes(),
                        [1, 2, 0, 0, 0, 0, 0, 0, 0, 1],
                    )
                    .unwrap();
            }
            "malformed-floor" => {
                let mut floor = floor_bytes(&f);
                floor[25] = 3;
                r.store
                    .db
                    .put_cf(
                        r.store.db.cf_handle("receipts").unwrap(),
                        producer_key(1).encode().unwrap().as_bytes(),
                        floor,
                    )
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
        let files = inputs(&f.path());
        assert!(empty_plan(&f, &r).is_err(), "{fault}");
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&f.path()), files);
    }
}

#[test]
fn compacted_empty_grant_and_delivery_failure_preserve_exact_close_retry() {
    for fault in [
        "metadata",
        "native",
        "cancel",
        "authority1",
        "authority3",
        "authority4",
        "authority5",
    ] {
        let f = prepared_fixture();
        let mut r = f.reopen();
        install(&f, &r, 0);
        let (plan, metadata) = empty_plan(&f, &r).unwrap();
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let mut calls = 0;
        let cancelled = AtomicBool::new(fault == "cancel");
        let result = r.close_compacted_empty_initial_attempt(
            &plan,
            grant(
                &f.budget,
                COMPACTED_EMPTY_DATA_BYTES - u64::from(fault == "metadata"),
                4096 - u64::from(fault == "native"),
            ),
            &cancelled,
            || {
                calls += 1;
                if fault == format!("authority{calls}") {
                    Err(BootstrapError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err(), "{fault}");
        let closed = matches!(fault, "authority4" | "authority5");
        assert_eq!(
            kv(&r.store),
            if closed {
                closed_rows(&before)
            } else {
                before.clone()
            }
        );
        assert_eq!(
            r.store.db.latest_sequence_number(),
            sequence + u64::from(closed)
        );
        assert_eq!(
            r.validate_compacted_empty_initial_retirement(&plan, &metadata, &READY)
                .unwrap(),
            closed
        );
        drop(
            r.close_compacted_empty_initial_attempt(&plan, close_grant(&f), &READY, || Ok(()))
                .unwrap(),
        );
        assert_eq!(kv(&r.store), closed_rows(&before));
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
    }
}

#[test]
fn compacted_empty_original_cancellation_cannot_be_replaced_by_a_fresh_flag() {
    let f = prepared_fixture();
    let r = f.reopen();
    install(&f, &r, 0);
    let lineage = empty_lineage(&f, &r).unwrap();
    let original = AtomicBool::new(false);
    let context = r
        .compacted_empty_initial_attempt(&literal_handoff(&f), &lineage, &original)
        .unwrap();
    let effects = grant(&f.budget, 40 * MIB, 0);
    let ancestor = context
        .compacted_initial_ancestor(AdmissionLimits::default(), &effects, &READY)
        .unwrap();
    let proof = resolve(&f, ancestor.resolution(&READY).unwrap()).unwrap();
    original.store(true, Ordering::Relaxed);
    assert!(context.reconstruct_retirement(&proof, &READY).is_err());
    let (plan, metadata) = empty_plan(&f, &r).unwrap();
    let view_flag = AtomicBool::new(false);
    let view = plan.predecessor(&r, &metadata, &view_flag).unwrap();
    let mut cursor = view.resolution(&READY).unwrap();
    let first = &f.originals[0];
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    assert!(
        cursor
            .record(
                CompactedOutcomeRow {
                    request: request(7, 1),
                    digest: input_digest(first),
                    kind: NativeOutcomeKind::NewFacts,
                    policy_epoch: PolicyEpoch::new(1),
                    bytes: &first.outcome,
                    input_bytes: first.input.len() as u32
                },
                AdmissionLimits::default(),
                &effects,
                &READY,
                || {
                    view_flag.store(true, Ordering::Relaxed);
                    Ok(())
                }
            )
            .is_err()
    );
    assert!(view.validate(&r, &READY).is_err());
    assert_eq!(kv(&r.store), before);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}
