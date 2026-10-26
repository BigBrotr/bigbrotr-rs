//! Native component checks. Terminal kind and confirmed PG preparation are
//! explicit test premises, not connected authority or a delivered writer.
use super::*;
use crate::storage::rocks::store::{
    EmptySuccessorRetirement, SuccessorCandidate, SuccessorHandoff,
};
use bigbrotr_model::PolicyEpoch;
use std::sync::atomic::Ordering;

fn terminal_second(f: &Fixture) -> IngestRecovery {
    let r = f.reopen();
    // The independently qualified initial closer emits this exact marker. No
    // SQL retirement, activation or source proof is inferred from these bytes.
    put(&r.store, producer_key(2), &marker(true, 0));
    r
}

fn local_plan(
    f: &Fixture,
    c: &SuccessorCandidate,
    kind: PredecessorTerminalKind,
) -> (RetainedLineage, SuccessorHandoff) {
    let l = inspect(f, c.recovery(), None).unwrap();
    let plan = {
        let proof = replay(
            f,
            c.recovery(),
            &l,
            c.recovery().manifest().identity().owner_epoch.get(),
            Some(kind),
        )
        .unwrap();
        c.handoff(&proof, PolicyEpoch::new(7), &NOT_CANCELLED)
            .unwrap()
    };
    (l, plan)
}

fn literal_next(previous: &[u8], c: &SuccessorCandidate) -> Vec<u8> {
    let mut bytes = previous.to_vec();
    bytes[2..4].copy_from_slice(&3_u16.to_be_bytes());
    for offset in [26, 54] {
        let old = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        bytes[offset..offset + 8].copy_from_slice(&(old + 1).to_be_bytes());
    }
    bytes[62..78].copy_from_slice(c.process_incarnation().as_bytes());
    bytes[94..102].copy_from_slice(&7_u64.to_be_bytes());
    for pair in bytes[104..].as_chunks_mut::<18>().0.iter_mut() {
        let kind =
            DictionaryKind::try_from(u16::from_be_bytes(pair[..2].try_into().unwrap())).unwrap();
        pair[2..].copy_from_slice(c.allocation_namespaces().domain(kind).unwrap().as_bytes());
    }
    bytes
}

fn literal_record(
    previous: &[u8],
    next: &[u8],
    kind: u8,
    incoming: &[u8],
    issued: u64,
    cut: u64,
    digest: [u8; 32],
) -> Vec<u8> {
    let mut bytes = vec![2, 0x0a, kind];
    bytes.extend([7; 16]);
    bytes.extend(&previous[54..62]);
    bytes.extend(issued.to_be_bytes());
    bytes.extend(cut.to_be_bytes());
    bytes.extend(digest);
    bytes.extend(Sha256::digest(incoming));
    bytes.extend((previous.len() as u16).to_be_bytes());
    bytes.extend(previous);
    bytes.extend((next.len() as u16).to_be_bytes());
    bytes.extend(next);
    bytes
}

fn replace(rows: &mut Rows, key: IngestKey, value: &[u8]) {
    let key = key.encode().unwrap();
    rows.retain(|(cf, bytes, _)| cf != key.column_family().name() || bytes != key.as_bytes());
    rows.push((
        key.column_family().name().to_owned(),
        key.as_bytes().to_vec(),
        value.to_vec(),
    ));
    rows.sort();
}

#[test]
fn successor_candidate_applies_exact_four_puts_from_both_terminal_kinds_and_later_frontier() {
    for variant in 0..3 {
        let f = prepared_fixture();
        let r = if variant == 2 {
            let r = valid_empty_third(&f);
            correction(&f, &r, false);
            r
        } else {
            terminal_second(&f)
        };
        let epoch = r.manifest().identity().owner_epoch.get();
        let kind = if variant == 0 {
            PredecessorTerminalKind::UnactivatedEmpty
        } else {
            PredecessorTerminalKind::Activated
        };
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let originals = inputs(&f.path());
        let previous = r.manifest().encode().as_bytes().to_vec();
        let incoming = value(
            &r.store,
            IngestKey::OwnerHandoff {
                dataset_id: f.previous.identity().dataset_id,
                owner_resource_id: f.previous.identity().owner_resource_id,
                predecessor_epoch: OwnerEpoch::new(epoch - 1).unwrap(),
            },
        );
        let count = u64::from(variant == 2);
        let digest = epoch_digest(&f, &r, epoch, count, count, variant == 0);
        let mut c = SuccessorCandidate::new(r).unwrap();
        let (l, p) = local_plan(&f, &c, kind);
        let next = literal_next(&previous, &c);
        let expected = literal_record(
            &previous,
            &next,
            if variant == 0 { 2 } else { 1 },
            &incoming,
            count,
            count,
            digest,
        );
        assert_eq!(p.encode().as_bytes(), expected);
        assert_eq!(p.record().fields().next_manifest.encode().as_bytes(), next);
        for epoch in 1..=epoch {
            let old = l.entry(epoch).unwrap().manifest();
            assert_ne!(old.identity().process_incarnation, c.process_incarnation());
            for kind in 1..=9 {
                let kind = DictionaryKind::try_from(kind).unwrap();
                if let Some(domain) = old.allocation_namespaces().domain(kind) {
                    assert_ne!(Some(domain), c.allocation_namespaces().domain(kind));
                } else {
                    assert!(c.allocation_namespaces().domain(kind).is_none());
                }
            }
        }
        let observed = c
            .apply_handoff(&l, &p, grant(&f.budget, 8192, 8192), &NOT_CANCELLED, || {
                Ok(())
            })
            .unwrap();
        assert_eq!(observed.record(), p.record());
        drop(observed);
        let mut wanted = before;
        replace(&mut wanted, p.record().key(), &expected);
        replace(&mut wanted, IngestKey::Manifest, &next);
        replace(
            &mut wanted,
            source_key(epoch + 1),
            &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0],
        );
        replace(&mut wanted, producer_key(epoch + 1), &marker(false, 0));
        assert_eq!(kv(&c.recovery().store), wanted);
        assert_eq!(c.recovery().store.db.latest_sequence_number(), sequence + 4);
        assert_eq!(inputs(&f.path()), originals);
        assert!(l.validate(c.recovery(), &NOT_CANCELLED).is_err());
        c.apply_handoff(&l, &p, grant(&f.budget, 8192, 8192), &NOT_CANCELLED, || {
            Ok(())
        })
        .unwrap();
        assert_eq!(kv(&c.recovery().store), wanted);
        assert_eq!(c.recovery().store.db.latest_sequence_number(), sequence + 4);
        assert!(
            c.recovery()
                .store
                .require_admission_owner(Some(at(epoch + 1, 1)))
                .is_err()
        );
    }
}

#[test]
fn successor_gap_reconstructs_exact_intent_without_adopting_candidate() {
    let f = prepared_fixture();
    let c = SuccessorCandidate::new(terminal_second(&f)).unwrap();
    let (l, local) = local_plan(&f, &c, PredecessorTerminalKind::UnactivatedEmpty);
    {
        let proof = replay(
            &f,
            c.recovery(),
            &l,
            2,
            Some(PredecessorTerminalKind::UnactivatedEmpty),
        )
        .unwrap();
        assert!(
            SuccessorHandoff::reconstruct_gap(&proof, *local.record(), &NOT_CANCELLED).is_err()
        );
    }
    drop(l);
    let l = inspect(&f, c.recovery(), Some(3)).unwrap();
    let gap = {
        let proof = replay(
            &f,
            c.recovery(),
            &l,
            2,
            Some(PredecessorTerminalKind::UnactivatedEmpty),
        )
        .unwrap();
        assert!(
            c.handoff(&proof, PolicyEpoch::new(7), &NOT_CANCELLED)
                .is_err()
        );
        for mutation in 0..5 {
            let mut fields = *local.record().fields();
            match mutation {
                0 => fields.closure_digest[0] ^= 1,
                1 => fields.incoming_handoff_sha256[0] ^= 1,
                2 => {
                    let mut id = *fields.next_manifest.identity();
                    id.process_incarnation = f.previous.identity().process_incarnation;
                    fields.next_manifest = IngestManifest::for_repeatable_successor(
                        id,
                        *fields.next_manifest.allocation_namespaces(),
                    );
                }
                3 => {
                    let next = fields.next_manifest;
                    let domains: Vec<_> = (1..=9)
                        .filter_map(|number| {
                            let kind = DictionaryKind::try_from(number).unwrap();
                            next.allocation_namespaces().domain(kind).map(|domain| {
                                (
                                    kind,
                                    if kind == DictionaryKind::Event {
                                        f.previous.allocation_namespaces().domain(kind).unwrap()
                                    } else {
                                        domain
                                    },
                                )
                            })
                        })
                        .collect();
                    fields.next_manifest = IngestManifest::for_repeatable_successor(
                        *next.identity(),
                        AllocationNamespaces::from_entries(&domains).unwrap(),
                    );
                }
                _ => fields.producer_slot_id = [8; 16].try_into().unwrap(),
            }
            let record = SuccessionRecord::new(fields).unwrap();
            assert!(SuccessorHandoff::reconstruct_gap(&proof, record, &NOT_CANCELLED).is_err());
        }
        let gap =
            SuccessorHandoff::reconstruct_gap(&proof, *local.record(), &NOT_CANCELLED).unwrap();
        assert_eq!(gap.encode().as_bytes(), local.encode().as_bytes());
        assert!(c.require_handoff(&l, &gap, &NOT_CANCELLED).is_err());
        gap
    };
    let before = kv(&c.recovery().store);
    let sequence = c.recovery().store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    drop(c);
    let mut r = f.reopen();
    r.apply_successor_handoff(
        &l,
        &gap,
        grant(&f.budget, 8192, 8192),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(kv(&r.store).len(), before.len() + 3);
    let after = kv(&r.store);
    r.apply_successor_handoff(
        &l,
        &gap,
        grant(&f.budget, 8192, 8192),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(kv(&r.store), after);
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(inputs(&f.path()), originals);
    assert!(r.store.require_admission_owner(Some(at(3, 1))).is_err());
}

#[test]
fn successor_plans_bind_root_sequence_sidecars_and_future_capacity() {
    let f = prepared_fixture();
    let c = SuccessorCandidate::new(terminal_second(&f)).unwrap();
    let l = RetainedLineage::inspect(
        c.recovery(),
        None,
        grant(
            &f.budget,
            SuccessionProfile::for_recovery(2, None)
                .unwrap()
                .required_metadata_bytes(),
            0,
        ),
        &NOT_CANCELLED,
    )
    .unwrap();
    {
        let proof = replay(
            &f,
            c.recovery(),
            &l,
            2,
            Some(PredecessorTerminalKind::UnactivatedEmpty),
        )
        .unwrap();
        assert!(matches!(
            c.handoff(&proof, PolicyEpoch::new(7), &NOT_CANCELLED),
            Err(BootstrapError::InsufficientReservation)
        ));
    }
    drop(l);
    let (l, p) = local_plan(&f, &c, PredecessorTerminalKind::UnactivatedEmpty);
    let other = prepared_fixture();
    let foreign = terminal_second(&other);
    assert_eq!(kv(&foreign.store), kv(&c.recovery().store));
    assert_eq!(
        foreign.store.db.latest_sequence_number(),
        c.recovery().store.db.latest_sequence_number()
    );
    assert!(
        foreign
            .validate_successor_handoff(&l, &p, &NOT_CANCELLED)
            .is_err()
    );
    drop(foreign);
    let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
    let original = std::fs::read(&path).unwrap();
    let sequence = c.recovery().store.db.latest_sequence_number();
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    assert_eq!(c.recovery().store.db.latest_sequence_number(), sequence);
    assert!(c.require_handoff(&l, &p, &NOT_CANCELLED).is_err());
    let replacement = f.root.path().join("replacement-input");
    std::fs::write(&replacement, original).unwrap();
    std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::rename(replacement, &path).unwrap();
    // File identity metadata is part of the census too; restoring bytes is not
    // a resurrection of the old witness.
    assert!(c.require_handoff(&l, &p, &NOT_CANCELLED).is_err());
    drop(l);
    let (l, p) = local_plan(&f, &c, PredecessorTerminalKind::UnactivatedEmpty);
    let before = kv(&c.recovery().store);
    put(&c.recovery().store, producer_key(2), &marker(true, 0));
    assert_eq!(kv(&c.recovery().store), before);
    assert!(c.require_handoff(&l, &p, &NOT_CANCELLED).is_err());
    let high = prepared_fixture();
    let r = extend_to(&high, 255);
    // Structural resource boundary only; these test-only high-epoch receipts
    // are not claimed as positively resolved ancestors.
    assert!(matches!(
        SuccessorCandidate::new(r),
        Err(BootstrapError::InsufficientReservation)
    ));
}

#[test]
fn successor_handoff_last_guard_refuses_and_post_dispatch_failure_keeps_exact_retry() {
    for fault in 0..6 {
        let f = prepared_fixture();
        let mut c = SuccessorCandidate::new(terminal_second(&f)).unwrap();
        let (l, p) = local_plan(&f, &c, PredecessorTerminalKind::UnactivatedEmpty);
        let before = kv(&c.recovery().store);
        let sequence = c.recovery().store.db.latest_sequence_number();
        let originals = inputs(&f.path());
        let cancelled = AtomicBool::new(false);
        let mut calls = 0;
        let result = c.apply_handoff(
            &l,
            &p,
            grant(
                &f.budget,
                if fault == 0 { 8191 } else { 8192 },
                if fault == 5 { 8191 } else { 8192 },
            ),
            &cancelled,
            || {
                calls += 1;
                if (fault == 1 && calls == 1)
                    || (fault == 2 && calls == 2)
                    || (fault == 3 && calls == 3)
                {
                    return Err(BootstrapError::AuthorityUnavailable);
                }
                if fault == 4 && calls == 2 {
                    cancelled.store(true, Ordering::Relaxed);
                }
                Ok(())
            },
        );
        assert!(result.is_err());
        if fault == 3 {
            assert_eq!(c.recovery().store.db.latest_sequence_number(), sequence + 4);
            let after = kv(&c.recovery().store);
            c.apply_handoff(&l, &p, grant(&f.budget, 8192, 8192), &NOT_CANCELLED, || {
                Ok(())
            })
            .unwrap();
            assert_eq!(kv(&c.recovery().store), after);
            assert_eq!(c.recovery().store.db.latest_sequence_number(), sequence + 4);
        } else {
            assert_eq!(kv(&c.recovery().store), before);
            assert_eq!(c.recovery().store.db.latest_sequence_number(), sequence);
        }
        assert_eq!(inputs(&f.path()), originals);
    }
}

#[test]
fn successor_empty_closes_once_and_rebuilds_after_reopen_without_future_epoch_capacity() {
    let f = prepared_fixture();
    let mut c = SuccessorCandidate::new(terminal_second(&f)).unwrap();
    let (l, p) = local_plan(&f, &c, PredecessorTerminalKind::UnactivatedEmpty);
    c.apply_handoff(&l, &p, grant(&f.budget, 8192, 8192), &NOT_CANCELLED, || {
        Ok(())
    })
    .unwrap();
    drop(l);
    drop(c);
    let mut r = f.reopen();
    let l = RetainedLineage::inspect(
        &r,
        None,
        grant(
            &f.budget,
            SuccessionProfile::for_recovery(3, None)
                .unwrap()
                .required_metadata_bytes(),
            0,
        ),
        &NOT_CANCELLED,
    )
    .unwrap();
    let empty = {
        let proof = replay(&f, &r, &l, 2, None).unwrap();
        let empty = EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).unwrap();
        assert!(!empty.initially_closing());
        assert_eq!(empty.closure_digest(), epoch_digest(&f, &r, 3, 0, 0, true));
        empty
    };
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    let observed = r
        .close_empty_successor(
            &l,
            &empty,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    assert_eq!(observed.closure_digest(), empty.closure_digest());
    assert_eq!(observed.record(), empty.record());
    drop(observed);
    let mut wanted = before;
    replace(&mut wanted, producer_key(3), &marker(true, 0));
    assert_eq!(kv(&r.store), wanted);
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
    assert!(l.validate(&r, &NOT_CANCELLED).is_err());
    r.close_empty_successor(
        &l,
        &empty,
        grant(&f.budget, 4096, 4096),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
    drop(l);
    drop(r);
    let mut r = f.reopen();
    let l = inspect(&f, &r, None).unwrap();
    let empty = {
        let proof = replay(&f, &r, &l, 2, None).unwrap();
        let empty = EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).unwrap();
        assert!(empty.initially_closing());
        empty
    };
    r.close_empty_successor(
        &l,
        &empty,
        grant(&f.budget, 4096, 4096),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(kv(&r.store), wanted);
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
    assert_eq!(inputs(&f.path()), originals);
}

#[test]
fn successor_empty_refuses_stale_marker_or_input_and_preserves_full_spool() {
    let f = zero_fixture(64);
    let mut c = SuccessorCandidate::new(terminal_second(&f)).unwrap();
    let (l, p) = local_plan(&f, &c, PredecessorTerminalKind::UnactivatedEmpty);
    c.apply_handoff(&l, &p, grant(&f.budget, 8192, 8192), &NOT_CANCELLED, || {
        Ok(())
    })
    .unwrap();
    drop(l);
    drop(c);
    let mut r = f.reopen();
    let l = inspect(&f, &r, None).unwrap();
    let empty = {
        let proof = replay(&f, &r, &l, 2, None).unwrap();
        EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).unwrap()
    };
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    assert_eq!(originals.len(), 64);
    for (data, native) in [(4095, 4096), (4096, 4095)] {
        assert!(
            r.close_empty_successor(
                &l,
                &empty,
                grant(&f.budget, data, native),
                &NOT_CANCELLED,
                || Ok(())
            )
            .is_err()
        );
    }
    assert!(
        r.close_empty_successor(
            &l,
            &empty,
            grant(&f.budget, 4096, 4096),
            &AtomicBool::new(true),
            || Ok(())
        )
        .is_err()
    );
    assert_eq!(kv(&r.store), before);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
    r.close_empty_successor(
        &l,
        &empty,
        grant(&f.budget, 4096, 4096),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(inputs(&f.path()), originals);
    let closed = kv(&r.store);
    put(&r.store, source_key(3), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(kv(&r.store), closed);
    assert!(
        r.validate_empty_successor_retirement(&l, &empty, &NOT_CANCELLED)
            .is_err()
    );
    drop(l);
    let l = inspect(&f, &r, None).unwrap();
    let empty = {
        let proof = replay(&f, &r, &l, 2, None).unwrap();
        EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).unwrap()
    };
    stored_input(&f, 3, 1, &originals[0].1);
    let injected = inputs(&f.path());
    assert_eq!(injected.len(), 65);
    assert!(
        r.validate_empty_successor_retirement(&l, &empty, &NOT_CANCELLED)
            .is_err()
    );
    assert_eq!(inputs(&f.path()), injected);
    assert_eq!(kv(&r.store), closed);
}

#[path = "successor_writer_tests.rs"]
mod successor_writer_tests;
