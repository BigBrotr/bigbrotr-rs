//! Native-only retirement purpose. Confirmed activation, closed SQL source and
//! existing SQL copies are explicit caller premises, not inferred by this test.
//! No SQL retirement, workflow completion or next writer is exercised here.
use super::*;
use crate::storage::rocks::store::{
    NativeSuccessorRetirement, PendingSuccessorRetirementRequest, ResolvedSuccessorRetirement,
    SuccessorRetirementResolution,
};

fn retirement(f: &Fixture, r: &IngestRecovery) -> NativeSuccessorRetirement {
    NativeSuccessorRetirement::inspect(r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED).unwrap()
}
fn observe(f: &Fixture, r: &IngestRecovery, owner: &mut NativeSuccessorRetirement) {
    owner
        .observe_closed(r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(()))
        .unwrap();
}
fn retirement_preview<'s, 'a>(
    f: &Fixture,
    cursor: &'s mut SuccessorRetirementResolution<'a>,
) -> Result<PendingSuccessorRetirementRequest<'s, 'a>, ProducerRecoveryError> {
    cursor.preview_next(
        input_hash(f, cursor.next_request().unwrap()),
        recovery_limits(),
        grant(&f.budget, 3 * MIB, 0),
        grant(&f.budget, 40 * MIB, 0),
        || Ok(()),
    )
}
fn complete<'a>(
    f: &Fixture,
    mut cursor: SuccessorRetirementResolution<'a>,
) -> Result<ResolvedSuccessorRetirement<'a>, ProducerRecoveryError> {
    while cursor.current_epoch().is_some() {
        while cursor.next_request().is_some() {
            let expected = cursor.next_request().unwrap();
            let pending = retirement_preview(f, &mut cursor)?;
            assert_eq!(pending.request(), expected);
            assert_eq!(pending.resolved().native_outcome().request(), expected);
            pending.accept(|| Ok(())).map_err(AdmissionError::from)?;
        }
        cursor.finish_epoch().map_err(AdmissionError::from)?;
    }
    let limits = AdmissionLimits::default();
    cursor.finish(
        limits,
        grant(
            &f.budget,
            limits.required_dictionary_replay_data_bytes().unwrap(),
            0,
        ),
    )
}

#[test]
fn successor_retirement_requires_existing_closing_without_tail_or_legacy_adoption() {
    {
        let f = prepared_fixture();
        drop(active(&f, false));
        let r = f.reopen();
        let before = snapshot(&f, &r.store);
        assert!(matches!(
            NativeSuccessorRetirement::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED),
            Err(BootstrapError::StateMismatch)
        ));
        assert_eq!(snapshot(&f, &r.store), before, "OPEN0 cannot be normalized");
    }
    let f = prepared_fixture();
    let r = terminal_second(&f);
    let before = snapshot(&f, &r.store);
    assert!(matches!(
        NativeSuccessorRetirement::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED),
        Err(BootstrapError::InvalidIntent)
    ));
    assert_eq!(snapshot(&f, &r.store), before);
    drop(r);

    let mut writer = active(&f, false);
    let open = snapshot(&f, writer.store());
    let staged = event(&f, 3, 1, true);
    let permit = grant(&f.budget, 40 * MIB, 10 * MIB);
    writer
        .stage_event(&staged, AdmissionLimits::default(), &permit, &NOT_CANCELLED)
        .unwrap();
    drop(staged);
    drop(permit);
    drop(writer);
    let mut r = f.reopen();
    let staged = snapshot(&f, &r.store);
    assert_eq!((staged.0.clone(), staged.1), (open.0, open.1));
    assert_eq!(staged.2.len(), open.2.len() + 1);
    assert!(matches!(
        NativeSuccessorRetirement::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(snapshot(&f, &r.store), staged);
    // Genuine recovery may still close this unresolved producer. Retirement
    // does not inherit that mutation API and continues to refuse the tail.
    let mut recovery_owner = owner(&f, &r);
    close(&f, &mut r, &mut recovery_owner);
    drop(recovery_owner);
    let closed = snapshot(&f, &r.store);
    assert_eq!(closed.1, staged.1 + 1);
    assert!(matches!(
        NativeSuccessorRetirement::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(snapshot(&f, &r.store), closed);
}

#[test]
fn successor_retirement_replays_three_and_four_with_exact_event_discrimination_and_no_writes() {
    for later in [false, true] {
        for kind in 0..4 {
            let f = prepared_fixture();
            let mut writer = active(&f, later);
            let epoch = writer.manifest().identity().owner_epoch.get();
            if kind == 1 {
                append_and_check(&f, &mut writer, 1); // independently literal Evidence receipt
            } else if kind >= 2 {
                let result = writer
                    .admit_event(
                        event(&f, epoch, 1, kind == 2),
                        ObservedTime::new(77),
                        false,
                        AdmissionLimits::default(),
                        grant(&f.budget, 40 * MIB, 10 * MIB),
                        &NOT_CANCELLED,
                        || Ok(()),
                    )
                    .unwrap();
                if kind == 2 {
                    assert!(matches!(result.outcome(), AdmissionOutcome::Rejected(_)));
                } else {
                    let AdmissionOutcome::Durable(receipt) = result.outcome() else {
                        panic!("known signed historical duplicate")
                    };
                    assert!(receipt.batch().is_none());
                }
            }
            drop(writer);
            let mut r = f.reopen();
            let mut barrier = owner(&f, &r);
            close(&f, &mut r, &mut barrier);
            drop(barrier);
            let before = snapshot(&f, &r.store);
            let mut observed = retirement(&f, &r);
            let pointer = observed.entry(1).unwrap() as *const _;
            let reserved = f.budget.snapshot();
            assert!(observed.head().is_closing());
            assert_eq!(observed.producer_slot_id(), at(epoch, 1).producer_slot_id);
            assert_eq!(observed.issued_through(), u64::from(kind != 0));
            observed.validate(&r, &NOT_CANCELLED).unwrap();
            observe(&f, &r, &mut observed);
            let retained = f.budget.snapshot();
            assert_eq!(
                retained.used().data_bytes(),
                reserved.used().data_bytes() + 4096
            );
            assert_eq!(
                retained.used().native_bytes(),
                reserved.used().native_bytes() + 4096
            );
            assert_eq!(observed.entry(1).unwrap() as *const _, pointer);
            let other = retirement(&f, &r);
            let cursor = observed.resolution(&r, &NOT_CANCELLED).unwrap();
            assert_eq!(cursor.owner().entry(1).unwrap() as *const _, pointer);
            {
                let proof = complete(&f, cursor).unwrap();
                assert!(proof.matches_owner(proof.owner()));
                assert_eq!(proof.owner().entry(1).unwrap() as *const _, pointer);
                assert!(!proof.matches_owner(&other));
                assert_eq!(proof.verified_through(), epoch);
                let summary = proof.last_epoch();
                assert_eq!(summary.terminal_kind, RetainedTerminalKind::Activated);
                assert_eq!(summary.resolved_requests, u64::from(kind != 0));
                assert_eq!(summary.last_complete_sequence, u64::from(kind == 1));
                assert_eq!(summary.event_request_mask, u64::from(kind >= 2));
                assert_eq!(
                    summary.digest,
                    epoch_digest(
                        &f,
                        &r,
                        epoch,
                        u64::from(kind != 0),
                        u64::from(kind == 1),
                        false
                    )
                );
                proof.validate(&NOT_CANCELLED).unwrap();
                let records: Vec<_> = (1..=u64::from(kind != 0))
                    .map(|n| {
                        (
                            n,
                            *input_hash(&f, at(epoch, n)).as_bytes(),
                            Sha256::digest(value(
                                &r.store,
                                IngestKey::Request {
                                    request_id: at(epoch, n),
                                },
                            ))
                            .into(),
                        )
                    })
                    .collect();
                assert!(proof.matches_recorded(epoch, records.iter().copied()));
                let mut false_records = records;
                if let Some(row) = false_records.first_mut() {
                    row.2[0] ^= 1;
                } else {
                    false_records.push((1, [0; 32], [0; 32]));
                }
                assert!(!proof.matches_recorded(epoch, false_records));
                assert_eq!(snapshot(&f, &r.store), before);
            }
            drop(other);
            assert_eq!(observed.entry(1).unwrap() as *const _, pointer);
            assert_eq!(f.budget.snapshot(), retained);
            assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
            assert_eq!(
                observed.observe_closed(
                    &r,
                    grant(&f.budget, 4096, 4096),
                    &NOT_CANCELLED,
                    || Ok(())
                ),
                Err(BootstrapError::AuthorityUnavailable)
            );
            assert_eq!(snapshot(&f, &r.store), before);
        }
    }
}

#[test]
fn successor_retirement_failed_observation_keeps_one_allocation_and_cannot_rearm() {
    for fault in 0..5 {
        let f = prepared_fixture();
        let mut r = closed_recovery(&f);
        let mut observed = retirement(&f, &r);
        let allocation = observed.entry(1).unwrap() as *const _;
        let reserved = f.budget.snapshot();
        let cancelled = AtomicBool::new(fault == 1);
        if fault == 4 {
            r.store.write_uncertain = true;
        }
        let before = snapshot(&f, &r.store);
        let mut calls = 0;
        let result = observed.observe_closed(
            &r,
            grant(&f.budget, if fault == 0 { 4095 } else { 4096 }, 4096),
            &cancelled,
            || {
                calls += 1;
                if fault == 2 || (fault == 3 && calls == 2) {
                    Err(BootstrapError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(
            result,
            Err(match fault {
                0 => BootstrapError::InsufficientReservation,
                4 => BootstrapError::CommitUnknown,
                _ => BootstrapError::AuthorityUnavailable,
            })
        );
        assert_eq!(
            calls,
            match fault {
                2 => 1,
                3 => 2,
                _ => 0,
            }
        );
        assert_eq!(observed.entry(1).unwrap() as *const _, allocation);
        let retained = f.budget.snapshot();
        assert_eq!(
            retained.used().data_bytes(),
            reserved.used().data_bytes() + if fault == 0 { 0 } else { 4096 }
        );
        assert_eq!(
            retained.used().native_bytes(),
            reserved.used().native_bytes() + if fault == 0 { 0 } else { 4096 }
        );
        cancelled.store(false, Ordering::Relaxed);
        r.store.write_uncertain = false;
        assert!(observed.resolution(&r, &cancelled).is_err());
        assert_eq!(
            observed.observe_closed(&r, grant(&f.budget, 4096, 4096), &cancelled, || Ok(())),
            Err(BootstrapError::AuthorityUnavailable)
        );
        assert_eq!(f.budget.snapshot(), retained);
        assert_eq!(snapshot(&f, &r.store), before);
    }
}

#[test]
fn successor_retirement_lost_copy_step_cannot_advance_or_restart_replay() {
    for fault in 0..5 {
        let f = prepared_fixture();
        let r = closed_recovery(&f);
        let mut observed = retirement(&f, &r);
        observe(&f, &r, &mut observed);
        let cancelled = AtomicBool::new(false);
        let mut cursor = observed.resolution(&r, &cancelled).unwrap();
        let next = cursor.next_request();
        let pending = retirement_preview(&f, &mut cursor).unwrap();
        match fault {
            0 => drop(pending),
            1 => assert_eq!(
                pending.accept(|| Err(BootstrapError::AuthorityUnavailable)),
                Err(BootstrapError::AuthorityUnavailable)
            ),
            2 => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
                assert!(pending.accept(|| Ok(())).is_err());
            }
            3 => {
                cancelled.store(true, Ordering::Relaxed);
                assert!(pending.accept(|| Ok(())).is_err());
            }
            _ => {
                drop(pending);
                cursor.poison();
            }
        }
        let before = snapshot(&f, &r.store);
        cancelled.store(false, Ordering::Relaxed);
        assert_eq!(cursor.next_request(), next);
        assert!(retirement_preview(&f, &mut cursor).is_err());
        assert!(cursor.finish_epoch().is_err());
        drop(cursor);
        assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
        assert_eq!(snapshot(&f, &r.store), before);
    }
    // Dropping even an untouched cursor spends this owner, without SQL activity.
    let f = prepared_fixture();
    let r = closed_recovery(&f);
    let mut observed = retirement(&f, &r);
    observe(&f, &r, &mut observed);
    drop(observed.resolution(&r, &NOT_CANCELLED).unwrap());
    assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
}

#[test]
fn successor_retirement_refuses_future_dependency_before_existing_copy_acceptance() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    correction(&f, &r, true);
    let before = snapshot(&f, &r.store);
    let mut observed = retirement(&f, &r);
    observe(&f, &r, &mut observed);
    let mut cursor = observed.resolution(&r, &NOT_CANCELLED).unwrap();
    while cursor.current_epoch().unwrap() < 3 {
        while cursor.next_request().is_some() {
            retirement_preview(&f, &mut cursor)
                .unwrap()
                .accept(|| Ok(()))
                .unwrap();
        }
        cursor.finish_epoch().unwrap();
    }
    let next = cursor.next_request();
    assert!(retirement_preview(&f, &mut cursor).is_err());
    assert_eq!(cursor.next_request(), next);
    assert!(cursor.finish_epoch().is_err());
    drop(cursor);
    assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
    assert_eq!(snapshot(&f, &r.store), before);
}

#[test]
fn successor_retirement_rechecks_root_sequence_input_stamp_and_metadata_grant() {
    for changed in 0..3 {
        let f = prepared_fixture();
        let r = closed_recovery(&f);
        let before = snapshot(&f, &r.store);
        assert!(matches!(
            NativeSuccessorRetirement::inspect(&r, grant(&f.budget, 132607, 0), &NOT_CANCELLED),
            Err(BootstrapError::InsufficientReservation)
        ));
        assert_eq!(snapshot(&f, &r.store), before);
        let mut observed = retirement(&f, &r);
        if changed == 0 {
            let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(path, bytes).unwrap();
        } else if changed == 1 {
            put(
                &r.store,
                IngestKey::Manifest,
                r.manifest().encode().as_bytes(),
            );
        } else {
            let path = f.root.path().join("retirement-foreign-root");
            drop(r);
            copy_native_fixture(&f.path(), &path);
            let foreign = IngestRecovery::inspect(
                &path,
                f.previous.identity().dataset_id,
                f.native,
                grant(&f.budget, 0, 128 * MIB),
            )
            .unwrap();
            assert_eq!(kv(&foreign.store), before.0);
            assert_eq!(foreign.store.db.latest_sequence_number(), before.1);
            assert_eq!(inputs(&path), before.2);
            assert_eq!(
                observed.observe_closed(
                    &foreign,
                    grant(&f.budget, 4096, 4096),
                    &NOT_CANCELLED,
                    || Ok(())
                ),
                Err(BootstrapError::StateMismatch)
            );
            assert_eq!(kv(&foreign.store), before.0);
            assert_eq!(foreign.store.db.latest_sequence_number(), before.1);
            assert_eq!(inputs(&path), before.2);
            continue;
        }
        let changed = snapshot(&f, &r.store);
        assert_eq!(
            observed.observe_closed(&r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(())),
            Err(BootstrapError::StateMismatch)
        );
        assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
        assert_eq!(snapshot(&f, &r.store), changed);
    }
}

#[test]
fn successor_retirement_completed_proof_revalidates_after_wait_without_rearming() {
    for changed in 0..3 {
        let f = prepared_fixture();
        let r = closed_recovery(&f);
        let mut observed = retirement(&f, &r);
        observe(&f, &r, &mut observed);
        let (cancelled, before) = {
            let proof = complete(&f, observed.resolution(&r, &NOT_CANCELLED).unwrap()).unwrap();
            let cancelled = AtomicBool::new(changed == 0);
            if changed == 1 {
                put(
                    &r.store,
                    IngestKey::Manifest,
                    r.manifest().encode().as_bytes(),
                );
            } else if changed == 2 {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            let before = snapshot(&f, &r.store);
            assert_eq!(
                proof.validate(&cancelled),
                Err(if changed == 0 {
                    BootstrapError::AuthorityUnavailable
                } else {
                    BootstrapError::StateMismatch
                })
            );
            (cancelled, before)
        };
        cancelled.store(false, Ordering::Relaxed);
        assert!(observed.resolution(&r, &cancelled).is_err());
        assert_eq!(snapshot(&f, &r.store), before);
    }
}
