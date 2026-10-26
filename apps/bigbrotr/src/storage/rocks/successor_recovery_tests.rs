//! Fresh native recovery components. PG activation/reservation/COMMIT are
//! explicit caller premises here, never inferred from native fixture bytes.
use super::*;
use crate::storage::rocks::store::{
    NativeSuccessorRecovery, PendingSuccessorRequest, ResolvedSuccessorRecovery,
    SuccessorRecoveryResolution,
};

#[path = "successor_retirement_tests.rs"]
mod successor_retirement_tests;

fn owner(f: &Fixture, r: &IngestRecovery) -> NativeSuccessorRecovery {
    NativeSuccessorRecovery::inspect(r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED).unwrap()
}
fn close(f: &Fixture, r: &mut IngestRecovery, owner: &mut NativeSuccessorRecovery) {
    owner
        .close_producer(r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(()))
        .unwrap();
}
fn input_hash(f: &Fixture, id: ProducerRequestId) -> Digest256 {
    Digest256::from_bytes(
        Sha256::digest(
            std::fs::read(f.path().join("producer-inputs-v1").join(input_name(
                id.producer_epoch.get(),
                id.request_sequence.get(),
            )))
            .unwrap(),
        )
        .into(),
    )
}
fn preview<'s, 'a>(
    f: &Fixture,
    cursor: &'s mut SuccessorRecoveryResolution<'a>,
) -> Result<PendingSuccessorRequest<'s, 'a>, ProducerRecoveryError> {
    let id = cursor.next_request().unwrap();
    cursor.preview_next(
        input_hash(f, id),
        recovery_limits(),
        grant(&f.budget, 3 * MIB, 0),
        grant(&f.budget, 40 * MIB, 0),
        || Ok(()),
    )
}
fn finish<'a>(
    f: &Fixture,
    r: &'a IngestRecovery,
    owner: &'a NativeSuccessorRecovery,
) -> Result<ResolvedSuccessorRecovery<'a>, ProducerRecoveryError> {
    let mut cursor = owner.resolution(r, &NOT_CANCELLED).unwrap();
    while cursor.current_epoch().is_some() {
        while cursor.next_request().is_some() {
            let step = preview(f, &mut cursor)?;
            // Only the caller's independently confirmed SQL copy would permit
            // this callback in the connected owner. This fixture has no SQL.
            step.accept(|| Ok(())).map_err(AdmissionError::from)?;
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
fn reach_head(f: &Fixture, cursor: &mut SuccessorRecoveryResolution<'_>, epoch: u64) {
    while cursor.current_epoch().unwrap() < epoch {
        while cursor.next_request().is_some() {
            preview(f, cursor).unwrap().accept(|| Ok(())).unwrap();
        }
        cursor.finish_epoch().unwrap();
    }
}

fn copy_native_fixture(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir(to).unwrap();
    std::fs::set_permissions(to, std::fs::metadata(from).unwrap().permissions()).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_native_fixture(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn closed_recovery(f: &Fixture) -> IngestRecovery {
    drop(active(f, false));
    let mut r = f.reopen();
    let mut barrier = owner(f, &r);
    close(f, &mut r, &mut barrier);
    drop(barrier);
    r
}

#[test]
fn successor_source_observes_only_existing_closing_without_native_writes() {
    let f = prepared_fixture();
    let mut writer = active(&f, false);
    append_and_check(&f, &mut writer, 1);
    drop(writer);
    let mut r = f.reopen();
    let mut observed = owner(&f, &r);
    let allocation = observed.entry(1).unwrap() as *const _;
    let open = snapshot(&f, &r.store);
    assert_eq!(
        observed.observe_closed(&r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(())),
        Err(BootstrapError::StateMismatch)
    );
    assert_eq!(observed.entry(1).unwrap() as *const _, allocation);
    assert_eq!(snapshot(&f, &r.store), open);
    // A refused observation cannot be reused to dispatch a barrier.
    assert_eq!(
        observed.close_producer(&mut r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(
            ()
        )),
        Err(BootstrapError::AuthorityUnavailable)
    );
    assert_eq!(snapshot(&f, &r.store), open);
    drop(observed);
    let mut barrier = owner(&f, &r);
    close(&f, &mut r, &mut barrier);
    drop(barrier);

    let closed = snapshot(&f, &r.store);
    let mut observed = owner(&f, &r);
    let allocation = observed.entry(1).unwrap() as *const _;
    let reserved = f.budget.snapshot();
    assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
    assert_eq!(
        observed.observe_closed(&r, grant(&f.budget, 4095, 4096), &NOT_CANCELLED, || Ok(())),
        Err(BootstrapError::InsufficientReservation)
    );
    assert_eq!(f.budget.snapshot(), reserved);
    observed.validate(&r, &NOT_CANCELLED).unwrap();
    observed
        .observe_closed(&r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(()))
        .unwrap();
    assert_eq!(observed.entry(1).unwrap() as *const _, allocation);
    let retained = f.budget.snapshot();
    assert_eq!(
        retained.used().data_bytes(),
        reserved.used().data_bytes() + 4096
    );
    assert_eq!(
        retained.used().native_bytes(),
        reserved.used().native_bytes() + 4096
    );
    assert_eq!(retained.active_permits(), reserved.active_permits() + 1);
    observed
        .observe_closed(&r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(()))
        .unwrap();
    assert_eq!(
        f.budget.snapshot(),
        retained,
        "retry retains no second grant"
    );
    let proof = finish(&f, &r, &observed).unwrap();
    assert!(proof.matches_owner(&observed));
    assert_eq!(
        proof.last_epoch().digest,
        epoch_digest(&f, &r, 3, 1, 1, false)
    );
    assert_eq!(proof.last_epoch().resolved_requests, 1);
    assert_eq!(proof.last_epoch().last_complete_sequence, 1);
    proof.validate(&NOT_CANCELLED).unwrap();
    assert_eq!(snapshot(&f, &r.store), closed);
}

#[test]
fn successor_source_observation_refuses_stale_sequence_inputs_and_foreign_root() {
    for change_input in [false, true] {
        let f = prepared_fixture();
        let r = closed_recovery(&f);
        let mut observed = owner(&f, &r);
        let sequence = r.store.db.latest_sequence_number();
        if change_input {
            let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(path, bytes).unwrap();
            assert_eq!(r.store.db.latest_sequence_number(), sequence);
        } else {
            let manifest = r.manifest().encode();
            put(&r.store, IngestKey::Manifest, manifest.as_bytes());
            assert_eq!(r.store.db.latest_sequence_number(), sequence + 1);
        }
        let before = snapshot(&f, &r.store);
        assert_eq!(
            observed.observe_closed(&r, grant(&f.budget, 4096, 4096), &NOT_CANCELLED, || Ok(())),
            Err(BootstrapError::StateMismatch)
        );
        assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
        assert_eq!(snapshot(&f, &r.store), before);
    }

    let f = prepared_fixture();
    let r = closed_recovery(&f);
    let mut observed = owner(&f, &r);
    let original = snapshot(&f, &r.store);
    let root = r.store.handoff_root_identity().unwrap();
    let path = f.root.path().join("closed-equal-content-foreign-root");
    drop(r);
    copy_native_fixture(&f.path(), &path);
    let copied = IngestRecovery::inspect(
        &path,
        f.previous.identity().dataset_id,
        f.native,
        grant(&f.budget, 0, 128 * MIB),
    )
    .unwrap();
    assert_eq!(kv(&copied.store), original.0);
    assert_eq!(copied.store.db.latest_sequence_number(), original.1);
    assert_eq!(inputs(&path), original.2);
    assert_ne!(copied.store.handoff_root_identity().unwrap(), root);
    assert_eq!(
        observed.observe_closed(
            &copied,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || Ok(())
        ),
        Err(BootstrapError::StateMismatch)
    );
    assert_eq!(kv(&copied.store), original.0);
    assert_eq!(copied.store.db.latest_sequence_number(), original.1);
    assert_eq!(inputs(&path), original.2);
}

#[test]
fn successor_source_observation_keeps_custody_after_cancellation_or_authority_loss() {
    for fault in 0..5 {
        let f = prepared_fixture();
        let mut r = closed_recovery(&f);
        let baseline = f.budget.snapshot();
        let mut observed = owner(&f, &r);
        let allocation = observed.entry(1).unwrap() as *const _;
        let reserved = f.budget.snapshot();
        let cancelled = AtomicBool::new(fault == 0);
        if fault == 4 {
            r.store.write_uncertain = true;
        }
        let before = snapshot(&f, &r.store);
        let mut calls = 0;
        let result = observed.observe_closed(&r, grant(&f.budget, 4096, 4096), &cancelled, || {
            calls += 1;
            if fault == 1 || (fault == 2 && calls == 2) {
                return Err(BootstrapError::AuthorityUnavailable);
            }
            if fault == 3 && calls == 2 {
                cancelled.store(true, Ordering::Relaxed);
            }
            Ok(())
        });
        assert_eq!(
            result,
            Err(if fault == 4 {
                BootstrapError::CommitUnknown
            } else {
                BootstrapError::AuthorityUnavailable
            })
        );
        assert_eq!(
            calls,
            if fault == 0 || fault == 4 {
                0
            } else if fault == 1 {
                1
            } else {
                2
            }
        );
        assert_eq!(observed.entry(1).unwrap() as *const _, allocation);
        let retained = f.budget.snapshot();
        assert_eq!(
            retained.used().data_bytes(),
            reserved.used().data_bytes() + 4096
        );
        assert_eq!(
            retained.used().native_bytes(),
            reserved.used().native_bytes() + 4096
        );
        assert_eq!(retained.active_permits(), reserved.active_permits() + 1);
        cancelled.store(false, Ordering::Relaxed);
        r.store.write_uncertain = false;
        assert_eq!(
            observed.validate(&r, &NOT_CANCELLED),
            Err(BootstrapError::AuthorityUnavailable)
        );
        assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
        assert_eq!(snapshot(&f, &r.store), before);
        drop(observed);
        assert_eq!(
            f.budget.snapshot(),
            baseline,
            "grant released only with retained owner"
        );
    }
}

#[test]
fn successor_recovery_closes_and_replays_real_writer_three_and_four_with_literal_outcomes() {
    for variant in 0..3 {
        let f = prepared_fixture();
        let mut writer = if variant == 1 {
            let (mut activation, _) = envelope(&f, false, PredecessorTerminalKind::Activated, true);
            activation.activate(&NOT_CANCELLED, || Ok(())).unwrap()
        } else {
            active(&f, variant == 2)
        };
        let epoch = writer.manifest().identity().owner_epoch.get();
        append_and_check(&f, &mut writer, 1); // independent literal input/fact/receipt/eight-put recipe
        let reference = scope(&writer);
        let literal = expected(epoch, reference.allocation_domain.as_bytes(), 1);
        let before = snapshot(&f, writer.store());
        drop(writer);
        let mut r = f.reopen();
        assert!(r.store.live_successor.is_none());
        let mut observed = owner(&f, &r);
        let pointer = observed.entry(1).unwrap() as *const _;
        assert!(!observed.is_closing());
        assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
        let strict = inspect(&f, &r, None).unwrap();
        close(&f, &mut r, &mut observed);
        assert_eq!(observed.entry(1).unwrap() as *const _, pointer);
        assert!(strict.validate(&r, &NOT_CANCELLED).is_err());
        drop(strict);
        let mut wanted = before.0.clone();
        replace(&mut wanted, producer_key(epoch), &marker(true, 1));
        assert_eq!(
            snapshot(&f, &r.store),
            (wanted, before.1 + 1, before.2.clone())
        );
        let closed = snapshot(&f, &r.store);
        close(&f, &mut r, &mut observed);
        assert_eq!(snapshot(&f, &r.store), closed);
        let mut cursor = observed.resolution(&r, &NOT_CANCELLED).unwrap();
        reach_head(&f, &mut cursor, epoch);
        let step = preview(&f, &mut cursor).unwrap();
        assert_eq!(step.request(), at(epoch, 1));
        assert_eq!(step.resolved().native_outcome().bytes(), literal.receipt);
        step.accept(|| Ok(())).unwrap();
        let summary = cursor.finish_epoch().unwrap();
        assert_eq!(
            (
                summary.resolved_requests,
                summary.last_complete_sequence,
                summary.event_request_mask
            ),
            (1, 1, 0)
        );
        assert_eq!(summary.digest, epoch_digest(&f, &r, epoch, 1, 1, false));
        let limits = AdmissionLimits::default();
        {
            let proof = cursor
                .finish(
                    limits,
                    grant(
                        &f.budget,
                        limits.required_dictionary_replay_data_bytes().unwrap(),
                        0,
                    ),
                )
                .unwrap();
            assert!(proof.matches_owner(&observed));
            assert_eq!(proof.verified_through(), epoch);
            assert!(proof.matches_recorded(
                epoch,
                [(
                    1,
                    Sha256::digest(&literal.input).into(),
                    Sha256::digest(&literal.receipt).into()
                )]
            ));
            assert!(!proof.matches_recorded(epoch, []));
            proof.validate(&NOT_CANCELLED).unwrap();
        }
        assert_eq!(snapshot(&f, &r.store), closed);
        drop(observed);
        drop(r);
        let mut r = f.reopen();
        let mut retry = owner(&f, &r);
        close(&f, &mut r, &mut retry);
        finish(&f, &r, &retry).unwrap();
        assert_eq!(snapshot(&f, &r.store), closed);
    }
}

#[test]
fn successor_recovery_distinguishes_activated_zero_and_no_fact_request_one() {
    for kind in 0..3 {
        let f = prepared_fixture();
        let mut writer = active(&f, false);
        if kind != 0 {
            writer
                .admit_event(
                    event(&f, 3, 1, kind == 1),
                    ObservedTime::new(77),
                    false,
                    AdmissionLimits::default(),
                    grant(&f.budget, 40 * MIB, 10 * MIB),
                    &NOT_CANCELLED,
                    || Ok(()),
                )
                .unwrap();
        }
        let before = snapshot(&f, writer.store());
        drop(writer);
        let mut r = f.reopen();
        let mut observed = owner(&f, &r);
        assert_eq!(observed.head().source_through(), 0);
        assert_eq!(observed.issued_through(), u64::from(kind != 0));
        close(&f, &mut r, &mut observed);
        let proof = finish(&f, &r, &observed).unwrap();
        let summary = proof.last_epoch();
        assert_eq!(summary.terminal_kind, RetainedTerminalKind::Activated);
        assert_eq!(summary.last_complete_sequence, 0);
        assert_eq!(summary.resolved_requests, u64::from(kind != 0));
        assert_eq!(summary.event_request_mask, u64::from(kind != 0));
        assert_eq!(
            summary.digest,
            epoch_digest(&f, &r, 3, u64::from(kind != 0), 0, false)
        );
        let mut wanted = before.0;
        replace(
            &mut wanted,
            producer_key(3),
            &marker(true, u64::from(kind != 0)),
        );
        assert_eq!(snapshot(&f, &r.store), (wanted, before.1 + 1, before.2));
    }
}

#[test]
fn successor_recovery_preserves_one_unresolved_staged_tail_and_refuses_foreign_files() {
    for fault in 0..5 {
        let f = prepared_fixture();
        let mut writer = active(&f, false);
        append_and_check(&f, &mut writer, 1);
        let staged = event(&f, 3, 2, true);
        let permit = grant(&f.budget, 40 * MIB, 10 * MIB);
        let actual = writer
            .stage_event(&staged, AdmissionLimits::default(), &permit, &NOT_CANCELLED)
            .unwrap();
        drop(staged);
        drop(permit);
        drop(writer);
        let r = f.reopen();
        match fault {
            1 => stored_input(&f, 3, 3, b"second unresolved tail"),
            2 => stored_input(&f, 4, 1, b"future stream"),
            3 => {
                std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(1, 1)))
                    .unwrap();
            }
            4 => {
                let path = f.path().join("producer-inputs-v1").join(input_name(3, 2));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            _ => {}
        }
        let before = snapshot(&f, &r.store);
        assert!(
            inspect(&f, &r, None).is_err(),
            "ordinary predecessor census remains strict"
        );
        let result =
            NativeSuccessorRecovery::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED);
        if (1..=3).contains(&fault) {
            assert!(result.is_err());
            assert_eq!(snapshot(&f, &r.store), before);
            continue;
        }
        let mut observed = result.unwrap();
        let tail = observed.staged_input().unwrap();
        assert_eq!(tail.request, at(3, 2));
        if fault == 4 {
            assert!(
                observed
                    .validate_tail(
                        &r,
                        recovery_limits().input,
                        grant(&f.budget, 3 * MIB, 0),
                        &NOT_CANCELLED
                    )
                    .is_err()
            );
            assert_eq!(snapshot(&f, &r.store), before);
            continue;
        }
        assert_eq!(
            (tail.digest, tail.input_bytes),
            (actual.digest, actual.input_bytes)
        );
        observed
            .validate_tail(
                &r,
                recovery_limits().input,
                grant(&f.budget, 3 * MIB, 0),
                &NOT_CANCELLED,
            )
            .unwrap();
        let mut r = r;
        close(&f, &mut r, &mut observed);
        let closed = snapshot(&f, &r.store);
        // The same native observation supports either PG reserved==1 or==2;
        // no PG reservation is manufactured by this native component.
        assert!(matches!(
            finish(&f, &r, &observed),
            Err(ProducerRecoveryError::Unresolved)
        ));
        assert_eq!(snapshot(&f, &r.store), closed);
        assert_eq!(closed.2, before.2);
        assert_eq!(observed.issued_through(), 1);
    }
}

#[test]
fn successor_recovery_pending_step_is_one_shot_and_rechecks_sidecars_before_acceptance() {
    for fault in 0..4 {
        let f = prepared_fixture();
        drop(active(&f, false));
        let mut r = f.reopen();
        let mut observed = owner(&f, &r);
        close(&f, &mut r, &mut observed);
        let cancelled = AtomicBool::new(false);
        let mut cursor = observed.resolution(&r, &cancelled).unwrap();
        let next = cursor.next_request();
        let step = preview(&f, &mut cursor).unwrap();
        match fault {
            0 => drop(step), // unknown SQL outcome: no committed acceptance
            1 => assert_eq!(
                step.accept(|| Err(BootstrapError::AuthorityUnavailable)),
                Err(BootstrapError::AuthorityUnavailable)
            ),
            2 => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
                assert!(step.accept(|| Ok(())).is_err());
            }
            _ => {
                cancelled.store(true, Ordering::Relaxed);
                assert_eq!(
                    step.accept(|| Ok(())),
                    Err(BootstrapError::AuthorityUnavailable)
                );
            }
        }
        assert_eq!(cursor.next_request(), next);
        assert!(preview(&f, &mut cursor).is_err());
        assert!(cursor.finish_epoch().is_err());
    }
}

#[test]
fn successor_recovery_rejects_future_dependencies_before_a_sql_copy_step_exists() {
    let f = prepared_fixture();
    let mut r = valid_empty_third(&f);
    correction(&f, &r, true); // literal first receipt borrows request2's later fact
    let before = snapshot(&f, &r.store);
    let mut observed = owner(&f, &r);
    close(&f, &mut r, &mut observed);
    assert_eq!(snapshot(&f, &r.store), before); // fixture already has CLOSING2
    let mut cursor = observed.resolution(&r, &NOT_CANCELLED).unwrap();
    reach_head(&f, &mut cursor, 3);
    let next = cursor.next_request();
    assert!(preview(&f, &mut cursor).is_err());
    assert_eq!(cursor.next_request(), next);
    assert!(cursor.finish_epoch().is_err());
    assert_eq!(snapshot(&f, &r.store), before);
}

#[test]
fn successor_recovery_binds_root_sequence_and_retains_failed_barrier_custody() {
    for fault in 0..5 {
        let f = prepared_fixture();
        drop(active(&f, false));
        let mut r = f.reopen();
        let mut observed = owner(&f, &r);
        let pointer = observed.entry(1).unwrap() as *const _;
        if fault == 0 {
            let bytes = r.manifest().encode();
            put(&r.store, IngestKey::Manifest, bytes.as_bytes());
        } else if fault == 1 {
            r.store.write_uncertain = true; // explicit FFI unknown outcome premise
        }
        let before = snapshot(&f, &r.store);
        let mut calls = 0;
        let result = observed.close_producer(
            &mut r,
            grant(&f.budget, 4096, 4096),
            &AtomicBool::new(fault == 2),
            || {
                calls += 1;
                // Initial check, immediate native dispatch check, then delivery.
                if fault == 3 || (fault == 4 && calls == 3) {
                    Err(BootstrapError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        assert_eq!(observed.entry(1).unwrap() as *const _, pointer);
        assert!(observed.resolution(&r, &NOT_CANCELLED).is_err());
        if fault == 4 {
            assert_eq!(calls, 3);
            let mut wanted = before.0;
            replace(&mut wanted, producer_key(3), &marker(true, 0));
            assert_eq!(snapshot(&f, &r.store), (wanted, before.1 + 1, before.2));
            // Lost final delivery retained both owners; fresh inspection can
            // observe the actual completed native marker without rewriting it.
            drop(observed);
            drop(r);
            let mut r = f.reopen();
            let mut retry = owner(&f, &r);
            let closed = snapshot(&f, &r.store);
            close(&f, &mut r, &mut retry);
            finish(&f, &r, &retry).unwrap();
            assert_eq!(snapshot(&f, &r.store), closed);
        } else {
            assert_eq!(snapshot(&f, &r.store), before);
        }
    }
    let f = prepared_fixture();
    drop(active(&f, false));
    let r = f.reopen();
    let observed = owner(&f, &r);
    let original = snapshot(&f, &r.store);
    let original_root = r.store.handoff_root_identity().unwrap();
    let copy_path = f.root.path().join("equal-content-foreign-root");
    // Copy native bytes and private sidecars as a complete operator fixture;
    // equal logical state never transfers the original handle's root identity.
    drop(r);
    copy_native_fixture(&f.path(), &copy_path);
    let copied = IngestRecovery::inspect(
        &copy_path,
        f.previous.identity().dataset_id,
        f.native,
        grant(&f.budget, 0, 128 * MIB),
    )
    .unwrap();
    assert_eq!(kv(&copied.store), original.0);
    assert_eq!(copied.store.db.latest_sequence_number(), original.1);
    assert_eq!(inputs(&copy_path), original.2);
    assert_ne!(copied.store.handoff_root_identity().unwrap(), original_root);
    assert!(observed.validate(&copied, &NOT_CANCELLED).is_err());
}

#[test]
fn successor_recovery_keeps_epoch_255_and_full_original_spool_with_one_allocation() {
    let f = prepared_fixture();
    let r = f.reopen();
    let mut previous = *r.manifest();
    let mut incoming = value(&r.store, handoff_key());
    // Literal empty-closure preimages and portable record framing, independent
    // of the production digest/record constructors. These are component bytes,
    // not a claim of 255 real PG preparations or activations.
    for epoch in 3_u64..=255 {
        let parent = epoch - 1;
        let domain: &[u8] = if parent == 2 {
            b"bigbrotr/initial-empty-attempt-closure/1\0"
        } else {
            b"bigbrotr/ingest-successor-empty-closure/1\0"
        };
        let mut preimage = domain.to_vec();
        preimage.extend(Sha256::digest(&incoming));
        preimage.extend(Sha256::digest(previous.encode().as_bytes()));
        preimage.extend([7; 16]);
        preimage.extend(parent.to_be_bytes());
        preimage.extend(0_u64.to_be_bytes());
        preimage.extend(0_u64.to_be_bytes());
        let next = later_manifest(previous, epoch);
        let encoded = literal_record(
            previous.encode().as_bytes(),
            next.encode().as_bytes(),
            2,
            &incoming,
            0,
            0,
            Sha256::digest(preimage).into(),
        );
        let key = IngestKey::OwnerHandoff {
            dataset_id: previous.identity().dataset_id,
            owner_resource_id: previous.identity().owner_resource_id,
            predecessor_epoch: previous.identity().owner_epoch,
        };
        put(&r.store, producer_key(parent), &marker(true, 0));
        put(&r.store, key, &encoded);
        put(&r.store, producer_key(epoch), &marker(false, 0));
        put(&r.store, source_key(epoch), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        put(&r.store, IngestKey::Manifest, next.encode().as_bytes());
        incoming = encoded;
        previous = next;
    }
    drop(r);
    let mut r = f.reopen();
    let before = snapshot(&f, &r.store);
    let minimum = 128 * 1024 + 512 * 255;
    assert!(matches!(
        NativeSuccessorRecovery::inspect(&r, grant(&f.budget, minimum - 1, 0), &NOT_CANCELLED),
        Err(BootstrapError::InsufficientReservation)
    ));
    assert_eq!(snapshot(&f, &r.store), before);
    let mut observed =
        NativeSuccessorRecovery::inspect(&r, grant(&f.budget, minimum, 0), &NOT_CANCELLED).unwrap();
    let allocation = observed.entry(1).unwrap() as *const _;
    assert_eq!(observed.head().manifest().identity().owner_epoch.get(), 255);
    assert!(SuccessionProfile::for_preparation(255).is_err());
    close(&f, &mut r, &mut observed);
    assert_eq!(observed.entry(1).unwrap() as *const _, allocation);
    {
        let proof = finish(&f, &r, &observed).unwrap();
        assert_eq!(proof.verified_through(), 255);
        assert_eq!(
            proof.last_epoch().digest,
            epoch_digest(&f, &r, 255, 0, 0, false)
        );
        let mut wanted = before.0;
        replace(&mut wanted, producer_key(255), &marker(true, 0));
        assert_eq!(snapshot(&f, &r.store), (wanted, before.1 + 1, before.2));
    }
    drop(observed);
    drop(r);

    let f = zero_fixture(64);
    drop(active(&f, false));
    let mut r = f.reopen();
    let mut observed = owner(&f, &r);
    assert_eq!(observed.entry(1).unwrap().issued_through(), 64);
    assert_eq!(inputs(&f.path()).len(), 64);
    close(&f, &mut r, &mut observed);
    let closed = snapshot(&f, &r.store);
    finish(&f, &r, &observed).unwrap();
    assert_eq!(snapshot(&f, &r.store), closed);
    // A current staged+1 is structurally allowed only within the GLOBAL cap.
    // The 65th file remains preserved and prevents a new observation.
    stored_input(
        &f,
        3,
        1,
        b"retained unresolved bytes beyond the global bound",
    );
    let excess = snapshot(&f, &r.store);
    assert!(matches!(
        NativeSuccessorRecovery::inspect(&r, grant(&f.budget, 256 * 1024, 0), &NOT_CANCELLED),
        Err(BootstrapError::Limit)
    ));
    assert!(observed.validate(&r, &NOT_CANCELLED).is_err());
    assert_eq!(snapshot(&f, &r.store), excess);
}
