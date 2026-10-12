//! Native close/reopen fixtures for a retained target with the old store intact.
//! No PostgreSQL, separate process or writer activation is qualified here.
use super::*;

fn literal_manifests() -> (Vec<u8>, Vec<u8>) {
    let mut previous = vec![1, 1, 0, 1, 0, 1, 0, 1, 0, 1];
    previous.extend_from_slice(&[1; 16]); // dataset
    previous.extend_from_slice(&1_u64.to_be_bytes()); // topology
    previous.extend_from_slice(&[3; 16]); // stable owner resource
    previous.extend_from_slice(&0_u32.to_be_bytes()); // shard
    previous.extend_from_slice(&1_u64.to_be_bytes()); // owner epoch
    previous.extend_from_slice(&[4; 16]); // original incarnation
    previous.extend_from_slice(&[6; 16]); // retained recovery generation
    previous.extend_from_slice(&1_u64.to_be_bytes()); // policy epoch
    previous.extend_from_slice(&[1, 7]); // LocalSynced, seven allocation domains
    for kind in [1_u16, 2, 3, 4, 5, 8, 9] {
        previous.extend_from_slice(&kind.to_be_bytes());
        previous.extend_from_slice(&[kind as u8; 16]);
    }
    assert_eq!(previous.len(), 230);
    let mut target = previous.clone();
    target[3] = 2;
    target[26..34].copy_from_slice(&2_u64.to_be_bytes());
    target[54..62].copy_from_slice(&2_u64.to_be_bytes());
    target[62..78].fill(44);
    for entry in target[104..].chunks_exact_mut(18) {
        let domain = entry[1] + 32;
        entry[2..].fill(domain);
    }
    (previous, target)
}

fn literal_handoff(fixture: &Fixture) -> Vec<u8> {
    let (previous, target) = literal_manifests();
    let closure = expected_closure(&previous, &fixture.originals);
    assert_eq!(closure, fixture.closure);
    let mut value = vec![1, 10];
    value.extend_from_slice(&[7; 16]);
    value.extend_from_slice(&1_u64.to_be_bytes());
    value.extend_from_slice(&4_u64.to_be_bytes());
    value.extend_from_slice(&9_u64.to_be_bytes());
    value.extend_from_slice(&closure);
    for manifest in [&previous, &target] {
        value.extend_from_slice(&230_u16.to_be_bytes());
        value.extend_from_slice(manifest);
    }
    assert_eq!(value.len(), 538);
    value
}

fn expected_rows(fixture: &Fixture) -> Rows {
    let (previous, target) = literal_manifests();
    let mut rows = fixture.closed_rows.clone();
    let manifest = rows
        .iter_mut()
        .find(|(cf, key, _)| cf == "default" && key == &[1, 1])
        .unwrap();
    assert_eq!(manifest.2, previous);
    manifest.2 = target;
    let mut handoff = vec![1, 10];
    handoff.extend_from_slice(&[1; 16]);
    handoff.extend_from_slice(&[3; 16]);
    handoff.extend_from_slice(&1_u64.to_be_bytes());
    let mut source = vec![1, 2];
    source.extend_from_slice(&[1; 16]);
    source.extend_from_slice(&0_u32.to_be_bytes());
    source.extend_from_slice(&2_u64.to_be_bytes());
    let mut producer = vec![1, 49];
    producer.extend_from_slice(&[7; 16]);
    producer.extend_from_slice(&2_u64.to_be_bytes());
    let mut open = vec![1, 49];
    open.extend_from_slice(&[0; 24]);
    rows.extend([
        ("default".into(), handoff, literal_handoff(fixture)),
        ("default".into(), source, vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 0]),
        ("receipts".into(), producer, open),
    ]);
    rows.sort();
    rows
}

fn rebuild(fixture: &Fixture, recovery: &IngestRecovery) -> InitialHandoff {
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let original_inputs = inputs(&fixture.path());
    let closing = recovery
        .observe_closed_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&fixture.budget, 4096, 4096),
        )
        .unwrap();
    assert_eq!(closing.issued_through(), 4);
    let mut source = closing.source_resolution(recovery, 4).unwrap();
    for (index, original) in fixture.originals.iter().enumerate() {
        let request_sequence = index as u64 + 1;
        let resolved = recovery
            .resolve_original_request(
                request(7, request_sequence),
                input_digest(original),
                recovery_limits(),
                grant(&fixture.budget, 3 * MIB, 0),
                grant(&fixture.budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
            panic!("the original signed fixture has four positive outcomes")
        };
        assert_receipt(receipt, request_sequence);
        assert_eq!(resolved.native_outcome().bytes(), original.outcome);
        assert_eq!(resolved.native_outcome().digest(), input_digest(original));
        assert_eq!(resolved.input_bytes() as usize, original.input.len());
        assert_eq!(resolved.is_event(), request_sequence >= 3);
        source.record(&resolved).unwrap();
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&fixture.path()), original_inputs);
    }
    let source = source.finish().unwrap();
    assert_eq!(source.resolved_requests(), 4);
    assert_eq!(source.last_complete_sequence(), 9);
    assert_eq!(source.digest(), fixture.closure);
    assert_eq!(source.event_request_mask(), 0b1100);
    let handoff = InitialHandoff::from_source(&source, fixture.next).unwrap();
    assert_eq!(handoff.encode().as_bytes(), literal_handoff(fixture));
    handoff
}

#[test]
fn reopened_initial_gap_requires_all_positive_originals_before_exact_four_write_completion() {
    let fixture = closed_fixture();
    // The preparation fixture retained no native proof/plan or live owner.
    assert_eq!(fixture.budget.snapshot().active_permits(), 0);
    let mut recovery = fixture.reopen();
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let original_inputs = inputs(&fixture.path());
    assert_eq!(before, fixture.closed_rows);
    assert_eq!(recovery.manifest(), &fixture.previous);
    assert_eq!(
        value(&recovery.store, IngestKey::Manifest),
        literal_manifests().0
    );
    let closing = recovery
        .observe_closed_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&fixture.budget, 4096, 4096),
        )
        .unwrap();
    let mut incomplete = closing.source_resolution(&recovery, 4).unwrap();
    let first = recovery
        .resolve_original_request(
            request(7, 1),
            input_digest(&fixture.originals[0]),
            recovery_limits(),
            grant(&fixture.budget, 3 * MIB, 0),
            grant(&fixture.budget, 40 * MIB, 0),
            || Ok(()),
        )
        .unwrap();
    incomplete.record(&first).unwrap();
    assert!(matches!(
        incomplete.finish(),
        Err(BootstrapError::StateMismatch)
    ));
    drop(first);
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&fixture.path()), original_inputs);

    let handoff = rebuild(&fixture, &recovery);
    let wanted = expected_rows(&fixture);
    let mut authority_calls = 0;
    let observed = recovery
        .apply_initial_handoff(&handoff, grant(&fixture.budget, 8192, 8192), || {
            authority_calls += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        observed.handoff().encode().as_bytes(),
        literal_handoff(&fixture)
    );
    assert_eq!(authority_calls, 3); // Native seam only; actual Control is a separate fixture.
    drop(observed);
    assert_eq!(kv(&recovery.store), wanted);
    assert_eq!(wanted.len(), before.len() + 3);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(inputs(&fixture.path()), original_inputs);
    assert_eq!(recovery.manifest(), &fixture.next);
    drop(handoff);
    drop(recovery);

    let mut recovery = fixture.reopen();
    let rebuilt = reconstruct(&fixture, &recovery).unwrap();
    assert_eq!(rebuilt.encode().as_bytes(), literal_handoff(&fixture));
    drop(
        recovery
            .apply_initial_handoff(&rebuilt, grant(&fixture.budget, 8192, 8192), || Ok(()))
            .unwrap(),
    );
    assert_eq!(kv(&recovery.store), wanted);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(inputs(&fixture.path()), original_inputs);
}

#[test]
fn initial_gap_preflight_and_dispatch_refuse_target_inputs_even_without_native_sequence_change() {
    for schedule in ["before-proof", "after-proof", "completed-replay"] {
        let fixture = closed_fixture();
        let mut recovery = fixture.reopen();
        let old_sequence = recovery.store.db.latest_sequence_number();
        let target = fixture
            .path()
            .join("producer-inputs-v1")
            .join(input_name(2, 1));
        let inject = || {
            std::fs::copy(
                fixture
                    .path()
                    .join("producer-inputs-v1")
                    .join(input_name(1, 1)),
                &target,
            )
            .unwrap();
        };
        if schedule == "before-proof" {
            inject();
        }
        let handoff = rebuild(&fixture, &recovery);
        if schedule == "completed-replay" {
            drop(
                recovery
                    .apply_initial_handoff(&handoff, grant(&fixture.budget, 8192, 8192), || Ok(()))
                    .unwrap(),
            );
        }
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        assert_eq!(
            sequence,
            old_sequence + if schedule == "completed-replay" { 4 } else { 0 }
        );
        if schedule != "before-proof" {
            inject();
        }
        let original_inputs = inputs(&fixture.path());
        assert_eq!(original_inputs.len(), 5);
        assert_eq!(std::fs::read(&target).unwrap(), fixture.originals[0].input);
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
        assert_eq!(
            recovery.validate_initial_handoff(&handoff),
            Err(BootstrapError::RequestInput(RequestInputError::Conflict)),
            "{schedule}"
        );
        assert!(
            matches!(
                recovery.apply_initial_handoff(
                    &handoff,
                    grant(&fixture.budget, 8192, 8192),
                    || Ok(())
                ),
                Err(BootstrapError::RequestInput(RequestInputError::Conflict))
            ),
            "{schedule}"
        );
        assert_eq!(kv(&recovery.store), before, "{schedule}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{schedule}"
        );
        assert_eq!(inputs(&fixture.path()), original_inputs, "{schedule}");
    }
}
