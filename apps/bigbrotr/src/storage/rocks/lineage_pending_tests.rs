//! Bridge checks reuse the signed original-history fixture and its independently
//! specified source/dependency sets. Raw original facts are captured before the
//! new accessor runs; they are not reconstructed by that accessor. SQL terminal
//! authority, Archive publication and cancellation inside native I/O are excluded.
use super::*;
use crate::storage::rocks::store::{LineagePendingError, PendingError, PendingReadLimits};
use bigbrotr_model::codec::FactDecodeLimits;

fn limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: 64,
        max_encoded_bytes: MIB as usize,
        fact: FactDecodeLimits::default(),
    }
}

fn read(
    f: &Fixture,
    proof: &ResolvedLineage<'_>,
    first: SourceRecordId,
) -> Result<PendingBatch, LineagePendingError> {
    proof.read_complete_pending_batch(
        first,
        limits(),
        grant(&f.budget, limits().required_data_bytes().unwrap(), 0),
        &NOT_CANCELLED,
    )
}

#[test]
fn lineage_pending_reads_original_historical_and_current_batches_without_writes() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    correction(&f, &r, false);
    let before = kv(&r.store);
    let originals = inputs(&f.path());
    let sequence = r.store.db.latest_sequence_number();
    let l = inspect(&f, &r, None).unwrap();
    let proof = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
    let used = f.budget.snapshot().used().data_bytes();
    let permits = f.budget.snapshot().active_permits();
    let required = limits().required_data_bytes().unwrap();
    // Default decoding is an additional 8MiB, not part of the 1MiB encoded cap.
    assert!(required > 9 * MIB);
    for (epoch, first, last) in [(1, 1, 4), (1, 5, 5), (1, 6, 9), (3, 1, 1)] {
        let expected: Vec<_> = (first..=last)
            .map(|sequence| {
                let key = IngestKey::Fact {
                    source_record_id: fact_id(epoch, sequence),
                }
                .encode()
                .unwrap();
                let (_, _, value) = before
                    .iter()
                    .find(|(cf, bytes, _)| cf == "facts" && bytes == key.as_bytes())
                    .unwrap();
                value[34..].to_vec()
            })
            .collect();
        let batch = read(&f, &proof, fact_id(epoch, first)).unwrap();
        assert_eq!(batch.batch().first(), fact_id(epoch, first));
        assert_eq!(batch.batch().last(), fact_id(epoch, last));
        assert_eq!(batch.records(), expected);
        let original_receipt = if epoch == 1 {
            f.originals[match first {
                1 => 0,
                5 => 1,
                6 => 2,
                _ => unreachable!(),
            }]
            .outcome
            .clone()
        } else {
            value(
                &r.store,
                IngestKey::Request {
                    request_id: at(3, 1),
                },
            )
        };
        let receipt = Receipt::decode_persistent(
            &original_receipt,
            bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
                max_encoded_bytes: 8192,
                max_allocation_bytes: 8192,
                max_batch_records: 64,
                max_dependencies: 64,
            },
        )
        .unwrap();
        assert_eq!(Some(batch.batch()), receipt.batch());
        assert_eq!(f.budget.snapshot().used().data_bytes(), used + required);
        assert_eq!(f.budget.snapshot().active_permits(), permits + 1);
        drop(batch);
        assert_eq!(f.budget.snapshot().used().data_bytes(), used);
        assert_eq!(f.budget.snapshot().active_permits(), permits);
    }
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}

#[test]
fn lineage_pending_requires_full_head_without_pending_preparation() {
    let f = prepared_fixture();
    let r = f.reopen();
    {
        let l = inspect(&f, &r, None).unwrap();
        let prefix = replay(&f, &r, &l, 1, None).unwrap();
        assert_eq!(
            read(&f, &prefix, fact_id(1, 1)).err(),
            Some(LineagePendingError::Proof(BootstrapError::StateMismatch))
        );
    }
    put(&r.store, producer_key(2), &marker(true, 0));
    let before = kv(&r.store);
    {
        let l = inspect(&f, &r, Some(3)).unwrap();
        let pending = replay(
            &f,
            &r,
            &l,
            2,
            Some(PredecessorTerminalKind::UnactivatedEmpty),
        )
        .unwrap();
        assert_eq!(pending.verified_through(), 2);
        assert_eq!(
            read(&f, &pending, fact_id(1, 1)).err(),
            Some(LineagePendingError::Proof(BootstrapError::StateMismatch))
        );
    }
    let l = inspect(&f, &r, None).unwrap();
    let complete = replay(
        &f,
        &r,
        &l,
        2,
        Some(PredecessorTerminalKind::UnactivatedEmpty),
    )
    .unwrap();
    assert!(read(&f, &complete, fact_id(1, 1)).is_ok());
    assert_eq!(kv(&r.store), before);
}

#[test]
fn lineage_pending_refuses_foreign_zero_out_of_cut_and_interior_identities() {
    {
        let f = prepared_fixture();
        let r = valid_empty_third(&f);
        correction(&f, &r, false);
        let l = inspect(&f, &r, None).unwrap();
        let proof = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
        let before = kv(&r.store);
        for (id, error) in [
            (
                SourceRecordId {
                    dataset_id: [2; 16].try_into().unwrap(),
                    ..fact_id(1, 1)
                },
                LineagePendingError::Read(PendingError::ForeignDataset),
            ),
            (
                SourceRecordId {
                    shard_id: 1,
                    ..fact_id(1, 1)
                },
                LineagePendingError::OutsideVerifiedCut,
            ),
            (fact_id(4, 1), LineagePendingError::OutsideVerifiedCut),
            (fact_id(1, 10), LineagePendingError::OutsideVerifiedCut),
            (fact_id(2, 1), LineagePendingError::OutsideVerifiedCut),
            (fact_id(3, 2), LineagePendingError::OutsideVerifiedCut),
            // This fact is inside the first historical batch, not a new boundary.
            (fact_id(1, 2), LineagePendingError::RequiredBatchMissing),
        ] {
            assert_eq!(read(&f, &proof, id).err(), Some(error));
        }
        assert_eq!(kv(&r.store), before);
    }

    let f = zero_fixture(1);
    let r = f.reopen();
    put(&r.store, producer_key(2), &marker(true, 0));
    let before = kv(&r.store);
    let l = inspect(&f, &r, None).unwrap();
    let proof = replay(
        &f,
        &r,
        &l,
        2,
        Some(PredecessorTerminalKind::UnactivatedEmpty),
    )
    .unwrap();
    assert_eq!(l.entry(1).unwrap().issued_through(), 1);
    for epoch in [1, 2] {
        assert_eq!(
            read(&f, &proof, fact_id(epoch, 1)).err(),
            Some(LineagePendingError::OutsideVerifiedCut)
        );
    }
    assert_eq!(
        kv(&r.store),
        before,
        "no zero counter or pending marker repair"
    );
}

#[test]
fn lineage_pending_preserves_limit_cancel_and_grant_errors_without_delivery() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    correction(&f, &r, false);
    let l = inspect(&f, &r, None).unwrap();
    let proof = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
    let required = limits().required_data_bytes().unwrap();
    let used = f.budget.snapshot().used().data_bytes();
    let permits = f.budget.snapshot().active_permits();
    for (cap, bytes, items, cancelled, error) in [
        (
            limits(),
            required - 1,
            1,
            false,
            LineagePendingError::Read(PendingError::InsufficientReservation),
        ),
        (
            limits(),
            required,
            0,
            false,
            LineagePendingError::Read(PendingError::InsufficientReservation),
        ),
        (
            PendingReadLimits {
                max_records: 0,
                ..limits()
            },
            required,
            1,
            false,
            LineagePendingError::Read(PendingError::InvalidLimits),
        ),
        (
            PendingReadLimits {
                max_records: 1,
                ..limits()
            },
            required,
            1,
            false,
            LineagePendingError::Read(PendingError::Limit),
        ),
        (
            PendingReadLimits {
                max_encoded_bytes: 1,
                ..limits()
            },
            required,
            1,
            false,
            LineagePendingError::Read(PendingError::Limit),
        ),
        (
            PendingReadLimits {
                fact: FactDecodeLimits {
                    max_allocation_bytes: 1,
                    ..limits().fact
                },
                ..limits()
            },
            required,
            1,
            false,
            LineagePendingError::Read(PendingError::Limit),
        ),
        (
            limits(),
            required,
            1,
            true,
            LineagePendingError::Proof(BootstrapError::AuthorityUnavailable),
        ),
    ] {
        let permit = f
            .budget
            .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
            .unwrap();
        assert_eq!(
            proof
                .read_complete_pending_batch(
                    fact_id(1, 1),
                    cap,
                    permit,
                    &AtomicBool::new(cancelled)
                )
                .err(),
            Some(error)
        );
        assert_eq!(f.budget.snapshot().used().data_bytes(), used);
        assert_eq!(f.budget.snapshot().active_permits(), permits);
    }
    // Cancellation and resource refusal neither poison nor consume a read-only proof.
    assert!(read(&f, &proof, fact_id(1, 1)).is_ok());
}

#[test]
fn lineage_pending_refuses_stale_native_and_physical_input_custody() {
    for fault in 0..4 {
        let f = prepared_fixture();
        let r = valid_empty_third(&f);
        correction(&f, &r, false);
        let l = inspect(&f, &r, None).unwrap();
        let proof = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
        let sequence = r.store.db.latest_sequence_number();
        match fault {
            0 => put(&r.store, source_key(3), &value(&r.store, source_key(3))),
            1 => delete(
                &r.store,
                IngestKey::PendingBatch {
                    first: fact_id(1, 1),
                },
            ),
            2 | 3 => {
                let path = f.path().join("producer-inputs-v1").join(input_name(1, 1));
                let mut bytes = std::fs::read(&path).unwrap();
                if fault == 2 {
                    bytes[0] ^= 1;
                    std::fs::write(path, bytes).unwrap();
                } else {
                    let replacement = f.root.path().join("replacement");
                    std::fs::write(&replacement, bytes).unwrap();
                    std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    std::fs::rename(replacement, path).unwrap();
                }
                assert_eq!(r.store.db.latest_sequence_number(), sequence);
            }
            _ => unreachable!(),
        }
        let before = kv(&r.store);
        let originals = inputs(&f.path());
        // Resource refusal precedes even the now-stale proof's native/spool read.
        assert_eq!(
            proof
                .read_complete_pending_batch(
                    fact_id(1, 1),
                    limits(),
                    grant(&f.budget, limits().required_data_bytes().unwrap() - 1, 0),
                    &NOT_CANCELLED,
                )
                .err(),
            Some(LineagePendingError::Read(
                PendingError::InsufficientReservation
            ))
        );
        assert_eq!(
            read(&f, &proof, fact_id(1, 1)).err(),
            Some(LineagePendingError::Proof(BootstrapError::StateMismatch))
        );
        assert_eq!(kv(&r.store), before);
        assert_eq!(inputs(&f.path()), originals);
        if fault == 1 {
            // Missing coverage already refuses structural inspection; no forged
            // proof is manufactured to bypass it and exercise a later branch.
            assert!(inspect(&f, &r, None).is_err());
        }
    }
}
