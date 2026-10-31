//! Current profile accounting controls; real native execution is root-owned.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request};
use super::*;
use crate::storage::rocks::keys::{ColumnFamily, IngestKey};
use bigbrotr_model::ObservedTime;

const MIB: u64 = 1024 * 1024;

fn charged_native_bytes(store: &IngestStore) -> u64 {
    // Independent encoded-byte oracle: count every facts-family key and whole
    // native value once, including the marker. No receipt/index/counter copies.
    store
        .db
        .iterator_cf(
            store.db.cf_handle("facts").unwrap(),
            rocksdb::IteratorMode::Start,
        )
        .map(|row| {
            let (key, value) = row.unwrap();
            u64::try_from(key.len() + value.len()).unwrap()
        })
        .sum()
}
fn populate(store: &mut IngestStore, b: &crate::runtime::ResourceBudget) {
    let limits = BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    };
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(123),
                limits,
                grant(
                    b,
                    limits.required_data_bytes().unwrap(),
                    limits.required_native_bytes().unwrap(),
                ),
                || Ok(()),
            )
            .unwrap(),
    );
}

#[test]
fn fresh_counter_and_bootstrap_are_atomic_and_replay_preserves_current_accounting() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    assert_eq!(store.db.latest_sequence_number(), 3);
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    assert_eq!(store.pending_accounting().unwrap().source_through(), 0);
    for (name, wanted) in [("default", 3), ("facts", 0), ("receipts", 0)] {
        assert_eq!(
            store
                .db
                .iterator_cf(
                    store.db.cf_handle(name).unwrap(),
                    rocksdb::IteratorMode::Start
                )
                .count(),
            wanted
        );
    }
    populate(&mut store, &b);
    let counted = charged_native_bytes(&store);
    assert!(counted > 0);
    let current = store.pending_accounting().unwrap();
    assert_eq!(current.pending_bytes(), counted);
    assert_eq!(current.source_through(), 1);
    // A populated current store still uses ordinary producer-state format1.
    let key = IngestKey::ProducerState {
        producer_slot_id: request().producer_slot_id,
        producer_epoch: request().producer_epoch,
    }
    .encode()
    .unwrap();
    assert_eq!(
        &store
            .db
            .get_cf(store.db.cf_handle("receipts").unwrap(), key.as_bytes())
            .unwrap()
            .unwrap()[..2],
        &[1, 0x31]
    );
    let sequence = store.db.latest_sequence_number();
    populate(&mut store, &b);
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(store.pending_accounting().unwrap(), current);
    drop(store);
    let store = IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    assert_eq!(store.pending_accounting().unwrap(), current);
    assert_eq!(charged_native_bytes(&store), counted);
}

#[test]
fn current_counter_damage_refuses_reopen_without_repair() {
    for case in 0..16 {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let mut store =
            IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        populate(&mut store, &b);
        let mut bytes = store.accounting.encode().to_vec();
        match case {
            0 => {
                store.db.delete([1, 0x0b]).unwrap();
            }
            1 => {
                bytes[0] = 2;
            }
            2 => {
                bytes[1] = 0x0c;
            }
            3 => {
                bytes.pop();
            }
            4 => {
                bytes.push(0);
            }
            5 => {
                bytes[2] ^= 1;
            }
            6 => {
                bytes[34..42].copy_from_slice(&0_u64.to_be_bytes());
            }
            7 => {
                bytes[34..42].copy_from_slice(&u64::MAX.to_be_bytes());
            }
            8 => {
                bytes[42..50].copy_from_slice(&0_u64.to_be_bytes());
            }
            9 => {
                bytes[42..50].copy_from_slice(&2_u64.to_be_bytes());
            }
            10 | 11 => {
                let cf = store.db.cf_handle("facts").unwrap();
                let (key, mut value) = store
                    .db
                    .iterator_cf(cf, rocksdb::IteratorMode::Start)
                    .next()
                    .unwrap()
                    .unwrap();
                if case == 10 {
                    store.db.delete_cf(cf, key).unwrap();
                } else {
                    value[34] ^= 1;
                    store.db.put_cf(cf, key, value).unwrap();
                }
            }
            12 => {
                let id = store.manifest().identity();
                let key = IngestKey::SourceStream {
                    dataset_id: id.dataset_id,
                    shard_id: id.shard_id,
                    owner_epoch: id.owner_epoch,
                }
                .encode()
                .unwrap();
                store.db.delete(key.as_bytes()).unwrap();
            }
            13..=15 => {
                let name = ["default", "facts", "receipts"][case - 13];
                store
                    .db
                    .put_cf(
                        store.db.cf_handle(name).unwrap(),
                        b"unexpected",
                        b"preserve evidence",
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        if (1..=9).contains(&case) {
            store.db.put([1, 0x0b], bytes).unwrap();
        }
        drop(store);
        assert!(
            matches!(
                IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)),
                Err(StoreError::InvalidAccounting(_))
            ),
            "case {case}"
        );
    }
}

#[test]
fn actual_empty_close_retains_explicit_source_and_counter_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    drop(IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap());
    let mut recovery =
        IngestRecovery::open(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let counter = recovery.store.accounting;
    let sequence = recovery.store.db.latest_sequence_number();
    let closing = recovery
        .close_producer(
            request().producer_slot_id,
            request().producer_epoch,
            grant(&b, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(closing.issued_through(), 0);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence + 1);
    assert_eq!(recovery.store.pending_accounting().unwrap(), counter);
    let source = closing
        .source_resolution(&recovery, 0)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(source.last_complete_sequence(), 0);
    drop(source);
    drop(recovery);
    let recovery =
        IngestRecovery::open(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    assert_eq!(recovery.store.pending_accounting().unwrap(), counter);
    let closing = recovery
        .observe_closed_producer(
            request().producer_slot_id,
            request().producer_epoch,
            grant(&b, 4096, 4096),
        )
        .unwrap();
    assert_eq!(closing.issued_through(), 0);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence + 1);
}

#[test]
fn accounting_codec_checks_full_unsigned_domain_and_exact_binding() {
    let manifest = manifest();
    let value = PendingAccounting::new(&manifest, u64::MAX, 1_u64 << 63);
    let bytes = value.encode();
    assert_eq!(bytes.len(), 50);
    assert_eq!(&bytes[..2], &[1, 0x0b]);
    assert_eq!(&bytes[34..42], &[0xff; 8]);
    assert_eq!(&bytes[42..50], &[0x80, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(PendingAccounting::decode(&bytes, &manifest), Ok(value));
    assert_eq!(
        value.advance(1, (1_u64 << 63) + 1),
        Err(PendingAccountingError::Overflow)
    );
    assert_eq!(value.advance(0, 1_u64 << 63), Ok(value));
    assert_eq!(
        value.advance(0, (1_u64 << 63) + 1),
        Err(PendingAccountingError::SourceMismatch)
    );
    assert_eq!(
        value.advance(1, 1_u64 << 63),
        Err(PendingAccountingError::SourceMismatch)
    );
    assert_eq!(
        pending_accounting::charge(u64::MAX, 1, 0),
        Err(PendingAccountingError::Overflow)
    );
    for end in 0..bytes.len() {
        assert!(PendingAccounting::decode(&bytes[..end], &manifest).is_err());
    }
    let mut other = *manifest.identity();
    other.policy_epoch = bigbrotr_model::PolicyEpoch::new(2);
    let other = IngestManifest::new(other, *manifest.allocation_namespaces());
    assert_eq!(
        PendingAccounting::decode(&bytes, &other),
        Err(PendingAccountingError::ManifestMismatch)
    );
    let key = IngestKey::PendingAccounting.encode().unwrap();
    assert_eq!(key.column_family(), ColumnFamily::Default);
    assert_eq!(key.as_bytes(), &[1, 0x0b]);
}

#[test]
fn counterless_value_schema_one_refuses_even_for_an_empty_historical_store() {
    for populated in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let mut store =
            IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        if populated {
            populate(&mut store, &b);
        }
        let mut old = store.manifest().encode().as_bytes().to_vec();
        old[4..6].copy_from_slice(&1_u16.to_be_bytes());
        store.db.put([1, 1], old).unwrap();
        store.db.delete([1, 0x0b]).unwrap();
        drop(store);
        assert!(matches!(
            IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)),
            Err(StoreError::InvalidManifest(
                ManifestError::UnsupportedFormat
            ))
        ));
    }
}
