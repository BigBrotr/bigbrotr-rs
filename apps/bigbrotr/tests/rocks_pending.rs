use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::rocks::{
        RocksNativeLimits, RocksNativeSettings,
        manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
        store::{IngestStore, PendingBatchHeader, PendingError, PendingReadLimits},
    },
};
use bigbrotr_model::{
    AdmissionBatch, FactRef, OwnerEpoch, PolicyEpoch, RecoveryGenerationId, Sequence,
    SourceRecordId, TopologyGeneration, codec::FactDecodeLimits,
};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use std::{path::Path, sync::atomic::AtomicBool};

const MIB: u64 = 1024 * 1024;
const DIGESTS: [&str; 2] = [
    "31ef855d10a0edf3786dc3f761be51f4a53be706eeddb4eed0388830a513d449",
    "583ec59a3b00e25525e47f6b77446be10f6b0e151b2b8e89526ae9ec6c999c4e",
];
const BATCH_DIGEST: &str = "5a407318a8317d6c3dfb7a765cffc8b77cabd4602b7cf64d991d853d0651ad96";
fn hex(text: &str) -> Vec<u8> {
    let (pairs, remainder) = text.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn id(sequence: u64) -> SourceRecordId {
    SourceRecordId {
        dataset_id: [1; 16].try_into().unwrap(),
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(sequence).unwrap(),
    }
}
fn manifest() -> IngestManifest {
    IngestManifest::new(
        ManifestIdentity {
            dataset_id: id(2).dataset_id,
            topology_generation: TopologyGeneration::new(1),
            owner_resource_id: [2; 16].try_into().unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: [3; 16].try_into().unwrap(),
            recovery_generation: RecoveryGenerationId::from_bytes([4; 16]),
            policy_epoch: PolicyEpoch::new(1),
        },
        AllocationNamespaces::from_entries(&[]).unwrap(),
    )
}
fn native() -> RocksNativeLimits {
    RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 32,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .unwrap()
}
fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            131 * MIB,
            0,
            ResourceLimits::new(2 * MIB, 10, 128 * MIB, MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    )
}
fn grant(b: &ResourceBudget, data: u64, native: u64) -> OwnedPermit {
    b.try_acquire(ResourceRequest::data(data, 1, native).unwrap())
        .unwrap()
}
fn limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: 4,
        max_encoded_bytes: 16 * 1024,
        fact: FactDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 65536,
            max_tag_nodes: 128,
            max_string_bytes: 1024,
        },
    }
}
fn raw(path: &Path) -> DB {
    let mut options = Options::default();
    options.set_max_open_files(32);
    options.set_max_background_jobs(2);
    options.set_write_buffer_size(MIB as usize);
    DB::open_cf(&options, path, ["default", "facts", "receipts"]).unwrap()
}
fn source_key(tag: u8, sequence: Option<u64>) -> Vec<u8> {
    // Independent fixed-width key fixture, not the product encoder.
    let mut key = vec![1, tag];
    key.extend([1; 16]);
    key.extend([0; 4]);
    key.extend(1u64.to_be_bytes());
    if let Some(sequence) = sequence {
        key.extend(sequence.to_be_bytes());
    }
    key
}
fn header() -> Vec<u8> {
    let mut bytes = vec![1, 0x21];
    bytes.extend(3u64.to_be_bytes());
    bytes.extend(hex(BATCH_DIGEST));
    bytes
}
fn facts() -> Vec<Vec<u8>> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/core-fingerprints.json"
    ))
    .unwrap();
    let mut values: Vec<Vec<u8>> = (0..2)
        .map(|i| hex(fixture["vectors"][i]["preimage_hex"].as_str().unwrap()))
        .collect();
    // The frozen receive vector has source sequence2; this independent fixture
    // changes that exact one-byte CBOR coordinate to3. Its digest above was
    // computed with Python hashlib from the literal bytes, before the adapter.
    assert_eq!(values[1][37], 2);
    values[1][37] = 3;
    values
}
fn value(index: usize) -> Vec<u8> {
    let mut v = vec![1, 0x20];
    v.extend(hex(DIGESTS[index]));
    v.extend(&facts()[index]);
    v
}
fn install(path: &Path) -> u64 {
    let db = raw(path);
    let cf = db.cf_handle("facts").unwrap();
    let mut batch = WriteBatch::default();
    let mut watermark = vec![1, 2];
    watermark.extend(3u64.to_be_bytes());
    batch.put(source_key(2, None), watermark);
    batch.put_cf(cf, source_key(0x21, Some(2)), header());
    for index in 0..2 {
        batch.put_cf(cf, source_key(0x20, Some(index as u64 + 2)), value(index));
    }
    // Receipt detail is deliberately absent: permanent batch completeness must
    // not depend on an operational request result surviving its authorized GC.
    let mut write = WriteOptions::default();
    write.set_sync(true);
    db.write_opt(batch, &write).unwrap();
    db.latest_sequence_number()
}
fn initialize(path: &Path, b: &ResourceBudget) {
    drop(IngestStore::initialize(path, manifest(), native(), grant(b, 0, 128 * MIB)).unwrap());
}

#[test]
fn boundary_value_matches_literal_and_rejects_every_truncation_or_extension() {
    let expected = header();
    let refs = (0..2)
        .map(|i| FactRef {
            source_record_id: id(i as u64 + 2),
            payload_digest: hex(DIGESTS[i])
                .try_into()
                .map(bigbrotr_model::Digest256::from_bytes)
                .unwrap(),
        })
        .collect();
    let batch = AdmissionBatch::new(refs).unwrap();
    let encoded = PendingBatchHeader::from_batch(&batch);
    assert_eq!(encoded.encode().as_slice(), expected);
    assert_eq!(
        PendingBatchHeader::decode(id(2), &expected).unwrap(),
        encoded
    );
    for end in 0..expected.len() {
        assert_eq!(
            PendingBatchHeader::decode(id(2), &expected[..end]),
            Err(PendingError::Malformed)
        );
    }
    let mut bad = expected.clone();
    bad.push(0);
    assert_eq!(
        PendingBatchHeader::decode(id(2), &bad),
        Err(PendingError::Malformed)
    );
    for position in [0, 1] {
        let mut bad = expected.clone();
        bad[position] = 0xff;
        assert_eq!(
            PendingBatchHeader::decode(id(2), &bad),
            Err(PendingError::Malformed)
        );
    }
    let mut bad = expected;
    bad[2..10].copy_from_slice(&1u64.to_be_bytes());
    assert_eq!(
        PendingBatchHeader::decode(id(2), &bad),
        Err(PendingError::Malformed)
    );
}

#[test]
fn real_pending_read_survives_reopen_without_receipts_and_holds_its_grant() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    initialize(&path, &b);
    let before = install(&path);
    let store = IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let pending = store
        .read_pending_batch(id(2), limits(), grant(&b, MIB, 0), &AtomicBool::new(false))
        .unwrap_or_else(|e| panic!("complete native fixture refused: {e}"))
        .unwrap();
    assert_eq!(pending.records(), facts());
    assert_eq!(pending.batch().first(), id(2));
    assert_eq!(pending.batch().last(), id(3));
    assert_eq!(
        pending.batch().digest().as_bytes().as_slice(),
        hex(BATCH_DIGEST)
    );
    assert_eq!(b.snapshot().used().data_bytes(), MIB);
    drop(pending);
    assert_eq!(b.snapshot().used().data_bytes(), 0);
    assert!(
        store
            .read_pending_batch(id(4), limits(), grant(&b, MIB, 0), &AtomicBool::new(false))
            .unwrap()
            .is_none()
    );
    drop(store);
    assert_eq!(
        raw(&path).latest_sequence_number(),
        before,
        "read must not write a receipt or cursor"
    );
}

#[test]
fn incomplete_corrupt_or_unbounded_native_batches_never_return_partial_success() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    initialize(&path, &b);
    for (fault, expected) in [
        (0, PendingError::MissingFact),
        (1, PendingError::CorruptFact),
        (2, PendingError::IncompleteBatch),
        (3, PendingError::IncompleteBatch),
        (4, PendingError::IncompleteBatch),
        (5, PendingError::Malformed),
        (6, PendingError::CorruptFact),
        (7, PendingError::Limit),
        (8, PendingError::IncompatibleSchema),
    ] {
        install(&path);
        {
            let db = raw(&path);
            let cf = db.cf_handle("facts").unwrap();
            match fault {
                0 => db.delete_cf(cf, source_key(0x20, Some(3))).unwrap(),
                1 => {
                    let mut bytes = value(1);
                    *bytes.last_mut().unwrap() ^= 1;
                    db.put_cf(cf, source_key(0x20, Some(3)), bytes).unwrap();
                }
                2 => {
                    let mut bytes = header();
                    bytes[2..10].copy_from_slice(&4u64.to_be_bytes());
                    db.put_cf(cf, source_key(0x21, Some(2)), bytes).unwrap();
                }
                3 => {
                    let mut bytes = header();
                    bytes[10] ^= 1;
                    db.put_cf(cf, source_key(0x21, Some(2)), bytes).unwrap();
                }
                4 => db.delete(source_key(2, None)).unwrap(),
                5 => {
                    let mut bytes = header();
                    bytes[0] = 2;
                    db.put_cf(cf, source_key(0x21, Some(2)), bytes).unwrap();
                }
                6 => db.put_cf(cf, source_key(0x20, Some(3)), value(0)).unwrap(),
                7 => {
                    let mut bytes = header();
                    bytes[2..10].copy_from_slice(&u64::MAX.to_be_bytes());
                    db.put_cf(cf, source_key(0x21, Some(2)), bytes).unwrap();
                }
                8 => {
                    use sha2::Digest;
                    let mut bytes = value(1);
                    assert_eq!(bytes[48], b'1');
                    bytes[48] = b'2';
                    let digest = sha2::Sha256::digest(&bytes[34..]);
                    bytes[2..34].copy_from_slice(&digest);
                    db.put_cf(cf, source_key(0x20, Some(3)), bytes).unwrap();
                }
                _ => unreachable!(),
            }
        }
        let store =
            IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        let result =
            store.read_pending_batch(id(2), limits(), grant(&b, MIB, 0), &AtomicBool::new(false));
        assert!(
            matches!(result,Err(e) if e==expected),
            "fault {fault} must refuse as {expected:?}"
        );
        assert_eq!(b.snapshot().used().data_bytes(), 0);
    }
}

#[test]
fn read_limits_permits_namespace_and_cancellation_precede_success() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    initialize(&path, &b);
    install(&path);
    let store = IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    for (first, cap, bytes, cancel, expected) in [
        (
            id(2),
            limits(),
            0,
            false,
            PendingError::InsufficientReservation,
        ),
        (id(2), limits(), MIB, true, PendingError::Cancelled),
        (
            SourceRecordId {
                dataset_id: [2; 16].try_into().unwrap(),
                ..id(2)
            },
            limits(),
            MIB,
            false,
            PendingError::ForeignDataset,
        ),
        (
            id(2),
            PendingReadLimits {
                max_records: 1,
                ..limits()
            },
            MIB,
            false,
            PendingError::Limit,
        ),
        (
            id(2),
            PendingReadLimits {
                max_encoded_bytes: 1,
                ..limits()
            },
            MIB,
            false,
            PendingError::Limit,
        ),
        (
            id(2),
            PendingReadLimits {
                max_records: 0,
                ..limits()
            },
            MIB,
            false,
            PendingError::InvalidLimits,
        ),
        (
            id(2),
            PendingReadLimits {
                max_encoded_bytes: usize::MAX,
                ..limits()
            },
            MIB,
            false,
            PendingError::InvalidLimits,
        ),
    ] {
        let result =
            store.read_pending_batch(first, cap, grant(&b, bytes, 0), &AtomicBool::new(cancel));
        assert!(
            matches!(result,Err(e) if e==expected),
            "expected {expected:?}"
        );
        assert_eq!(b.snapshot().used().data_bytes(), 0);
    }
}

#[test]
fn valid_facts_over_decode_limits_are_limited_not_corrupt() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    initialize(&path, &b);
    install(&path);
    let store = IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    for fact in [
        FactDecodeLimits {
            max_allocation_bytes: 1,
            ..limits().fact
        },
        FactDecodeLimits {
            max_string_bytes: 1,
            ..limits().fact
        },
        FactDecodeLimits {
            max_tag_nodes: 1,
            ..limits().fact
        },
    ] {
        let result = store.read_pending_batch(
            id(2),
            PendingReadLimits { fact, ..limits() },
            grant(&b, MIB, 0),
            &AtomicBool::new(false),
        );
        assert!(
            matches!(result, Err(PendingError::Limit)),
            "valid bounded input is not corruption"
        );
    }
}
