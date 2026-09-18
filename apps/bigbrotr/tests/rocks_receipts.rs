use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::rocks::{
        RocksNativeLimits, RocksNativeSettings,
        manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
        store::{IngestStore, ReceiptReadError, required_receipt_data_bytes},
    },
};
use bigbrotr_model::{
    Digest256, OwnerEpoch, PolicyEpoch, ProducerEpoch, ProducerRequestId, RecoveryGenerationId,
    RequestSequence, TopologyGeneration,
    receipt_codec::{ReceiptCodecError, ReceiptDecodeLimits},
};
use rocksdb::{DB, Options, WriteOptions};
use std::{path::Path, sync::atomic::AtomicBool};

const MIB: u64 = 1024 * 1024;
fn manifest() -> IngestManifest {
    IngestManifest::new(
        ManifestIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
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
fn permit(b: &ResourceBudget, data: u64, native: u64) -> OwnedPermit {
    b.try_acquire(ResourceRequest::data(data, 1, native).unwrap())
        .unwrap()
}
fn limits() -> ReceiptDecodeLimits {
    ReceiptDecodeLimits {
        max_encoded_bytes: 8192,
        max_allocation_bytes: 65536,
        max_batch_records: 8,
        max_dependencies: 8,
    }
}
fn request() -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: [4; 16].try_into().unwrap(),
        producer_epoch: ProducerEpoch::new(u64::MAX).unwrap(),
        request_sequence: RequestSequence::new(24).unwrap(),
    }
}
fn key(request: ProducerRequestId) -> Vec<u8> {
    let mut bytes = vec![1, 0x30];
    bytes.extend(request.producer_slot_id.as_bytes());
    bytes.extend(request.producer_epoch.get().to_be_bytes());
    bytes.extend(request.request_sequence.get().to_be_bytes());
    bytes
}
fn fixture_named(name: &str) -> Vec<u8> {
    let json: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/receipts-v1.json"
    ))
    .unwrap();
    json["vectors"][name]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn fixture() -> Vec<u8> {
    fixture_named("new_facts")
}
fn raw(path: &Path) -> DB {
    let mut options = Options::default();
    options.set_max_open_files(32);
    options.set_max_background_jobs(2);
    options.set_write_buffer_size(MIB as usize);
    DB::open_cf(&options, path, ["default", "facts", "receipts"]).unwrap()
}
fn install(path: &Path, request: ProducerRequestId, bytes: &[u8]) -> u64 {
    let db = raw(path);
    let mut write = WriteOptions::default();
    write.set_sync(true);
    db.put_cf_opt(
        db.cf_handle("receipts").unwrap(),
        key(request),
        bytes,
        &write,
    )
    .unwrap();
    db.latest_sequence_number()
}
fn open(path: &Path, b: &ResourceBudget) -> IngestStore {
    IngestStore::reopen(path, manifest(), native(), permit(b, 0, 128 * MIB)).unwrap()
}
fn init(path: &Path, b: &ResourceBudget) {
    drop(IngestStore::initialize(path, manifest(), native(), permit(b, 0, 128 * MIB)).unwrap());
}

#[test]
fn actual_receipt_read_keeps_original_context_and_complete_bytes_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    init(&path, &b);
    let before = install(&path, request(), &fixture());
    let store = open(&path, &b);
    let found = store
        .read_receipt(
            request(),
            Digest256::from_bytes([8; 32]),
            limits(),
            permit(&b, MIB, 0),
            &AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();
    assert_eq!(found.receipt().encode_persistent(8192).unwrap(), fixture());
    assert_eq!(found.receipt().request_id(), request());
    assert_eq!(found.receipt().batch().unwrap().first().sequence.get(), 23);
    assert_eq!(found.receipt().batch().unwrap().last().sequence.get(), 24);
    assert_eq!(found.receipt().dependencies().records().len(), 4);
    assert_eq!(found.receipt().protection_context().policy_epoch.get(), 0);
    assert_eq!(
        found
            .receipt()
            .protection_context()
            .topology_generation
            .get(),
        u64::MAX
    );
    assert_eq!(b.snapshot().used().data_bytes(), MIB);
    drop(found);
    assert_eq!(b.snapshot().used().data_bytes(), 0);
    let missing = ProducerRequestId {
        request_sequence: RequestSequence::new(25).unwrap(),
        ..request()
    };
    assert!(
        store
            .read_receipt(
                missing,
                Digest256::from_bytes([8; 32]),
                limits(),
                permit(&b, MIB, 0),
                &AtomicBool::new(false)
            )
            .unwrap()
            .is_none()
    );
    drop(store);
    assert_eq!(raw(&path).latest_sequence_number(), before);
}

#[test]
fn conflict_key_binding_corruption_and_future_schema_never_become_absence() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    init(&path, &b);
    let mut future = fixture();
    future[b"bigbrotr/receipt/".len()] = b'2';
    let mut truncated = fixture();
    truncated.pop();
    for (bytes, lookup, digest, expected) in [
        (
            fixture(),
            request(),
            [9; 32],
            ReceiptReadError::RequestConflict,
        ),
        (
            fixture(),
            ProducerRequestId {
                request_sequence: RequestSequence::new(25).unwrap(),
                ..request()
            },
            [8; 32],
            ReceiptReadError::WrongRequest,
        ),
        (
            future,
            request(),
            [8; 32],
            ReceiptReadError::Decode(ReceiptCodecError::IncompatibleSchema),
        ),
        (
            truncated,
            request(),
            [8; 32],
            ReceiptReadError::Decode(ReceiptCodecError::Malformed),
        ),
    ] {
        let before = install(&path, lookup, &bytes);
        let store = open(&path, &b);
        let result = store.read_receipt(
            lookup,
            Digest256::from_bytes(digest),
            limits(),
            permit(&b, MIB, 0),
            &AtomicBool::new(false),
        );
        assert!(
            matches!(result,Err(e) if e==expected),
            "expected {expected:?}"
        );
        assert_eq!(b.snapshot().used().data_bytes(), 0);
        drop(store);
        assert_eq!(raw(&path).latest_sequence_number(), before);
    }
}

#[test]
fn read_budget_and_cancellation_fail_without_claiming_an_outcome() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    init(&path, &b);
    install(&path, request(), &fixture());
    let store = open(&path, &b);
    assert!(required_receipt_data_bytes(limits()).unwrap() > 65536);
    for (cap, bytes, cancel, expected) in [
        (
            limits(),
            0,
            false,
            ReceiptReadError::InsufficientReservation,
        ),
        (limits(), MIB, true, ReceiptReadError::Cancelled),
        (
            ReceiptDecodeLimits {
                max_encoded_bytes: 1,
                ..limits()
            },
            MIB,
            false,
            ReceiptReadError::Decode(ReceiptCodecError::InputLimit),
        ),
        (
            ReceiptDecodeLimits {
                max_dependencies: 1,
                ..limits()
            },
            MIB,
            false,
            ReceiptReadError::Decode(ReceiptCodecError::DependencyLimit),
        ),
        (
            ReceiptDecodeLimits {
                max_allocation_bytes: 1,
                ..limits()
            },
            MIB,
            false,
            ReceiptReadError::Decode(ReceiptCodecError::AllocationLimit),
        ),
        (
            ReceiptDecodeLimits {
                max_encoded_bytes: 0,
                ..limits()
            },
            MIB,
            false,
            ReceiptReadError::InvalidLimits,
        ),
        (
            ReceiptDecodeLimits {
                max_allocation_bytes: usize::MAX,
                ..limits()
            },
            MIB,
            false,
            ReceiptReadError::InvalidLimits,
        ),
    ] {
        let result = store.read_receipt(
            request(),
            Digest256::from_bytes([8; 32]),
            cap,
            permit(&b, bytes, 0),
            &AtomicBool::new(cancel),
        );
        assert!(
            matches!(result,Err(e) if e==expected),
            "expected {expected:?}"
        );
        assert_eq!(b.snapshot().used().data_bytes(), 0);
    }
}

#[test]
fn valid_detail_from_another_dataset_is_not_a_local_result() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let other = IngestManifest::new(
        ManifestIdentity {
            dataset_id: [2; 16].try_into().unwrap(),
            ..*manifest().identity()
        },
        *manifest().allocation_namespaces(),
    );
    drop(IngestStore::initialize(&path, other, native(), permit(&b, 0, 128 * MIB)).unwrap());
    install(&path, request(), &fixture());
    let store = IngestStore::reopen(&path, other, native(), permit(&b, 0, 128 * MIB)).unwrap();
    let result = store.read_receipt(
        request(),
        Digest256::from_bytes([8; 32]),
        limits(),
        permit(&b, MIB, 0),
        &AtomicBool::new(false),
    );
    assert!(matches!(result, Err(ReceiptReadError::ForeignDataset)));
    assert_eq!(b.snapshot().used().data_bytes(), 0);
}

#[test]
fn already_durable_detail_preserves_null_batch_and_its_retained_dependencies() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    init(&path, &b);
    let req = ProducerRequestId {
        producer_slot_id: [5; 16].try_into().unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let bytes = fixture_named("already_durable");
    let before = install(&path, req, &bytes);
    let store = open(&path, &b);
    let found = store
        .read_receipt(
            req,
            Digest256::from_bytes([9; 32]),
            limits(),
            permit(&b, MIB, 0),
            &AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();
    assert_eq!(found.receipt().encode_persistent(8192).unwrap(), bytes);
    assert!(found.receipt().batch().is_none());
    let refs = found.receipt().dependencies().records();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].source_record_id.shard_id, u32::MAX);
    assert_eq!(refs[0].source_record_id.owner_epoch.get(), u64::MAX);
    assert_eq!(refs[0].source_record_id.sequence.get(), u64::MAX);
    assert_eq!(refs[0].payload_digest.as_bytes(), &[0xee; 32]);
    drop(found);
    drop(store);
    assert_eq!(raw(&path).latest_sequence_number(), before);
}
