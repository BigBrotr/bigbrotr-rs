use super::*;
use bigbrotr_model::TopologyGeneration;
use bigbrotr_protocol::archive_cut::CutBound;

fn fixture(
    operation: [u8; 16],
    binding: Digest256,
    dataset: DatasetId,
    catalog: OwnerResourceId,
) -> Data {
    let read = CutRead {
        dataset_id: dataset,
        topology_generation: TopologyGeneration::new(2),
        bounds: vec![CutBound {
            shard_id: 0,
            owner_epoch: 3,
            complete_through: 0,
        }],
        receipts: vec![],
        snapshot_id: Some(7),
    };
    let observation = CutObservation {
        format: FORMAT.into(),
        snapshot_id: 7,
        request_digest: read.digest(),
        family_counts: [0; 5],
    };
    Data {
        binding,
        dataset,
        catalog,
        operation,
        expected_revision: 5,
        observation,
        read,
    }
}
fn sample() -> Data {
    fixture(
        [4; 16],
        Digest256::from_bytes([2; 32]),
        [1; 16].try_into().unwrap(),
        [3; 16].try_into().unwrap(),
    )
}
fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn rehash(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let hash = Sha256::new()
        .chain_update(DOMAIN)
        .chain_update(&bytes[..end])
        .finalize();
    bytes[end..].copy_from_slice(&hash);
}

// Independent Python struct.pack/JSON/hashlib vector; no Rust encoder generated
// these expected bytes. Actual filesystem/native/PG qualification is separate.
const EXPECTED: &str = "424246524f4e54010202020202020202020202020202020202020202020202020202020202020202010101010101010101010101010101010303030303030303030303030303030304040404040404040404040404040404000000000000000500000000000000079d43ca21e09a81ebfc647de1ba0ef789575f1f8d9b30b53289459e35cdf3aa4200000000000000000000000000000000000000000000000000000000000000000000000000000000000000a87b22646174617365745f6964223a223031303130313031303130313031303130313031303130313031303130313031222c22746f706f6c6f67795f67656e65726174696f6e223a2232222c22626f756e6473223a5b7b2273686172645f6964223a302c226f776e65725f65706f6368223a332c22636f6d706c6574655f7468726f756768223a307d5d2c227265636569707473223a5b5d2c22736e617073686f745f6964223a377d6f8cc906f2542e2dc2d4e0baec2ba93565a1492a2e746fb37926fc5dd9ed4ebb";

#[test]
fn literal_bytes_checksum_and_every_truncated_or_single_byte_changed_record() {
    let data = sample();
    let expected = hex(EXPECTED);
    assert_eq!(encode(&data).unwrap(), expected);
    let decoded = decode(
        &expected,
        data.binding,
        data.dataset,
        data.catalog,
        data.operation,
    )
    .unwrap();
    assert_eq!(decoded.read, data.read);
    assert_eq!(decoded.observation, data.observation);
    for end in 0..expected.len() {
        assert!(
            decode(
                &expected[..end],
                data.binding,
                data.dataset,
                data.catalog,
                data.operation
            )
            .is_err(),
            "prefix {end}"
        );
    }
    for index in 0..expected.len() {
        let mut changed = expected.clone();
        changed[index] ^= 1;
        assert!(
            decode(
                &changed,
                data.binding,
                data.dataset,
                data.catalog,
                data.operation
            )
            .is_err(),
            "byte {index}"
        );
    }
}
#[test]
fn rehashed_wrong_semantics_or_noncanonical_json_remain_invalid() {
    let data = sample();
    let expected = hex(EXPECTED);
    for index in [0, 8, 40, 56, 72, 96, 104, 136] {
        let mut changed = expected.clone();
        changed[index] ^= 1;
        rehash(&mut changed);
        assert!(
            decode(
                &changed,
                data.binding,
                data.dataset,
                data.catalog,
                data.operation
            )
            .is_err(),
            "field {index}"
        );
    }
    let mut changed = expected.clone();
    changed[88..96].fill(255);
    rehash(&mut changed);
    assert!(
        decode(
            &changed,
            data.binding,
            data.dataset,
            data.catalog,
            data.operation
        )
        .is_err()
    );
    let mut changed = expected.clone();
    changed.insert(PREFIX, b' ');
    let n = changed.len() - PREFIX - 32;
    changed[176..180].copy_from_slice(&(n as u32).to_be_bytes());
    rehash(&mut changed);
    assert!(
        decode(
            &changed,
            data.binding,
            data.dataset,
            data.catalog,
            data.operation
        )
        .is_err()
    );
    assert!(parse_name(b"frontier-0404040404040404040404040404040A.intent").is_err());
    assert!(parse_name(b"frontier-partial").is_err());
    assert!(parse_name(b"frontier-00000000000000000000000000000000.intent").is_err());
    assert_eq!(parse_name(b"coordinator.json").unwrap(), None);
}

// Internal fixture only: creates declared journal bytes, never ObservedCut or a
// capture. The production API accepts only the actual joined observation owner.
pub(in crate::storage::archive) fn retain_fixture(
    owner: &LocalCoordinator,
    operation: [u8; 16],
    permit: OwnedPermit,
    cancel: &AtomicBool,
) -> Result<RetainedFrontierIntent> {
    owner.store_frontier_intent(
        fixture(
            operation,
            owner.binding_digest(),
            owner.identity().dataset(),
            owner.identity().catalog(),
        ),
        permit,
        cancel,
    )
}
pub(in crate::storage::archive) fn fixture_bytes(
    owner: &LocalCoordinator,
    operation: [u8; 16],
) -> Vec<u8> {
    encode(&fixture(
        operation,
        owner.binding_digest(),
        owner.identity().dataset(),
        owner.identity().catalog(),
    ))
    .unwrap()
}
pub(in crate::storage::archive) fn fixture_name(operation: [u8; 16]) -> String {
    name(operation)
}
