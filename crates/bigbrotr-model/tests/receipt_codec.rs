use bigbrotr_model::{
    AdmissionBatch, DatasetId, Digest256, FactRef, OwnerEpoch, PolicyEpoch, ProducerEpoch,
    ProducerRequestId, ProducerSlotId, ProtectionClass, ProtectionContext, Receipt,
    RecoveryGenerationId, RequestSequence, Sequence, SourceDependencies, SourceRecordId,
    TopologyGeneration,
    effect::EffectError,
    receipt_codec::{ReceiptCodecError, ReceiptDecodeLimits},
    source::SourceError,
};
use minicbor::Encoder;
use std::{convert::Infallible, mem::size_of};

const PREFIX: &[u8] = b"bigbrotr/receipt/1\0";
const NEW_FACTS_HEX: &str = "62696762726f74722f726563656970742f310086018350040404040404040404040404040404041bffffffffffffffff1818582008080808080808080808080808080808080808080808080808080808080808088550010101010101010101010101010101011bffffffffffffffff0150030303030303030303030303030303030082828450010101010101010101010101010101010102175820bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb82845001010101010101010101010101010101010218185820cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc848284500101010101010101010101010101010100011bffffffffffffffff5820aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa828450010101010101010101010101010101010102175820bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb82845001010101010101010101010101010101010218185820cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc8284500101010101010101010101010101010102011bffffffffffffffff5820dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const ALREADY_DURABLE_HEX: &str = "62696762726f74722f726563656970742f31008602835005050505050505050505050505050505010158200909090909090909090909090909090909090909090909090909090909090909855001010101010101010101010101010101000150000000000000000000000000000000001bfffffffffffffffff681828450010101010101010101010101010101011affffffff1bffffffffffffffff1bffffffffffffffff5820eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn dataset(value: u8) -> DatasetId {
    DatasetId::try_from([value; 16]).unwrap()
}
fn source(dataset_id: DatasetId, shard: u32, epoch: u64, sequence: u64) -> SourceRecordId {
    SourceRecordId {
        dataset_id,
        shard_id: shard,
        owner_epoch: OwnerEpoch::new(epoch).unwrap(),
        sequence: Sequence::new(sequence).unwrap(),
    }
}
fn fact(dataset_id: DatasetId, shard: u32, epoch: u64, sequence: u64, byte: u8) -> FactRef {
    FactRef {
        source_record_id: source(dataset_id, shard, epoch, sequence),
        payload_digest: Digest256::from_bytes([byte; 32]),
    }
}
fn request(slot: u8, epoch: u64, sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from([slot; 16]).unwrap(),
        producer_epoch: ProducerEpoch::new(epoch).unwrap(),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}
fn context(dataset_id: DatasetId, topology: u64, recovery: u8, policy: u64) -> ProtectionContext {
    ProtectionContext {
        dataset_id,
        topology_generation: TopologyGeneration::new(topology),
        protection_class: ProtectionClass::LocalSynced,
        recovery_generation: RecoveryGenerationId::from_bytes([recovery; 16]),
        policy_epoch: PolicyEpoch::new(policy),
    }
}
fn new_facts() -> Receipt {
    let d = dataset(1);
    let batch = vec![fact(d, 1, 2, 23, 0xbb), fact(d, 1, 2, 24, 0xcc)];
    let dependencies = vec![
        fact(d, 0, 1, u64::MAX, 0xaa),
        batch[0],
        batch[1],
        fact(d, 2, 1, u64::MAX, 0xdd),
    ];
    Receipt::new_facts(
        request(4, u64::MAX, 24),
        Digest256::from_bytes([8; 32]),
        context(d, u64::MAX, 3, 0),
        AdmissionBatch::new(batch).unwrap(),
        SourceDependencies::new(d, dependencies).unwrap(),
    )
    .unwrap()
}
fn already_durable() -> Receipt {
    let d = dataset(1);
    Receipt::already_durable(
        request(5, 1, 1),
        Digest256::from_bytes([9; 32]),
        context(d, 0, 0, u64::MAX),
        SourceDependencies::new(d, vec![fact(d, u32::MAX, u64::MAX, u64::MAX, 0xee)]).unwrap(),
    )
    .unwrap()
}

#[test]
fn frozen_operational_receipts_round_trip_exactly() {
    let cases = [
        (new_facts(), bytes(NEW_FACTS_HEX)),
        (already_durable(), bytes(ALREADY_DURABLE_HEX)),
    ];
    for (receipt, expected) in cases {
        assert_eq!(receipt.encode_persistent(expected.len()).unwrap(), expected);
        assert_eq!(
            Receipt::decode_persistent(&expected, ReceiptDecodeLimits::default()).unwrap(),
            receipt
        );
        assert_eq!(
            receipt.encode_persistent(expected.len() - 1),
            Err(ReceiptCodecError::InputLimit)
        );
    }
}

fn encode_raw(
    result: u8,
    request: ProducerRequestId,
    request_digest: Digest256,
    context: ProtectionContext,
    protection_code: u8,
    batch: Option<&[FactRef]>,
    dependencies: &[FactRef],
) -> Vec<u8> {
    struct Bytes(Vec<u8>);
    impl minicbor::encode::Write for Bytes {
        type Error = Infallible;
        fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
            self.0.extend_from_slice(bytes);
            Ok(())
        }
    }
    let mut encoder = Encoder::new(Bytes(PREFIX.to_vec()));
    encoder
        .array(6)
        .unwrap()
        .u8(result)
        .unwrap()
        .array(3)
        .unwrap();
    encoder
        .bytes(request.producer_slot_id.as_bytes())
        .unwrap()
        .u64(request.producer_epoch.get())
        .unwrap()
        .u64(request.request_sequence.get())
        .unwrap()
        .bytes(request_digest.as_bytes())
        .unwrap()
        .array(5)
        .unwrap()
        .bytes(context.dataset_id.as_bytes())
        .unwrap()
        .u64(context.topology_generation.get())
        .unwrap()
        .u8(protection_code)
        .unwrap()
        .bytes(context.recovery_generation.as_bytes())
        .unwrap()
        .u64(context.policy_epoch.get())
        .unwrap();
    match batch {
        Some(records) => {
            encoder.array(records.len() as u64).unwrap();
            for record in records {
                encode_fact_ref(record, &mut encoder);
            }
        }
        None => {
            encoder.null().unwrap();
        }
    }
    encoder.array(dependencies.len() as u64).unwrap();
    for record in dependencies {
        encode_fact_ref(record, &mut encoder);
    }
    encoder.into_writer().0
}
fn encode_fact_ref<W>(record: &FactRef, encoder: &mut Encoder<W>)
where
    W: minicbor::encode::Write,
    W::Error: std::fmt::Debug,
{
    let id = record.source_record_id;
    encoder
        .array(2)
        .unwrap()
        .array(4)
        .unwrap()
        .bytes(id.dataset_id.as_bytes())
        .unwrap()
        .u32(id.shard_id)
        .unwrap()
        .u64(id.owner_epoch.get())
        .unwrap()
        .u64(id.sequence.get())
        .unwrap()
        .bytes(record.payload_digest.as_bytes())
        .unwrap();
}
fn decode(input: &[u8]) -> Result<Receipt, ReceiptCodecError> {
    Receipt::decode_persistent(input, ReceiptDecodeLimits::default())
}

#[test]
fn variant_version_protection_and_framing_are_closed() {
    let new = bytes(NEW_FACTS_HEX);
    let already = bytes(ALREADY_DURABLE_HEX);
    for length in 0..new.len() {
        assert!(decode(&new[..length]).is_err(), "truncated at {length}");
    }
    let mut trailing = new.clone();
    trailing.push(0);
    assert_eq!(decode(&trailing), Err(ReceiptCodecError::NonCanonical));
    let mut version = new.clone();
    version[PREFIX.len() - 2] = b'2';
    assert_eq!(decode(&version), Err(ReceiptCodecError::IncompatibleSchema));
    let mut arity = new.clone();
    arity[PREFIX.len()] = 0x85;
    assert_eq!(decode(&arity), Err(ReceiptCodecError::Malformed));
    let mut indefinite = new.clone();
    indefinite[PREFIX.len()] = 0x9f;
    assert_eq!(decode(&indefinite), Err(ReceiptCodecError::Malformed));
    let mut result = new.clone();
    result[PREFIX.len() + 1] = 3;
    assert_eq!(decode(&result), Err(ReceiptCodecError::IncompatibleSchema));

    let d = dataset(1);
    let dependency = fact(d, 0, 1, 1, 1);
    assert_eq!(
        decode(&encode_raw(
            1,
            request(1, 1, 1),
            Digest256::from_bytes([1; 32]),
            context(d, 1, 1, 1),
            1,
            None,
            &[dependency]
        )),
        Err(ReceiptCodecError::IncompatibleSchema)
    );
    assert_eq!(
        decode(&encode_raw(
            2,
            request(1, 1, 1),
            Digest256::from_bytes([1; 32]),
            context(d, 1, 1, 1),
            1,
            Some(&[dependency]),
            &[dependency]
        )),
        Err(ReceiptCodecError::IncompatibleSchema)
    );
    assert_eq!(
        decode(&encode_raw(
            2,
            request(1, 1, 1),
            Digest256::from_bytes([1; 32]),
            context(d, 1, 1, 1),
            2,
            None,
            &[dependency]
        )),
        Err(ReceiptCodecError::IncompatibleSchema)
    );
    assert!(decode(&already).is_ok());
}

// Independent definite-length walker for frozen fixtures only. Product decode
// never calls it. It replaces each primitive with a larger equivalent head.
fn heads(bytes: &[u8], position: &mut usize, found: &mut Vec<(usize, usize, u8, u64)>) {
    let start = *position;
    let head = bytes[*position];
    *position += 1;
    let extra = match head & 31 {
        n @ 0..=23 => {
            found.push((start, *position, head & 0xe0, n.into()));
            0
        }
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => panic!("closed fixture expected"),
    };
    let mut argument = u64::from(head & 31);
    if extra > 0 {
        argument = 0;
        for _ in 0..extra {
            argument = argument * 256 + u64::from(bytes[*position]);
            *position += 1;
        }
        found.push((start, *position, head & 0xe0, argument));
    }
    match head & 0xe0 {
        0x00 | 0xe0 => (),
        0x40 => *position += argument as usize,
        0x80 => {
            for _ in 0..argument {
                heads(bytes, position, found);
            }
        }
        _ => panic!("closed fixture expected"),
    }
}

#[test]
fn all_equivalent_wider_heads_are_noncanonical() {
    let mut checked = 0;
    for input in [bytes(NEW_FACTS_HEX), bytes(ALREADY_DURABLE_HEX)] {
        let mut position = PREFIX.len();
        let mut found = Vec::new();
        heads(&input, &mut position, &mut found);
        assert_eq!(position, input.len());
        for (start, end, major, value) in found {
            if major == 0xe0 {
                continue;
            }
            let width = end - start;
            for extra in [1usize, 2, 4, 8] {
                if extra < width {
                    continue;
                }
                let info = match extra {
                    1 => 24,
                    2 => 25,
                    4 => 26,
                    _ => 27,
                };
                let mut modified = input.clone();
                let mut replacement = vec![major | info];
                replacement.extend_from_slice(&value.to_be_bytes()[8 - extra..]);
                modified.splice(start..end, replacement);
                assert_eq!(decode(&modified), Err(ReceiptCodecError::NonCanonical));
                checked += 1;
            }
        }
    }
    assert!(
        checked > 0,
        "the literal corpus must exercise noncanonical heads"
    );
}

#[test]
fn constructors_reject_corrupt_namespaces_order_and_closure() {
    let d = dataset(1);
    let foreign = dataset(2);
    let req = request(1, 1, 1);
    let digest = Digest256::from_bytes([1; 32]);
    let ctx = context(d, 1, 1, 1);
    let r1 = fact(d, 0, 1, 1, 1);
    let r2 = fact(d, 0, 1, 2, 2);
    let r3 = fact(d, 0, 1, 3, 3);

    let already = |dependencies: &[FactRef]| encode_raw(2, req, digest, ctx, 1, None, dependencies);
    assert_eq!(
        decode(&already(&[])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::EmptyDependencies
        ))
    );
    assert_eq!(
        decode(&already(&[r2, r1])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::UnorderedDependencies
        ))
    );
    assert_eq!(
        decode(&already(&[r1, r1])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::UnorderedDependencies
        ))
    );
    assert_eq!(
        decode(&already(&[fact(foreign, 0, 1, 1, 1)])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::ForeignDataset
        ))
    );

    let raw_new = |batch: &[FactRef], dependencies: &[FactRef]| {
        encode_raw(1, req, digest, ctx, 1, Some(batch), dependencies)
    };
    assert_eq!(
        decode(&raw_new(&[], &[r1])),
        Err(ReceiptCodecError::InvalidBatch(SourceError::InvalidBatch))
    );
    assert_eq!(
        decode(&raw_new(&[r1, r3], &[r1, r3])),
        Err(ReceiptCodecError::InvalidBatch(SourceError::InvalidBatch))
    );
    assert_eq!(
        decode(&raw_new(&[r1, r2], &[r1])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::MissingBatchDependency
        ))
    );
    let mut changed = r1;
    changed.payload_digest = Digest256::from_bytes([9; 32]);
    assert_eq!(
        decode(&raw_new(&[r1], &[changed])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::ConflictingDependency
        ))
    );
    assert_eq!(
        decode(&raw_new(&[r1, r2], &[r1, r2, r3])),
        Err(ReceiptCodecError::InvalidReceipt(
            EffectError::FutureDependency
        ))
    );
}

#[test]
fn byte_allocation_and_record_counts_are_bounded_before_reserve() {
    let input = bytes(NEW_FACTS_HEX);
    let mut limits = ReceiptDecodeLimits {
        max_encoded_bytes: input.len() - 1,
        ..Default::default()
    };
    assert_eq!(
        Receipt::decode_persistent(&input, limits),
        Err(ReceiptCodecError::InputLimit)
    );
    limits = ReceiptDecodeLimits {
        max_batch_records: 1,
        ..Default::default()
    };
    assert_eq!(
        Receipt::decode_persistent(&input, limits),
        Err(ReceiptCodecError::BatchRecordLimit)
    );
    limits = ReceiptDecodeLimits {
        max_dependencies: 3,
        ..Default::default()
    };
    assert_eq!(
        Receipt::decode_persistent(&input, limits),
        Err(ReceiptCodecError::DependencyLimit)
    );
    let requested = 6 * size_of::<FactRef>();
    limits = ReceiptDecodeLimits {
        max_allocation_bytes: requested,
        ..Default::default()
    };
    assert!(Receipt::decode_persistent(&input, limits).is_ok());
    limits.max_allocation_bytes -= 1;
    assert_eq!(
        Receipt::decode_persistent(&input, limits),
        Err(ReceiptCodecError::AllocationLimit)
    );

    let mut huge_batch = input.clone();
    let marker = bytes(&format!("0082828450{}", "01".repeat(16)));
    let position = huge_batch
        .windows(marker.len())
        .position(|part| part == marker)
        .unwrap()
        + 1;
    huge_batch.splice(
        position..position + 1,
        [0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    );
    assert_eq!(
        decode(&huge_batch),
        Err(ReceiptCodecError::BatchRecordLimit)
    );

    let mut huge_dependencies = bytes(ALREADY_DURABLE_HEX);
    let marker = bytes(&format!("f681828450{}", "01".repeat(16)));
    let position = huge_dependencies
        .windows(marker.len())
        .position(|part| part == marker)
        .unwrap()
        + 1;
    huge_dependencies.splice(
        position..position + 1,
        [0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    );
    assert_eq!(
        decode(&huge_dependencies),
        Err(ReceiptCodecError::DependencyLimit)
    );
}

#[test]
fn scalar_domains_and_fixed_identity_widths_are_enforced() {
    let d = dataset(1);
    let dependency = fact(d, 0, 1, 1, 1);
    let valid = encode_raw(
        2,
        request(1, 1, 1),
        Digest256::from_bytes([1; 32]),
        context(d, 0, 0, 0),
        1,
        None,
        &[dependency],
    );
    let slot = valid
        .windows(17)
        .position(|part| part == [&[0x50][..], &[1; 16][..]].concat())
        .unwrap();
    let mut short_slot = valid.clone();
    short_slot[slot] = 0x4f;
    assert_eq!(decode(&short_slot), Err(ReceiptCodecError::InvalidValue));
    let mut zero_epoch = valid.clone();
    zero_epoch[slot + 17] = 0;
    assert_eq!(decode(&zero_epoch), Err(ReceiptCodecError::InvalidValue));
    let mut zero_sequence = valid;
    zero_sequence[slot + 18] = 0;
    assert_eq!(decode(&zero_sequence), Err(ReceiptCodecError::InvalidValue));
}
