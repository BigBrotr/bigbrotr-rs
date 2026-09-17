#[path = "../src/storage/rocks/keys.rs"]
mod keys;

use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, Digest256, EventId, Ordinal, OwnerEpoch,
    ProducerEpoch, ProducerRequestId, ProducerSlotId, QualifiedRef, RequestSequence, Sequence,
    SourceRecordId,
};
use keys::{ColumnFamily, IngestKey, KeyDomain, KeyError, MAX_INGEST_KEY_BYTES, decode_key};

fn literal_bytes(hex: &str) -> Vec<u8> {
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let nibble = |byte| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => panic!("golden key must be lowercase hexadecimal"),
            };
            (nibble(pair[0]) << 4) | nibble(pair[1])
        })
        .collect()
}

fn reference(kind: DictionaryKind, domain_byte: u8, ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dataset_id: DatasetId::try_from([0x11; 16]).unwrap(),
        dictionary_kind: kind,
        allocation_domain: AllocationDomainId::try_from([domain_byte; 16]).unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    }
}

#[test]
fn manifest_key_has_a_frozen_prefix_and_column_family() {
    let encoded = IngestKey::Manifest.encode().unwrap();

    assert_eq!(encoded.column_family(), ColumnFamily::Default);
    assert_eq!(encoded.domain(), KeyDomain::Manifest);
    assert_eq!(encoded.as_bytes(), &[0x01, 0x01]);
    assert_eq!(
        decode_key(ColumnFamily::Default, encoded.as_bytes()).unwrap(),
        IngestKey::Manifest
    );
}

#[test]
fn every_domain_matches_independent_literal_bytes_and_round_trips() {
    let dataset_id = DatasetId::try_from([0x11; 16]).unwrap();
    let event_id = EventId::from_bytes([0x33; 32]);
    let event_ref = reference(DictionaryKind::Event, 0x22, 0x0102_0304_0506_0708);
    let visibility = reference(DictionaryKind::VisibilityScope, 0x24, 0x1112_1314_1516_1718);
    let endpoint = reference(DictionaryKind::Endpoint, 0x21, 0x2122_2324_2526_2728);
    let observer = reference(DictionaryKind::Observer, 0x22, 0x3132_3334_3536_3738);
    let access = reference(DictionaryKind::AccessScope, 0x23, 0x4142_4344_4546_4748);
    let request_id = ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from([0x55; 16]).unwrap(),
        producer_epoch: ProducerEpoch::new(0x8000_0000_0000_0000).unwrap(),
        request_sequence: RequestSequence::new(u64::MAX).unwrap(),
    };

    let cases = [
        (
            IngestKey::Manifest,
            ColumnFamily::Default,
            KeyDomain::Manifest,
            "0101",
        ),
        (
            IngestKey::SourceStream {
                dataset_id,
                shard_id: 0x89ab_cdef,
                owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
            },
            ColumnFamily::Default,
            KeyDomain::SourceStream,
            concat!(
                "0102",
                "11111111111111111111111111111111",
                "89abcdef",
                "ffffffffffffffff"
            ),
        ),
        (
            IngestKey::AllocationStream {
                dataset_id,
                dictionary_kind: DictionaryKind::DatasetLineage,
                allocation_domain: AllocationDomainId::try_from([0x22; 16]).unwrap(),
            },
            ColumnFamily::Default,
            KeyDomain::AllocationStream,
            concat!(
                "0103",
                "11111111111111111111111111111111",
                "0009",
                "22222222222222222222222222222222"
            ),
        ),
        (
            IngestKey::Dictionary {
                reference: event_ref,
            },
            ColumnFamily::Default,
            KeyDomain::Dictionary,
            concat!(
                "0104",
                "11111111111111111111111111111111",
                "0008",
                "22222222222222222222222222222222",
                "0102030405060708"
            ),
        ),
        (
            IngestKey::DictionaryReverseCandidate {
                dataset_id,
                dictionary_kind: DictionaryKind::Event,
                descriptor_digest: Digest256::from_bytes([0x44; 32]),
                reference: event_ref,
            },
            ColumnFamily::Default,
            KeyDomain::DictionaryReverseCandidate,
            concat!(
                "0105",
                "11111111111111111111111111111111",
                "0008",
                "4444444444444444444444444444444444444444444444444444444444444444",
                "11111111111111111111111111111111",
                "0008",
                "22222222222222222222222222222222",
                "0102030405060708"
            ),
        ),
        (
            IngestKey::EventPayload {
                dataset_id,
                event_id,
            },
            ColumnFamily::Default,
            KeyDomain::EventPayload,
            concat!(
                "0106",
                "11111111111111111111111111111111",
                "3333333333333333333333333333333333333333333333333333333333333333"
            ),
        ),
        (
            IngestKey::EventEligibility {
                dataset_id,
                event_id,
                visibility_scope: visibility,
            },
            ColumnFamily::Default,
            KeyDomain::EventEligibility,
            concat!(
                "0107",
                "11111111111111111111111111111111",
                "3333333333333333333333333333333333333333333333333333333333333333",
                "11111111111111111111111111111111",
                "0004",
                "24242424242424242424242424242424",
                "1112131415161718"
            ),
        ),
        (
            IngestKey::EventReceive {
                dataset_id,
                event_id,
                visibility_scope: visibility,
            },
            ColumnFamily::Default,
            KeyDomain::EventReceive,
            concat!(
                "0108",
                "11111111111111111111111111111111",
                "3333333333333333333333333333333333333333333333333333333333333333",
                "11111111111111111111111111111111",
                "0004",
                "24242424242424242424242424242424",
                "1112131415161718"
            ),
        ),
        (
            IngestKey::Provenance {
                dataset_id,
                event_id,
                endpoint_id: endpoint,
                observer_id: observer,
                access_scope_id: access,
                visibility_scope_id: visibility,
            },
            ColumnFamily::Default,
            KeyDomain::Provenance,
            concat!(
                "0109",
                "11111111111111111111111111111111",
                "3333333333333333333333333333333333333333333333333333333333333333",
                "11111111111111111111111111111111",
                "0001",
                "21212121212121212121212121212121",
                "2122232425262728",
                "11111111111111111111111111111111",
                "0002",
                "22222222222222222222222222222222",
                "3132333435363738",
                "11111111111111111111111111111111",
                "0003",
                "23232323232323232323232323232323",
                "4142434445464748",
                "11111111111111111111111111111111",
                "0004",
                "24242424242424242424242424242424",
                "1112131415161718"
            ),
        ),
        (
            IngestKey::Fact {
                source_record_id: SourceRecordId {
                    dataset_id,
                    shard_id: 0x89ab_cdef,
                    owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
                    sequence: Sequence::new(0xf1f2_f3f4_f5f6_f7f8).unwrap(),
                },
            },
            ColumnFamily::Facts,
            KeyDomain::Fact,
            concat!(
                "0120",
                "11111111111111111111111111111111",
                "89abcdef",
                "ffffffffffffffff",
                "f1f2f3f4f5f6f7f8"
            ),
        ),
        (
            IngestKey::Request { request_id },
            ColumnFamily::Receipts,
            KeyDomain::Request,
            concat!(
                "0130",
                "55555555555555555555555555555555",
                "8000000000000000",
                "ffffffffffffffff"
            ),
        ),
        (
            IngestKey::ProducerState {
                producer_slot_id: request_id.producer_slot_id,
                producer_epoch: ProducerEpoch::new(u64::MAX).unwrap(),
            },
            ColumnFamily::Receipts,
            KeyDomain::ProducerState,
            concat!(
                "0131",
                "55555555555555555555555555555555",
                "ffffffffffffffff"
            ),
        ),
    ];

    for (key, column_family, domain, literal) in cases {
        let expected = literal_bytes(literal);
        let encoded = key.encode().unwrap();
        assert_eq!(encoded.column_family(), column_family, "{domain:?}");
        assert_eq!(encoded.domain(), domain);
        assert_eq!(encoded.as_bytes(), expected, "{domain:?}");
        assert_eq!(decode_key(column_family, &expected).unwrap(), key);
    }
}

#[test]
fn malformed_or_cross_namespace_keys_fail_closed() {
    assert_eq!(
        decode_key(ColumnFamily::Default, &[]),
        Err(KeyError::WrongLength)
    );
    assert_eq!(
        decode_key(ColumnFamily::Default, &[0x01]),
        Err(KeyError::WrongLength)
    );
    assert_eq!(
        decode_key(ColumnFamily::Default, &[0x02, 0x01]),
        Err(KeyError::UnsupportedVersion)
    );
    assert_eq!(
        decode_key(ColumnFamily::Default, &[0x01, 0xff]),
        Err(KeyError::UnknownDomain)
    );
    assert_eq!(
        decode_key(ColumnFamily::Facts, &[0x01, 0x01]),
        Err(KeyError::WrongColumnFamily)
    );
    assert_eq!(
        decode_key(ColumnFamily::Default, &[0x01, 0x01, 0x00]),
        Err(KeyError::WrongLength)
    );

    let mut zero_dataset = vec![0x01, 0x06];
    zero_dataset.extend([0; 16]);
    zero_dataset.extend([0x33; 32]);
    assert_eq!(
        decode_key(ColumnFamily::Default, &zero_dataset),
        Err(KeyError::ZeroIdentity)
    );

    let mut unknown_kind = literal_bytes(concat!(
        "0103",
        "11111111111111111111111111111111",
        "000a",
        "22222222222222222222222222222222"
    ));
    assert_eq!(
        decode_key(ColumnFamily::Default, &unknown_kind),
        Err(KeyError::UnknownDictionaryKind)
    );
    unknown_kind.pop();
    assert_eq!(
        decode_key(ColumnFamily::Default, &unknown_kind),
        Err(KeyError::WrongLength)
    );

    let mut zero_ordinal = literal_bytes(concat!(
        "0104",
        "11111111111111111111111111111111",
        "0008",
        "22222222222222222222222222222222",
        "0000000000000000"
    ));
    assert_eq!(
        decode_key(ColumnFamily::Default, &zero_ordinal),
        Err(KeyError::ZeroCounter)
    );
    zero_ordinal.extend([0xaa]);
    assert_eq!(
        decode_key(ColumnFamily::Default, &zero_ordinal),
        Err(KeyError::WrongLength)
    );

    let dataset_id = DatasetId::try_from([0x11; 16]).unwrap();
    let foreign_visibility = QualifiedRef {
        dataset_id: DatasetId::try_from([0x12; 16]).unwrap(),
        ..reference(DictionaryKind::VisibilityScope, 0x24, 1)
    };
    assert_eq!(
        IngestKey::EventEligibility {
            dataset_id,
            event_id: EventId::from_bytes([0x33; 32]),
            visibility_scope: foreign_visibility,
        }
        .encode(),
        Err(KeyError::ForeignDataset)
    );

    assert_eq!(
        IngestKey::EventReceive {
            dataset_id,
            event_id: EventId::from_bytes([0x33; 32]),
            visibility_scope: reference(DictionaryKind::Endpoint, 0x21, 1),
        }
        .encode(),
        Err(KeyError::WrongDictionaryKind)
    );

    assert_eq!(
        IngestKey::DictionaryReverseCandidate {
            dataset_id,
            dictionary_kind: DictionaryKind::Observer,
            descriptor_digest: Digest256::from_bytes([0x44; 32]),
            reference: reference(DictionaryKind::Event, 0x22, 1),
        }
        .encode(),
        Err(KeyError::WrongDictionaryKind)
    );
}

#[test]
fn fixed_width_big_endian_fields_preserve_unsigned_order() {
    let dataset_id = DatasetId::try_from([0x11; 16]).unwrap();
    let source = |shard_id, epoch, sequence| {
        IngestKey::Fact {
            source_record_id: SourceRecordId {
                dataset_id,
                shard_id,
                owner_epoch: OwnerEpoch::new(epoch).unwrap(),
                sequence: Sequence::new(sequence).unwrap(),
            },
        }
        .encode()
        .unwrap()
    };

    let first = source(1, 1, 1);
    let later_sequence = source(1, 1, u64::MAX);
    let later_epoch = source(1, 2, 1);
    let later_shard = source(u32::MAX, 1, 1);
    assert!(first.as_bytes() < later_sequence.as_bytes());
    assert!(later_sequence.as_bytes() < later_epoch.as_bytes());
    assert!(later_epoch.as_bytes() < later_shard.as_bytes());

    let low_ordinal = IngestKey::Dictionary {
        reference: reference(DictionaryKind::Event, 0x22, 1),
    }
    .encode()
    .unwrap();
    let maximum_ordinal = IngestKey::Dictionary {
        reference: reference(DictionaryKind::Event, 0x22, u64::MAX),
    }
    .encode()
    .unwrap();
    assert!(low_ordinal.as_bytes() < maximum_ordinal.as_bytes());
}

#[test]
fn compact_references_never_collapse_to_a_bare_ordinal() {
    let one = 7;
    let event = IngestKey::Dictionary {
        reference: reference(DictionaryKind::Event, 0x22, one),
    }
    .encode()
    .unwrap();
    let another_domain = IngestKey::Dictionary {
        reference: reference(DictionaryKind::Event, 0x23, one),
    }
    .encode()
    .unwrap();
    let another_kind = IngestKey::Dictionary {
        reference: reference(DictionaryKind::Observer, 0x22, one),
    }
    .encode()
    .unwrap();

    assert_ne!(event.as_bytes(), another_domain.as_bytes());
    assert_ne!(event.as_bytes(), another_kind.as_bytes());
    assert_eq!(event.as_bytes().len(), 2 + 16 + 2 + 16 + 8);
}

#[test]
fn domain_prefixes_are_closed_and_match_complete_keys() {
    let request = IngestKey::Request {
        request_id: ProducerRequestId {
            producer_slot_id: ProducerSlotId::try_from([0x55; 16]).unwrap(),
            producer_epoch: ProducerEpoch::new(1).unwrap(),
            request_sequence: RequestSequence::new(1).unwrap(),
        },
    }
    .encode()
    .unwrap();

    assert_eq!(KeyDomain::Manifest.prefix(), [0x01, 0x01]);
    assert_eq!(KeyDomain::Fact.prefix(), [0x01, 0x20]);
    assert_eq!(KeyDomain::Request.prefix(), [0x01, 0x30]);
    assert!(request.as_bytes().starts_with(&KeyDomain::Request.prefix()));
}

#[test]
fn column_family_names_match_the_persistent_store_contract() {
    assert_eq!(ColumnFamily::Default.name(), "default");
    assert_eq!(ColumnFamily::Facts.name(), "facts");
    assert_eq!(ColumnFamily::Receipts.name(), "receipts");
}

#[test]
fn maximum_key_is_bounded_and_corrupt_embedded_references_are_rejected() {
    let dataset_id = DatasetId::try_from([0x11; 16]).unwrap();
    let event_id = EventId::from_bytes([0x33; 32]);
    let maximum = IngestKey::Provenance {
        dataset_id,
        event_id,
        endpoint_id: reference(DictionaryKind::Endpoint, 0x21, u64::MAX),
        observer_id: reference(DictionaryKind::Observer, 0x22, u64::MAX),
        access_scope_id: reference(DictionaryKind::AccessScope, 0x23, u64::MAX),
        visibility_scope_id: reference(DictionaryKind::VisibilityScope, 0x24, u64::MAX),
    }
    .encode()
    .unwrap();
    assert_eq!(maximum.as_bytes().len(), MAX_INGEST_KEY_BYTES);

    let mut foreign_scope = IngestKey::EventEligibility {
        dataset_id,
        event_id,
        visibility_scope: reference(DictionaryKind::VisibilityScope, 0x24, 1),
    }
    .encode()
    .unwrap()
    .as_bytes()
    .to_vec();
    foreign_scope[50] = 0x12;
    assert_eq!(
        decode_key(ColumnFamily::Default, &foreign_scope),
        Err(KeyError::ForeignDataset)
    );

    let mut wrong_scope_kind = foreign_scope;
    wrong_scope_kind[50] = 0x11;
    wrong_scope_kind[66..68].copy_from_slice(&1u16.to_be_bytes());
    assert_eq!(
        decode_key(ColumnFamily::Default, &wrong_scope_kind),
        Err(KeyError::WrongDictionaryKind)
    );

    let mut zero_source_counter = IngestKey::Fact {
        source_record_id: SourceRecordId {
            dataset_id,
            shard_id: 1,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
    }
    .encode()
    .unwrap()
    .as_bytes()
    .to_vec();
    zero_source_counter[22..30].fill(0);
    assert_eq!(
        decode_key(ColumnFamily::Facts, &zero_source_counter),
        Err(KeyError::ZeroCounter)
    );
}
