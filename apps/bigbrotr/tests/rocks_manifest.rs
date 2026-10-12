use bigbrotr::storage::rocks::manifest::{
    AllocationNamespaces, IngestManifest, ManifestError, ManifestIdentity,
};
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, OwnerEpoch, OwnerResourceId, PolicyEpoch,
    ProcessIncarnation, RecoveryGenerationId, TopologyGeneration,
};

fn expected_bytes() -> Vec<u8> {
    // Independently written fixed-width format, including two allocation kinds.
    let hex = concat!(
        "01010001000100010001",
        "00112233445566778899aabbccddeeff",
        "ffffffffffffffff",
        "102132435465768798a9bacbdcedfe0f",
        "89abcdef",
        "8000000000000000",
        "112233445566778899aabbccddeeff00",
        "00000000000000000000000000000000",
        "0000000000000000",
        "0102",
        "0004",
        "123456789abcdef00123456789abcdef",
        "0008",
        "fedcba9876543210fedcba9876543210"
    );
    assert_eq!(hex.len(), 280);
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn manifest() -> IngestManifest {
    let identity = ManifestIdentity {
        dataset_id: "00112233445566778899aabbccddeeff"
            .parse::<DatasetId>()
            .unwrap(),
        topology_generation: TopologyGeneration::new(u64::MAX),
        owner_resource_id: "102132435465768798a9bacbdcedfe0f"
            .parse::<OwnerResourceId>()
            .unwrap(),
        shard_id: 0x89abcdef,
        owner_epoch: OwnerEpoch::new(1 << 63).unwrap(),
        process_incarnation: "112233445566778899aabbccddeeff00"
            .parse::<ProcessIncarnation>()
            .unwrap(),
        recovery_generation: RecoveryGenerationId::from_bytes([0; 16]),
        policy_epoch: PolicyEpoch::new(0),
    };
    let namespaces = AllocationNamespaces::from_entries(&[
        (
            DictionaryKind::VisibilityScope,
            "123456789abcdef00123456789abcdef"
                .parse::<AllocationDomainId>()
                .unwrap(),
        ),
        (
            DictionaryKind::Event,
            "fedcba9876543210fedcba9876543210"
                .parse::<AllocationDomainId>()
                .unwrap(),
        ),
    ])
    .unwrap();
    IngestManifest::new(identity, namespaces)
}

#[test]
fn literal_manifest_preserves_full_identity_and_declared_zero_coordinates() {
    let value = manifest();
    let bytes = expected_bytes();
    assert_eq!(value.encode().as_bytes(), bytes);
    assert_eq!(IngestManifest::decode(&bytes).unwrap(), value);
    assert_eq!(value.identity().owner_epoch.get(), 1 << 63);
    assert_eq!(
        value
            .allocation_namespaces()
            .domain(DictionaryKind::Observer),
        None
    );
    assert!(
        value
            .allocation_namespaces()
            .domain(DictionaryKind::Event)
            .is_some()
    );
}

#[test]
fn manifest_refuses_partial_unknown_and_noncanonical_inputs() {
    let bytes = expected_bytes();
    for length in 0..bytes.len() {
        assert!(IngestManifest::decode(&bytes[..length]).is_err());
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert_eq!(
        IngestManifest::decode(&extra),
        Err(ManifestError::WrongLength)
    );
    for position in [0, 1, 3, 5, 7, 9] {
        let mut changed = bytes.clone();
        changed[position] = if position == 3 { 3 } else { 2 };
        assert_eq!(
            IngestManifest::decode(&changed),
            Err(ManifestError::UnsupportedFormat)
        );
    }
    for range in [10..26, 34..50, 62..78, 106..122] {
        let mut changed = bytes.clone();
        changed[range].fill(0);
        assert_eq!(
            IngestManifest::decode(&changed),
            Err(ManifestError::InvalidIdentity)
        );
    }
    let mut changed = bytes.clone();
    changed[54..62].fill(0);
    assert_eq!(
        IngestManifest::decode(&changed),
        Err(ManifestError::InvalidCounter)
    );
    let mut changed = bytes.clone();
    changed[102] = 0;
    assert_eq!(
        IngestManifest::decode(&changed),
        Err(ManifestError::UnsupportedProtection)
    );
    let mut changed = bytes.clone();
    changed[123] = 4;
    assert_eq!(
        IngestManifest::decode(&changed),
        Err(ManifestError::UnorderedNamespaces)
    );
    let mut changed = bytes.clone();
    changed[105] = 10;
    assert_eq!(
        IngestManifest::decode(&changed),
        Err(ManifestError::UnknownDictionaryKind)
    );
    let mut changed = bytes;
    changed[103] = 10;
    assert!(IngestManifest::decode(&changed).is_err());
}

#[test]
fn successor_profile_changes_only_the_key_schema_coordinate() {
    let old = manifest();
    let successor = IngestManifest::for_successor(*old.identity(), *old.allocation_namespaces());
    let mut expected = expected_bytes();
    expected[2..4].copy_from_slice(&[0, 2]);
    assert_eq!(successor.encode().as_bytes(), expected);
    assert_eq!(IngestManifest::decode(&expected).unwrap(), successor);
    assert_eq!(old.key_schema(), 1);
    assert_eq!(successor.key_schema(), 2);
    assert_ne!(successor, old);
    for end in 0..expected.len() {
        assert!(IngestManifest::decode(&expected[..end]).is_err());
    }
    expected.push(0);
    assert_eq!(
        IngestManifest::decode(&expected),
        Err(ManifestError::WrongLength)
    );
}

#[test]
fn namespaces_preserve_empty_full_and_reject_ambiguous_order() {
    let identity = *manifest().identity();
    let empty = IngestManifest::new(identity, AllocationNamespaces::from_entries(&[]).unwrap());
    assert_eq!(empty.encode().as_bytes().len(), 104);
    assert_eq!(empty.encode().as_bytes()[103], 0);
    assert_eq!(
        IngestManifest::decode(empty.encode().as_bytes()).unwrap(),
        empty
    );
    let entries: Vec<_> = (1..=9)
        .map(|kind| {
            (
                DictionaryKind::try_from(kind).unwrap(),
                AllocationDomainId::try_from([kind as u8; 16]).unwrap(),
            )
        })
        .collect();
    let full = IngestManifest::new(
        identity,
        AllocationNamespaces::from_entries(&entries).unwrap(),
    );
    let encoded = full.encode();
    assert_eq!(encoded.as_bytes().len(), 266);
    assert_eq!(encoded.as_bytes()[103], 9);
    // Independently enumerate every entry's literal two-byte kind and identity.
    for (index, entry) in encoded.as_bytes()[104..]
        .as_chunks::<18>()
        .0
        .iter()
        .enumerate()
    {
        assert_eq!(&entry[..2], &[0, index as u8 + 1]);
        assert_eq!(&entry[2..], &[index as u8 + 1; 16]);
    }
    assert_eq!(IngestManifest::decode(encoded.as_bytes()).unwrap(), full);
    assert_eq!(
        AllocationNamespaces::from_entries(&[entries[1], entries[0]]),
        Err(ManifestError::UnorderedNamespaces)
    );
    assert_eq!(
        AllocationNamespaces::from_entries(&[entries[0], entries[0]]),
        Err(ManifestError::UnorderedNamespaces)
    );
    assert_eq!(
        AllocationNamespaces::from_entries(&[entries[0]; 10]),
        Err(ManifestError::WrongLength)
    );
    assert!(OwnerResourceId::try_from([0; 16]).is_err());
    assert!(
        "00000000000000000000000000000000"
            .parse::<OwnerResourceId>()
            .is_err()
    );
}
