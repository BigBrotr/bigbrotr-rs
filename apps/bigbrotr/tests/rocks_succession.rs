use bigbrotr::storage::rocks::{
    keys::{ColumnFamily, decode_key},
    manifest::{AllocationNamespaces, IngestManifest},
    succession::{
        LINEAGE_ENTRY_BYTES, LINEAGE_SCRATCH_BYTES, MAX_LINEAGE_EPOCHS, MAX_LINEAGE_METADATA_BYTES,
        MAX_SUCCESSION_BYTES, MAX_TOPOLOGY_MEMBERSHIPS, MIN_SUCCESSION_BYTES,
        PredecessorTerminalKind, SuccessionError, SuccessionFields, SuccessionProfile,
        SuccessionProfileError, SuccessionRecord,
    },
};
use bigbrotr_model::{DictionaryKind, OwnerEpoch, ProducerEpoch, ProducerSlotId};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../experiments/ingest-succession/portable-handoff-v2.json"
    ))
    .unwrap()
}

fn bytes(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2));
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn hex(value: &Value) -> Vec<u8> {
    bytes(value.as_str().unwrap())
}

fn number(value: &Value) -> u64 {
    value.as_str().unwrap().parse().unwrap()
}

fn fields(value: &Value) -> SuccessionFields {
    SuccessionFields {
        terminal_kind: match value["terminal_kind"].as_u64().unwrap() {
            1 => PredecessorTerminalKind::Activated,
            2 => PredecessorTerminalKind::UnactivatedEmpty,
            _ => panic!("fixture terminal kind"),
        },
        producer_slot_id: ProducerSlotId::try_from(
            <[u8; 16]>::try_from(hex(&value["producer_slot_hex"])).unwrap(),
        )
        .unwrap(),
        predecessor_epoch: ProducerEpoch::new(number(&value["predecessor_epoch"])).unwrap(),
        issued_through: number(&value["issued_through"]),
        last_complete_sequence: number(&value["last_complete_sequence"]),
        closure_digest: hex(&value["closure_digest_hex"]).try_into().unwrap(),
        incoming_handoff_sha256: hex(&value["incoming_handoff_sha256"]).try_into().unwrap(),
        previous_manifest: IngestManifest::decode(&hex(&value["previous_manifest_hex"])).unwrap(),
        next_manifest: IngestManifest::decode(&hex(&value["next_manifest_hex"])).unwrap(),
    }
}

#[test]
fn independent_literal_records_preserve_unsigned_fields_manifests_and_keys() {
    let fixture = fixture();
    assert_eq!(fixture["format"], "bigbrotr-repeatable-handoff-fixtures/1");
    let cases = fixture["valid"].as_array().unwrap();
    assert_eq!(cases.len(), 11);
    for case in cases {
        let id = case["id"].as_str().unwrap();
        let expected = hex(&case["value_hex"]);
        let expected_fields = fields(&case["fields"]);
        let record = SuccessionRecord::new(expected_fields).unwrap();
        assert_eq!(record.encode().as_bytes(), expected, "{id}");
        assert_eq!(SuccessionRecord::decode(&expected).unwrap(), record, "{id}");
        assert_eq!(*record.fields(), expected_fields, "{id}");
        assert_eq!(
            record.key().encode().unwrap().as_bytes(),
            hex(&case["key_hex"])
        );
        assert_eq!(
            decode_key(ColumnFamily::Default, &hex(&case["key_hex"])).unwrap(),
            record.key()
        );
        assert_eq!(
            &Sha256::digest(&expected)[..],
            hex(&case["value_sha256"]),
            "{id}"
        );
        assert_eq!(
            SuccessionProfile::for_preparation(expected_fields.predecessor_epoch.get()).is_ok(),
            case["profile_epoch_admitted"].as_bool().unwrap(),
            "portable validity is separate from resource admission: {id}"
        );
    }
    assert_eq!(MIN_SUCCESSION_BYTES, 319);
    assert_eq!(MAX_SUCCESSION_BYTES, 643);
}

#[test]
fn independent_malformed_values_and_every_proper_prefix_refuse() {
    let fixture = fixture();
    let invalid = fixture["invalid"].as_array().unwrap();
    assert_eq!(invalid.len(), 38);
    for case in invalid {
        assert!(
            SuccessionRecord::decode(&hex(&case["value_hex"])).is_err(),
            "{}: {}",
            case["id"],
            case["reason"]
        );
    }
    let mut truncated = 0;
    for case in fixture["valid"].as_array().unwrap() {
        let value = hex(&case["value_hex"]);
        for end in 0..value.len() {
            assert!(
                SuccessionRecord::decode(&value[..end]).is_err(),
                "{} prefix {end}",
                case["id"]
            );
            truncated += 1;
        }
        let mut extended = value;
        extended.push(0);
        assert_eq!(
            SuccessionRecord::decode(&extended),
            Err(SuccessionError::WrongLength)
        );
    }
    assert_eq!(truncated, 6533);
}

#[test]
fn construction_validates_counts_transition_and_namespaces_without_claiming_hash_proof() {
    let fixture = fixture();
    let original = fields(&fixture["valid"][0]["fields"]);
    // A no-fact outcome still advances the request count. It must not be
    // confused with an unactivated empty attempt or require a positive cut.
    let no_facts = SuccessionRecord::new(SuccessionFields {
        issued_through: 1,
        last_complete_sequence: 0,
        ..original
    })
    .unwrap();
    assert_eq!(
        no_facts.fields().terminal_kind,
        PredecessorTerminalKind::Activated
    );
    assert_eq!(
        &no_facts.encode().as_bytes()[27..43],
        &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        SuccessionRecord::decode(no_facts.encode().as_bytes()).unwrap(),
        no_facts
    );
    for (issued, cut, kind) in [
        (65, 5, PredecessorTerminalKind::Activated),
        (0, 1, PredecessorTerminalKind::Activated),
        (1, 0, PredecessorTerminalKind::UnactivatedEmpty),
        (0, 1, PredecessorTerminalKind::UnactivatedEmpty),
    ] {
        assert_eq!(
            SuccessionRecord::new(SuccessionFields {
                issued_through: issued,
                last_complete_sequence: cut,
                terminal_kind: kind,
                ..original
            }),
            Err(SuccessionError::InvalidCounts)
        );
    }
    assert_eq!(
        SuccessionRecord::new(SuccessionFields {
            predecessor_epoch: ProducerEpoch::new(1).unwrap(),
            ..original
        }),
        Err(SuccessionError::InvalidEpoch)
    );
    let next = original.next_manifest;
    let mut wrong_identity = *next.identity();
    wrong_identity.owner_epoch = OwnerEpoch::new(4).unwrap();
    assert_eq!(
        SuccessionRecord::new(SuccessionFields {
            next_manifest: IngestManifest::for_repeatable_successor(
                wrong_identity,
                *next.allocation_namespaces(),
            ),
            ..original
        }),
        Err(SuccessionError::InvalidTransition)
    );
    let repeated = next
        .allocation_namespaces()
        .domain(DictionaryKind::VisibilityScope)
        .unwrap();
    let entries: Vec<_> = (1..=9)
        .map(|kind| (DictionaryKind::try_from(kind).unwrap(), repeated))
        .collect();
    assert_eq!(
        SuccessionRecord::new(SuccessionFields {
            next_manifest: IngestManifest::for_repeatable_successor(
                *next.identity(),
                AllocationNamespaces::from_entries(&entries).unwrap(),
            ),
            ..original
        }),
        Err(SuccessionError::InvalidNamespaces)
    );
    // Zero checksum bytes are structurally 32 bytes. Positive lineage/effects and
    // digest equality are deliberately outside this portable codec's authority.
    let unproved = SuccessionRecord::new(SuccessionFields {
        incoming_handoff_sha256: [0; 32],
        closure_digest: [0; 32],
        ..original
    })
    .unwrap();
    assert_eq!(
        SuccessionRecord::decode(unproved.encode().as_bytes()).unwrap(),
        unproved
    );
}

#[test]
fn profile_counts_pending_target_and_preserves_last_epoch_recovery_capacity() {
    let preparation = SuccessionProfile::for_preparation(254).unwrap();
    assert_eq!(
        preparation,
        SuccessionProfile::for_recovery(254, Some(255)).unwrap()
    );
    assert_eq!(
        preparation,
        SuccessionProfile::for_recovery(255, None).unwrap()
    );
    assert_eq!(preparation.registered_epochs(), 255);
    assert_eq!(preparation.topology_memberships(), 32640);
    assert_eq!(preparation.required_metadata_bytes(), 261632);
    assert_eq!(preparation.maximum_topology_generations(), 256);
    assert_eq!(MAX_LINEAGE_EPOCHS, 255);
    assert_eq!(MAX_TOPOLOGY_MEMBERSHIPS, 32768);
    assert_eq!(MAX_LINEAGE_METADATA_BYTES, 262144);
    assert_eq!(LINEAGE_SCRATCH_BYTES, 131072);
    assert_eq!(LINEAGE_ENTRY_BYTES, 512);
    // Epoch256 fits the byte formula exactly but exceeds the independent row cap.
    for result in [
        SuccessionProfile::for_preparation(255),
        SuccessionProfile::for_recovery(255, Some(256)),
        SuccessionProfile::for_recovery(256, None),
    ] {
        assert_eq!(result, Err(SuccessionProfileError::TopologyLimit));
    }
    let fixture = fixture();
    let profile = &fixture["profile"];
    for case in profile["cases"].as_array().unwrap() {
        let result = SuccessionProfile::for_recovery(case["total_epochs"].as_u64().unwrap(), None);
        assert_eq!(result.is_ok(), case["admitted"].as_bool().unwrap());
        if let Ok(profile) = result {
            assert_eq!(
                profile.topology_memberships(),
                case["topology_memberships"].as_u64().unwrap()
            );
            assert_eq!(
                profile.required_metadata_bytes(),
                case["metadata_formula_bytes"].as_u64().unwrap()
            );
        }
    }
}

#[test]
fn profile_refuses_nonadjacent_gaps_and_unsigned_overflow_without_narrowing() {
    for epoch in [0, 1] {
        assert_eq!(
            SuccessionProfile::for_recovery(epoch, None),
            Err(SuccessionProfileError::InvalidEpoch)
        );
        assert_eq!(
            SuccessionProfile::for_preparation(epoch),
            Err(SuccessionProfileError::InvalidEpoch)
        );
    }
    for target in [0, 1, 2, 4, u64::MAX] {
        assert_eq!(
            SuccessionProfile::for_recovery(2, Some(target)),
            Err(SuccessionProfileError::InvalidPendingTarget)
        );
    }
    for result in [
        SuccessionProfile::for_preparation(u64::MAX),
        SuccessionProfile::for_recovery(u64::MAX, None),
        SuccessionProfile::for_recovery(u64::MAX - 1, Some(u64::MAX)),
        SuccessionProfile::for_recovery(1 << 63, None),
    ] {
        assert_eq!(result, Err(SuccessionProfileError::ArithmeticOverflow));
    }
    // Independent successive sums cover every admitted profile, including empty
    // lifetimes: input-file counts are intentionally absent from this arithmetic.
    let mut members = 1;
    let mut metadata = 131584;
    for epoch in 2..=255 {
        members += epoch;
        metadata += 512;
        let profile = SuccessionProfile::for_recovery(epoch, None).unwrap();
        assert_eq!(profile.topology_memberships(), members);
        assert_eq!(profile.required_metadata_bytes(), metadata);
    }
}
