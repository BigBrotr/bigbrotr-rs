use bigbrotr_model::{
    AllocationDomainId, DatasetId, Digest256, DurationUs, ObservedTime, Ordinal, OwnerEpoch,
    ProducerSlotId, ProjectionGenerationId, Sequence, TopologyGeneration,
};

#[test]
fn opaque_ids_reject_zero_and_keep_exact_byte_order() {
    let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let id = DatasetId::try_from(bytes).unwrap();
    assert_eq!(id.to_string(), "0102030405060708090a0b0c0d0e0f10");
    assert_eq!(id.as_bytes(), &bytes);
    assert_eq!(id.to_string().parse::<DatasetId>().unwrap(), id);
    assert!(DatasetId::try_from([0; 16]).is_err());
    assert!(AllocationDomainId::try_from([0; 16]).is_err());
    assert!(ProducerSlotId::try_from([0; 16]).is_err());
    assert!(ProjectionGenerationId::try_from([0; 16]).is_err());
    for text in [
        "00".repeat(16),
        "AA".repeat(16),
        "1".repeat(31),
        "1".repeat(33),
    ] {
        assert!(text.parse::<DatasetId>().is_err());
    }
    assert_eq!(Digest256::from_bytes([0; 32]).as_bytes(), &[0; 32]);
}

#[test]
fn allocation_and_sequence_exhaustion_never_wraps() {
    assert!(Ordinal::new(0).is_err());
    assert!(OwnerEpoch::new(0).is_err());
    assert!(Sequence::new(0).is_err());
    assert!(Ordinal::new(u64::MAX).unwrap().checked_add(1).is_err());
    assert!(Sequence::new(u64::MAX).unwrap().checked_add(1).is_err());
    assert_eq!(Sequence::new(1).unwrap().checked_add(2).unwrap().get(), 3);
    assert!(TopologyGeneration::new(u64::MAX).checked_add(1).is_err());
    assert_eq!(TopologyGeneration::new(0).checked_add(1).unwrap().get(), 1);
}

#[test]
fn clock_uncertainty_is_required_nullable_and_reserved_flags_are_refused() {
    use bigbrotr_model::{ClockQuality, DictionaryKind, QualifiedRef};
    let clock = QualifiedRef {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        dictionary_kind: DictionaryKind::Observer,
        allocation_domain: AllocationDomainId::try_from([2; 16]).unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    let quality = ClockQuality::new(clock, DurationUs::new(1), None, 7).unwrap();
    let json = serde_json::to_string(&quality).unwrap();
    assert!(json.contains("\"uncertainty_us\":null"));
    assert_eq!(
        serde_json::from_str::<ClockQuality>(&json).unwrap(),
        quality
    );
    assert!(
        serde_json::from_str::<ClockQuality>(&json.replace("\"uncertainty_us\":null,", ""))
            .is_err()
    );
    assert!(
        serde_json::from_str::<ClockQuality>(
            &json.replace("\"anomaly_flags\":7", "\"anomaly_flags\":8")
        )
        .is_err()
    );
    assert!(
        ClockQuality::new(
            QualifiedRef {
                dictionary_kind: DictionaryKind::Method,
                ..clock
            },
            DurationUs::new(1),
            None,
            0
        )
        .is_err()
    );
    let exact = ClockQuality::new(clock, DurationUs::new(1), Some(DurationUs::new(0)), 0).unwrap();
    assert_eq!(exact.uncertainty_us(), Some(DurationUs::new(0)));
    assert_ne!(exact.uncertainty_us(), quality.uncertainty_us());
}

#[test]
fn metadata_numbers_are_canonical_decimal_strings_with_full_domains() {
    assert_eq!(
        serde_json::to_string(&Ordinal::new(u64::MAX).unwrap()).unwrap(),
        "\"18446744073709551615\""
    );
    assert_eq!(
        serde_json::to_string(&ObservedTime::new(i64::MIN)).unwrap(),
        "\"-9223372036854775808\""
    );
    assert_eq!(serde_json::to_string(&DurationUs::new(0)).unwrap(), "\"0\"");
    assert_eq!(
        serde_json::from_str::<ObservedTime>("\"0\"").unwrap().get(),
        0
    );
    assert_eq!(
        serde_json::from_str::<DurationUs>("\"18446744073709551615\"")
            .unwrap()
            .get(),
        u64::MAX
    );
    for invalid in [
        "0",
        "1",
        "null",
        "\"\"",
        "\"-0\"",
        "\"00\"",
        "\"01\"",
        "\"+1\"",
        "\" 1\"",
        "\"1e0\"",
        "\"18446744073709551616\"",
    ] {
        assert!(
            serde_json::from_str::<DurationUs>(invalid).is_err(),
            "{invalid}"
        );
        assert!(
            serde_json::from_str::<Ordinal>(invalid).is_err(),
            "{invalid}"
        );
    }
    assert!(serde_json::from_str::<Ordinal>("\"0\"").is_err());
    assert!(serde_json::from_str::<DatasetId>(&format!("\"{}\"", "00".repeat(16))).is_err());
}
