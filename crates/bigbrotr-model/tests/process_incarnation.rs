use bigbrotr_model::ProcessIncarnation;

#[test]
fn process_identity_is_distinct_nonzero_portable_data_not_a_lease() {
    assert!(ProcessIncarnation::try_from([0; 16]).is_err());
    let identity = ProcessIncarnation::try_from([0xab; 16]).unwrap();
    assert_eq!(identity.as_bytes(), &[0xab; 16]);
    let encoded = serde_json::to_string(&identity).unwrap();
    assert_eq!(encoded, format!("\"{}\"", "ab".repeat(16)));
    assert_eq!(
        serde_json::from_str::<ProcessIncarnation>(&encoded).unwrap(),
        identity
    );
    assert!(serde_json::from_str::<ProcessIncarnation>(&encoded.to_uppercase()).is_err());
    assert!(
        serde_json::from_str::<ProcessIncarnation>(&format!("\"{}\"", "ab".repeat(15))).is_err()
    );
}
