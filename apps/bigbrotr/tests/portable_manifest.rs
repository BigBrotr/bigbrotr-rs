//! The manifest oracle is authored by a separate Python serializer and SHA-256.
use bigbrotr::export::{
    jsonl::write_exact_event,
    manifest::{MAX_MANIFEST_BYTES, ManifestError, SignedEventsManifest},
};
use bigbrotr_model::{
    ArtifactId, CompleteThrough, DatasetId, DictionaryKind, Ordinal, OwnerEpoch, QualifiedRef,
    SourceCut, StreamBound, StreamId, TopologyGeneration,
};
use std::sync::atomic::AtomicBool;

fn cut(count: usize) -> SourceCut {
    SourceCut::new(
        DatasetId::try_from([1; 16]).unwrap(),
        TopologyGeneration::new(4),
        (0..count)
            .map(|i| StreamBound {
                stream: StreamId {
                    shard_id: i as u32,
                    owner_epoch: OwnerEpoch::new(i as u64 + 1).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(if i == 0 { 0 } else { 10 }),
            })
            .collect(),
    )
    .unwrap()
}
fn manifest(cut: &SourceCut) -> SignedEventsManifest<'_> {
    SignedEventsManifest {
        artifact_id: ArtifactId::try_from([0x10; 16]).unwrap(),
        job_id: [0x20; 16].try_into().unwrap(),
        source_cut: cut,
        catalog_id: [0x30; 16].try_into().unwrap(),
        frontier_revision: 5,
        snapshot_id: 11,
        event_id: "e258cc73c1ed89e28f28705e91a79dcb0c62f0ee4a61756730525c03dd68283c"
            .parse()
            .unwrap(),
        scope: QualifiedRef {
            dataset_id: cut.dataset_id(),
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: [2; 16].try_into().unwrap(),
            ordinal: Ordinal::new(u64::MAX).unwrap(),
        },
        accepted_config_revision: 7,
        accepted_policy_revision: 9,
    }
}
fn file() -> bigbrotr::export::jsonl::JsonlFile {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    let event = bigbrotr_protocol::verify_json(
        &serde_json::to_vec(&fixture["events"]["timestamp-zero"]["event"]).unwrap(),
        bigbrotr_protocol::EventLimits::default(),
    )
    .unwrap();
    write_exact_event(
        std::io::sink(),
        event.event().id,
        Some(&event),
        1024,
        &AtomicBool::new(false),
    )
    .unwrap()
}
#[test]
fn manifest_matches_independent_bytes_with_zero_prefix_and_exclusions() {
    let cut = cut(2);
    let manifest = manifest(&cut);
    let expected = include_bytes!("../../../tests/fixtures/exports/signed-jsonl-manifest.json");
    assert_eq!(manifest.encode(file(), expected.len()).unwrap(), expected);
    assert_eq!(
        manifest.encode(file(), expected.len() - 1),
        Err(ManifestError::ResourceLimit)
    );
}
#[test]
fn incomplete_or_foreign_metadata_cannot_describe_a_complete_export() {
    let cut = cut(2);
    for field in 0..5 {
        let mut m = manifest(&cut);
        match field {
            0 => m.frontier_revision = 0,
            1 => m.snapshot_id = 0,
            2 => m.accepted_config_revision = 0,
            3 => m.accepted_policy_revision = 0,
            _ => m.scope.dataset_id = [9; 16].try_into().unwrap(),
        }
        assert_eq!(
            m.encode(file(), MAX_MANIFEST_BYTES),
            Err(ManifestError::InvalidInput)
        );
    }
    for field in 0..3 {
        let mut file = file();
        match field {
            0 => file.rows = 2,
            1 => file.rows = 0,
            _ => file.bytes = 0,
        }
        assert_eq!(
            manifest(&cut).encode(file, MAX_MANIFEST_BYTES),
            Err(ManifestError::InvalidInput)
        );
    }
    let mut m = manifest(&cut);
    m.scope.dictionary_kind = DictionaryKind::AccessScope;
    assert_eq!(
        m.encode(file(), MAX_MANIFEST_BYTES),
        Err(ManifestError::InvalidInput)
    );
    assert!(ArtifactId::try_from([0; 16]).is_err());
}
#[test]
fn manifest_stream_count_and_output_are_bounded_independently() {
    let maximum = SourceCut::new(
        [1; 16].try_into().unwrap(),
        TopologyGeneration::new(u64::MAX),
        (0..1024)
            .map(|i| StreamBound {
                stream: StreamId {
                    shard_id: u32::MAX - 1023 + i,
                    owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(u64::MAX),
            })
            .collect(),
    )
    .unwrap();
    let encoded = manifest(&maximum)
        .encode(file(), MAX_MANIFEST_BYTES)
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        value["source"]["cut"]["bounds"].as_array().unwrap().len(),
        1024
    );
    assert_eq!(
        value["source"]["cut"]["bounds"][1023]["last_complete_sequence"],
        "18446744073709551615"
    );
    let excessive = cut(1025);
    assert_eq!(
        manifest(&excessive).encode(file(), usize::MAX),
        Err(ManifestError::ResourceLimit)
    );
    assert_eq!(
        manifest(&maximum).encode(file(), 0),
        Err(ManifestError::ResourceLimit)
    );
}

#[test]
fn native_snapshot_domain_is_distinct_from_unsigned_source_counters() {
    let cut = cut(2);
    let mut m = manifest(&cut);
    m.snapshot_id = i64::MAX as u64;
    m.frontier_revision = u64::MAX;
    assert!(m.encode(file(), MAX_MANIFEST_BYTES).is_ok());
    m.snapshot_id += 1;
    assert_eq!(
        m.encode(file(), MAX_MANIFEST_BYTES),
        Err(ManifestError::InvalidInput)
    );
}

#[test]
fn empty_selection_requires_the_actual_empty_file_digest() {
    let cut = cut(2);
    let m = manifest(&cut);
    let mut file = write_exact_event(
        std::io::sink(),
        m.event_id,
        None,
        0,
        &AtomicBool::new(false),
    )
    .unwrap();
    let bytes = m.encode(file, MAX_MANIFEST_BYTES).unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed["files"][0]["rows"], "0");
    assert_eq!(parsed["files"][0]["bytes"], "0");
    assert_eq!(
        parsed["files"][0]["sha256"],
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    file.sha256 = bigbrotr_model::Digest256::from_bytes([0; 32]);
    assert_eq!(
        m.encode(file, MAX_MANIFEST_BYTES),
        Err(ManifestError::InvalidInput)
    );
}
