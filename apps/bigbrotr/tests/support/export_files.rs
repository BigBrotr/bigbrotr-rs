//! Shared synthetic inputs for export-file and isolated umask integration tests.
use bigbrotr::export::{destination::LocalDestination, manifest::SignedEventsManifest};
use bigbrotr_model::{
    CompleteThrough, DictionaryKind, Ordinal, OwnerEpoch, QualifiedRef, SourceCut, StreamBound,
    StreamId, TopologyGeneration,
};
use bigbrotr_protocol::{EventLimits, VerifiedEvent, verify_json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

pub const ID: &str = "10101010101010101010101010101010";

pub fn input() -> VerifiedEvent {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    verify_json(
        &serde_json::to_vec(&fixtures["events"]["timestamp-zero"]["event"]).unwrap(),
        EventLimits::default(),
    )
    .unwrap()
}
pub fn cut() -> SourceCut {
    SourceCut::new(
        [1; 16].try_into().unwrap(),
        TopologyGeneration::new(4),
        (0..2)
            .map(|i| StreamBound {
                stream: StreamId {
                    shard_id: i,
                    owner_epoch: OwnerEpoch::new(i as u64 + 1).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(if i == 0 { 0 } else { 10 }),
            })
            .collect(),
    )
    .unwrap()
}
pub fn manifest<'a>(cut: &'a SourceCut, event: &VerifiedEvent) -> SignedEventsManifest<'a> {
    SignedEventsManifest {
        artifact_id: ID.parse().unwrap(),
        job_id: [0x20; 16].try_into().unwrap(),
        source_cut: cut,
        catalog_id: [0x30; 16].try_into().unwrap(),
        frontier_revision: 5,
        snapshot_id: 11,
        event_id: event.event().id,
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
pub fn private_dir(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
pub fn destination(temp: &Path) -> (PathBuf, Arc<LocalDestination>) {
    let root = temp.canonicalize().unwrap().join("exports");
    private_dir(&root);
    let value = LocalDestination::initialize(
        &root,
        [1; 16].try_into().unwrap(),
        [0x40; 16].try_into().unwrap(),
    )
    .unwrap();
    drop(value);
    let value = Arc::new(
        LocalDestination::open(
            &root,
            [1; 16].try_into().unwrap(),
            [0x40; 16].try_into().unwrap(),
        )
        .unwrap(),
    );
    (root, value)
}
