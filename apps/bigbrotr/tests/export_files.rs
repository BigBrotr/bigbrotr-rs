//! Real private filesystem and independently authored signed/manifest bytes.
//! These tests do not supply SQL authority or qualify crash/power-loss recovery.
//! Keep process-spawning cases in their own integration target: a child may
//! inherit a sibling test's directory or unit flock until exec on Linux.
#[path = "support/export_files.rs"]
mod fixture;
use bigbrotr::export::files::{ExportFileError, ExportUnit};
use bigbrotr_protocol::EventLimits;
use fixture::{ID, cut, destination, input, manifest, private_dir};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::Path,
    sync::atomic::AtomicBool,
};

const MANIFEST: &[u8] =
    include_bytes!("../../../tests/fixtures/exports/signed-jsonl-manifest.json");

fn private_file(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
#[test]
fn publishes_complete_files_and_rebuilds_same_nonterminal_unit_on_fresh_inodes() {
    let temp = tempfile::tempdir().unwrap();
    let (root, destination) = destination(temp.path());
    let mut unit = ExportUnit::acquire(destination, ID.parse().unwrap()).unwrap();
    let event = input();
    let cut = cut();
    let m = manifest(&cut, &event);
    let cancel = AtomicBool::new(false);
    let first = unit.write(&m, Some(&event), 4096, 8192, &cancel).unwrap();
    let final_dir = root.join(ID);
    let data = fs::read(final_dir.join("events.jsonl")).unwrap();
    assert_eq!(data.len(), 334);
    assert_eq!(
        bigbrotr_protocol::verify_json(&data, EventLimits::default())
            .unwrap()
            .event(),
        event.event()
    );
    assert_eq!(fs::read(final_dir.join("manifest.json")).unwrap(), MANIFEST);
    assert_eq!(first.data.bytes, 334);
    assert_eq!(first.manifest_bytes, MANIFEST.len() as u64);
    assert_eq!(
        first.data.sha256.to_string(),
        "2006c3bc3473e92f0bcf8e5b8905bc0784ce18e13b00b31ab095bfb469f0f06f"
    );
    let old = fs::metadata(final_dir.join("events.jsonl")).unwrap().ino();
    let second = unit.write(&m, Some(&event), 4096, 8192, &cancel).unwrap();
    assert_eq!(first, second);
    assert_ne!(
        fs::metadata(final_dir.join("events.jsonl")).unwrap().ino(),
        old
    );
    let staging = root.join(format!(".artifact-{ID}.partial"));
    assert_eq!(
        fs::metadata(staging.join("events.jsonl")).unwrap().ino(),
        old
    );
    assert_eq!(fs::read(staging.join("manifest.json")).unwrap(), MANIFEST);
    for _ in 0..4 {
        unit.write(&m, Some(&event), 4096, 8192, &cancel).unwrap();
    }
    assert_eq!(fs::read_dir(&root).unwrap().count(), 4); // marker, lock, final, one staging
    assert_eq!(fs::read(final_dir.join("events.jsonl")).unwrap(), data);
}

#[test]
fn incomplete_known_prefix_is_rebuilt_but_conflicts_preserve_all_files() {
    let temp = tempfile::tempdir().unwrap();
    let (root, destination) = destination(temp.path());
    let mut unit = ExportUnit::acquire(destination, ID.parse().unwrap()).unwrap();
    let event = input();
    let cut = cut();
    let m = manifest(&cut, &event);
    let staging = root.join(format!(".artifact-{ID}.partial"));
    private_dir(&staging);
    private_file(&staging.join("events.jsonl"), b"{\"id\":\"e258");
    unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false))
        .unwrap();
    assert!(!staging.exists());
    private_dir(&staging);
    private_file(&staging.join("events.jsonl"), b"{\"id\":\"different");
    let original = fs::read(root.join(ID).join("events.jsonl")).unwrap();
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
        Err(ExportFileError::Conflict)
    );
    assert_eq!(
        fs::read(staging.join("events.jsonl")).unwrap(),
        b"{\"id\":\"different"
    );
    assert_eq!(
        fs::read(root.join(ID).join("events.jsonl")).unwrap(),
        original
    );
    private_file(&staging.join("events.jsonl"), b"{");
    private_file(&staging.join("unknown"), b"preserve");
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
        Err(ExportFileError::Conflict)
    );
    assert_eq!(fs::read(staging.join("unknown")).unwrap(), b"preserve");
    fs::remove_file(staging.join("unknown")).unwrap();
    private_file(
        &root.join(ID).join("manifest.json"),
        b"conflicting manifest",
    );
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
        Err(ExportFileError::Conflict)
    );
    assert_eq!(fs::read(staging.join("events.jsonl")).unwrap(), b"{");
}

#[test]
fn retained_custody_excludes_other_writers_and_rejects_linked_files() {
    let temp = tempfile::tempdir().unwrap();
    let (root, destination) = destination(temp.path());
    let mut unit = ExportUnit::acquire(destination.clone(), ID.parse().unwrap()).unwrap();
    assert!(matches!(
        ExportUnit::acquire(destination.clone(), ID.parse().unwrap()),
        Err(ExportFileError::Busy)
    ));
    let custody = unit.custody();
    let event = input();
    let cut = cut();
    let m = manifest(&cut, &event);
    let staging = root.join(format!(".artifact-{ID}.partial"));
    private_dir(&staging);
    let external = temp.path().join("external");
    private_file(&external, b"{");
    symlink(&external, staging.join("events.jsonl")).unwrap();
    assert!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false))
            .is_err()
    );
    assert_eq!(fs::read(&external).unwrap(), b"{");
    assert!(!root.join(ID).exists());
    fs::remove_file(staging.join("events.jsonl")).unwrap();
    fs::hard_link(&external, staging.join("events.jsonl")).unwrap();
    assert!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false))
            .is_err()
    );
    assert_eq!(fs::metadata(&external).unwrap().nlink(), 2);
    drop(unit);
    assert!(matches!(
        ExportUnit::acquire(destination.clone(), ID.parse().unwrap()),
        Err(ExportFileError::Busy)
    ));
    drop(custody);
    assert!(ExportUnit::acquire(destination, ID.parse().unwrap()).is_ok());
}

#[test]
fn cancellation_and_limits_refuse_before_staging_and_empty_output_is_explicit() {
    let temp = tempfile::tempdir().unwrap();
    let (root, destination) = destination(temp.path());
    let mut unit = ExportUnit::acquire(destination, ID.parse().unwrap()).unwrap();
    let event = input();
    let cut = cut();
    let m = manifest(&cut, &event);
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(true)),
        Err(ExportFileError::Cancelled)
    );
    let total = 334 + MANIFEST.len() as u64;
    assert_eq!(
        unit.write(
            &m,
            Some(&event),
            total - 1,
            2 * total,
            &AtomicBool::new(false)
        ),
        Err(ExportFileError::ResourceLimit)
    );
    assert_eq!(
        unit.write(
            &m,
            Some(&event),
            total,
            2 * total - 1,
            &AtomicBool::new(false)
        ),
        Err(ExportFileError::ResourceLimit)
    );
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
    unit.write(&m, None, 4096, 8192, &AtomicBool::new(false))
        .unwrap();
    assert!(
        fs::read(root.join(ID).join("events.jsonl"))
            .unwrap()
            .is_empty()
    );
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join(ID).join("manifest.json")).unwrap()).unwrap();
    assert_eq!(value["files"][0]["rows"], "0");
    // A changed observation cannot silently replace this same unit's artifact.
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
        Err(ExportFileError::Conflict)
    );
}

#[test]
fn every_partial_cleanup_subset_is_recoverable_but_missing_final_file_is_not() {
    for subset in 0..4 {
        let temp = tempfile::tempdir().unwrap();
        let (root, destination) = destination(temp.path());
        let mut unit = ExportUnit::acquire(destination, ID.parse().unwrap()).unwrap();
        let event = input();
        let cut = cut();
        let m = manifest(&cut, &event);
        let staging = root.join(format!(".artifact-{ID}.partial"));
        private_dir(&staging);
        if subset & 1 != 0 {
            private_file(&staging.join("events.jsonl"), b"{");
        }
        if subset & 2 != 0 {
            private_file(&staging.join("manifest.json"), &MANIFEST[..17]);
        }
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(
            fs::read(root.join(ID).join("manifest.json")).unwrap(),
            MANIFEST
        );
        assert!(!staging.exists());
        fs::remove_file(root.join(ID).join("events.jsonl")).unwrap();
        assert_eq!(
            unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
            Err(ExportFileError::Conflict)
        );
        assert!(!staging.exists());
        assert_eq!(
            fs::read(root.join(ID).join("manifest.json")).unwrap(),
            MANIFEST
        );
    }
}

#[test]
fn refuses_foreign_lock_marker_root_and_unit_before_staging() {
    let temp = tempfile::tempdir().unwrap();
    let (root, destination) = destination(temp.path());
    let lock = root.join(format!(".artifact-{ID}.lock"));
    private_file(&lock, b"foreign");
    assert!(matches!(
        ExportUnit::acquire(destination.clone(), ID.parse().unwrap()),
        Err(ExportFileError::Conflict)
    ));
    assert_eq!(fs::read(&lock).unwrap(), b"foreign");
    fs::remove_file(lock).unwrap();
    let mut unit = ExportUnit::acquire(destination, ID.parse().unwrap()).unwrap();
    let event = input();
    let cut = cut();
    let mut m = manifest(&cut, &event);
    m.artifact_id = [0x11; 16].try_into().unwrap();
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
        Err(ExportFileError::Conflict)
    );
    m.artifact_id = ID.parse().unwrap();
    let old = root.with_file_name("old");
    fs::rename(&root, &old).unwrap();
    private_dir(&root);
    assert_eq!(
        unit.write(&m, Some(&event), 4096, 8192, &AtomicBool::new(false)),
        Err(ExportFileError::Conflict)
    );
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&old).unwrap().count(), 2);
}
