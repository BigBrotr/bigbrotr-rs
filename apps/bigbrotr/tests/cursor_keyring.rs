use bigbrotr::{
    read::keyring::{KEYRING_RESERVATION, Keyring, KeyringError},
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
};
use std::{fs, os::unix::fs::PermissionsExt};

fn permit(bytes: u64) -> OwnedPermit {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
    .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
    .unwrap()
}
fn fixture() -> (tempfile::TempDir, String) {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let reference = format!(
        "file:{}/keys.bin",
        root.path().canonicalize().unwrap().display()
    );
    (root, reference)
}

#[test]
fn explicit_preparation_persists_the_same_keys_across_independent_opens_and_retry() {
    let (root, reference) = fixture();
    let dataset = [1; 16].try_into().unwrap();
    assert_eq!(
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::NotInitialized
    );
    assert!(!root.path().join("keys.bin").exists());
    let first = Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap();
    let id = first.id();
    let key_ids: Vec<_> = first.key_ids().collect();
    let fingerprint = first.fingerprint();
    assert_ne!(id, [0; 16]);
    assert_eq!(key_ids.len(), 1);
    assert_ne!(key_ids[0], [0; 16]);
    assert_eq!(format!("{first:?}"), "Keyring([redacted])");
    let original = fs::read(root.path().join("keys.bin")).unwrap();
    assert_eq!(
        fs::metadata(root.path().join("keys.bin"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );
    drop(first);
    for keyring in [
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap(),
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap(),
    ] {
        assert_eq!(keyring.id(), id);
        assert_eq!(keyring.key_ids().collect::<Vec<_>>(), key_ids);
        assert_eq!(keyring.fingerprint(), fingerprint);
    }
    assert!(fs::read(root.path().join("keys.bin")).unwrap() == original);
}

#[test]
fn insufficient_reservation_fails_before_creating_or_opening_secrets() {
    let (root, reference) = fixture();
    let dataset = [1; 16].try_into().unwrap();
    assert_eq!(
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION - 1)).unwrap_err(),
        KeyringError::Capacity
    );
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

fn public_vectors() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/keyring-vectors.json"
    ))
    .unwrap()
}
fn decode(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn write_private(path: &std::path::Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn independent_wire_fixtures_and_every_single_byte_change_are_discriminated() {
    let vectors = public_vectors();
    let dataset = vectors["dataset_id"].as_str().unwrap().parse().unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let (root, reference) = fixture();
        let bytes = decode(case["hex"].as_str().unwrap());
        let path = root.path().join("keys.bin");
        write_private(&path, &bytes);
        for prepare in [false, true] {
            let result = if prepare {
                Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION))
            } else {
                Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION))
            };
            if let Some(error) = case["error"].as_str() {
                assert_eq!(
                    format!("{:?}", result.unwrap_err()),
                    error,
                    "{}",
                    case["name"]
                );
            } else {
                let keys = result.unwrap();
                assert_eq!(keys.id(), [0x22; 16]);
                if case["name"] == "two-public-keys" {
                    assert_eq!(
                        keys.key_ids().collect::<Vec<_>>(),
                        vec![[0x33; 16], [0x44; 16]]
                    );
                    assert_eq!(
                        keys.fingerprint().to_string(),
                        "6062f6146f09e56040f7953de2f4e4f2603fbb857008f16018867d093c5a2f8a"
                    );
                } else {
                    assert_eq!(keys.key_ids().count(), 16);
                }
            }
            assert!(fs::read(&path).unwrap() == bytes);
            assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        }
    }
    let original = decode(vectors["cases"][0]["hex"].as_str().unwrap());
    let (root, reference) = fixture();
    for index in 0..original.len() {
        let mut changed = original.clone();
        changed[index] ^= 1;
        write_private(&root.path().join("keys.bin"), &changed);
        assert!(
            Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).is_err(),
            "offset {index}"
        );
    }
}

#[test]
fn incomplete_staging_is_preserved_and_does_not_regenerate_keys() {
    let (root, reference) = fixture();
    let dataset = [1; 16].try_into().unwrap();
    let staging = root.path().join(".cursor-keyring-interrupted.partial");
    write_private(&staging, b"incomplete public fixture");
    assert_eq!(
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Incomplete
    );
    assert_eq!(
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::NotInitialized
    );
    assert!(!root.path().join("keys.bin").exists());
    assert_eq!(fs::read(&staging).unwrap(), b"incomplete public fixture");
    // A complete final snapshot remains readable; staging is never interpreted.
    fs::remove_file(&staging).unwrap();
    let first = Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap();
    write_private(&staging, b"incomplete next attempt");
    assert_eq!(
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Incomplete
    );
    let current = Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap();
    assert_eq!(current.id(), first.id());
    assert_eq!(current.fingerprint(), first.fingerprint());
    assert_eq!(fs::read(&staging).unwrap(), b"incomplete next attempt");
}

#[test]
fn directory_locks_exclude_preparation_and_readers_without_waiting() {
    use rustix::fs::{FlockOperation, flock};
    let (root, reference) = fixture();
    let dataset = [1; 16].try_into().unwrap();
    let first = Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap();
    let directory = fs::File::open(root.path()).unwrap();
    flock(&directory, FlockOperation::NonBlockingLockExclusive).unwrap();
    let start = std::time::Instant::now();
    assert_eq!(
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Busy
    );
    assert_eq!(
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Busy
    );
    assert!(start.elapsed() < std::time::Duration::from_secs(1));
    flock(&directory, FlockOperation::NonBlockingLockShared).unwrap();
    assert_eq!(
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION))
            .unwrap()
            .id(),
        first.id()
    );
    assert_eq!(
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Busy
    );
    drop(directory);
    assert_eq!(
        Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION))
            .unwrap()
            .id(),
        first.id()
    );
}

#[test]
fn concurrent_preparers_publish_one_identity_and_keep_no_extra_files() {
    let (root, reference) = fixture();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let barrier = barrier.clone();
        let reference = reference.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            Keyring::prepare(
                &reference,
                [1; 16].try_into().unwrap(),
                permit(KEYRING_RESERVATION),
            )
        }));
    }
    let mut ids = Vec::new();
    for thread in threads {
        match thread.join().unwrap() {
            Ok(keys) => ids.push(keys.id()),
            Err(error) => assert_eq!(error, KeyringError::Busy),
        }
    }
    assert!(!ids.is_empty());
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    assert_eq!(
        Keyring::open(
            &reference,
            [1; 16].try_into().unwrap(),
            permit(KEYRING_RESERVATION)
        )
        .unwrap()
        .id(),
        ids[0]
    );
}

#[test]
fn modes_links_and_nonregular_files_are_refused_without_modification() {
    use std::os::unix::fs::symlink;
    let vectors = public_vectors();
    let bytes = decode(vectors["cases"][0]["hex"].as_str().unwrap());
    let dataset = vectors["dataset_id"].as_str().unwrap().parse().unwrap();
    let (root, reference) = fixture();
    let path = root.path().join("keys.bin");
    write_private(&path, &bytes);
    for mode in [0o644, 0o640, 0o700, 0o4600, 0o000] {
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        assert!(Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).is_err());
        assert!(Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).is_err());
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
    Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap();
    fs::hard_link(&path, root.path().join("link")).unwrap();
    assert_eq!(
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Permissions
    );
    fs::remove_file(&path).unwrap();
    symlink("link", &path).unwrap();
    assert!(Keyring::prepare(&reference, dataset, permit(KEYRING_RESERVATION)).is_err());
    assert_eq!(fs::read(root.path().join("link")).unwrap(), bytes);
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).is_err());
    fs::remove_dir(&path).unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    assert!(Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).is_err());
    fs::remove_file(&path).unwrap();
    write_private(&path, &bytes);
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o750)).unwrap();
    assert_eq!(
        Keyring::open(&reference, dataset, permit(KEYRING_RESERVATION)).unwrap_err(),
        KeyringError::Permissions
    );
}
