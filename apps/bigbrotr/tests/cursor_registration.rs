//! Initial Control registration first requires an existing durable local snapshot.
use bigbrotr::{
    read::keyring::{KEYRING_RESERVATION, KeyringError, PreparedKeyring},
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
};

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            2 * KEYRING_RESERVATION,
            0,
            ResourceLimits::new(KEYRING_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn permit(b: &ResourceBudget) -> bigbrotr::runtime::OwnedPermit {
    b.try_acquire(ResourceRequest::data(KEYRING_RESERVATION, 1, 0).unwrap())
        .unwrap()
}
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn registration_preparation_preserves_keys_and_binds_each_material_digest() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("keys");
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/keyring-vectors.json"
    ))
    .unwrap();
    let bytes = unhex(vectors["cases"][0]["hex"].as_str().unwrap());
    fs::write(&path, &bytes).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let before = fs::metadata(&path).unwrap().ino();
    let b = budget();
    let prepared = PreparedKeyring::existing(
        &format!("file:{}", path.display()),
        "11111111111111111111111111111111".parse().unwrap(),
        permit(&b),
    )
    .unwrap();
    assert_ne!(fs::metadata(&path).unwrap().ino(), before);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let bindings: Vec<_> = prepared.keys().bindings().collect();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/key-binding-vectors.json"
    ))
    .unwrap();
    for (actual, expected) in bindings.iter().zip(oracle.as_array().unwrap()) {
        assert_eq!(
            actual.0.as_slice(),
            unhex(expected["key_id"].as_str().unwrap())
        );
        assert_eq!(
            actual.1.as_bytes().as_slice(),
            unhex(expected["digest"].as_str().unwrap())
        );
    }
    assert_eq!(bindings.len(), 2);
    assert_eq!(prepared.keys().dataset().as_bytes(), &[0x11; 16]);
    assert!(
        b.try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
            .is_err()
    );
    drop(prepared);
    assert!(
        b.try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
            .is_ok()
    );
}

#[test]
fn registration_never_creates_missing_keys_or_repairs_incomplete_preparation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("keys");
    let b = budget();
    let attempt = || {
        PreparedKeyring::existing(
            &format!("file:{}", path.display()),
            "11111111111111111111111111111111".parse().unwrap(),
            permit(&b),
        )
    };
    assert!(matches!(attempt(), Err(KeyringError::NotInitialized)));
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    fs::write(
        root.join("unresolved.partial"),
        b"preserve independent sentinel",
    )
    .unwrap();
    assert!(matches!(attempt(), Err(KeyringError::Incomplete)));
    assert!(!path.exists());
    assert_eq!(
        fs::read(root.join("unresolved.partial")).unwrap(),
        b"preserve independent sentinel"
    );
}
