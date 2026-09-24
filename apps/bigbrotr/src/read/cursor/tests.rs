use super::*;
use crate::read::keyring::KEYRING_RESERVATION;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use std::os::unix::fs::PermissionsExt;

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
fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn keys() -> Keyring {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/read/keyring-vectors.json"
    ))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().canonicalize().unwrap().join("keys");
    std::fs::write(&path, hex(fixture["cases"][0]["hex"].as_str().unwrap())).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    Keyring::open(
        &format!("file:{}", path.display()),
        [0x11; 16].try_into().unwrap(),
        permit(KEYRING_RESERVATION),
    )
    .unwrap()
}

#[test]
fn exact_encryption_matches_independent_libsodium_with_injected_public_nonces() {
    let keys = keys();
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/read/cursor-vectors.json"
    ))
    .unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let nonce = hex(case["nonce"].as_str().unwrap());
        let token = seal_with_entropy(
            &keys,
            hex(case["key_id"].as_str().unwrap()).try_into().unwrap(),
            "management.jobs/1",
            &hex(case["plaintext"].as_str().unwrap()),
            permit(CURSOR_RESERVATION),
            |output| {
                output.copy_from_slice(&nonce);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(token.as_str(), case["token"].as_str().unwrap());
    }
}

#[test]
fn entropy_failure_never_returns_a_token_and_admission_precedes_entropy() {
    let keys = keys();
    assert_eq!(
        seal_with_entropy(
            &keys,
            [0x33; 16],
            "management.jobs/1",
            b"x",
            permit(CURSOR_RESERVATION),
            |_| Err(CursorError::Entropy)
        )
        .unwrap_err(),
        CursorError::Entropy
    );
    assert_eq!(
        seal_with_entropy(
            &keys,
            [0x33; 16],
            "management.jobs/1",
            b"x",
            permit(CURSOR_RESERVATION - 1),
            |_| panic!("unadmitted entropy")
        )
        .unwrap_err(),
        CursorError::Capacity
    );
    assert_eq!(
        seal_with_entropy(
            &keys,
            [0x33; 16],
            "management.jobs/1",
            &vec![0; MAX_PLAINTEXT_BYTES + 1],
            permit(CURSOR_RESERVATION),
            |_| panic!("oversized entropy")
        )
        .unwrap_err(),
        CursorError::Capacity
    );
    assert_eq!(
        seal_with_entropy(
            &keys,
            [0x33; 16],
            "invalid resource",
            b"x",
            permit(CURSOR_RESERVATION),
            |_| panic!("invalid resource entropy")
        )
        .unwrap_err(),
        CursorError::Invalid
    );
}
