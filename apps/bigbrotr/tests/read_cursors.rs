use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bigbrotr::{
    read::{
        cursor::{self, CURSOR_RESERVATION, CursorError, MAX_ENCODED_BYTES, MAX_PLAINTEXT_BYTES},
        keyring::{KEYRING_RESERVATION, Keyring},
    },
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
};
use std::{fs, os::unix::fs::PermissionsExt};

fn budget(bytes: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn permit(bytes: u64) -> OwnedPermit {
    budget(bytes)
        .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
        .unwrap()
}
fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn vectors() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/cursor-vectors.json"
    ))
    .unwrap()
}
fn keyring() -> Keyring {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/keyring-vectors.json"
    ))
    .unwrap();
    load_keys(
        &hex(fixtures["cases"][0]["hex"].as_str().unwrap()),
        [0x11; 16],
    )
}
fn load_keys(bytes: &[u8], dataset: [u8; 16]) -> Keyring {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.path().canonicalize().unwrap().join("keys");
    fs::write(&path, bytes).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    Keyring::open(
        &format!("file:{}", path.display()),
        dataset.try_into().unwrap(),
        permit(KEYRING_RESERVATION),
    )
    .unwrap()
}

#[test]
fn frozen_independent_envelopes_open_with_both_persistent_keys() {
    let keys = keyring();
    let value = vectors();
    for case in value["cases"].as_array().unwrap() {
        let opened = cursor::open(
            &keys,
            "management.jobs/1",
            case["token"].as_str().unwrap(),
            permit(CURSOR_RESERVATION),
        )
        .unwrap();
        assert_eq!(opened.payload(), hex(case["plaintext"].as_str().unwrap()));
        assert_eq!(
            opened.key_id().as_slice(),
            hex(case["key_id"].as_str().unwrap())
        );
        assert_eq!(format!("{opened:?}"), "OpenedCursor([redacted])");
    }
}

#[test]
fn independent_keyring_instances_can_read_new_randomized_tokens_at_the_exact_limit() {
    let keys = keyring();
    let second = keyring();
    let payload = vec![0x9f; MAX_PLAINTEXT_BYTES];
    let first = cursor::seal(
        &keys,
        [0x33; 16],
        "management.jobs/1",
        &payload,
        permit(CURSOR_RESERVATION),
    )
    .unwrap();
    let next = cursor::seal(
        &keys,
        [0x33; 16],
        "management.jobs/1",
        &payload,
        permit(CURSOR_RESERVATION),
    )
    .unwrap();
    assert_eq!(first.as_str().len(), MAX_ENCODED_BYTES);
    assert_ne!(first.as_str(), next.as_str());
    assert_eq!(format!("{first:?}"), "EncodedCursor([redacted])");
    let wire = URL_SAFE_NO_PAD.decode(first.as_str()).unwrap();
    assert_eq!(&wire[..17], &[&[1][..], &[0x33; 16]].concat());
    assert!(!wire.windows(64).any(|w| w == &payload[..64]));
    assert_eq!(
        cursor::open(
            &second,
            "management.jobs/1",
            first.as_str(),
            permit(CURSOR_RESERVATION)
        )
        .unwrap()
        .payload(),
        payload
    );
}

#[test]
fn every_wire_byte_is_authenticated_and_malformed_base64_is_refused() {
    let keys = keyring();
    let value = vectors();
    let token = value["cases"][1]["token"].as_str().unwrap();
    let wire = URL_SAFE_NO_PAD.decode(token).unwrap();
    let mut mutations = 0;
    for case in value["cases"].as_array().unwrap() {
        let bytes = URL_SAFE_NO_PAD
            .decode(case["token"].as_str().unwrap())
            .unwrap();
        for index in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert_eq!(
                cursor::open(
                    &keys,
                    "management.jobs/1",
                    &URL_SAFE_NO_PAD.encode(&changed),
                    permit(CURSOR_RESERVATION)
                )
                .unwrap_err(),
                CursorError::Invalid,
                "{} offset{index}",
                case["name"]
            );
            mutations += 1;
        }
    }
    assert_eq!(mutations, 4081); // 3511 ciphertext bytes + ten57-byte envelopes.
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut noncanonical = token.as_bytes().to_vec();
    let last = noncanonical.last_mut().unwrap();
    *last = alphabet[alphabet.iter().position(|b| b == last).unwrap() + 1];
    for malformed in [
        String::new(),
        format!("{token}="),
        format!(" {token}"),
        String::from_utf8(noncanonical).unwrap(),
        URL_SAFE_NO_PAD.encode(&wire[..56]),
        format!("{token}\n"),
        "é".to_owned(),
    ] {
        assert_eq!(
            cursor::open(
                &keys,
                "management.jobs/1",
                &malformed,
                permit(CURSOR_RESERVATION)
            )
            .unwrap_err(),
            CursorError::Invalid
        );
    }
}

#[test]
fn dataset_ring_resource_and_key_changes_cannot_reuse_a_token() {
    use sha2::{Digest, Sha256};
    let keys = keyring();
    let value = vectors();
    let token = value["cases"][1]["token"].as_str().unwrap();
    for domain in [
        "management.jobs/2",
        "management.job/1",
        "Management.jobs/1",
        "",
        "bad\0resource",
    ] {
        assert_eq!(
            cursor::open(&keys, domain, token, permit(CURSOR_RESERVATION)).unwrap_err(),
            CursorError::Invalid
        );
    }
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/keyring-vectors.json"
    ))
    .unwrap();
    let bytes = hex(fixtures["cases"][0]["hex"].as_str().unwrap());
    for offset in [8, 24, 106] {
        let mut changed = bytes.clone();
        changed[offset] ^= 1;
        let length = changed.len() - 32;
        let sum = Sha256::digest(&changed[..length]);
        changed[length..].copy_from_slice(&sum);
        let second = load_keys(&changed, changed[8..24].try_into().unwrap());
        assert_eq!(
            cursor::open(
                &second,
                "management.jobs/1",
                token,
                permit(CURSOR_RESERVATION)
            )
            .unwrap_err(),
            CursorError::Invalid
        );
    }
    assert_eq!(
        cursor::seal(
            &keys,
            [0x99; 16],
            "management.jobs/1",
            b"x",
            permit(CURSOR_RESERVATION)
        )
        .unwrap_err(),
        CursorError::Invalid
    );
}

#[test]
fn oversized_inputs_and_insufficient_charges_fail_with_capacity() {
    let keys = keyring();
    let value = vectors();
    let token = value["cases"][0]["token"].as_str().unwrap();
    assert_eq!(
        cursor::open(
            &keys,
            "management.jobs/1",
            &"A".repeat(MAX_ENCODED_BYTES + 1),
            permit(CURSOR_RESERVATION)
        )
        .unwrap_err(),
        CursorError::Capacity
    );
    assert_eq!(
        cursor::seal(
            &keys,
            [0x33; 16],
            "management.jobs/1",
            &vec![0; MAX_PLAINTEXT_BYTES + 1],
            permit(CURSOR_RESERVATION)
        )
        .unwrap_err(),
        CursorError::Capacity
    );
    assert_eq!(
        cursor::open(
            &keys,
            "management.jobs/1",
            token,
            permit(CURSOR_RESERVATION - 1)
        )
        .unwrap_err(),
        CursorError::Capacity
    );
    assert_eq!(
        cursor::seal(
            &keys,
            [0x33; 16],
            "management.jobs/1",
            b"x",
            permit(CURSOR_RESERVATION - 1)
        )
        .unwrap_err(),
        CursorError::Capacity
    );
}

#[test]
fn returned_bytes_retain_the_charge_and_failures_release_it() {
    let keys = keyring();
    let budget = budget(CURSOR_RESERVATION);
    let request = ResourceRequest::data(CURSOR_RESERVATION, 1, 0).unwrap();
    let sealed = cursor::seal(
        &keys,
        [0x33; 16],
        "management.jobs/1",
        b"private scan",
        budget.try_acquire(request).unwrap(),
    )
    .unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert!(budget.try_acquire(request).is_err());
    let copy = sealed.as_str().to_owned();
    drop(sealed);
    assert_eq!(budget.snapshot().active_permits(), 0);
    let opened = cursor::open(
        &keys,
        "management.jobs/1",
        &copy,
        budget.try_acquire(request).unwrap(),
    )
    .unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert_eq!(opened.payload(), b"private scan");
    drop(opened);
    assert!(
        cursor::open(
            &keys,
            "management.jobs/2",
            &copy,
            budget.try_acquire(request).unwrap()
        )
        .is_err()
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn unrelated_key_changes_preserve_existing_tokens_and_resource_names_are_bounded() {
    use sha2::{Digest, Sha256};
    let keys = keyring();
    let domain = "a".repeat(128);
    let token = cursor::seal(
        &keys,
        [0x33; 16],
        &domain,
        b"retained",
        permit(CURSOR_RESERVATION),
    )
    .unwrap();
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/read/keyring-vectors.json"
    ))
    .unwrap();
    let mut bytes = hex(fixtures["cases"][0]["hex"].as_str().unwrap());
    bytes[106] ^= 1;
    let length = bytes.len() - 32;
    let sum = Sha256::digest(&bytes[..length]);
    bytes[length..].copy_from_slice(&sum);
    let second = load_keys(&bytes, [0x11; 16]);
    assert_ne!(keys.fingerprint(), second.fingerprint());
    assert_eq!(
        cursor::open(&second, &domain, token.as_str(), permit(CURSOR_RESERVATION))
            .unwrap()
            .payload(),
        b"retained"
    );
    for invalid in [
        "a".repeat(129),
        String::new(),
        "résource".to_owned(),
        "a b".to_owned(),
    ] {
        assert_eq!(
            cursor::seal(
                &keys,
                [0x33; 16],
                &invalid,
                b"x",
                permit(CURSOR_RESERVATION)
            )
            .unwrap_err(),
            CursorError::Invalid
        );
        assert_eq!(
            cursor::open(&keys, &invalid, token.as_str(), permit(CURSOR_RESERVATION)).unwrap_err(),
            CursorError::Invalid
        );
    }
}
