//! Qualification probe for the selected dependency API, not product crypto.
use chacha20poly1305::{AeadInOut, KeyInit, Tag, XChaCha20Poly1305, XNonce};
use serde_json::{Value, json};

fn hex(value: &Value) -> Vec<u8> {
    let value = value.as_str().unwrap();
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn main() {
    let fixtures: Value = serde_json::from_str(include_str!("../vectors.json")).unwrap();
    let mut positives = 0;
    let mut refusals = 0;
    for case in fixtures["vectors"].as_array().unwrap() {
        let key: [u8; 32] = hex(&case["key"]).try_into().unwrap();
        let nonce: XNonce = <[u8; 24]>::try_from(hex(&case["nonce"])).unwrap().into();
        let aad = hex(&case["aad"]);
        let plaintext = hex(&case["plaintext"]);
        let expected = hex(&case["ciphertext_and_tag"]);
        let cipher = XChaCha20Poly1305::new((&key).into());
        let mut encrypted = plaintext.clone();
        let tag = cipher
            .encrypt_inout_detached(&nonce, &aad, encrypted.as_mut_slice().into())
            .unwrap();
        assert_eq!(encrypted, expected[..plaintext.len()], "{}", case["name"]);
        assert_eq!(tag.as_slice(), &expected[plaintext.len()..]);
        cipher
            .decrypt_inout_detached(&nonce, &aad, encrypted.as_mut_slice().into(), &tag)
            .unwrap();
        assert_eq!(encrypted, plaintext);
        positives += 1;

        // Flip every ciphertext/tag byte independently. The selected concrete
        // implementation must reject before mutating the caller-owned buffer.
        for index in 0..expected.len() {
            let mut changed = expected.clone();
            changed[index] ^= 1;
            let tag: Tag = changed[plaintext.len()..].try_into().unwrap();
            changed.truncate(plaintext.len());
            let before = changed.clone();
            assert!(
                cipher
                    .decrypt_inout_detached(&nonce, &aad, changed.as_mut_slice().into(), &tag)
                    .is_err()
            );
            assert_eq!(changed, before);
            refusals += 1;
        }
        for field in ["key", "nonce", "aad"] {
            let mut key = key;
            let mut nonce = nonce;
            let mut aad = aad.clone();
            match field {
                "key" => key[0] ^= 1,
                "nonce" => nonce[0] ^= 1,
                "aad" => aad[0] ^= 1,
                _ => unreachable!(),
            }
            let cipher = XChaCha20Poly1305::new((&key).into());
            let mut bytes = expected[..plaintext.len()].to_vec();
            let before = bytes.clone();
            let tag: Tag = expected[plaintext.len()..].try_into().unwrap();
            assert!(
                cipher
                    .decrypt_inout_detached(&nonce, &aad, bytes.as_mut_slice().into(), &tag)
                    .is_err()
            );
            assert_eq!(bytes, before);
            refusals += 1;
        }
    }
    assert_eq!(positives, 11);
    assert_eq!(refusals, 4915);
    println!(
        "{}",
        json!({"scope":"dependency API only", "status":"PASS", "positive_vectors":positives, "tamper_refusals_with_unchanged_buffer":refusals})
    );
}
