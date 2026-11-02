use super::*;

#[test]
fn operational_engine_receipt_has_an_independent_literal_encoding() {
    // Python hashlib over literal fields, fixed independently of the Rust
    // implementation and all engine observations. This is not a physical proof.
    let mut hash = receipt_start(
        Digest256::from_bytes([1; 32]),
        Digest256::from_bytes([2; 32]),
    );
    for target in IndexTarget::ALL {
        receipt_row(
            &mut hash,
            target,
            target as u64 * 3,
            Digest256::from_bytes([40 + target as u8; 32]),
        );
    }
    assert_eq!(
        Digest256::from_bytes(hash.finalize().into()).to_string(),
        "648671b6ea935e4fb1db26d4b25e6dbf01a059b7741d22ebcb335516cf5ed592"
    );
}
