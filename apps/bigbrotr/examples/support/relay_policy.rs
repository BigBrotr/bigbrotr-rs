//! Independent fixture policy comparison for the actual Control adapter probes.
use bigbrotr::storage::postgres::control_owner::OwnerError;
use sha2::{Digest, Sha256};

pub fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    // Fixture policy is supplied independently by the laboratory. This does not
    // replace production root/closed-schema validation or grant network access.
    let supplied: serde_json::Value =
        serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let bytes = std::fs::read(std::env::args().nth(3).expect("fixture policy path"))
        .expect("fixture policy");
    let expected: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let canonical = serde_json::to_vec(&expected).unwrap();
    let expected_digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/3\0")
        .chain_update(canonical)
        .finalize()
        .into();
    if supplied != expected || digest != expected_digest {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}
