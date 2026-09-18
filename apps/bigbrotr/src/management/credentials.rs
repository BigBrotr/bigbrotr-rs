//! Closed initial password format, explicit memory ownership and opaque tokens.
use crate::runtime::OwnedPermit;
use argon2::{Algorithm, Argon2, Block, Params, Version};
use sha2::{Digest, Sha256};
use std::fmt;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

pub const HASH_RESERVATION: u64 = 65 * 1024 * 1024;
const BLOCKS: usize = 65536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialError {
    InvalidPassword,
    InvalidToken,
    InvalidName,
    Capacity,
    Entropy,
    Hash,
}
impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "credential refused: {self:?}")
    }
}
impl std::error::Error for CredentialError {}

pub fn validate_name(name: &str) -> Result<(), CredentialError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
    {
        return Err(CredentialError::InvalidName);
    }
    Ok(())
}

/// Only a successful bounded hash constructs an enrollment credential.
pub struct PasswordCredential {
    pub(crate) salt: [u8; 16],
    pub(crate) hash: Zeroizing<[u8; 32]>,
}
impl fmt::Debug for PasswordCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PasswordCredential([redacted])")
    }
}
impl PasswordCredential {
    pub fn create(password: &[u8], grant: &mut OwnedPermit) -> Result<Self, CredentialError> {
        validate_password(password)?;
        let salt = random()?;
        let hash = hash(password, &salt, grant)?;
        Ok(Self { salt, hash })
    }
}

fn validate_password(password: &[u8]) -> Result<(), CredentialError> {
    let text = std::str::from_utf8(password).map_err(|_| CredentialError::InvalidPassword)?;
    if password.len() > 1024 || text.chars().count() < 12 || text.chars().any(char::is_control) {
        return Err(CredentialError::InvalidPassword);
    }
    Ok(())
}

fn hash(
    password: &[u8],
    salt: &[u8; 16],
    grant: &mut OwnedPermit,
) -> Result<Zeroizing<[u8; 32]>, CredentialError> {
    if grant.request().units().data_bytes() < HASH_RESERVATION
        || grant.request().units().items() < 1
    {
        return Err(CredentialError::Capacity);
    }
    let params = Params::new(65536, 3, 1, Some(32)).map_err(|_| CredentialError::Hash)?;
    // No allocator or unbounded blocking worker hidden in the primitive. The
    // caller exclusively lends its permit through this synchronous operation
    // and wiping; safe callers cannot run two hashes under the same permit.
    let mut blocks = Zeroizing::new(Vec::new());
    blocks
        .try_reserve_exact(BLOCKS)
        .map_err(|_| CredentialError::Capacity)?;
    blocks.resize(BLOCKS, Block::default());
    if blocks.capacity() > (HASH_RESERVATION as usize / size_of::<Block>()) {
        return Err(CredentialError::Capacity);
    }
    let mut output = Zeroizing::new([0; 32]);
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into_with_memory(password, salt, &mut output[..], &mut blocks[..])
        .map_err(|_| CredentialError::Hash)?;
    Ok(output)
}

pub(crate) fn random<const N: usize>() -> Result<[u8; N], CredentialError> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| CredentialError::Entropy)?;
    if bytes == [0; N] {
        return Err(CredentialError::Entropy);
    }
    Ok(bytes)
}

pub struct OwnerToken(Zeroizing<[u8; 32]>);
impl fmt::Debug for OwnerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OwnerToken([redacted])")
    }
}
impl OwnerToken {
    pub(crate) fn generate() -> Result<Self, CredentialError> {
        Ok(Self(Zeroizing::new(random()?)))
    }
    pub fn parse_hex(bytes: &[u8]) -> Result<Self, CredentialError> {
        if bytes.len() != 64 {
            return Err(CredentialError::InvalidToken);
        }
        let mut value = Zeroizing::new([0; 32]);
        for (target, pair) in value.iter_mut().zip(bytes.as_chunks::<2>().0) {
            let digit = |b| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                _ => Err(CredentialError::InvalidToken),
            };
            *target = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        Ok(Self(value))
    }
    /// Explicit one-time credential output only; never put this in routine logs.
    pub fn expose_hex(&self) -> Zeroizing<String> {
        Zeroizing::new(self.0.iter().map(|b| format!("{b:02x}")).collect())
    }
    pub(crate) fn digest(&self) -> [u8; 32] {
        Sha256::new()
            .chain_update(b"bigbrotr/first-owner-token/1\0")
            .chain_update(&self.0[..])
            .finalize()
            .into()
    }
    pub(crate) fn matches(&self, digest: &[u8; 32]) -> bool {
        bool::from(self.digest().ct_eq(digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    };
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
    #[test]
    fn password_baseline_matches_independent_phc_c_reference() {
        // PHC f57e61e19229e23c4445b85494dbf7c07de721cb, reference_vector.c.
        let result = hash(
            b"Independent owner password 2026",
            &[2; 16],
            &mut permit(HASH_RESERVATION),
        )
        .unwrap();
        let hex: String = result.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff"
        );
        assert_eq!(size_of::<Block>(), 1024);
    }
    #[test]
    fn cannot_hash_under_an_insufficient_reservation() {
        assert_eq!(
            hash(
                b"Independent owner password 2026",
                &[2; 16],
                &mut permit(HASH_RESERVATION - 1)
            )
            .unwrap_err(),
            CredentialError::Capacity
        );
    }
    #[test]
    fn password_and_name_policy_are_exact_and_bounded() {
        for value in [
            &b"short"[..],
            &b"long enough but newline\n"[..],
            &[255; 12][..],
        ] {
            assert_eq!(
                validate_password(value),
                Err(CredentialError::InvalidPassword)
            );
        }
        assert!(validate_password("twelve spaces            ".as_bytes()).is_ok());
        assert!(validate_password("é".repeat(12).as_bytes()).is_ok());
        assert!(validate_password(&[b'a'; 1024]).is_ok());
        assert!(validate_password(&[b'a'; 1025]).is_err());
        for value in ["", "Alice", "a/b", "x\n", "é"] {
            assert!(validate_name(value).is_err());
        }
        assert!(validate_name("alice-2026.test_1").is_ok());
    }
    #[test]
    fn token_parsing_is_closed_and_comparison_rejects_changed_digest() {
        let token = OwnerToken::parse_hex(&[b'a'; 64]).unwrap();
        assert_eq!(&*token.expose_hex(), &"a".repeat(64));
        assert!(token.matches(&token.digest()));
        let mut wrong = token.digest();
        wrong[31] ^= 1;
        assert!(!token.matches(&wrong));
        assert!(OwnerToken::parse_hex(&[b'A'; 64]).is_err());
        assert!(OwnerToken::parse_hex(&[b'a'; 63]).is_err());
        assert_eq!(format!("{token:?}"), "OwnerToken([redacted])");
    }
}
