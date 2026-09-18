//! Ephemeral random writer passwords and PostgreSQL16 SCRAM verifiers. Never
//! send a plaintext password in CREATE ROLE SQL or retain it in catalog rows.
use super::GuardError;
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::{hmac, pbkdf2};
use sha2::{Digest, Sha256};
use std::{fmt, fmt::Write, num::NonZeroU32};
use zeroize::{Zeroize, Zeroizing};

pub(super) struct Material {
    pub(super) password: Zeroizing<String>,
    pub(super) verifier: Zeroizing<String>,
}
impl fmt::Debug for Material {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WriterMaterial([redacted])")
    }
}
impl Material {
    pub(super) fn generate() -> Result<Self, GuardError> {
        Self::generate_with(|bytes| getrandom::fill(bytes).map_err(|_| ()))
    }
    fn generate_with(fill: impl FnOnce(&mut [u8]) -> Result<(), ()>) -> Result<Self, GuardError> {
        let mut entropy = Zeroizing::new([0; 48]);
        fill(&mut entropy[..]).map_err(|_| GuardError::Entropy)?;
        if entropy[..32] == [0; 32] {
            return Err(GuardError::Entropy);
        }
        Ok(Self::from_bytes(
            entropy[..32].try_into().expect("fixed password entropy"),
            entropy[32..].try_into().expect("fixed salt entropy"),
        ))
    }
    fn from_bytes(mut entropy: [u8; 32], salt: [u8; 16]) -> Self {
        // Hex preserves all256 random bits and needs no SASLprep normalization.
        // This is not a human-password policy or a general SCRAM implementation.
        let mut password = Zeroizing::new(String::with_capacity(64));
        for byte in &entropy {
            write!(&mut *password, "{byte:02x}").expect("String write");
        }
        entropy.zeroize();
        let mut salted = Zeroizing::new([0; 32]);
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(4096).expect("positive fixed SCRAM iterations"),
            &salt,
            password.as_bytes(),
            &mut salted[..],
        );
        let key = hmac::Key::new(hmac::HMAC_SHA256, &salted[..]);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored_key = Sha256::digest(client_key.as_ref());
        let server_key = hmac::sign(&key, b"Server Key");
        let verifier = Zeroizing::new(format!(
            "SCRAM-SHA-256$4096:{}${}:{}",
            STANDARD.encode(salt),
            STANDARD.encode(stored_key),
            STANDARD.encode(server_key.as_ref())
        ));
        Self { password, verifier }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_matches_independent_python_hashlib_vector() {
        let secret = Material::from_bytes(
            std::array::from_fn(|i| i as u8),
            std::array::from_fn(|i| i as u8 + 16),
        );
        assert_eq!(
            &*secret.password,
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
        assert_eq!(
            &*secret.verifier,
            "SCRAM-SHA-256$4096:EBESExQVFhcYGRobHB0eHw==$5KYIfObOVF0xImP6XVKn5DRP335rrG9YIJPE9WxRI6U=:t505VkfZcCzOJCRmJAyu2tY+FKty2OQTlnAA44VUj7k="
        );
        assert!(!format!("{secret:?}").contains(&*secret.password));
        assert!(!format!("{secret:?}").contains(&*secret.verifier));
    }

    #[test]
    fn entropy_failure_or_zero_password_never_produces_credentials() {
        assert!(
            Material::generate_with(|bytes| {
                bytes.fill(7);
                Err(())
            })
            .is_err()
        );
        assert!(Material::generate_with(|_| Ok(())).is_err());
        assert!(
            Material::generate_with(|bytes| {
                bytes[32..].fill(7);
                Ok(())
            })
            .is_err()
        );
    }

    #[test]
    fn fresh_os_passwords_are_bounded_and_sql_safe() {
        let first = Material::generate().unwrap();
        let second = Material::generate().unwrap();
        assert_ne!(first.password, second.password);
        assert_ne!(first.verifier, second.verifier);
        assert_eq!(first.password.len(), 64);
        assert!(
            first
                .password
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert!(!first.verifier.contains(['\\', '\'', '\0', '\n', '\r']));
        assert_eq!(first.verifier.len(), 133);
    }
}
