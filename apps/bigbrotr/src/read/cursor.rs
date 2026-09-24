//! Confidential continuation bytes, not current read or issuance authority.
//!
//! Callers authenticate before opening and check current Control key lifecycle,
//! resource-specific query/principal/policy/expiry bindings and retention after
//! authentication. This codec never accesses files, clocks or the network.
use super::keyring::Keyring;
use crate::runtime::OwnedPermit;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chacha20poly1305::{AeadInOut, KeyInit, Tag, XChaCha20Poly1305};
use std::fmt;
use zeroize::Zeroizing;

pub const CURSOR_RESERVATION: u64 = 16 * 1024;
pub const MAX_ENCODED_BYTES: usize = 4096;
const MAX_BINARY_BYTES: usize = 3072;
const HEADER: usize = 1 + 16 + 24;
const TAG: usize = 16;
pub const MAX_PLAINTEXT_BYTES: usize = MAX_BINARY_BYTES - HEADER - TAG;
const DOMAIN: &[u8; 16] = b"BigBrotr cursor\0";
const MAX_RESOURCE: usize = 128;
const AAD_PREFIX: usize = 16 + 16 + 16 + 17 + 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorError {
    /// Malformed, unsupported, unknown key or failed authentication; no fallback.
    Invalid,
    Capacity,
    Entropy,
}
impl fmt::Display for CursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cursor envelope refused: {self:?}")
    }
}
impl std::error::Error for CursorError {}

/// The charge remains live through delivery or the caller's explicitly charged copy.
pub struct EncodedCursor {
    bytes: Zeroizing<Vec<u8>>,
    _permit: OwnedPermit,
}
impl EncodedCursor {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes).expect("base64 ASCII")
    }
}
impl fmt::Debug for EncodedCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EncodedCursor([redacted])")
    }
}

/// Authenticated payload only. Neither this type nor its key ID proves authority.
/// The buffer is wiped before its charge drops; it cannot be extracted uncharged.
pub struct OpenedCursor {
    bytes: Zeroizing<Vec<u8>>,
    _permit: OwnedPermit,
}
impl OpenedCursor {
    pub fn payload(&self) -> &[u8] {
        &self.bytes[HEADER..self.bytes.len() - TAG]
    }
    pub fn key_id(&self) -> [u8; 16] {
        self.bytes[1..17].try_into().expect("authenticated header")
    }
}
impl fmt::Debug for OpenedCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenedCursor([redacted])")
    }
}

/// Encrypt with the key selected by the caller's current Control decision.
/// A fresh OS nonce is mandatory; no public deterministic-nonce API exists.
pub fn seal(
    keys: &Keyring,
    key_id: [u8; 16],
    resource: &str,
    plaintext: &[u8],
    permit: OwnedPermit,
) -> Result<EncodedCursor, CursorError> {
    seal_with_entropy(keys, key_id, resource, plaintext, permit, |nonce| {
        getrandom::fill(nonce).map_err(|_| CursorError::Entropy)
    })
}

fn seal_with_entropy(
    keys: &Keyring,
    key_id: [u8; 16],
    resource: &str,
    plaintext: &[u8],
    permit: OwnedPermit,
    fill: impl FnOnce(&mut [u8; 24]) -> Result<(), CursorError>,
) -> Result<EncodedCursor, CursorError> {
    admission(&permit)?;
    if plaintext.len() > MAX_PLAINTEXT_BYTES {
        return Err(CursorError::Capacity);
    }
    let (aad, aad_len) = associated_data(keys, &key_id, resource)?;
    let key = keys.key(&key_id).ok_or(CursorError::Invalid)?;
    let mut nonce = [0; 24];
    fill(&mut nonce)?;
    let mut wire = buffer(HEADER + plaintext.len() + TAG)?;
    wire[0] = 1;
    wire[1..17].copy_from_slice(&key_id);
    wire[17..HEADER].copy_from_slice(&nonce);
    let end = HEADER + plaintext.len();
    wire[HEADER..end].copy_from_slice(plaintext);
    let cipher = XChaCha20Poly1305::new(key.into());
    let tag = cipher
        .encrypt_inout_detached(
            (&nonce).into(),
            &aad[..aad_len],
            (&mut wire[HEADER..end]).into(),
        )
        .map_err(|_| CursorError::Invalid)?;
    wire[end..].copy_from_slice(&tag);
    let mut encoded = buffer(MAX_ENCODED_BYTES)?;
    let len = URL_SAFE_NO_PAD
        .encode_slice(&wire, &mut encoded)
        .map_err(|_| CursorError::Capacity)?;
    encoded.truncate(len);
    Ok(EncodedCursor {
        bytes: encoded,
        _permit: permit,
    })
}

/// Open only after authenticating current access. Invalid tokens yield no bytes.
/// The returned key ID still needs Control's authoritative revocation/lifetime check.
pub fn open(
    keys: &Keyring,
    resource: &str,
    token: &str,
    permit: OwnedPermit,
) -> Result<OpenedCursor, CursorError> {
    admission(&permit)?;
    if token.len() > MAX_ENCODED_BYTES {
        return Err(CursorError::Capacity);
    }
    validate_resource(resource)?;
    if token.len() < 76 {
        return Err(CursorError::Invalid);
    }
    let mut wire = buffer(MAX_BINARY_BYTES)?;
    let len = URL_SAFE_NO_PAD
        .decode_slice(token, &mut wire)
        .map_err(|_| CursorError::Invalid)?;
    wire.truncate(len);
    if wire.len() < HEADER + TAG || wire[0] != 1 {
        return Err(CursorError::Invalid);
    }
    let key_id: [u8; 16] = wire[1..17].try_into().expect("bounded header");
    let key = keys.key(&key_id).ok_or(CursorError::Invalid)?;
    let (aad, aad_len) = associated_data(keys, &key_id, resource)?;
    let nonce: [u8; 24] = wire[17..HEADER].try_into().expect("bounded nonce");
    let end = wire.len() - TAG;
    let tag: Tag = wire[end..].try_into().expect("bounded tag");
    let cipher = XChaCha20Poly1305::new(key.into());
    cipher
        .decrypt_inout_detached(
            (&nonce).into(),
            &aad[..aad_len],
            (&mut wire[HEADER..end]).into(),
            &tag,
        )
        .map_err(|_| CursorError::Invalid)?;
    Ok(OpenedCursor {
        bytes: wire,
        _permit: permit,
    })
}

fn admission(permit: &OwnedPermit) -> Result<(), CursorError> {
    let units = permit.request().units();
    if units.data_bytes() < CURSOR_RESERVATION || units.items() < 1 {
        return Err(CursorError::Capacity);
    }
    Ok(())
}

fn buffer(len: usize) -> Result<Zeroizing<Vec<u8>>, CursorError> {
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(len)
        .map_err(|_| CursorError::Capacity)?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn validate_resource(resource: &str) -> Result<(), CursorError> {
    if resource.is_empty()
        || resource.len() > MAX_RESOURCE
        || !resource
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
    {
        return Err(CursorError::Invalid);
    }
    Ok(())
}

fn associated_data(
    keys: &Keyring,
    key_id: &[u8; 16],
    resource: &str,
) -> Result<([u8; AAD_PREFIX + MAX_RESOURCE], usize), CursorError> {
    validate_resource(resource)?;
    let mut aad = [0; AAD_PREFIX + MAX_RESOURCE];
    aad[..16].copy_from_slice(DOMAIN);
    aad[16..32].copy_from_slice(keys.dataset_bytes());
    // Stable identity, not a file checksum changed by retaining/adding other keys.
    aad[32..48].copy_from_slice(&keys.id());
    aad[48] = 1;
    aad[49..65].copy_from_slice(key_id);
    aad[65..AAD_PREFIX].copy_from_slice(&(resource.len() as u16).to_be_bytes());
    let len = AAD_PREFIX + resource.len();
    aad[AAD_PREFIX..len].copy_from_slice(resource.as_bytes());
    Ok((aad, len))
}

#[cfg(test)]
mod tests;
