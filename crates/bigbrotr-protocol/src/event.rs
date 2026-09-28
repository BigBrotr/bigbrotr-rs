use crate::bounds::{EventLimits, preflight};
use bigbrotr_model::{Digest256, EventId, SignedEvent};
use secp256k1::{XOnlyPublicKey, schnorr};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{fmt, io};

/// Failure reasons omit untrusted payloads and distinguish policy size limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventError {
    InvalidLimits,
    WireLimit,
    DepthLimit,
    NodeLimit,
    StringLimit,
    ArenaLimit,
    EventSizeLimit,
    InvalidJson,
    EmptyTag,
    EventIdMismatch,
    InvalidPublicKey,
    InvalidSignature,
}
impl EventError {
    pub fn is_limit(self) -> bool {
        matches!(
            self,
            Self::WireLimit
                | Self::DepthLimit
                | Self::NodeLimit
                | Self::StringLimit
                | Self::ArenaLimit
                | Self::EventSizeLimit
        )
    }
}
impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "invalid-parser-limits",
            Self::WireLimit => "wire-limit",
            Self::DepthLimit => "depth-limit",
            Self::NodeLimit => "node-limit",
            Self::StringLimit => "string-limit",
            Self::ArenaLimit => "parser-arena-limit",
            Self::EventSizeLimit => "event-size-limit",
            Self::InvalidJson => "invalid-json",
            Self::EmptyTag => "empty-tag",
            Self::EventIdMismatch => "event-id-mismatch",
            Self::InvalidPublicKey => "invalid-public-key",
            Self::InvalidSignature => "invalid-signature",
        })
    }
}
impl std::error::Error for EventError {}

/// Proof local to this verifier invocation, never a durable receipt or eligibility.
/// It has no Deserialize implementation or public unchecked constructor.
#[derive(Debug)]
pub struct VerifiedEvent {
    event: SignedEvent,
    normalized_bytes: usize,
}
impl VerifiedEvent {
    pub fn event(&self) -> &SignedEvent {
        &self.event
    }
    pub fn into_event(self) -> SignedEvent {
        self.event
    }
    pub fn normalized_bytes(&self) -> usize {
        self.normalized_bytes
    }
    /// Profile-1 payload identity for the value verified by this invocation.
    /// Scope eligibility, source envelopes and commit receipts remain separate.
    pub fn body_digest(&self) -> Digest256 {
        self.event.body_digest()
    }
}

struct SizeCounter {
    bytes: usize,
    maximum: usize,
}
impl io::Write for SizeCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|&size| size <= self.maximum)
            .ok_or_else(|| io::Error::other("event-size-limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct HashWriter(Sha256);
impl io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Write NIP-01's exact six-element preimage without materializing another event.
pub fn write_canonical(
    event: &SignedEvent,
    writer: impl io::Write,
) -> Result<(), serde_json::Error> {
    (
        0u8,
        event.pubkey,
        event.created_at,
        event.kind,
        &event.tags,
        &event.content,
    )
        .serialize(&mut serde_json::Serializer::new(writer))
}

pub fn verify_json(bytes: &[u8], limits: EventLimits) -> Result<VerifiedEvent, EventError> {
    let event = decode_event_json(bytes, limits)?;
    // Keep the wire path's structural error ordering after bounded parsing.
    if event.tags.iter().any(Vec::is_empty) {
        return Err(EventError::EmptyTag);
    }
    verify_owned(event, limits.event_bytes)
}

/// Decode the complete signed fields under the existing wire/parser bounds.
/// This grants no ID, signature, normalized-size or semantic validity. Invalid
/// candidates stay available for an attributed rejection by the real consumer.
/// The caller must reserve `parse_bytes` before decoding and retain its grant
/// for the owned buffers; the input wire bytes require a separate reservation.
pub fn decode_event_json(bytes: &[u8], limits: EventLimits) -> Result<SignedEvent, EventError> {
    preflight(bytes, limits)?;
    // IgnoredAny skips unknown string values without checking their raw UTF-8.
    // An ignored extension must not turn non-UTF-8 input into accepted JSON text.
    std::str::from_utf8(bytes).map_err(|_| EventError::InvalidJson)?;
    // Serde structs also accept positional sequences; Nostr's wire is an object.
    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(EventError::InvalidJson);
    }
    // Derive rejects duplicate/missing known fields. Unknown top-level keys are
    // bounded above and ignored, as NIP-01 excludes them from signed content.
    serde_json::from_slice(bytes).map_err(|_| EventError::InvalidJson)
}

/// Verify an already owned event without serializing and parsing another buffer.
/// The positive limit bounds normalized JSON bytes and verification work. It is
/// not a wire/parser limit or a memory grant: the caller must already own and
/// account for the event's buffers (including spare capacity) until this returns.
/// A typed value, or a previously seen event ID, never bypasses ID/signature checks.
pub fn verify_owned(
    event: SignedEvent,
    max_event_bytes: usize,
) -> Result<VerifiedEvent, EventError> {
    if max_event_bytes == 0 {
        return Err(EventError::InvalidLimits);
    }
    // Before walking or escaping strings, reject an allocation-free lower bound
    // on their encoded size. Count even empty arrays/strings: a byte limit alone
    // on content cannot bound a typed input containing millions of empty tags.
    let mut remaining = max_event_bytes
        .checked_sub(event.content.len())
        .and_then(|n| n.checked_sub(event.tags.len()))
        .ok_or(EventError::EventSizeLimit)?;
    for tag in &event.tags {
        remaining = remaining
            .checked_sub(tag.len())
            .ok_or(EventError::EventSizeLimit)?;
        if tag.is_empty() {
            return Err(EventError::EmptyTag);
        }
        for value in tag {
            remaining = remaining
                .checked_sub(value.len())
                .ok_or(EventError::EventSizeLimit)?;
        }
    }
    let mut size = SizeCounter {
        bytes: 0,
        maximum: max_event_bytes,
    };
    serde_json::to_writer(&mut size, &event).map_err(|_| EventError::EventSizeLimit)?;
    let mut hash = HashWriter(Sha256::new());
    write_canonical(&event, &mut hash).map_err(|_| EventError::InvalidJson)?;
    let id: [u8; 32] = hash.0.finalize().into();
    if event.id != EventId::from_bytes(id) {
        return Err(EventError::EventIdMismatch);
    }
    let public = XOnlyPublicKey::from_byte_array(*event.pubkey.as_bytes())
        .map_err(|_| EventError::InvalidPublicKey)?;
    let signature = schnorr::Signature::from_byte_array(*event.sig.as_bytes());
    schnorr::verify(&signature, &id, &public).map_err(|_| EventError::InvalidSignature)?;
    Ok(VerifiedEvent {
        event,
        normalized_bytes: size.bytes,
    })
}
