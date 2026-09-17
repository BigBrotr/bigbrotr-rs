use crate::{EventId, PublicKey, Signature};
use serde::{Deserialize, Serialize};

/// Authored unsigned seconds, including zero and u64::MAX; never a calendar cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NostrTime(pub u64);

/// All seven signed-event fields, retaining order, duplicate tags and empty strings.
///
/// This value is structurally typed but unverified. Only the protocol verifier can
/// produce `VerifiedEvent`; ordinary deserialization cannot certify a signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedEvent {
    pub id: EventId,
    pub pubkey: PublicKey,
    pub created_at: NostrTime,
    pub kind: u16,
    pub tags: Vec<Vec<String>>,
    pub content: String,
    pub sig: Signature,
}

/// Shared authenticated fields, excluding the scope's signature representative.
/// As with SignedEvent, constructing this value does not verify it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBody {
    pub event_id: EventId,
    pub pubkey: PublicKey,
    pub created_at: NostrTime,
    pub kind: u16,
    pub tags: Vec<Vec<String>>,
    pub content: String,
}
impl From<SignedEvent> for EventBody {
    fn from(event: SignedEvent) -> Self {
        Self {
            event_id: event.id,
            pubkey: event.pubkey,
            created_at: event.created_at,
            kind: event.kind,
            tags: event.tags,
            content: event.content,
        }
    }
}
impl EventBody {
    /// Join only a representative authorized for the caller's selected scope.
    /// Signature verification and scope authorization belong to their owners.
    pub fn with_signature(self, sig: Signature) -> SignedEvent {
        SignedEvent {
            id: self.event_id,
            pubkey: self.pubkey,
            created_at: self.created_at,
            kind: self.kind,
            tags: self.tags,
            content: self.content,
            sig,
        }
    }
}
