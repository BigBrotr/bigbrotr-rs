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
