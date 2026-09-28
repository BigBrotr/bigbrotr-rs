//! Preserving relay envelopes. Parsing grants no request, source or admission authority.
mod request;
pub use request::{RequestEncodeError, encode_request};

use crate::{EventError, EventLimits, bounds::preflight};
use bigbrotr_model::EventId;
use serde::{Deserialize, Deserializer, de};
use serde_json::value::RawValue;
use std::{borrow::Cow, fmt};

/// Original UTF-8 input, including future trailing fields, and its known fields.
/// No Debug implementation: these bytes may contain private event/relay data.
pub struct RelayEnvelope<'a> {
    json: &'a str,
    message: RelayMessage<'a>,
}

impl<'a> RelayEnvelope<'a> {
    pub fn json(&self) -> &'a str {
        self.json
    }

    pub fn message(&self) -> &RelayMessage<'a> {
        &self.message
    }
}

/// Correlation data, not proof of an active generation or a successful operation.
/// EVENT's value is deliberately unverified: pass its original bytes to verify_json.
/// Decoding must use the bounded entrypoint; there is no unbounded Deserialize path.
///
/// ```compile_fail
/// use bigbrotr_protocol::wire::RelayMessage;
/// let _ = serde_json::from_str::<RelayMessage<'_>>(r#"["EOSE","r",[]]"#);
/// ```
pub enum RelayMessage<'a> {
    Event {
        subscription: Cow<'a, str>,
        json: &'a RawValue,
    },
    Eose {
        subscription: Cow<'a, str>,
        /// None and an explicit empty hint array remain distinct. Order, unknown
        /// strings, duplicates and contradictions survive for the scoped consumer.
        hints: Option<Vec<Cow<'a, str>>>,
    },
    Closed {
        subscription: Cow<'a, str>,
        message: Cow<'a, str>,
    },
    Auth {
        challenge: Cow<'a, str>,
    },
    Notice {
        message: Cow<'a, str>,
    },
    Ok {
        event_id: EventId,
        accepted: bool,
        message: Cow<'a, str>,
    },
    Unknown {
        kind: Cow<'a, str>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    Bounds(EventError),
    InvalidEnvelope,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bounds(error) => error.fmt(f),
            Self::InvalidEnvelope => f.write_str("invalid-relay-envelope"),
        }
    }
}

impl std::error::Error for WireError {}

/// Bounds the complete message before any decoded allocations, including ignored
/// extensions. The caller retains its grant for wire, decoded fields and subsequent
/// verification together. event_bytes applies later, when verifying EVENT's value.
pub fn decode_relay(input: &[u8], limits: EventLimits) -> Result<RelayEnvelope<'_>, WireError> {
    preflight(input, limits).map_err(WireError::Bounds)?;
    // Serde may skip ignored raw string bytes; validate UTF-8 for all positions.
    let json = std::str::from_utf8(input).map_err(|_| WireError::InvalidEnvelope)?;
    let Decoded(message) = serde_json::from_str(json).map_err(|_| WireError::InvalidEnvelope)?;
    Ok(RelayEnvelope { json, message })
}

// Derive's borrowing Cow visitor borrows plain strings and owns decoded escapes.
// Direct Cow deserialization would always own them. Hint strings use that simpler
// owned path; the preflight's string and per-node budget covers both representations.
#[derive(Deserialize)]
struct Text<'a>(#[serde(borrow)] Cow<'a, str>);

fn required<'de, T: Deserialize<'de>, S: de::SeqAccess<'de>>(
    fields: &mut S,
) -> Result<T, S::Error> {
    fields
        .next_element()?
        .ok_or_else(|| de::Error::custom("missing relay field"))
}

fn subscription<'de, S: de::SeqAccess<'de>>(fields: &mut S) -> Result<Cow<'de, str>, S::Error> {
    let Text(value) = required(fields)?;
    // NIP-01 says characters. Locally generated IDs can use a stricter ASCII
    // alphabet, but a peer's well-formed non-ASCII identifier is not malformed.
    if value.is_empty() || value.chars().take(65).count() > 64 {
        return Err(de::Error::custom("invalid subscription identifier"));
    }
    Ok(value)
}

struct Decoded<'a>(RelayMessage<'a>);

impl<'de> Deserialize<'de> for Decoded<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EnvelopeVisitor;
        impl<'de> de::Visitor<'de> for EnvelopeVisitor {
            type Value = RelayMessage<'de>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a relay message array")
            }

            fn visit_seq<S: de::SeqAccess<'de>>(
                self,
                mut fields: S,
            ) -> Result<Self::Value, S::Error> {
                let Text(kind) = required(&mut fields)?;
                let message = match kind.as_ref() {
                    "EVENT" => RelayMessage::Event {
                        subscription: subscription(&mut fields)?,
                        json: required(&mut fields)?,
                    },
                    "EOSE" => RelayMessage::Eose {
                        subscription: subscription(&mut fields)?,
                        // Absent is allowed. Present null or a non-string member
                        // is an error, never silently downgraded to plain EOSE.
                        hints: fields.next_element()?,
                    },
                    "CLOSED" => RelayMessage::Closed {
                        subscription: subscription(&mut fields)?,
                        message: required::<Text<'de>, _>(&mut fields)?.0,
                    },
                    "AUTH" => RelayMessage::Auth {
                        challenge: required::<Text<'de>, _>(&mut fields)?.0,
                    },
                    "NOTICE" => RelayMessage::Notice {
                        message: required::<Text<'de>, _>(&mut fields)?.0,
                    },
                    "OK" => RelayMessage::Ok {
                        event_id: required(&mut fields)?,
                        accepted: required(&mut fields)?,
                        message: required::<Text<'de>, _>(&mut fields)?.0,
                    },
                    _ => RelayMessage::Unknown { kind },
                };
                // Forward-compatible fields remain in RelayEnvelope::json. Drain
                // them to validate grammar; from_str also rejects trailing documents.
                while fields.next_element::<de::IgnoredAny>()?.is_some() {}
                Ok(message)
            }
        }
        deserializer.deserialize_seq(EnvelopeVisitor).map(Self)
    }
}
