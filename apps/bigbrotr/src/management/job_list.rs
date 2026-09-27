//! Bounded operational list bindings. Decoding never grants current authority.
use super::jobs::JobObservation;
use crate::{export::execution::ExportState, management::sessions::SessionError};
use crate::{read::cursor::EncodedCursor, runtime::OwnedPermit};
use bigbrotr_model::{JobId, PrincipalId};
use zeroize::Zeroizing;

/// 201 bounded SQL rows at1KiB each,128KiB output, and decoding/serialization
/// headroom. Codec scratch has its own16KiB reservation; connection bytes remain
/// charged by the existing HTTP owner independently.
pub const PAGE_RESERVATION: u64 = 384 * 1024;
const RESPONSE_BYTES: usize = 128 * 1024;

pub(crate) struct Page {
    pub items: Vec<JobObservation>,
    pub next_cursor: Option<EncodedCursor>,
    pub effective_page_size: u16,
    pub observed_at_us: i64,
    pub permit: OwnedPermit,
}
/// The actual socket buffer retains its reservation and wipes the private token
/// when the final Hyper buffer owner drops. No uncharged copy is extracted.
pub struct PageBody {
    bytes: Zeroizing<Vec<u8>>,
    _permit: OwnedPermit,
}
impl AsRef<[u8]> for PageBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Page {
    pub fn encode(self) -> Result<PageBody, SessionError> {
        #[derive(serde::Serialize)]
        struct Payload<'a> {
            format: &'static str,
            items: &'a [JobObservation],
            effective_page_size: u16,
            next_cursor: Option<&'a str>,
            observed_at_us: String,
            authority: &'static str,
        }
        struct Buffer(Zeroizing<Vec<u8>>);
        impl std::io::Write for Buffer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > RESPONSE_BYTES.saturating_sub(self.0.len()) {
                    return Err(std::io::Error::other("job page byte limit"));
                }
                self.0.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut bytes = Buffer(Zeroizing::new(Vec::new()));
        bytes
            .0
            .try_reserve_exact(RESPONSE_BYTES)
            .map_err(|_| SessionError::Capacity)?;
        serde_json::to_writer(
            &mut bytes,
            &Payload {
                format: "bigbrotr-job-list/1",
                items: &self.items,
                effective_page_size: self.effective_page_size,
                next_cursor: self.next_cursor.as_ref().map(EncodedCursor::as_str),
                observed_at_us: self.observed_at_us.to_string(),
                authority: "observation-only",
            },
        )
        .map_err(|_| SessionError::Capacity)?;
        Ok(PageBody {
            bytes: bytes.0,
            _permit: self.permit,
        })
    }
}

#[derive(Clone, Copy)]
pub struct CursorLifetime {
    pub ttl_us: u64,
    pub skew_us: u64,
}

pub const RESOURCE: &str = "management.jobs/1";
pub const MAX_PAGE_SIZE: u16 = 200;
pub const MAX_SKEW_US: i64 = 30_000_000;
const PLAINTEXT_BYTES: usize = 103;
const STATES: [ExportState; 10] = [
    ExportState::Accepted,
    ExportState::Queued,
    ExportState::Running,
    ExportState::Paused,
    ExportState::CancelRequested,
    ExportState::Succeeded,
    ExportState::Partial,
    ExportState::Failed,
    ExportState::Cancelled,
    ExportState::Skipped,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobListQuery {
    state: Option<ExportState>,
    created_from_us: Option<i64>,
    created_before_us: Option<i64>,
    page_size: u16,
}
impl JobListQuery {
    pub fn new(
        state: Option<ExportState>,
        created_from_us: Option<i64>,
        created_before_us: Option<i64>,
        page_size: u16,
    ) -> Result<Self, SessionError> {
        if page_size == 0
            || created_from_us
                .zip(created_before_us)
                .is_some_and(|(from, before)| from >= before)
        {
            return Err(SessionError::InvalidInput);
        }
        Ok(Self {
            state,
            created_from_us,
            created_before_us,
            page_size: page_size.min(MAX_PAGE_SIZE),
        })
    }
    pub const fn state(self) -> Option<ExportState> {
        self.state
    }
    pub const fn created_from_us(self) -> Option<i64> {
        self.created_from_us
    }
    pub const fn created_before_us(self) -> Option<i64> {
        self.created_before_us
    }
    pub const fn page_size(self) -> u16 {
        self.page_size
    }
    fn contains_time(self, time: i64) -> bool {
        self.created_from_us.is_none_or(|from| time >= from)
            && self.created_before_us.is_none_or(|before| time < before)
    }
}

/// The transport's request reservation owns this bounded token copy through SQL.
/// Never log it or substitute decoding for session/status.read authentication.
pub struct JobListRequest {
    pub query: JobListQuery,
    pub cursor: Option<Zeroizing<String>>,
}

pub fn parse_request(query: &str) -> Result<JobListRequest, SessionError> {
    if query.len() > crate::read::cursor::MAX_ENCODED_BYTES + 256 {
        return Err(SessionError::InvalidInput);
    }
    let (mut state, mut from, mut before, mut page, mut cursor) = (None, None, None, 50, None);
    let mut seen = 0u8;
    if !query.is_empty() {
        for pair in query.split('&') {
            let (name, value) = pair
                .split_once('=')
                .filter(|(_, v)| !v.is_empty())
                .ok_or(SessionError::InvalidInput)?;
            let bit = match name {
                "kind" => 1,
                "state" => 2,
                "created_from_us" => 4,
                "created_before_us" => 8,
                "page_size" => 16,
                "cursor" => 32,
                _ => return Err(SessionError::InvalidInput),
            };
            if seen & bit != 0 {
                return Err(SessionError::InvalidInput);
            }
            seen |= bit;
            match name {
                "kind" if value == "export" => {}
                "state" => {
                    state = Some(ExportState::from_code(value).ok_or(SessionError::InvalidInput)?)
                }
                "created_from_us" => from = Some(decimal(value)?),
                "created_before_us" => before = Some(decimal(value)?),
                "page_size" => page = decimal(value)?,
                "cursor"
                    if value.len() <= crate::read::cursor::MAX_ENCODED_BYTES
                        && value
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')) =>
                {
                    cursor = Some(Zeroizing::new(value.to_owned()));
                }
                _ => return Err(SessionError::InvalidInput),
            }
        }
    }
    Ok(JobListRequest {
        query: JobListQuery::new(state, from, before, page)?,
        cursor,
    })
}
fn decimal<T: std::str::FromStr + std::fmt::Display>(text: &str) -> Result<T, SessionError> {
    text.parse::<T>()
        .ok()
        .filter(|value| value.to_string() == text)
        .ok_or(SessionError::InvalidInput)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Position {
    pub created_at_us: i64,
    pub job: JobId,
}
pub(crate) struct Continuation {
    pub actor: PrincipalId,
    pub policy_revision: u64,
    pub grant_revision: u64,
    pub query: JobListQuery,
    pub admission_cut: u64,
    pub position: Position,
    pub issued_at_us: i64,
    pub expires_at_us: i64,
}
impl Continuation {
    pub fn encode(&self) -> Zeroizing<[u8; PLAINTEXT_BYTES]> {
        // Fixed offsets follow management.jobs/1; no generic serialization tree
        // or caller-controlled length exists inside this confidential payload.
        let mut out = Zeroizing::new([0; PLAINTEXT_BYTES]);
        out[0] = 1;
        out[1] = 1;
        out[2..18].copy_from_slice(self.actor.as_bytes());
        out[18..26].copy_from_slice(&self.policy_revision.to_be_bytes());
        out[26..34].copy_from_slice(&self.grant_revision.to_be_bytes());
        out[34] = self.query.state.map_or(0, |state| {
            (STATES
                .iter()
                .position(|s| *s == state)
                .expect("closed state")
                + 1) as u8
        });
        if let Some(from) = self.query.created_from_us {
            out[35] = 1;
            out[36..44].copy_from_slice(&from.to_be_bytes());
        }
        if let Some(before) = self.query.created_before_us {
            out[44] = 1;
            out[45..53].copy_from_slice(&before.to_be_bytes());
        }
        out[53..55].copy_from_slice(&self.query.page_size.to_be_bytes());
        out[55..63].copy_from_slice(&self.admission_cut.to_be_bytes());
        out[63..71].copy_from_slice(&self.position.created_at_us.to_be_bytes());
        out[71..87].copy_from_slice(self.position.job.as_bytes());
        out[87..95].copy_from_slice(&self.issued_at_us.to_be_bytes());
        out[95..103].copy_from_slice(&self.expires_at_us.to_be_bytes());
        out
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, SessionError> {
        let invalid = SessionError::CursorInvalid;
        if bytes.len() != PLAINTEXT_BYTES || bytes[..2] != [1, 1] {
            return Err(invalid);
        }
        let unsigned =
            |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("fixed payload"));
        let signed =
            |at: usize| i64::from_be_bytes(bytes[at..at + 8].try_into().expect("fixed payload"));
        let optional = |at: usize| match bytes[at] {
            0 if signed(at + 1) == 0 => Ok(None),
            1 => Ok(Some(signed(at + 1))),
            _ => Err(invalid),
        };
        let state = match bytes[34] {
            0 => None,
            code => Some(*STATES.get(usize::from(code) - 1).ok_or(invalid)?),
        };
        let page = u16::from_be_bytes(bytes[53..55].try_into().expect("fixed payload"));
        if page > MAX_PAGE_SIZE {
            return Err(invalid);
        }
        let result = Self {
            actor: PrincipalId::try_from(
                <[u8; 16]>::try_from(&bytes[2..18]).expect("fixed payload"),
            )
            .map_err(|_| invalid)?,
            policy_revision: unsigned(18),
            grant_revision: unsigned(26),
            query: JobListQuery::new(state, optional(35)?, optional(44)?, page)
                .map_err(|_| invalid)?,
            admission_cut: unsigned(55),
            position: Position {
                created_at_us: signed(63),
                job: JobId::try_from(<[u8; 16]>::try_from(&bytes[71..87]).expect("fixed payload"))
                    .map_err(|_| invalid)?,
            },
            issued_at_us: signed(87),
            expires_at_us: signed(95),
        };
        if result.policy_revision == 0
            || result.grant_revision == 0
            || result.admission_cut == 0
            || result.issued_at_us >= result.expires_at_us
            || result.expires_at_us.checked_add(MAX_SKEW_US).is_none()
            || !result.query.contains_time(result.position.created_at_us)
        {
            return Err(invalid);
        }
        Ok(result)
    }
    pub fn validate(
        &self,
        actor: PrincipalId,
        policy: u64,
        grants: u64,
        query: JobListQuery,
        now: i64,
        skew: i64,
    ) -> Result<(), SessionError> {
        if self.actor != actor
            || self.policy_revision != policy
            || self.grant_revision != grants
            || self.query != query
        {
            return Err(SessionError::CursorIncompatible);
        }
        if !(0..=MAX_SKEW_US).contains(&skew)
            || i128::from(now) + i128::from(skew) < i128::from(self.issued_at_us)
        {
            return Err(SessionError::CursorInvalid);
        }
        if i128::from(now) >= i128::from(self.expires_at_us) + i128::from(skew) {
            return Err(SessionError::CursorExpired);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
