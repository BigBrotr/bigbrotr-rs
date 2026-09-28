//! Encode the exact borrowed predicate; generated page limits are not scope identity.
use bigbrotr_model::acquisition::{Filter, MAX_FILTERS};
use serde::{Serialize, Serializer, ser::SerializeSeq};
use std::{fmt, io};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestEncodeError {
    InvalidRequest,
    EncodedLimit,
    Allocation,
}
impl fmt::Display for RequestEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRequest => "invalid-relay-request",
            Self::EncodedLimit => "relay-request-encoded-limit",
            Self::Allocation => "relay-request-allocation-unavailable",
        })
    }
}
impl std::error::Error for RequestEncodeError {}

#[derive(Serialize)]
struct LimitedFilter<'a> {
    #[serde(flatten)]
    filter: &'a Filter,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
}
struct Request<'a> {
    subscription: &'a str,
    filters: &'a [Filter],
    limit: Option<u64>,
}
impl Serialize for Request<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.filters.len() + 2))?;
        seq.serialize_element("REQ")?;
        seq.serialize_element(self.subscription)?;
        for filter in self.filters {
            seq.serialize_element(&LimitedFilter {
                filter,
                limit: self.limit,
            })?;
        }
        seq.end()
    }
}

/// Measure escaping before allocation. `max_bytes` bounds JSON payload bytes;
/// the transport separately bounds framing. The caller retains a grant for the
/// filters and encoded command until its send completes. None omits `limit`;
/// Some(0) explicitly requests live delivery and never changes membership.
pub fn encode_request(
    subscription: &str,
    filters: &[Filter],
    limit: Option<u64>,
    max_bytes: usize,
) -> Result<String, RequestEncodeError> {
    if subscription.is_empty()
        || subscription.chars().take(65).count() > 64
        || filters.is_empty()
        || filters.len() > MAX_FILTERS
    {
        return Err(RequestEncodeError::InvalidRequest);
    }
    struct Count {
        bytes: usize,
        limit: usize,
    }
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| io::Error::other("request limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let request = Request {
        subscription,
        filters,
        limit,
    };
    let mut count = Count {
        bytes: 0,
        limit: max_bytes,
    };
    serde_json::to_writer(&mut count, &request).map_err(|_| RequestEncodeError::EncodedLimit)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(count.bytes)
        .map_err(|_| RequestEncodeError::Allocation)?;
    serde_json::to_writer(&mut output, &request).expect("measured immutable request");
    Ok(String::from_utf8(output).expect("JSON encoder emits UTF-8"))
}
