//! Bounded collection-intent JSON. A decoded selector grants no work or visibility.
use crate::{EventError, EventLimits, bounds::preflight_with_node_bytes};
use bigbrotr_model::{
    NostrTime,
    acquisition::{
        CollectionSelector, EphemeralPolicy, Filter, FilterFields, MAX_FILTERS, SelectorError,
    },
};
use serde::{Deserialize, Deserializer, de};
use std::{fmt, io};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    EncodedLimit,
    WorkingLimit,
    Allocation,
}
impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EncodedLimit => "selector-document-limit",
            Self::WorkingLimit => "selector-replay-working-limit",
            Self::Allocation => "selector-document-allocation-unavailable",
        })
    }
}
impl std::error::Error for EncodeError {}

/// Normalized profile-1 bytes for exact persistent comparison. Counts JSON
/// escaping before allocating output and ensures the result fits replay bounds.
/// The caller retains grants for the owned selector and resulting document.
pub fn encode_selector(selector: &CollectionSelector) -> Result<Vec<u8>, EncodeError> {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= EventLimits::default().wire_bytes)
                .ok_or_else(|| io::Error::other("selector document limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, selector).map_err(|_| EncodeError::EncodedLimit)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(count.0)
        .map_err(|_| EncodeError::Allocation)?;
    serde_json::to_writer(&mut output, selector)
        .expect("checked typed selector and reserved output");
    preflight_with_node_bytes(&output, EventLimits::default(), 256)
        .map_err(|_| EncodeError::WorkingLimit)?;
    Ok(output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    Bounds(EventError),
    InvalidJson,
    Unsupported,
    Profile(SelectorError),
}
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bounds(error) => error.fmt(f),
            Self::InvalidJson => f.write_str("invalid-selector-json"),
            Self::Unsupported => f.write_str("unsupported-selector-profile"),
            Self::Profile(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for DecodeError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u16,
    filters: Vec<RawFilter>,
    history_since: NostrTime,
    ephemeral: String,
}

/// The caller reserves the 8MiB parser grant before entry and accounts for the
/// retained selector after return. The 256-byte node charge covers filter records,
/// list growth, BTreeMap nodes and the bounded normalization output together.
pub fn decode_selector(input: &[u8]) -> Result<CollectionSelector, DecodeError> {
    preflight_with_node_bytes(input, EventLimits::default(), 256).map_err(DecodeError::Bounds)?;
    std::str::from_utf8(input).map_err(|_| DecodeError::InvalidJson)?;
    // Serde structs also accept positional arrays; this profile is an object.
    if input.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(DecodeError::InvalidJson);
    }
    let document: Document = serde_json::from_slice(input).map_err(|_| DecodeError::InvalidJson)?;
    if document.version != 1 {
        return Err(DecodeError::Unsupported);
    }
    let ephemeral = match document.ephemeral.as_str() {
        "retain" => EphemeralPolicy::Retain,
        "drop" => EphemeralPolicy::Drop,
        _ => return Err(DecodeError::Unsupported),
    };
    // Bound a second vector before transforming parser-owned records. The whole
    // first parse was already charged, even for invalid or excessive unions.
    if document.filters.len() > MAX_FILTERS {
        return Err(DecodeError::Profile(SelectorError::FilterLimit));
    }
    let filters = document
        .filters
        .into_iter()
        .map(|filter| Filter::new(filter.0))
        .collect::<Result<Vec<_>, _>>()
        .map_err(DecodeError::Profile)?;
    CollectionSelector::new(filters, document.history_since, ephemeral)
        .map_err(DecodeError::Profile)
}

struct RawFilter(FilterFields);

fn singleton<'de, M: de::MapAccess<'de>, T: Deserialize<'de>>(
    map: &mut M,
    target: &mut Option<T>,
) -> Result<(), M::Error> {
    if target.is_some() {
        return Err(de::Error::custom("duplicate selector field"));
    }
    *target = Some(map.next_value()?);
    Ok(())
}

impl<'de> Deserialize<'de> for RawFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = FilterFields;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a closed collection filter object")
            }
            fn visit_map<M: de::MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = FilterFields::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "ids" => singleton(&mut map, &mut fields.ids)?,
                        "authors" => singleton(&mut map, &mut fields.authors)?,
                        "kinds" => singleton(&mut map, &mut fields.kinds)?,
                        "since" => singleton(&mut map, &mut fields.since)?,
                        "until" => singleton(&mut map, &mut fields.until)?,
                        _ if key.len() == 2
                            && key.as_bytes()[0] == b'#'
                            && key.as_bytes()[1].is_ascii_alphabetic() =>
                        {
                            let key = key.as_bytes()[1] as char;
                            if fields.tags.contains_key(&key) {
                                return Err(de::Error::custom("duplicate selector tag"));
                            }
                            fields.tags.insert(key, map.next_value()?);
                        }
                        _ => return Err(de::Error::custom("unsupported selector field")),
                    }
                }
                Ok(fields)
            }
        }
        deserializer.deserialize_map(Visitor).map(Self)
    }
}
