//! Collection intent and membership. No signature, source or acquisition authority.
use crate::{EventId, NostrTime, PublicKey, SignedEvent};
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::{collections::BTreeMap, fmt};

pub const MAX_FILTERS: usize = 16;
pub const MAX_VALUES_PER_FIELD: usize = 1024;
pub const MAX_TOTAL_VALUES: usize = 16_384;
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;
pub const MAX_PREIMAGE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorError {
    EmptyUnion,
    EmptyList,
    InvalidTag,
    InvalidTagValue,
    FilterLimit,
    ValueLimit,
    TextLimit,
    CanonicalLimit,
}
impl fmt::Display for SelectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyUnion => "empty-selector-union",
            Self::EmptyList => "empty-selector-list",
            Self::InvalidTag => "invalid-selector-tag",
            Self::InvalidTagValue => "invalid-selector-tag-value",
            Self::FilterLimit => "selector-filter-limit",
            Self::ValueLimit => "selector-value-limit",
            Self::TextLimit => "selector-text-limit",
            Self::CanonicalLimit => "selector-canonical-limit",
        })
    }
}
impl std::error::Error for SelectorError {}

/// Builder input, not normalized or verified. Callers already own these buffers.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct FilterFields {
    pub ids: Option<Vec<EventId>>,
    pub authors: Option<Vec<PublicKey>>,
    pub kinds: Option<Vec<u16>>,
    pub tags: BTreeMap<char, Vec<String>>,
    pub since: Option<NostrTime>,
    pub until: Option<NostrTime>,
}

/// An immutable normalized conjunction. No unchecked Deserialize implementation.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Filter(FilterFields);

fn list<T>(values: &[T]) -> Result<(), SelectorError> {
    if values.is_empty() {
        return Err(SelectorError::EmptyList);
    }
    if values.len() > MAX_VALUES_PER_FIELD {
        return Err(SelectorError::ValueLimit);
    }
    Ok(())
}

fn set<T: Ord>(values: &mut Option<Vec<T>>) {
    if let Some(values) = values {
        values.sort_unstable();
        values.dedup();
    }
}

impl Filter {
    pub fn new(mut fields: FilterFields) -> Result<Self, SelectorError> {
        if let Some(values) = &fields.ids {
            list(values)?;
        }
        if let Some(values) = &fields.authors {
            list(values)?;
        }
        if let Some(values) = &fields.kinds {
            list(values)?;
        }
        for (key, values) in &fields.tags {
            if !key.is_ascii_alphabetic() {
                return Err(SelectorError::InvalidTag);
            }
            list(values)?;
            if matches!(key, 'e' | 'p')
                && values.iter().any(|value| value.parse::<EventId>().is_err())
            {
                return Err(SelectorError::InvalidTagValue);
            }
        }
        measure(&fields)?;
        set(&mut fields.ids);
        set(&mut fields.authors);
        set(&mut fields.kinds);
        for values in fields.tags.values_mut() {
            values.sort_unstable();
            values.dedup();
        }
        Ok(Self(fields))
    }

    pub fn fields(&self) -> &FilterFields {
        &self.0
    }

    /// Structural membership only. A matching unverified event still needs crypto.
    pub fn matches(&self, event: &SignedEvent) -> bool {
        let f = &self.0;
        fn contains<T: Ord>(values: &Option<Vec<T>>, value: &T) -> bool {
            values
                .as_ref()
                .is_none_or(|values| values.binary_search(value).is_ok())
        }
        contains(&f.ids, &event.id)
            && contains(&f.authors, &event.pubkey)
            && contains(&f.kinds, &event.kind)
            && f.since.is_none_or(|since| event.created_at >= since)
            && f.until.is_none_or(|until| event.created_at <= until)
            && f.tags.iter().all(|(key, values)| {
                event.tags.iter().any(|tag| {
                    tag.len() >= 2
                        && tag[0].as_bytes() == [*key as u8]
                        && values.binary_search(&tag[1]).is_ok()
                })
            })
    }
}

fn measure(fields: &FilterFields) -> Result<(usize, usize), SelectorError> {
    let mut values = fields.ids.as_ref().map_or(0, Vec::len)
        + fields.authors.as_ref().map_or(0, Vec::len)
        + fields.kinds.as_ref().map_or(0, Vec::len);
    let mut text = 0usize;
    for items in fields.tags.values() {
        values = values
            .checked_add(items.len())
            .ok_or(SelectorError::ValueLimit)?;
        for item in items {
            text = text
                .checked_add(item.len())
                .ok_or(SelectorError::TextLimit)?;
        }
    }
    if values > MAX_TOTAL_VALUES {
        return Err(SelectorError::ValueLimit);
    }
    if text > MAX_TEXT_BYTES {
        return Err(SelectorError::TextLimit);
    }
    Ok((values, text))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EphemeralPolicy {
    Retain,
    Drop,
}
impl EphemeralPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Retain => "retain",
            Self::Drop => "drop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    Included,
    OutsideSelector,
    EphemeralExcluded,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CollectionSelector {
    filters: Vec<Filter>,
    history_since: NostrTime,
    ephemeral: EphemeralPolicy,
}
impl CollectionSelector {
    pub fn new(
        mut filters: Vec<Filter>,
        history_since: NostrTime,
        ephemeral: EphemeralPolicy,
    ) -> Result<Self, SelectorError> {
        if filters.is_empty() {
            return Err(SelectorError::EmptyUnion);
        }
        if filters.len() > MAX_FILTERS {
            return Err(SelectorError::FilterLimit);
        }
        let (mut values, mut text) = (0usize, 0usize);
        for filter in &filters {
            let (count, bytes) = measure(filter.fields())?;
            values = values.checked_add(count).ok_or(SelectorError::ValueLimit)?;
            text = text.checked_add(bytes).ok_or(SelectorError::TextLimit)?;
            if values > MAX_TOTAL_VALUES {
                return Err(SelectorError::ValueLimit);
            }
            if text > MAX_TEXT_BYTES {
                return Err(SelectorError::TextLimit);
            }
        }
        filters.sort_unstable();
        filters.dedup();
        let value = Self {
            filters,
            history_since,
            ephemeral,
        };
        crate::fingerprint::selector::check_size(&value)?;
        Ok(value)
    }

    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }
    pub fn history_since(&self) -> NostrTime {
        self.history_since
    }
    pub fn ephemeral(&self) -> EphemeralPolicy {
        self.ephemeral
    }

    pub fn classify(&self, event: &SignedEvent) -> Membership {
        if event.created_at < self.history_since
            || !self.filters.iter().any(|filter| filter.matches(event))
        {
            Membership::OutsideSelector
        } else if self.ephemeral == EphemeralPolicy::Drop && (20000..30000).contains(&event.kind) {
            Membership::EphemeralExcluded
        } else {
            Membership::Included
        }
    }
}

impl Serialize for Filter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = self.fields();
        let mut map = serializer.serialize_map(None)?;
        if let Some(ids) = &fields.ids {
            map.serialize_entry("ids", ids)?;
        }
        if let Some(authors) = &fields.authors {
            map.serialize_entry("authors", authors)?;
        }
        if let Some(kinds) = &fields.kinds {
            map.serialize_entry("kinds", kinds)?;
        }
        for (key, values) in &fields.tags {
            let bytes = [b'#', *key as u8];
            let key = std::str::from_utf8(&bytes).expect("validated ASCII tag key");
            map.serialize_entry(key, values)?;
        }
        if let Some(since) = fields.since {
            map.serialize_entry("since", &since)?;
        }
        if let Some(until) = fields.until {
            map.serialize_entry("until", &until)?;
        }
        map.end()
    }
}

impl Serialize for CollectionSelector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("version", &1u16)?;
        map.serialize_entry("filters", self.filters())?;
        map.serialize_entry("history_since", &self.history_since())?;
        map.serialize_entry("ephemeral", self.ephemeral().as_str())?;
        map.end()
    }
}
