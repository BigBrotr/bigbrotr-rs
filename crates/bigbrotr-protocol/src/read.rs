//! Shared HTTP request codec for the initial exact-ID history consumer.
use bigbrotr_model::{
    DictionaryKind, EventId,
    read::{QueryRequest, ReadMode, ReadResource},
};

pub const MAX_REQUEST_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryError {
    Invalid,
    Limit,
    Unsupported,
}

pub fn decode_post(bytes: &[u8]) -> Result<QueryRequest, QueryError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(QueryError::Limit);
    }
    let request = serde_json::from_slice(bytes).map_err(|_| QueryError::Invalid)?;
    validate(&request)?;
    Ok(request)
}

/// Percent decoding is not form decoding: a literal '+' remains '+'. Each key
/// appears once, and an unknown field cannot silently remove a constraint.
pub fn decode_get(query: &str) -> Result<QueryRequest, QueryError> {
    if query.len() > MAX_REQUEST_BYTES {
        return Err(QueryError::Limit);
    }
    let mut fields = std::collections::BTreeMap::new();
    for parameter in query.split('&') {
        let (key, value) = parameter.split_once('=').ok_or(QueryError::Invalid)?;
        let key = percent(key)?;
        if !matches!(
            key.as_str(),
            "version" | "mode" | "scope" | "filters" | "page_size" | "cursor" | "projection"
        ) {
            return Err(QueryError::Invalid);
        }
        if fields.insert(key, percent(value)?).is_some() {
            return Err(QueryError::Invalid);
        }
    }
    fn json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, QueryError> {
        serde_json::from_str(value).map_err(|_| QueryError::Invalid)
    }
    let request = QueryRequest {
        version: fields
            .remove("version")
            .map(|v| json(&v))
            .transpose()?
            .unwrap_or(1),
        resource: ReadResource::EventHistory,
        mode: match fields.remove("mode").as_deref() {
            None | Some("snapshot") => ReadMode::Snapshot,
            Some("traversal") => ReadMode::Traversal,
            _ => return Err(QueryError::Invalid),
        },
        scope: json(&fields.remove("scope").ok_or(QueryError::Invalid)?)?,
        filters: json(&fields.remove("filters").ok_or(QueryError::Invalid)?)?,
        page_size: fields.remove("page_size").map(|v| json(&v)).transpose()?,
        cursor: fields.remove("cursor"),
        projection: fields
            .remove("projection")
            .map(|v| json::<Option<Vec<String>>>(&v))
            .transpose()?
            .flatten(),
    };
    validate(&request)?;
    Ok(request)
}

fn percent(input: &str) -> Result<String, QueryError> {
    let mut result = Vec::with_capacity(input.len());
    let mut bytes = input.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let digit = |b| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                b'A'..=b'F' => Ok(b - b'A' + 10),
                _ => Err(QueryError::Invalid),
            };
            result.push(
                digit(bytes.next().ok_or(QueryError::Invalid)?)? * 16
                    + digit(bytes.next().ok_or(QueryError::Invalid)?)?,
            );
        } else {
            result.push(byte);
        }
    }
    String::from_utf8(result).map_err(|_| QueryError::Invalid)
}

pub fn validate(request: &QueryRequest) -> Result<EventId, QueryError> {
    if request.version != 1
        || request.scope.dictionary_kind != DictionaryKind::VisibilityScope
        || !(1..=1000).contains(&request.page_size.unwrap_or(100))
    {
        return Err(QueryError::Invalid);
    }
    if request.filters.len() > 8 || request.filters.iter().any(|f| f.ids.len() > 100) {
        return Err(QueryError::Limit);
    }
    if request.resource != ReadResource::EventHistory
        || request.mode != ReadMode::Snapshot
        || request.cursor.is_some()
        || request.projection.is_some()
        || request.filters.len() != 1
        || request.filters[0].ids.len() != 1
    {
        return Err(QueryError::Unsupported);
    }
    Ok(request.filters[0].ids[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> String {
        format!(
            r#"{{"dataset_id":"{}","dictionary_kind":4,"allocation_domain":"{}","ordinal":"18446744073709551615"}}"#,
            "01".repeat(16),
            "02".repeat(16)
        )
    }
    fn filters() -> String {
        format!(r#"[{{"ids":["{}"]}}]"#, "03".repeat(32))
    }
    fn post() -> String {
        format!(
            r#"{{"version":1,"resource":"event_history","scope":{},"filters":{}}}"#,
            scope(),
            filters()
        )
    }
    fn encode(value: &str) -> String {
        value
            .as_bytes()
            .iter()
            .map(|b| format!("%{b:02X}"))
            .collect()
    }
    #[test]
    fn get_and_post_preserve_one_exact_selection_without_duplicate_json_keys() {
        let get = format!("scope={}&filters={}", encode(&scope()), encode(&filters()));
        let actual = decode_post(post().as_bytes()).unwrap();
        assert_eq!(decode_get(&get).unwrap(), actual);
        assert_eq!(actual.scope.ordinal.get(), u64::MAX);
        assert_eq!(actual.page_size, None);
        let duplicate = format!(
            r#"[{{"ids":["{}"],"ids":["{}"]}}]"#,
            "04".repeat(32),
            "03".repeat(32)
        );
        assert_eq!(
            decode_get(&format!(
                "scope={}&filters={}",
                encode(&scope()),
                encode(&duplicate)
            )),
            Err(QueryError::Invalid)
        );
        for query in [
            format!("{get}&filters={}", encode(&filters())),
            format!("{get}&principal=owner"),
            format!("{get}&mode=%FF"),
            format!("{get}&page_size=%"),
            format!("{get}&page_size=0"),
        ] {
            assert_eq!(decode_get(&query), Err(QueryError::Invalid));
        }
        for value in [
            post().replace("\"version\":1", "\"version\":1,\"version\":1"),
            post().replace("\"version\":1", "\"version\":2"),
            post().replace("\"version\":1", "\"version\":1,\"principal\":\"owner\""),
        ] {
            assert_eq!(decode_post(value.as_bytes()), Err(QueryError::Invalid));
        }
    }
    #[test]
    fn unsupported_constraints_never_disappear_or_broaden_the_read() {
        for extra in ["&mode=traversal", "&cursor=abc", "&projection=%5B%5D"] {
            assert_eq!(
                decode_get(&format!(
                    "scope={}&filters={}{}",
                    encode(&scope()),
                    encode(&filters()),
                    extra
                )),
                Err(QueryError::Unsupported)
            );
        }
        assert_eq!(
            decode_get(&"x".repeat(MAX_REQUEST_BYTES + 1)),
            Err(QueryError::Limit)
        );
        assert_eq!(percent("a,b+a%2Bb%20%E2%98%83").unwrap(), "a,b+a+b ☃");
        assert_eq!(percent("%2520").unwrap(), "%20");
    }
}
