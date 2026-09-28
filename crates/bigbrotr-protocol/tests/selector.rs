use bigbrotr_model::acquisition::{Membership, SelectorError};
use bigbrotr_protocol::{
    EventError, EventLimits,
    selector::{DecodeError, decode_selector},
    verify_json,
};
use serde_json::{Value, json};

fn document(filters: &str) -> String {
    format!(r##"{{"version":1,"filters":{filters},"history_since":0,"ephemeral":"retain"}}"##)
}

#[test]
fn closed_json_normalizes_without_losing_signed_integer_or_tag_semantics() {
    let input = document(
        r##"[{"until":18446744073709551615,"since":0,"#t":["é","e\u0301","","é"],"kinds":[65535,0,65535]},{"kinds":[65535,0],"#t":["","é","é"],"since":0,"until":18446744073709551615}]"##,
    );
    let selector = decode_selector(input.as_bytes()).unwrap();
    assert_eq!(selector.filters().len(), 1);
    let f = selector.filters()[0].fields();
    assert_eq!(f.until.unwrap().0, u64::MAX);
    assert_eq!(f.tags[&'t'], ["", "e\u{301}", "é"]);
    let output = serde_json::to_vec(&selector).unwrap();
    let roundtrip = decode_selector(&output).unwrap();
    assert_eq!(selector.digest(), roundtrip.digest());
    let normalized: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(normalized["filters"].as_array().unwrap().len(), 1);
    assert_eq!(normalized["filters"][0]["kinds"], json!([0, 65535]));
}

#[test]
fn duplicates_unknown_fields_and_lossy_shapes_fail_instead_of_dropping_constraints() {
    for filters in [
        r##"[{"ids":null}]"##,
        r##"[{"kinds":[0],"kinds":[1]}]"##,
        r##"[{"since":0,"\u0073ince":1}]"##,
        r##"[{"#t":["x"],"#\u0074":["y"]}]"##,
        r##"[{"limit":0}]"##,
        r##"[{"search":"x"}]"##,
        r##"[{"kinds":[65536]}]"##,
        r##"[{"since":-0}]"##,
        r##"[{"until":1e0}]"##,
        r##"[{"until":18446744073709551616}]"##,
        r##"[{"#tag":["x"]}]"##,
        r##"[{"#1":["x"]}]"##,
        r##"[{"#é":["x"]}]"##,
        r##"[{"authors":["npub1qqqq"]}]"##,
        r##"[[]]"##,
        r##"[null]"##,
    ] {
        assert!(
            decode_selector(document(filters).as_bytes()).is_err(),
            "{filters}"
        );
    }
    for input in [
        document("[{}]").replace("\"version\":1", "\"version\":1,\"version\":1"),
        document("[{}]").replace("\"history_since\":0", "\"history_since\":null"),
        document("[{}]").replace("\"history_since\":0", "\"history_since\":-1"),
        document("[{}]").replace("\"history_since\":0", "\"extra\":0"),
        document("[{}]") + " []",
    ] {
        assert!(decode_selector(input.as_bytes()).is_err());
    }
    assert_eq!(
        decode_selector(
            document("[{}]")
                .replace("\"version\":1", "\"version\":2")
                .as_bytes()
        )
        .err(),
        Some(DecodeError::Unsupported)
    );
    assert_eq!(
        decode_selector(document("[{}]").replace("retain", "future").as_bytes()).err(),
        Some(DecodeError::Unsupported)
    );
}

#[test]
fn profile_and_whole_json_bounds_are_reported_distinctly() {
    assert_eq!(
        decode_selector(br#"[1,[{}],0,"retain"]"#).err(),
        Some(DecodeError::InvalidJson)
    );
    assert_eq!(
        decode_selector(document("[]").as_bytes()).err(),
        Some(DecodeError::Profile(SelectorError::EmptyUnion))
    );
    assert_eq!(
        decode_selector(document(r##"[{"kinds":[]}]"##).as_bytes()).err(),
        Some(DecodeError::Profile(SelectorError::EmptyList))
    );
    assert_eq!(
        decode_selector(document(r##"[{"#e":["ab"]}]"##).as_bytes()).err(),
        Some(DecodeError::Profile(SelectorError::InvalidTagValue))
    );
    let filters = format!("[{}]", vec!["{}"; 17].join(","));
    assert_eq!(
        decode_selector(document(&filters).as_bytes()).err(),
        Some(DecodeError::Profile(SelectorError::FilterLimit))
    );
    let input = document("[{}]");
    let mut exact = input.as_bytes().to_vec();
    exact.resize(2 * 1024 * 1024, b' ');
    assert!(decode_selector(&exact).is_ok());
    exact.push(b' ');
    assert_eq!(
        decode_selector(&exact).err(),
        Some(DecodeError::Bounds(EventError::WireLimit))
    );
    let deep = document(&format!("[{{\"ignored\":{}}}]", "[".repeat(33)));
    assert_eq!(
        decode_selector(deep.as_bytes()).err(),
        Some(DecodeError::Bounds(EventError::DepthLimit))
    );
    assert_eq!(
        decode_selector(b"{\"\xff\":null}").err(),
        Some(DecodeError::InvalidJson)
    );
}

#[test]
fn independently_signed_events_match_the_full_collection_and_extra_request_filter() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    let event = verify_json(
        &serde_json::to_vec(&corpus["events"]["E_private"]).unwrap(),
        EventLimits::default(),
    )
    .unwrap();
    let selector =
        decode_selector(document(r##"[{"kinds":[1],"#t":["é"],"since":0,"until":1}]"##).as_bytes())
            .unwrap();
    assert_eq!(selector.classify(event.event()), Membership::Included);
    let request =
        decode_selector(document(r##"[{"kinds":[1],"#t":["é"],"since":2}]"##).as_bytes()).unwrap();
    assert!(!request.filters()[0].matches(event.event()));
}

#[test]
fn persistent_encoding_is_bounded_and_replayable_without_jsonb_expansion() {
    use bigbrotr_model::{
        NostrTime,
        acquisition::{CollectionSelector, EphemeralPolicy, Filter, FilterFields},
    };
    use bigbrotr_protocol::selector::{EncodeError, encode_selector};
    use std::collections::BTreeMap;
    let original =
        decode_selector(document(r##"[{"#t":["é","\u0000","a"],"kinds":[2,1]}]"##).as_bytes())
            .unwrap();
    let bytes = encode_selector(&original).unwrap();
    assert_eq!(bytes, "{\"version\":1,\"filters\":[{\"kinds\":[1,2],\"#t\":[\"\\u0000\",\"a\",\"é\"]}],\"history_since\":0,\"ephemeral\":\"retain\"}".as_bytes());
    assert_eq!(decode_selector(&bytes).unwrap().digest(), original.digest());
    let escaped = decode_selector(
        document(r##"[{"#t":["é","/","\\","\"","\u0000","\b","\t","\n","\f","\r"]}]"##).as_bytes(),
    )
    .unwrap();
    assert_eq!(encode_selector(&escaped).unwrap(), concat!(
        r##"{"version":1,"filters":[{"#t":["\u0000","\b","\t","\n","\f","\r","\"","/","\\","é"]}],"##,
        r##""history_since":0,"ephemeral":"retain"}"##
    ).as_bytes());
    let large = CollectionSelector::new(
        vec![
            Filter::new(FilterFields {
                tags: BTreeMap::from([('t', vec!["\0".repeat(1024 * 1024)])]),
                ..FilterFields::default()
            })
            .unwrap(),
        ],
        NostrTime(0),
        EphemeralPolicy::Retain,
    )
    .unwrap();
    // Valid typed intent, but its six-byte JSON escapes exceed the document cap.
    assert_eq!(
        encode_selector(&large).err(),
        Some(EncodeError::EncodedLimit)
    );
    let filters = (0..16)
        .map(|group| {
            Filter::new(FilterFields {
                tags: BTreeMap::from([(
                    't',
                    (0..1024)
                        .map(|item| format!("{group:02}-{item:04}{}", "\u{1}".repeat(16)))
                        .collect(),
                )]),
                ..FilterFields::default()
            })
            .unwrap()
        })
        .collect();
    let dense = CollectionSelector::new(filters, NostrTime(0), EphemeralPolicy::Retain).unwrap();
    assert!(serde_json::to_vec(&dense).unwrap().len() < 2 * 1024 * 1024);
    assert_eq!(
        encode_selector(&dense).err(),
        Some(EncodeError::WorkingLimit)
    );
}

#[test]
fn persisted_selector_read_requires_exact_normalized_bytes() {
    use bigbrotr_protocol::selector::decode_normalized_selector;
    const NORMAL: &str = concat!(
        r##"{"version":1,"filters":[{"kinds":[1,2],"#t":["/","\u0000","é"]}],"##,
        r##""history_since":0,"ephemeral":"retain"}"##
    );
    // The literal order above is deliberately wrong: U+0000 sorts before '/'.
    assert_eq!(
        decode_normalized_selector(NORMAL.as_bytes()).err(),
        Some(DecodeError::NonCanonical)
    );
    const EXACT: &str = concat!(
        r##"{"version":1,"filters":[{"kinds":[1,2],"#t":["\u0000","/","é"]}],"##,
        r##""history_since":0,"ephemeral":"retain"}"##
    );
    let retained = decode_normalized_selector(EXACT.as_bytes()).unwrap();
    assert_eq!(retained.filters()[0].fields().kinds, Some(vec![1, 2]));
    assert_eq!(retained.filters()[0].fields().tags[&'t'], ["\0", "/", "é"]);
    for changed in [
        format!(" {EXACT}"),
        format!("{EXACT}\n"),
        EXACT.replace("[1,2]", "[2,1]"),
        EXACT.replace("[1,2]", "[1,2,2]"),
        EXACT.replace("é", "\\u00e9"),
        EXACT.replace("/", "\\/"),
        EXACT
            .replace("\"version\":1,", "")
            .replace("\"history_since\":0", "\"version\":1,\"history_since\":0"),
    ] {
        assert!(decode_selector(changed.as_bytes()).is_ok());
        assert_eq!(
            decode_normalized_selector(changed.as_bytes()).err(),
            Some(DecodeError::NonCanonical)
        );
    }
    assert_eq!(
        decode_normalized_selector(b"[]").err(),
        Some(DecodeError::InvalidJson)
    );
    assert_eq!(
        decode_normalized_selector(EXACT.replace("\"version\":1", "\"version\":2").as_bytes())
            .err(),
        Some(DecodeError::Unsupported)
    );
}
