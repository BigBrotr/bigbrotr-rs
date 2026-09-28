use bigbrotr_protocol::{
    EventError, EventLimits, verify_json,
    wire::{RelayMessage, WireError, decode_relay},
};
use serde_json::Value;
use std::borrow::Cow;

fn event_json() -> String {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    serde_json::to_string_pretty(&corpus["events"]["timestamp-zero"]["event"]).unwrap()
}

#[test]
fn event_borrows_original_json_without_reserializing_signed_fields() {
    let event = event_json();
    let input =
        format!(" [\"EVENT\",\"request-1\",{event},{{\"future\":18446744073709551615}}] \n");
    let envelope = decode_relay(input.as_bytes(), EventLimits::default()).unwrap();
    assert_eq!(envelope.json(), input);
    assert_eq!(envelope.json().as_ptr(), input.as_ptr());
    let RelayMessage::Event { subscription, json } = envelope.message() else {
        panic!("not EVENT");
    };
    assert!(matches!(subscription, Cow::Borrowed("request-1")));
    assert_eq!(json.get(), event);
    let offset = input.find('{').unwrap();
    assert_eq!(json.get().as_ptr(), input[offset..].as_ptr());
    let verified = verify_json(json.get().as_bytes(), EventLimits::default()).unwrap();
    assert_eq!(verified.event().created_at.0, 0);
}

#[test]
fn duplicate_and_nonintegral_signed_fields_reach_the_strict_verifier_unchanged() {
    let event = event_json();
    for changed in [
        format!("{{\"content\":\"tampered\",{}", &event[1..]),
        format!("{{\"\\u0069d\":\"{}\",{}", "00".repeat(32), &event[1..]),
        event.replace("\"created_at\": 0", "\"created_at\": -0"),
        event.replace("\"created_at\": 0", "\"created_at\": 0e0"),
    ] {
        assert_ne!(changed, event);
        let input = format!("[\"EVENT\",\"r\",{changed}]");
        let envelope = decode_relay(input.as_bytes(), EventLimits::default()).unwrap();
        let RelayMessage::Event { json, .. } = envelope.message() else {
            panic!("not EVENT");
        };
        assert_eq!(json.get(), changed);
        assert_eq!(
            verify_json(json.get().as_bytes(), EventLimits::default()).unwrap_err(),
            EventError::InvalidJson
        );
    }
    // A Value round trip erases the duplicated first content. This negative
    // control establishes that the raw boundary is necessary, not incidental.
    let duplicate = format!("{{\"content\":\"tampered\",{}", &event[1..]);
    let erased: Value = serde_json::from_str(&duplicate).unwrap();
    assert!(
        verify_json(
            &serde_json::to_vec(&erased).unwrap(),
            EventLimits::default()
        )
        .is_ok()
    );
}

#[test]
fn invalid_event_values_keep_subscription_attribution() {
    for value in ["null", "[]", "17", "\"event\"", "{}"] {
        let input = format!("[\"EVENT\",\"r\",{value}]");
        let envelope = decode_relay(input.as_bytes(), EventLimits::default()).unwrap();
        let RelayMessage::Event { subscription, json } = envelope.message() else {
            panic!("not EVENT");
        };
        assert_eq!(subscription, "r");
        assert_eq!(json.get(), value);
        assert_eq!(
            verify_json(json.get().as_bytes(), EventLimits::default()).unwrap_err(),
            EventError::InvalidJson
        );
    }
}

#[test]
fn eose_preserves_absent_empty_unknown_duplicate_and_contradictory_hints() {
    for (input, expected) in [
        (r#"["EOSE","r"]"#, None),
        (r#"["EOSE","r",[]]"#, Some(vec![])),
        (
            r#"["EOSE","r",["finish","more","auth","future","future","fini\u0073h"],{"later":true}]"#,
            Some(vec!["finish", "more", "auth", "future", "future", "finish"]),
        ),
    ] {
        let envelope = decode_relay(input.as_bytes(), EventLimits::default()).unwrap();
        assert_eq!(envelope.json(), input);
        let RelayMessage::Eose {
            subscription,
            hints,
        } = envelope.message()
        else {
            panic!("not EOSE");
        };
        assert_eq!(subscription, "r");
        assert_eq!(
            hints
                .as_ref()
                .map(|h| h.iter().map(Cow::as_ref).collect::<Vec<_>>()),
            expected
        );
    }
}

#[test]
fn control_messages_retain_their_distinct_correlation_fields() {
    let limits = EventLimits::default();
    let closed = decode_relay(br#"["CLOSED","r","auth-required: sign in"]"#, limits).unwrap();
    assert!(
        matches!(closed.message(), RelayMessage::Closed { subscription, message }
        if subscription == "r" && message == "auth-required: sign in")
    );
    let auth = decode_relay(br#"["AUTH","ch\u0061llenge"]"#, limits).unwrap();
    assert!(matches!(auth.message(), RelayMessage::Auth { challenge }
        if matches!(challenge, Cow::Owned(_)) && challenge == "challenge"));
    let notice = decode_relay(br#"["NOTICE","not scoped"]"#, limits).unwrap();
    assert!(
        matches!(notice.message(), RelayMessage::Notice { message } if message == "not scoped")
    );
    let input = format!(r#"["OK","{}",false,"blocked: reason"]"#, "ab".repeat(32));
    let ok = decode_relay(input.as_bytes(), limits).unwrap();
    assert!(
        matches!(ok.message(), RelayMessage::Ok { event_id, accepted: false, message }
        if event_id.as_bytes() == &[0xab; 32] && message == "blocked: reason")
    );
    let unknown = decode_relay(br#"["NEW",{"future":[1,2,3]}]"#, limits).unwrap();
    assert!(matches!(unknown.message(), RelayMessage::Unknown { kind } if kind == "NEW"));
}

#[test]
fn malformed_required_positions_and_document_tails_are_rejected() {
    for input in [
        r#"{}"#,
        r#"[]"#,
        r#"[null]"#,
        r#"["EVENT"]"#,
        r#"["EVENT","r"]"#,
        r#"["EVENT",null,{}]"#,
        r#"["EOSE"]"#,
        r#"["EOSE","r",null]"#,
        r#"["EOSE","r","finish"]"#,
        r#"["EOSE","r",["finish",false]]"#,
        r#"["CLOSED","r"]"#,
        r#"["CLOSED","r",null]"#,
        r#"["AUTH"]"#,
        r#"["NOTICE",false]"#,
        r#"["OK","bad-id",true,"x"]"#,
        r#"["OK","0000000000000000000000000000000000000000000000000000000000000000",1,"x"]"#,
        r#"["NOTICE","x"] []"#,
        r#"["UNKNOWN",{"broken":}]"#,
        r#"["EVENT","r",{"broken":}]"#,
    ] {
        assert_eq!(
            decode_relay(input.as_bytes(), EventLimits::default()).err(),
            Some(WireError::InvalidEnvelope),
            "{input}"
        );
    }
    assert_eq!(
        decode_relay(b"[\"UNKNOWN\",\"\xff\"]", EventLimits::default()).err(),
        Some(WireError::InvalidEnvelope)
    );
}

#[test]
fn subscription_limit_counts_decoded_characters_instead_of_utf8_bytes() {
    for (subscription, accepted) in [
        (String::new(), false),
        ("a".repeat(64), true),
        ("a".repeat(65), false),
        ("🦀".repeat(64), true),
        ("🦀".repeat(65), false),
        ("\\u0061".repeat(64), true),
        ("\\u0061".repeat(65), false),
    ] {
        for (kind, tail) in [("EOSE", ""), ("EVENT", ",{}"), ("CLOSED", ",\"reason\"")] {
            let input = format!("[\"{kind}\",\"{subscription}\"{tail}]");
            assert_eq!(
                decode_relay(input.as_bytes(), EventLimits::default()).is_ok(),
                accepted
            );
        }
    }
}

#[test]
fn whole_message_bounds_apply_to_unknown_and_trailing_values_before_parsing() {
    let input = br#"["EOSE","r",[],{"future":["value",0]}]"#;
    // Independent lexical inventory: 4 containers, 4 strings, 1 number, depth 3.
    let exact = EventLimits {
        wire_bytes: input.len(),
        string_bytes: 6,
        nodes: 9,
        depth: 3,
        parse_bytes: input.len() * 3 + 9 * 96,
        ..EventLimits::default()
    };
    assert!(decode_relay(input, exact).is_ok());
    for (limits, expected) in [
        (
            EventLimits {
                wire_bytes: input.len() - 1,
                ..exact
            },
            EventError::WireLimit,
        ),
        (
            EventLimits {
                string_bytes: 5,
                ..exact
            },
            EventError::StringLimit,
        ),
        (EventLimits { nodes: 8, ..exact }, EventError::NodeLimit),
        (EventLimits { depth: 2, ..exact }, EventError::DepthLimit),
        (
            EventLimits {
                parse_bytes: exact.parse_bytes - 1,
                ..exact
            },
            EventError::ArenaLimit,
        ),
        (EventLimits { nodes: 0, ..exact }, EventError::InvalidLimits),
    ] {
        assert_eq!(
            decode_relay(input, limits).err(),
            Some(WireError::Bounds(expected))
        );
    }
    let malformed = b"[\"UNKNOWN\",[[[[[[";
    assert_eq!(
        decode_relay(
            malformed,
            EventLimits {
                depth: 2,
                ..EventLimits::default()
            }
        )
        .err(),
        Some(WireError::Bounds(EventError::DepthLimit))
    );
}
