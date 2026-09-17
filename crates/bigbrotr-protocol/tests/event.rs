use bigbrotr_protocol::{EventError, EventLimits, event::write_canonical, verify_json};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap()
}

#[test]
fn exact_canonical_bytes_match_independently_authored_literals() {
    for row in fixture()["events"].as_object().unwrap().values() {
        let bytes = serde_json::to_vec(&row["event"]).unwrap();
        let verified = verify_json(&bytes, EventLimits::default()).unwrap();
        let mut canonical = Vec::new();
        write_canonical(verified.event(), &mut canonical).unwrap();
        let hex: String = canonical.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hex, row["canonical_hex"]);
        let normalized = serde_json::to_vec(verified.event()).unwrap();
        assert_eq!(normalized.len(), verified.normalized_bytes());
        let at_limit = EventLimits {
            event_bytes: normalized.len(),
            ..EventLimits::default()
        };
        assert!(verify_json(&bytes, at_limit).is_ok());
        assert_eq!(
            verify_json(
                &bytes,
                EventLimits {
                    event_bytes: normalized.len() - 1,
                    ..at_limit
                }
            )
            .unwrap_err(),
            EventError::EventSizeLimit
        );
    }
}

#[test]
fn limits_are_checked_before_structure_or_crypto_and_do_not_overflow() {
    let small = EventLimits {
        string_bytes: 3,
        ..EventLimits::default()
    };
    assert_eq!(
        verify_json(br#"{"xxxx":0}"#, small).unwrap_err(),
        EventError::StringLimit
    );
    assert_eq!(
        verify_json(
            br#"{"x":0}"#,
            EventLimits {
                parse_bytes: 1,
                ..small
            }
        )
        .unwrap_err(),
        EventError::ArenaLimit
    );
    assert_eq!(
        verify_json(b"{}", EventLimits { depth: 0, ..small }).unwrap_err(),
        EventError::InvalidLimits
    );
    assert_eq!(
        verify_json(
            b"{}",
            EventLimits {
                depth: usize::MAX,
                ..small
            }
        )
        .unwrap_err(),
        EventError::InvalidLimits
    );
    assert_eq!(
        verify_json(
            br#"{"x":"\\\""}"#,
            EventLimits {
                depth: 1,
                ..EventLimits::default()
            }
        )
        .unwrap_err(),
        EventError::InvalidJson
    );
}

#[test]
fn every_invocation_checks_actual_bytes_and_signature_even_after_success() {
    let event = fixture()["events"]["timestamp-zero"]["event"].clone();
    let mut changed = event.clone();
    assert!(verify_json(&serde_json::to_vec(&event).unwrap(), EventLimits::default()).is_ok());
    changed["content"] = json!("changed");
    assert_eq!(
        verify_json(
            &serde_json::to_vec(&changed).unwrap(),
            EventLimits::default()
        )
        .unwrap_err(),
        EventError::EventIdMismatch
    );
    changed = event.clone();
    changed["sig"] = json!("00".repeat(64));
    assert_eq!(
        verify_json(
            &serde_json::to_vec(&changed).unwrap(),
            EventLimits::default()
        )
        .unwrap_err(),
        EventError::InvalidSignature
    );
    assert!(verify_json(&serde_json::to_vec(&event).unwrap(), EventLimits::default()).is_ok());
}

#[test]
fn strict_text_and_known_keys_refuse_ambiguous_or_non_json_input() {
    let event = fixture()["events"]["timestamp-zero"]["event"].clone();
    let bytes = serde_json::to_string(&event).unwrap();
    for raw in [
        format!("{{\"\\u0069d\":{},{}", event["id"], &bytes[1..]),
        bytes.replace("\"created_at\":0", "\"created_at\":-0"),
        bytes.replace("\"content\":\"\"", "\"content\":\"\\ud800\""),
        bytes.replace("\"content\":\"\"", "\"content\":\"\\u000a\""),
    ] {
        assert!(verify_json(raw.as_bytes(), EventLimits::default()).is_err());
    }
    for field in ["id", "pubkey", "sig"] {
        for replacement in ["npub1qqqq", "nostr:note1qqqq", "00", "gg"] {
            let mut malformed = event.clone();
            malformed[field] = json!(replacement);
            assert_eq!(
                verify_json(
                    &serde_json::to_vec(&malformed).unwrap(),
                    EventLimits::default()
                )
                .unwrap_err(),
                EventError::InvalidJson
            );
        }
    }
    assert_eq!(
        verify_json(b"{\"content\":\"\xff\"}", EventLimits::default()).unwrap_err(),
        EventError::InvalidJson
    );
}
