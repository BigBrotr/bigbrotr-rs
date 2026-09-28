use bigbrotr_model::{Signature, SignedEvent};
use bigbrotr_protocol::verify_owned;
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

#[test]
fn owned_verification_preserves_buffers_and_checks_exact_size() {
    for row in fixture()["events"].as_object().unwrap().values() {
        let event: SignedEvent = serde_json::from_value(row["event"].clone()).unwrap();
        let original = event.clone();
        let content = event.content.as_ptr();
        let tags = event.tags.as_ptr();
        let normalized_len = serde_json::to_vec(&event).unwrap().len();
        let verified = verify_owned(event, normalized_len).unwrap();
        assert_eq!(verified.event(), &original);
        assert_eq!(verified.event().content.as_ptr(), content);
        assert_eq!(verified.event().tags.as_ptr(), tags);
        assert_eq!(verified.normalized_bytes(), normalized_len);
        assert_eq!(
            verify_owned(original, normalized_len - 1).unwrap_err(),
            EventError::EventSizeLimit
        );
    }
}

#[test]
fn owned_verification_never_reuses_an_event_id_as_signature_proof() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    let rows = corpus["events"].as_object().unwrap();
    for name in ["E_private", "E_public", "I"] {
        let event: SignedEvent = serde_json::from_value(rows[name].clone()).unwrap();
        assert!(verify_owned(event.clone(), 256 * 1024).is_ok());
        let mut changed = event.clone();
        let mut signature = *changed.sig.as_bytes();
        signature[0] ^= 1;
        changed.sig = Signature::from_bytes(signature);
        assert_eq!(
            verify_owned(changed, 256 * 1024).unwrap_err(),
            EventError::InvalidSignature
        );
        let mut changed = event;
        changed.content.push('x');
        assert_eq!(
            verify_owned(changed, 256 * 1024).unwrap_err(),
            EventError::EventIdMismatch
        );
    }
}

#[test]
fn owned_verification_bounds_work_before_hashing_or_crypto() {
    let event: SignedEvent =
        serde_json::from_value(fixture()["events"]["timestamp-zero"]["event"].clone()).unwrap();
    assert_eq!(
        verify_owned(event.clone(), 0).unwrap_err(),
        EventError::InvalidLimits
    );
    let mut changed = event.clone();
    changed.content = "x".repeat(2048);
    assert_eq!(
        verify_owned(changed, 1024).unwrap_err(),
        EventError::EventSizeLimit
    );
    let mut changed = event.clone();
    changed.tags = vec![vec![String::new()]; 1025];
    assert_eq!(
        verify_owned(changed, 1024).unwrap_err(),
        EventError::EventSizeLimit
    );
    let mut changed = event.clone();
    changed.tags = vec![vec![String::new(); 1025]];
    assert_eq!(
        verify_owned(changed, 1024).unwrap_err(),
        EventError::EventSizeLimit
    );
    let mut changed = event.clone();
    changed.tags = vec![vec!["x".repeat(2048)]];
    assert_eq!(
        verify_owned(changed, 1024).unwrap_err(),
        EventError::EventSizeLimit
    );
    let mut changed = event;
    changed.tags = vec![vec![]];
    assert_eq!(
        verify_owned(changed, 1024).unwrap_err(),
        EventError::EmptyTag
    );
}

#[test]
fn bounded_decode_preserves_invalid_candidates_without_granting_verification() {
    use bigbrotr_protocol::decode_event_json;
    let original = fixture()["events"]["timestamp-zero"]["event"].clone();
    for (field, value, error) in [
        ("sig", json!("00".repeat(64)), EventError::InvalidSignature),
        ("content", json!("altered"), EventError::EventIdMismatch),
        ("tags", json!([[]]), EventError::EmptyTag),
    ] {
        let mut changed = original.clone();
        changed[field] = value;
        let decoded = decode_event_json(
            &serde_json::to_vec(&changed).unwrap(),
            EventLimits::default(),
        )
        .unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), changed);
        assert_eq!(verify_owned(decoded, 256 * 1024).unwrap_err(), error);
    }
    let bytes = serde_json::to_vec(&original).unwrap();
    let limits = EventLimits {
        event_bytes: 1,
        ..EventLimits::default()
    };
    let decoded = decode_event_json(&bytes, limits).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), original);
    assert_eq!(
        verify_owned(decoded, limits.event_bytes).unwrap_err(),
        EventError::EventSizeLimit
    );
}

#[test]
fn bounded_decode_keeps_strict_shape_utf8_and_duplicate_key_rules() {
    use bigbrotr_protocol::decode_event_json;
    let original = fixture()["events"]["timestamp-zero"]["event"].clone();
    let text = serde_json::to_string(&original).unwrap();
    for text in [
        format!("{{\"\\u0069d\":{},{}", original["id"], &text[1..]),
        format!("{text}{{}}"),
        serde_json::to_string(&json!([
            original["id"],
            original["pubkey"],
            0,
            1,
            [],
            "",
            original["sig"]
        ]))
        .unwrap(),
        text.replace("\"created_at\":0", "\"created_at\":-0"),
        text.replace("\"sig\":", "\"missing_sig\":"),
    ] {
        assert_eq!(
            decode_event_json(text.as_bytes(), EventLimits::default()).unwrap_err(),
            EventError::InvalidJson
        );
    }
    let mut bytes = format!("{{\"extension\":\"x\",{}", &text[1..]).into_bytes();
    bytes[14] = 0xff;
    assert_eq!(
        decode_event_json(&bytes, EventLimits::default()).unwrap_err(),
        EventError::InvalidJson
    );
}

#[test]
fn bounded_decode_refuses_resource_limits_before_event_fields() {
    use bigbrotr_protocol::decode_event_json;
    for (limits, error) in [
        (
            EventLimits {
                wire_bytes: 1,
                ..EventLimits::default()
            },
            EventError::WireLimit,
        ),
        (
            EventLimits {
                parse_bytes: 1,
                ..EventLimits::default()
            },
            EventError::ArenaLimit,
        ),
        (
            EventLimits {
                string_bytes: 1,
                ..EventLimits::default()
            },
            EventError::StringLimit,
        ),
        (
            EventLimits {
                nodes: 1,
                ..EventLimits::default()
            },
            EventError::NodeLimit,
        ),
        (
            EventLimits {
                depth: 1,
                ..EventLimits::default()
            },
            EventError::DepthLimit,
        ),
    ] {
        assert_eq!(
            decode_event_json(br#"{"unknown":[0]}"#, limits).unwrap_err(),
            error
        );
    }
}
