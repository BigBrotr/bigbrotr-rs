use bigbrotr_model::{
    ClockQuality, DictionaryDescriptor, DictionaryEntry, DictionaryKind, Digest256, DurationUs,
    Fact, FactPayload, Measurement, MeasurementAttemptId, ObservedTime, Ordinal, Sequence,
    codec::{CodecError, FactDecodeLimits},
    measurement::*,
};
use sha2::{Digest, Sha256};

fn bytes(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/measurement-v1.json")).unwrap()
}
fn decode(input: &[u8]) -> Result<Fact, CodecError> {
    Fact::decode_preimage(
        input,
        Digest256::from_bytes(Sha256::digest(input).into()),
        FactDecodeLimits::default(),
    )
}
fn literal(name: &str) -> Fact {
    let fixture = fixture();
    let row = fixture["valid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == name)
        .unwrap();
    decode(&bytes(row["preimage_hex"].as_str().unwrap())).unwrap()
}
fn measurement(fact: &Fact) -> &Measurement {
    let FactPayload::Measurement(value) = fact.payload() else {
        panic!("wrong literal family")
    };
    value
}
fn terminal(fact: &Fact) -> TerminalMeasurement {
    match measurement(fact).state() {
        MeasurementState::Terminal(value)
        | MeasurementState::Resolution {
            terminal: value, ..
        } => value,
        _ => panic!("not terminal"),
    }
}
fn replace(fact: &Fact, value: Measurement) -> Fact {
    Fact::new(
        fact.source_record_id(),
        fact.visibility_scope_id(),
        fact.recorded_at(),
        FactPayload::Measurement(value),
    )
    .unwrap()
}
fn with_state(value: &Measurement, state: MeasurementState) -> Measurement {
    Measurement::new(
        value.attempt_id(),
        value.context(),
        value.profile(),
        value.clock_quality(),
        state,
    )
    .unwrap()
}

#[test]
fn all_four_states_match_independent_literal_bytes_and_digests() {
    let fixture = fixture();
    let rows = fixture["valid"].as_array().unwrap();
    assert_eq!(rows.len(), 15);
    for row in rows {
        let input = bytes(row["preimage_hex"].as_str().unwrap());
        let expected = row["sha256"].as_str().unwrap().parse().unwrap();
        let value = Fact::decode_preimage(&input, expected, FactDecodeLimits::default()).unwrap();
        assert_eq!(value.payload_digest(), expected, "{}", row["name"]);
        assert_eq!(value.encode_preimage(input.len()).unwrap(), input);
        assert_eq!(value.record_kind(), "measurement");
        assert_eq!(value.record_schema_version(), 1);
        assert_eq!(value.corrects(), None);
        assert_eq!(value.payload().event_id(), None);
        assert_eq!(
            value
                .dictionary_refs()
                .map(|r| r.dictionary_kind as u16)
                .collect::<Vec<_>>(),
            [4, 1, 2, 6, 3, 5, 2]
        );
        assert_eq!(value.dictionary_refs().last().unwrap().ordinal.get(), 2);
        assert_eq!(
            Fact::retained_source_record_id(&input, input.len()).unwrap(),
            value.source_record_id()
        );
        assert!(matches!(
            value.encode_preimage(input.len() - 1),
            Err(CodecError::InputLimit)
        ));
        // Exactly one fixed allocation, charged before decoding/allocating. Its
        // caller additionally budgets input/output, inline Fact and any clone.
        let required = Measurement::required_allocation_bytes();
        assert_eq!(measurement(&value).heap_allocation_bytes(), required);
        assert_eq!(
            Fact::decode_preimage(
                &input,
                expected,
                FactDecodeLimits {
                    max_allocation_bytes: required,
                    max_tag_nodes: 0,
                    max_string_bytes: 0,
                    ..FactDecodeLimits::default()
                }
            )
            .unwrap(),
            value
        );
        assert_eq!(
            Fact::decode_preimage(
                &input,
                expected,
                FactDecodeLimits {
                    max_allocation_bytes: required - 1,
                    ..FactDecodeLimits::default()
                }
            ),
            Err(CodecError::AllocationLimit)
        );
    }
}

#[test]
fn independent_negative_controls_and_noncanonical_bytes_are_refused() {
    let fixture = fixture();
    let rows = fixture["invalid"].as_array().unwrap();
    assert_eq!(rows.len(), 33);
    for row in rows {
        assert!(
            decode(&bytes(row["preimage_hex"].as_str().unwrap())).is_err(),
            "{}",
            row["name"]
        );
    }
    for row in fixture["valid"].as_array().unwrap() {
        let input = bytes(row["preimage_hex"].as_str().unwrap());
        for length in 0..input.len() {
            assert!(decode(&input[..length]).is_err());
        }
        let mut wide = input.clone();
        let prefix = b"bigbrotr/fact/1\0".len();
        wide.splice(prefix..prefix + 1, [0x98, 6]);
        assert_eq!(decode(&wide), Err(CodecError::NonCanonical));
        let mut trailing = input;
        trailing.push(0);
        assert_eq!(decode(&trailing), Err(CodecError::NonCanonical));
    }
    assert!(MeasurementAttemptId::try_from([0; 16]).is_err());
    assert!(
        "AA000000000000000000000000000000"
            .parse::<MeasurementAttemptId>()
            .is_err()
    );
}

#[test]
fn literal_observations_preserve_unknown_empty_auth_event_timeout_and_utc_rollback() {
    assert_eq!(
        measurement(&literal("execution-unknown")).state(),
        MeasurementState::ExecutionUnknown
    );
    let pending = literal("completion-unknown-prefix-pending");
    let MeasurementState::CompletionUnknown(observed) = measurement(&pending).state() else {
        panic!()
    };
    assert!(observed.read.eose.is_some());
    assert!(!observed.read.eose_prefix_verified);
    assert!(
        observed.read.first_valid_event.unwrap().verified.elapsed_us
            > observed.read.eose.unwrap().elapsed_us
    );
    let empty = terminal(&literal("empty-eose-auth-utc-rollback"));
    assert_eq!(empty.termination, MeasurementTermination::Eose);
    assert!(empty.observed.read.auth_observed);
    assert_eq!(empty.observed.read.first_valid_event, None);
    assert!(
        empty.observed.open.tcp_connected.unwrap().observed_at
            > empty.observed.open.websocket_opened.unwrap().observed_at
    );
    let timeout = terminal(&literal("event-before-timeout"));
    assert_eq!(
        timeout.termination,
        MeasurementTermination::Deadline(MeasurementPhase::ResponseRead)
    );
    assert!(timeout.observed.read.first_valid_event.is_some());
    assert!(timeout.observed.read.eose.is_none());
    let immediate = terminal(&literal("immediate-local-permission"));
    assert_eq!(immediate.observed.execution.observed_at, None);
    assert_eq!(
        immediate.observed.execution.result,
        ConnectCallResult::ImmediateError(ConnectErrorClass::Permission)
    );
    assert_eq!(immediate.ended.elapsed_us, Some(DurationUs::new(0)));
    let cap = terminal(&literal("physical-frame-cap"));
    assert_eq!(cap.observed.counters.rx_physical_frames, 65);
    assert_eq!(cap.observed.counters.rx_messages, 0);
    assert_eq!(cap.observed.counters.rx_payload_bytes, 0);
}

#[test]
fn counter_minima_preserve_silent_unknown_prefixes_and_closed_termination_kinds() {
    let closed = terminal(&literal("correlated-closed"));
    assert_eq!(closed.termination, MeasurementTermination::Closed);
    assert!(closed.observed.read.correlated_closed);
    assert!(closed.observed.read.auth_observed);
    let authority = terminal(&literal("event-before-authority-interruption"));
    assert_eq!(
        authority.termination,
        MeasurementTermination::AuthorityInterruption(MeasurementPhase::ResponseRead)
    );
    assert!(authority.observed.read.first_valid_event.is_some());
    let cancelled = terminal(&literal("cancelled-after-request-before-response"));
    assert_eq!(
        cancelled.termination,
        MeasurementTermination::Cancelled(MeasurementPhase::ResponseRead)
    );
    assert_eq!(
        cancelled.observed.read.request_flushed,
        Some(MeasurementMoment {
            observed_at: None,
            elapsed_us: None
        })
    );
    assert_eq!(cancelled.observed.counters.rx_messages, 0);
    assert_eq!(cancelled.observed.counters.rx_physical_frames, 0);
    assert_eq!(cancelled.observed.counters.rx_ws_bytes, 0);
    assert_eq!(cancelled.observed.counters.rx_payload_bytes, 0);
    assert!(cancelled.observed.counters.tx_ws_bytes > 0);
    for name in [
        "completion-unknown-tcp-zero-counters",
        "completion-unknown-upgrade-zero-counters",
    ] {
        let value = literal(name);
        let MeasurementState::CompletionUnknown(observed) = measurement(&value).state() else {
            panic!()
        };
        assert!(observed.open.tcp_connected.is_some());
        assert_eq!(observed.read, ReadObservation::default());
        assert_eq!(observed.counters, MeasurementCounters::default());
    }
    let partial = literal("completion-unknown-partial-input");
    let MeasurementState::CompletionUnknown(observed) = measurement(&partial).state() else {
        panic!()
    };
    assert_eq!(observed.read, ReadObservation::default());
    assert_eq!(observed.counters.rx_physical_frames, 1);
    assert_eq!(observed.counters.rx_messages, 0);
    assert_eq!(observed.counters.rx_payload_bytes, 3);
    assert_eq!(observed.counters.rx_ws_bytes, 5);
}

#[test]
fn resolutions_require_exact_target_context_and_preserved_known_prefix() {
    let unknown = literal("execution-unknown");
    let completion = literal("completion-unknown-prefix-pending");
    let resolution = literal("resolve-execution-unknown");
    let resolved_completion = literal("resolve-completion-unknown");
    resolution
        .validate_measurement_resolution(&unknown)
        .unwrap();
    resolved_completion
        .validate_measurement_resolution(&completion)
        .unwrap();
    literal("resolve-across-owner-stream")
        .validate_measurement_resolution(&unknown)
        .unwrap();
    assert!(
        resolution
            .validate_measurement_resolution(&completion)
            .is_err()
    );
    assert!(
        resolution
            .validate_measurement_resolution(&resolution)
            .is_err()
    );
    let same_id_changed_digest = Fact::new(
        unknown.source_record_id(),
        unknown.visibility_scope_id(),
        ObservedTime::new(0),
        unknown.payload().clone(),
    )
    .unwrap();
    assert!(
        resolution
            .validate_measurement_resolution(&same_id_changed_digest)
            .is_err()
    );
    assert_ne!(
        measurement(&resolution).clock_quality(),
        measurement(&unknown).clock_quality()
    );
    let current = measurement(&resolved_completion);
    let prior = completion.fact_ref();
    let original = terminal(&resolved_completion);
    let mut changes = Vec::new();
    let mut changed = original;
    changed.observed.execution.observed_at = Some(ObservedTime::new(0));
    changes.push(changed);
    let mut changed = original;
    changed.observed.read.auth_observed = false;
    changes.push(changed);
    let mut changed = original;
    changed.observed.counters.rx_ws_bytes -= 1;
    changes.push(changed);
    let mut changed = original;
    changed
        .observed
        .read
        .first_valid_event
        .as_mut()
        .unwrap()
        .event_id = bigbrotr_model::EventId::from_bytes([0xbb; 32]);
    changes.push(changed);
    for terminal in changes {
        let bad = replace(
            &resolved_completion,
            with_state(
                current,
                MeasurementState::Resolution {
                    prior_unknown: prior,
                    terminal,
                },
            ),
        );
        assert!(bad.validate_measurement_resolution(&completion).is_err());
    }
    let mut context = current.context();
    context.route_id.ordinal = Ordinal::new(2).unwrap();
    let bad = replace(
        &resolved_completion,
        Measurement::new(
            current.attempt_id(),
            context,
            current.profile(),
            current.clock_quality(),
            current.state(),
        )
        .unwrap(),
    );
    assert!(bad.validate_measurement_resolution(&completion).is_err());
    let bad = replace(
        &resolved_completion,
        Measurement::new(
            current.attempt_id(),
            current.context(),
            PassiveReadProfile::first(DurationUs::new(3_000_000)).unwrap(),
            current.clock_quality(),
            current.state(),
        )
        .unwrap(),
    );
    assert!(bad.validate_measurement_resolution(&completion).is_err());
    // Naming a terminal or a resolution is rejected even with its exact digest.
    for target in [&literal("empty-eose-auth-utc-rollback"), &resolution] {
        let mut id = resolved_completion.source_record_id();
        id.sequence = Sequence::new(10).unwrap();
        let bad = Fact::new(
            id,
            resolved_completion.visibility_scope_id(),
            resolved_completion.recorded_at(),
            FactPayload::Measurement(with_state(
                current,
                MeasurementState::Resolution {
                    prior_unknown: target.fact_ref(),
                    terminal: original,
                },
            )),
        )
        .unwrap();
        assert!(bad.validate_measurement_resolution(target).is_err());
    }
}

#[test]
fn unknown_samples_can_be_filled_but_known_samples_and_clock_identity_cannot_change() {
    let completion = literal("completion-unknown-prefix-pending");
    let current = measurement(&completion);
    let MeasurementState::CompletionUnknown(mut partial) = current.state() else {
        panic!()
    };
    partial.execution.observed_at = None;
    partial
        .read
        .first_valid_event
        .as_mut()
        .unwrap()
        .received
        .observed_at = None;
    let prior = replace(
        &completion,
        with_state(current, MeasurementState::CompletionUnknown(partial)),
    );
    let literal = literal("resolve-completion-unknown");
    let terminal = terminal(&literal);
    let value = with_state(
        measurement(&literal),
        MeasurementState::Resolution {
            prior_unknown: prior.fact_ref(),
            terminal,
        },
    );
    let resolved = replace(&literal, value);
    resolved.validate_measurement_resolution(&prior).unwrap();
    let value = measurement(&resolved);
    let mut clock = value.clock_quality().clock_id();
    clock.ordinal = Ordinal::new(3).unwrap();
    let changed = Measurement::new(
        value.attempt_id(),
        value.context(),
        value.profile(),
        ClockQuality::new(clock, DurationUs::new(1), None, 0).unwrap(),
        value.state(),
    )
    .unwrap();
    assert!(
        replace(&resolved, changed)
            .validate_measurement_resolution(&prior)
            .is_err()
    );
}

#[test]
fn dictionary_dependencies_require_passive_method_and_exact_complete_mappings() {
    let fact = literal("execution-unknown");
    let value = measurement(&fact);
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dictionary-v1.json")).unwrap();
    let named = |name: &str| {
        let row = fixtures["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["family"] == "fact" && r["name"] == name)
            .unwrap();
        let decoded = decode(&bytes(row["preimage_hex"].as_str().unwrap())).unwrap();
        let FactPayload::Dictionary(entry) = decoded.payload() else {
            panic!()
        };
        entry.clone()
    };
    let mut entries = vec![
        named("endpoint"),
        named("observer-known"),
        named("direct-route-public"),
        named("access-public"),
        named("passive-ws-read"),
    ];
    let mut clock = entries[1].reference();
    clock.ordinal = Ordinal::new(2).unwrap();
    entries.push(DictionaryEntry::new(clock, entries[1].descriptor().clone()).unwrap());
    value
        .validate_dictionary_dependencies(|reference| {
            entries.iter().find(|e| e.reference() == reference)
        })
        .unwrap();
    for missing in value.references().map(|(reference, _)| reference) {
        assert!(
            value
                .validate_dictionary_dependencies(|reference| {
                    entries
                        .iter()
                        .find(|e| e.reference() == reference && reference != missing)
                })
                .is_err()
        );
    }
    assert!(value.validate_dictionary_dependencies(|_| None).is_err());
    let method = value.context().method_id;
    let wrong = DictionaryEntry::new(method, DictionaryDescriptor::EventVerification).unwrap();
    assert!(
        value
            .validate_dictionary_dependencies(|r| if r == method {
                Some(&wrong)
            } else {
                entries.iter().find(|e| e.reference() == r)
            })
            .is_err()
    );
    let mut context = value.context();
    context.route_id.dictionary_kind = DictionaryKind::Endpoint;
    assert!(
        Measurement::new(
            value.attempt_id(),
            context,
            value.profile(),
            value.clock_quality(),
            value.state()
        )
        .is_err()
    );
}
