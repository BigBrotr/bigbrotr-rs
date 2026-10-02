use super::*;

#[test]
fn request_normalization_is_closed_and_effective_page_size_is_bound() {
    let request = parse_request("kind=export&state=paused&created_from_us=-100&created_before_us=200&page_size=999&cursor=abc-_12").unwrap();
    assert_eq!(request.query.page_size(), 200);
    assert_eq!(request.query.state(), Some(ExportState::Paused));
    assert_eq!(request.query.created_from_us(), Some(-100));
    assert_eq!(request.query.created_before_us(), Some(200));
    assert_eq!(
        request.cursor.as_deref().map(|v| v.as_str()),
        Some("abc-_12")
    );
    assert_eq!(
        parse_request("").unwrap().query,
        parse_request("kind=export&page_size=50").unwrap().query
    );
    for query in [
        "page_size=0",
        "page_size=01",
        "page_size=65536",
        "kind=import",
        "state=nope",
        "cursor=",
        "cursor=abc=",
        "cursor=%61bc",
        "cursor=a&cursor=b",
        "created_from_us=-0",
        "created_before_us=+1",
        "created_from_us=5&created_before_us=5",
        "created_from_us=9223372036854775808",
        "unknown=x",
        "state=paused&state=paused",
        "&",
        "page_size=2&",
    ] {
        assert!(parse_request(query).is_err(), "{query}");
    }
    assert!(parse_request(&format!("cursor={}", "a".repeat(4097))).is_err());
}

fn expected_cursor() -> Continuation {
    Continuation {
        actor: PrincipalId::try_from([0x11; 16]).unwrap(),
        policy_revision: 5,
        grant_revision: 7,
        query: JobListQuery::new(Some(ExportState::Paused), Some(-100), Some(200), 2).unwrap(),
        admission_cut: 42,
        position: Position {
            created_at_us: 50,
            job: JobId::try_from([0x66; 16]).unwrap(),
        },
        issued_at_us: 1_000_000,
        expires_at_us: 901_000_000,
    }
}
fn fixture_bytes() -> Vec<u8> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/management/job-cursor-vectors.json"
    ))
    .unwrap();
    fixture["plaintext_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn literal_cursor_bytes_preserve_all_bindings_without_sliding_time() {
    let expected = expected_cursor();
    let bytes = fixture_bytes();
    assert_eq!(&expected.encode()[..], &bytes);
    let decoded = Continuation::decode(&bytes).unwrap();
    assert_eq!(decoded.actor, expected.actor);
    assert_eq!(decoded.policy_revision, 5);
    assert_eq!(decoded.grant_revision, 7);
    assert_eq!(decoded.query, expected.query);
    assert_eq!(decoded.admission_cut, 42);
    assert_eq!(decoded.position, expected.position);
    assert_eq!(
        (decoded.issued_at_us, decoded.expires_at_us),
        (1_000_000, 901_000_000)
    );
    assert_eq!(
        decoded.validate(
            expected.actor,
            5,
            7,
            expected.query,
            901_000_001,
            30_000_000
        ),
        Ok(())
    );
    assert_eq!(
        decoded.validate(
            expected.actor,
            5,
            7,
            expected.query,
            931_000_000,
            30_000_000
        ),
        Err(SessionError::CursorExpired)
    );
    assert_eq!(
        decoded.validate(
            expected.actor,
            5,
            7,
            expected.query,
            -29_000_001,
            30_000_000
        ),
        Err(SessionError::CursorInvalid)
    );
    assert_eq!(
        decoded.validate(expected.actor, 6, 7, expected.query, 2_000_000, 0),
        Err(SessionError::CursorIncompatible)
    );
    assert_eq!(
        decoded.validate(expected.actor, 5, 8, expected.query, 2_000_000, 0),
        Err(SessionError::CursorIncompatible)
    );
    assert_eq!(
        decoded.validate(
            PrincipalId::try_from([0x22; 16]).unwrap(),
            5,
            7,
            expected.query,
            2_000_000,
            0
        ),
        Err(SessionError::CursorIncompatible)
    );
    let other = JobListQuery::new(None, Some(-100), Some(200), 2).unwrap();
    assert_eq!(
        decoded.validate(expected.actor, 5, 7, other, 2_000_000, 0),
        Err(SessionError::CursorIncompatible)
    );
}

#[test]
fn cursor_rejects_noncanonical_options_lengths_and_invalid_ranges() {
    let wire = fixture_bytes();
    for length in 0..wire.len() {
        assert!(Continuation::decode(&wire[..length]).is_err());
    }
    let mut longer = wire.clone();
    longer.push(0);
    assert!(Continuation::decode(&longer).is_err());
    for (offset, value) in [(0, 2), (1, 0), (34, 11), (35, 2), (44, 2)] {
        let mut invalid = wire.clone();
        invalid[offset] = value;
        assert!(Continuation::decode(&invalid).is_err());
    }
    for range in [2..18, 18..26, 26..34, 53..55, 55..63, 71..87] {
        let mut invalid = wire.clone();
        invalid[range].fill(0);
        assert!(Continuation::decode(&invalid).is_err());
    }
    let mut absent = wire.clone();
    absent[35] = 0;
    assert!(Continuation::decode(&absent).is_err());
    absent[36..44].fill(0);
    assert!(Continuation::decode(&absent).is_ok());
    let mut expired = wire.clone();
    expired[95..103].copy_from_slice(&1_000_000i64.to_be_bytes());
    assert!(Continuation::decode(&expired).is_err());
    expired[95..103].copy_from_slice(&i64::MAX.to_be_bytes());
    assert!(Continuation::decode(&expired).is_err());
    let mut outside = wire;
    outside[63..71].copy_from_slice(&200i64.to_be_bytes());
    assert!(Continuation::decode(&outside).is_err());
}

#[test]
fn maximum_page_has_literal_wire_fields_and_keeps_its_memory_until_body_drop() {
    use crate::management::jobs::JobReason;
    use crate::runtime::{
        ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    };
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            PAGE_RESERVATION,
            0,
            ResourceLimits::new(PAGE_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let permit = budget
        .try_acquire(ResourceRequest::data(PAGE_RESERVATION, 1, 0).unwrap())
        .unwrap();
    let items = (0..200)
        .map(|_| JobObservation {
            kind: crate::management::jobs::JobKind::Export,
            job_id: JobId::try_from([0xaa; 16]).unwrap(),
            state: ExportState::CancelRequested,
            job_revision: u64::MAX,
            checkpoint_revision: u64::MAX,
            created_at_us: i64::MIN,
            completed_at_us: Some(i64::MIN),
            observed_at_us: i64::MIN,
            reason: Some(JobReason::ExecutionRefused),
        })
        .collect();
    let body = Page {
        items,
        next_cursor: None,
        effective_page_size: 200,
        observed_at_us: i64::MIN,
        permit,
    }
    .encode()
    .unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert_eq!(budget.snapshot().used().data_bytes(), PAGE_RESERVATION);
    assert!(body.as_ref().len() < RESPONSE_BYTES);
    let value: serde_json::Value = serde_json::from_slice(body.as_ref()).unwrap();
    let expected = serde_json::json!({
        "format":"bigbrotr-job-status/1", "job_id":"aa".repeat(16), "kind":"export",
        "state":"cancel_requested", "job_revision":"18446744073709551615",
        "checkpoint_revision":"18446744073709551615", "created_at_us":"-9223372036854775808",
        "completed_at_us":"-9223372036854775808", "observed_at_us":"-9223372036854775808",
        "reason_code":"execution_refused", "authority":"observation-only"
    });
    assert_eq!(
        value,
        serde_json::json!({
            "format":"bigbrotr-job-list/1", "items":vec![expected;200], "effective_page_size":200,
            "next_cursor":null,"observed_at_us":"-9223372036854775808","authority":"observation-only"
        })
    );
    drop(body);
    assert_eq!(budget.snapshot().active_permits(), 0);
}
