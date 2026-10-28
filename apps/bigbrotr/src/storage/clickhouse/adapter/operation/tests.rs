use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use std::{num::NonZeroUsize, time::Duration};

fn limits() -> PhysicalLimits {
    PhysicalLimits::new(
        NonZeroUsize::new(65536).unwrap(),
        4 * 1024 * 1024,
        1000,
        1024 * 1024,
        Duration::from_secs(1),
    )
    .unwrap()
}
fn new_budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            2 * 1024 * 1024,
            0,
            ResourceLimits::new(1024 * 1024, 1, 0, 64),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    )
}
fn permit(budget: &ResourceBudget) -> OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(1024 * 1024, 1, 0).unwrap())
        .unwrap()
}
fn operation(sequence: u64) -> IndexOperationId {
    IndexOperationId::new(
        ProcessIncarnation::try_from([0x19; 16]).unwrap(),
        NonZeroU64::new(sequence).unwrap(),
    )
}
#[test]
fn unknown_join_and_caller_drop_keep_the_actual_remote_reservation() {
    let budget = new_budget();
    let slot = Arc::new(IndexEngineSlot::new());
    let id = operation(1);
    slot.reserve(id, permit(&budget), limits()).unwrap();
    let query = id.query(1);
    slot.begin_query(id, query.clone()).unwrap();
    slot.joined(id);
    // Already locally joined: dropping this caller handle cannot clear the
    // separate service-owned remote slot or claim a server cessation event.
    let caller = IndexOperation::<()> {
        lane: None,
        _source: None,
        _metadata: None,
        slot: Arc::clone(&slot),
        operation: id,
    };
    assert_eq!(caller.identity(), id);
    assert_eq!(
        caller.identity().incarnation(),
        ProcessIncarnation::try_from([0x19; 16]).unwrap()
    );
    assert_eq!(caller.identity().sequence().get(), 1);
    drop(caller);
    assert_eq!(slot.unresolved_query(), Some(query));
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert!(
        budget
            .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
            .is_err()
    );
    let extra = new_budget();
    assert_eq!(
        slot.reserve(operation(2), permit(&extra), limits()),
        Err(PhysicalError::RemoteUnresolved)
    );
    assert_eq!(extra.snapshot().active_permits(), 0);
    // Whole service teardown ends local accounting, not remote success. A new
    // process still needs retained recovery and the persistent server hard cap.
    drop(slot);
    assert_eq!(budget.snapshot().active_permits(), 0);
}
#[test]
fn complete_response_still_waits_for_join_and_old_handles_cannot_clear_next_work() {
    let budget = new_budget();
    let slot = Arc::new(IndexEngineSlot::new());
    let first = operation(1);
    slot.reserve(first, permit(&budget), limits()).unwrap();
    let query = first.query(1);
    slot.begin_query(first, query.clone()).unwrap();
    slot.completed_query(first, &query).unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    slot.joined(first);
    assert_eq!(budget.snapshot().active_permits(), 0);
    let old = IndexOperation::<()> {
        lane: None,
        _source: None,
        _metadata: None,
        slot: Arc::clone(&slot),
        operation: first,
    };
    let second = operation(2);
    slot.reserve(second, permit(&budget), limits()).unwrap();
    let second_query = second.query(1);
    slot.begin_query(second, second_query.clone()).unwrap();
    drop(old);
    assert_eq!(budget.snapshot().active_permits(), 1);
    slot.joined(second);
    assert_eq!(slot.unresolved_query(), Some(second_query));
}
#[test]
fn mismatched_completion_and_insufficient_credit_cannot_release_or_dispatch() {
    let budget = new_budget();
    let slot = IndexEngineSlot::new();
    let id = operation(1);
    let small = budget
        .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
        .unwrap();
    assert_eq!(
        slot.reserve(id, small, limits()),
        Err(PhysicalError::InsufficientReservation)
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
    let control = budget
        .try_acquire(ResourceRequest::control(64).unwrap())
        .unwrap();
    assert_eq!(
        slot.reserve(id, control, limits()),
        Err(PhysicalError::InsufficientReservation)
    );
    slot.reserve(id, permit(&budget), limits()).unwrap();
    let query = id.query(2);
    slot.begin_query(id, query.clone()).unwrap();
    assert_eq!(
        slot.completed_query(id, &id.query(1)),
        Err(PhysicalError::RemoteUnresolved)
    );
    assert_eq!(
        slot.completed_query(operation(2), &query),
        Err(PhysicalError::RemoteUnresolved)
    );
    slot.joined(id);
    assert_eq!(slot.unresolved_query(), Some(query));
    assert_eq!(budget.snapshot().active_permits(), 1);
}
#[test]
fn query_names_separate_metadata_data_attempts_and_process_incarnations() {
    let first = operation(1);
    let second = operation(2);
    let other = IndexOperationId::new(
        ProcessIncarnation::try_from([0x29; 16]).unwrap(),
        NonZeroU64::new(1).unwrap(),
    );
    let names = [
        first.query(1),
        first.query(2),
        second.query(1),
        other.query(1),
    ];
    for (i, name) in names.iter().enumerate() {
        assert!(!names[..i].contains(name));
        assert!(name.len() <= 128);
        assert!(name.bytes().all(|v| v.is_ascii_alphanumeric() || v == b'-'));
    }
}
#[test]
fn local_and_remote_bounds_are_separate_and_overflow_never_wraps() {
    let limits = limits();
    assert_eq!(limits.required_engine_data_bytes(), 1024 * 1024);
    assert!(limits.required_local_data_bytes().unwrap() > limits.required_engine_data_bytes());
    assert_eq!(limits.required_local_native_bytes(), 4 * 1024 * 1024);
    assert!(
        PhysicalLimits::new(
            NonZeroUsize::new(usize::MAX).unwrap(),
            1,
            1,
            1,
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(
        PhysicalLimits::new(
            NonZeroUsize::new(1).unwrap(),
            1,
            1,
            1,
            Duration::from_millis(1)
        )
        .is_err()
    );
}

#[test]
fn staged_wire_allowance_is_required_by_the_predispatch_local_gate() {
    let limits = limits();
    let required = limits.required_local_data_bytes().unwrap();
    // This fixture's pre-staging local grant, pinned independently of the new
    // requirement:64KiB wire, original four wire copies and its existing fixed
    // tag/core/query/driver allocations. It must no longer admit dispatch.
    let original_data = 114_032_640;
    assert_eq!(required, original_data + 65_536);
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            required + limits.required_local_native_bytes() + 64,
            0,
            ResourceLimits::new(required, 1, limits.required_local_native_bytes(), 64),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let original = budget
        .try_acquire(
            ResourceRequest::data(original_data, 1, limits.required_local_native_bytes()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        limits.check_local(&original),
        Err(PhysicalError::InsufficientReservation)
    );
    drop(original);
    let admitted = budget
        .try_acquire(
            ResourceRequest::data(required, 1, limits.required_local_native_bytes()).unwrap(),
        )
        .unwrap();
    assert_eq!(limits.check_local(&admitted), Ok(()));
    drop(admitted);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn credentials_only_target_a_pinned_numeric_address_inside_the_declared_boundary() {
    use crate::secrets::SecretFile;
    use std::{io::Write, os::unix::fs::PermissionsExt};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let path = root.join("synthetic-password");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .unwrap();
    file.write_all(b"synthetic-fixture-only").unwrap();
    drop(file);
    let reference = format!("file:{}", path.display());
    let make = |endpoint: &str, boundary| {
        IndexConnection::from_local_root(
            endpoint.into(),
            boundary,
            "index_writer".into(),
            SecretFile::parse(&reference).unwrap().read(64).unwrap(),
        )
    };
    for (endpoint, boundary, canonical) in [
        (
            "http://127.0.0.1:8123",
            PlaintextBoundary::Loopback,
            "http://127.0.0.1:8123/",
        ),
        (
            "http://[::1]:8123",
            PlaintextBoundary::Loopback,
            "http://[::1]:8123/",
        ),
        (
            "http://[::ffff:127.0.0.1]:8123",
            PlaintextBoundary::Loopback,
            "http://127.0.0.1:8123/",
        ),
        (
            "http://172.18.0.2:8123",
            PlaintextBoundary::PrivateBridge,
            "http://172.18.0.2:8123/",
        ),
        (
            "http://[fd00::2]:8123",
            PlaintextBoundary::PrivateBridge,
            "http://[fd00::2]:8123/",
        ),
    ] {
        assert_eq!(make(endpoint, boundary).unwrap().endpoint, canonical);
    }
    for endpoint in [
        "http://localhost:8123",
        "http://clickhouse:8123",
        "http://example.com:8123",
        "http://8.8.8.8:8123",
        "http://[fe80::1]:8123",
        "http://[::ffff:8.8.8.8]:8123",
        "https://127.0.0.1:8123",
        "http://127.0.0.1:0",
        "http://user@127.0.0.1:8123",
        "http://127.0.0.1:8123/?extra=1",
        "http://127.0.0.1:8123/path",
    ] {
        for boundary in [
            PlaintextBoundary::Loopback,
            PlaintextBoundary::PrivateBridge,
        ] {
            assert!(matches!(
                make(endpoint, boundary),
                Err(PhysicalError::InvalidInput)
            ));
        }
    }
    assert!(make("http://127.0.0.1:8123", PlaintextBoundary::PrivateBridge).is_err());
    assert!(
        make(
            "http://[::ffff:127.0.0.1]:8123",
            PlaintextBoundary::PrivateBridge
        )
        .is_err()
    );
    assert!(make("http://10.0.0.1:8123", PlaintextBoundary::Loopback).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn definite_preconnection_failure_releases_only_after_join() {
    // Numeric TCP port zero cannot host a listening service; no secret or
    // hostname is involved. Exercise the selected real Hyper error chain.
    let budget = new_budget();
    let slot = Arc::new(IndexEngineSlot::new());
    let id = operation(1);
    slot.reserve(id, permit(&budget), limits()).unwrap();
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(Duration::from_secs(1)));
    let client = crate::storage::clickhouse::transport::bounded_client(
        connector,
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_url("http://127.0.0.1:0");
    let mut owner = QueryOwner::new(client, Arc::clone(&slot), id);
    let result = tokio::time::timeout(Duration::from_secs(2), owner.execute("SELECT 1"))
        .await
        .unwrap();
    assert_eq!(result, Err(PhysicalError::Unavailable));
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(owner);
    slot.joined(id);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn response_errors_do_not_claim_remote_completion() {
    let budget = new_budget();
    let slot = Arc::new(IndexEngineSlot::new());
    let id = operation(1);
    slot.reserve(id, permit(&budget), limits()).unwrap();
    let mut owner = QueryOwner::new(clickhouse::Client::default(), Arc::clone(&slot), id);
    let query = owner.begin().unwrap();
    assert_eq!(
        owner.failed(
            &query,
            clickhouse::error::Error::BadResponse("synthetic response".into())
        ),
        PhysicalError::Query
    );
    drop(owner);
    slot.joined(id);
    assert_eq!(slot.unresolved_query(), Some(query));
    assert_eq!(budget.snapshot().active_permits(), 1);
}
