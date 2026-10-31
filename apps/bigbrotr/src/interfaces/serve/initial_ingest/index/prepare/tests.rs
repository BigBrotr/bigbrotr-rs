use super::*;
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::{PermissionsExt, symlink},
};

struct Fixture {
    _directory: tempfile::TempDir,
    root: Resolved,
    password: PathBuf,
    state: PathBuf,
}
fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().canonicalize().unwrap();
    let state = parent.join("index");
    fs::create_dir(&state).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    let password = parent.join("writer-password");
    fs::write(&password, b"synthetic-preparation-only").unwrap();
    fs::set_permissions(&password, fs::Permissions::from_mode(0o600)).unwrap();
    // Exact resolved inputs to this consumer, not a parser/accepted-policy
    // fixture. The ordinary preparation must never consume policy/provenance.
    let values = json!({
        "runtime.processes.clickhouse.enabled":true,
        "runtime.processes.clickhouse.memory_max_bytes":4096,
        "runtime.processes.clickhouse.baseline_bytes":256,
        "runtime.processes.clickhouse.payload_bytes":2048,
        "runtime.processes.clickhouse.native_bytes":1024,
        "runtime.processes.clickhouse.completion_reserve_bytes":128,
        "clickhouse.placement":"local",
        "clickhouse.tls.mode":"local-plaintext",
        "clickhouse.tls.local_boundary":"loopback",
        "clickhouse.tls.ca_file":null,
        "clickhouse.tls.server_name":null,
        "clickhouse.tls.client_cert_ref":null,
        "clickhouse.tls.client_key_ref":null,
        "clickhouse.endpoint":"http://127.0.0.1:8123",
        "clickhouse.database":"serving_fixture",
        "clickhouse.write_user":"fixture_writer",
        "clickhouse.read_user":"fixture_reader",
        "clickhouse.write_password_ref":format!("file:{}", password.display()),
        "clickhouse.initialize_user":"fixture_initializer",
        "clickhouse.initialize_password_ref":format!("file:{}/missing-initializer",parent.display()),
        "storage.index_root":state.to_str().unwrap(),
        "storage.ingest_root":parent.join("not-yet-created-ingest").to_str().unwrap(),
        "sync.provenance":"off"
    }).as_object().unwrap().clone().into_iter().collect();
    let root = Resolved {
        keys: vec![],
        values,
        sources: Default::default(),
        local_roles: BTreeSet::from(["index".into(), "ingest".into()]),
        dependencies: vec![],
        document_sha256: String::new(),
    };
    Fixture {
        _directory: directory,
        root,
        password,
        state,
    }
}
fn budget(bytes: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, ITEMS, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn no_dns(_: &str, _: u16) -> std::io::Result<Vec<SocketAddr>> {
    panic!("numeric preparation must not call DNS")
}
fn assert_no_journal(f: &Fixture) {
    assert!(fs::read_dir(&f.state).unwrap().next().is_none());
    assert!(!Path::new(f.root.text("storage.ingest_root").unwrap()).exists());
}
fn error(result: Result<PreparedIndex>) -> String {
    match result {
        Ok(_) => panic!("expected refusal"),
        Err((code, message)) => {
            assert_eq!(code, 3);
            assert!(!message.contains("synthetic-preparation-only"));
            message
        }
    }
}

#[test]
fn ordinary_preparation_never_opens_initializer_or_creates_state_and_retains_credit() {
    let f = fixture();
    let app = budget(PREPARATION_BYTES);
    let prepared = prepare_with(&f.root, &app, false, no_dns).unwrap();
    assert!(prepared.initializer().is_none());
    assert_eq!(prepared.database(), "serving_fixture");
    assert_eq!(prepared.state_root(), f.state);
    assert_eq!(Arc::strong_count(prepared.writer()), 1);
    assert_eq!(app.snapshot().active_permits(), 1);
    assert_eq!(app.snapshot().used().data_bytes(), PREPARATION_BYTES);
    assert_no_journal(&f);
    drop(prepared);
    assert_eq!(app.snapshot().active_permits(), 0);
}

#[test]
fn explicit_initialization_requires_the_separate_protected_pair() {
    let mut f = fixture();
    let app = budget(PREPARATION_BYTES);
    assert!(error(prepare_with(&f.root, &app, true, no_dns)).contains("credential"));
    assert_eq!(app.snapshot().active_permits(), 0);
    f.root.values.insert(
        "clickhouse.initialize_password_ref".into(),
        f.root.values["clickhouse.write_password_ref"].clone(),
    );
    let prepared = prepare_with(&f.root, &app, true, no_dns).unwrap();
    assert!(prepared.initializer().is_some());
    assert!(!Arc::ptr_eq(
        prepared.writer(),
        prepared.initializer().unwrap()
    ));
    drop(prepared);
    for user in ["fixture_writer", "fixture_reader", "default"] {
        f.root
            .values
            .insert("clickhouse.initialize_user".into(), json!(user));
        assert!(error(prepare_with(&f.root, &app, true, no_dns)).contains("separate account"));
    }
    f.root.values.insert(
        "clickhouse.initialize_user".into(),
        json!("fixture_initializer"),
    );
    f.root
        .values
        .insert("clickhouse.initialize_password_ref".into(), json!(null));
    assert!(error(prepare_with(&f.root, &app, true, no_dns)).contains("deployment input missing"));
    assert_no_journal(&f);
}

#[test]
fn reservation_and_tls_profile_refusals_precede_dns_and_secret_reads() {
    let mut f = fixture();
    fs::remove_file(&f.password).unwrap();
    f.root.values.insert(
        "clickhouse.endpoint".into(),
        json!("http://not-resolved:8123"),
    );
    let app = budget(PREPARATION_BYTES - 1);
    assert!(error(prepare_with(&f.root, &app, false, no_dns)).contains("reservation"));
    let app = budget(PREPARATION_BYTES);
    for (key, value) in [
        ("clickhouse.tls.mode", json!("verify-full")),
        ("clickhouse.placement", json!("remote")),
        ("runtime.processes.clickhouse.enabled", json!(false)),
        ("clickhouse.tls.ca_file", json!("/unused/ca.pem")),
    ] {
        let mut root = f.root.clone();
        root.values.insert(key.into(), value);
        error(prepare_with(&root, &app, false, no_dns));
        assert_eq!(app.snapshot().active_permits(), 0);
    }
    assert_no_journal(&f);
}

#[test]
fn every_dns_answer_must_match_the_fixed_boundary_and_answer_limit() {
    let mut f = fixture();
    let app = budget(PREPARATION_BYTES);
    f.root.values.insert(
        "clickhouse.endpoint".into(),
        json!("http://fixture-service:8123"),
    );
    f.root.values.insert(
        "clickhouse.tls.local_boundary".into(),
        json!("private-bridge"),
    );
    for values in [
        vec![],
        vec!["10.0.0.1:8123", "8.8.8.8:8123"],
        vec!["[::ffff:127.0.0.1]:8123"],
        vec!["[fe80::1]:8123"],
        vec!["10.0.0.1:8123"; 17],
    ] {
        let result = prepare_with(&f.root, &app, false, |host, port| {
            assert_eq!((host, port), ("fixture-service", 8123));
            Ok(values
                .into_iter()
                .map(|v| v.parse::<SocketAddr>().unwrap())
                .collect::<Vec<_>>())
        });
        assert!(error(result).contains("resolved route"));
        assert_eq!(app.snapshot().active_permits(), 0);
    }
    // Sixteen valid answers are inspected; only the selected numeric address
    // can satisfy the actual IndexConnection constructor, which refuses names.
    let prepared = prepare_with(&f.root, &app, false, |_, _| {
        Ok(vec!["[fd00::1]:8123".parse::<SocketAddr>().unwrap(); 16])
    })
    .unwrap();
    drop(prepared);
    assert_no_journal(&f);
}

#[test]
fn numeric_mapped_loopback_needs_no_resolver_and_unsafe_endpoint_forms_refuse() {
    let mut f = fixture();
    let app = budget(PREPARATION_BYTES);
    f.root.values.insert(
        "clickhouse.endpoint".into(),
        json!("http://[::ffff:127.0.0.1]:8123"),
    );
    drop(prepare_with(&f.root, &app, false, no_dns).unwrap());
    for endpoint in [
        "https://127.0.0.1:8123",
        "http://127.0.0.1:0",
        "http://user@127.0.0.1:8123",
        "http://127.0.0.1:8123/path",
        "http://127.0.0.1:8123/?x=1",
        "http://8.8.8.8:8123",
    ] {
        f.root
            .values
            .insert("clickhouse.endpoint".into(), json!(endpoint));
        error(prepare_with(&f.root, &app, false, no_dns));
        assert_eq!(app.snapshot().active_permits(), 0);
    }
}

#[test]
fn root_and_credential_refusals_preserve_input_and_return_the_grant() {
    let mut f = fixture();
    let app = budget(PREPARATION_BYTES);
    fs::set_permissions(&f.state, fs::Permissions::from_mode(0o755)).unwrap();
    error(prepare_with(&f.root, &app, false, no_dns));
    fs::set_permissions(&f.state, fs::Permissions::from_mode(0o700)).unwrap();
    let alias = f.state.with_file_name("state-alias");
    symlink(&f.state, &alias).unwrap();
    f.root
        .values
        .insert("storage.index_root".into(), json!(alias));
    error(prepare_with(&f.root, &app, false, no_dns));
    f.root
        .values
        .insert("storage.index_root".into(), json!(f.state));
    for bytes in [
        vec![b'x'; PASSWORD_BYTES + 1],
        b"synthetic\npassword".to_vec(),
        vec![0xff],
    ] {
        fs::write(&f.password, &bytes).unwrap();
        error(prepare_with(&f.root, &app, false, no_dns));
        assert_eq!(fs::read(&f.password).unwrap(), bytes);
        assert_eq!(app.snapshot().active_permits(), 0);
    }
    fs::write(&f.password, b"synthetic-preparation-only").unwrap();
    fs::set_permissions(&f.password, fs::Permissions::from_mode(0o644)).unwrap();
    error(prepare_with(&f.root, &app, false, no_dns));
    assert_no_journal(&f);
}

#[test]
fn engine_envelope_preserves_independent_control_credit_and_never_waits() {
    let f = fixture();
    let app = budget(PREPARATION_BYTES);
    let prepared = prepare_with(&f.root, &app, false, no_dns).unwrap();
    assert_eq!(
        prepared.engine().snapshot().limits(),
        ResourceLimits::new(2048, 1, 1024, 128)
    );
    let data = prepared
        .engine()
        .try_acquire(ResourceRequest::data(2048, 1, 1024).unwrap())
        .unwrap();
    assert!(
        prepared
            .engine()
            .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
            .is_err()
    );
    let control = prepared
        .engine()
        .try_acquire(ResourceRequest::control(128).unwrap())
        .unwrap();
    assert_eq!(prepared.engine().snapshot().waiting_data(), 0);
    assert_eq!(prepared.engine().snapshot().waiting_control(), 0);
    drop((data, control, prepared));
    assert_eq!(app.snapshot().active_permits(), 0);
}
