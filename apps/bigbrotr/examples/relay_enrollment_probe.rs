//! Isolated actual-session/Control acceptance; never an installed user command.
use bigbrotr::{
    management::{
        credentials::HASH_RESERVATION,
        grants::Revision,
        relay_enrollment::{EnrollmentCommand, EnrollmentReceipt},
        sessions::{BrowserSecret, SessionError, SessionLifetime},
    },
    network::{DirectPolicy, PrivateRule, RouteSelection},
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::postgres::control_owner::{ClaimRequest, ControlSession, EnrollmentRoutes},
};
use bigbrotr_model::{
    AllocationDomainId, CommandKey, DatasetId, DictionaryKind, Ordinal, ProcessIncarnation,
    QualifiedRef,
};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{cell::Cell, time::Duration};
use tokio_postgres::{Config, NoTls};
#[path = "support/relay_policy.rs"]
mod relay_policy;
fn permit() -> OwnedPermit {
    ResourceBudget::new(
        ProcessEnvelope::new(
            HASH_RESERVATION,
            0,
            ResourceLimits::new(HASH_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
    .try_acquire(ResourceRequest::data(HASH_RESERVATION, 1, 0).unwrap())
    .unwrap()
}
fn key(n: u8) -> CommandKey {
    CommandKey::try_from([n; 16]).unwrap()
}
fn revision(n: u64) -> Revision {
    Revision::try_from(n.to_string()).unwrap()
}
fn record(name: &str, receipt: Option<&EnrollmentReceipt>) {
    println!(
        "{}",
        serde_json::json!({"case":name,"status":"PASS","receipt":receipt})
    );
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(90), run())
        .await
        .expect("finite enrollment probe");
}
async fn run() {
    let args: Vec<_> = std::env::args().collect();
    let mut config = Config::new();
    config
        .host("postgres")
        .user("bigbrotr_control")
        .dbname(&args[1])
        .password(std::fs::read(&args[2]).unwrap());
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea,instance_id::bytea FROM control.datasets",
            &[],
        )
        .await
        .unwrap();
    let dataset =
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(0)).unwrap()).unwrap();
    let instance =
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(1)).unwrap()).unwrap();
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance,
            authority_mode: "managed".into(),
            role_instance_id: [111; 16],
            incarnation: ProcessIncarnation::try_from([112; 16]).unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        relay_policy::validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge
        .verify(b"Independent owner password 2026", &mut permit())
        .unwrap();
    let session = owner
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    // Separate laboratory administrator. Product commands still use only the
    // actual non-superuser Control login. No secret is printed or put in argv.
    config
        .user("postgres")
        .password(std::fs::read(&args[4]).unwrap());
    let (admin, admin_connection) = config.connect(NoTls).await.unwrap();
    let admin_driver = tokio::spawn(admin_connection);
    let url = Endpoint::parse("wss://fixture.example/private?x=1&x=2").unwrap();
    let registered = owner
        .register_relay_endpoint(&url, &Cell::new(false))
        .await
        .unwrap();
    let roots = DirectPolicy::private(vec![
        PrivateRule::hostname("fixture.example", "10.2.0.0/16", 443).unwrap(),
    ])
    .unwrap();
    let allowed = EnrollmentRoutes {
        public_enabled: true,
        private_ceiling: Some(&roots),
    };
    let removed = EnrollmentRoutes {
        public_enabled: false,
        private_ceiling: None,
    };
    let original = EnrollmentCommand {
        endpoint_id: registered.identity,
        visibility: QualifiedRef {
            dataset_id: dataset,
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: AllocationDomainId::try_from([55; 16]).unwrap(),
            ordinal: Ordinal::new(7).unwrap(),
        },
        route: serde_json::from_str(r#"{"class":"private","cidr":"10.2.1.0/24"}"#).unwrap(),
        enabled: true,
        expected_generation: None,
        expected_config_revision: revision(1),
        expected_policy_revision: revision(1),
    };
    let started = Cell::new(false);
    let first = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            key(1),
            &original,
            allowed,
            &started,
        )
        .await
        .unwrap();
    assert!(first.created && started.get() && first.generation.get() == 1 && first.enabled);
    record("private-enable", Some(&first));
    let mut expected = first.clone();
    expected.created = false;
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(1),
                &original,
                removed,
                &Cell::new(false)
            )
            .await
            .unwrap(),
        expected
    );
    record("historical-retry-with-roots-removed", Some(&expected));
    let mut altered = original.clone();
    altered.route = serde_json::from_str(r#"{"class":"private","cidr":"10.0.0.0/8"}"#).unwrap();
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(1),
                &altered,
                allowed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(2),
                &altered,
                allowed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Forbidden)
    );
    record("changed-key-digest-and-wider-prefix-refused", None);
    let mut withdrawn = original.clone();
    withdrawn.enabled = false;
    withdrawn.expected_generation = Some(revision(1));
    let second = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            key(3),
            &withdrawn,
            removed,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert_eq!(
        (
            second.access_scope_id,
            second.route_id,
            second.generation.get(),
            second.enabled
        ),
        (first.access_scope_id, first.route_id, 2, false)
    );
    record("withdraw-without-root", Some(&second));
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(1),
                &original,
                removed,
                &Cell::new(false)
            )
            .await
            .unwrap(),
        expected
    );
    let persisted=admin.query_one("SELECT enabled,generation::text FROM control.relay_enrollments WHERE endpoint_id=$1::bytea AND access_scope_id=$2::bytea", &[&&first.endpoint_id.as_bytes()[..],&&first.access_scope_id.as_bytes()[..]]).await.unwrap();
    assert!(!persisted.get::<_, bool>(0));
    assert_eq!(persisted.get::<_, String>(1), "2");
    record("old-enable-does-not-reapply", Some(&expected));
    let mut public = original.clone();
    public.expected_generation = Some(revision(2));
    public.route = RouteSelection::Public {};
    let third = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            key(4),
            &public,
            allowed,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert_eq!(
        (third.access_scope_id, third.generation.get(), third.enabled),
        (first.access_scope_id, 3, true)
    );
    assert_ne!(third.route_id, first.route_id);
    record("reenable-new-route-generation", Some(&third));
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(5),
                &public,
                allowed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    public.expected_generation = Some(revision(3));
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(5),
                &public,
                allowed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    let wrong = BrowserSecret::parse_hex(
        b"1111111111111111111111111111111111111111111111111111111111111111",
    )
    .unwrap();
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &wrong,
                key(1),
                &original,
                allowed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Denied)
    );
    record("stale-cas-noop-and-wrong-csrf-refused", None);
    let other = owner
        .register_relay_endpoint(
            &Endpoint::parse("wss://other.example/").unwrap(),
            &Cell::new(false),
        )
        .await
        .unwrap();
    let mut other_command = public.clone();
    other_command.expected_generation = None;
    other_command.endpoint_id = other.identity;
    let other_receipt = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            key(6),
            &other_command,
            allowed,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert_eq!(other_receipt.access_scope_id, first.access_scope_id);
    assert_ne!(other_receipt.route_id, third.route_id);
    record(
        "shared-noauth-context-different-endpoint",
        Some(&other_receipt),
    );
    let mut another_scope = original.clone();
    another_scope.visibility.ordinal = Ordinal::new(8).unwrap();
    another_scope.route = RouteSelection::Public {};
    let another_receipt = owner
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            key(7),
            &another_scope,
            allowed,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert_ne!(another_receipt.access_scope_id, first.access_scope_id);
    assert_eq!(another_receipt.route_id, third.route_id);
    record(
        "different-visibility-is-another-enrollment",
        Some(&another_receipt),
    );
    admin
        .batch_execute("DELETE FROM control.principal_grants WHERE action='policy.change'")
        .await
        .unwrap();
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(1),
                &original,
                removed,
                &Cell::new(false)
            )
            .await
            .unwrap(),
        expected
    );
    let mut disable = another_scope.clone();
    disable.enabled = false;
    disable.expected_generation = Some(revision(1));
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(8),
                &disable,
                removed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Forbidden)
    );
    admin.batch_execute("INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts WHERE username='alice'").await.unwrap();
    record(
        "historical-acceptance-survives-permission-removal-fresh-work-refuses",
        Some(&expected),
    );
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge
        .verify(b"Independent owner password 2026", &mut permit())
        .unwrap();
    let short = owner
        .finish_browser_login(
            verified,
            SessionLifetime::new(Duration::from_millis(300), Duration::from_millis(300)).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &short.session,
                &short.csrf,
                key(8),
                &disable,
                removed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Denied)
    );
    record("expired-session-refused", None);
    let state_sql = "SELECT jsonb_build_array((SELECT jsonb_agg(to_jsonb(t)) FROM control.relay_access_contexts t),(SELECT jsonb_agg(to_jsonb(t)) FROM control.network_routes t),(SELECT jsonb_agg(to_jsonb(t)) FROM control.relay_enrollments t),(SELECT jsonb_agg(to_jsonb(t)) FROM control.relay_enrollment_changes t),(SELECT jsonb_agg(to_jsonb(t)) FROM control.management_commands t),(SELECT jsonb_agg(to_jsonb(t)) FROM control.management_audit t))::text";
    let before: String = admin.query_one(state_sql, &[]).await.unwrap().get(0);
    admin.batch_execute("CREATE FUNCTION public.fail_enrollment_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture audit refusal' USING ERRCODE='23514'; END $$; CREATE TRIGGER fail_enrollment_audit BEFORE INSERT ON control.management_audit FOR EACH ROW WHEN (NEW.action='relay.enrollment.change') EXECUTE FUNCTION public.fail_enrollment_audit()").await.unwrap();
    let failed = Cell::new(false);
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(8),
                &disable,
                removed,
                &failed
            )
            .await,
        Err(SessionError::Backend)
    );
    assert!(!failed.get());
    assert_eq!(
        owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                key(1),
                &original,
                removed,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Unavailable)
    );
    drop(owner);
    driver.await.unwrap().unwrap();
    assert_eq!(
        before,
        admin
            .query_one(state_sql, &[])
            .await
            .unwrap()
            .get::<_, String>(0)
    );
    admin.batch_execute("DROP TRIGGER fail_enrollment_audit ON control.management_audit; DROP FUNCTION public.fail_enrollment_audit()").await.unwrap();
    record("audit-failure-rolls-back-poisons-and-joins-driver", None);
    drop(admin);
    admin_driver.await.unwrap().unwrap();
}
