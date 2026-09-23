//! Real password -> certified native visibility -> atomic grant/revoke fixture.
use super::*;
use bigbrotr::{
    management::{
        credentials::HASH_RESERVATION,
        grants::{GrantCommand, GrantReceipt},
        sessions::{BrowserSecret, IssuedSession, SessionError, SessionLifetime},
    },
    storage::postgres::{
        catalog_guard::CatalogCapture,
        control_owner::{ArchivePinRequest, ScopeGrantAllocation},
    },
};
use bigbrotr_model::{CommandKey, ReauthenticationId};
use bigbrotr_protocol::{archive_cut::CutBound, archive_visibility::VisibilityRead};
const PASSWORD: &[u8] = b"Independent owner password 2026";
fn hash_permit() -> OwnedPermit {
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
pub async fn prepare(control: &str) {
    let (mut admin, driver) = connect(control, "postgres").await;
    migration_runner::run(&mut admin, 810, true, &Cell::new(false))
        .await
        .unwrap();
    admin.batch_execute(r#"
        INSERT INTO control.principals VALUES(decode(repeat('10',16),'hex'),decode(repeat('01',16),'hex'),'local',0,'fixture',0);
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            VALUES(decode(repeat('10',16),'hex'),decode(repeat('01',16),'hex'),'alice',true,true,1,1);
        INSERT INTO control.local_credentials VALUES(decode(repeat('10',16),'hex'),decode(repeat('01',16),'hex'),1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex'));
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,action,'dataset',dataset_id FROM control.local_accounts,
            unnest(ARRAY['data.read','status.read','roles.control','budgets.change','policy.change','principals.manage','audit.read']) action;
        GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;
        GRANT INSERT,UPDATE ON control.browser_sessions,control.login_buckets,control.local_accounts,control.local_credentials,control.scope_grants TO bigbrotr_control;
        GRANT INSERT ON control.reauthentication_proofs,control.scope_grant_changes TO bigbrotr_control;
        GRANT DELETE ON control.reauthentication_proofs,control.browser_sessions TO bigbrotr_control;
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;
    "#).await.unwrap();
    // PostgreSQL row locks require UPDATE on at least one column, even though
    // this immutable table is only inserted, locked and consumed by Control.
    let denied = admin.batch_execute("BEGIN; SET LOCAL ROLE bigbrotr_control; SELECT proof_id FROM control.reauthentication_proofs FOR UPDATE").await.unwrap_err();
    assert_eq!(denied.code().unwrap().code(), "42501");
    admin.batch_execute("ROLLBACK").await.unwrap();
    // Commit provisioning separately: a later ROLLBACK in the same simple-query
    // message can also undo commands preceding its explicit BEGIN.
    admin
        .batch_execute(
            "GRANT UPDATE(proof_id) ON control.reauthentication_proofs TO bigbrotr_control",
        )
        .await
        .unwrap();
    admin.batch_execute("BEGIN; SET LOCAL ROLE bigbrotr_control; SELECT proof_id FROM control.reauthentication_proofs FOR UPDATE; ROLLBACK").await.unwrap();
    assert!(admin.query_one("SELECT has_column_privilege('bigbrotr_control','control.reauthentication_proofs','proof_id','UPDATE')", &[]).await.unwrap().get::<_,bool>(0));
    let values:Vec<String>=admin.query("SELECT encode(control.command_u64_bytes(n::control.u64),'hex') FROM (VALUES (0::numeric),(1),(9223372036854775807),(9223372036854775808),(18446744073709551614),(18446744073709551615)) t(n)",&[]).await.unwrap().iter().map(|r|r.get(0)).collect();
    assert_eq!(
        values,
        [
            "0000000000000000",
            "0000000000000001",
            "7fffffffffffffff",
            "8000000000000000",
            "fffffffffffffffe",
            "ffffffffffffffff"
        ]
    );
    let rounded: String = admin.query_one("SELECT encode(decode(lpad(to_hex(trunc(18446744073709551615::numeric/4294967296)::bigint),8,'0')||lpad(to_hex(mod(18446744073709551615::numeric,4294967296)::bigint),8,'0'),'hex'),'hex')",&[]).await.unwrap().get(0);
    assert_ne!(
        rounded, "ffffffffffffffff",
        "negative control: rounded numeric division is not unsigned integer division"
    );
    let before = state(&admin).await;
    let credentials: String = admin.query_one("SELECT jsonb_agg(to_jsonb(c) ORDER BY principal_id)::text FROM control.local_credentials c", &[]).await.unwrap().get(0);
    migration_runner::run(&mut admin, 811, true, &Cell::new(false))
        .await
        .unwrap();
    assert_eq!(state(&admin).await, before);
    assert_eq!(admin.query_one("SELECT jsonb_agg(to_jsonb(c) ORDER BY principal_id)::text FROM control.local_credentials c", &[]).await.unwrap().get::<_,String>(0), credentials);
    drop(admin);
    driver.await.unwrap().unwrap();
}
fn command(enabled: bool, revision: u64) -> GrantCommand {
    serde_json::from_value(serde_json::json!({"principal_id":"10".repeat(16),"action":"history.read",
        "scope":{"dataset_id":"01".repeat(16),"dictionary_kind":4,"allocation_domain":"04".repeat(16),"ordinal":"1"},
        "enabled":enabled,"expected_config_revision":"1","expected_policy_revision":"1","expected_grant_revision":revision.to_string(),
        "allocation":if enabled {serde_json::json!({"catalog_id":"5a".repeat(16),"frontier_revision":"1"})} else {serde_json::Value::Null}})).unwrap()
}
async fn proof(
    owner: &mut ControlSession,
    session: &IssuedSession,
    key: CommandKey,
    cmd: &GrantCommand,
) -> ReauthenticationId {
    let challenge = owner
        .begin_browser_reauthentication(
            &session.session,
            &session.csrf,
            key,
            cmd,
            "127.0.0.1".parse().unwrap(),
        )
        .await
        .unwrap();
    let verified = challenge.verify(PASSWORD, &mut hash_permit()).unwrap();
    owner
        .finish_browser_reauthentication(verified)
        .await
        .unwrap()
        .id
}
async fn state(admin: &Client) -> String {
    admin.query_one("SELECT jsonb_build_object('commands',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.management_commands t),'details',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.scope_grant_changes t),'grants',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY ordinal) FROM control.scope_grants t),'audit',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY audit_id) FROM control.management_audit t),'proofs',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY proof_id) FROM control.reauthentication_proofs t),'accounts',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY principal_id) FROM control.local_accounts t))::text",&[]).await.unwrap().get(0)
}
fn case(name: &str) {
    emit(serde_json::json!({"case":name,"status":"PASS"}));
}
#[allow(clippy::too_many_arguments)]
pub async fn run(
    owner: &mut ControlSession,
    manager: &mut Client,
    coordinator: &LocalCoordinator,
    installation: &WorkerInstallation,
    home: &Path,
    budget: &ResourceBudget,
    database: &str,
    password: &str,
    cancel: &AtomicBool,
) {
    let (admin, driver) = connect("bbpub_control_normal", "postgres").await;
    owner.refresh(validate).await.unwrap();
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let session = owner
        .finish_browser_login(
            challenge.verify(PASSWORD, &mut hash_permit()).unwrap(),
            SessionLifetime::default(),
        )
        .await
        .unwrap();
    let enable = command(true, 1);
    let key = CommandKey::try_from([0xc1; 16]).unwrap();
    let first_proof = proof(owner, &session, key, &enable).await;
    let before = state(&admin).await;
    let immutable = admin.batch_execute("BEGIN; SET LOCAL ROLE bigbrotr_control; UPDATE control.reauthentication_proofs SET proof_id=proof_id").await.unwrap_err();
    assert_eq!(immutable.code().unwrap().code(), "23514");
    admin.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(state(&admin).await, before);
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &BrowserSecret::parse_hex(&[b'1'; 64]).unwrap(),
                key,
                &enable,
                first_proof,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Denied)
    );
    assert_eq!(state(&admin).await, before);
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &enable,
                first_proof,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::ArchiveUnavailable)
    );
    assert_eq!(state(&admin).await, before);
    case("scope-grant-csrf-and-missing-native-evidence-refuse-without-durable-effects");
    let capture = CatalogCapture::at_snapshot(manager, coordinator, 3, Duration::from_secs(120))
        .await
        .unwrap();
    let visibility = preparation(
        installation,
        &directory(home.join("scope-visibility-temp")),
        cancel,
    )
    .read_visibility(
        VisibilityRead {
            dataset: dataset(),
            scope: enable.scope,
            topology_generation: TopologyGeneration::new(0),
            bounds: vec![CutBound {
                shard_id: 0,
                owner_epoch: 1,
                complete_through: 10,
            }],
            snapshot_id: 3,
        },
        coordinator,
        &CatalogRoute {
            address: "127.0.0.1".parse().unwrap(),
            port: 5432,
            database,
            tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
        },
        CatalogReaderCredentials {
            login: "bigbrotr_receipt_reader",
            password,
        },
        budget,
    )
    .unwrap();
    assert_eq!(visibility.entry().reference(), enable.scope);
    assert_eq!(visibility.fact_ref().source_record_id.sequence.get(), 1);
    let old_pin = owner
        .register_archive_pin(
            &capture,
            &ArchivePinRequest {
                lease_id: [0xf6; 16],
                frontier_revision: 1,
                ttl: Duration::from_secs(30),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    owner
        .release_archive_pin(&old_pin, &Cell::new(false))
        .await
        .unwrap();
    let stale = ScopeGrantAllocation {
        capture: &capture,
        visibility: &visibility,
        pin: &old_pin,
    };
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &enable,
                first_proof,
                Some(&stale),
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    assert_eq!(state(&admin).await, before);
    case("scope-grant-historical-active-receipt-cannot-reuse-a-released-pin");
    let expiring = owner
        .register_archive_pin(
            &capture,
            &ArchivePinRequest {
                lease_id: [0xf5; 16],
                frontier_revision: 1,
                ttl: Duration::from_millis(500),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    admin.batch_execute("CREATE SEQUENCE public.scope_delay_marker; CREATE FUNCTION public.delay_scope_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.scope_delay_marker'); PERFORM pg_sleep(1.3); RETURN NULL; END $$; CREATE CONSTRAINT TRIGGER a_scope_delay AFTER INSERT ON control.management_audit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.action='scope-grant.change') EXECUTE FUNCTION public.delay_scope_commit(); GRANT USAGE ON SCHEMA public TO bigbrotr_control; GRANT USAGE ON SEQUENCE public.scope_delay_marker TO bigbrotr_control").await.unwrap();
    let attempted = Cell::new(false);
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &enable,
                first_proof,
                Some(&ScopeGrantAllocation {
                    capture: &capture,
                    visibility: &visibility,
                    pin: &expiring
                }),
                &attempted
            )
            .await,
        Err(SessionError::Denied)
    );
    assert!(attempted.get());
    assert_eq!(
        state(&admin).await,
        before,
        "explicit COMMIT expiry rolls back proof, audit, command, account and grant"
    );
    let marker: bool = admin
        .query_one(
            "SELECT is_called AND last_value=1 FROM public.scope_delay_marker",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        marker,
        "nontransactional marker proves the delayed COMMIT path ran"
    );
    owner.refresh(validate).await.unwrap();
    admin.batch_execute("DROP TRIGGER a_scope_delay ON control.management_audit; DROP FUNCTION public.delay_scope_commit(); DROP SEQUENCE public.scope_delay_marker").await.unwrap();
    owner
        .release_archive_pin(&expiring, &Cell::new(false))
        .await
        .unwrap();
    case("scope-grant-pin-expiry-at-observed-commit-rolls-back-and-keeps-control-usable");
    let pin = owner
        .register_archive_pin(
            &capture,
            &ArchivePinRequest {
                lease_id: [0xf7; 16],
                frontier_revision: 1,
                ttl: Duration::from_secs(30),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    let allocation = ScopeGrantAllocation {
        capture: &capture,
        visibility: &visibility,
        pin: &pin,
    };
    // The deferred-expiry fault is gone. Exercise SQL omissions on this same
    // real Control backend, then roll them back before the ordinary acceptance.
    admin
        .batch_execute(include_str!(
            "../../../../experiments/archive-engine/scope_grant_omissions.sql"
        ))
        .await
        .unwrap();
    let accepted = owner
        .accept_scope_grant(
            &session.session,
            &session.csrf,
            key,
            &enable,
            first_proof,
            Some(&allocation),
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(accepted.created && accepted.enabled);
    assert_eq!(
        (
            accepted.scope_generation.get(),
            accepted.account_grant_revision.get()
        ),
        (1, 2)
    );
    let outcomes: Vec<String> = admin.query("SELECT case_name||':'||phase||':'||sqlstate FROM public.scope_fixture_results ORDER BY case_name", &[]).await.unwrap().iter().map(|r|r.get(0)).collect();
    assert_eq!(
        outcomes,
        [
            "account:constraints:23514",
            "audit:command:23503",
            "command:detail:P0002",
            "detail:grant:23514",
            "grant:constraints:23514",
            "none:positive-rollback:BB099",
            "proof:constraints:23514"
        ]
    );
    admin.batch_execute("DROP TRIGGER a_scope_omissions ON control.management_audit; DROP FUNCTION public.scope_fixture_omissions(); DROP FUNCTION public.scope_fixture_state()").await.unwrap();
    case("scope-grant-complete-positive-and-six-omissions-restore-full-state-and-xmin");
    let durable = state(&admin).await;
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &enable,
                first_proof,
                None,
                &Cell::new(false)
            )
            .await
            .unwrap(),
        GrantReceipt {
            created: false,
            ..accepted.clone()
        }
    );
    assert_eq!(state(&admin).await, durable);
    let altered = command(false, 1);
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &altered,
                first_proof,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    assert_eq!(state(&admin).await, durable);
    case("scope-grant-real-password-native-allocation-self-cas-and-identical-retry");
    drop(visibility);
    owner
        .release_archive_pin(&pin, &Cell::new(false))
        .await
        .unwrap();
    capture.finish().await.unwrap();
    let revoke = command(false, 2);
    let revoke_key = CommandKey::try_from([0xc2; 16]).unwrap();
    let revoke_proof = proof(owner, &session, revoke_key, &revoke).await;
    let revoked = owner
        .accept_scope_grant(
            &session.session,
            &session.csrf,
            revoke_key,
            &revoke,
            revoke_proof,
            None,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(!revoked.enabled && revoked.created);
    assert_eq!(
        (
            revoked.scope_generation.get(),
            revoked.account_grant_revision.get()
        ),
        (2, 3)
    );
    let durable = state(&admin).await;
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &enable,
                first_proof,
                None,
                &Cell::new(false)
            )
            .await
            .unwrap(),
        GrantReceipt {
            created: false,
            ..accepted.clone()
        }
    );
    assert_eq!(state(&admin).await, durable);
    admin
        .batch_execute("DELETE FROM control.principal_grants WHERE action='principals.manage'")
        .await
        .unwrap();
    assert_eq!(
        owner
            .accept_scope_grant(
                &session.session,
                &session.csrf,
                key,
                &enable,
                first_proof,
                None,
                &Cell::new(false)
            )
            .await
            .unwrap(),
        GrantReceipt {
            created: false,
            ..accepted
        }
    );
    assert_eq!(state(&admin).await, durable);
    case("scope-grant-revoke-without-archive-and-historical-enable-retry-never-reapplies");
    let rows=admin.query_one("SELECT (SELECT count(*) FROM control.management_commands WHERE action='scope-grant.change'),(SELECT count(*) FROM control.scope_grant_changes),(SELECT count(*) FROM control.management_audit WHERE action='scope-grant.change'),(SELECT count(*) FROM control.reauthentication_proofs),(SELECT count(*) FROM control.scope_grants WHERE NOT enabled AND generation=2),(SELECT grant_revision::text FROM control.local_accounts WHERE username='alice')",&[]).await.unwrap();
    assert_eq!(
        (
            rows.get::<_, i64>(0),
            rows.get::<_, i64>(1),
            rows.get::<_, i64>(2),
            rows.get::<_, i64>(3),
            rows.get::<_, i64>(4),
            rows.get::<_, String>(5)
        ),
        (2, 2, 2, 0, 1, "3".into())
    );
    let fingerprints: Vec<String> = admin.query("SELECT encode(request_digest,'hex') FROM control.management_commands WHERE action='scope-grant.change' ORDER BY request_key", &[]).await.unwrap().iter().map(|r|r.get(0)).collect();
    // Frozen separately with Python struct.pack/hashlib, never SQL or GrantCommand.
    assert_eq!(
        fingerprints,
        [
            "255d4ff62015e9202e1ee0d6425ce0c48998fdfa2186046de5471e20a033c533",
            "4b2800db8cadbc4ae1dbc1de39ab51a63c2056613ef74d982ba9562ea2d02a66"
        ]
    );
    drop(admin);
    driver.await.unwrap().unwrap();
}
