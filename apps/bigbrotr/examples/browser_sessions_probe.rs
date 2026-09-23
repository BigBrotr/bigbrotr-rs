//! Isolated real PostgreSQL adapter test. Never a product management command.
use bigbrotr::{
    management::{
        credentials::HASH_RESERVATION,
        sessions::{BrowserSecret, IssuedSession, SessionError, SessionLifetime},
    },
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::postgres::{
        control_owner::{ClaimRequest, ControlSession, OwnerError},
        migration_runner,
    },
};
use bigbrotr_model::{DatasetId, ProcessIncarnation};
use sha2::{Digest, Sha256};
use std::{cell::Cell, time::Duration};
use tokio_postgres::{Client, Config, NoTls};

const PASSWORD: &[u8] = b"Independent owner password 2026";
const POLICY: &str = r#"{"fixture":"browser-session-storage-only"}"#;
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    if serde_json::from_str::<serde_json::Value>(document).unwrap()
        != serde_json::from_str::<serde_json::Value>(POLICY).unwrap()
        || digest != <[u8; 32]>::from(Sha256::digest(POLICY))
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}
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
async fn login(owner: &mut ControlSession, lifetime: SessionLifetime) -> IssuedSession {
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge.verify(PASSWORD, &mut permit()).unwrap();
    owner
        .finish_browser_login(verified, lifetime)
        .await
        .unwrap()
}
async fn reset_buckets(admin: &Client) {
    admin
        .batch_execute("DELETE FROM control.login_buckets")
        .await
        .unwrap();
}
fn case(name: &str) {
    println!("{}", serde_json::json!({"case":name,"status":"PASS"}));
}

async fn acquire(
    config: &Config,
    identity: u8,
) -> (
    ControlSession,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, owner_driver) = config.connect(NoTls).await.unwrap();
    let owner_driver = tokio::spawn(owner_driver);
    let (owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: DatasetId::try_from([1; 16]).unwrap(),
            instance: DatasetId::try_from([2; 16]).unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [identity; 16],
            incarnation: ProcessIncarnation::try_from([identity + 10; 16]).unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    (owner, owner_driver)
}
async fn session_audit_state(admin: &Client) -> String {
    admin.query_one("SELECT json_build_array((SELECT json_agg(json_build_array(row_to_json(s),s.xmin::text) ORDER BY session_id) FROM control.browser_sessions s),(SELECT json_agg(json_build_array(row_to_json(a),a.xmin::text) ORDER BY audit_id) FROM control.management_audit a))::text", &[]).await.unwrap().get(0)
}
async fn audit_failure(admin: &Client, enabled: bool) {
    let sql = if enabled {
        "CREATE FUNCTION public.refuse_session_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit refusal' USING ERRCODE='23514'; END $$; CREATE TRIGGER refuse_session_audit BEFORE INSERT ON control.management_audit FOR EACH ROW WHEN (NEW.action IN ('browser.session.issue','browser.session.revoke')) EXECUTE FUNCTION public.refuse_session_audit()"
    } else {
        "DROP TRIGGER refuse_session_audit ON control.management_audit; DROP FUNCTION public.refuse_session_audit()"
    };
    admin.batch_execute(sql).await.unwrap();
}

#[path = "browser_sessions_probe/reauthentication.rs"]
mod reauthentication;

async fn run(mut config: Config, reauth: bool) {
    let (mut admin, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    migration_runner::run(&mut admin, 18, true, &Cell::new(false))
        .await
        .unwrap();
    admin.batch_execute(r#"
        INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(decode(repeat('01',16),'hex'),decode(repeat('02',16),'hex'),0);
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            VALUES(decode(repeat('10',16),'hex'),decode(repeat('01',16),'hex'),'local',0,'fixture',0);
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            VALUES(decode(repeat('10',16),'hex'),decode(repeat('01',16),'hex'),'alice',true,true,1,1);
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            VALUES(decode(repeat('10',16),'hex'),decode(repeat('01',16),'hex'),1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex'));
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,action,'dataset',dataset_id FROM control.local_accounts,
            unnest(ARRAY['data.read','status.read','roles.control','budgets.change','policy.change','principals.manage','audit.read']) action;
    "#).await.unwrap();
    let digest: [u8; 32] = Sha256::digest(POLICY).into();
    admin.execute("INSERT INTO control.config_revisions(revision,dataset_id,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us,actor_id) VALUES(1,decode(repeat('01',16),'hex'),1,'managed',$1::text::jsonb,$2::bytea,$2::bytea,0,decode(repeat('10',16),'hex'))", &[&POLICY,&&digest[..]]).await.unwrap();
    admin.execute("INSERT INTO control.policy_revisions(revision,dataset_id,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) VALUES(1,decode(repeat('01',16),'hex'),1,1,$1::text::jsonb,$2::bytea,0,decode(repeat('10',16),'hex'))", &[&POLICY,&&digest[..]]).await.unwrap();
    admin
        .batch_execute("UPDATE control.datasets SET config_revision=1,policy_revision=1")
        .await
        .unwrap();
    let before: String = admin
        .query_one(
            "SELECT row_to_json(c)::text FROM control.local_credentials c",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        migration_runner::run(&mut admin, 806, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        1
    );
    assert_eq!(
        before,
        admin
            .query_one(
                "SELECT row_to_json(c)::text FROM control.local_credentials c",
                &[]
            )
            .await
            .unwrap()
            .get::<_, String>(0)
    );
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.principal_grants", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        7
    );
    // A real pre-0807 revocation remains unattributed; upgrade never fabricates
    // a caller for old history. Token hashes are independent fixed test values.
    admin.batch_execute(r#"WITH t AS (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS n)
        INSERT INTO control.browser_sessions SELECT decode(repeat('d1',16),'hex'),dataset_id,principal_id,credential_revision,authentication_revision,
        decode('cbf00566adf0b082a29ee703cbd8097341e3dec290e3e7ecf6879b7d44abac4b','hex'),decode('8140f81bce52aad45ec535eeba3857c9ca7c69842b31a329e0de64e48d6be8e9','hex'),n,n+60000000,30000000,n,n+30000000,NULL FROM control.local_accounts,t;
        UPDATE control.browser_sessions SET revoked_at_us=issued_at_us;"#).await.unwrap();
    admin.batch_execute(r#"
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            VALUES(decode(repeat('f1',16),'hex'),decode(repeat('01',16),'hex'),'installation',0,'fixture',0);
        INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,reason,requester_database_role,requester_os_uid)
        SELECT decode(repeat(id,16),'hex'),decode(repeat('01',16),'hex'),decode(repeat('f1',16),'hex'),action,outcome,0,revision,CASE WHEN subject IS NULL THEN NULL ELSE decode(repeat(subject,16),'hex') END,reason,requester,uid FROM (VALUES
            ('a1','dataset.initialize','accepted',1::numeric,NULL::text,NULL::text,NULL::text,NULL::bigint),
            ('a2','policy.activate-initial','accepted',1,'f2',NULL,'fixture',0),
            ('a3','owner-token.issue','accepted',NULL,'f3',NULL,'fixture',0),
            ('a4','owner-token.revoke','accepted',NULL,'f3',NULL,'fixture',0),
            ('a5','owner.enroll','accepted',NULL,'f4',NULL,'fixture',0),
            ('a6','owner.enroll','rejected',NULL,'f5','invalid-token','fixture',0),
            ('a7','owner.enroll','rejected',NULL,'f5','expired','fixture',0)
        ) v(id,action,outcome,revision,subject,reason,requester,uid);
        INSERT INTO control.config_revisions(revision,previous_revision,dataset_id,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us,actor_id)
            SELECT 2,1,dataset_id,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us,actor_id FROM control.config_revisions WHERE revision=1;
    "#).await.unwrap();
    // ADD COLUMN of a constrained domain rewrites the heap in PostgreSQL16.
    // Logical receipt fields must remain exact; xmin is only compared across
    // ordinary mutations/retries below, not across this explicit DDL rewrite.
    let legacy_audits: String = admin.query_one("SELECT jsonb_agg(to_jsonb(a)-'policy_revision' ORDER BY audit_id)::text FROM control.management_audit a", &[]).await.unwrap().get(0);
    let legacy_physical: String = admin.query_one("SELECT json_build_array(pg_relation_filenode('control.management_audit'),array_agg(DISTINCT xmin::text))::text FROM control.management_audit", &[]).await.unwrap().get(0);
    assert_eq!(
        migration_runner::run(&mut admin, 807, true, &Cell::new(false))
            .await
            .unwrap()
            .applied,
        1
    );
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action LIKE 'browser.session.%'", &[]).await.unwrap().get::<_,i64>(0), 0);
    assert!(legacy_audits == admin.query_one("SELECT jsonb_agg(to_jsonb(a)-'policy_revision' ORDER BY audit_id)::text FROM control.management_audit a", &[]).await.unwrap().get::<_,String>(0), "upgrade changed legacy audit fields");
    let after_physical: String = admin.query_one("SELECT json_build_array(pg_relation_filenode('control.management_audit'),array_agg(DISTINCT xmin::text))::text FROM control.management_audit", &[]).await.unwrap().get(0);
    println!(
        "{}",
        serde_json::json!({"observation":"audit-upgrade-physical-state","before":serde_json::from_str::<serde_json::Value>(&legacy_physical).unwrap(),"after":serde_json::from_str::<serde_json::Value>(&after_physical).unwrap(),"legacy_fields_unchanged":true})
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM control.management_audit WHERE policy_revision IS NULL",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        7
    );
    for (config_revision, policy_revision, code) in [
        (None, Some("1"), "23514"),
        (Some("1"), None, "23514"),
        (Some("2"), Some("1"), "23503"),
    ] {
        let error=admin.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,policy_revision,subject_id) VALUES(decode(repeat('ee',16),'hex'),decode(repeat('01',16),'hex'),decode(repeat('10',16),'hex'),'browser.session.issue','accepted',0,$1::text::numeric,$2::text::numeric,decode(repeat('d1',16),'hex'))", &[&config_revision,&policy_revision]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), code);
    }
    case("audit-upgrade-preserves-legacy-shapes-and-refuses-null-or-mixed-revisions");
    migration_runner::run(&mut admin, 809, true, &Cell::new(false))
        .await
        .unwrap();
    admin.batch_execute("GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control").await.unwrap();
    case("additive-upgrade-preserves-password-and-seven-grants");
    config
        .user("bigbrotr_control")
        .password(std::fs::read("/secrets/bigbrotr_control").unwrap());
    let (mut owner, mut owner_driver) = acquire(&config, 31).await;

    if reauth {
        reauthentication::run(&mut admin, &mut owner).await;
        owner.release().await.unwrap();
        owner_driver.await.unwrap().unwrap();
        drop(admin);
        driver.await.unwrap().unwrap();
        return;
    }
    owner.require_browser_sessions().await.unwrap();
    owner
        .revoke_browser_session(
            &BrowserSecret::parse_hex(&[b'b'; 64]).unwrap(),
            &BrowserSecret::parse_hex(&[b'c'; 64]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action='browser.session.revoke'", &[]).await.unwrap().get::<_,i64>(0), 0);
    case("legacy-revocation-retry-does-not-invent-audit");
    let first = login(&mut owner, SessionLifetime::default()).await;
    // Independent durable oracle: an issued credential must have one immutable
    // attributable receipt in the same database, even if its HTTP reply is lost.
    let issuance_audits: i64 = admin
        .query_one(
            "SELECT count(*) FROM control.management_audit a JOIN control.browser_sessions s ON a.dataset_id=s.dataset_id AND a.actor_id=s.principal_id AND a.subject_id=s.session_id WHERE a.action='browser.session.issue' AND a.outcome='accepted' AND a.config_revision=1 AND a.policy_revision=1 AND a.requester_database_role IS NULL AND a.requester_os_uid IS NULL AND a.reason IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        issuance_audits, 1,
        "issued session lacks its atomic audit receipt"
    );
    let seen = owner
        .inspect_browser_session(&first.session, true)
        .await
        .unwrap();
    assert_eq!(seen.stored_actions.len(), 7);
    assert!(!seen.stored_actions.iter().any(|a| a == "history.read"));
    assert_eq!(seen.expires_at_us - first.observation.expires_at_us, 0);
    let different = BrowserSecret::parse_hex(&[b'a'; 64]).unwrap();
    assert_eq!(
        owner
            .revoke_browser_session(&first.session, &different)
            .await,
        Err(SessionError::Denied)
    );
    assert!(
        owner
            .inspect_browser_session(&first.session, false)
            .await
            .is_ok()
    );
    case("login-me-wrong-csrf-and-no-implicit-history-grant");
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        challenge.verify(b"Independent owner password WRONG", &mut permit()),
        Err(SessionError::Denied)
    ));
    owner.poll_policy(validate).await.unwrap();
    case("wrong-password-does-not-poison-control");

    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge.verify(PASSWORD, &mut permit()).unwrap();
    admin.batch_execute("UPDATE control.local_accounts SET active=false; UPDATE control.local_accounts SET active=true").await.unwrap();
    assert!(matches!(
        owner
            .finish_browser_login(verified, SessionLifetime::default())
            .await,
        Err(SessionError::Denied)
    ));
    assert!(matches!(
        owner.inspect_browser_session(&first.session, false).await,
        Err(SessionError::Denied)
    ));
    owner.poll_policy(validate).await.unwrap();
    case("deactivate-reactivate-invalidates-hash-and-old-session");

    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge.verify(PASSWORD, &mut permit()).unwrap();
    admin.batch_execute("BEGIN; UPDATE control.local_accounts SET credential_revision=2; UPDATE control.local_credentials SET credential_revision=2; COMMIT").await.unwrap();
    assert!(matches!(
        owner
            .finish_browser_login(verified, SessionLifetime::default())
            .await,
        Err(SessionError::Denied)
    ));
    let same_revision = admin
        .batch_execute(
            "UPDATE control.local_credentials SET password_hash=decode(repeat('ff',32),'hex')",
        )
        .await
        .unwrap_err();
    assert_eq!(same_revision.code().unwrap().code(), "23514");
    case("reset-during-hash-refuses-and-same-revision-replacement-rejected");
    reset_buckets(&admin).await;
    let second = login(&mut owner, SessionLifetime::default()).await;
    admin
        .batch_execute("UPDATE control.local_accounts SET grant_revision=grant_revision+1")
        .await
        .unwrap();
    assert_eq!(
        owner
            .inspect_browser_session(&second.session, false)
            .await
            .unwrap()
            .grant_revision,
        2
    );
    owner
        .revoke_browser_session(&second.session, &second.csrf)
        .await
        .unwrap();
    let audit_before_retry: String = admin.query_one("SELECT json_agg(json_build_array(row_to_json(a),a.xmin::text) ORDER BY audit_id)::text FROM control.management_audit a", &[]).await.unwrap().get(0);
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action='browser.session.revoke'", &[]).await.unwrap().get::<_,i64>(0),1);

    owner
        .revoke_browser_session(&second.session, &second.csrf)
        .await
        .unwrap();
    assert_eq!(audit_before_retry,admin.query_one("SELECT json_agg(json_build_array(row_to_json(a),a.xmin::text) ORDER BY audit_id)::text FROM control.management_audit a", &[]).await.unwrap().get::<_,String>(0));
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action='browser.session.issue'", &[]).await.unwrap().get::<_,i64>(0),2);
    for statement in [
        "UPDATE control.management_audit SET created_at_us=created_at_us+1 WHERE action='browser.session.issue'",
        "DELETE FROM control.management_audit WHERE action='browser.session.issue'",
    ] {
        assert_eq!(
            admin
                .batch_execute(statement)
                .await
                .unwrap_err()
                .code()
                .unwrap()
                .code(),
            "23514"
        );
    }
    case("logout-retry-preserves-audit-and-session-cleanup-retains-attribution");

    assert!(matches!(
        owner.inspect_browser_session(&second.session, false).await,
        Err(SessionError::Denied)
    ));
    let resurrection = admin
        .batch_execute("UPDATE control.browser_sessions SET revoked_at_us=NULL")
        .await
        .unwrap_err();
    assert_eq!(resurrection.code().unwrap().code(), "23514");
    case("grant-edit-keeps-session-logout-terminal-idempotent");

    let short = login(
        &mut owner,
        SessionLifetime::new(Duration::from_secs(10), Duration::from_secs(1)).unwrap(),
    )
    .await;
    let blocker = admin.transaction().await.unwrap();
    blocker
        .query_one(
            "SELECT principal_id FROM control.local_accounts FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
    let inspect = owner.inspect_browser_session(&short.session, true);
    let release = async {
        tokio::time::sleep(Duration::from_millis(1300)).await;
        blocker.rollback().await.unwrap();
    };
    let (result, ()) = tokio::join!(inspect, release);
    assert!(matches!(result, Err(SessionError::Denied)));
    owner.poll_policy(validate).await.unwrap();
    case("account-lock-held-beyond-idle-cannot-resurrect-by-touch");

    reset_buckets(&admin).await;
    let crossing = login(
        &mut owner,
        SessionLifetime::new(Duration::from_secs(10), Duration::from_secs(1)).unwrap(),
    )
    .await;
    admin.batch_execute("CREATE TABLE public.touch_marker(entered boolean NOT NULL); GRANT INSERT ON public.touch_marker TO bigbrotr_control; CREATE FUNCTION public.delay_touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.touch_marker VALUES(true); PERFORM pg_sleep(1.3); RETURN NEW; END $$; CREATE TRIGGER a_delay_touch BEFORE UPDATE ON control.browser_sessions FOR EACH ROW WHEN (NEW.revoked_at_us IS NULL) EXECUTE FUNCTION public.delay_touch()").await.unwrap();
    let before_touch: String = admin
        .query_one(
            "SELECT json_agg(s ORDER BY session_id)::text FROM control.browser_sessions s",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(matches!(
        owner.inspect_browser_session(&crossing.session, true).await,
        Err(SessionError::Denied)
    ));
    assert_eq!(
        before_touch,
        admin
            .query_one(
                "SELECT json_agg(s ORDER BY session_id)::text FROM control.browser_sessions s",
                &[]
            )
            .await
            .unwrap()
            .get::<_, String>(0)
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM public.touch_marker WHERE entered",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1,
        "the delayed trigger must actually run"
    );
    owner.poll_policy(validate).await.unwrap();
    admin.batch_execute("DROP TRIGGER a_delay_touch ON control.browser_sessions; DROP FUNCTION public.delay_touch(); DROP TABLE public.touch_marker").await.unwrap();
    case("expiry-crossing-between-clock-read-and-touch-is-normal-denial");

    let isolation = admin
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .unwrap();
    let refused = isolation.execute("INSERT INTO control.browser_sessions SELECT decode(repeat('fe',16),'hex'),dataset_id,principal_id,credential_revision,authentication_revision,decode(repeat('fe',32),'hex'),csrf_digest,issued_at_us,expires_at_us,idle_window_us,last_seen_at_us,idle_expires_at_us,NULL FROM control.browser_sessions LIMIT 1", &[]).await.unwrap_err();
    assert_eq!(refused.code().unwrap().code(), "23514");
    assert_eq!(
        refused.as_db_error().unwrap().message(),
        "browser session issuance requires read committed"
    );
    isolation.rollback().await.unwrap();
    case("repeatable-read-issuance-refused-before-stale-capacity-snapshot");

    reset_buckets(&admin).await;
    for _ in 0..5 {
        assert!(
            owner
                .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
                .await
                .is_ok()
        );
    }
    assert!(matches!(
        owner
            .begin_browser_login("alice", "127.0.0.2".parse().unwrap())
            .await,
        Err(SessionError::Throttled)
    ));
    owner.poll_policy(validate).await.unwrap();
    case("persistent-account-quota-across-peer-change");
    reset_buckets(&admin).await;
    for n in 0..20 {
        assert!(
            owner
                .begin_browser_login(&format!("unknown{n}"), "::ffff:127.0.0.1".parse().unwrap())
                .await
                .is_ok()
        );
    }
    assert!(matches!(
        owner
            .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
            .await,
        Err(SessionError::Throttled)
    ));
    case("peer-quota-canonicalizes-ipv4-mapped-ipv6");
    reset_buckets(&admin).await;
    for n in 0..8 {
        if n == 4 {
            reset_buckets(&admin).await;
        }
        let _ = login(&mut owner, SessionLifetime::default()).await;
    }
    reset_buckets(&admin).await;
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let verified = challenge.verify(PASSWORD, &mut permit()).unwrap();
    assert!(matches!(
        owner
            .finish_browser_login(verified, SessionLifetime::default())
            .await,
        Err(SessionError::Capacity)
    ));
    owner.poll_policy(validate).await.unwrap();
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.browser_sessions", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        8
    );
    case("eight-session-capacity-and-safe-terminal-cleanup");
    // Fail after the session SQL mutation, inside its audit INSERT. A failed
    // audit must poison this owner and roll back both session cleanup and write.
    admin.batch_execute("UPDATE control.browser_sessions SET revoked_at_us=GREATEST(issued_at_us,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) WHERE revoked_at_us IS NULL").await.unwrap();
    reset_buckets(&admin).await;
    let before = session_audit_state(&admin).await;
    audit_failure(&admin, true).await;
    let verified = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap()
        .verify(PASSWORD, &mut permit())
        .unwrap();
    assert!(matches!(
        owner
            .finish_browser_login(verified, SessionLifetime::default())
            .await,
        Err(SessionError::Backend)
    ));
    drop(owner);
    owner_driver.await.unwrap().unwrap();
    assert!(
        before == session_audit_state(&admin).await,
        "audit failure changed session or audit rows/xmin"
    );
    audit_failure(&admin, false).await;
    (owner, owner_driver) = acquire(&config, 32).await;
    let last = login(&mut owner, SessionLifetime::default()).await;
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action='browser.session.issue'", &[]).await.unwrap().get::<_,i64>(0),13);
    case("audit-insert-failure-rolls-back-issuance-and-terminal-cleanup");

    let before = session_audit_state(&admin).await;
    audit_failure(&admin, true).await;
    assert!(matches!(
        owner
            .revoke_browser_session(&last.session, &last.csrf)
            .await,
        Err(SessionError::Backend)
    ));
    drop(owner);
    owner_driver.await.unwrap().unwrap();
    assert!(
        before == session_audit_state(&admin).await,
        "audit failure changed revocation or audit rows/xmin"
    );
    audit_failure(&admin, false).await;
    (owner, owner_driver) = acquire(&config, 33).await;
    owner
        .revoke_browser_session(&last.session, &last.csrf)
        .await
        .unwrap();
    let before = session_audit_state(&admin).await;
    owner.release().await.unwrap();
    owner_driver.await.unwrap().unwrap();
    (owner, owner_driver) = acquire(&config, 34).await;
    owner
        .revoke_browser_session(&last.session, &last.csrf)
        .await
        .unwrap();
    assert!(
        before == session_audit_state(&admin).await,
        "logout retry changed its committed receipt"
    );
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action='browser.session.revoke'", &[]).await.unwrap().get::<_,i64>(0),2);
    case("audit-insert-failure-rolls-back-logout-and-successor-retry-is-idempotent");
    owner.release().await.unwrap();
    owner_driver.await.unwrap().unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
}
fn main() {
    let database = std::env::args().nth(1).expect("fresh isolated database");
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .dbname(database)
        .user("postgres")
        .password(std::fs::read("/secrets/postgres").unwrap())
        .connect_timeout(Duration::from_secs(5))
        .application_name("bigbrotr-browser-session-test");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(120),
                run(
                    config,
                    std::env::args().nth(2).as_deref() == Some("--reauthentication"),
                ),
            )
            .await
            .expect("bounded native scenarios");
        });
}
