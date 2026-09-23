//! Real adapter and independent SQL oracles; requires an isolated fixture DB.
use super::*;
use bigbrotr::management::{
    grants::{CommandKey, GrantCommand},
    reauthentication::ReauthenticationChallenge,
};

fn command() -> GrantCommand {
    serde_json::from_value(serde_json::json!({
        "principal_id":"10".repeat(16), "action":"history.read", "enabled":false,
        "scope":{"dataset_id":"01".repeat(16),"dictionary_kind":4,"allocation_domain":"05".repeat(16),"ordinal":"7"},
        "expected_config_revision":"1", "expected_policy_revision":"1", "expected_grant_revision":"1", "allocation":null
    })).unwrap()
}
fn key(n: u8) -> CommandKey {
    CommandKey::try_from([n; 16]).unwrap()
}
async fn begin(
    owner: &mut ControlSession,
    session: &IssuedSession,
    n: u8,
    command: &GrantCommand,
) -> ReauthenticationChallenge {
    owner
        .begin_browser_reauthentication(
            &session.session,
            &session.csrf,
            key(n),
            command,
            "127.0.0.1".parse().unwrap(),
        )
        .await
        .unwrap()
}
async fn state(admin: &Client) -> String {
    admin.query_one("SELECT json_build_array((SELECT json_agg(json_build_array(row_to_json(p),p.xmin::text) ORDER BY proof_id) FROM control.reauthentication_proofs p),(SELECT json_agg(json_build_array(row_to_json(a),a.xmin::text) ORDER BY audit_id) FROM control.management_audit a))::text",&[]).await.unwrap().get(0)
}
async fn refused(admin: &mut Client, sql: &str, code: &str) {
    let before = state(admin).await;
    let tx = admin.transaction().await.unwrap();
    let error = match tx.batch_execute(sql).await {
        Err(e) => {
            tx.rollback().await.unwrap();
            e
        }
        Ok(()) => tx.commit().await.unwrap_err(),
    };
    assert_eq!(error.code().unwrap().code(), code);
    assert!(
        before == state(admin).await,
        "refused SQL changed durable proof/audit rows"
    );
}

pub(super) async fn run(admin: &mut Client, owner: &mut ControlSession) {
    owner.require_browser_sessions().await.unwrap();
    let mut command = command();
    let session = login(owner, SessionLifetime::default()).await;
    reset_buckets(admin).await;
    let other = BrowserSecret::parse_hex(&[b'a'; 64]).unwrap();
    assert!(matches!(
        owner
            .begin_browser_reauthentication(
                &session.session,
                &other,
                key(1),
                &command,
                "127.0.0.1".parse().unwrap()
            )
            .await,
        Err(SessionError::Denied)
    ));
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.login_buckets", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    admin
        .batch_execute("DELETE FROM control.principal_grants WHERE action='principals.manage'")
        .await
        .unwrap();
    assert!(matches!(
        owner
            .begin_browser_reauthentication(
                &session.session,
                &session.csrf,
                key(1),
                &command,
                "127.0.0.1".parse().unwrap()
            )
            .await,
        Err(SessionError::Forbidden)
    ));
    admin.batch_execute("INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'principals.manage','dataset',dataset_id FROM control.local_accounts").await.unwrap();
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.login_buckets", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    case("reauth-wrong-csrf-and-missing-action-charge-no-password-attempt");

    assert!(matches!(
        begin(owner, &session, 1, &command)
            .await
            .verify(b"Independent owner password WRONG", &mut permit()),
        Err(SessionError::Denied)
    ));
    assert_eq!(
        admin
            .query_one(
                "SELECT sum(attempts)::bigint FROM control.login_buckets",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    owner.poll_policy(validate).await.unwrap();
    case("reauth-wrong-password-charges-both-buckets-and-keeps-control-usable");

    let verified = begin(owner, &session, 1, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    let proof = owner
        .finish_browser_reauthentication(verified)
        .await
        .unwrap();
    let row=admin.query_one("SELECT encode(actor_id,'hex'),encode(session_id,'hex'),encode(request_key,'hex'),encode(request_digest,'hex'),config_revision::text,policy_revision::text,credential_revision::text,authentication_revision::text,grant_revision::text,expires_at_us-issued_at_us,EXISTS(SELECT 1 FROM control.management_audit a WHERE a.subject_id=p.proof_id AND a.actor_id=p.actor_id AND a.dataset_id=p.dataset_id AND a.action='browser.session.reauthenticate' AND a.outcome='accepted' AND a.config_revision=p.config_revision AND a.policy_revision=p.policy_revision AND a.created_at_us=p.issued_at_us) FROM control.reauthentication_proofs p",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "10".repeat(16));
    assert_eq!(row.get::<_, String>(2), "01".repeat(16));
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/management/scope-grant.json"
    ))
    .unwrap();
    assert_eq!(
        row.get::<_, String>(3),
        vectors["vectors"][2]["sha256"].as_str().unwrap()
    );
    assert_eq!(
        proof.command_digest.to_string(),
        vectors["vectors"][2]["sha256"].as_str().unwrap()
    );
    // Resolve the actual cookie digest independently of the proof response.
    let cookie = hex_decode(&session.session.expose_hex());
    let token: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/browser-session/1\0")
        .chain_update(cookie)
        .finalize()
        .into();
    let session_id: String=admin.query_one("SELECT encode(session_id,'hex') FROM control.browser_sessions WHERE token_digest=$1::bytea", &[&&token[..]]).await.unwrap().get(0);
    assert_eq!(row.get::<_, String>(1), session_id);
    for col in 4..9 {
        assert_eq!(row.get::<_, String>(col), "1");
    }
    assert_eq!(row.get::<_, i64>(9), 300_000_000);
    assert!(row.get::<_, bool>(10));
    let before = state(admin).await;
    let verified = begin(owner, &session, 1, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    let retry = owner
        .finish_browser_reauthentication(verified)
        .await
        .unwrap();
    assert_eq!(proof.id, retry.id);
    assert_eq!(proof.expires_at_us, retry.expires_at_us);
    assert!(
        before == state(admin).await,
        "retry rewrote original proof or audit"
    );
    let mut changed = command.clone();
    changed.expected_grant_revision = "2".to_owned().try_into().unwrap();
    let verified = begin(owner, &session, 1, &changed)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    assert!(matches!(
        owner.finish_browser_reauthentication(verified).await,
        Err(SessionError::Conflict)
    ));
    assert!(before == state(admin).await);
    case("reauth-proof-exact-audit-bounded-lifetime-and-idempotent-content-binding");

    reset_buckets(admin).await;
    let verified = begin(owner, &session, 2, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    admin
        .batch_execute("UPDATE control.local_accounts SET grant_revision=2")
        .await
        .unwrap();
    assert!(matches!(
        owner.finish_browser_reauthentication(verified).await,
        Err(SessionError::Denied)
    ));
    assert!(before == state(admin).await);
    command.expected_grant_revision = "2".to_owned().try_into().unwrap();
    let verified = begin(owner, &session, 2, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    owner
        .revoke_browser_session(&session.session, &session.csrf)
        .await
        .unwrap();
    let before = state(admin).await;
    assert!(matches!(
        owner.finish_browser_reauthentication(verified).await,
        Err(SessionError::Denied)
    ));
    assert!(before == state(admin).await);
    case("reauth-grant-change-and-logout-during-hash-invalidate-proof-issuance");

    reset_buckets(admin).await;
    let current = login(owner, SessionLifetime::default()).await;
    let verified = begin(owner, &current, 3, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    admin.batch_execute("BEGIN; UPDATE control.local_accounts SET credential_revision=2; UPDATE control.local_credentials SET credential_revision=2; COMMIT").await.unwrap();
    assert!(matches!(
        owner.finish_browser_reauthentication(verified).await,
        Err(SessionError::Denied)
    ));
    let current = login(owner, SessionLifetime::default()).await;
    let verified = begin(owner, &current, 3, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    admin.batch_execute("UPDATE control.local_accounts SET active=false; UPDATE control.local_accounts SET active=true").await.unwrap();
    assert!(matches!(
        owner.finish_browser_reauthentication(verified).await,
        Err(SessionError::Denied)
    ));
    owner.poll_policy(validate).await.unwrap();
    case("reauth-credential-reset-and-reactivation-cannot-revive-password-work");

    reset_buckets(admin).await;
    let short = login(
        owner,
        SessionLifetime::new(Duration::from_secs(10), Duration::from_secs(1)).unwrap(),
    )
    .await;
    let verified = begin(owner, &short, 4, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    let block = admin.transaction().await.unwrap();
    block
        .query_one(
            "SELECT principal_id FROM control.local_accounts FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
    let wait = async {
        tokio::time::sleep(Duration::from_millis(1200)).await;
        block.rollback().await.unwrap();
    };
    let (result, ()) = tokio::join!(owner.finish_browser_reauthentication(verified), wait);
    assert!(matches!(result, Err(SessionError::Denied)));
    owner.poll_policy(validate).await.unwrap();
    case("reauth-reads-expiry-after-account-lock-wait");

    reset_buckets(admin).await;
    let current = login(owner, SessionLifetime::default()).await;
    let verified = begin(owner, &current, 5, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    let proof = owner
        .finish_browser_reauthentication(verified)
        .await
        .unwrap();
    refused(
        admin,
        "UPDATE control.reauthentication_proofs SET expires_at_us=expires_at_us+1",
        "23514",
    )
    .await;
    refused(
        admin,
        "DELETE FROM control.reauthentication_proofs",
        "23514",
    )
    .await;
    // Explicit columns keep the oracle independent of physical table ordering.
    let copy = format!(
        "INSERT INTO control.reauthentication_proofs(proof_id,dataset_id,actor_id,session_id,request_key,request_digest,config_revision,policy_revision,credential_revision,authentication_revision,grant_revision,issued_at_us,expires_at_us) SELECT decode(repeat('e1',16),'hex'),dataset_id,actor_id,session_id,decode(repeat('e2',16),'hex'),request_digest,config_revision,policy_revision,credential_revision,authentication_revision,grant_revision,issued_at_us,expires_at_us FROM control.reauthentication_proofs WHERE proof_id=decode('{}','hex');",
        proof.id
    );
    refused(admin, &copy, "23514").await;
    let orphan_audit = "INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision) SELECT decode(repeat('e3',16),'hex'),dataset_id,actor_id,'browser.session.reauthenticate','accepted',issued_at_us,config_revision,decode(repeat('e1',16),'hex'),policy_revision FROM control.reauthentication_proofs ORDER BY issued_at_us DESC LIMIT 1;";
    refused(admin, orphan_audit, "23514").await;
    admin.batch_execute("INSERT INTO control.policy_revisions(revision,previous_revision,dataset_id,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 2,1,dataset_id,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id FROM control.policy_revisions WHERE revision=1").await.unwrap();
    refused(
        admin,
        &format!(
            "{copy}{}",
            orphan_audit.replace(
                "config_revision,decode(repeat('e1',16),'hex'),policy_revision",
                "config_revision,decode(repeat('e1',16),'hex'),2"
            )
        ),
        "23514",
    )
    .await;
    case("reauth-schema-refuses-mutation-live-delete-orphan-effects-and-mixed-audit-policy");

    reset_buckets(admin).await;
    let crossing = login(
        owner,
        SessionLifetime::new(Duration::from_secs(10), Duration::from_secs(1)).unwrap(),
    )
    .await;
    let verified = begin(owner, &crossing, 9, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    admin.batch_execute("CREATE TABLE public.reauth_marker(entered boolean NOT NULL); GRANT INSERT ON public.reauth_marker TO bigbrotr_control; CREATE FUNCTION public.delay_reauth() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.reauth_marker VALUES(true); PERFORM pg_sleep(1.3); RETURN NEW; END $$; CREATE TRIGGER a_delay_reauth BEFORE INSERT ON control.reauthentication_proofs FOR EACH ROW EXECUTE FUNCTION public.delay_reauth()").await.unwrap();
    let before = state(admin).await;
    assert!(
        matches!(
            owner.finish_browser_reauthentication(verified).await,
            Err(SessionError::Denied)
        ),
        "ordinary expiry at INSERT must not poison Control"
    );
    assert!(before == state(admin).await);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM public.reauth_marker WHERE entered",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    owner.poll_policy(validate).await.unwrap();
    admin.batch_execute("DROP TRIGGER a_delay_reauth ON control.reauthentication_proofs; DROP FUNCTION public.delay_reauth(); DROP TABLE public.reauth_marker").await.unwrap();
    case("reauth-expiry-at-insert-is-normal-denial-after-observed-trigger-entry");

    // Fill only the remaining thirty slots with short-lived fixture proofs.
    // These SQL rows test persistence bounds, not successful password checks.
    let tx = admin.transaction().await.unwrap();
    tx.batch_execute(&format!("WITH t AS (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint n)
        INSERT INTO control.reauthentication_proofs(proof_id,dataset_id,actor_id,session_id,request_key,request_digest,config_revision,policy_revision,credential_revision,authentication_revision,grant_revision,issued_at_us,expires_at_us)
        SELECT decode(lpad(to_hex(1000+v.i),32,'0'),'hex'),p.dataset_id,p.actor_id,p.session_id,decode(lpad(to_hex(2000+v.i),32,'0'),'hex'),p.request_digest,p.config_revision,p.policy_revision,p.credential_revision,p.authentication_revision,p.grant_revision,t.n,t.n+2000000
        FROM control.reauthentication_proofs p,t,generate_series(1,30) v(i) WHERE p.proof_id=decode('{}','hex');
        INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
        SELECT proof_id,dataset_id,actor_id,'browser.session.reauthenticate','accepted',issued_at_us,config_revision,proof_id,policy_revision
        FROM control.reauthentication_proofs WHERE proof_id>=decode(lpad(to_hex(1001),32,'0'),'hex') AND proof_id<=decode(lpad(to_hex(1030),32,'0'),'hex');",proof.id)).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.reauthentication_proofs", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        32
    );
    let before = state(admin).await;
    let verified = begin(owner, &current, 6, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    assert!(matches!(
        owner.finish_browser_reauthentication(verified).await,
        Err(SessionError::Capacity)
    ));
    assert!(before == state(admin).await);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let verified = begin(owner, &current, 6, &command)
        .await
        .verify(PASSWORD, &mut permit())
        .unwrap();
    owner
        .finish_browser_reauthentication(verified)
        .await
        .unwrap();
    assert_eq!(
        admin
            .query_one("SELECT count(*) FROM control.reauthentication_proofs", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
    assert_eq!(admin.query_one("SELECT count(*) FROM control.management_audit WHERE action='browser.session.reauthenticate'",&[]).await.unwrap().get::<_,i64>(0),33);
    case("reauth-account-capacity-and-expired-cleanup-retain-issuance-audit");
}

fn hex_decode(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
