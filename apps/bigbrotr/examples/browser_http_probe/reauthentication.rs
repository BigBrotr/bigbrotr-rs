//! Actual executable, password worker, HTTP and durable PostgreSQL effects.
use super::*;
use sha2::{Digest, Sha256};

const ROUTE: &str = "/management/v1/sessions/current/reauthenticate";
fn reauthenticate(token: &str, csrf: &str, command: &Value, password: &str, origin: bool) -> Http {
    request(
        "POST",
        ROUTE,
        &format!(
            "Cookie: BigBrotrLocalSession={token}\r\nX-CSRF-Token: {csrf}\r\nIdempotency-Key: {}\r\n",
            "04".repeat(16)
        ),
        &json!({"password":password,"command":command}).to_string(),
        origin,
    )
}
async fn proof_state(admin: &Client) -> String {
    admin.query_one("SELECT json_build_array((SELECT json_agg(json_build_array(row_to_json(p),p.xmin::text) ORDER BY proof_id) FROM control.reauthentication_proofs p),(SELECT json_agg(json_build_array(row_to_json(a),a.xmin::text) ORDER BY audit_id) FROM control.management_audit a WHERE action='browser.session.reauthenticate'))::text",&[]).await.unwrap().get(0)
}
fn digest(token: &str) -> [u8; 32] {
    let raw: Vec<u8> = token
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    Sha256::new()
        .chain_update(b"bigbrotr/browser-session/1\0")
        .chain_update(raw)
        .finalize()
        .into()
}
pub(super) async fn run(
    binary: &Path,
    config: &Path,
    home: &Path,
    admin: &Client,
    principal: &str,
) {
    let mut server = Server::start(binary, config, home, "reauthentication");
    server.ready();
    let first = login(PASSWORD);
    assert_eq!(first.status, 201);
    let second = login(PASSWORD);
    assert_eq!(second.status, 201);
    let token1 = first.cookie("BigBrotrLocalSession");
    let csrf1 = first.cookie("BigBrotrLocalCsrf");
    let token2 = second.cookie("BigBrotrLocalSession");
    let csrf2 = second.cookie("BigBrotrLocalCsrf");
    let dataset: String = admin
        .query_one("SELECT encode(dataset_id,'hex') FROM control.datasets", &[])
        .await
        .unwrap()
        .get(0);
    let command = json!({"principal_id":principal,"action":"history.read","enabled":false,
        "scope":{"dataset_id":dataset,"dictionary_kind":4,"allocation_domain":"05".repeat(16),"ordinal":"7"},
        "expected_config_revision":"1","expected_policy_revision":"1","expected_grant_revision":"1","allocation":null});
    admin
        .batch_execute("DELETE FROM control.login_buckets")
        .await
        .unwrap();
    assert_eq!(
        reauthenticate(&token1, &csrf1, &command, PASSWORD, false).status,
        403
    );
    assert_eq!(
        reauthenticate(&token1, &csrf2, &command, PASSWORD, true).status,
        401
    );
    assert_eq!(
        count(admin, "SELECT count(*) FROM control.login_buckets").await,
        0
    );
    assert_eq!(
        reauthenticate(
            &token1,
            &csrf1,
            &command,
            "Independent WRONG password",
            true
        )
        .status,
        401
    );
    assert_eq!(
        count(
            admin,
            "SELECT sum(attempts)::bigint FROM control.login_buckets"
        )
        .await,
        2
    );
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.reauthentication_proofs"
        )
        .await,
        0
    );
    assert_eq!(me(&token1).status, 200);
    case(
        "http-reauth-origin-session-csrf-and-wrong-password",
        json!({"proofs":0,"charged_buckets":2}),
    );

    let one = reauthenticate(&token1, &csrf1, &command, PASSWORD, true);
    assert_eq!(one.status, 201);
    let two = reauthenticate(&token2, &csrf2, &command, PASSWORD, true);
    assert_eq!(two.status, 201);
    for reply in [&one, &two] {
        assert!(reply.headers.iter().all(|(name, _)| name != "set-cookie"));
    }
    let one = one.value();
    let two = two.value();
    assert_ne!(one["proof_id"], two["proof_id"]);
    assert_eq!(one["command_digest"], two["command_digest"]);
    for (reply, token) in [(&one, &token1), (&two, &token2)] {
        let bound: bool=admin.query_one("SELECT EXISTS(SELECT 1 FROM control.reauthentication_proofs p JOIN control.browser_sessions s ON p.dataset_id=s.dataset_id AND p.actor_id=s.principal_id AND p.session_id=s.session_id JOIN control.management_audit a ON a.subject_id=p.proof_id AND a.dataset_id=p.dataset_id AND a.actor_id=p.actor_id WHERE p.proof_id=decode($1::text,'hex') AND s.token_digest=$2::bytea AND a.action='browser.session.reauthenticate' AND a.outcome='accepted' AND a.config_revision=p.config_revision AND a.policy_revision=p.policy_revision AND a.created_at_us=p.issued_at_us)",&[&reply["proof_id"].as_str().unwrap(),&&digest(token)[..]]).await.unwrap().get(0);
        assert!(
            bound,
            "proof is not bound to its actual HTTP session and audit"
        );
    }
    let before = proof_state(admin).await;
    let retry = reauthenticate(&token1, &csrf1, &command, PASSWORD, true);
    assert_eq!(retry.status, 201);
    assert_eq!(retry.value(), one);
    assert!(before == proof_state(admin).await);
    let mut changed = command.clone();
    changed["scope"]["ordinal"] = "8".into();
    assert_eq!(
        reauthenticate(&token1, &csrf1, &changed, PASSWORD, true).status,
        409
    );
    assert!(before == proof_state(admin).await);
    case(
        "http-reauth-two-sessions-bind-independent-proofs-and-identical-retry",
        json!({"proofs":2,"audit_rows":2,"original_xmin_preserved":true}),
    );

    // A correct password still spends the shared quota, including proof retries.
    assert_eq!(
        reauthenticate(&token1, &csrf1, &command, PASSWORD, true).status,
        429
    );
    assert_eq!(me(&token1).status, 200);
    let before = proof_state(admin).await;
    server.stop();
    admin
        .batch_execute("DELETE FROM control.login_buckets")
        .await
        .unwrap();
    let mut successor = Server::start(binary, config, home, "reauthentication-successor");
    successor.ready();
    let retry = reauthenticate(&token1, &csrf1, &command, PASSWORD, true);
    assert_eq!(retry.status, 201);
    assert_eq!(retry.value(), one);
    assert!(before == proof_state(admin).await);
    successor.stop();
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.reauthentication_proofs"
        )
        .await,
        2
    );
    assert_eq!(count(admin,"SELECT count(*) FROM control.management_audit WHERE action='browser.session.reauthenticate'").await,2);
    case(
        "http-reauth-quota-and-restart-retain-original-proof-and-audit",
        json!({"proofs":2,"audit_rows":2}),
    );
}
