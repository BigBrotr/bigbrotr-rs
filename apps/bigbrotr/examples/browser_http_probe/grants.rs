//! Real private HTTP refusals without a selected native reader. This does not
//! qualify successful native enablement, restrictive drain or history delivery.
use super::*;

pub(super) async fn upgrade(
    binary: &Path,
    config: &Path,
    home: &Path,
    admin: &Client,
    check: bool,
) {
    if check {
        let mut server = Server::start(binary, config, home, "grant-schema-missing");
        server.terminal(3);
        assert!(fs::read(&server.stdout).unwrap().is_empty());
        assert!(
            fs::read_to_string(&server.stderr)
                .unwrap()
                .contains("Prerequisite")
        );
        case(
            "http-grant-requires-explicit-811-before-binding",
            json!({"exit":3}),
        );
    }
    cli(
        binary,
        config,
        &[
            "migrate",
            "apply",
            "--target",
            "811",
            "--user",
            "postgres",
            "--password-ref",
            &format!("file:{}", home.join("postgres").display()),
        ],
    );
    admin.batch_execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT ON control.management_commands,control.scope_grant_changes TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON control.scope_grants TO bigbrotr_control").await.unwrap();
    if check {
        let mut server = Server::start(binary, config, home, "proof-lock-privilege-missing");
        server.terminal(3);
        assert!(fs::read(&server.stdout).unwrap().is_empty());
        assert!(
            fs::read_to_string(&server.stderr)
                .unwrap()
                .contains("Prerequisite")
        );
    }
    admin
        .batch_execute(
            "GRANT UPDATE(proof_id) ON control.reauthentication_proofs TO bigbrotr_control",
        )
        .await
        .unwrap();
    if check {
        for (index, (table, privilege)) in [
            ("control.management_commands", "INSERT"),
            ("control.scope_grant_changes", "SELECT"),
            ("control.scope_grants", "UPDATE"),
        ]
        .iter()
        .enumerate()
        {
            admin
                .batch_execute(&format!(
                    "REVOKE {privilege} ON {table} FROM bigbrotr_control"
                ))
                .await
                .unwrap();
            let mut server = Server::start(
                binary,
                config,
                home,
                &format!("grant-privilege-missing-{index}"),
            );
            server.terminal(3);
            assert!(fs::read(&server.stdout).unwrap().is_empty());
            assert!(
                fs::read_to_string(&server.stderr)
                    .unwrap()
                    .contains("Prerequisite")
            );
            admin
                .batch_execute(&format!("GRANT {privilege} ON {table} TO bigbrotr_control"))
                .await
                .unwrap();
        }
        admin.batch_execute("REVOKE EXECUTE ON FUNCTION control.command_u64_bytes(control.u64) FROM bigbrotr_control").await.unwrap();
        let mut server = Server::start(binary, config, home, "grant-invoker-privilege-missing");
        server.terminal(3);
        assert!(fs::read(&server.stdout).unwrap().is_empty());
        assert!(
            fs::read_to_string(&server.stderr)
                .unwrap()
                .contains("Prerequisite")
        );
        admin.batch_execute("GRANT EXECUTE ON FUNCTION control.command_u64_bytes(control.u64) TO bigbrotr_control").await.unwrap();
        case(
            "http-grant-startup-checks-each-privilege-and-proof-column-lock",
            json!({"refused_startups":5}),
        );
    }
}

async fn state(admin: &Client) -> String {
    admin.query_one("SELECT jsonb_build_object('commands',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.management_commands t),'details',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.scope_grant_changes t),'grants',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY ordinal) FROM control.scope_grants t),'audit',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY audit_id) FROM control.management_audit t),'proofs',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY proof_id) FROM control.reauthentication_proofs t),'accounts',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY principal_id) FROM control.local_accounts t))::text", &[]).await.unwrap().get(0)
}
fn headers(token: &str, csrf: &str) -> String {
    format!(
        "Cookie: BigBrotrLocalSession={token}\r\nX-CSRF-Token: {csrf}\r\nIdempotency-Key: {}\r\n",
        "c1".repeat(16)
    )
}
fn change(principal: &str, headers: &str, command: &Value, proof: &Value) -> Http {
    request(
        "PUT",
        &format!("/management/v1/principals/{principal}/grants"),
        headers,
        &json!({"proof_id":proof,"command":command}).to_string(),
        true,
    )
}
pub(super) async fn run(
    binary: &Path,
    config: &Path,
    home: &Path,
    admin: &Client,
    principal: &str,
) {
    let mut server = Server::start(binary, config, home, "grant-refusals");
    let ready = server.ready();
    assert_eq!(ready["native_stores"], json!([]));
    let login = login(PASSWORD);
    assert_eq!(login.status, 201);
    let token = login.cookie("BigBrotrLocalSession");
    let csrf = login.cookie("BigBrotrLocalCsrf");
    let auth = headers(&token, &csrf);
    let dataset: String = admin
        .query_one("SELECT encode(dataset_id,'hex') FROM control.datasets", &[])
        .await
        .unwrap()
        .get(0);
    let command = json!({"principal_id":principal,"action":"history.read","enabled":true,
        "scope":{"dataset_id":dataset,"dictionary_kind":4,"allocation_domain":"04".repeat(16),"ordinal":"1"},
        "expected_config_revision":"1","expected_policy_revision":"1","expected_grant_revision":"1",
        "allocation":{"catalog_id":"5a".repeat(16),"frontier_revision":"1"}});
    let response = request(
        "POST",
        "/management/v1/sessions/current/reauthenticate",
        &auth,
        &json!({"password":PASSWORD,"command":command}).to_string(),
        true,
    );
    assert_eq!(response.status, 201);
    let proof = response.value()["proof_id"].clone();
    let before = state(admin).await;
    for (target, request_auth, input, proof, expected) in [
        (
            principal.to_string(),
            headers(&token, &"22".repeat(32)),
            command.clone(),
            proof.clone(),
            401,
        ),
        (
            "ff".repeat(16),
            auth.clone(),
            command.clone(),
            proof.clone(),
            400,
        ),
        (
            principal.to_string(),
            auth.clone(),
            command.clone(),
            json!("99".repeat(16)),
            401,
        ),
    ] {
        assert_eq!(
            change(&target, &request_auth, &input, &proof).status,
            expected
        );
        assert_eq!(state(admin).await, before);
    }
    case(
        "http-grant-real-session-csrf-target-and-proof-refusals-preserve-state",
        json!({"compared_tables":6,"xmin_unchanged":true}),
    );
    for _ in 0..2 {
        let response = change(principal, &auth, &command, &proof);
        assert_eq!(response.status, 503);
        assert_eq!(response.value()["error"]["code"], "authority_unavailable");
        assert_eq!(state(admin).await, before);
        assert_eq!(
            me(&token).status,
            200,
            "missing native reader stopped healthy Control"
        );
    }
    case(
        "http-grant-unconfigured-reader-refuses-repeatedly-without-consuming-proof",
        json!({"compared_tables":6,"healthy_me":true,"native_stores":[]}),
    );
    server.stop();
    let mut server = Server::start(binary, config, home, "grant-refusal-restart");
    server.ready();
    assert_eq!(change(principal, &auth, &command, &proof).status, 503);
    assert_eq!(state(admin).await, before);
    let mut revoke_unknown = command.clone();
    revoke_unknown["enabled"] = false.into();
    revoke_unknown["allocation"] = Value::Null;
    assert_eq!(
        change(principal, &auth, &revoke_unknown, &proof).status,
        409
    );
    assert_eq!(state(admin).await, before);
    assert_eq!(me(&token).status, 200);
    server.stop();
    assert_eq!(
        count(admin, "SELECT count(*) FROM control.scope_grants").await,
        0
    );
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.management_commands WHERE action='scope-grant.change'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.reauthentication_proofs"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.owner_epochs WHERE state='active'"
        )
        .await,
        0
    );
    case(
        "http-grant-restart-retains-proof-and-unknown-revoke-creates-no-tombstone",
        json!({"proofs":1,"scope_grants":0,"grant_commands":0,"active_control":0}),
    );
}
