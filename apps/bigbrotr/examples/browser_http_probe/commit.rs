//! Real wire loss and recovery. Only the separate owned lab controller kills PG.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    io::{BufRead, BufReader},
    process::ChildStdin,
    sync::mpsc::{Receiver, channel},
    thread::JoinHandle,
};

struct Proxy {
    child: Child,
    input: ChildStdin,
    reader: Option<JoinHandle<()>>,
    events: Receiver<Value>,
    ready: Value,
    stderr: PathBuf,
}
impl Proxy {
    fn start(home: &Path, name: &str, mode: &str, target: &str) -> Self {
        let stderr = home.join(format!("{name}-proxy.stderr"));
        let mut child = Command::new("python3")
            .args([
                "-B",
                "/out/build/source/experiments/archive-engine/native_commit_proxy.py",
                "--mode",
                mode,
                "--target",
                target,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(File::create_new(&stderr).unwrap()))
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (send, events) = channel();
        let reader = std::thread::spawn(move || {
            // The proxy reports only closed counters, never wire credentials.
            for (n, line) in BufReader::new(output.take(64 << 10)).lines().enumerate() {
                assert!(n < 32, "proxy metadata budget exceeded");
                let value: Value = serde_json::from_str(&line.unwrap()).unwrap();
                if send.send(value).is_err() {
                    break;
                }
            }
        });
        let mut proxy = Self {
            child,
            input,
            reader: Some(reader),
            events,
            ready: Value::Null,
            stderr,
        };
        proxy.ready = proxy.events.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(proxy.ready["event"], "ready");
        proxy
    }
    fn config(&self, config: &Path, home: &Path, name: &str) -> PathBuf {
        let mut document: toml::Table = fs::read_to_string(config).unwrap().parse().unwrap();
        document
            .get_mut("postgres")
            .unwrap()
            .get_mut("control")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert(
                "endpoint".into(),
                format!("127.0.0.1:{}", self.ready["port"].as_u64().unwrap()).into(),
            );
        let path = home.join(format!("{name}.toml"));
        fs::write(&path, toml::to_string(&document).unwrap()).unwrap();
        path
    }
    fn finish(mut self, mode: &str) -> Vec<Value> {
        self.input.write_all(b"stop\n").unwrap();
        self.input.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(0));
                break;
            }
            assert!(Instant::now() < deadline, "proxy termination timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
        self.reader.take().unwrap().join().unwrap();
        assert!(fs::read(&self.stderr).unwrap().is_empty());
        let mut events = vec![self.ready.clone()];
        events.extend(self.events.try_iter());
        let last = events.last().unwrap();
        assert_eq!(last["event"], "terminal");
        assert_eq!(last["unexpected"], 0);
        for field in ["data_queries", "data_commits", "session_mutations_complete"] {
            assert_eq!(last[field], 1, "wrong transaction targeted: {field}");
        }
        assert_eq!(last["faults"], u8::from(mode != "pass"));
        assert_eq!(last["server_complete"], u8::from(mode != "before-forward"));
        assert_eq!(last["client_complete"], u8::from(mode == "pass"));
        events
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn unknown(method: &str, path: &str, extra: &str, body: &str) {
    let mut socket = socket();
    socket
        .write_all(request_text(method, path, extra, body, true).as_bytes())
        .unwrap();
    let mut bytes = Vec::new();
    let result = socket.read_to_end(&mut bytes);
    assert!(result.is_ok() || result.unwrap_err().kind() == std::io::ErrorKind::ConnectionReset);
    assert!(bytes.is_empty(), "unknown COMMIT emitted success/cookies");
}
fn unknown_exit(server: &mut Server) {
    server.terminal(4);
    assert!(
        fs::read_to_string(&server.stderr)
            .unwrap()
            .contains("private management backend unavailable: CommitUnknown")
    );
    assert_eq!(
        fs::read_to_string(&server.stdout).unwrap().lines().count(),
        1
    );
}
async fn no_control(admin: &Client) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while count(admin,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'").await != 0 {
        assert!(Instant::now()<deadline,"Control backend remained after exit");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn rows(admin: &Client) -> Vec<Value> {
    let text: String = admin.query_one("SELECT coalesce(json_agg(s ORDER BY session_id)::text,'[]') FROM (SELECT s.*,xmin::text AS xmin FROM control.browser_sessions s) s", &[]).await.unwrap().get(0);
    serde_json::from_str(&text).unwrap()
}
fn token_digest(token: &str) -> [u8; 32] {
    assert_eq!(token.len(), 64);
    let bytes: Vec<u8> = (0..32)
        .map(|n| u8::from_str_radix(&token[n * 2..n * 2 + 2], 16).unwrap())
        .collect();
    Sha256::new()
        .chain_update(b"bigbrotr/browser-session/1\0")
        .chain_update(bytes)
        .finalize()
        .into()
}
async fn other_rows(admin: &Client, digest: &[u8; 32]) -> String {
    admin.query_one("SELECT coalesce(json_agg(s ORDER BY session_id)::text,'[]') FROM (SELECT s.*,xmin::text AS xmin FROM control.browser_sessions s WHERE token_digest<>$1::bytea) s", &[&&digest[..]]).await.unwrap().get(0)
}

async fn audit_rows(admin: &Client) -> String {
    admin.query_one("SELECT coalesce(json_agg(json_build_array(row_to_json(a),a.xmin::text) ORDER BY audit_id)::text,'[]') FROM control.management_audit a WHERE action LIKE 'browser.session.%'", &[]).await.unwrap().get(0)
}

pub(super) async fn run(
    binary: &Path,
    config: &Path,
    home: &Path,
    admin: &Client,
) -> (String, String) {
    let mut known = None;
    for (mode, expected) in [("pass", 1), ("before-forward", 1), ("after-complete", 2)] {
        let name = format!("issuance-{mode}");
        let proxy = Proxy::start(home, &name, mode, "control-session-issue");
        let via = proxy.config(config, home, &name);
        let mut server = Server::start(binary, &via, home, &name);
        server.ready();
        let before = rows(admin).await;
        if mode == "pass" {
            let issued = login(PASSWORD);
            assert_eq!(issued.status, 201);
            known = Some((
                issued.cookie("BigBrotrLocalSession"),
                issued.cookie("BigBrotrLocalCsrf"),
            ));
            server.stop();
        } else {
            unknown(
                "POST",
                "/management/v1/sessions",
                "",
                &json!({"username":"alice","password":PASSWORD}).to_string(),
            );
            unknown_exit(&mut server);
        }
        let events = proxy.finish(mode);
        no_control(admin).await;
        assert_eq!(
            count(admin, "SELECT count(*) FROM control.browser_sessions").await,
            expected
        );
        assert_eq!(
            count(
                admin,
                "SELECT count(*) FROM control.management_audit WHERE action='browser.session.issue'"
            )
            .await,
            expected
        );
        assert_eq!(count(admin,"SELECT count(*) FROM control.management_audit a JOIN control.browser_sessions s ON a.dataset_id=s.dataset_id AND a.actor_id=s.principal_id AND a.subject_id=s.session_id WHERE a.action='browser.session.issue' AND a.config_revision=1 AND a.policy_revision=1").await,expected);
        let after = rows(admin).await;
        assert!(
            before.iter().all(|row| after.contains(row)),
            "issuance changed an existing session"
        );
        case(
            &name,
            json!({"proxy":events,"sessions":expected,"previous_rows_and_xmin_unchanged":true,"remaining_control_backends":0}),
        );
    }
    let mut server = Server::start(binary, config, home, "explicit-login-after-unknown");
    server.ready();
    let issued = login(PASSWORD);
    assert_eq!(issued.status, 201);
    let successor = (
        issued.cookie("BigBrotrLocalSession"),
        issued.cookie("BigBrotrLocalCsrf"),
    );
    assert!(
        known.as_ref().unwrap().0 != successor.0,
        "successor reused a credential"
    );
    assert_eq!(me(&successor.0).status, 200);
    assert_eq!(
        count(admin, "SELECT count(*) FROM control.browser_sessions").await,
        3
    );
    server.stop();
    case(
        "explicit-new-login-after-unknown-issuance",
        json!({"sessions":3,"new_distinct_credential":true}),
    );
    for (mode, credentials, revoked) in [
        ("pass", known.as_ref().unwrap(), 1),
        ("before-forward", &successor, 1),
        ("after-complete", &successor, 2),
    ] {
        let name = format!("logout-{mode}");
        let proxy = Proxy::start(home, &name, mode, "control-session-revoke");
        let via = proxy.config(config, home, &name);
        let mut server = Server::start(binary, &via, home, &name);
        server.ready();
        let headers = format!(
            "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\n",
            credentials.0, credentials.1
        );
        let before = session_state(admin).await;
        let audits_before = audit_rows(admin).await;
        let digest = token_digest(&credentials.0);
        let others = other_rows(admin, &digest).await;
        if mode == "pass" {
            let reply = request(
                "DELETE",
                "/management/v1/sessions/current",
                &headers,
                "",
                true,
            );
            assert_eq!(reply.status, 204);
            assert_eq!(
                reply
                    .headers
                    .iter()
                    .filter(|(k, v)| k == "set-cookie" && v.contains("Max-Age=0"))
                    .count(),
                2
            );
            server.stop();
        } else {
            unknown("DELETE", "/management/v1/sessions/current", &headers, "");
            unknown_exit(&mut server);
        }
        let events = proxy.finish(mode);
        no_control(admin).await;
        assert_eq!(
            count(
                admin,
                "SELECT count(*) FROM control.browser_sessions WHERE revoked_at_us IS NOT NULL"
            )
            .await,
            revoked
        );
        assert_eq!(count(admin,"SELECT count(*) FROM control.management_audit WHERE action='browser.session.revoke'").await,revoked);
        if mode == "before-forward" {
            assert_eq!(before, session_state(admin).await);
            assert_eq!(audits_before, audit_rows(admin).await);
        }
        let target_revoked: bool = admin.query_one("SELECT revoked_at_us IS NOT NULL FROM control.browser_sessions WHERE token_digest=$1::bytea", &[&&digest[..]]).await.unwrap().get(0);
        assert_eq!(target_revoked, mode != "before-forward");
        let target_audits: i64 = admin.query_one("SELECT count(*) FROM control.management_audit a JOIN control.browser_sessions s ON a.dataset_id=s.dataset_id AND a.actor_id=s.principal_id AND a.subject_id=s.session_id WHERE s.token_digest=$1::bytea AND a.action='browser.session.revoke' AND a.outcome='accepted' AND a.config_revision=1 AND a.policy_revision=1", &[&&digest[..]]).await.unwrap().get(0);
        assert_eq!(target_audits, i64::from(mode != "before-forward"));
        assert_eq!(other_rows(admin, &digest).await, others);
        case(
            &name,
            json!({"proxy":events,"revoked":revoked,"target_revoked":target_revoked,"other_rows_and_xmin_unchanged":true,"remaining_control_backends":0}),
        );
    }
    let mut server = Server::start(binary, config, home, "logout-retry-after-unknown");
    server.ready();
    assert_eq!(me(&successor.0).status, 401);
    let before = session_state(admin).await;
    let audits_before = audit_rows(admin).await;
    let reply = request(
        "DELETE",
        "/management/v1/sessions/current",
        &format!(
            "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\n",
            successor.0, successor.1
        ),
        "",
        true,
    );
    assert_eq!(reply.status, 204);
    assert_eq!(session_state(admin).await, before);
    assert_eq!(audits_before, audit_rows(admin).await);
    let issued = login(PASSWORD);
    assert_eq!(issued.status, 201);
    let live = (
        issued.cookie("BigBrotrLocalSession"),
        issued.cookie("BigBrotrLocalCsrf"),
    );
    assert_eq!(me(&live.0).status, 200);
    assert_eq!(
        count(admin, "SELECT count(*) FROM control.browser_sessions").await,
        2
    );
    server.stop();
    case(
        "confirmed-idempotent-logout-after-unknown",
        json!({"same_rows_and_xmin":true,"new_login_sessions":2}),
    );
    live
}

pub(super) async fn crash(
    binary: &Path,
    config: &Path,
    home: &Path,
    connection: &Config,
    token: &str,
) {
    let mut server = Server::start(binary, config, home, "pg-crash-live-cookie");
    server.ready();
    assert_eq!(me(token).status, 200);
    let (admin, driver) = connection.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let before = session_state(&admin).await;
    drop(admin);
    driver.await.unwrap().unwrap();
    // The host controller verifies lab ownership and observes the actual PG
    // postmaster identity before SIGQUIT/recovery. This marker never signals it.
    fs::write(home.join("pg-crash-ready"), b"ready\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !home.join("pg-crash-recovered").exists() {
        assert!(
            Instant::now() < deadline,
            "external PG crash controller unavailable"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    server.terminal(1);
    assert!(
        fs::read_to_string(&server.stderr)
            .unwrap()
            .contains("Control backend operation failed: Backend")
    );
    let (admin, driver) = connection.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    assert_eq!(session_state(&admin).await, before);
    no_control(&admin).await;
    let mut successor = Server::start(binary, config, home, "after-pg-crash");
    successor.ready();
    assert_eq!(me(token).status, 200);
    assert_eq!(
        count(&admin, "SELECT count(*) FROM control.browser_sessions").await,
        2
    );
    successor.stop();
    no_control(&admin).await;
    drop(admin);
    driver.await.unwrap().unwrap();
    case(
        "pg-crash-retains-session-and-live-cookie",
        json!({"same_persisted_rows_before_touch":true,"sessions":2,"successor_self":200}),
    );
}
