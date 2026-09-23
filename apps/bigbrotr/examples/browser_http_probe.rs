//! Isolated native executable/PG journey; never a product command or fake server.
#[path = "browser_http_probe/commit.rs"]
mod commit;
#[path = "browser_http_probe/grants.rs"]
mod grants;
#[path = "browser_http_probe/panel.rs"]
mod panel;
#[path = "browser_http_probe/reauthentication.rs"]
mod reauthentication;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::TcpStream,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_postgres::{Client, Config, NoTls};

const ADDRESS: &str = "127.0.0.1:8081";
const PASSWORD: &str = "Independent owner password 2026";
fn case(name: &str, detail: Value) {
    println!("{}", json!({"case":name,"status":"PASS","detail":detail}));
}
fn secret(path: &Path, bytes: &[u8]) {
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .unwrap();
    file.write_all(bytes).unwrap();
}
fn cli(binary: &Path, config: &Path, args: &[&str]) -> Value {
    let result = Command::new(binary)
        .args(args)
        .arg("--file")
        .arg(config)
        .output()
        .unwrap();
    assert!(result.status.success(), "fixture CLI failed: {}", args[0]);
    assert!(result.stderr.is_empty(), "fixture CLI stderr: {}", args[0]);
    serde_json::from_slice(&result.stdout).unwrap()
}
struct Server {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}
impl Server {
    fn start(binary: &Path, config: &Path, home: &Path, name: &str) -> Self {
        let stdout = home.join(format!("{name}.stdout"));
        let stderr = home.join(format!("{name}.stderr"));
        let child = Command::new(binary)
            .args(["serve", "--roles", "control", "--file"])
            .arg(config)
            .stdout(Stdio::from(File::create_new(&stdout).unwrap()))
            .stderr(Stdio::from(File::create_new(&stderr).unwrap()))
            .spawn()
            .unwrap();
        Self {
            child,
            stdout,
            stderr,
        }
    }
    fn ready(&mut self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "runtime stopped before ready"
            );
            let text = fs::read_to_string(&self.stdout).unwrap();
            if let Some(line) = text.lines().next() {
                let value: Value = serde_json::from_str(line).unwrap();
                assert_eq!(value["listeners"].as_array().unwrap().len(), 1);
                assert_eq!(value["listeners"][0]["profile"], "trusted-host-loopback");
                return value;
            }
            assert!(Instant::now() < deadline, "listener startup timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn terminal(&mut self, code: i32) {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(code));
                break;
            }
            assert!(Instant::now() < deadline, "runtime termination timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            TcpStream::connect(ADDRESS).is_err(),
            "listener retained after exit"
        );
    }
    fn stop(&mut self) -> u128 {
        let start = Instant::now();
        rustix::process::kill_process(
            rustix::process::Pid::from_child(&self.child),
            rustix::process::Signal::TERM,
        )
        .unwrap();
        self.terminal(0);
        self.require_release();
        start.elapsed().as_millis()
    }
    fn require_release(&self) {
        assert!(fs::read(&self.stderr).unwrap().is_empty());
        let output = fs::read_to_string(&self.stdout).unwrap();
        let last: Value = serde_json::from_str(output.lines().last().unwrap()).unwrap();
        assert_eq!(last["release"], "committed");
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
struct Http {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}
impl Http {
    fn parse(bytes: &[u8]) -> Self {
        let text = std::str::from_utf8(bytes).unwrap();
        let (headers, body) = text.split_once("\r\n\r\n").expect("complete HTTP response");
        let mut lines = headers.lines();
        let status = lines
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let headers = lines
            .map(|line| {
                let (name, value) = line.split_once(':').unwrap();
                (name.to_ascii_lowercase(), value.trim().to_string())
            })
            .collect();
        Self {
            status,
            headers,
            body: body.as_bytes().to_vec(),
        }
    }
    fn value(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
    fn cookie(&self, name: &str) -> String {
        self.headers
            .iter()
            .filter(|(key, _)| key == "set-cookie")
            .find_map(|(_, value)| value.strip_prefix(&format!("{name}=")))
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }
}
fn socket() -> TcpStream {
    let socket = TcpStream::connect(ADDRESS).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket
}
fn request_text(method: &str, path: &str, extra: &str, body: &str, origin: bool) -> String {
    format!(
        "{method} {path} HTTP/1.1\r\nHost: {ADDRESS}\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\n{extra}\r\n{body}",
        if origin {
            format!("Origin: http://{ADDRESS}\r\n")
        } else {
            String::new()
        },
        body.len()
    )
}
fn request(method: &str, path: &str, extra: &str, body: &str, origin: bool) -> Http {
    let mut socket = socket();
    socket
        .write_all(request_text(method, path, extra, body, origin).as_bytes())
        .unwrap();
    let mut bytes = Vec::new();
    socket.read_to_end(&mut bytes).unwrap();
    Http::parse(&bytes)
}
fn login(password: &str) -> Http {
    request(
        "POST",
        "/management/v1/sessions",
        "",
        &json!({"username":"alice","password":password}).to_string(),
        true,
    )
}
fn me(token: &str) -> Http {
    request(
        "GET",
        "/management/v1/me",
        &format!("Cookie: BigBrotrLocalSession={token}\r\n"),
        "",
        false,
    )
}
async fn count(admin: &Client, query: &str) -> i64 {
    admin.query_one(query, &[]).await.unwrap().get(0)
}
async fn session_state(admin: &Client) -> String {
    admin.query_one("SELECT json_agg(s ORDER BY session_id)::text FROM (SELECT s.*,xmin::text AS xmin FROM control.browser_sessions s) s",&[]).await.unwrap().get(0)
}

fn resident_kib(server: &Server) -> u64 {
    let status = fs::read_to_string(format!("/proc/{}/status", server.child.id())).unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

async fn faults(binary: &Path, config: &Path, home: &Path, admin: &Client) {
    for terminate in [true, false] {
        let name = if terminate {
            "hash-signal"
        } else {
            "hash-parent-loss"
        };
        let mut server = Server::start(binary, config, home, name);
        server.ready();
        let baseline = resident_kib(&server);
        let started = Instant::now();
        let client = std::thread::spawn(|| {
            let text = request_text(
                "POST",
                "/management/v1/sessions",
                "",
                &json!({"username":"alice","password":PASSWORD}).to_string(),
                true,
            );
            let mut socket = socket();
            socket.write_all(text.as_bytes()).unwrap();
            let mut bytes = Vec::new();
            let outcome = socket.read_to_end(&mut bytes);
            assert!(
                outcome.is_ok()
                    || outcome.unwrap_err().kind() == std::io::ErrorKind::ConnectionReset
            );
            assert!(
                bytes.is_empty(),
                "signal/crash fixture received a completed issuance"
            );
        });
        let resident = loop {
            assert!(server.child.try_wait().unwrap().is_none());
            let current = resident_kib(&server);
            if current >= baseline + 32 * 1024 {
                break current;
            }
            assert!(
                started.elapsed() < Duration::from_secs(4),
                "did not observe hash memory residency before deadline"
            );
            std::thread::sleep(Duration::from_millis(2));
        };
        let pid = rustix::process::Pid::from_child(&server.child);
        rustix::process::kill_process(pid, rustix::process::Signal::STOP).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let status = fs::read_to_string(format!("/proc/{}/status", server.child.id())).unwrap();
            if status
                .lines()
                .find_map(|line| line.strip_prefix("State:"))
                .unwrap()
                .trim_start()
                .starts_with('T')
            {
                break;
            }
            assert!(Instant::now() < deadline, "owned runtime did not stop");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            resident_kib(&server) >= baseline + 32 * 1024,
            "hash memory disappeared before the stopped-process observation"
        );
        assert_eq!(
            count(admin, "SELECT count(*) FROM control.browser_sessions").await,
            0
        );
        assert_eq!(
            count(admin, "SELECT count(*) FROM control.login_buckets").await,
            2
        );
        let signaled = Instant::now();
        let terminal_observed_ms;
        if terminate {
            rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
            rustix::process::kill_process(pid, rustix::process::Signal::CONT).unwrap();
            server.terminal(0);
            terminal_observed_ms = signaled.elapsed().as_millis();
            server.require_release();
        } else {
            server.child.kill().unwrap();
            let status = server.child.wait().unwrap();
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
            assert!(TcpStream::connect(ADDRESS).is_err());
            terminal_observed_ms = signaled.elapsed().as_millis();
        }
        client.join().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while count(admin,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'").await!=0 {
            assert!(Instant::now()<deadline,"owned Control backend retained after parent exit");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            count(admin, "SELECT count(*) FROM control.browser_sessions").await,
            0
        );
        case(
            name,
            json!({"baseline_rss_kib":baseline,"observed_rss_kib":resident,"signal_to_terminal_observed_ms":terminal_observed_ms,"signal_to_cleanup_ms":signaled.elapsed().as_millis(),"session_rows":0,"remaining_control_backends":0}),
        );
    }
    let mut server = Server::start(binary, config, home, "after-parent-loss");
    server.ready();
    let issued = login(PASSWORD);
    assert_eq!(issued.status, 201);
    let token = issued.cookie("BigBrotrLocalSession");
    assert_eq!(me(&token).status, 200);
    assert_eq!(
        count(admin, "SELECT count(*) FROM control.browser_sessions").await,
        1
    );
    server.stop();
    case(
        "explicit-successor-login-after-hash-parent-loss",
        json!({"sessions":1}),
    );
}

async fn run(binary: PathBuf, home: PathBuf, mode: &str, panel: Option<panel::Options>) {
    fs::create_dir(&home).unwrap();
    assert!(
        TcpStream::connect(ADDRESS).is_err(),
        "fixture port already occupied"
    );
    for role in ["postgres", "bigbrotr_control"] {
        secret(
            &home.join(role),
            &fs::read(format!("/secrets/{role}")).unwrap(),
        );
    }
    secret(&home.join("owner-password"), PASSWORD.as_bytes());
    let mut connection = Config::new();
    connection
        .host("127.0.0.1")
        .user("postgres")
        .password(fs::read(home.join("postgres")).unwrap())
        .dbname("postgres")
        .connect_timeout(Duration::from_secs(5));
    let (admin, driver) = connection.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let database = panel.as_ref().map_or_else(
        || {
            if mode == "audit-commits" {
                "http_audit_commits"
            } else {
                "http_sessions"
            }
            .to_owned()
        },
        panel::Options::database,
    );
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
    connection.dbname(&database);
    let (admin, driver) = connection.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let mut document: toml::Table = include_str!("../../../config/recipes/serve-existing.toml")
        .parse()
        .unwrap();
    let control = document
        .get_mut("postgres")
        .unwrap()
        .get_mut("control")
        .unwrap()
        .as_table_mut()
        .unwrap();
    control.insert("database".into(), database.into());
    control.insert(
        "password_ref".into(),
        format!("file:{}", home.join("bigbrotr_control").display()).into(),
    );
    if let Some(options) = &panel {
        options.configure(&mut document, &home);
    }
    let config = home.join("instance.toml");
    fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
    let admin_ref = format!("file:{}", home.join("postgres").display());
    cli(
        &binary,
        &config,
        &[
            "migrate",
            "apply",
            "--target",
            "18",
            "--user",
            "postgres",
            "--password-ref",
            &admin_ref,
        ],
    );
    admin.batch_execute("GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control").await.unwrap();
    cli(&binary, &config, &["config", "initialize"]);
    cli(
        &binary,
        &config,
        &[
            "config",
            "activate-initial",
            "--request-id",
            &"44".repeat(16),
            "--expected-revision",
            "1",
        ],
    );
    let issued = cli(
        &binary,
        &config,
        &[
            "access",
            "owner-token",
            "issue",
            "--username",
            "alice",
            "--request-id",
            &"31".repeat(16),
        ],
    );
    secret(
        &home.join("owner-token"),
        issued["token"].as_str().unwrap().as_bytes(),
    );
    drop(issued);
    let enrolled = cli(
        &binary,
        &config,
        &[
            "access",
            "enroll-owner",
            "--challenge-id",
            &"31".repeat(16),
            "--token-ref",
            &format!("file:{}", home.join("owner-token").display()),
            "--password-ref",
            &format!("file:{}", home.join("owner-password").display()),
        ],
    );
    let principal = enrolled["principal_id"].as_str().unwrap().to_string();
    if mode != "ordinary" {
        cli(
            &binary,
            &config,
            &[
                "migrate",
                "apply",
                "--target",
                "809",
                "--user",
                "postgres",
                "--password-ref",
                &admin_ref,
            ],
        );
        admin.batch_execute("GRANT SELECT,INSERT,UPDATE ON control.browser_sessions,control.login_buckets TO bigbrotr_control; GRANT SELECT,INSERT,DELETE ON control.reauthentication_proofs TO bigbrotr_control; GRANT DELETE ON control.browser_sessions TO bigbrotr_control").await.unwrap();
        grants::upgrade(&binary, &config, &home, &admin, mode == "grants").await;
        if mode == "grants" {
            grants::run(&binary, &config, &home, &admin, &principal).await;
            drop(admin);
            driver.await.unwrap().unwrap();
        } else if mode == "reauthentication" {
            reauthentication::run(&binary, &config, &home, &admin, &principal).await;
            drop(admin);
            driver.await.unwrap().unwrap();
        } else if mode == "faults" {
            faults(&binary, &config, &home, &admin).await;
            drop(admin);
            driver.await.unwrap().unwrap();
        } else if mode == "commits" || mode == "audit-commits" {
            let (token, _) = commit::run(&binary, &config, &home, &admin).await;
            drop(admin);
            driver.await.unwrap().unwrap();
            if mode == "commits" {
                commit::crash(&binary, &config, &home, &connection, &token).await;
            }
        } else {
            panel::run(
                &binary,
                &config,
                &home,
                &admin,
                &principal,
                panel.expect("panel options"),
            )
            .await;
            drop(admin);
            driver.await.unwrap().unwrap();
        }
        return;
    }
    let mut missing = Server::start(&binary, &config, &home, "missing-migration");
    missing.terminal(3);
    assert!(fs::read(&missing.stdout).unwrap().is_empty());
    assert!(
        fs::read_to_string(&missing.stderr)
            .unwrap()
            .contains("private session prerequisites refused: Prerequisite")
    );
    case(
        "missing-session-migration-refuses-before-bind",
        json!({"exit":3}),
    );
    cli(
        &binary,
        &config,
        &[
            "migrate",
            "apply",
            "--target",
            "806",
            "--user",
            "postgres",
            "--password-ref",
            &admin_ref,
        ],
    );
    admin.batch_execute("GRANT SELECT,INSERT,UPDATE ON control.browser_sessions,control.login_buckets TO bigbrotr_control; GRANT DELETE ON control.browser_sessions TO bigbrotr_control; GRANT INSERT ON control.management_audit TO bigbrotr_control").await.unwrap();
    let mut missing = Server::start(&binary, &config, &home, "missing-audit-migration");
    missing.terminal(3);
    assert!(fs::read(&missing.stdout).unwrap().is_empty());
    assert!(
        fs::read_to_string(&missing.stderr)
            .unwrap()
            .contains("private session prerequisites refused: Prerequisite")
    );
    case(
        "missing-audit-migration-refuses-before-bind",
        json!({"exit":3}),
    );
    cli(
        &binary,
        &config,
        &[
            "migrate",
            "apply",
            "--target",
            "809",
            "--user",
            "postgres",
            "--password-ref",
            &admin_ref,
        ],
    );
    admin
        .batch_execute(
            "GRANT SELECT,INSERT,DELETE ON control.reauthentication_proofs TO bigbrotr_control",
        )
        .await
        .unwrap();
    // The following refusals must isolate privileges, not a missing811 schema.
    grants::upgrade(&binary, &config, &home, &admin, false).await;
    admin
        .batch_execute("REVOKE DELETE ON control.browser_sessions FROM bigbrotr_control")
        .await
        .unwrap();
    let mut missing = Server::start(&binary, &config, &home, "missing-delete");
    missing.terminal(3);
    assert!(fs::read(&missing.stdout).unwrap().is_empty());
    assert!(
        fs::read_to_string(&missing.stderr)
            .unwrap()
            .contains("private session prerequisites refused: Prerequisite")
    );
    assert_eq!(
        count(&admin, "SELECT count(*) FROM control.browser_sessions").await,
        0
    );
    case(
        "one-missing-table-privilege-refuses-before-bind",
        json!({"missing":"DELETE"}),
    );
    admin
        .batch_execute("GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control")
        .await
        .unwrap();
    admin
        .batch_execute("REVOKE INSERT ON control.management_audit FROM bigbrotr_control")
        .await
        .unwrap();
    let mut missing = Server::start(&binary, &config, &home, "missing-audit-insert");
    missing.terminal(3);
    assert!(fs::read(&missing.stdout).unwrap().is_empty());
    assert!(
        fs::read_to_string(&missing.stderr)
            .unwrap()
            .contains("private session prerequisites refused: Prerequisite")
    );
    assert_eq!(
        count(&admin, "SELECT count(*) FROM control.browser_sessions").await,
        0
    );
    admin
        .batch_execute("GRANT INSERT ON control.management_audit TO bigbrotr_control")
        .await
        .unwrap();
    case(
        "missing-audit-insert-privilege-refuses-before-bind",
        json!({"missing":"INSERT audit"}),
    );
    // Managed file drift cannot disable an enabled accepted policy.
    document
        .get_mut("administration")
        .unwrap()
        .as_table_mut()
        .unwrap()
        .insert("enabled".into(), false.into());
    fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
    let mut server = Server::start(&binary, &config, &home, "initial");
    let initial = server.ready();
    assert_eq!(
        request(
            "POST",
            "/management/v1/sessions",
            "",
            &json!({"username":"alice","password":PASSWORD}).to_string(),
            false
        )
        .status,
        403
    );
    assert_eq!(
        count(&admin, "SELECT count(*) FROM control.login_buckets").await,
        0
    );
    assert_eq!(login("wrong password for the fixture").status, 401);
    let first = login(PASSWORD);
    assert_eq!(first.status, 201);
    let token = first.cookie("BigBrotrLocalSession");
    let csrf = first.cookie("BigBrotrLocalCsrf");
    assert_eq!(first.value()["principal_id"], principal);
    assert_ne!(token, csrf);
    assert!(!String::from_utf8_lossy(&first.body).contains(&token));
    let observed = me(&token);
    assert_eq!(observed.status, 200);
    assert_eq!(
        observed.value()["stored_actions"],
        json!([
            "audit.read",
            "budgets.change",
            "data.read",
            "policy.change",
            "principals.manage",
            "roles.control",
            "status.read"
        ])
    );
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM control.browser_sessions WHERE revoked_at_us IS NULL"
        )
        .await,
        1
    );
    case(
        "real-cli-enrollment-login-me-and-managed-drift",
        json!({"sessions":1,"actions":7}),
    );
    let second = login(PASSWORD);
    assert_eq!(second.status, 201);
    let token2 = second.cookie("BigBrotrLocalSession");
    let csrf2 = second.cookie("BigBrotrLocalCsrf");
    let mixed_before = session_state(&admin).await;
    for (t, c) in [(&token, &csrf2), (&token2, &csrf)] {
        assert_eq!(
            request(
                "DELETE",
                "/management/v1/sessions/current",
                &format!("Cookie: BigBrotrLocalSession={t}\r\nX-CSRF-Token: {c}\r\n"),
                "",
                true
            )
            .status,
            401
        );
    }
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM control.browser_sessions WHERE revoked_at_us IS NULL"
        )
        .await,
        2
    );
    assert_eq!(mixed_before, session_state(&admin).await);
    case(
        "mixed-session-csrf-pairs-cannot-revoke",
        json!({"live_sessions":2}),
    );
    let started = Instant::now();
    let requests = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let mut count = 0;
                    while started.elapsed() < Duration::from_secs(12) {
                        assert_eq!(me(&token).status, 200);
                        count += 1;
                    }
                    count
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).sum::<u64>()
    });
    assert!(requests > 20);
    assert!(server.child.try_wait().unwrap().is_none());
    case(
        "continuous-http-does-not-postpone-policy-renewal",
        json!({"requests":requests,"elapsed_ms":started.elapsed().as_millis(),"clients":4}),
    );
    let stop_ms = server.stop();
    let mut server = Server::start(&binary, &config, &home, "restart");
    let restarted = server.ready();
    assert_ne!(initial["owner_epoch"], restarted["owner_epoch"]);
    assert_ne!(
        initial["process_incarnation"],
        restarted["process_incarnation"]
    );
    assert_eq!(me(&token).status, 200);
    case(
        "actual-process-restart-preserves-existing-cookie",
        json!({"prior_stop_ms":stop_ms}),
    );
    admin.batch_execute("DELETE FROM control.login_buckets; CREATE TABLE public.http_marker(entered boolean NOT NULL); GRANT INSERT ON public.http_marker TO bigbrotr_control; CREATE FUNCTION public.delay_http() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.http_marker VALUES(true); PERFORM pg_sleep(0.9); RETURN NEW; END $$; CREATE TRIGGER a_delay_http BEFORE INSERT ON control.login_buckets FOR EACH ROW WHEN (NEW.kind='account') EXECUTE FUNCTION public.delay_http()").await.unwrap();
    let body = json!({"username":"alice","password":PASSWORD}).to_string();
    let text = request_text("POST", "/management/v1/sessions", "", &body, true);
    let mut slow = socket();
    slow.write_all(&text.as_bytes()[..text.len() - 1]).unwrap();
    std::thread::sleep(Duration::from_millis(4400));
    slow.write_all(&text.as_bytes()[text.len() - 1..]).unwrap();
    let mut bytes = Vec::new();
    slow.read_to_end(&mut bytes).unwrap();
    assert!(bytes.is_empty());
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        count(&admin, "SELECT count(*) FROM public.http_marker").await,
        1
    );
    assert_eq!(
        count(&admin, "SELECT count(*) FROM control.login_buckets").await,
        2
    );
    assert_eq!(
        count(&admin, "SELECT count(*) FROM control.browser_sessions").await,
        2
    );
    assert_eq!(me(&token).status, 200);
    admin.batch_execute("DROP TRIGGER a_delay_http ON control.login_buckets; DROP FUNCTION public.delay_http(); DROP TABLE public.http_marker").await.unwrap();
    case(
        "slow-client-expiry-collects-admitted-sql-without-late-issuance",
        json!({"marker_rows":1,"sessions":2}),
    );
    let logout = request(
        "DELETE",
        "/management/v1/sessions/current",
        &format!("Cookie: BigBrotrLocalSession={token}\r\nX-CSRF-Token: {csrf}\r\n"),
        "",
        true,
    );
    assert_eq!(logout.status, 204);
    assert_eq!(
        logout
            .headers
            .iter()
            .filter(|(n, v)| n == "set-cookie" && v.contains("Max-Age=0;"))
            .count(),
        2
    );
    assert_eq!(me(&token).status, 401);
    assert_eq!(me(&token2).status, 200);
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM control.browser_sessions WHERE revoked_at_us IS NOT NULL"
        )
        .await,
        1
    );
    case(
        "confirmed-logout-clears-only-after-durable-revocation",
        json!({"revoked":1,"live":1}),
    );
    let mut blocked = Vec::new();
    for _ in 0..8 {
        let mut socket = socket();
        socket
            .write_all(b"POST /management/v1/sessions HTTP/1.1\r\n")
            .unwrap();
        blocked.push(socket);
    }
    let stop_ms = server.stop();
    for mut socket in blocked {
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
    }
    assert_eq!(count(&admin,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'").await,0);
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM control.owner_epochs WHERE state='active'"
        )
        .await,
        0
    );
    case(
        "signal-joins-incomplete-http-and-control-before-release",
        json!({"stop_ms":stop_ms,"remaining_pg_clients":0}),
    );
    let mut server = Server::start(&binary, &config, &home, "new-policy-observed");
    server.ready();
    let before: String = admin
        .query_one(
            "SELECT json_agg(s ORDER BY session_id)::text FROM (SELECT s.*,xmin::text AS xmin FROM control.browser_sessions s) s",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    // A controlled legitimate new immutable revision, not a rewritten old row.
    // The fresh listener has just polled revision1; its next HTTP SQL operation
    // must notice this authoritative head before any session touch or response.
    admin.batch_execute("BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 2,dataset_id,1,config_revision,schema_version,document,document_digest,accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1; UPDATE control.datasets SET policy_revision=2; COMMIT").await.unwrap();
    let mut connection = socket();
    connection
        .write_all(
            request_text(
                "GET",
                "/management/v1/me",
                &format!("Cookie: BigBrotrLocalSession={token2}\r\n"),
                "",
                false,
            )
            .as_bytes(),
        )
        .unwrap();
    let mut response = Vec::new();
    connection.read_to_end(&mut response).unwrap();
    assert!(
        response.is_empty(),
        "new policy must not deliver old-epoch observation"
    );
    server.terminal(4);
    assert!(
        fs::read_to_string(&server.stderr)
            .unwrap()
            .contains("PolicyChanged"),
        "fixture did not reach the session transaction before the next poll"
    );
    let after: String = admin
        .query_one(
            "SELECT json_agg(s ORDER BY session_id)::text FROM (SELECT s.*,xmin::text AS xmin FROM control.browser_sessions s) s",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    let mut server = Server::start(&binary, &config, &home, "new-policy-restart");
    assert_eq!(server.ready()["policy_revision"], "2");
    assert_eq!(me(&token2).status, 200);
    server.stop();
    case(
        "session-sql-observes-new-policy-before-effects-and-restart",
        json!({"policy_revision":"2","unchanged_session_rows":true}),
    );
    drop(admin);
    driver.await.unwrap().unwrap();
}
fn main() {
    let mut args = std::env::args().skip(1);
    let binary = PathBuf::from(args.next().expect("actual product binary"));
    let home = PathBuf::from(args.next().expect("fresh isolated fixture directory"));
    let mut panel = None;
    let mode = match args.next().as_deref() {
        None => "ordinary",
        Some("--faults-only") => "faults",
        Some("--commits-only") => "commits",
        Some("--audit-commits-only") => "audit-commits",
        Some("--reauthentication-only") => "reauthentication",
        Some("--grants-only") => "grants",
        Some("--panel") => {
            panel = Some(panel::Options::parse(&mut args));
            "panel"
        }
        _ => panic!("unknown fixture mode"),
    };
    assert!(args.next().is_none());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(binary, home, mode, panel));
}
