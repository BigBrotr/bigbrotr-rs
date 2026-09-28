//! Independent HTTP peer and filesystem observations of the actual CLI.
use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const SESSION: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const CSRF: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const PASSWORD: &str = "Exact private password ä";

fn fixture() -> (tempfile::TempDir, PathBuf, String) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let password = root.join("password");
    fs::write(&password, PASSWORD).unwrap();
    fs::set_permissions(&password, fs::Permissions::from_mode(0o600)).unwrap();
    (directory, root, format!("file:{}", password.display()))
}
fn run(args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(args)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = thread::spawn(move || {
        let mut b = Vec::new();
        stdout.read_to_end(&mut b).unwrap();
        b
    });
    let err = thread::spawn(move || {
        let mut b = Vec::new();
        stderr.read_to_end(&mut b).unwrap();
        b
    });
    let started = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > Duration::from_secs(12) {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("CLI exceeded finite deadline: {:?}", output.status);
        }
        thread::sleep(Duration::from_millis(10));
    }
    Output {
        status: child.wait().unwrap(),
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
    }
}
fn observation() -> &'static str {
    r#"{"format":"bigbrotr-session/1","principal_id":"33333333333333333333333333333333","credential_revision":"1","grant_revision":"2","config_revision":"3","policy_revision":"4","expires_at_us":"2000000000000000","idle_expires_at_us":"1999999999999999","stored_actions":["status.read"],"authority":"observation-only"}"#
}
fn issued() -> String {
    let body = observation();
    format!(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nSet-Cookie: BigBrotrLocalSession={SESSION}; Path=/; Max-Age=28800; SameSite=Strict; HttpOnly\r\nSet-Cookie: BigBrotrLocalCsrf={CSRF}; Path=/; Max-Age=28800; SameSite=Strict\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
fn peer(response: String) -> (String, thread::JoinHandle<String>) {
    peer_action(move |stream| {
        let _ = stream.write_all(response.as_bytes());
    })
}
fn peer_action(
    action: impl FnOnce(&mut std::net::TcpStream) + Send + 'static,
) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = thread::spawn(move || {
        let start = Instant::now();
        let mut stream = loop {
            if let Ok((socket, _)) = listener.accept() {
                break socket;
            }
            if start.elapsed() > Duration::from_secs(10) {
                return String::new();
            }
            thread::sleep(Duration::from_millis(5));
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0];
            if stream.read(&mut byte).unwrap() == 0 {
                break;
            }
            bytes.push(byte[0]);
            assert!(bytes.len() < 8192);
            if let Some(at) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..at]).unwrap().to_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length: ")
                            .map(|n| n.parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if bytes.len() == at + 4 + length {
                    break;
                }
            }
        }
        action(&mut stream);
        String::from_utf8(bytes).unwrap()
    });
    (origin, task)
}
fn login(origin: &str, password: &str, output: &Path) -> Output {
    run(&[
        "access",
        "login",
        "--origin",
        origin,
        "--username",
        "alice",
        "--password-ref",
        password,
        "--session-output",
        output.to_str().unwrap(),
    ])
}
fn assert_no_secrets(output: &Output) {
    for bytes in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(bytes);
        for secret in [SESSION, CSRF, PASSWORD] {
            assert!(!text.contains(secret));
        }
    }
}

#[test]
fn login_publishes_protected_origin_bound_credentials_and_logout_keeps_them() {
    let (_directory, root, password) = fixture();
    let path = root.join("session.json");
    let (origin, task) = peer(issued());
    let output = login(&origin, &password, &path);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_secrets(&output);
    let request = task.join().unwrap();
    assert!(request.starts_with("POST /management/v1/sessions HTTP/1.1\r\n"));
    assert!(request.contains(&format!("origin: {origin}\r\n")));
    let body: serde_json::Value =
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"username":"alice","password":PASSWORD})
    );
    let bytes = fs::read(&path).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        saved,
        serde_json::json!({"format":"bigbrotr-cli-session/1","origin":origin,"session":SESSION,"csrf":CSRF})
    );
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.mode() & 0o7777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2);

    // A new listener at the same saved endpoint observes the original credentials.
    let listener = TcpListener::bind(origin.strip_prefix("http://").unwrap()).unwrap();
    let task = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            let mut one = [0];
            socket.read_exact(&mut one).unwrap();
            bytes.push(one[0]);
        }
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .unwrap();
        String::from_utf8(bytes).unwrap()
    });
    let output = run(&[
        "access",
        "logout",
        "--session-ref",
        &format!("file:{}", path.display()),
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_secrets(&output);
    let request = task.join().unwrap();
    assert!(request.starts_with("DELETE /management/v1/sessions/current HTTP/1.1\r\n"));
    assert!(request.contains(&format!("cookie: BigBrotrLocalSession={SESSION}\r\n")));
    assert!(request.contains(&format!("x-csrf-token: {CSRF}\r\n")));
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn existing_output_and_unsafe_parent_refuse_before_contacting_the_peer() {
    let (_directory, root, password) = fixture();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let path = root.join("session.json");
    fs::write(&path, b"preserve existing bytes").unwrap();
    let output = login(&origin, &password, &path);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(fs::read(&path).unwrap(), b"preserve existing bytes");
    fs::remove_file(&path).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
    let output = login(&origin, &password, &path);
    assert_eq!(output.status.code(), Some(3));
    assert!(!path.exists());
    assert!(listener.accept().is_err());
}

fn stored(root: &Path, origin: &str) -> String {
    let path = root.join("session.json");
    fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({
            "format":"bigbrotr-cli-session/1", "origin":origin, "session":SESSION, "csrf":CSRF
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    format!("file:{}", path.display())
}

#[test]
fn job_list_reads_one_filtered_page_without_touching_the_session() {
    let (_directory, root, _) = fixture();
    let body = r#"{"format":"bigbrotr-job-list/1","items":[],"effective_page_size":2,"next_cursor":"YWJj","observed_at_us":"-9223372036854775808","authority":"observation-only"}"#;
    let (origin, task) = peer(format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    ));
    let reference = stored(&root, &origin);
    let original = fs::read(root.join("session.json")).unwrap();
    let cursor = root.join("cursor");
    fs::write(&cursor, "YWJj").unwrap();
    fs::set_permissions(&cursor, fs::Permissions::from_mode(0o600)).unwrap();
    let output = run(&[
        "jobs",
        "list",
        "--session-ref",
        &reference,
        "--page-size",
        "2",
        "--state",
        "paused",
        "--created-from-us",
        "-9223372036854775808",
        "--cursor-ref",
        &format!("file:{}", cursor.display()),
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_secrets(&output);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::from_str::<serde_json::Value>(body).unwrap()
    );
    let request = task.join().unwrap();
    assert!(request.starts_with("GET /management/v1/jobs?kind=export&state=paused&created_from_us=-9223372036854775808&page_size=2&cursor=YWJj HTTP/1.1\r\n"));
    assert!(request.contains(&format!("x-csrf-token: {CSRF}\r\n")));
    assert_eq!(fs::read(root.join("session.json")).unwrap(), original);
    assert_eq!(fs::read(&cursor).unwrap(), b"YWJj");
}

#[test]
fn invalid_and_duplicate_issuance_headers_never_publish_credentials() {
    for response in [
        issued().replace(
            "Content-Type: application/json\r\n",
            "Content-Type: application/json\r\nContent-Type: text/html\r\n",
        ),
        issued().replace("; HttpOnly", ""),
        issued().replace("BigBrotrLocalCsrf=", "UnexpectedCookie="),
        issued().replace("\"grant_revision\":\"2\"", "\"grant_revision\":\"0\""),
        issued().replace("\"status.read\"", "\"secrets.get\""),
        issued().replace(
            "\"config_revision\":\"3\"",
            "\"config_revision\":\"3\",\"config_revision\":\"3\"",
        ),
    ] {
        let (_directory, root, password) = fixture();
        let path = root.join("session.json");
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        let headers = headers
            .lines()
            .map(|line| {
                if line.starts_with("Content-Length:") {
                    format!("Content-Length: {}", body.len())
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\r\n");
        let (origin, task) = peer(format!("{headers}\r\n\r\n{body}"));
        let output = login(&origin, &password, &path);
        assert_eq!(output.status.code(), Some(4));
        assert!(output.stdout.is_empty());
        assert_no_secrets(&output);
        assert!(!path.exists());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 2); // password and retained staging
        assert!(!task.join().unwrap().is_empty());
    }
}

#[test]
fn late_destination_creation_does_not_replace_it_or_lose_issued_staging() {
    let (_directory, root, password) = fixture();
    let path = root.join("session.json");
    let late = path.clone();
    let (origin, task) = peer_action(move |stream| {
        fs::write(&late, b"external provisioning bytes").unwrap();
        stream.write_all(issued().as_bytes()).unwrap();
    });
    let output = login(&origin, &password, &path);
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(fs::read(&path).unwrap(), b"external provisioning bytes");
    let staging = fs::read_dir(&root)
        .unwrap()
        .map(|p| p.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".partial")
        })
        .unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&fs::read(staging).unwrap()).unwrap();
    assert_eq!(saved["session"], SESSION);
    assert_eq!(saved["csrf"], CSRF);
    assert!(!task.join().unwrap().is_empty());
}

#[test]
fn redirects_and_origin_aliases_cannot_send_credentials_to_another_origin() {
    let (_directory, root, password) = fixture();
    let sink = TcpListener::bind("127.0.0.1:0").unwrap();
    sink.set_nonblocking(true).unwrap();
    let address = sink.local_addr().unwrap();
    for origin in [
        format!("http://localhost:{}", address.port()),
        format!("http://{address}/"),
        format!("http://alice@{address}"),
        "http://192.0.2.1:80".into(),
        "http://127.0.0.1:0".into(),
        "https://127.0.0.1:443".into(),
    ] {
        let output = login(&origin, &password, &root.join("session.json"));
        assert_eq!(output.status.code(), Some(3));
    }
    let (origin, task) = peer(format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/steal\r\nContent-Length: 0\r\n\r\n"
    ));
    let output = login(&origin, &password, &root.join("session.json"));
    assert_eq!(output.status.code(), Some(4));
    assert!(!task.join().unwrap().is_empty());
    assert!(sink.accept().is_err());
}

#[test]
fn withheld_response_closes_the_actual_socket_within_the_total_deadline() {
    let (_directory, root, password) = fixture();
    let (origin, task) = peer_action(|stream| {
        stream
            .set_read_timeout(Some(Duration::from_secs(9)))
            .unwrap();
        // Partial body: response headers alone cannot complete login.
        stream
            .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 100\r\n\r\n{")
            .unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let started = Instant::now();
    let output = login(&origin, &password, &root.join("session.json"));
    assert_eq!(output.status.code(), Some(4));
    assert!(started.elapsed() >= Duration::from_secs(5));
    assert!(started.elapsed() < Duration::from_secs(9));
    assert!(!task.join().unwrap().is_empty());
}

#[test]
fn blocked_stdout_terminates_after_publishing_a_reusable_session() {
    use rustix::{
        fs::{OFlags, fcntl_setfl},
        pipe::pipe,
    };
    let (_directory, root, password) = fixture();
    let path = root.join("session.json");
    let (origin, task) = peer(issued());
    let (reader, writer) = pipe().unwrap();
    for fd in [&reader, &writer] {
        rustix::io::fcntl_setfd(fd, rustix::io::FdFlags::CLOEXEC).unwrap();
    }
    fcntl_setfl(&writer, OFlags::NONBLOCK).unwrap();
    let block = [b'x'; 1024];
    let mut filled = 0;
    loop {
        match rustix::io::write(&writer, &block) {
            Ok(n) => filled += n,
            Err(rustix::io::Errno::WOULDBLOCK) => break,
            other => panic!("pipe fill: {other:?}"),
        }
    }
    assert!(filled > 0);
    fcntl_setfl(&writer, OFlags::empty()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args([
            "access",
            "login",
            "--origin",
            &origin,
            "--username",
            "alice",
            "--password-ref",
            &password,
            "--session-output",
            path.to_str().unwrap(),
        ])
        .env_clear()
        .stdout(Stdio::from(writer))
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(4) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("blocked stdout prevented exit");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(1));
    let saved: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(saved["session"], SESSION);
    assert!(!task.join().unwrap().is_empty());
    drop(reader);
}

fn page_fixture() -> serde_json::Value {
    serde_json::json!({"format":"bigbrotr-job-list/1", "effective_page_size":2,
    "next_cursor":null, "observed_at_us":"9223372036854775807", "authority":"observation-only",
    "items":[
        {"format":"bigbrotr-job-status/1", "job_id":"44444444444444444444444444444444", "kind":"export",
            "state":"succeeded", "job_revision":"18446744073709551615", "checkpoint_revision":"0",
            "created_at_us":"9007199254740993", "completed_at_us":"9223372036854775807",
            "observed_at_us":"9223372036854775807", "reason_code":null, "authority":"observation-only"},
        {"format":"bigbrotr-job-status/1", "job_id":"33333333333333333333333333333333", "kind":"export",
            "state":"paused", "job_revision":"1", "checkpoint_revision":"18446744073709551615",
            "created_at_us":"9007199254740993", "completed_at_us":null,
            "observed_at_us":"9223372036854775807", "reason_code":"resource_limit", "authority":"observation-only"}
    ]})
}
fn response_page(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
fn list_response(root: &Path, response: String) -> Output {
    let (origin, task) = peer(response);
    let reference = stored(root, &origin);
    let output = run(&[
        "jobs",
        "list",
        "--session-ref",
        &reference,
        "--page-size",
        "2",
    ]);
    assert!(!task.join().unwrap().is_empty());
    output
}
#[test]
fn job_page_preserves_large_integers_and_refuses_inconsistent_or_extra_fields() {
    let (_directory, root, _) = fixture();
    let original = page_fixture();
    let output = list_response(&root, response_page(&original.to_string()));
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        original
    );
    let mut cases = Vec::new();
    for (key, value) in [
        ("job_revision", serde_json::json!("18446744073709551616")),
        ("checkpoint_revision", serde_json::json!("01")),
        ("created_at_us", serde_json::json!("9223372036854775808")),
        ("observed_at_us", serde_json::json!("1")),
        ("authority", serde_json::json!("execution-granted")),
        (
            "job_id",
            serde_json::json!("00000000000000000000000000000000"),
        ),
        ("completed_at_us", serde_json::Value::Null),
        ("input_secret", serde_json::json!("not a selected field")),
    ] {
        let mut page = original.clone();
        page["items"][0][key] = value;
        cases.push(page);
    }
    let mut missing = original.clone();
    missing["items"][0]
        .as_object_mut()
        .unwrap()
        .remove("reason_code");
    cases.push(missing);
    let mut missing = original.clone();
    missing.as_object_mut().unwrap().remove("next_cursor");
    cases.push(missing);
    let mut order = original.clone();
    order["items"].as_array_mut().unwrap().reverse();
    cases.push(order);
    let mut duplicate = original.clone();
    duplicate["items"][1]["job_id"] = duplicate["items"][0]["job_id"].clone();
    cases.push(duplicate);
    let mut reason = original.clone();
    reason["items"][1]["reason_code"] = serde_json::Value::Null;
    cases.push(reason);
    for bad in cases {
        let output = list_response(&root, response_page(&bad.to_string()));
        assert_eq!(output.status.code(), Some(4));
        assert!(output.stdout.is_empty());
        assert_no_secrets(&output);
    }
    let repeated = original.to_string().replacen(
        "\"effective_page_size\":2",
        "\"effective_page_size\":2,\"effective_page_size\":2",
        1,
    );
    let output = list_response(&root, response_page(&repeated));
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
}

#[test]
fn response_limits_cover_actual_streamed_bytes_and_header_count() {
    let (_directory, root, _) = fixture();
    let body = page_fixture().to_string();
    let at_limit = format!("{body}{}", " ".repeat(128 * 1024 - body.len()));
    let output = list_response(&root, response_page(&at_limit));
    assert!(output.status.success());
    assert_eq!(output.stdout.len(), 128 * 1024 + 1);
    let chunked = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{} \r\n0\r\n\r\n",
        at_limit.len() + 1,
        at_limit
    );
    let many_headers = format!(
        "HTTP/1.1 200 OK\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        "X-Filler: one\r\n".repeat(33),
        body.len()
    );
    for bad in [
        response_page(&(at_limit + " ")),
        chunked,
        many_headers,
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{{",
            body.len()
        ),
    ] {
        let output = list_response(&root, bad);
        assert_eq!(output.status.code(), Some(4));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn malformed_private_files_and_locked_parent_refuse_without_network() {
    use rustix::fs::{FlockOperation, flock};
    let (_directory, root, password) = fixture();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let reference = stored(&root, &origin);
    let path = root.join("session.json");
    let original = fs::read(&path).unwrap();
    let action = || run(&["access", "logout", "--session-ref", &reference]);
    for mode in [0o644, 0o640, 0o4600] {
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        assert_eq!(action().status.code(), Some(3));
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::hard_link(&path, root.join("link")).unwrap();
    assert_eq!(action().status.code(), Some(3));
    fs::remove_file(root.join("link")).unwrap();
    for bytes in [
        b"{}".to_vec(),
        vec![b' '; 1025],
        String::from_utf8(original.clone())
            .unwrap()
            .replace("bigbrotr-cli-session/1", "bigbrotr-cli-session/2")
            .into_bytes(),
        String::from_utf8(original.clone())
            .unwrap()
            .replacen('{', "{\"password\":\"unexpected\",", 1)
            .into_bytes(),
    ] {
        fs::write(&path, bytes).unwrap();
        assert_eq!(action().status.code(), Some(3));
    }
    fs::write(&path, &original).unwrap();
    fs::rename(&path, root.join("real")).unwrap();
    std::os::unix::fs::symlink("real", &path).unwrap();
    assert_eq!(action().status.code(), Some(3));
    fs::remove_file(&path).unwrap();
    fs::rename(root.join("real"), &path).unwrap();
    let directory = fs::File::open(&root).unwrap();
    flock(&directory, FlockOperation::NonBlockingLockExclusive).unwrap();
    assert_eq!(action().status.code(), Some(3));
    assert_eq!(
        login(&origin, &password, &root.join("new")).status.code(),
        Some(3)
    );
    assert!(listener.accept().is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn a_concurrent_login_cannot_issue_again_while_the_first_holds_custody() {
    use std::sync::mpsc;
    let (_directory, root, password) = fixture();
    let path = root.join("session.json");
    let (accepted, ready) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::sync_channel(1);
    let (origin, task) = peer_action(move |stream| {
        accepted.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(4)).unwrap();
        stream.write_all(issued().as_bytes()).unwrap();
    });
    let first = {
        let origin = origin.clone();
        let password = password.clone();
        let path = path.clone();
        thread::spawn(move || login(&origin, &password, &path))
    };
    ready.recv_timeout(Duration::from_secs(4)).unwrap();
    let second = login(&origin, &password, &path);
    assert_eq!(second.status.code(), Some(3));
    assert!(!path.exists());
    release.send(()).unwrap();
    assert!(first.join().unwrap().status.success());
    assert!(!task.join().unwrap().is_empty());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
}

#[test]
fn termination_during_login_closes_socket_and_releases_the_directory() {
    use rustix::fs::{FlockOperation, flock};
    use std::sync::mpsc;
    let (_directory, root, password) = fixture();
    let path = root.join("session.json");
    let (accepted, ready) = mpsc::sync_channel(1);
    let (origin, task) = peer_action(move |stream| {
        accepted.send(()).unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args([
            "access",
            "login",
            "--origin",
            &origin,
            "--username",
            "alice",
            "--password-ref",
            &password,
            "--session-output",
            path.to_str().unwrap(),
        ])
        .env_clear()
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    ready.recv_timeout(Duration::from_secs(4)).unwrap();
    let directory = fs::File::open(&root).unwrap();
    assert!(flock(&directory, FlockOperation::NonBlockingLockExclusive).is_err());
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(child.id() as i32).unwrap(),
        rustix::process::Signal::TERM,
    )
    .unwrap();
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        if started.elapsed() > Duration::from_secs(2) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("SIGTERM did not terminate");
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(!task.join().unwrap().is_empty());
    flock(&directory, FlockOperation::NonBlockingLockExclusive).unwrap();
    assert!(!path.exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2); // unresolved staging retained
}
