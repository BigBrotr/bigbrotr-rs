//! Explicit finite login/logout over the same API used by the private panel.
use axum::{
    body::{Body, Bytes},
    http::{Method, StatusCode},
};
use bigbrotr::{
    management::{
        client::{
            self, ClientError, LoopbackOrigin, Response,
            session_file::{Session, SessionOutput},
        },
        credentials::validate_name,
        sessions::BrowserSecret,
    },
    secrets::SecretFile,
};
use clap::{Arg, ArgMatches, Command};
use std::process::ExitCode;
use zeroize::Zeroizing;

pub(super) fn commands(command: Command) -> Command {
    command.subcommand(Command::new("login")
        .about("Sign in through local HTTP and persist a new protected session file")
        .arg(Arg::new("origin").long("origin").required(true).value_name("http://LOOPBACK_IP:PORT"))
        .arg(Arg::new("username").long("username").required(true))
        .arg(Arg::new("password-ref").long("password-ref").required(true).value_name("file:/ABSOLUTE/PATH"))
        .arg(Arg::new("session-output").long("session-output").required(true).value_name("/ABSOLUTE/PATH"))
        .after_help("Use an existing owned mode0700 parent. No overwrite, redirects, proxy or automatic retry. A successful file persists even if stdout fails: reuse it with --session-ref. Exit4 means issuance or file publication is unresolved; inspect retained staging/final files before a new explicit login. No database deployment credential is used. Remote TLS is not yet implemented."))
        .subcommand(Command::new("logout")
        .about("Revoke the saved HTTP session; retain its file for explicit retry")
        .arg(session_arg())
        .after_help("The session file remains unchanged. After successful revocation, remove it explicitly when no retry is needed. Revocation acknowledgement is not completed work drain. Exit4 means the outcome is unknown; retry with the same file. Neither command refreshes credentials automatically."))
}
pub(super) fn session_arg() -> Arg {
    Arg::new("session-ref")
        .long("session-ref")
        .required(true)
        .value_name("file:/ABSOLUTE/PATH")
}
pub(super) fn input<'a>(args: &'a ArgMatches, key: &str) -> &'a str {
    args.get_one::<String>(key)
        .expect("required argument")
        .as_str()
}
pub(super) fn execute(action: &str, args: &ArgMatches) -> ExitCode {
    let result = if action == "login" {
        login(args)
    } else {
        logout(args)
    };
    match result {
        Ok(bytes) => super::command_output::finish(bytes, 0, false),
        Err((code, message)) => super::command_output::finish(
            Zeroizing::new(format!("error: {message}\n").into_bytes()),
            code,
            true,
        ),
    }
}
pub(super) type Result<T> = std::result::Result<T, (u8, &'static str)>;
pub(super) fn exchange(
    origin: &LoopbackOrigin,
    request: axum::http::Request<Body>,
) -> std::result::Result<Response, ClientError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| ClientError::Transport)?;
    // exchange owns and drops the actual driver/socket; no blocking task is spawned.
    runtime.block_on(client::exchange(origin, request))
}
struct PrivateBody(Zeroizing<Vec<u8>>);
impl AsRef<[u8]> for PrivateBody {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
fn login(args: &ArgMatches) -> Result<Zeroizing<Vec<u8>>> {
    let origin = LoopbackOrigin::parse(input(args, "origin"))
        .map_err(|_| (3, "expected a literal trusted loopback HTTP origin"))?;
    let username = input(args, "username");
    validate_name(username).map_err(|_| (3, "invalid account name"))?;
    let password = SecretFile::parse(input(args, "password-ref"))
        .and_then(|r| r.read(1024))
        .map_err(|_| (3, "cannot read protected password"))?;
    let password =
        std::str::from_utf8(password.as_bytes()).map_err(|_| (3, "invalid password encoding"))?;
    if password.chars().count() < 12 || password.chars().any(char::is_control) {
        return Err((3, "password does not match the local credential profile"));
    }
    #[derive(serde::Serialize)]
    struct Login<'a> {
        username: &'a str,
        password: &'a str,
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity(8192));
    serde_json::to_writer(&mut *bytes, &Login { username, password })
        .map_err(|_| (3, "cannot encode bounded login"))?;
    let request = client::request(&origin, Method::POST, "/management/v1/sessions")
        .header("content-type", "application/json")
        .body(Body::from(Bytes::from_owner(PrivateBody(bytes))))
        .map_err(|_| (3, "invalid login request"))?;
    let output = SessionOutput::prepare(input(args, "session-output")).map_err(|_| {
        (
            3,
            "session output must be absent in an available owned mode0700 parent",
        )
    })?;
    let response = exchange(&origin, request).map_err(|_| {
        (
            4,
            "login outcome unknown; inspect retained session staging; no automatic retry",
        )
    })?;
    if response.status != StatusCode::CREATED {
        if matches!(response.status.as_u16(), 400 | 401 | 403 | 409 | 429) {
            output
                .discard()
                .map_err(|_| (4, "login refused; staging cleanup unresolved"))?;
            return Err((
                3,
                "login refused by the server; credentials were not published",
            ));
        }
        return Err((
            4,
            "login outcome unknown; inspect retained session staging; no automatic retry",
        ));
    }
    let session = issued(origin, &response).map_err(|_| (4, "invalid issuance response; session may exist; inspect retained staging before a new login"))?;
    output.publish(&session).map_err(|_| (4, "session issued but local publication unknown; inspect retained staging/final file; do not overwrite"))?;
    Ok(Zeroizing::new(b"{\"format\":\"bigbrotr-cli-login/1\",\"status\":\"session-persisted\",\"authority\":\"observation-only\"}\n".to_vec()))
}
fn issued(
    origin: LoopbackOrigin,
    response: &Response,
) -> std::result::Result<Session, ClientError> {
    client::validation::session(&response.body)?;
    if response.headers.get_all("content-type").iter().count() != 1
        || response.headers.get("content-type").map(|v| v.as_bytes()) != Some(b"application/json")
    {
        return Err(ClientError::Response);
    }
    let (mut session, mut csrf) = (None, None);
    for header in response.headers.get_all("set-cookie") {
        let header = header.to_str().map_err(|_| ClientError::Response)?;
        let (pair, attributes) = header.split_once(';').ok_or(ClientError::Response)?;
        let (name, value) = pair.split_once('=').ok_or(ClientError::Response)?;
        let (slot, expected) = match name {
            "BigBrotrLocalSession" => (
                &mut session,
                " Path=/; Max-Age=28800; SameSite=Strict; HttpOnly",
            ),
            "BigBrotrLocalCsrf" => (&mut csrf, " Path=/; Max-Age=28800; SameSite=Strict"),
            _ => return Err(ClientError::Response),
        };
        if attributes != expected || slot.is_some() {
            return Err(ClientError::Response);
        }
        *slot =
            Some(BrowserSecret::parse_hex(value.as_bytes()).map_err(|_| ClientError::Response)?);
    }
    Ok(Session {
        origin,
        session: session.ok_or(ClientError::Response)?,
        csrf: csrf.ok_or(ClientError::Response)?,
    })
}
fn logout(args: &ArgMatches) -> Result<Zeroizing<Vec<u8>>> {
    let session = Session::open(input(args, "session-ref"))
        .map_err(|_| (3, "cannot load protected session file"))?;
    let request = session
        .authenticate(client::request(
            &session.origin,
            Method::DELETE,
            "/management/v1/sessions/current",
        ))
        .body(Body::empty())
        .map_err(|_| (3, "invalid logout request"))?;
    let response = exchange(&session.origin, request).map_err(|_| {
        (
            4,
            "logout outcome unknown; retry with the same session file",
        )
    })?;
    if response.status != StatusCode::NO_CONTENT || !response.body.is_empty() {
        return Err((
            if response.status.is_client_error() {
                3
            } else {
                4
            },
            "logout not acknowledged; session file retained for explicit retry",
        ));
    }
    Ok(Zeroizing::new(b"{\"format\":\"bigbrotr-cli-logout/1\",\"status\":\"revocation-acknowledged\",\"local_file\":\"retained\",\"drain\":\"unobserved\"}\n".to_vec()))
}
