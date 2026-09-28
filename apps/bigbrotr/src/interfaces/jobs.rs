//! One explicit actor-owned operational page; no idle refresh or auto-pagination.
use super::session_client::{self, Result};
use axum::{
    body::Body,
    http::{Method, StatusCode},
};
use bigbrotr::{
    management::{
        client::{self, session_file::Session},
        job_list,
    },
    secrets::SecretFile,
};
use clap::{Arg, ArgMatches, Command};
use std::process::ExitCode;
use zeroize::Zeroizing;

pub fn command() -> Command {
    let mut list = Command::new("list")
        .about("Read one page of your export jobs through the private API")
        .arg(session_client::session_arg());
    for name in [
        "kind",
        "state",
        "created-from-us",
        "created-before-us",
        "page-size",
        "cursor-ref",
    ] {
        let mut arg = Arg::new(name).long(name);
        if matches!(name, "created-from-us" | "created-before-us") {
            arg = arg.allow_negative_numbers(true);
        }
        list = list.arg(arg);
    }
    Command::new("jobs").about("Authenticated durable job observations")
        .subcommand_required(true).disable_help_subcommand(true)
        .subcommand(list.after_help("Output: one closed bigbrotr-job-list/1 JSON page. Default kind export; state and canonical i64 microsecond bounds are optional. Page size defaults50, max200 after clamping. Read --cursor-ref from an owned0400/0600 regular file containing exact bytes without newline; repeat the same filters. An empty page may advance: only next_cursor:null means completion. No fetch-all, automatic restart, /me call or cursor decoding. Protect redirected output with umask077. Exit0 page delivered,1 output/runtime failure,2 usage,3 invalid input or server refusal,4 transport/incompatible response. Nothing is emitted before validation; discard partial stdout on any nonzero exit."))
}
pub fn execute(matches: &ArgMatches) -> ExitCode {
    let args = matches.subcommand_matches("list").expect("required list");
    match list(args) {
        Ok(bytes) => super::command_output::finish(bytes, 0, false),
        Err((code, message)) => super::command_output::finish(
            Zeroizing::new(format!("error: {message}\n").into_bytes()),
            code,
            true,
        ),
    }
}
fn list(args: &ArgMatches) -> Result<Zeroizing<Vec<u8>>> {
    let mut query = Zeroizing::new(String::with_capacity(4352));
    for (name, wire, default) in [
        ("kind", "kind", Some("export")),
        ("state", "state", None),
        ("created-from-us", "created_from_us", None),
        ("created-before-us", "created_before_us", None),
        ("page-size", "page_size", Some("50")),
    ] {
        if let Some(value) = args.get_one::<String>(name).map(String::as_str).or(default) {
            if value.len() > 32
                || value
                    .bytes()
                    .any(|b| !b.is_ascii_alphanumeric() && b != b'-' && b != b'_')
            {
                return Err((3, "invalid bounded job filter"));
            }
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(wire);
            query.push('=');
            query.push_str(value);
        }
    }
    if let Some(reference) = args.get_one::<String>("cursor-ref") {
        let bytes = SecretFile::parse(reference)
            .and_then(|r| r.read(4096))
            .map_err(|_| (3, "cannot read protected cursor file"))?;
        let cursor =
            std::str::from_utf8(bytes.as_bytes()).map_err(|_| (3, "invalid cursor bytes"))?;
        if cursor.is_empty()
            || !cursor
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err((3, "invalid cursor bytes"));
        }
        query.push_str("&cursor=");
        query.push_str(cursor);
    }
    let parsed =
        job_list::parse_request(&query).map_err(|_| (3, "invalid job filters or cursor"))?;
    let session = Session::open(session_client::input(args, "session-ref"))
        .map_err(|_| (3, "cannot load protected session file"))?;
    let path = Zeroizing::new(format!("/management/v1/jobs?{}", query.as_str()));
    let request = session
        .authenticate(client::request(&session.origin, Method::GET, &path))
        .body(Body::empty())
        .map_err(|_| (3, "invalid job request"))?;
    let mut response = session_client::exchange(&session.origin, request)
        .map_err(|_| (4, "job page unavailable; no automatic retry or new listing"))?;
    if response.status != StatusCode::OK {
        return Err((
            3,
            match response.status.as_u16() {
                401 => "session denied or expired; sign in explicitly",
                403 => "job visibility denied",
                409 => "cursor binding changed; explicitly start a new listing",
                410 => "cursor expired; explicitly start a new listing",
                429 => "server capacity or attempt limit; retry explicitly",
                _ => "job page refused by the server",
            },
        ));
    }
    if response.headers.get_all("content-type").iter().count() != 1
        || response.headers.get("content-type").map(|v| v.as_bytes()) != Some(b"application/json")
        || response.headers.contains_key("set-cookie")
        || client::validation::page(&response.body, parsed.query).is_err()
    {
        return Err((4, "incompatible job page; private output suppressed"));
    }
    response.body.push(b'\n');
    Ok(response.body)
}
