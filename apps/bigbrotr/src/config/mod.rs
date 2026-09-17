mod recipes;
mod registry;
mod resolve;

pub(crate) use resolve::Resolved;

use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
    process::ExitCode,
};

pub fn command() -> Command {
    let file = || {
        Arg::new("file")
            .long("file")
            .required(true)
            .value_name("PATH")
    };
    Command::new("config").about("Inspect static configuration candidates; no durable activation")
        .subcommand_required(true).disable_help_subcommand(true)
        .subcommand(Command::new("init").about("Create an editable initialization candidate; refuse existing paths")
            .arg(Arg::new("recipe").long("recipe").required(true).value_parser(clap::builder::PossibleValuesParser::new(recipes::NAMES.iter().copied())))
            .arg(Arg::new("output").long("output").required(true).value_name("PATH")))
        .subcommand(Command::new("validate").about("Validate static syntax, composition and resource arithmetic without I/O dependencies").arg(file()))
        .subcommand(Command::new("show").about("Show redacted candidate values, attribution and reload effects; applied authority is unavailable offline").arg(file()))
        .subcommand(Command::new("schema").about("Print the generated schema and validation metadata"))
        .subcommand(Command::new("reference").about("Print the generated static key reference as Markdown"))
        .subcommand(Command::new("example").about("Print a deterministic documentation example; does not create an identity")
            .arg(Arg::new("recipe").long("recipe").required(true).value_parser(clap::builder::PossibleValuesParser::new(recipes::NAMES.iter().copied()))))
}

pub fn doctor_command() -> Command {
    Command::new("doctor")
        .about("Read-only local preflight; report absent live adapters without claiming readiness")
        .arg(
            Arg::new("file")
                .long("file")
                .required(true)
                .value_name("PATH"),
        )
}

pub fn execute(matches: &ArgMatches, doctor: bool) -> ExitCode {
    match run(matches, doctor) {
        Ok((output, code)) => {
            let mut stdout = std::io::stdout().lock();
            if stdout
                .write_all(output.as_bytes())
                .and_then(|()| stdout.flush())
                .is_err()
            {
                eprintln!("error: could not write configuration output");
                return ExitCode::FAILURE;
            }
            ExitCode::from(code)
        }
        Err((code, message)) => {
            eprintln!("error: {message}");
            ExitCode::from(code)
        }
    }
}

type Result<T> = std::result::Result<T, (u8, String)>;
fn run(matches: &ArgMatches, doctor: bool) -> Result<(String, u8)> {
    let (action, args) = if doctor {
        ("doctor", matches)
    } else {
        matches.subcommand().expect("required config subcommand")
    };
    if action == "schema" {
        return Ok((json_text(&schema()), 0));
    }
    if action == "reference" {
        return Ok((reference(), 0));
    }
    if action == "example" {
        let document = recipes::document(
            args.get_one::<String>("recipe").unwrap(),
            "11111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .map_err(|e| (3, e))?;
        resolve::parse(&document).map_err(|e| (3, e))?;
        return Ok((document, 0));
    }
    if action == "init" {
        let recipe = args.get_one::<String>("recipe").unwrap();
        let document =
            recipes::document(recipe, &new_identity()?, &new_identity()?).map_err(|e| (3, e))?;
        resolve::parse(&document).map_err(|e| (3, e))?;
        let output = args.get_one::<String>("output").unwrap();
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(output).map_err(|_| {
            (
                1,
                "cannot create configuration output; existing paths are never overwritten".into(),
            )
        })?;
        file.write_all(document.as_bytes()).and_then(|()|file.sync_all()).map_err(|_|(1,"could not finish configuration output; inspect the newly created file before use".into()))?;
        return Ok((
            json_text(&json!({"status":"created-candidate","recipe":recipe,"activated":false})),
            0,
        ));
    }
    let path = Path::new(args.get_one::<String>("file").unwrap());
    let resolved = load(path)?;
    if action == "doctor" {
        return Ok((json_text(&diagnose(&resolved)), 4));
    }
    Ok((json_text(&resolved.report(action == "show")), 0))
}

fn new_identity() -> Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| {
        (
            1,
            "operating-system randomness unavailable for candidate identity".into(),
        )
    })?;
    if bytes == [0; 16] {
        return Err((
            1,
            "random identity was reserved zero; retry initialization".into(),
        ));
    }
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// Explicit startup consumers share the same bounded offline candidate checks.
pub(crate) fn load(path: &Path) -> Result<Resolved> {
    let input = read_document(path)?;
    resolve::parse(&input).map_err(|error| (3, error))
}

fn read_document(path: &Path) -> Result<String> {
    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::NOCTTY,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| {
        (
            1,
            "explicit configuration file is missing, unreadable or a symlink".into(),
        )
    })?;
    let file = fs::File::from(descriptor);
    if !file
        .metadata()
        .map_err(|_| (1, "cannot inspect configuration descriptor".into()))?
        .is_file()
    {
        return Err((1, "configuration descriptor must be a regular file".into()));
    }
    let mut bytes = Vec::new();
    file.take(resolve::MAX_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| (1, "cannot read configuration file".into()))?;
    if bytes.len() > resolve::MAX_CONFIG_BYTES {
        return Err((3, "configuration exceeds 10MiB input limit".into()));
    }
    String::from_utf8(bytes).map_err(|_| (3, "configuration must be valid UTF-8".into()))
}

fn json_text(value: &Value) -> String {
    format!(
        "{}\n",
        serde_json::to_string_pretty(value).expect("finite configuration metadata")
    )
}

fn diagnose(config: &resolve::Resolved) -> Value {
    let mut checks = vec![json!({"name":"static-configuration","status":"PASS"})];
    for key in config.selected_storage_roots() {
        let status = match config.text(key).and_then(|p| fs::symlink_metadata(p).ok()) {
            Some(m) if m.is_dir() && !m.file_type().is_symlink() => "OBSERVED",
            Some(_) => "FAIL",
            None => "UNAVAILABLE",
        };
        checks.push(json!({"name":key,"status":status,"meaning":"metadata only; writability/free space/alias identity not certified"}));
    }
    for dependency in &config.dependencies {
        checks.push(json!({"name":dependency["name"],"status":"NOT_RUN","reason":"owning read-only schema/grant/dataset/TLS adapter not implemented"}));
    }
    checks.push(json!({"name":"policy-authority","status":"NOT_RUN","reason":"durable Control revision/application adapter not implemented"}));
    json!({"format":"bigbrotr-doctor/1","status":"incomplete","checks":checks,"network_checks":"not-run","secret_contents_opened":false,"ready":false})
}

fn schema() -> Value {
    let mut root = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","title":"BigBrotr static initialization candidate","type":"object","additionalProperties":false,"properties":{},"required":["config","topology"],"x-bigbrotr-version":"bigbrotr-config-static/1","x-bigbrotr-note":"Schema describes input shape. The actual CLI also checks normalization, cross-field budgets and role dependencies. Durable authority and remaining service settings are not implemented."});
    for key in registry::keys() {
        insert_schema(
            &mut root,
            &key.name.split('.').collect::<Vec<_>>(),
            key.schema(),
        );
    }
    root["properties"]["config"]["required"] = json!(["recipe"]);
    root["properties"]["topology"]["required"] =
        json!(["instance_id", "dataset_id", "local_roles"]);
    root
}

fn insert_schema(target: &mut Value, path: &[&str], leaf: Value) {
    if path.len() == 1 {
        target["properties"][path[0]] = leaf;
    } else {
        if target["properties"].get(path[0]).is_none() {
            target["properties"][path[0]] =
                json!({"type":"object","additionalProperties":false,"properties":{}});
        }
        insert_schema(&mut target["properties"][path[0]], &path[1..], leaf);
    }
}

fn reference() -> String {
    let mut output = String::from(
        "# Static configuration reference\n\nGenerated by `bigbrotr config reference` from the same Rust registry used by the parser and JSON Schema.\n\nThis initial schema resolves candidate roots, selected roles and resource arithmetic. It does not activate managed/declarative authority, open engines, establish ownership or enable runtime services. Remaining service-specific settings and adapters are explicit implementation work; unknown keys fail. `doctor` returns exit 4 while live authority/adapter checks are unavailable.\n\nUse `bigbrotr config init --recipe full --output instance.toml`, then `config validate --file instance.toml` or `config show --file instance.toml`. Initialization creates candidate IDs and refuses any existing output path. A copied ID is not an ownership grant. Fixed examples contain reserved `.example` destinations and need operator enrollment before future acquisition.\n\n`validate` is offline and never resolves secret references. `show` is a diagnostic document, not a round-trippable configuration: byte values and durations are normalized to bytes/microseconds and unsigned integers are decimal strings. Dependent defaults identify their source key and any microsecond offset; monitor stored checks follow the chosen computed set, and the default API timeout follows the read timeout plus 5s. Secret references and relay path/query values are redacted. Structured revisit bands use start_age/end_age/cadence with explicit durations, ordered nonoverlapping intervals and a positive cadence; their normalized diagnostic durations are decimal microseconds. Desired/applied revisions stay null until Control exists. No environment override stack is read.\n\nStorage-root comparison removes repeated separators and trailing slashes; configured paths stay intact, and filesystem links and mounts remain runtime checks.\n\nSecret references require `file:/absolute/filename` with a final filename, no dot components or trailing slash, and at most 8192 bytes including `file:`. Credential consumers require a regular file owned by the effective process UID, POSIX mode exactly `0400` or `0600`, and no symlink in any path component. Secret bytes are preserved exactly, including a trailing newline; adapters own text/key semantics. These constraints do not certify filesystem ACLs or promise memory zeroization. Offline configuration commands validate only reference syntax; they do not check ownership, permissions or contents. See [protected local credentials](../security/credentials.md) for mount and runtime limits.\n\nStatic validation checks the declaration, not live engine versions, TLS certificates, permissions, filesystem aliases, accepted-work drain capacity or measured memory. Limits are checked with u64 arithmetic; complete signed events remain invariant. JSON Schema documents shapes while the CLI enforces semantic relationships.\n\nExit codes: 0 requested output/static success, 1 I/O, 2 usage, 3 invalid configuration, 4 incomplete doctor checks. Input is a regular UTF-8 TOML file bounded to 10MiB, with the upstream parser's recursion bound. Values are not echoed in parse errors.\n\nMissing static contracts remain explicit P01-T04 work: structured private routes/auth bindings/bans, observer allocation, directory sources, Geo sources, collection selectors, import scope/source shape, retention/maintenance plans, archive backend/provider roots and detailed derivation/ranking/read exposure/signing/publication/gateway policies. Cadence/resource templates for Deriver, Ranker and export do not implement target/profile/seed selection or finite export commands. Only the keys below are accepted; unsupported feature enablement is refused. No P01-T04 completion is claimed.\n\nThe six [recipes](../../config/recipes/) are static candidates generated by `config example --recipe NAME`; no installation or runtime qualification is implied. The split example assigns only sync locally and declares authenticated Control/Ingest/Archive owners. Local plaintext database routes require an explicit loopback/private-bridge boundary; cross-host routes use verify-full.\n\n| Key | Input kind | Default | Reload | Owner | Deployment locked |\n|---|---|---|---|---|---|\n",
    );
    for key in registry::keys() {
        let default = if let Some(source) = key.default_from {
            if key.default_add_us == 0 {
                format!("same as {source}")
            } else {
                format!("{source} + {}us", key.default_add_us)
            }
        } else if key.default.is_null() {
            "absent".to_owned()
        } else if key.sensitive() {
            "redacted/empty".to_owned()
        } else {
            key.default.to_string().replace('|', "\\|")
        };
        output.push_str(&format!(
            "| `{}` | `{:?}` | `{}` | {} | {} | {} |\n",
            key.name, key.kind, default, key.reload, key.owner, key.root
        ));
    }
    output
}
