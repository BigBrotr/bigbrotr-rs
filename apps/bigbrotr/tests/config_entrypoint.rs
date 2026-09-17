use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn run(directory: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(args)
        .current_dir(directory)
        .env_clear()
        .output()
        .unwrap()
}

fn success(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn init_is_explicit_and_refuses_overwrite_and_missing_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let output = run(
        path,
        &[
            "config",
            "init",
            "--recipe",
            "archive-only",
            "--output",
            "instance.toml",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let original = fs::read(path.join("instance.toml")).unwrap();
    assert!(!original.is_empty());
    assert_eq!(
        run(
            path,
            &[
                "config",
                "init",
                "--recipe",
                "full",
                "--output",
                "instance.toml"
            ]
        )
        .status
        .code(),
        Some(1)
    );
    assert_eq!(fs::read(path.join("instance.toml")).unwrap(), original);
    assert_eq!(
        run(path, &["config", "validate", "--file", "missing.toml"])
            .status
            .code(),
        Some(1)
    );
    let result = success(run(
        path,
        &["config", "validate", "--file", "instance.toml"],
    ));
    assert_eq!(result["status"], "valid-static");
    assert_eq!(result["authority"]["applied_revision"], Value::Null);
    assert_eq!(result["authority"]["status"], "unavailable-offline");
}

#[test]
fn recipes_resolve_only_selected_work_and_never_activate_state() {
    let directory = tempfile::tempdir().unwrap();
    for recipe in [
        "full",
        "archive-only",
        "sync-only",
        "monitor-only",
        "serve-existing",
        "split-role",
    ] {
        let filename = format!("{recipe}.toml");
        let output = run(
            directory.path(),
            &["config", "init", "--recipe", recipe, "--output", &filename],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let show = success(run(
            directory.path(),
            &["config", "show", "--file", &filename],
        ));
        assert_eq!(show["authority"]["desired_revision"], Value::Null);
        assert_eq!(show["full_signed_events"], true);
        assert_eq!(show["listeners"]["public"], false);
        assert_eq!(show["signing"], false);
        assert_eq!(show["directory_discovery"], false);
        let roles = show["local_roles"].as_array().unwrap();
        if recipe == "monitor-only" {
            assert!(!roles.contains(&Value::from("sync")));
        }
        if recipe == "sync-only" {
            assert!(!roles.contains(&Value::from("monitor")));
        }
        if recipe == "serve-existing" {
            assert!(!roles.contains(&Value::from("ingest")));
        }
        if matches!(recipe, "archive-only" | "sync-only" | "monitor-only") {
            assert!(
                !show["required_dependencies"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v["name"] == "clickhouse")
            );
        }
    }
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 6);
}

fn example(path: &Path, recipe: &str) -> String {
    let output = run(path, &["config", "example", "--recipe", recipe]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn invalid_error(path: &Path, document: &str) -> String {
    fs::write(path.join("bad.toml"), document).unwrap();
    let output = run(path, &["config", "validate", "--file", "bad.toml"]);
    assert_eq!(
        output.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("SECRET_SENTINEL"));
    String::from_utf8(output.stderr).unwrap()
}

fn reject(path: &Path, document: &str) {
    invalid_error(path, document);
}

#[test]
fn unknown_duplicates_limits_and_fixed_mode_fail_without_echoing_values() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "sync-only");
    for invalid in [
        format!("{base}\n[unknown]\npassword='SECRET_SENTINEL'\n"),
        format!("{base}\n[extra]\n"),
        base.replace(
            "authority_mode = \"managed\"",
            "authority_mode = \"managed\"\nauthority_mode = \"managed\"",
        ),
        base.replace("mode = \"fixed\"", "mode = \"enrolled\""),
        base.replace("\"wss://relay.example/\"", ""),
        base.replace("mode = \"managed\"", "mode = \"invalid\""),
        format!("{base}\n[pipeline]\nprepare_slot_bytes='1MiB'\n"),
        format!("{base}\n[rpc]\nbatch_max_bytes='1MiB'\n"),
        format!("{base}\n[ingest]\npending_low_fraction=0.95\n"),
        format!("{base}\n[network]\nmax_connections=0\n"),
        format!("{base}\n[network]\nconnect_timeout=10\n"),
        format!("{base}\n[ingest]\npending_low_fraction=nan\n"),
        base.replace(
            "password_ref = \"file:/run/secrets/bigbrotr-control\"",
            "password_ref = \"SECRET_SENTINEL\"",
        ),
        base.replace(
            "memory_max_bytes = \"4GiB\"",
            "memory_max_bytes = \"18446744073709551615TiB\"",
        ),
        format!("{base}\n[ingest]\nheavy_fields=false\n"),
        format!(
            "{base}\n[monitor.active_write]\nenabled=true\nsigner='file:/secret/SECRET_SENTINEL'\n"
        ),
    ] {
        reject(path, &invalid);
    }
}

#[test]
fn required_owners_tls_and_unimplemented_enablement_cannot_be_hidden() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "serve-existing");
    for invalid in [
        base.replace("    \"control\",\n", ""),
        base.replace("127.0.0.1:5432", "db.example:5432"),
        base.replace(
            "http://127.0.0.1:8123",
            "http://user:SECRET_SENTINEL@db.example:8123",
        ),
        format!("{base}\n[mcp]\nenabled=true\n"),
        format!("{base}\n[network.tor]\nenabled=true\nproxy='socks5://127.0.0.1:9050'\n"),
    ] {
        reject(path, &invalid);
    }
    let sync = example(path, "sync-only");
    reject(
        path,
        &sync.replace("[sync]\n", "[sync]\nmode='negentropy'\n"),
    );
}

#[test]
fn redacted_show_and_incomplete_doctor_do_not_read_staged_secret_contents() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "monitor-only");
    let document = base.replace(
        "wss://relay.example/",
        "wss://private.example/SECRET_SENTINEL?token=SECRET_SENTINEL",
    );
    let document =
        format!("{document}\n[monitor.active_write]\nsigner='file:/missing/SECRET_SENTINEL'\n");
    fs::write(path.join("instance.toml"), &document).unwrap();
    let show = run(path, &["config", "show", "--file", "instance.toml"]);
    assert!(
        show.status.success(),
        "{}",
        String::from_utf8_lossy(&show.stderr)
    );
    assert!(!String::from_utf8_lossy(&show.stdout).contains("SECRET_SENTINEL"));
    assert!(!String::from_utf8_lossy(&show.stderr).contains("SECRET_SENTINEL"));
    let doctor = run(path, &["doctor", "--file", "instance.toml"]);
    assert_eq!(doctor.status.code(), Some(4));
    let report: Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["ready"], false);
    assert_eq!(report["secret_contents_opened"], false);
    assert_eq!(report["network_checks"], "not-run");
    assert_eq!(fs::read_dir(path).unwrap().count(), 1);
}

#[test]
fn disabled_connection_syntax_and_selected_process_grants_are_still_checked() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let archive = example(path, "archive-only");
    reject(
        path,
        &archive.replace(
            "http://127.0.0.1:8123",
            "http://user:SECRET_SENTINEL@db.example:8123",
        ),
    );
    let full = example(path, "full");
    let marker = "[runtime.processes.clickhouse]";
    let position = full.find(marker).unwrap();
    let mut changed = full[..position].to_owned();
    changed.push_str(&full[position..].replacen("enabled = true", "enabled = false", 1));
    assert_ne!(changed, full);
    reject(path, &changed);
}

#[test]
fn acquisition_tables_keep_units_ranges_and_disabled_block_validation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "sync-only");
    let configured = base.replace(
        "[sync]\n",
        "[sync]\nrequest_timeout='45s'\nlinear_limit=250\n",
    );
    fs::write(path.join("input.toml"), &configured).unwrap();
    let show = success(run(path, &["config", "show", "--file", "input.toml"]));
    assert_eq!(
        show["settings"]["sync.request_timeout"]["value"],
        "45000000"
    );
    assert_eq!(show["settings"]["sync.linear_limit"]["value"], "250");
    reject(
        path,
        &configured.replace("linear_limit=250", "linear_limit=0"),
    );
    reject(
        path,
        &format!("{base}\n[monitor.geo]\ngeohash_precision=13\n"),
    );
    reject(path, &base.replace("[monitor]\n", "[monitor]\nretries=0\n"));
}

#[cfg(unix)]
#[test]
fn configuration_descriptors_refuse_symlinks_devices_and_fifos() {
    use std::{
        os::unix::fs::symlink,
        thread,
        time::{Duration, Instant},
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    fs::write(path.join("real.toml"), example(path, "full")).unwrap();
    symlink("real.toml", path.join("link.toml")).unwrap();
    assert_eq!(
        run(path, &["config", "validate", "--file", "link.toml"])
            .status
            .code(),
        Some(1)
    );
    assert_eq!(
        run(path, &["config", "validate", "--file", "/dev/null"])
            .status
            .code(),
        Some(1)
    );
    assert!(
        Command::new("mkfifo")
            .arg(path.join("pipe"))
            .status()
            .expect("POSIX mkfifo fixture tool")
            .success()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(["config", "validate", "--file", "pipe"])
        .current_dir(path)
        .env_clear()
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(1));
            break;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("configuration FIFO blocked the CLI");
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn endpoint_lists_deduplicate_canonical_identity_and_keep_query_distinctions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "sync-only");
    let document = base.replace(
        "\"wss://relay.example/\"",
        "\"WSS://RELAY.EXAMPLE:443\", \"wss://relay.example/\", \"wss://relay.example/?\"",
    );
    fs::write(path.join("input.toml"), document).unwrap();
    let result = success(run(path, &["config", "validate", "--file", "input.toml"]));
    assert_eq!(result["fixed_endpoint_counts"]["sync"], 2);
}

#[test]
fn review_counterexamples_fail_at_the_relevant_static_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "full");
    for invalid in [
        format!("{base}\n[network.tor]\nproxy='socks5://user:SECRET_SENTINEL@127.0.0.1:9050'\n"),
        format!("{base}\n[network.tor]\nproxy='not a url'\n"),
        base.replace("[indexer]\n", "[indexer]\nbatch_max_bytes='1B'\n"),
        base.replace("[indexer]\n", "[indexer]\nquery_memory='1TiB'\n"),
        format!("{base}\n[ingest]\npending_max_bytes='1B'\n"),
    ] {
        reject(path, &invalid);
    }
    let serving = example(path, "serve-existing");
    // This catalog endpoint is unused by serving, but malformed staged input is invalid.
    reject(
        path,
        &serving.replacen(
            "endpoint = \"127.0.0.1:5432\"",
            "endpoint = \"127.0.0.1:\"",
            1,
        ),
    );
}

#[test]
fn raw_endpoint_array_limit_precedes_normalization_and_deduplication() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "sync-only");
    let endpoint = format!("wss://relay.example/{}", "./".repeat(4000));
    let entries = std::iter::repeat_n(format!("\"{endpoint}\""), 1100)
        .collect::<Vec<_>>()
        .join(",");
    assert!(entries.len() > 8 * 1024 * 1024);
    let document = base.replace("\"wss://relay.example/\"", &entries);
    assert!(document.len() < 10 * 1024 * 1024);
    fs::write(path.join("input.toml"), document).unwrap();
    let out = run(path, &["config", "validate", "--file", "input.toml"]);
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stderr).contains("8MiB"));
}

fn configured_roles(base: &str, roles: &[&str]) -> toml::Table {
    let mut document = base.parse::<toml::Table>().unwrap();
    document["topology"]["local_roles"] = toml::Value::Array(
        roles
            .iter()
            .map(|role| toml::Value::String((*role).into()))
            .collect(),
    );
    document
}

#[test]
fn active_storage_producers_require_sources_but_retained_storage_can_pause() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "full");
    let mut index = configured_roles(&base, &["control", "index"]);
    assert!(
        invalid_error(path, &toml::to_string(&index).unwrap()).contains("enabled Indexer requires")
    );
    index["indexer"]["enabled"] = toml::Value::Boolean(false);
    fs::write(path.join("index.toml"), toml::to_string(&index).unwrap()).unwrap();
    success(run(path, &["config", "validate", "--file", "index.toml"]));

    let mut archive = configured_roles(&base, &["control", "archive"]);
    assert!(
        invalid_error(path, &toml::to_string(&archive).unwrap())
            .contains("archive publisher requires")
    );
    archive["archive"]["publisher"]["enabled"] = toml::Value::Boolean(false);
    fs::write(
        path.join("archive.toml"),
        toml::to_string(&archive).unwrap(),
    )
    .unwrap();
    success(run(path, &["config", "validate", "--file", "archive.toml"]));
}

#[test]
fn applicability_follows_concrete_consumers_and_not_recipe_name() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    for (recipe, expected) in [
        (
            "full",
            vec![
                ("storage.ingest_root", "selected"),
                ("storage.index_root", "selected"),
                ("storage.archive_root", "selected"),
                ("network.max_connections", "selected"),
                ("indexer.batch_max_bytes", "selected"),
                ("clickhouse.write_password_ref", "selected"),
                ("postgres.catalog.password_ref", "selected"),
            ],
        ),
        (
            "serve-existing",
            vec![
                ("storage.ingest_root", "staged-unselected"),
                ("storage.archive_root", "staged-unselected"),
                ("network.max_connections", "staged-unselected"),
                ("indexer.batch_max_bytes", "staged-unselected"),
                ("clickhouse.write_password_ref", "staged-unselected"),
                ("postgres.catalog.password_ref", "staged-unselected"),
                ("clickhouse.read_password_ref", "selected"),
                ("postgres.control.password_ref", "selected"),
                ("administration.enabled", "selected"),
            ],
        ),
    ] {
        fs::write(path.join("input.toml"), example(path, recipe)).unwrap();
        let show = success(run(path, &["config", "show", "--file", "input.toml"]));
        for (key, applicability) in expected {
            assert_eq!(
                show["settings"][key]["applicability"], applicability,
                "{recipe}: {key}"
            );
        }
    }
}

#[test]
fn configuration_preserves_full_unsigned_owner_epoch_without_float_rounding() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let document = example(path, "split-role")
        .replace("owner_epoch = 1", "owner_epoch = 18446744073709551615");
    fs::write(path.join("input.toml"), &document).unwrap();
    let show = success(run(path, &["config", "show", "--file", "input.toml"]));
    assert_eq!(
        show["settings"]["topology.remote.ingest.owner_epoch"]["value"],
        "18446744073709551615"
    );
    reject(
        path,
        &document.replace("18446744073709551615", "18446744073709551616"),
    );
}

#[test]
fn generated_consumers_match_registry_and_examples_validate_outside_checkout() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (command, file) in [
        ("schema", "contracts/config/schema.json"),
        ("reference", "docs/reference/configuration.md"),
    ] {
        let output = run(path, &["config", command]);
        assert!(output.status.success());
        assert!(
            output.stdout == fs::read(root.join(file)).unwrap(),
            "regenerate {file}"
        );
    }
    for recipe in [
        "full",
        "archive-only",
        "sync-only",
        "monitor-only",
        "serve-existing",
        "split-role",
    ] {
        let generated = example(path, recipe);
        assert!(
            generated
                == fs::read_to_string(root.join(format!("config/recipes/{recipe}.toml"))).unwrap(),
            "regenerate {recipe}"
        );
        fs::write(path.join("input.toml"), generated).unwrap();
        success(run(path, &["config", "validate", "--file", "input.toml"]));
    }
}

#[test]
fn monitor_stored_checks_follow_selected_checks_and_refuse_incompatible_sets() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "monitor-only");
    let document = base.replace("[monitor]\n", "[monitor]\nchecks=['open','read']\n");
    fs::write(path.join("input.toml"), &document).unwrap();
    let show = success(run(path, &["config", "show", "--file", "input.toml"]));
    assert_eq!(
        show["settings"]["monitor.store_checks"]["value"],
        serde_json::json!(["open", "read"])
    );
    assert_eq!(
        show["settings"]["monitor.store_checks"]["source"],
        "derived-default"
    );
    assert_eq!(
        show["settings"]["monitor.store_checks"]["default_from"],
        "monitor.checks"
    );
    fs::write(
        path.join("input.toml"),
        document.replace("[monitor]\n", "[monitor]\nstore_checks=['read']\n"),
    )
    .unwrap();
    let explicit = success(run(path, &["config", "show", "--file", "input.toml"]));
    assert_eq!(
        explicit["settings"]["monitor.store_checks"]["value"],
        serde_json::json!(["read"])
    );
    assert_eq!(
        explicit["settings"]["monitor.store_checks"]["source"],
        "file"
    );
    reject(
        path,
        &document.replace("[monitor]\n", "[monitor]\nstore_checks=['nip11']\n"),
    );
    reject(path, &document.replace("['open','read']", "['made_up']"));
    reject(
        path,
        &document.replace("['open','read']", "['open','open']"),
    );
    reject(path, &document.replace("['open','read']", "[]"));
    let disabled = document.replace("['open','read']", "[]").replace(
        "[monitor]\nchecks=[]\nenabled = true",
        "[monitor]\nchecks=[]\nenabled = false",
    );
    fs::write(path.join("input.toml"), disabled).unwrap();
    success(run(path, &["config", "validate", "--file", "input.toml"]));
}

#[test]
fn configured_extra_ca_paths_are_bounded_paths_without_offline_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "sync-only");
    let document = format!("{base}\n[network.tls]\nextra_roots=['/missing/custom-ca.pem']\n");
    fs::write(path.join("input.toml"), &document).unwrap();
    let show = success(run(path, &["config", "show", "--file", "input.toml"]));
    assert_eq!(
        show["settings"]["network.tls.extra_roots"]["value"],
        serde_json::json!(["/missing/custom-ca.pem"])
    );
    reject(
        path,
        &document.replace("/missing/custom-ca.pem", "relative.pem"),
    );
    reject(
        path,
        &document.replace("/missing/custom-ca.pem", "/missing/../custom-ca.pem"),
    );
    reject(
        path,
        &document.replace("['/missing/custom-ca.pem']", "['/a.pem','//a.pem']"),
    );
}

#[test]
fn shared_read_limits_validate_page_delivery_authority_and_process_allowances() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "serve-existing");
    let configured = format!(
        "{base}\n[read]\ndefault_page_size=50\nmax_page_size=200\n[read.execution]\nmax_concurrent=8\ntimeout='10s'\n[read.cursor]\nclock_skew='0s'\n"
    );
    fs::write(path.join("input.toml"), &configured).unwrap();
    let show = success(run(path, &["config", "show", "--file", "input.toml"]));
    assert_eq!(show["settings"]["read.default_page_size"]["value"], "50");
    assert_eq!(show["settings"]["read.cursor.clock_skew"]["value"], "0");
    assert_eq!(show["settings"]["api.request_timeout"]["value"], "15000000");
    assert_eq!(
        show["settings"]["read.response.max_bytes"]["value"],
        "16777216"
    );
    for invalid in [
        configured.replace("default_page_size=50", "default_page_size=201"),
        configured.replace("clock_skew='0s'", "clock_skew='31s'"),
        configured.replace(
            "[read.execution]\n",
            "[read.execution]\nmax_memory_bytes='1TiB'\n",
        ),
        format!("{configured}\n[read.authorization]\nlease_ttl='6s'\n"),
        configured.replace("[api]\n", "[api]\nrequest_timeout='9s'\n"),
    ] {
        reject(path, &invalid);
    }
}

#[test]
fn known_lexical_storage_aliases_cannot_create_distinct_owner_roots() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let base = example(path, "full");
    let alias = base.replace("/var/lib/bigbrotr/index", "/var//lib/bigbrotr/ingest/");
    assert!(invalid_error(path, &alias).contains("storage roots must be distinct"));
}
