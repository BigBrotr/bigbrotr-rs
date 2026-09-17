use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const NAMES: &[&str] = &[
    "full",
    "archive-only",
    "sync-only",
    "monitor-only",
    "serve-existing",
    "split-role",
];

pub fn overrides(recipe: &str) -> Result<BTreeMap<String, Value>, String> {
    if !NAMES.contains(&recipe) {
        return Err("unsupported configuration recipe".into());
    }
    let mut values = BTreeMap::new();
    macro_rules! set {
        ($key:expr,$value:expr) => {
            values.insert($key.to_string(), json!($value));
        };
    }
    set!("config.recipe", recipe);
    let roles = match recipe {
        "full" => vec![
            "control",
            "discoverer",
            "validator",
            "monitor",
            "sync",
            "ingest",
            "archive",
            "index",
            "deriver",
            "ranker",
            "read",
        ],
        "archive-only" => vec![
            "control",
            "discoverer",
            "validator",
            "monitor",
            "sync",
            "ingest",
            "archive",
        ],
        "sync-only" => vec!["control", "sync", "ingest", "archive"],
        "monitor-only" => vec!["control", "monitor", "ingest", "archive"],
        "serve-existing" => vec!["control", "read"],
        "split-role" => vec!["sync"],
        _ => unreachable!(),
    };
    set!("topology.local_roles", roles);
    for worker in [
        "validator",
        "monitor",
        "sync",
        "deriver",
        "ranker",
        "indexer",
    ] {
        let role = if worker == "indexer" { "index" } else { worker };
        set!(format!("{worker}.enabled"), roles.contains(&role));
    }
    set!("discoverer.events.enabled", roles.contains(&"discoverer"));
    for field in ["api.enabled", "dashboard.enabled", "search.enabled"] {
        set!(field, roles.contains(&"read"));
    }
    set!("administration.enabled", roles.contains(&"control"));
    set!("archive.publisher.enabled", roles.contains(&"archive"));
    let seeding = matches!(recipe, "full" | "archive-only");
    set!("bootstrap.seed", seeding);
    set!(
        "relay_policy.candidate_validation",
        if seeding { "public" } else { "disabled" }
    );
    set!("relay_policy.auto_enroll_validated_public", seeding);
    if matches!(recipe, "sync-only" | "split-role") {
        set!("sync.endpoints.mode", "fixed");
        set!("sync.endpoints.urls", ["wss://relay.example/"]);
    }
    if recipe == "monitor-only" {
        set!("monitor.endpoints.mode", "fixed");
        set!("monitor.endpoints.urls", ["wss://relay.example/"]);
    }
    for (field, name) in [
        ("ingest_root", "ingest"),
        ("index_root", "index"),
        ("archive_root", "archive"),
        ("scratch_root", "scratch"),
    ] {
        set!(
            format!("storage.{field}"),
            format!("/var/lib/bigbrotr/{name}")
        );
    }
    for pg in ["control", "catalog"] {
        set!(format!("postgres.{pg}.endpoint"), "127.0.0.1:5432");
        set!(format!("postgres.{pg}.database"), format!("bigbrotr_{pg}"));
        set!(format!("postgres.{pg}.user"), format!("bigbrotr_{pg}"));
        set!(
            format!("postgres.{pg}.password_ref"),
            format!("file:/run/secrets/bigbrotr-{pg}")
        );
        set!(format!("postgres.{pg}.tls.mode"), "local-plaintext");
        set!(format!("postgres.{pg}.tls.local_boundary"), "loopback");
    }
    set!("clickhouse.endpoint", "http://127.0.0.1:8123");
    set!("clickhouse.database", "bigbrotr");
    for action in ["read", "write"] {
        set!(
            format!("clickhouse.{action}_user"),
            format!("bigbrotr_{action}")
        );
        set!(
            format!("clickhouse.{action}_password_ref"),
            format!("file:/run/secrets/bigbrotr-ch-{action}")
        );
    }
    set!("clickhouse.tls.mode", "local-plaintext");
    set!("clickhouse.tls.local_boundary", "loopback");
    for (process, memory, baseline, payload, native, reserve) in [
        ("application", "4GiB", "512MiB", "2GiB", "512MiB", "512MiB"),
        (
            "postgres", "1536MiB", "512MiB", "256MiB", "512MiB", "256MiB",
        ),
        ("clickhouse", "4GiB", "512MiB", "1GiB", "2GiB", "512MiB"),
        ("native", "2GiB", "128MiB", "256MiB", "1GiB", "512MiB"),
    ] {
        for (field, value) in [
            ("memory_max_bytes", memory),
            ("baseline_bytes", baseline),
            ("payload_bytes", payload),
            ("native_bytes", native),
            ("completion_reserve_bytes", reserve),
        ] {
            set!(format!("runtime.processes.{process}.{field}"), value);
        }
    }
    set!(
        "runtime.processes.postgres.enabled",
        roles.contains(&"control") || roles.contains(&"archive")
    );
    set!(
        "runtime.processes.clickhouse.enabled",
        roles.contains(&"read") || roles.contains(&"index")
    );
    set!(
        "runtime.processes.native.enabled",
        roles.contains(&"archive") || roles.contains(&"deriver") || roles.contains(&"ranker")
    );
    if recipe == "split-role" {
        for role in ["control", "ingest", "archive"] {
            let prefix = format!("topology.remote.{role}");
            set!(
                format!("{prefix}.endpoint"),
                format!("https://{role}.example:9443")
            );
            set!(
                format!("{prefix}.dataset_id"),
                "22222222222222222222222222222222"
            );
            set!(format!("{prefix}.owner_epoch"), 1);
            set!(format!("{prefix}.peer_id"), format!("bigbrotr-{role}"));
            set!(format!("{prefix}.ca_file"), "/etc/bigbrotr/rpc-ca.pem");
            set!(
                format!("{prefix}.client_cert_ref"),
                "file:/run/secrets/bigbrotr-rpc-cert"
            );
            set!(
                format!("{prefix}.client_key_ref"),
                "file:/run/secrets/bigbrotr-rpc-key"
            );
        }
    }
    Ok(values)
}

pub fn document(recipe: &str, instance: &str, dataset: &str) -> Result<String, String> {
    let mut values = overrides(recipe)?;
    values.insert("config.schema_version".into(), json!(1));
    values.insert("config.authority_mode".into(), json!("managed"));
    values.insert("topology.instance_id".into(), json!(instance));
    values.insert("topology.dataset_id".into(), json!(dataset));
    for role in ["control", "ingest", "archive"] {
        let key = format!("topology.remote.{role}.dataset_id");
        if values.contains_key(&key) {
            values.insert(key, json!(dataset));
        }
    }
    let mut table = toml::Table::new();
    for (path, value) in values {
        let parts = path.split('.').collect::<Vec<_>>();
        insert(
            &mut table,
            &parts,
            toml::Value::try_from(value).map_err(|_| "recipe serialization failed")?,
        );
    }
    let body = toml::to_string_pretty(&table).map_err(|_| "recipe serialization failed")?;
    Ok(format!(
        "# BigBrotr static initialization candidate, schema 1.\n# No database, owner, enrollment, listener or applied policy is created by this file.\n# Replace .example endpoints and protected secret paths before runtime activation.\n# Copying IDs does not grant ownership; config init generates fresh candidate IDs.\n{body}"
    ))
}

fn insert(table: &mut toml::Table, parts: &[&str], value: toml::Value) {
    if parts.len() == 1 {
        table.insert(parts[0].to_owned(), value);
    } else {
        let entry = table
            .entry(parts[0])
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        insert(
            entry.as_table_mut().expect("registry table path"),
            &parts[1..],
            value,
        );
    }
}
