//! This registry owns static defaults, accepted value shapes and reload metadata.
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Bool,
    UInt(u64, u64),
    Bytes,
    Duration,
    DurationZero,
    Ratio,
    Enum(&'static [&'static str]),
    Text,
    Path,
    Secret,
    Proxy,
    Identity,
    Strings,
    Paths,
    ChoiceSet(&'static [&'static str]),
    Endpoints,
}

#[derive(Clone, Debug)]
pub struct Key {
    pub name: String,
    pub kind: Kind,
    pub default: Value,
    pub default_from: Option<&'static str>,
    pub default_add_us: u64,
    pub reload: &'static str,
    pub owner: &'static str,
    pub root: bool,
}

impl Key {
    pub fn sensitive(&self) -> bool {
        matches!(self.kind, Kind::Secret | Kind::Endpoints | Kind::Proxy)
    }
    pub fn schema(&self) -> Value {
        let mut value = match self.kind {
            Kind::Bool => json!({"type":"boolean"}),
            Kind::UInt(min, max) => {
                json!({"oneOf":[{"type":"integer", "minimum":min,"maximum":max},{"type":"string","pattern":"^(0|[1-9][0-9]*)$","format":"bigbrotr-u64"}],"x-bigbrotr-minimum":min,"x-bigbrotr-maximum":max})
            }
            Kind::Bytes => json!({"type":"string", "pattern":"^[1-9][0-9]*(B|KiB|MiB|GiB|TiB)$"}),
            Kind::Duration => json!({"type":"string", "pattern":"^[1-9][0-9]*(us|ms|s|m|h|d)$"}),
            Kind::DurationZero => {
                json!({"type":"string", "pattern":"^(0|[1-9][0-9]*)(us|ms|s|m|h|d)$"})
            }
            Kind::Ratio => json!({"type":"number","minimum":0,"maximum":1}),
            Kind::Enum(values) => json!({"type":"string", "enum":values}),
            Kind::Text => json!({"type":"string","maxLength":8192}),
            Kind::Path => json!({"type":"string","format":"bigbrotr-absolute-path"}),
            Kind::Proxy => json!({"type":"string","format":"bigbrotr-proxy-url","writeOnly":true}),
            Kind::Secret => {
                json!({"type":"string","format":"bigbrotr-file-secret-reference", "writeOnly":true})
            }
            Kind::Identity => {
                json!({"type":"string","pattern":"^[0-9a-f]{32}$", "not":{"const":"00000000000000000000000000000000"}})
            }
            Kind::Strings => {
                json!({"type":"array","items":{"type":"string","maxLength":8192},"maxItems":100000,"uniqueItems":true})
            }
            Kind::Paths => {
                json!({"type":"array","items":{"type":"string","format":"bigbrotr-absolute-path","maxLength":8192},"maxItems":100000,"uniqueItems":true})
            }
            Kind::ChoiceSet(allowed) => {
                json!({"type":"array","items":{"type":"string","enum":allowed},"maxItems":allowed.len(),"uniqueItems":true})
            }
            Kind::Endpoints => {
                json!({"type":"array","items":{"type":"string","format":"bigbrotr-endpoint/1","maxLength":8192},"maxItems":100000})
            }
        };
        if !self.default.is_null() {
            value["default"] = self.default.clone();
        }
        if let Some(source) = self.default_from {
            value["x-bigbrotr-default-from"] = json!(source);
            if self.default_add_us > 0 {
                value["x-bigbrotr-default-add-us"] = json!(self.default_add_us.to_string());
            }
        }
        value["x-bigbrotr-reload"] = json!(self.reload);
        value["x-bigbrotr-owner"] = json!(self.owner);
        value["x-bigbrotr-deployment-locked"] = json!(self.root);
        value
    }
}

pub fn keys() -> Vec<Key> {
    use Kind::*;
    let mut keys = Vec::new();
    macro_rules! add {
        ($name:expr, $kind:expr, $default:expr, $reload:expr, $owner:expr, $root:expr) => {
            keys.push(Key {
                name: $name.to_string(),
                kind: $kind,
                default: json!($default),
                default_from: None,
                default_add_us: 0,
                reload: $reload,
                owner: $owner,
                root: $root,
            });
        };
    }
    add!(
        "config.schema_version",
        UInt(1, 1),
        1,
        "restart",
        "control",
        true
    );
    add!(
        "config.authority_mode",
        Enum(&["managed", "declarative"]),
        "managed",
        "restart",
        "control",
        true
    );
    add!(
        "config.recipe",
        Enum(&[
            "full",
            "archive-only",
            "sync-only",
            "monitor-only",
            "serve-existing",
            "split-role"
        ]),
        "full",
        "initialization",
        "control",
        true
    );
    add!(
        "topology.instance_id",
        Identity,
        Value::Null,
        "identity",
        "control",
        true
    );
    add!(
        "topology.dataset_id",
        Identity,
        Value::Null,
        "identity",
        "control",
        true
    );
    add!(
        "topology.local_roles",
        Strings,
        Value::Null,
        "restart",
        "control",
        true
    );
    add!(
        "topology.ordered_set_reader",
        Enum(&["absent", "index"]),
        "absent",
        "restart",
        "index",
        true
    );
    for role in ["control", "ingest", "archive", "index", "read"] {
        for (field, kind) in [
            ("endpoint", Text),
            ("dataset_id", Identity),
            ("owner_epoch", UInt(1, u64::MAX)),
            ("peer_id", Text),
            ("ca_file", Path),
            ("client_cert_ref", Secret),
            ("client_key_ref", Secret),
        ] {
            add!(
                format!("topology.remote.{role}.{field}"),
                kind,
                Value::Null,
                "restart",
                "control",
                true
            );
        }
    }
    add!(
        "secrets.file_provider",
        Enum(&["protected-files"]),
        "protected-files",
        "restart",
        "control",
        true
    );
    for name in ["ingest_root", "index_root", "archive_root", "scratch_root"] {
        add!(
            format!("storage.{name}"),
            Path,
            Value::Null,
            "restart",
            "storage",
            true
        );
    }
    for pg in ["control", "catalog"] {
        add!(
            format!("postgres.{pg}.placement"),
            Enum(&["local", "remote"]),
            "local",
            "restart",
            "control",
            true
        );
        for field in ["endpoint", "database", "user"] {
            add!(
                format!("postgres.{pg}.{field}"),
                Text,
                Value::Null,
                "restart",
                "control",
                true
            );
        }
        add!(
            format!("postgres.{pg}.password_ref"),
            Secret,
            Value::Null,
            "restart",
            "control",
            true
        );
    }
    add!(
        "clickhouse.placement",
        Enum(&["local", "remote"]),
        "local",
        "restart",
        "index",
        true
    );
    for field in ["endpoint", "database", "read_user", "write_user"] {
        add!(
            format!("clickhouse.{field}"),
            Text,
            Value::Null,
            "restart",
            "index",
            true
        );
    }
    for field in ["read_password_ref", "write_password_ref"] {
        add!(
            format!("clickhouse.{field}"),
            Secret,
            Value::Null,
            "restart",
            "index",
            true
        );
    }
    for connection in ["postgres.control", "postgres.catalog", "clickhouse"] {
        add!(
            format!("{connection}.tls.mode"),
            Enum(&["verify-full", "local-plaintext"]),
            "verify-full",
            "restart",
            "control",
            true
        );
        add!(
            format!("{connection}.tls.local_boundary"),
            Enum(&["loopback", "private-bridge"]),
            Value::Null,
            "restart",
            "control",
            true
        );
        add!(
            format!("{connection}.tls.ca_file"),
            Path,
            Value::Null,
            "restart",
            "control",
            true
        );
        add!(
            format!("{connection}.tls.server_name"),
            Text,
            Value::Null,
            "restart",
            "control",
            true
        );
        add!(
            format!("{connection}.tls.client_cert_ref"),
            Secret,
            Value::Null,
            "restart",
            "control",
            true
        );
        add!(
            format!("{connection}.tls.client_key_ref"),
            Secret,
            Value::Null,
            "restart",
            "control",
            true
        );
    }
    for (name, default) in [
        ("host_memory_bytes", "16GiB"),
        ("os_headroom_bytes", "2GiB"),
        ("disk_completion_reserve_bytes", "64GiB"),
        ("disk_resume_margin_bytes", "16GiB"),
    ] {
        add!(
            format!("runtime.{name}"),
            Bytes,
            default,
            "restart",
            "runtime",
            true
        );
    }
    for process in ["application", "postgres", "clickhouse", "native"] {
        for (field, default) in [
            ("memory_max_bytes", "1GiB"),
            ("baseline_bytes", "128MiB"),
            ("payload_bytes", "256MiB"),
            ("native_bytes", "256MiB"),
            ("completion_reserve_bytes", "32MiB"),
        ] {
            add!(
                format!("runtime.processes.{process}.{field}"),
                Bytes,
                default,
                "restart",
                "runtime",
                true
            );
        }
        add!(
            format!("runtime.processes.{process}.enabled"),
            Bool,
            true,
            "restart",
            "runtime",
            true
        );
    }
    add!(
        "runtime.heavy_job_slots",
        UInt(1, 64),
        1,
        "cycle",
        "runtime",
        false
    );
    for (field, default) in [
        ("lease_ttl", "60s"),
        ("heartbeat_interval", "20s"),
        ("renewal_timeout", "5s"),
        ("renewal_safety_margin", "20s"),
        ("graceful_shutdown_timeout", "30s"),
        ("pressure_stable_duration", "30s"),
        ("authorization_ttl", "5s"),
    ] {
        add!(
            format!("runtime.{field}"),
            Duration,
            default,
            "restart",
            "runtime",
            true
        );
    }
    for (key, default, owner, reload) in [
        ("ingest.max_event_bytes", "256KiB", "ingest", "generation"),
        ("ingest.producer_queue_bytes", "32MiB", "ingest", "live"),
        ("ingest.group_commit_max_bytes", "4MiB", "ingest", "live"),
        ("ingest.pending_max_bytes", "256GiB", "ingest", "live"),
        ("ingest.identity_cache_bytes", "256MiB", "ingest", "restart"),
        ("pipeline.max_fact_bytes", "2MiB", "ingest", "generation"),
        (
            "pipeline.prepare_slot_bytes",
            "16MiB",
            "ingest",
            "generation",
        ),
        ("pipeline.parse_arena_bytes", "8MiB", "ingest", "generation"),
        (
            "network.max_message_bytes",
            "2MiB",
            "acquisition",
            "connection",
        ),
        (
            "network.max_inflated_bytes",
            "2MiB",
            "acquisition",
            "connection",
        ),
        ("rpc.max_encode_bytes", "8MiB", "runtime", "restart"),
        ("rpc.max_decode_bytes", "8MiB", "runtime", "restart"),
        ("rpc.batch_max_bytes", "4MiB", "runtime", "restart"),
        ("archive.target_file_bytes", "256MiB", "archive", "cycle"),
        ("archive.worker_memory_bytes", "512MiB", "archive", "job"),
        ("indexer.batch_max_bytes", "16MiB", "index", "cycle"),
        ("indexer.query_memory", "256MiB", "index", "cycle"),
        (
            "archive_relay.outbound_message_max_bytes",
            "1MiB",
            "read",
            "generation",
        ),
    ] {
        add!(key, Bytes, default, reload, owner, false);
    }
    for (key, default, owner) in [
        ("pipeline.parse_max_nodes", 65536, "ingest"),
        ("pipeline.parse_max_depth", 32, "ingest"),
        ("ingest.producer_queue_items", 1024, "ingest"),
        ("ingest.producer_outstanding_requests", 64, "ingest"),
        ("ingest.verify_workers", 2, "ingest"),
        ("network.max_connections", 128, "acquisition"),
        ("network.endpoint_connections", 1, "acquisition"),
        ("network.host_connections", 4, "acquisition"),
        ("network.host_request_rate", 2, "acquisition"),
        ("network.max_inflate_ratio", 10, "acquisition"),
        ("archive.row_group_rows", 64000, "archive"),
        ("archive.prepare_workers", 2, "archive"),
        ("indexer.batch_max_rows", 20000, "index"),
        ("indexer.insert_workers", 2, "index"),
    ] {
        add!(
            key,
            UInt(1, u32::MAX as u64),
            default,
            if key.starts_with("pipeline.") {
                "generation"
            } else if key.starts_with("network.") || key.starts_with("ingest.") {
                "live"
            } else {
                "cycle"
            },
            owner,
            false
        );
    }
    add!(
        "network.http_redirects",
        UInt(0, 10),
        3,
        "connection",
        "acquisition",
        false
    );
    add!(
        "network.public.enabled",
        Bool,
        true,
        "live",
        "acquisition",
        false
    );
    add!(
        "network.websocket_compression",
        Enum(&["disabled"]),
        "disabled",
        "connection",
        "acquisition",
        false
    );
    add!(
        "ingest.wal_sync",
        Enum(&["required"]),
        "required",
        "invariant",
        "ingest",
        true
    );
    add!(
        "ingest.pending_high_fraction",
        Ratio,
        0.90,
        "live",
        "ingest",
        false
    );
    add!(
        "ingest.pending_low_fraction",
        Ratio,
        0.75,
        "live",
        "ingest",
        false
    );
    for (key, default, owner, reload) in [
        ("ingest.group_commit_max_delay", "5ms", "ingest", "live"),
        (
            "network.connect_timeout",
            "10s",
            "acquisition",
            "connection",
        ),
        ("archive.max_publish_delay", "30s", "archive", "cycle"),
        ("archive.scrub_interval", "7d", "archive", "live"),
        ("indexer.flush_interval", "1s", "index", "cycle"),
    ] {
        add!(key, Duration, default, reload, owner, false);
    }
    add!(
        "archive.compression",
        Enum(&["zstd-3"]),
        "zstd-3",
        "cycle",
        "archive",
        false
    );
    add!(
        "bootstrap.seed",
        Bool,
        true,
        "initialization",
        "control",
        false
    );
    add!(
        "relay_policy.candidate_validation",
        Enum(&["public", "enrolled_only", "disabled"]),
        "public",
        "live",
        "validator",
        false
    );
    add!(
        "relay_policy.auto_enroll_validated_public",
        Bool,
        true,
        "live",
        "validator",
        false
    );
    for worker in [
        "validator",
        "monitor",
        "sync",
        "deriver",
        "ranker",
        "indexer",
    ] {
        add!(
            format!("{worker}.enabled"),
            Bool,
            true,
            "live",
            worker,
            false
        );
    }
    add!(
        "discoverer.events.enabled",
        Bool,
        true,
        "live",
        "discoverer",
        false
    );
    add!(
        "discoverer.api.enabled",
        Bool,
        false,
        "live",
        "discoverer",
        false
    );
    add!(
        "discoverer.api.sources",
        Strings,
        Vec::<String>::new(),
        "live",
        "discoverer",
        false
    );
    for worker in ["sync", "monitor"] {
        add!(
            format!("{worker}.endpoints.mode"),
            Enum(&["enrolled", "fixed"]),
            "enrolled",
            "live",
            worker,
            false
        );
        add!(
            format!("{worker}.endpoints.urls"),
            Endpoints,
            Vec::<String>::new(),
            "live",
            worker,
            false
        );
    }
    add!(
        "sync.mode",
        Enum(&["auto", "linear", "negentropy"]),
        "auto",
        "live",
        "sync",
        false
    );
    add!(
        "sync.provenance",
        Enum(&["local_first_witness", "off"]),
        "local_first_witness",
        "generation",
        "sync",
        false
    );
    add!(
        "sync.ephemeral",
        Enum(&["retain", "drop"]),
        "retain",
        "generation",
        "sync",
        false
    );
    add!(
        "sync.history_since",
        UInt(0, u64::MAX),
        0,
        "generation",
        "sync",
        false
    );
    add!("sync.live_enabled", Bool, true, "live", "sync", false);
    add!(
        "monitor.active_write.enabled",
        Bool,
        false,
        "live",
        "monitor",
        false
    );
    add!(
        "monitor.active_write.signer",
        Secret,
        Value::Null,
        "identity",
        "monitor",
        true
    );
    add!(
        "monitor.active_write.endpoints",
        Endpoints,
        Vec::<String>::new(),
        "live",
        "monitor",
        false
    );
    for (feature, owner) in [
        ("api", "read"),
        ("dashboard", "read"),
        ("administration", "control"),
    ] {
        add!(
            format!("{feature}.enabled"),
            Bool,
            feature != "api",
            if feature == "api" { "live" } else { "restart" },
            owner,
            false
        );
    }
    add!(
        "api.listen.host",
        Text,
        "127.0.0.1",
        "restart",
        "read",
        true
    );
    add!(
        "api.listen.port",
        UInt(1, 65535),
        8080,
        "restart",
        "read",
        true
    );
    add!(
        "administration.listen.host",
        Text,
        "127.0.0.1",
        "restart",
        "control",
        true
    );
    add!(
        "administration.listen.port",
        UInt(1, 65535),
        8081,
        "restart",
        "control",
        true
    );
    add!("api.public", Bool, false, "restart", "read", true);
    for feature in ["archive_relay", "dvm", "nip85", "nip66", "mcp", "contextvm"] {
        add!(
            format!("{feature}.enabled"),
            Bool,
            false,
            "restart",
            "read",
            false
        );
    }
    add!("search.enabled", Bool, true, "generation", "read", false);
    add!(
        "search.backend",
        Enum(&["clickhouse"]),
        "clickhouse",
        "generation",
        "read",
        false
    );
    for route in ["tor", "i2p", "lokinet"] {
        add!(
            format!("network.{route}.enabled"),
            Bool,
            false,
            "live",
            "acquisition",
            true
        );
        add!(
            format!("network.{route}.proxy"),
            Proxy,
            Value::Null,
            "connection",
            "acquisition",
            true
        );
    }
    add!(
        "network.tls.extra_roots",
        Paths,
        Vec::<String>::new(),
        "connection",
        "acquisition",
        true
    );
    const MONITOR_CHECKS: &[&str] = &[
        "open", "read", "dns", "tls", "http", "nip11", "nip42", "nip45", "nip50", "nip77", "geo",
    ];
    add!(
        "monitor.checks",
        ChoiceSet(MONITOR_CHECKS),
        MONITOR_CHECKS,
        "live",
        "monitor",
        false
    );
    add!(
        "monitor.store_checks",
        ChoiceSet(MONITOR_CHECKS),
        Value::Null,
        "live",
        "monitor",
        false
    );
    keys.last_mut()
        .expect("store-check registry entry")
        .default_from = Some("monitor.checks");
    // Common read budgets are shared across adapters, never a separate budget
    // for every HTTP, relay or gateway mount.
    for (name, default) in [
        ("default_page_size", 100),
        ("max_page_size", 1000),
        ("max_filters", 8),
        ("max_values_per_filter_field", 100),
        ("execution.max_concurrent", 16),
        ("admission.queue_max", 64),
        ("public.rate", 5),
        ("public.burst", 10),
    ] {
        add!(
            format!("read.{name}"),
            UInt(1, u32::MAX as u64),
            default,
            "live",
            "read",
            false
        );
    }
    for (name, default) in [
        ("request.max_bytes", "64KiB"),
        ("execution.max_scan_bytes", "256MiB"),
        ("execution.max_memory_bytes", "64MiB"),
        ("response.max_bytes", "16MiB"),
    ] {
        add!(
            format!("read.{name}"),
            Bytes,
            default,
            "live",
            "read",
            false
        );
    }
    for (name, default) in [
        ("execution.timeout", "15s"),
        ("cursor.ttl", "15m"),
        ("policy.cache_max_age", "60s"),
        ("authorization.lease_ttl", "5s"),
    ] {
        add!(
            format!("read.{name}"),
            Duration,
            default,
            "live",
            "read",
            false
        );
    }
    add!(
        "read.cursor.clock_skew",
        DurationZero,
        "30s",
        "live",
        "read",
        false
    );
    add!(
        "read.count.mode",
        Enum(&["auto", "exact"]),
        "auto",
        "live",
        "read",
        false
    );
    add!(
        "api.request_timeout",
        Duration,
        "20s",
        "live",
        "read",
        false
    );
    let timeout = keys.last_mut().expect("API timeout registry entry");
    timeout.default_from = Some("read.execution.timeout");
    timeout.default_add_us = 5_000_000;
    // Acquisition settings have concrete units even before their workers exist.
    for (key, default, owner, reload) in [
        ("seed.max_line_bytes", "8KiB", "control", "job"),
        (
            "discoverer.api.max_response_bytes",
            "5MiB",
            "discoverer",
            "live",
        ),
        ("validator.nip11_max_bytes", "1MiB", "validator", "live"),
        ("monitor.nip11_max_bytes", "1MiB", "monitor", "live"),
        (
            "monitor.geo.max_download_bytes",
            "100MiB",
            "monitor",
            "live",
        ),
        ("sync.negentropy.max_set_bytes", "64MiB", "sync", "live"),
        (
            "sync.negentropy.max_frame_bytes",
            "64KiB",
            "sync",
            "connection",
        ),
        ("import.max_record_bytes", "16MiB", "import", "job"),
        ("import.max_event_bytes", "256KiB", "import", "job"),
        ("import.memory_bytes", "256MiB", "import", "job"),
        ("import.scratch_bytes", "20GiB", "import", "job"),
        ("import.reject_artifact_bytes", "64MiB", "import", "job"),
    ] {
        add!(key, Bytes, default, reload, owner, false);
    }
    for (key, default, owner, reload) in [
        ("discoverer.events.slice_time", "30s", "discoverer", "live"),
        ("discoverer.api.interval", "24h", "discoverer", "live"),
        ("discoverer.api.failure_retry", "15m", "discoverer", "live"),
        (
            "discoverer.api.request_timeout",
            "15s",
            "discoverer",
            "connection",
        ),
        ("discoverer.api.request_delay", "1s", "discoverer", "live"),
        (
            "validator.handshake_timeout",
            "10s",
            "validator",
            "connection",
        ),
        ("validator.nip11_timeout", "10s", "validator", "connection"),
        ("validator.retry_initial", "1h", "validator", "live"),
        ("validator.retry_max", "7d", "validator", "live"),
        ("validator.revival_min_interval", "1h", "validator", "live"),
        ("monitor.liveness_interval", "5m", "monitor", "live"),
        ("monitor.catalog_interval", "6h", "monitor", "live"),
        ("monitor.check_timeout", "15s", "monitor", "connection"),
        ("monitor.retry_initial", "1s", "monitor", "live"),
        ("monitor.failure_max_interval", "7d", "monitor", "live"),
        ("monitor.geo.max_age", "30d", "monitor", "live"),
        ("monitor.geo.refresh_interval", "24h", "monitor", "live"),
        ("monitor.active_write.interval", "24h", "monitor", "live"),
        ("monitor.active_write.pow_timeout", "1s", "monitor", "live"),
        ("sync.work_quantum", "2m", "sync", "live"),
        ("sync.live_rotation", "10m", "sync", "live"),
        ("sync.connection_lifetime", "24h", "sync", "connection"),
        ("sync.request_timeout", "30s", "sync", "connection"),
        ("sync.idle_timeout", "5m", "sync", "connection"),
        ("sync.window_width", "24h", "sync", "live"),
        ("sync.capability_probe_timeout", "10s", "sync", "connection"),
        ("sync.capability_reprobe", "7d", "sync", "live"),
        ("sync.safety_lag", "48h", "sync", "live"),
        ("sync.deep_reverify_interval", "7d", "sync", "live"),
        ("import.checkpoint_interval", "5s", "import", "job"),
    ] {
        add!(key, Duration, default, reload, owner, false);
    }
    for (key, default, owner) in [
        ("seed.batch_size", 500, "control"),
        ("discoverer.events.page_size", 500, "discoverer"),
        ("discoverer.events.batch_size", 500, "discoverer"),
        ("discoverer.events.workers", 4, "discoverer"),
        ("discoverer.events.quantum", 1000, "discoverer"),
        ("discoverer.evidence_recent_limit", 100, "discoverer"),
        ("validator.claim_batch", 100, "validator"),
        ("validator.concurrency", 20, "validator"),
        ("validator.retry_factor", 2, "validator"),
        ("validator.dormant_after", 10, "validator"),
        ("monitor.concurrency", 32, "monitor"),
        ("monitor.claim_batch", 100, "monitor"),
        ("monitor.per_host_concurrency", 1, "monitor"),
        ("monitor.host_budget", 32, "monitor"),
        ("monitor.retries", 1, "monitor"),
        ("monitor.failure_backoff_factor", 2, "monitor"),
        ("sync.active_endpoints", 64, "sync"),
        ("sync.subscriptions_per_endpoint", 3, "sync"),
        ("sync.linear_limit", 500, "sync"),
        ("sync.partition_budget", 128, "sync"),
        ("sync.negentropy.max_rounds", 64, "sync"),
        ("sync.negentropy.max_messages", 1000, "sync"),
        ("sync.negentropy.need_batch", 200, "sync"),
        ("sync.negentropy.fetch_attempts", 3, "sync"),
        ("network.address_connections", 8, "acquisition"),
        ("network.address_start_rate", 4, "acquisition"),
        ("import.batch_records", 1000, "import"),
        ("import.workers", 4, "import"),
    ] {
        add!(
            key,
            UInt(1, u32::MAX as u64),
            default,
            if owner == "import" || key.starts_with("seed.") {
                "job"
            } else {
                "live"
            },
            owner,
            false
        );
    }
    add!(
        "validator.retry_jitter",
        Ratio,
        0.2,
        "live",
        "validator",
        false
    );
    add!(
        "monitor.schedule_jitter",
        Ratio,
        0.1,
        "live",
        "monitor",
        false
    );
    add!(
        "monitor.geo.geohash_precision",
        UInt(1, 12),
        5,
        "live",
        "monitor",
        false
    );
    add!(
        "monitor.active_write.max_pow",
        UInt(0, 256),
        20,
        "live",
        "monitor",
        false
    );
    add!("monitor.geo.refresh", Bool, false, "live", "monitor", false);
    add!(
        "monitor.availability.enabled",
        Bool,
        true,
        "live",
        "monitor",
        false
    );
    add!("issuer.enabled", Bool, true, "live", "monitor", false);
    add!("seed.enroll", Bool, false, "job", "control", false);
    add!(
        "seed.assumed_scheme",
        Enum(&["ws", "wss"]),
        "wss",
        "job",
        "control",
        false
    );
    add!("seed.file", Path, Value::Null, "job", "control", false);
    add!(
        "monitor.geo.asn_file",
        Path,
        Value::Null,
        "connection",
        "monitor",
        false
    );
    add!(
        "monitor.geo.city_file",
        Path,
        Value::Null,
        "connection",
        "monitor",
        false
    );
    add!(
        "monitor.raw_retention",
        Enum(&["forever"]),
        "forever",
        "live",
        "monitor",
        false
    );
    add!(
        "monitor.aggregate_retention",
        Enum(&["none"]),
        "none",
        "live",
        "monitor",
        false
    );
    add!(
        "sync.relay_byte_budget",
        Bytes,
        Value::Null,
        "live",
        "sync",
        false
    );
    add!(
        "import.format",
        Enum(&["auto", "jsonl", "native", "pensieve"]),
        "auto",
        "job",
        "import",
        false
    );
    add!(
        "import.claims",
        Enum(&["enabled-families", "require-all", "none"]),
        "enabled-families",
        "job",
        "import",
        false
    );
    add!(
        "import.reject_policy",
        Enum(&["continue_records"]),
        "continue_records",
        "job",
        "import",
        false
    );
    add!(
        "archive.publisher.enabled",
        Bool,
        true,
        "live",
        "archive",
        false
    );
    for key in &mut keys {
        if matches!(
            key.name.as_str(),
            "runtime.disk_resume_margin_bytes" | "runtime.pressure_stable_duration"
        ) {
            key.reload = "live";
            key.root = false;
        }
        if key.name == "runtime.authorization_ttl" {
            key.reload = "live-revocation-barrier";
        }
        if key.name == "network.max_inflate_ratio" {
            key.reload = "connection";
        }
    }
    keys
}
