use super::registry::{Key, Kind, keys};
use bigbrotr::runtime::{EnvelopeError, ProcessEnvelope, ResourceLimits, WaiterLimits};
use bigbrotr::runtime::{ownership::OwnershipTiming, policy_authority::PolicyLeaseTtl};
use bigbrotr_protocol::endpoint::Endpoint;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    path::{Component, Path},
    time::Duration,
};

pub const MAX_CONFIG_BYTES: usize = 10 * 1024 * 1024;
pub const ROLES: &[&str] = &[
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
];

#[derive(Clone)]
pub struct Resolved {
    pub keys: Vec<Key>,
    pub values: BTreeMap<String, Value>,
    pub sources: BTreeMap<String, &'static str>,
    pub local_roles: BTreeSet<String>,
    pub dependencies: Vec<Value>,
    pub document_sha256: String,
}

pub fn parse(input: &str) -> Result<Resolved, String> {
    use sha2::{Digest, Sha256};
    if input.len() > MAX_CONFIG_BYTES {
        return Err("configuration exceeds 10MiB input limit".into());
    }
    // The maintained parser rejects duplicate keys and bounds recursion. Never display
    // its source excerpt: even invalid documents may contain pasted credentials.
    let table = toml::de::DeTable::parse(input).map_err(|_: toml::de::Error| {
        "invalid TOML syntax or duplicate key (input values withheld)".to_owned()
    })?;
    let keys = keys();
    let mut explicit = BTreeMap::new();
    flatten(table.get_ref(), "", &keys, &mut explicit)?;
    let recipe = explicit
        .get("config.recipe")
        .and_then(Value::as_str)
        .ok_or("config.recipe must explicitly select an initialization recipe")?;
    let mut values: BTreeMap<String, Value> = keys
        .iter()
        .map(|k| (k.name.clone(), k.default.clone()))
        .collect();
    let mut sources = values
        .keys()
        .map(|k| (k.clone(), "schema-default"))
        .collect::<BTreeMap<_, _>>();
    let mut recipe_values = super::recipes::overrides(recipe)?;
    for prefix in ["postgres.control", "postgres.catalog", "clickhouse"] {
        let boundary = format!("{prefix}.tls.local_boundary");
        // TOML cannot express null. Selecting TLS without a plaintext boundary
        // must preserve the schema's absence, rather than inherit an inapplicable
        // recipe exception. An explicit incompatible boundary is still rejected.
        if explicit
            .get(&format!("{prefix}.tls.mode"))
            .and_then(Value::as_str)
            == Some("verify-full")
            && !explicit.contains_key(&boundary)
        {
            recipe_values.remove(&boundary);
        }
    }
    for (key, value) in recipe_values {
        values.insert(key.clone(), value);
        sources.insert(key, "recipe");
    }
    // A recipe is a seed, never an implicit process assignment in an omitted root.
    for required in [
        "topology.instance_id",
        "topology.dataset_id",
        "topology.local_roles",
    ] {
        if !explicit.contains_key(required) {
            return Err(format!(
                "{required} must be explicit in the deployment root"
            ));
        }
    }
    for (key, value) in explicit {
        values.insert(key.clone(), value);
        sources.insert(key, "file");
    }
    for key in &keys {
        let value = &values[&key.name];
        if value.is_null() {
            continue;
        }
        let normalized =
            normalize(key, value).map_err(|reason| format!("{}: {reason}", key.name))?;
        values.insert(key.name.clone(), normalized);
    }
    // Resolve dependent defaults from normalized declared values. Current
    // dependencies point only to literal fields, not to other derived defaults.
    for key in &keys {
        if let Some(source) = key.default_from
            && sources[&key.name] == "schema-default"
        {
            let value = if key.default_add_us == 0 {
                values[source].clone()
            } else {
                let duration = values[source].as_u64().expect("duration default source");
                json!(
                    duration
                        .checked_add(key.default_add_us)
                        .ok_or_else(|| format!("{}: derived duration exceeds u64", key.name))?
                )
            };
            values.insert(key.name.clone(), value);
            sources.insert(key.name.clone(), "derived-default");
        }
    }
    let local_roles = values["topology.local_roles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    if local_roles.is_empty() || local_roles.iter().any(|r| !ROLES.contains(&r.as_str())) {
        return Err("topology.local_roles requires a nonempty set of supported roles".into());
    }
    let document_sha256 = Sha256::digest(input.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut result = Resolved {
        keys,
        values,
        sources,
        local_roles,
        dependencies: vec![],
        document_sha256,
    };
    result.validate()?;
    Ok(result)
}

fn flatten(
    table: &toml::de::DeTable<'_>,
    prefix: &str,
    keys: &[Key],
    out: &mut BTreeMap<String, Value>,
) -> Result<(), String> {
    for (name, spanned) in table {
        let name: &str = name.get_ref().as_ref();
        let value = spanned.get_ref();
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err("unknown configuration key (key text withheld)".into());
        }
        let path = if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}.{name}")
        };
        if let Some(table) = value.as_table() {
            let child_prefix = format!("{path}.");
            if !keys.iter().any(|k| k.name.starts_with(&child_prefix)) {
                return Err("unknown configuration table (key text withheld)".into());
            }
            flatten(table, &path, keys, out)?;
        } else {
            if !keys.iter().any(|k| k.name == path) {
                return Err("unknown configuration key (key text withheld)".into());
            }
            if matches!(
                keys.iter().find(|k| k.name == path).unwrap().kind,
                Kind::Endpoints
            ) && spanned.span().len() > 8 * 1024 * 1024
            {
                return Err(format!("{path}: raw encoded input exceeds 8MiB"));
            }
            let json = decoded_value(value)?;
            // TOML allows nonfinite floats; JSON null must not make them look absent.
            if json.is_null() {
                return Err(format!("{path}: nonfinite or unsupported value"));
            }
            out.insert(path, json);
        }
    }
    Ok(())
}

fn decoded_value(value: &toml::de::DeValue<'_>) -> Result<Value, String> {
    use toml::de::DeValue;
    match value {
        DeValue::String(text) => Ok(json!(text)),
        DeValue::Boolean(value) => Ok(json!(value)),
        DeValue::Integer(value) => i64::from_str_radix(value.as_str(), value.radix())
            .map(Value::from)
            .or_else(|_| u64::from_str_radix(value.as_str(), value.radix()).map(Value::from))
            .map_err(|_| "configuration integer exceeds portable range".into()),
        DeValue::Float(value) => value
            .as_str()
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| "configuration float must be finite".into()),
        DeValue::Array(values) => values
            .iter()
            .map(|v| decoded_value(v.get_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        DeValue::Table(table) => table
            .iter()
            .map(|(key, value)| {
                decoded_value(value.get_ref()).map(|value| (key.get_ref().to_string(), value))
            })
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),
        DeValue::Datetime(_) => Err("unsupported datetime configuration value".into()),
    }
}

fn normalize(key: &Key, value: &Value) -> Result<Value, &'static str> {
    use Kind::*;
    match key.kind {
        Bool => value.as_bool().map(Value::from).ok_or("expected boolean"),
        UInt(min, max) => {
            let n = value
                .as_u64()
                .or_else(|| value.as_str().and_then(decimal))
                .ok_or("expected canonical unsigned integer")?;
            if n < min || n > max {
                return Err("integer outside allowed range");
            }
            Ok(json!(n))
        }
        Bytes => units(
            value,
            &[
                ("TiB", 1 << 40),
                ("GiB", 1 << 30),
                ("MiB", 1 << 20),
                ("KiB", 1 << 10),
                ("B", 1),
            ],
            false,
        )
        .map(Value::from),
        Duration | DurationZero => {
            duration(value, matches!(key.kind, DurationZero)).map(Value::from)
        }
        RevisitBands => revisit_bands(value),
        Ratio => value
            .as_f64()
            .filter(|n| n.is_finite() && (0.0..=1.0).contains(n))
            // Signed zero has one effective fraction and one policy identity.
            .map(|n| Value::from(if n == 0.0 { 0.0 } else { n }))
            .ok_or("expected finite fraction in [0,1]"),
        Enum(allowed) => value
            .as_str()
            .filter(|s| allowed.contains(s))
            .map(Value::from)
            .ok_or("unsupported value"),
        Identity => {
            let text = value
                .as_str()
                .ok_or("expected nonzero lowercase 128-bit identity")?;
            text.parse::<bigbrotr_model::DatasetId>()
                .map_err(|_| "expected nonzero lowercase 128-bit identity")?;
            Ok(value.clone())
        }
        Proxy => {
            let raw = value.as_str().ok_or("expected proxy URL")?;
            if raw.len() > 8192
                || raw.chars().any(|c| c.is_control() || c.is_whitespace())
                || raw.contains(['\\', '@', '?', '#'])
            {
                return Err("proxy credentials/query/fragment/controls are forbidden");
            }
            let url = url::Url::parse(raw).map_err(|_| "invalid proxy URL")?;
            if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h")
                || url.host_str().is_none()
                || !matches!(url.path(), "" | "/")
                || url.port() == Some(0)
                || (!matches!(url.scheme(), "http" | "https") && url.port().is_none())
            {
                return Err(
                    "proxy URL needs a supported scheme, host and usable port without path",
                );
            }
            Ok(json!(url.as_str()))
        }
        Text | Path | Secret => {
            let text = value.as_str().ok_or("expected string")?;
            if text.is_empty() || text.len() > 8192 || text.chars().any(char::is_control) {
                return Err("expected nonempty bounded text without controls");
            }
            if matches!(key.kind, Path) && !absolute_path(text) {
                return Err("expected absolute path without dot components");
            }
            if matches!(key.kind, Secret) && bigbrotr::secrets::SecretFile::parse(text).is_err() {
                return Err(
                    "expected protected file:/absolute/filename reference, never plaintext",
                );
            }
            Ok(value.clone())
        }
        Strings | Paths | ChoiceSet(_) | Endpoints => {
            let values = value.as_array().ok_or("expected array")?;
            if values.len() > 100000 {
                return Err("array exceeds 100000 entries");
            }
            let mut result = Vec::with_capacity(values.len());
            let mut seen = BTreeSet::new();
            let mut bytes = 2_usize;
            for value in values {
                let text = value.as_str().ok_or("expected string array entries")?;
                if text.is_empty() || text.len() > 8192 || text.chars().any(char::is_control) {
                    return Err("invalid bounded string array entry");
                }
                if matches!(key.kind, Paths) && !absolute_path(text) {
                    return Err("expected absolute paths without dot components");
                }
                if let ChoiceSet(allowed) = key.kind
                    && !allowed.contains(&text)
                {
                    return Err("unsupported set member");
                }
                let normalized = if matches!(key.kind, Endpoints) {
                    Endpoint::parse(text)
                        .map_err(|_| "invalid ws/wss endpoint (value withheld)")?
                        .as_str()
                        .to_owned()
                } else {
                    text.to_owned()
                };
                bytes = bytes
                    .checked_add(serde_json::to_vec(&normalized).unwrap().len() + 1)
                    .ok_or("array byte bound overflow")?;
                if bytes > 8 * 1024 * 1024 {
                    return Err("array exceeds 8MiB encoded bytes");
                }
                let comparison = if matches!(key.kind, Paths) {
                    lexical_path(text)
                } else {
                    normalized.clone()
                };
                if seen.insert(comparison) {
                    result.push(Value::from(normalized));
                } else if !matches!(key.kind, Endpoints) {
                    return Err("duplicate set entry");
                }
            }
            Ok(Value::Array(result))
        }
    }
}

// Stored values have already resolved units and defaults. Validate them with
// the same registry rules, but require the closed normalized representation.
pub(super) fn stored_value(key: &Key, value: &Value) -> Result<Value, &'static str> {
    use Kind::*;
    if value.is_null() {
        return if key.default.is_null() && key.default_from.is_none() {
            Ok(Value::Null)
        } else {
            Err("required stored value is absent")
        };
    }
    let wire = match key.kind {
        Bytes => json!(format!(
            "{}B",
            value.as_u64().ok_or("stored bytes must be u64")?
        )),
        Duration | DurationZero => json!(format!(
            "{}us",
            value.as_u64().ok_or("stored duration must be u64")?
        )),
        RevisitBands => {
            let mut wire = value.clone();
            for band in wire
                .as_array_mut()
                .ok_or("stored revisit bands must be an array")?
            {
                let fields = band
                    .as_object_mut()
                    .ok_or("stored band must be an object")?;
                for key in ["start_age", "end_age", "cadence"] {
                    let n = fields
                        .get(key)
                        .and_then(Value::as_u64)
                        .ok_or("stored band duration must be u64")?;
                    fields.insert(key.into(), json!(format!("{n}us")));
                }
            }
            wire
        }
        Secret => return Err("credentials cannot be stored policy"),
        _ => value.clone(),
    };
    let normalized = normalize(key, &wire)?;
    if !matches!(key.kind, Ratio) && &normalized != value {
        return Err("stored value is not normalized");
    }
    Ok(normalized)
}

fn duration(value: &Value, allow_zero: bool) -> Result<u64, &'static str> {
    units(
        value,
        &[
            ("us", 1),
            ("ms", 1000),
            ("s", 1000000),
            ("m", 60000000),
            ("h", 3600000000),
            ("d", 86400000000),
        ],
        allow_zero,
    )
}

fn revisit_bands(value: &Value) -> Result<Value, &'static str> {
    let bands = value.as_array().ok_or("expected array of revisit bands")?;
    if bands.is_empty() || bands.len() > 1024 {
        return Err("expected 1..1024 revisit bands");
    }
    let mut previous_end = 0;
    let mut result = Vec::with_capacity(bands.len());
    for band in bands {
        let fields = band
            .as_object()
            .ok_or("expected closed revisit band record")?;
        if fields.len() != 3
            || !["start_age", "end_age", "cadence"]
                .iter()
                .all(|name| fields.contains_key(*name))
        {
            return Err("revisit band requires only start_age, end_age and cadence");
        }
        let start = duration(&fields["start_age"], true)?;
        let end = duration(&fields["end_age"], false)?;
        let cadence = duration(&fields["cadence"], false)?;
        if start >= end || start < previous_end {
            return Err("revisit bands must be nonempty, ordered and disjoint");
        }
        previous_end = end;
        result.push(json!({"start_age":start, "end_age":end, "cadence":cadence}));
    }
    Ok(Value::Array(result))
}

// Diagnostic numbers, including structured values, must survive JavaScript's
// narrower exact-integer range. This never changes the resolved typed values.
fn diagnostic_integers(value: &mut Value) {
    match value {
        Value::Number(number) if number.is_u64() => {
            *value = json!(number.as_u64().unwrap().to_string())
        }
        Value::Array(items) => items.iter_mut().for_each(diagnostic_integers),
        Value::Object(fields) => fields.values_mut().for_each(diagnostic_integers),
        _ => (),
    }
}

fn decimal(text: &str) -> Option<u64> {
    if text.is_empty()
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    text.parse().ok()
}

fn units(value: &Value, suffixes: &[(&str, u64)], allow_zero: bool) -> Result<u64, &'static str> {
    let text = value.as_str().ok_or("explicit unit required")?;
    for (suffix, factor) in suffixes {
        if let Some(number) = text.strip_suffix(suffix) {
            return decimal(number)
                .and_then(|n| n.checked_mul(*factor))
                .filter(|n| allow_zero || *n > 0)
                .ok_or("unit value is outside the allowed u64 range");
        }
    }
    Err("unsupported unit; bytes use IEC units, durations use us/ms/s/m/h/d")
}

fn lexical_path(text: &str) -> String {
    // Input is UTF-8 and already excludes dot components. This removes repeated
    // separators and trailing slashes only; it never resolves filesystem links.
    Path::new(text)
        .components()
        .collect::<std::path::PathBuf>()
        .into_os_string()
        .into_string()
        .expect("UTF-8 components")
}

pub fn absolute_path(text: &str) -> bool {
    Path::new(text).is_absolute()
        && !text.split('/').any(|part| part == "." || part == "..")
        && !Path::new(text)
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
}

impl Resolved {
    /// Expected installation binding from the deployment root, never learned
    /// from the catalog being inspected. No filesystem or credential I/O here.
    pub fn catalog_identity(
        &self,
    ) -> Result<bigbrotr::storage::postgres::catalog_guard::CatalogIdentity, String> {
        use bigbrotr::storage::postgres::catalog_guard::CatalogIdentity;
        if !self.local("archive") {
            return Err("catalog inspection requires a declared local Archive role".into());
        }
        let dataset = self
            .present("topology.dataset_id")?
            .parse()
            .map_err(|_| "invalid dataset identity")?;
        let catalog = self
            .present("archive.catalog_id")?
            .parse()
            .map_err(|_| "invalid archive.catalog_id")?;
        let root = self.present("storage.archive_root")?;
        // Native catalog metadata uses a terminal slash. Preserve every other
        // byte: aliases, repeated separators and glob names require explicit
        // correction rather than silently changing an installation binding.
        let root = if root.ends_with('/') {
            root.to_owned()
        } else {
            format!("{root}/")
        };
        CatalogIdentity::local(dataset, catalog, &root)
            .map_err(|_| "storage.archive_root requires a bounded canonical local path without glob characters or repeated separators".into())
    }

    pub fn value(&self, key: &str) -> &Value {
        &self.values[key]
    }
    pub fn text(&self, key: &str) -> Option<&str> {
        self.value(key).as_str()
    }
    pub fn flag(&self, key: &str) -> bool {
        self.value(key).as_bool().expect("registry boolean")
    }
    pub fn number(&self, key: &str) -> u64 {
        self.value(key).as_u64().expect("registry unsigned number")
    }
    pub fn local(&self, role: &str) -> bool {
        self.local_roles.contains(role)
    }
    pub fn selected_storage_roots(&self) -> impl Iterator<Item = &'static str> {
        [
            ("storage.ingest_root", self.local("ingest")),
            ("storage.index_root", self.local("index")),
            ("storage.archive_root", self.local("archive")),
            ("storage.archive_state_root", self.local("archive")),
            (
                "storage.scratch_root",
                self.flag("runtime.processes.native.enabled"),
            ),
        ]
        .into_iter()
        .filter_map(|(key, selected)| selected.then_some(key))
    }
    pub fn remote(&self, role: &str) -> bool {
        self.values
            .get(&format!("topology.remote.{role}.endpoint"))
            .is_some_and(|v| !v.is_null())
    }
    pub fn owner(&self, role: &str) -> bool {
        self.local(role) || self.remote(role)
    }
    fn require(&self, role: &str, by: &str) -> Result<(), String> {
        if self.owner(role) {
            Ok(())
        } else {
            Err(format!(
                "{by} requires declared local or authenticated remote {role} owner"
            ))
        }
    }
    fn present(&self, key: &str) -> Result<&str, String> {
        self.text(key)
            .ok_or_else(|| format!("{key} is required by a selected role"))
    }
    pub(super) fn validate(&mut self) -> Result<(), String> {
        // Disabled dependencies still have closed syntax. Only actual consumers
        // require complete credentials and contribute live dependency checks.
        for pg in ["control", "catalog"] {
            let prefix = format!("postgres.{pg}");
            if let Some(raw) = self.text(&format!("{prefix}.endpoint")) {
                let url = pg_endpoint(raw)?;
                self.tls(&prefix, &url, false)?;
            }
        }
        if let Some(raw) = self.text("clickhouse.endpoint") {
            let url = endpoint(raw, false)?;
            self.tls("clickhouse", &url, false)?;
            if self.text("clickhouse.tls.mode") == Some("verify-full") && url.scheme() != "https" {
                return Err("clickhouse verify-full requires HTTPS".into());
            }
        }
        for (required, process) in [
            (true, "application"),
            (
                (self.local("control") && self.text("postgres.control.placement") == Some("local"))
                    || (self.local("archive")
                        && self.text("postgres.catalog.placement") == Some("local")),
                "postgres",
            ),
            (
                (self.local("index") || self.local("read"))
                    && self.text("clickhouse.placement") == Some("local"),
                "clickhouse",
            ),
            (
                self.local("archive") || self.local("deriver") || self.local("ranker"),
                "native",
            ),
        ] {
            if required && !self.flag(&format!("runtime.processes.{process}.enabled")) {
                return Err(format!(
                    "selected local roles require a declared {process} process grant"
                ));
            }
        }
        for role in ["control", "ingest", "archive", "index", "read"] {
            let prefix = format!("topology.remote.{role}.");
            let selected = self
                .keys
                .iter()
                .filter(|k| k.name.starts_with(&prefix))
                .any(|k| !self.value(&k.name).is_null());
            if selected {
                if self.local(role) {
                    return Err(format!(
                        "{role} cannot have both local and remote ownership"
                    ));
                }
                for key in self.keys.iter().filter(|k| k.name.starts_with(&prefix)) {
                    if self.value(&key.name).is_null() {
                        return Err(format!(
                            "{} is required for the declared remote owner",
                            key.name
                        ));
                    }
                }
                if self.value(&format!("{prefix}dataset_id")) != self.value("topology.dataset_id") {
                    return Err(format!("remote {role} dataset does not match root"));
                }
                endpoint(self.present(&format!("{prefix}endpoint"))?, true)?;
            }
        }
        for (worker, gate) in [
            ("sync", "sync.enabled"),
            ("monitor", "monitor.enabled"),
            ("validator", "validator.enabled"),
            ("discoverer", "discoverer.events.enabled"),
            ("deriver", "deriver.enabled"),
            ("ranker", "ranker.enabled"),
        ] {
            if self.local(worker) && self.flag(gate) {
                self.require("control", worker)?;
                if ["sync", "monitor", "validator", "discoverer"].contains(&worker) {
                    self.require("ingest", worker)?;
                    self.require("archive", worker)?;
                }
                if ["deriver", "ranker"].contains(&worker) {
                    self.require("index", worker)?;
                }
            }
        }
        if self.local("ingest") {
            self.require("control", "ingest")?;
            self.require("archive", "ingest")?;
            self.present("storage.ingest_root")?;
        }
        if self.flag("archive.reader.enabled") {
            if !self.local("archive") || !self.local("control") {
                return Err("archive reader requires local Control and Archive custody".into());
            }
            self.present("postgres.catalog.read_user")?;
            self.present("postgres.catalog.read_password_ref")?;
        }
        if self.local("archive") {
            self.require("control", "archive")?;
            if self.flag("archive.publisher.enabled") {
                self.require("ingest", "archive publisher")?;
            }
            self.present("storage.archive_root")?;
            self.present("storage.archive_state_root")?;
            self.catalog_identity()?;
            self.present("storage.scratch_root")?;
            self.pg("catalog")?;
            let reader = self.text("postgres.catalog.read_user");
            let password = self.text("postgres.catalog.read_password_ref");
            if reader.is_some() != password.is_some() {
                return Err("catalog reader credentials must be paired".into());
            }
            if let Some(reader) = reader
                && (reader.len() > 63 || Some(reader) == self.text("postgres.catalog.user"))
            {
                return Err(
                    "a separate catalog reader login of at most 63 bytes is required".into(),
                );
            }
        }
        if self.local("index")
            && self.flag("indexer.enabled")
            && !self.owner("ingest")
            && !self.owner("archive")
        {
            return Err(
                "enabled Indexer requires declared ingest/pending or archive replay source".into(),
            );
        }
        if self.local("index") {
            self.require("control", "index")?;
            self.present("storage.index_root")?;
            self.ch(true)?;
        }
        if self.local("control") {
            self.pg("control")?;
        }
        if self.local("read") {
            self.require("control", "read")?;
            self.ch(false)?;
        }
        for role in ["control", "ingest", "archive", "index", "read"] {
            if self.remote(role) {
                self.dependencies.push(json!({"name":format!("{role}-rpc"),"owner":"remote","runtime_identity":"not-checked"}));
            }
        }
        for worker in ["sync", "monitor"] {
            let urls = self
                .value(&format!("{worker}.endpoints.urls"))
                .as_array()
                .unwrap();
            let mode = self.text(&format!("{worker}.endpoints.mode")).unwrap();
            if (mode == "fixed" && urls.is_empty()) || (mode == "enrolled" && !urls.is_empty()) {
                return Err(format!(
                    "{worker}.endpoints: fixed requires a nonempty list; enrolled forbids an ineffective list"
                ));
            }
        }
        if self.local("sync")
            && self.flag("sync.enabled")
            && self.text("sync.mode") == Some("negentropy")
            && (self.text("topology.ordered_set_reader") != Some("index") || !self.owner("index"))
        {
            return Err("forced negentropy requires a declared eligible ordered-set reader".into());
        }
        if self.text("topology.ordered_set_reader") == Some("index") {
            self.require("index", "ordered-set reader")?;
        }
        for feature in ["archive_relay", "dvm", "nip85", "nip66", "mcp", "contextvm"] {
            if self.flag(&format!("{feature}.enabled")) {
                return Err(format!(
                    "{feature}: static adapter settings are not implemented in this schema revision"
                ));
            }
        }
        if self.flag("monitor.active_write.enabled") {
            return Err("monitor.active_write: signer purpose/endpoint policy adapter not implemented; refusing enablement".into());
        }
        if self.flag("monitor.geo.refresh") {
            return Err("monitor.geo.refresh: typed source/download adapter not implemented; refusing enablement".into());
        }
        if !self
            .value("discoverer.api.sources")
            .as_array()
            .unwrap()
            .is_empty()
        {
            return Err("discoverer.api.sources: structured source definitions are not implemented in this schema revision".into());
        }
        if self.flag("discoverer.api.enabled") {
            return Err("discoverer.api: typed directory source adapter not implemented; refusing enablement".into());
        }
        for route in ["tor", "i2p", "lokinet"] {
            if self.flag(&format!("network.{route}.enabled")) {
                return Err(format!(
                    "network.{route}: route adapter not implemented; refusing enablement"
                ));
            }
        }
        if self.flag("api.public") {
            return Err(
                "api.public: exposure/authentication adapter not implemented; refusing enablement"
                    .into(),
            );
        }
        for bind in ["api.listen.host", "administration.listen.host"] {
            let address: IpAddr = self
                .present(bind)?
                .parse()
                .map_err(|_| format!("{bind}: expected literal IP address"))?;
            if !address.is_loopback() {
                return Err(format!(
                    "{bind}: initial static schema permits only explicit loopback listeners"
                ));
            }
        }
        let checks = self.value("monitor.checks").as_array().unwrap();
        let stored = self.value("monitor.store_checks").as_array().unwrap();
        if stored.iter().any(|check| !checks.contains(check)) {
            return Err("monitor.store_checks must be a subset of monitor.checks".into());
        }
        if self.local("monitor") && self.flag("monitor.enabled") && checks.is_empty() {
            return Err("enabled monitor requires a nonempty computed check set".into());
        }
        if self.text("export.destination.id").is_some()
            != self.text("export.destination.root").is_some()
        {
            return Err("export destination requires both id and root".into());
        }
        if let Some(root) = self.text("export.destination.root") {
            bigbrotr::export::destination::validate_root(Path::new(root))
                .map_err(|_| "export destination requires a bounded canonical absolute root")?;
            for key in self.selected_storage_roots() {
                let other = Path::new(self.present(key)?);
                if Path::new(root).starts_with(other) || other.starts_with(root) {
                    return Err(
                        "export destination must be disjoint from selected writable roots".into(),
                    );
                }
            }
        }
        self.budgets()?;
        let mut paths = BTreeSet::new();
        for key in self.selected_storage_roots() {
            if !paths.insert(lexical_path(self.present(key)?)) {
                return Err("local writable storage roots must be distinct; filesystem alias checks require doctor/runtime".into());
            }
        }
        if self.local("archive") {
            let state = lexical_path(self.present("storage.archive_state_root")?);
            for key in self
                .selected_storage_roots()
                .filter(|key| *key != "storage.archive_state_root")
            {
                let other = lexical_path(self.present(key)?);
                if Path::new(&state).starts_with(&other) || Path::new(&other).starts_with(&state) {
                    return Err("storage.archive_state_root must be disjoint from other selected writable roots".into());
                }
            }
        }
        Ok(())
    }
    /// Filesystem checks for the actual local coordinator consumer. Offline
    /// validation never opens these paths. Absent other stores are still checked
    /// lexically above and must pass their own placement checks before opening.
    pub fn archive_state_path(&self) -> Result<std::path::PathBuf, String> {
        self.catalog_identity()?;
        let state = Path::new(self.present("storage.archive_state_root")?);
        let canonical = state
            .canonicalize()
            .map_err(|_| "archive state root is unavailable")?;
        if canonical != state {
            return Err("archive state root must be canonical".into());
        }
        for key in self
            .selected_storage_roots()
            .filter(|key| *key != "storage.archive_state_root")
        {
            let other = Path::new(self.present(key)?);
            match other.canonicalize() {
                Ok(other) => bigbrotr::storage::archive::LocalCoordinator::require_disjoint(&canonical, &other)
                    .map_err(|_| "archive state root aliases another selected writable root or its placement cannot be verified")?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(_) => return Err("selected storage root placement cannot be inspected".into()),
            }
        }
        Ok(canonical)
    }
    fn pg(&mut self, kind: &str) -> Result<(), String> {
        let prefix = format!("postgres.{kind}");
        let raw = self.present(&format!("{prefix}.endpoint"))?;
        let url = pg_endpoint(raw)?;
        for field in ["database", "user", "password_ref"] {
            self.present(&format!("{prefix}.{field}"))?;
        }
        self.tls(&prefix, &url, true)?;
        self.dependencies
            .push(json!({"name":prefix,"owner":"engine","runtime_identity":"not-checked"}));
        Ok(())
    }
    fn ch(&mut self, writer: bool) -> Result<(), String> {
        let raw = self.present("clickhouse.endpoint")?;
        let url = endpoint(raw, false)?;
        self.present("clickhouse.database")?;
        let credential = if writer { "write" } else { "read" };
        if self.present(&format!("clickhouse.{credential}_user"))? == "default" {
            return Err(
                "clickhouse: explicit least-privilege account required; default is not selected"
                    .into(),
            );
        }
        self.present(&format!("clickhouse.{credential}_password_ref"))?;
        self.tls("clickhouse", &url, true)?;
        if self.text("clickhouse.tls.mode") == Some("verify-full") && url.scheme() != "https" {
            return Err("clickhouse verify-full requires HTTPS".into());
        }
        if !self.dependencies.iter().any(|d| d["name"] == "clickhouse") {
            self.dependencies.push(
                json!({"name":"clickhouse","owner":"engine","runtime_identity":"not-checked"}),
            );
        }
        Ok(())
    }
    fn tls(&self, prefix: &str, url: &url::Url, selected: bool) -> Result<(), String> {
        let mode = self.text(&format!("{prefix}.tls.mode")).unwrap();
        let cert = self
            .value(&format!("{prefix}.tls.client_cert_ref"))
            .is_null();
        let key = self
            .value(&format!("{prefix}.tls.client_key_ref"))
            .is_null();
        if cert != key {
            return Err(format!(
                "{prefix}: TLS client certificate and key references must be paired"
            ));
        }
        if mode == "local-plaintext" {
            if self.text(&format!("{prefix}.placement")) == Some("remote") {
                return Err(format!(
                    "{prefix}: remote engine placement requires verify-full TLS"
                ));
            }
            if !cert
                || !self.value(&format!("{prefix}.tls.ca_file")).is_null()
                || !self.value(&format!("{prefix}.tls.server_name")).is_null()
            {
                return Err(format!(
                    "{prefix}: TLS credentials/settings are ineffective in local-plaintext mode"
                ));
            }
            let boundary = self.present(&format!("{prefix}.tls.local_boundary"))?;
            if boundary == "loopback"
                && !url.host_str().is_some_and(|h| {
                    h == "localhost"
                        || h.trim_matches(['[', ']'])
                            .parse::<IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())
                })
            {
                return Err(format!(
                    "{prefix}: plaintext loopback exception requires loopback endpoint"
                ));
            }
            if boundary == "private-bridge"
                && url
                    .host_str()
                    .is_some_and(|h| h.contains('.') || h.contains(':'))
            {
                return Err(format!(
                    "{prefix}: private-bridge exception requires an explicit local service DNS name"
                ));
            }
        } else if !self
            .value(&format!("{prefix}.tls.local_boundary"))
            .is_null()
        {
            return Err(format!(
                "{prefix}: local boundary is ineffective under verify-full"
            ));
        }
        if selected && mode == "verify-full" {
            // Presence only: certificate loading and peer identity verification
            // belong to the actual connection, never offline configuration.
            self.present(&format!("{prefix}.tls.ca_file"))?;
            self.present(&format!("{prefix}.tls.server_name"))?;
        }
        Ok(())
    }
    fn budgets(&self) -> Result<(), String> {
        let n = |key: &str| self.number(key);
        let add = |a: u64, b: u64| {
            a.checked_add(b)
                .ok_or_else(|| "resource sum overflows u64".to_owned())
        };
        let mut host = n("runtime.os_headroom_bytes");
        for process in ["application", "postgres", "clickhouse", "native"] {
            let prefix = format!("runtime.processes.{process}");
            // Share the runtime owner's checked envelope. This offline check
            // admits no items or waiters and creates no running budget owner.
            ProcessEnvelope::new(
                n(&format!("{prefix}.memory_max_bytes")),
                n(&format!("{prefix}.baseline_bytes")),
                ResourceLimits::new(
                    n(&format!("{prefix}.payload_bytes")),
                    0,
                    n(&format!("{prefix}.native_bytes")),
                    n(&format!("{prefix}.completion_reserve_bytes")),
                ),
                WaiterLimits::new(0, 0),
            ).map_err(|error| match error {
                EnvelopeError::MemoryArithmeticOverflow => "resource sum overflows u64".to_owned(),
                EnvelopeError::MemoryLimitExceeded { .. } => format!(
                    "{prefix}: baseline + payload + native + completion reserve exceeds memory maximum"
                ),
            })?;
            if self.flag(&format!("{prefix}.enabled")) {
                host = add(host, n(&format!("{prefix}.memory_max_bytes")))?;
            }
        }
        if host > n("runtime.host_memory_bytes") {
            return Err("OS headroom plus enabled process grants exceeds host memory".into());
        }
        OwnershipTiming::new(
            Duration::from_micros(n("runtime.lease_ttl")),
            Duration::from_micros(n("runtime.heartbeat_interval")),
            Duration::from_micros(n("runtime.renewal_timeout")),
            Duration::from_micros(n("runtime.renewal_safety_margin")),
            Duration::from_micros(n("runtime.graceful_shutdown_timeout")),
        )
        .map_err(|error| format!("unsafe ownership timing: {error}"))?;
        PolicyLeaseTtl::new(Duration::from_micros(n("runtime.authorization_ttl")))
            .map_err(|error| error.to_string())?;
        if n("read.default_page_size") > n("read.max_page_size") {
            return Err("read default page size exceeds maximum page size".into());
        }
        if n("read.authorization.lease_ttl") > n("runtime.authorization_ttl")
            || n("read.cursor.clock_skew") > 30_000_000
        {
            return Err(
                "read authorization or cursor clock-skew allowance exceeds shared ceiling".into(),
            );
        }
        if n("api.request_timeout") < add(n("read.execution.timeout"), 5_000_000)? {
            return Err(
                "API request timeout must retain 5s delivery allowance beyond read execution"
                    .into(),
            );
        }
        if self.local("read") {
            let read_unit = add(
                add(
                    n("read.request.max_bytes"),
                    n("read.execution.max_memory_bytes"),
                )?,
                n("read.response.max_bytes"),
            )?;
            let read_total = read_unit
                .checked_mul(n("read.execution.max_concurrent"))
                .ok_or("read concurrent allowance overflows u64")?;
            if read_total > n("runtime.processes.application.payload_bytes") {
                return Err("read concurrent request/query/response allowance exceeds application work grant".into());
            }
            if self.text("clickhouse.placement") == Some("local")
                && n("read.execution.max_memory_bytes")
                    .checked_mul(n("read.execution.max_concurrent"))
                    .ok_or("read query allowance overflows u64")?
                    > n("runtime.processes.clickhouse.payload_bytes")
            {
                return Err(
                    "read concurrent query allowance exceeds local ClickHouse work grant".into(),
                );
            }
        }
        let low = self.value("ingest.pending_low_fraction").as_f64().unwrap();
        let high = self.value("ingest.pending_high_fraction").as_f64().unwrap();
        if !(0.0 < low && low < high && high < 1.0) {
            return Err("pending fractions require 0 < low < high < 1".into());
        }
        if n("import.max_event_bytes") > n("ingest.max_event_bytes")
            || n("import.max_record_bytes") > n("import.memory_bytes")
        {
            return Err(
                "import maximum record/event exceeds its memory or shared event allowance".into(),
            );
        }
        if n("validator.retry_initial") > n("validator.retry_max") {
            return Err("validator retry initial exceeds maximum".into());
        }
        if n("sync.negentropy.max_frame_bytes") > n("network.max_message_bytes") {
            return Err("NEG frame exceeds the shared network message allowance".into());
        }
        let fact = n("pipeline.max_fact_bytes");
        for document in ["monitor.nip11_max_bytes", "validator.nip11_max_bytes"] {
            if add(n(document), 65536)? > fact {
                return Err(format!(
                    "{document}: retained document plus conservative metadata allowance exceeds maximum fact"
                ));
            }
        }
        if n("ingest.max_event_bytes") > n("network.max_message_bytes")
            || n("ingest.max_event_bytes") > n("network.max_inflated_bytes")
            || n("ingest.max_event_bytes") > fact
            || fact > n("ingest.group_commit_max_bytes")
            || fact > n("ingest.producer_queue_bytes")
        {
            return Err("maximum accepted unit does not fit fact/commit/producer bounds".into());
        }
        let framed = add(fact, 65536)?;
        if framed > n("ingest.pending_max_bytes") {
            return Err("pending allowance cannot retain a maximum accepted atomic unit".into());
        }
        if self.local("index") {
            if fact > n("indexer.batch_max_bytes")
                || n("indexer.batch_max_bytes") > n("runtime.processes.application.payload_bytes")
            {
                return Err(
                    "Indexer maximum atomic source batch does not fit its staging grant".into(),
                );
            }
            let query_total = n("indexer.query_memory")
                .checked_mul(n("indexer.insert_workers"))
                .ok_or("Indexer query grants overflow")?;
            if self.text("clickhouse.placement") == Some("local")
                && query_total > n("runtime.processes.clickhouse.payload_bytes")
            {
                return Err(
                    "Indexer concurrent query allowance exceeds local ClickHouse work grant".into(),
                );
            }
        }
        let peer = n("rpc.max_encode_bytes").min(n("rpc.max_decode_bytes"));
        if framed > n("rpc.batch_max_bytes") || add(n("rpc.batch_max_bytes"), 65536)? > peer {
            return Err(
                "RPC limits must fit atomic fact and conservative 64KiB framing allowance".into(),
            );
        }
        let mut prep = 0;
        for key in [
            "network.max_message_bytes",
            "network.max_inflated_bytes",
            "pipeline.parse_arena_bytes",
            "pipeline.max_fact_bytes",
        ] {
            prep = add(prep, n(key))?;
        }
        if prep > n("pipeline.prepare_slot_bytes")
            || n("pipeline.prepare_slot_bytes") > n("runtime.processes.application.payload_bytes")
            || framed > n("runtime.processes.application.completion_reserve_bytes")
        {
            return Err(
                "pipeline maximum preparation or completion unit exceeds reserved process capacity"
                    .into(),
            );
        }
        if framed > n("deriver.work.max_ingestion_bytes") || framed > n("deriver.memory.max_bytes")
        {
            return Err(
                "Deriver maximum source unit does not fit its chunk or resident allowance".into(),
            );
        }
        for role in ["deriver", "ranker"] {
            if self.local(role)
                && n(&format!("{role}.memory.max_bytes"))
                    > n("runtime.processes.native.native_bytes")
            {
                return Err(format!(
                    "{role}: native job memory exceeds declared native work grant"
                ));
            }
        }
        if self.local("archive") {
            let archive_total = n("archive.worker_memory_bytes")
                .checked_mul(n("archive.prepare_workers"))
                .ok_or("archive worker grant multiplication overflows")?;
            if archive_total > n("runtime.processes.native.native_bytes")
                || framed > n("archive.worker_memory_bytes")
                || !self.flag("runtime.processes.native.enabled")
            {
                return Err(
                    "archive complete atomic unit/workers exceed native process grant".into(),
                );
            }
        }
        Ok(())
    }
    fn applicable(&self, key: &Key) -> bool {
        let name = key.name.as_str();
        if name.starts_with("export.destination.") {
            return self.text("export.destination.id").is_some();
        }
        if name.starts_with("postgres.catalog.") {
            return self.local("archive");
        }
        if name.starts_with("postgres.control.") {
            return self.local("control");
        }
        if name.starts_with("clickhouse.read_") {
            return self.local("read");
        }
        if name.starts_with("clickhouse.write_") {
            return self.local("index");
        }
        if name.starts_with("clickhouse.") {
            return self.local("read") || self.local("index");
        }
        if name.starts_with("monitor.active_write.") {
            return self.local("monitor") && self.flag("monitor.active_write.enabled");
        }
        for route in ["tor", "i2p", "lokinet"] {
            if name.starts_with(&format!("network.{route}.")) {
                return self.flag(&format!("network.{route}.enabled"));
            }
        }
        if name.starts_with("storage.") {
            return self.selected_storage_roots().any(|key| key == name);
        }
        if name.starts_with("topology.")
            || name.starts_with("config.")
            || name.starts_with("secrets.")
        {
            return true;
        }
        if key.owner == "acquisition" {
            return ["sync", "monitor", "validator", "discoverer"]
                .iter()
                .any(|role| self.local(role));
        }
        if name.starts_with("indexer.") {
            return self.local("index");
        }
        self.local(key.owner) || key.owner == "runtime"
    }
    pub fn report(&self, show: bool) -> Result<Value, String> {
        let mut report = json!({"format":"bigbrotr-config-static/1","status":"valid-static","document_sha256":self.document_sha256,"authority":{"mode":self.text("config.authority_mode"),"status":"unavailable-offline","desired_revision":null,"applied_revision":null,"activation":"not-performed"},"local_roles":self.local_roles,"required_dependencies":self.dependencies,"full_signed_events":true,"listeners":{"public":false},"signing":false,"directory_discovery":false,"sync_inventory":if self.text("topology.ordered_set_reader")==Some("absent") {"unavailable; auto uses linear fallback"}else{"declared; readiness not checked"},"limitations":["Static desired configuration only; no roles, stores, authority or listeners are started.","Engine identity/schema/grants, filesystem aliases and accepted-work drain capacity require owning runtime adapters."]});
        report["fixed_endpoint_counts"] = json!({"sync":self.value("sync.endpoints.urls").as_array().unwrap().len(),"monitor":self.value("monitor.endpoints.urls").as_array().unwrap().len()});
        report["initial_policy"] = super::policy::PolicySeed::from_resolved(self)?.report();
        if show {
            let mut fields = serde_json::Map::new();
            for key in &self.keys {
                let mut value = self.value(&key.name).clone();
                if key.sensitive() && !value.is_null() {
                    value = json!("[redacted]");
                }
                // Public metadata never rounds large integers through a JS number.
                diagnostic_integers(&mut value);
                fields.insert(key.name.clone(),json!({"value":value,"source":self.sources[&key.name],"default_from":key.default_from,"default_add_us":key.default_add_us.to_string(),"reload":key.reload,"deployment_locked":key.root,"applicability":if self.applicable(key) {"selected"}else{"staged-unselected"}}));
            }
            report["settings"] = Value::Object(fields);
        }
        Ok(report)
    }
}

fn pg_endpoint(raw: &str) -> Result<url::Url, String> {
    let port = raw
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .filter(|p| *p > 0);
    if port.is_none() || raw.contains(['/', '?', '#', '@']) {
        return Err("PG endpoint requires explicit host:port without credentials".into());
    }
    endpoint(&format!("https://{raw}"), true)
}

fn endpoint(raw: &str, require_https: bool) -> Result<url::Url, String> {
    if raw.len() > 8192
        || raw.chars().any(|c| c.is_control() || c.is_whitespace())
        || raw.contains(['\\', '@', '?', '#'])
    {
        return Err("infrastructure endpoint contains forbidden syntax (value withheld)".into());
    }
    let url =
        url::Url::parse(raw).map_err(|_| "invalid infrastructure endpoint (value withheld)")?;
    if !matches!(url.scheme(), "http" | "https")
        || (require_https && url.scheme() != "https")
        || url.host_str().is_none()
        || url.port() == Some(0)
        || url.path() != "/"
    {
        return Err("infrastructure endpoint requires explicit HTTP(S) authority without path/credentials/query".into());
    }
    Ok(url)
}
