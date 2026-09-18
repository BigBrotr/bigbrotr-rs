//! Closed normalized product-policy encoding; a document never grants authority.
use super::{registry::Kind, resolve::Resolved};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{self, Write},
};

const FORMAT: &str = "bigbrotr-product-policy/1";
const DOMAIN: &[u8] = b"bigbrotr/product-policy/1\0";

struct Count {
    bytes: usize,
    maximum: usize,
}
impl Count {
    fn add(&mut self, bytes: usize) -> io::Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|&count| count <= self.maximum)
            .ok_or_else(|| io::Error::other("policy seed exceeds encoded limit"))?;
        Ok(())
    }
}
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.add(bytes.len())?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct PolicySeed {
    document: Vec<u8>,
    digest: [u8; 32],
    settings: usize,
}
impl PolicySeed {
    pub(crate) fn from_resolved(value: &Resolved) -> Result<Self, String> {
        let mut settings = BTreeMap::new();
        for key in &value.keys {
            if !key.root {
                // A future registry edit must not make a credential reference
                // ordinary persisted policy just by changing its root flag.
                if matches!(key.kind, Kind::Secret) {
                    return Err("secret references must remain deployment roots".into());
                }
                settings.insert(key.name.as_str(), value.value(&key.name));
            }
        }
        Self::encode(&settings, super::resolve::MAX_CONFIG_BYTES)
    }

    fn encode(settings: &BTreeMap<&str, &Value>, maximum: usize) -> Result<Self, String> {
        fn write(settings: &BTreeMap<&str, &Value>, mut out: impl Write) -> io::Result<()> {
            out.write_all(b"{\"format\":")?;
            serde_json::to_writer(&mut out, FORMAT).map_err(io::Error::other)?;
            out.write_all(b",\"settings\":")?;
            serde_json::to_writer(&mut out, settings).map_err(io::Error::other)?;
            out.write_all(b"}")
        }
        let mut count = Count { bytes: 0, maximum };
        write(settings, &mut count).map_err(|_| "policy seed exceeds 10MiB encoded limit")?;
        // The SQL CHECK and every accepted-document reader bound JSONB text,
        // whose separators and decimal numbers differ from our digest bytes.
        jsonb_policy_bytes(settings, super::resolve::MAX_CONFIG_BYTES)
            .map_err(|_| "policy seed exceeds 10MiB persisted JSONB limit")?;
        let mut document = Vec::new();
        document
            .try_reserve_exact(count.bytes)
            .map_err(|_| "policy seed allocation unavailable")?;
        write(settings, &mut document).map_err(|_| "policy seed encoding failed")?;
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(&document);
        Ok(Self {
            document,
            digest: hash.finalize().into(),
            settings: settings.len(),
        })
    }

    /// Rebind exact accepted policy to current deployment roots. No recipe/default
    /// fills a missing stored field, and the input digest never authorizes work.
    pub(crate) fn restore(
        document: &str,
        digest: [u8; 32],
        mut root: Resolved,
    ) -> Result<Resolved, String> {
        if document.len() > super::resolve::MAX_CONFIG_BYTES {
            return Err("stored policy exceeds encoded limit".into());
        }
        let mut value: Value = serde_json::from_str(document)
            .map_err(|_| "invalid stored policy encoding (values withheld)")?;
        let object = value
            .as_object_mut()
            .ok_or("stored policy must be an object")?;
        if object.len() != 2 || object.get("format").and_then(Value::as_str) != Some(FORMAT) {
            return Err("incompatible stored policy format".into());
        }
        let settings = object
            .get_mut("settings")
            .and_then(Value::as_object_mut)
            .ok_or("stored policy settings must be an object")?;
        if settings.len() != root.keys.iter().filter(|key| !key.root).count() {
            return Err("stored policy has missing or unknown settings".into());
        }
        for key in &root.keys {
            if !key.root {
                let value = settings
                    .remove(&key.name)
                    .ok_or("stored policy has missing or unknown settings")?;
                let normalized = super::resolve::stored_value(key, &value)
                    .map_err(|_| "stored policy has an invalid normalized value")?;
                root.values.insert(key.name.clone(), normalized);
                root.sources.insert(key.name.clone(), "accepted-policy");
            }
        }
        root.dependencies.clear();
        root.validate()?;
        let encoded = Self::from_resolved(&root)?;
        if encoded.digest != digest {
            return Err("stored policy digest mismatch".into());
        }
        Ok(root)
    }

    pub(crate) fn document(&self) -> &str {
        std::str::from_utf8(&self.document).expect("JSON writer emits UTF-8")
    }

    pub(crate) fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub(crate) fn report(&self) -> Value {
        let digest: String = self
            .digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        json!({"format":FORMAT,"status":"candidate-only","sha256":digest,
            "encoded_bytes":self.document.len().to_string(),"settings":self.settings.to_string()})
    }
}

/// Size only, never an alternative serializer or policy identity. Pinned PG16
/// JsonbToCStringWorker adds spaces after commas/colons; numeric_out expands
/// exponent-form finite numbers. Key ordering does not affect the byte count.
fn jsonb_policy_bytes(settings: &BTreeMap<&str, &Value>, maximum: usize) -> io::Result<usize> {
    let mut count = Count { bytes: 0, maximum };
    count.write_all(b"{\"format\": ")?;
    serde_json::to_writer(&mut count, FORMAT).map_err(io::Error::other)?;
    count.write_all(b", \"settings\": {")?;
    for (index, (key, value)) in settings.iter().enumerate() {
        if index > 0 {
            count.add(2)?;
        }
        serde_json::to_writer(&mut count, key).map_err(io::Error::other)?;
        count.add(2)?;
        jsonb_value_bytes(value, &mut count)?;
    }
    count.add(2)?; // close settings and document
    Ok(count.bytes)
}

fn jsonb_value_bytes(value: &Value, count: &mut Count) -> io::Result<()> {
    match value {
        Value::Array(values) => {
            count.add(2)?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    count.add(2)?;
                }
                jsonb_value_bytes(value, count)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            count.add(2)?;
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    count.add(2)?;
                }
                serde_json::to_writer(&mut *count, key).map_err(io::Error::other)?;
                count.add(2)?;
                jsonb_value_bytes(value, count)?;
            }
            Ok(())
        }
        Value::Number(number) if number.is_f64() => {
            // Number::Display and Serialize use the same pinned zmij formatting.
            // This string is one finite f64, never an arbitrary policy string.
            count.add(jsonb_number_bytes(&number.to_string())?)
        }
        _ => serde_json::to_writer(count, value).map_err(io::Error::other),
    }
}

fn jsonb_number_bytes(text: &str) -> io::Result<usize> {
    let Some((coefficient, exponent)) = text.split_once('e') else {
        return Ok(text.len());
    };
    let exponent = i64::from(exponent.parse::<i32>().map_err(io::Error::other)?);
    let sign = usize::from(coefficient.starts_with('-'));
    let coefficient = coefficient.strip_prefix('-').unwrap_or(coefficient);
    let (integer, fraction) = coefficient.split_once('.').unwrap_or((coefficient, ""));
    let integer = (integer.len() as i64 + exponent).max(1);
    let fraction = (fraction.len() as i64 - exponent).max(0);
    usize::try_from(integer + fraction + i64::from(fraction > 0))
        .ok()
        .and_then(|n| n.checked_add(sign))
        .ok_or_else(|| io::Error::other("policy numeric rendering exceeds range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_size_matches_independent_renderings_at_exact_boundaries() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../experiments/configuration-inspection/jsonb-cases.json"
        ))
        .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let value: Value = serde_json::from_str(case["compact"].as_str().unwrap()).unwrap();
            let rendered = case["rendered"].as_str().unwrap();
            let mut count = Count {
                bytes: 0,
                maximum: rendered.len(),
            };
            jsonb_value_bytes(&value, &mut count).unwrap();
            assert_eq!(count.bytes, rendered.len(), "{case}");
            count.bytes = 0;
            count.maximum -= 1;
            assert!(jsonb_value_bytes(&value, &mut count).is_err(), "{case}");

            let settings = BTreeMap::from([("x", &value)]);
            let expected = format!(
                "{{\"format\": \"bigbrotr-product-policy/1\", \"settings\": {{\"x\": {rendered}}}}}"
            );
            assert_eq!(
                jsonb_policy_bytes(&settings, expected.len()).unwrap(),
                expected.len()
            );
            assert!(jsonb_policy_bytes(&settings, expected.len() - 1).is_err());
        }
    }

    #[test]
    fn literal_encoding_preserves_order_unsigned_values_and_null_with_exact_limit() {
        let maximum = json!(u64::MAX);
        let null = Value::Null;
        let text = json!("é\n\"");
        let settings = BTreeMap::from([("z", &text), ("a", &maximum), ("n", &null)]);
        let expected = "{\"format\":\"bigbrotr-product-policy/1\",\"settings\":{\"a\":18446744073709551615,\"n\":null,\"z\":\"é\\n\\\"\"}}";
        let seed = PolicySeed::encode(&settings, expected.len()).unwrap();
        assert_eq!(seed.document, expected.as_bytes());
        // Independent Python hashlib over the literal UTF-8 document and domain.
        assert_eq!(
            seed.report()["sha256"],
            "1c8054d6d7d21473b7588e39dfbea8f2cabbd58987763c1a022cb7a6ca101a7a"
        );
        assert!(PolicySeed::encode(&settings, expected.len() - 1).is_err());
        assert!(PolicySeed::encode(&settings, 0).is_err());
    }

    fn root() -> Resolved {
        super::super::resolve::parse(include_str!("../../../../config/recipes/archive-only.toml"))
            .unwrap()
    }

    #[test]
    fn compact_candidate_cannot_exceed_persisted_jsonb_limit() {
        let mut root = root();
        for role in ["sync", "monitor"] {
            root.values
                .insert(format!("{role}.endpoints.mode"), json!("fixed"));
            root.values.insert(
                format!("{role}.endpoints.urls"),
                Value::Array(
                    (0..100_000)
                        .map(|n| json!(format!("wss://relay.example/{n:06}/{}", "x".repeat(22))))
                        .collect(),
                ),
            );
        }
        root.validate().unwrap();
        assert!(PolicySeed::from_resolved(&root).is_err());
    }

    #[test]
    fn persisted_policy_uses_accepted_values_and_revalidates_current_roots() {
        let seed = PolicySeed::from_resolved(&root()).unwrap();
        let document = String::from_utf8(seed.document.clone()).unwrap();
        let changed = super::super::resolve::parse(&format!(
            "{}\n[ingest]\nmax_event_bytes='512KiB'\n",
            include_str!("../../../../config/recipes/archive-only.toml")
        ))
        .unwrap();
        let effective = PolicySeed::restore(&document, seed.digest, changed).unwrap();
        assert_eq!(effective.number("ingest.max_event_bytes"), 262144);
        assert_eq!(
            effective.sources["ingest.max_event_bytes"],
            "accepted-policy"
        );
        assert_eq!(
            PolicySeed::from_resolved(&effective).unwrap().digest,
            seed.digest
        );
        assert!(PolicySeed::restore(&document, [0; 32], root()).is_err());
        for (name, value) in [
            ("ingest.max_event_bytes", json!(0)),
            ("ingest.max_event_bytes", Value::Null),
            (
                "topology.dataset_id",
                json!("11111111111111111111111111111111"),
            ),
        ] {
            let mut invalid: Value = serde_json::from_str(&document).unwrap();
            invalid["settings"][name] = value;
            let bytes = invalid.to_string();
            let digest: [u8; 32] = Sha256::new()
                .chain_update(DOMAIN)
                .chain_update(bytes.as_bytes())
                .finalize()
                .into();
            assert!(PolicySeed::restore(&bytes, digest, root()).is_err());
        }
        let mut incomplete: Value = serde_json::from_str(&document).unwrap();
        incomplete["settings"]
            .as_object_mut()
            .unwrap()
            .remove("ranker.enabled");
        assert!(PolicySeed::restore(&incomplete.to_string(), seed.digest, root()).is_err());
    }

    #[test]
    fn persisted_finite_fraction_keeps_exact_binary_value_and_digest() {
        // Upstream serde_json's f64 round-trip regression, also a legal fraction.
        let input = include_str!("../../../../config/recipes/archive-only.toml").replace(
            "[monitor]\n",
            "[monitor]\nschedule_jitter=2.0030397744267762e-253\n",
        );
        let candidate = super::super::resolve::parse(&input).unwrap();
        let bits = candidate
            .value("monitor.schedule_jitter")
            .as_f64()
            .unwrap()
            .to_bits();
        let seed = PolicySeed::from_resolved(&candidate).unwrap();
        let document = std::str::from_utf8(&seed.document).unwrap();
        let restored = PolicySeed::restore(document, seed.digest, root()).unwrap();
        assert_eq!(
            restored
                .value("monitor.schedule_jitter")
                .as_f64()
                .unwrap()
                .to_bits(),
            bits
        );
    }
}
