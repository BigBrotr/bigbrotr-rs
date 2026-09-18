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
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|&count| count <= self.maximum)
            .ok_or_else(|| io::Error::other("policy seed exceeds encoded limit"))?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
