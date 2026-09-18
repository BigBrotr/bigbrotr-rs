//! Closed normalized initialization data; never accepted policy or authority.
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

pub(super) struct PolicySeed {
    document: Vec<u8>,
    digest: [u8; 32],
    settings: usize,
}
impl PolicySeed {
    pub(super) fn from_resolved(value: &Resolved) -> Result<Self, String> {
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

    pub(super) fn report(&self) -> Value {
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
}
