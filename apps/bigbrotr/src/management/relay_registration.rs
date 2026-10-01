//! Authenticated registration intent. Normalized identity is not enrollment,
//! visibility allocation, route permission or a network capability.
use super::grants::Revision;
use bigbrotr_model::{CommandKey, DatasetId, Digest256, EndpointId, PrincipalId};
use bigbrotr_protocol::endpoint::Endpoint;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationCommand {
    #[serde(deserialize_with = "read_endpoint", serialize_with = "write_endpoint")]
    pub url: Endpoint,
    pub expected_config_revision: Revision,
    pub expected_policy_revision: Revision,
}

fn read_endpoint<'de, D: Deserializer<'de>>(input: D) -> Result<Endpoint, D::Error> {
    Endpoint::parse(&String::deserialize(input)?).map_err(de::Error::custom)
}

fn write_endpoint<S: Serializer>(endpoint: &Endpoint, output: S) -> Result<S::Ok, S::Error> {
    output.serialize_str(endpoint.as_str())
}

impl RegistrationCommand {
    pub fn digest(&self, dataset: DatasetId, actor: PrincipalId, key: CommandKey) -> Digest256 {
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/relay-registration/1\0");
        for bytes in [dataset.as_bytes(), actor.as_bytes(), key.as_bytes()] {
            hash.update(bytes);
        }
        hash.update(self.expected_config_revision.get().to_be_bytes());
        hash.update(self.expected_policy_revision.get().to_be_bytes());
        hash.update(1u16.to_be_bytes());
        let url = self.url.as_str().as_bytes();
        // Endpoint owns the 8192-byte bound; this length is independent of the
        // JSON spelling, and the prefix leaves future trailing fields unambiguous.
        hash.update((url.len() as u32).to_be_bytes());
        hash.update(url);
        Digest256::from_bytes(hash.finalize().into())
    }
}

/// Original command acceptance. `created` names this command, never whether
/// this caller first registered the endpoint. Retries preserve its identity/time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegistrationReceipt {
    pub command_key: CommandKey,
    pub endpoint_id: EndpointId,
    pub accepted_at_us: i64,
    pub created: bool,
}
