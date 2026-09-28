//! Exact no-auth enrollment intent. Acceptance does not authorize a socket or
//! prove visibility allocation, reachability or completed restrictive drain.
use super::{grants::Revision, sessions::SessionError};
use crate::network::RouteSelection;
use bigbrotr_model::{
    AccessScopeId, CommandKey, DatasetId, DictionaryKind, Digest256, EndpointId, PrincipalId,
    QualifiedRef, RouteId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentCommand {
    pub endpoint_id: EndpointId,
    pub visibility: QualifiedRef,
    pub route: RouteSelection,
    pub enabled: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    pub expected_generation: Option<Revision>,
    pub expected_config_revision: Revision,
    pub expected_policy_revision: Revision,
}

impl EnrollmentCommand {
    pub fn validate(&self, dataset: DatasetId) -> Result<(), SessionError> {
        if self.visibility.dataset_id != dataset
            || self.visibility.dictionary_kind != DictionaryKind::VisibilityScope
            || (!self.enabled && self.expected_generation.is_none())
        {
            return Err(SessionError::InvalidInput);
        }
        Ok(())
    }

    pub fn digest(
        &self,
        dataset: DatasetId,
        actor: PrincipalId,
        key: CommandKey,
    ) -> Result<Digest256, SessionError> {
        self.validate(dataset)?;
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/relay-enrollment-change/1\0");
        for bytes in [
            dataset.as_bytes(),
            actor.as_bytes(),
            key.as_bytes(),
            self.endpoint_id.as_bytes(),
            self.visibility.dataset_id.as_bytes(),
        ] {
            hash.update(bytes);
        }
        hash.update(4u16.to_be_bytes());
        hash.update(self.visibility.allocation_domain.as_bytes());
        hash.update(self.visibility.ordinal.get().to_be_bytes());
        hash.update([u8::from(self.enabled)]);
        for revision in [
            self.expected_generation.map_or(0, Revision::get),
            self.expected_config_revision.get(),
            self.expected_policy_revision.get(),
        ] {
            hash.update(revision.to_be_bytes());
        }
        hash.update(self.route.canonical_bytes());
        Ok(Digest256::from_bytes(hash.finalize().into()))
    }
}

/// Historical acceptance only. The same endpoint with another visibility names
/// a separate access/enrollment key; it does not replace this one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnrollmentReceipt {
    pub command_key: CommandKey,
    pub endpoint_id: EndpointId,
    pub access_scope_id: AccessScopeId,
    pub route_id: RouteId,
    pub enabled: bool,
    pub generation: Revision,
    pub accepted_at_us: i64,
    pub created: bool,
}
