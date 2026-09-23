//! Exact desired command inputs, never grant or reauthentication authority.
use super::sessions::SessionError;
pub use bigbrotr_model::CommandKey;
use bigbrotr_model::{
    DatasetId, DictionaryKind, Digest256, OwnerResourceId, PrincipalId, QualifiedRef,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::num::NonZeroU64;

/// Accepted revisions are positive and use lossless canonical decimal strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Revision(NonZeroU64);
impl Revision {
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
impl TryFrom<String> for Revision {
    type Error = SessionError;
    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse::<u64>()
            .ok()
            .filter(|n| n.to_string() == text)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(SessionError::InvalidInput)
    }
}
impl From<Revision> for String {
    fn from(value: Revision) -> Self {
        value.get().to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopeAction {
    #[serde(rename = "history.read")]
    HistoryRead,
}
impl ScopeAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HistoryRead => "history.read",
        }
    }
    const fn code(self) -> u8 {
        match self {
            Self::HistoryRead => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationSelection {
    pub catalog_id: OwnerResourceId,
    pub frontier_revision: Revision,
}

/// The route binds principal_id to its target and resolves this selection from
/// current Control and retained native evidence. Client fields assert no facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantCommand {
    pub principal_id: PrincipalId,
    pub action: ScopeAction,
    pub scope: QualifiedRef,
    pub enabled: bool,
    pub expected_config_revision: Revision,
    pub expected_policy_revision: Revision,
    pub expected_grant_revision: Revision,
    #[serde(deserialize_with = "Option::deserialize")]
    pub allocation: Option<AllocationSelection>,
}
impl GrantCommand {
    pub fn validate(&self, dataset: DatasetId) -> Result<(), SessionError> {
        if self.scope.dataset_id != dataset
            || self.scope.dictionary_kind != DictionaryKind::VisibilityScope
            || self.enabled != self.allocation.is_some()
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
        hash.update(b"bigbrotr/scope-grant-change/1\0");
        for bytes in [
            dataset.as_bytes(),
            actor.as_bytes(),
            key.as_bytes(),
            self.principal_id.as_bytes(),
            self.scope.dataset_id.as_bytes(),
        ] {
            hash.update(bytes);
        }
        hash.update(4u16.to_be_bytes());
        hash.update(self.scope.allocation_domain.as_bytes());
        hash.update(self.scope.ordinal.get().to_be_bytes());
        hash.update([self.action.code(), u8::from(self.enabled)]);
        for revision in [
            self.expected_config_revision,
            self.expected_policy_revision,
            self.expected_grant_revision,
        ] {
            hash.update(revision.get().to_be_bytes());
        }
        hash.update([u8::from(self.allocation.is_some())]);
        if let Some(allocation) = &self.allocation {
            hash.update(allocation.catalog_id.as_bytes());
            hash.update(allocation.frontier_revision.get().to_be_bytes());
        }
        Ok(Digest256::from_bytes(hash.finalize().into()))
    }
}

/// Immutable command result. It records acceptance, never completed restrictive
/// drain or reusable authorization to read. Retries return this original result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GrantReceipt {
    pub command_key: CommandKey,
    pub principal_id: PrincipalId,
    pub scope: QualifiedRef,
    pub enabled: bool,
    pub scope_generation: Revision,
    pub account_grant_revision: Revision,
    pub accepted_at_us: i64,
    pub created: bool,
}
