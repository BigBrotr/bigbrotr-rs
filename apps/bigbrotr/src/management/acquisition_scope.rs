//! Explicit immutable collection intent. Acceptance grants no acquisition or read.
use super::{grants::Revision, sessions::SessionError};
use bigbrotr_model::{
    AccessScopeId, AcquisitionScopeId, CommandKey, DatasetId, DictionaryKind, Digest256,
    EndpointId, PrincipalId, QualifiedRef, acquisition::CollectionSelector,
};
use bigbrotr_protocol::selector::{DecodeError, decode_selector, encode_selector};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::fmt;

pub const BODY_BYTES: usize = 2 * 1024 * 1024 + 1024;
/// Two body buffers,8MiB parser/selector,2MiB canonical output and envelope slack.
/// The HTTP caller reserves this before reading and retains it through SQL/queue.
pub const REQUEST_BYTES: u64 = 14 * 1024 * 1024 + 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<'a> {
    endpoint_id: EndpointId,
    access_scope_id: AccessScopeId,
    #[serde(borrow)]
    visibility: &'a RawValue,
    #[serde(borrow)]
    selector: &'a RawValue,
    expected_config_revision: Revision,
    expected_policy_revision: Revision,
}

/// One normalized selector and its exact profile1 bytes; no independently mutable
/// representations. Deliberately not Clone: consumers borrow under the same grant.
pub struct ScopeCommand {
    pub endpoint_id: EndpointId,
    pub access_scope_id: AccessScopeId,
    pub visibility: QualifiedRef,
    pub expected_config_revision: Revision,
    pub expected_policy_revision: Revision,
    selector: CollectionSelector,
    document: Vec<u8>,
}
impl fmt::Debug for ScopeCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopeCommand")
            .field("document_bytes", &self.document.len())
            .finish_non_exhaustive()
    }
}
impl ScopeCommand {
    /// Caller retains REQUEST_BYTES and one item through the input's and this
    /// value's actual lifetime. The borrowed envelope creates no generic JSON tree.
    pub fn decode(input: &[u8]) -> Result<Self, SessionError> {
        if input.len() > BODY_BYTES {
            return Err(SessionError::Capacity);
        }
        // Derived structs also accept arrays; the public envelope is an object.
        if input.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
            return Err(SessionError::InvalidInput);
        }
        let raw: Envelope<'_> =
            serde_json::from_slice(input).map_err(|_| SessionError::InvalidInput)?;
        if !raw.visibility.get().starts_with('{') {
            return Err(SessionError::InvalidInput);
        }
        let visibility: QualifiedRef =
            serde_json::from_str(raw.visibility.get()).map_err(|_| SessionError::InvalidInput)?;
        if visibility.dictionary_kind != DictionaryKind::VisibilityScope {
            return Err(SessionError::InvalidInput);
        }
        let selector =
            decode_selector(raw.selector.get().as_bytes()).map_err(|error| match error {
                DecodeError::Bounds(_) => SessionError::Capacity,
                _ => SessionError::InvalidInput,
            })?;
        let document = encode_selector(&selector).map_err(|_| SessionError::Capacity)?;
        Ok(Self {
            endpoint_id: raw.endpoint_id,
            access_scope_id: raw.access_scope_id,
            visibility,
            expected_config_revision: raw.expected_config_revision,
            expected_policy_revision: raw.expected_policy_revision,
            selector,
            document,
        })
    }

    pub fn selector(&self) -> &CollectionSelector {
        &self.selector
    }

    pub fn document(&self) -> &[u8] {
        &self.document
    }

    pub fn digest(
        &self,
        dataset: DatasetId,
        actor: PrincipalId,
        key: CommandKey,
    ) -> Result<Digest256, SessionError> {
        if self.visibility.dataset_id != dataset
            || self.visibility.dictionary_kind != DictionaryKind::VisibilityScope
        {
            return Err(SessionError::InvalidInput);
        }
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/acquisition-scope-registration/1\0");
        for bytes in [
            dataset.as_bytes(),
            actor.as_bytes(),
            key.as_bytes(),
            self.endpoint_id.as_bytes(),
            self.access_scope_id.as_bytes(),
            self.visibility.dataset_id.as_bytes(),
        ] {
            hash.update(bytes);
        }
        hash.update(4u16.to_be_bytes());
        hash.update(self.visibility.allocation_domain.as_bytes());
        for value in [
            self.visibility.ordinal.get(),
            self.expected_config_revision.get(),
            self.expected_policy_revision.get(),
        ] {
            hash.update(value.to_be_bytes());
        }
        // The existing selector encoder bounds this to2MiB, well below u32.
        hash.update((self.document.len() as u32).to_be_bytes());
        hash.update(&self.document);
        Ok(Digest256::from_bytes(hash.finalize().into()))
    }
}

/// Original command acceptance, not whether the actor first created the scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScopeReceipt {
    pub command_key: CommandKey,
    pub scope_id: AcquisitionScopeId,
    pub accepted_at_us: i64,
    pub created: bool,
}
