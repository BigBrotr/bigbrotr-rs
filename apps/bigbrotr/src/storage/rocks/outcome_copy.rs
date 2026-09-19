//! Private handoff witness, returned only after a synced write or validated replay.
use super::{super::manifest::IngestManifest, BootstrapError};
use bigbrotr_model::{DatasetId, Digest256, PolicyEpoch, ProducerRequestId, Receipt};
use sha2::{Digest, Sha256};

pub(in crate::storage) const MAX_OUTCOME_BYTES: usize = 64 * 1024;
// Retained bytes plus parameter/driver staging. Native engine memory is separate.
pub(super) const HANDOFF_DATA_BYTES: usize = 4 * MAX_OUTCOME_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum NativeOutcomeKind {
    NewFacts = 1,
    AlreadyDurable = 2,
    Rejected = 3,
}

/// Not constructible by a PG caller or a structural receipt reader. Creating the
/// bytes is preparation; only the native primitive can return the witness after
/// it establishes the commit and complete retained dependency closure.
pub(in crate::storage) struct NativeOutcomeCopy {
    request: ProducerRequestId,
    digest: Digest256,
    dataset: DatasetId,
    policy_epoch: PolicyEpoch,
    kind: NativeOutcomeKind,
    manifest_sha256: [u8; 32],
    bytes: Vec<u8>,
}
impl NativeOutcomeCopy {
    pub(super) fn receipt(
        receipt: &Receipt,
        manifest: &IngestManifest,
    ) -> Result<Self, BootstrapError> {
        Ok(Self {
            request: receipt.request_id(),
            digest: receipt.request_digest(),
            dataset: receipt.protection_context().dataset_id,
            policy_epoch: receipt.protection_context().policy_epoch,
            kind: if receipt.batch().is_some() {
                NativeOutcomeKind::NewFacts
            } else {
                NativeOutcomeKind::AlreadyDurable
            },
            manifest_sha256: Sha256::digest(manifest.encode().as_bytes()).into(),
            bytes: receipt.encode_persistent(MAX_OUTCOME_BYTES)?,
        })
    }
    pub(super) fn rejection(
        request: ProducerRequestId,
        digest: Digest256,
        policy_epoch: PolicyEpoch,
        manifest: &IngestManifest,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            request,
            digest,
            dataset: manifest.identity().dataset_id,
            policy_epoch,
            kind: NativeOutcomeKind::Rejected,
            manifest_sha256: Sha256::digest(manifest.encode().as_bytes()).into(),
            bytes,
        }
    }
    pub(in crate::storage) fn request(&self) -> ProducerRequestId {
        self.request
    }
    pub(in crate::storage) fn digest(&self) -> Digest256 {
        self.digest
    }
    pub(in crate::storage) fn dataset(&self) -> DatasetId {
        self.dataset
    }
    pub(in crate::storage) fn policy_epoch(&self) -> PolicyEpoch {
        self.policy_epoch
    }
    pub(in crate::storage) fn kind(&self) -> NativeOutcomeKind {
        self.kind
    }
    pub(in crate::storage) fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }
    pub(in crate::storage) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
