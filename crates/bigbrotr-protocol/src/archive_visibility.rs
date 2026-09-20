//! Exact archived visibility allocation, never a principal/action permission.
use crate::{archive_cut::CutBound, archive_receipt::MAX_RANGES};
use bigbrotr_model::{
    DatasetId, DictionaryDescriptor, DictionaryKind, Digest256, Fact, FactPayload, QualifiedRef,
    SourceRecordId, TopologyGeneration,
    codec::{CodecError, FactDecodeLimits},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const FORMAT: &str = "bigbrotr-native-visibility/1";
// A visibility fact contains fixed identities/scalars and one <=64-byte ASCII
// class. Other dictionary variants are deliberately not accepted by this read.
pub const MAX_FACT_BYTES: usize = 512;
pub const MAX_RESPONSE_BYTES: usize = 1536;
pub const MAX_NATIVE_ROW_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityReadError {
    InvalidInput,
    Missing,
    Mismatch,
    ResourceLimit,
    Native,
}
impl std::fmt::Display for VisibilityReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "private visibility read failed: {self:?}")
    }
}
impl std::error::Error for VisibilityReadError {}
impl From<CodecError> for VisibilityReadError {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::InputLimit
            | CodecError::AllocationLimit
            | CodecError::TagLimit
            | CodecError::StringLimit
            | CodecError::AllocationFailed => Self::ResourceLimit,
            _ => Self::Mismatch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityRead {
    pub dataset: DatasetId,
    pub scope: QualifiedRef,
    pub topology_generation: TopologyGeneration,
    pub bounds: Vec<CutBound>,
    pub snapshot_id: u64,
}
impl VisibilityRead {
    pub fn validate(&self) -> Result<(), VisibilityReadError> {
        if self.bounds.len() > MAX_RANGES {
            return Err(VisibilityReadError::ResourceLimit);
        }
        if self
            .scope
            .require(self.dataset, DictionaryKind::VisibilityScope)
            .is_err()
            || self.snapshot_id > i64::MAX as u64
            || self.bounds.iter().any(|b| b.owner_epoch == 0)
            || self
                .bounds
                .windows(2)
                .any(|b| b[0].stream() >= b[1].stream())
        {
            return Err(VisibilityReadError::InvalidInput);
        }
        Ok(())
    }
    pub fn contains(&self, source: SourceRecordId) -> bool {
        source.dataset_id == self.dataset
            && self
                .bounds
                .binary_search_by_key(&(source.shard_id, source.owner_epoch.get()), |b| b.stream())
                .is_ok_and(|i| source.sequence.get() <= self.bounds[i].complete_through)
    }
    /// Request correlation only. The caller still needs a certified retained pin.
    pub fn digest(&self) -> Digest256 {
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/archive-visibility-read/1\0");
        hash.update(self.dataset.as_bytes());
        hash.update(self.scope.dataset_id.as_bytes());
        hash.update((self.scope.dictionary_kind as u16).to_be_bytes());
        hash.update(self.scope.allocation_domain.as_bytes());
        hash.update(self.scope.ordinal.get().to_be_bytes());
        hash.update(self.topology_generation.get().to_be_bytes());
        hash.update(self.snapshot_id.to_be_bytes());
        hash.update((self.bounds.len() as u64).to_be_bytes());
        for bound in &self.bounds {
            hash.update(bound.shard_id.to_be_bytes());
            hash.update(bound.owner_epoch.to_be_bytes());
            hash.update(bound.complete_through.to_be_bytes());
        }
        Digest256::from_bytes(hash.finalize().into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityObservation {
    pub format: String,
    pub snapshot_id: u64,
    pub request_digest: Digest256,
    pub fact_digest: Digest256,
    pub preimage_hex: String,
}
impl VisibilityObservation {
    /// Integrity/selection check, not proof that caller-created bytes were stored.
    /// Only the actual storage adapter may mint an allocated-scope observation.
    pub fn verify(&self, read: &VisibilityRead) -> Result<Fact, VisibilityReadError> {
        use VisibilityReadError as E;
        read.validate()?;
        if self.preimage_hex.len() > MAX_FACT_BYTES * 2 {
            return Err(E::ResourceLimit);
        }
        if self.format != FORMAT
            || self.snapshot_id != read.snapshot_id
            || self.request_digest != read.digest()
            || !self.preimage_hex.len().is_multiple_of(2)
        {
            return Err(E::Mismatch);
        }
        // Fixed stack buffer: neither length nor hex can force an allocation.
        let mut bytes = [0u8; MAX_FACT_BYTES];
        let n = self.preimage_hex.len() / 2;
        for (out, pair) in bytes[..n]
            .iter_mut()
            .zip(self.preimage_hex.as_bytes().as_chunks::<2>().0)
        {
            let digit = |c| match c {
                b'0'..=b'9' => Ok(c - b'0'),
                b'a'..=b'f' => Ok(c - b'a' + 10),
                _ => Err(E::Mismatch),
            };
            *out = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        let fact = Fact::decode_preimage(
            &bytes[..n],
            self.fact_digest,
            FactDecodeLimits {
                max_encoded_bytes: MAX_FACT_BYTES,
                max_allocation_bytes: 4096,
                max_tag_nodes: 0,
                max_string_bytes: 64,
            },
        )?;
        if !read.contains(fact.source_record_id()) {
            return Err(E::Mismatch);
        }
        match fact.payload() {
            FactPayload::Dictionary(entry)
                if entry.reference() == read.scope
                    && matches!(entry.descriptor(), DictionaryDescriptor::Visibility { .. }) =>
            {
                Ok(fact)
            }
            _ => Err(E::Mismatch),
        }
    }
}
