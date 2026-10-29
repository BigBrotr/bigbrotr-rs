//! Bounded alternative attempt identities for one complete retained batch.
//! This private observation request is neither writer nor frontier authority.
use super::{ExpectationError, ReceiptExpectation};
use bigbrotr_model::Digest256;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_MEMBERS: usize = 128;
pub const FORMAT: &str = "bigbrotr-native-receipt-group/1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub attempt_id: [u8; 16],
    pub publication_ordinal: u64,
}
impl Member {
    pub fn from_receipt(receipt: &ReceiptExpectation) -> Self {
        Self {
            attempt_id: receipt.attempt_id,
            publication_ordinal: receipt.publication_ordinal,
        }
    }
}

/// Borrows the original range vector; only the compact identity list is new.
#[derive(Clone, Copy, Serialize)]
pub struct GroupRead<'a> {
    pub common: &'a ReceiptExpectation,
    pub members: &'a [Member],
}
impl GroupRead<'_> {
    pub fn validate(&self) -> Result<(), ExpectationError> {
        self.common.validate()?;
        if self.members.len() > MAX_MEMBERS {
            return Err(ExpectationError::ResourceLimit);
        }
        if self.members.first().copied() != Some(Member::from_receipt(self.common)) {
            return Err(ExpectationError::InvalidIdentity);
        }
        for (index, member) in self.members.iter().enumerate() {
            if member.attempt_id == [0; 16]
                || member.publication_ordinal == 0
                || self.members[..index].iter().any(|prior| {
                    prior.attempt_id == member.attempt_id
                        || prior.publication_ordinal >= member.publication_ordinal
                })
            {
                return Err(ExpectationError::InvalidIdentity);
            }
        }
        Ok(())
    }

    /// Explicit domain-separated binary binding, independent of JSON layout.
    /// Integers are big endian; range/member lengths use u32, after validation.
    pub fn digest(&self) -> Result<Digest256, ExpectationError> {
        self.validate()?;
        let c = self.common;
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr-private-receipt-group-v1\0");
        hash.update(c.dataset_id.as_bytes());
        hash.update(c.schema_version.to_be_bytes());
        hash.update(c.topology_generation.get().to_be_bytes());
        hash.update(c.batch_digest.as_bytes());
        hash.update(c.record_count.to_be_bytes());
        hash.update((c.input_ranges.len() as u32).to_be_bytes());
        for r in &c.input_ranges {
            hash.update(r.shard_id.to_be_bytes());
            hash.update(r.owner_epoch.to_be_bytes());
            hash.update(r.first_sequence.to_be_bytes());
            hash.update(r.last_sequence.to_be_bytes());
        }
        hash.update(c.attempt_id);
        hash.update(c.publication_ordinal.to_be_bytes());
        hash.update((self.members.len() as u32).to_be_bytes());
        for member in self.members {
            hash.update(member.attempt_id);
            hash.update(member.publication_ordinal.to_be_bytes());
        }
        Ok(Digest256::from_bytes(hash.finalize().into()))
    }

    pub fn member_index(&self, member: Member) -> Option<usize> {
        self.members.iter().position(|expected| *expected == member)
    }

    pub fn same_semantics(&self, actual: &ReceiptExpectation) -> bool {
        let c = self.common;
        c.dataset_id == actual.dataset_id
            && c.schema_version == actual.schema_version
            && c.topology_generation == actual.topology_generation
            && c.batch_digest == actual.batch_digest
            && c.record_count == actual.record_count
            && c.input_ranges == actual.input_ranges
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupObservation {
    pub format: String,
    pub request_digest: Digest256,
    #[serde(deserialize_with = "required_present")]
    pub present: Option<Member>,
}
fn required_present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Member>, D::Error> {
    Option::deserialize(deserializer)
}
impl GroupObservation {
    pub fn validate(&self, read: GroupRead<'_>) -> Result<Option<usize>, ExpectationError> {
        if self.format != FORMAT || self.request_digest != read.digest()? {
            return Err(ExpectationError::InvalidIdentity);
        }
        self.present
            .map(|member| {
                read.member_index(member)
                    .ok_or(ExpectationError::InvalidIdentity)
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests;
