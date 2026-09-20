//! Private source/digest and original-file census; never a protection certificate.
use crate::archive_receipt::{ExpectationError, ReceiptExpectation};
use bigbrotr_model::Digest256;
use serde::{Deserialize, Serialize};

pub const FORMAT: &str = "bigbrotr-native-membership/1";
pub const MAX_PATH_BYTES: usize = 8192;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRead {
    pub receipt: ReceiptExpectation,
    pub snapshot_id: Option<u64>,
}
impl MembershipRead {
    pub fn validate(&self, max_facts: usize) -> Result<(), ExpectationError> {
        self.receipt.validate()?;
        if self.snapshot_id.is_some_and(|id| id > i64::MAX as u64) {
            return Err(ExpectationError::InvalidIdentity);
        }
        if max_facts == 0 || self.receipt.record_count > max_facts as u64 {
            return Err(ExpectationError::ResourceLimit);
        }
        Ok(())
    }
}

/// Counts describe stored identity columns, not recomputed scientific payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipObservation {
    pub format: String,
    pub snapshot_id: u64,
    pub batch_digest: Digest256,
    pub attempt_id: [u8; 16],
    pub publication_ordinal: u64,
    /// Dictionaries, payloads, eligibility, receive, provenance, in that order.
    pub family_counts: [u64; 5],
    /// Populated original family files plus the one original receipt file.
    pub files: u32,
}
impl MembershipObservation {
    pub fn validate(&self, read: &MembershipRead) -> Result<(), ExpectationError> {
        if self.format != FORMAT
            || self.snapshot_id > i64::MAX as u64
            || read.snapshot_id.is_some_and(|id| id != self.snapshot_id)
            || self.batch_digest != read.receipt.batch_digest
            || self.attempt_id != read.receipt.attempt_id
            || self.publication_ordinal != read.receipt.publication_ordinal
            || self
                .family_counts
                .iter()
                .try_fold(0u64, |a, b| a.checked_add(*b))
                != Some(read.receipt.record_count)
            || self.files != 1 + self.family_counts.iter().filter(|n| **n != 0).count() as u32
        {
            return Err(ExpectationError::InvalidIdentity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_distinguishes_impossible_identity_from_finite_capacity() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/protocol/archive-membership.json"
        ))
        .unwrap();
        let mut read: MembershipRead = serde_json::from_value(vector["read"].clone()).unwrap();
        assert_eq!(read.validate(4), Ok(()));
        for maximum in [0, 3] {
            assert_eq!(read.validate(maximum), Err(ExpectationError::ResourceLimit));
        }
        read.snapshot_id = Some(i64::MAX as u64);
        assert_eq!(read.validate(4), Ok(()));
        read.snapshot_id = Some(i64::MAX as u64 + 1);
        assert_eq!(read.validate(4), Err(ExpectationError::InvalidIdentity));
        read.snapshot_id = None;
        read.receipt.input_ranges[0].last_sequence = 25;
        assert_eq!(read.validate(4), Err(ExpectationError::InvalidIdentity));
    }
}
