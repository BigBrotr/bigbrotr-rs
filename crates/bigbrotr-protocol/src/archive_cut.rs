//! Private complete-prefix observation inputs. These values carry no authority.

use crate::archive_receipt::{ExpectationError, MAX_RANGES, ReceiptExpectation};
use bigbrotr_model::{DatasetId, Digest256, TopologyGeneration};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const FORMAT: &str = "bigbrotr-native-cut/1";
pub const MAX_BATCHES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutBound {
    pub shard_id: u32,
    pub owner_epoch: u64,
    pub complete_through: u64,
}
impl CutBound {
    pub fn stream(self) -> (u32, u64) {
        (self.shard_id, self.owner_epoch)
    }
}

/// The parent resolves the sealed topology and authoritative closed-source bounds,
/// and derives each receipt from retained COMPLETE admission inputs. This private
/// structural check cannot establish those authorities or certify persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutRead {
    pub dataset_id: DatasetId,
    pub topology_generation: TopologyGeneration,
    pub bounds: Vec<CutBound>,
    pub receipts: Vec<ReceiptExpectation>,
    pub snapshot_id: Option<u64>,
}
impl CutRead {
    pub fn validate(&self, max_facts: usize) -> Result<u64, ExpectationError> {
        use ExpectationError::{InvalidIdentity, ResourceLimit};
        if self.bounds.len() > MAX_RANGES || self.receipts.len() > MAX_BATCHES {
            return Err(ResourceLimit);
        }
        if self.snapshot_id.is_some_and(|id| id > i64::MAX as u64)
            || self.bounds.iter().any(|b| b.owner_epoch == 0)
            || self
                .bounds
                .windows(2)
                .any(|b| b[0].stream() >= b[1].stream())
        {
            return Err(InvalidIdentity);
        }
        let mut count = 0u64;
        let mut range_count = 0usize;
        for (i, receipt) in self.receipts.iter().enumerate() {
            receipt.validate()?;
            if receipt.dataset_id != self.dataset_id
                || receipt.topology_generation > self.topology_generation
                || i > 0 && self.receipts[i - 1].publication_ordinal >= receipt.publication_ordinal
                || self.receipts[..i].iter().any(|r| {
                    r.attempt_id == receipt.attempt_id || r.batch_digest == receipt.batch_digest
                })
            {
                return Err(InvalidIdentity);
            }
            count = count
                .checked_add(receipt.record_count)
                .ok_or(InvalidIdentity)?;
            range_count = range_count
                .checked_add(receipt.input_ranges.len())
                .ok_or(ResourceLimit)?;
        }
        if max_facts == 0 || count > max_facts as u64 || range_count > MAX_RANGES {
            return Err(ResourceLimit);
        }
        // Sort only borrowed ranges: the complete inputs and receipt identities
        // remain unchanged. Publication ordinal gaps do not imply source gaps.
        let mut ranges: Vec<&crate::archive_receipt::ReceiptRange> = Vec::new();
        ranges
            .try_reserve_exact(range_count)
            .map_err(|_| ResourceLimit)?;
        ranges.extend(self.receipts.iter().flat_map(|r| &r.input_ranges));
        ranges.sort_unstable_by_key(|r| (r.shard_id, r.owner_epoch, r.first_sequence));
        let mut ranges = ranges.into_iter().peekable();
        for bound in &self.bounds {
            let mut complete = 0u64;
            while ranges
                .peek()
                .is_some_and(|r| (r.shard_id, r.owner_epoch) == bound.stream())
            {
                let range = ranges.next().expect("peeked range");
                if complete.checked_add(1) != Some(range.first_sequence) {
                    return Err(InvalidIdentity);
                }
                complete = range.last_sequence;
            }
            if complete != bound.complete_through {
                return Err(InvalidIdentity);
            }
        }
        if ranges.next().is_some() {
            return Err(InvalidIdentity);
        }
        Ok(count)
    }
    /// Private request/response binding, not the scientific SourceCut identity.
    /// Fixed-width big-endian scalars and counted ordered collections.
    pub fn digest(&self) -> Digest256 {
        let mut h = Sha256::new();
        h.update(b"bigbrotr/archive-cut-inspection/1\0");
        h.update(self.dataset_id.as_bytes());
        h.update(self.topology_generation.get().to_be_bytes());
        h.update((self.bounds.len() as u64).to_be_bytes());
        for b in &self.bounds {
            h.update(b.shard_id.to_be_bytes());
            h.update(b.owner_epoch.to_be_bytes());
            h.update(b.complete_through.to_be_bytes());
        }
        h.update((self.receipts.len() as u64).to_be_bytes());
        for r in &self.receipts {
            h.update(r.dataset_id.as_bytes());
            h.update(r.schema_version.to_be_bytes());
            h.update(r.topology_generation.get().to_be_bytes());
            h.update(r.batch_digest.as_bytes());
            h.update(r.record_count.to_be_bytes());
            h.update(r.attempt_id);
            h.update(r.publication_ordinal.to_be_bytes());
            h.update((r.input_ranges.len() as u64).to_be_bytes());
            for range in &r.input_ranges {
                h.update(range.shard_id.to_be_bytes());
                h.update(range.owner_epoch.to_be_bytes());
                h.update(range.first_sequence.to_be_bytes());
                h.update(range.last_sequence.to_be_bytes());
            }
        }
        h.update([u8::from(self.snapshot_id.is_some())]);
        if let Some(id) = self.snapshot_id {
            h.update(id.to_be_bytes());
        }
        Digest256::from_bytes(h.finalize().into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutObservation {
    pub format: String,
    pub snapshot_id: u64,
    pub request_digest: Digest256,
    pub family_counts: [u64; 5],
}
impl CutObservation {
    pub fn validate(&self, read: &CutRead, max_facts: usize) -> Result<(), ExpectationError> {
        let count = read.validate(max_facts)?;
        if self.format != FORMAT
            || self.snapshot_id > i64::MAX as u64
            || read.snapshot_id.is_some_and(|s| s != self.snapshot_id)
            || self.request_digest != read.digest()
            || self
                .family_counts
                .iter()
                .try_fold(0u64, |a, b| a.checked_add(*b))
                != Some(count)
        {
            return Err(ExpectationError::InvalidIdentity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_receipt::{ReceiptExpectation, ReceiptRange};
    use bigbrotr_model::{DatasetId, Digest256, TopologyGeneration};

    fn read() -> CutRead {
        let dataset = DatasetId::try_from([1; 16]).unwrap();
        let receipt = |ordinal, ranges: &[(u32, u64, u64)]| ReceiptExpectation {
            dataset_id: dataset,
            schema_version: 1,
            topology_generation: TopologyGeneration::new(0),
            batch_digest: Digest256::from_bytes([ordinal; 32]),
            record_count: ranges.iter().map(|(_, first, last)| last - first + 1).sum(),
            input_ranges: ranges
                .iter()
                .map(|&(shard_id, first_sequence, last_sequence)| ReceiptRange {
                    shard_id,
                    owner_epoch: 1,
                    first_sequence,
                    last_sequence,
                })
                .collect(),
            attempt_id: [ordinal; 16],
            publication_ordinal: ordinal.into(),
        };
        CutRead {
            dataset_id: dataset,
            topology_generation: TopologyGeneration::new(0),
            bounds: vec![
                CutBound {
                    shard_id: 0,
                    owner_epoch: 1,
                    complete_through: 7,
                },
                CutBound {
                    shard_id: 1,
                    owner_epoch: 1,
                    complete_through: 0,
                },
                CutBound {
                    shard_id: 2,
                    owner_epoch: 1,
                    complete_through: 2,
                },
            ],
            receipts: vec![
                receipt(2, &[(0, 1, 3)]),
                receipt(5, &[(0, 4, 7), (2, 1, 2)]),
            ],
            snapshot_id: Some(9),
        }
    }
    #[test]
    fn complete_source_prefixes_include_zero_and_allow_failed_publication_ordinals() {
        assert_eq!(read().validate(9), Ok(9));
        let mut empty = read();
        empty.receipts.clear();
        for bound in &mut empty.bounds {
            bound.complete_through = 0;
        }
        assert_eq!(empty.validate(9), Ok(0));
        empty.bounds.clear();
        assert_eq!(empty.validate(9), Ok(0));
    }
    #[test]
    fn holes_overlaps_foreign_streams_and_partial_receipts_refuse() {
        for case in [
            "gap",
            "overlap",
            "prefix-missing",
            "partial",
            "unlisted",
            "foreign",
            "order",
            "epoch",
            "attempt",
            "ordinal",
            "digest",
            "topology",
            "snapshot",
        ] {
            let mut bad = read();
            match case {
                "gap" => {
                    bad.receipts[1].input_ranges[0].first_sequence = 5;
                    bad.receipts[1].record_count -= 1;
                }
                "overlap" => {
                    bad.receipts[1].input_ranges[0].first_sequence = 3;
                    bad.receipts[1].record_count += 1;
                }
                "prefix-missing" => {
                    bad.receipts.remove(0);
                }
                "partial" => bad.bounds[0].complete_through = 6,
                "unlisted" => {
                    bad.bounds.pop();
                }
                "foreign" => bad.receipts[0].dataset_id = [9; 16].try_into().unwrap(),
                "order" => bad.bounds.swap(0, 1),
                "epoch" => bad.bounds[0].owner_epoch = 0,
                "attempt" => bad.receipts[1].attempt_id = bad.receipts[0].attempt_id,
                "ordinal" => bad.receipts[1].publication_ordinal = 1,
                "digest" => bad.receipts[1].batch_digest = bad.receipts[0].batch_digest,
                "topology" => bad.receipts[0].topology_generation = TopologyGeneration::new(1),
                "snapshot" => bad.snapshot_id = Some(i64::MAX as u64 + 1),
                _ => unreachable!(),
            }
            assert_eq!(
                bad.validate(100),
                Err(ExpectationError::InvalidIdentity),
                "{case}"
            );
        }
        assert_eq!(read().validate(8), Err(ExpectationError::ResourceLimit));
        assert_eq!(read().validate(0), Err(ExpectationError::ResourceLimit));
    }
    #[test]
    fn unsigned_maximum_is_a_valid_prefix_and_never_wraps() {
        let mut read = read();
        read.bounds.truncate(1);
        read.bounds[0].complete_through = u64::MAX;
        read.receipts.truncate(1);
        read.receipts[0].input_ranges[0].last_sequence = u64::MAX;
        read.receipts[0].record_count = u64::MAX;
        assert_eq!(read.validate(usize::MAX), Ok(u64::MAX));
        read.receipts.push(read.receipts[0].clone());
        read.receipts[1].publication_ordinal = 6;
        read.receipts[1].batch_digest = Digest256::from_bytes([6; 32]);
        read.receipts[1].attempt_id = [6; 16];
        assert_eq!(
            read.validate(usize::MAX),
            Err(ExpectationError::InvalidIdentity)
        );
    }

    #[test]
    fn literal_request_binding_and_closed_response_preserve_the_entire_cut() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/protocol/archive-cut.json"
        ))
        .unwrap();
        let read: CutRead = serde_json::from_value(vector["read"].clone()).unwrap();
        assert_eq!(read, self::read());
        assert_eq!(
            read.digest().to_string(),
            vector["request_digest"].as_str().unwrap()
        );
        let response = CutObservation {
            format: FORMAT.into(),
            snapshot_id: 9,
            request_digest: read.digest(),
            family_counts: [5, 1, 1, 1, 1],
        };
        assert_eq!(response.validate(&read, 9), Ok(()));
        let mut altered = read.clone();
        altered.bounds[1].shard_id = 0;
        assert_ne!(read.digest(), altered.digest());
        for snapshot in [None, Some(0), Some(8)] {
            altered = read.clone();
            altered.snapshot_id = snapshot;
            assert_eq!(
                response.validate(&altered, 9),
                Err(ExpectationError::InvalidIdentity)
            );
        }
        for case in ["format", "snapshot", "digest", "counts", "overflow"] {
            let mut bad = response.clone();
            match case {
                "format" => bad.format = "bigbrotr-native-membership/1".into(),
                "snapshot" => bad.snapshot_id = 8,
                "digest" => bad.request_digest = Digest256::from_bytes([8; 32]),
                "counts" => bad.family_counts[0] = 4,
                "overflow" => bad.family_counts = [u64::MAX, 10, 0, 0, 0],
                _ => unreachable!(),
            }
            assert_eq!(
                bad.validate(&read, 9),
                Err(ExpectationError::InvalidIdentity),
                "{case}"
            );
        }
        let encoded = serde_json::to_string(&response).unwrap();
        for bad in [
            format!("{encoded}{{}}"),
            format!("{},\"snapshot_id\":9}}", &encoded[..encoded.len() - 1]),
            format!("{},\"protected\":true}}", &encoded[..encoded.len() - 1]),
        ] {
            assert!(serde_json::from_str::<CutObservation>(&bad).is_err());
        }
    }
}
