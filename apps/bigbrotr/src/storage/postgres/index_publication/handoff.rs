//! One explicit initial topology bridge. This metadata is not a source proof,
//! physical receipt or authority to publish; the connected owner checks all three.
use super::*;
use bigbrotr_model::{CompleteThrough, StreamBound, StreamId, TopologyGeneration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHandoff {
    binding: IndexBinding,
    expected_revision: u64,
    ingest_resource: OwnerResourceId,
    succession_digest: Digest256,
    before: SourceCut,
    after: SourceCut,
    digest: Digest256,
}
impl IndexHandoff {
    /// Structural metadata only. The predecessor's exact terminal bound and
    /// native/SQL succession identity are verified again at publication.
    pub fn new(
        binding: IndexBinding,
        expected_revision: u64,
        ingest_resource: OwnerResourceId,
        succession_digest: Digest256,
        before: SourceCut,
    ) -> Result<Self, IndexError> {
        let [old] = before.bounds() else {
            return Err(IndexError::InvalidInput);
        };
        if expected_revision == 0
            || expected_revision == u64::MAX
            || before.dataset_id() != binding.dataset()
            || before.topology_generation().get() != 0
            || old.stream.owner_epoch.get() != 1
            || succession_digest.as_bytes() == &[0; 32]
        {
            return Err(IndexError::InvalidInput);
        }
        let after = SourceCut::new(
            binding.dataset(),
            TopologyGeneration::new(1),
            vec![
                *old,
                StreamBound {
                    stream: StreamId {
                        shard_id: old.stream.shard_id,
                        owner_epoch: OwnerEpoch::new(2).unwrap(),
                    },
                    last_complete_sequence: CompleteThrough::new(0),
                },
            ],
        )
        .map_err(|_| IndexError::InvalidInput)?;
        let mut value = Self {
            binding,
            expected_revision,
            ingest_resource,
            succession_digest,
            before,
            after,
            digest: Digest256::from_bytes([0; 32]),
        };
        value.digest = value.metadata_digest();
        Ok(value)
    }
    pub fn binding(&self) -> &IndexBinding {
        &self.binding
    }
    pub fn expected_revision(&self) -> u64 {
        self.expected_revision
    }
    pub fn ingest_resource(&self) -> OwnerResourceId {
        self.ingest_resource
    }
    pub fn succession_digest(&self) -> Digest256 {
        self.succession_digest
    }
    pub fn before(&self) -> &SourceCut {
        &self.before
    }
    pub fn after(&self) -> &SourceCut {
        &self.after
    }
    pub fn digest(&self) -> Digest256 {
        self.digest
    }

    fn metadata_digest(&self) -> Digest256 {
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/index-initial-topology-handoff/1\0");
        for id in [
            self.binding.dataset.as_bytes(),
            self.binding.resource.as_bytes(),
            self.binding.generation.as_bytes(),
            self.binding.storage_incarnation.as_bytes(),
            self.ingest_resource.as_bytes(),
        ] {
            hash.update(id);
        }
        hash.update(self.binding.table_identity_digest.as_bytes());
        hash.update(self.binding.custody_binding_digest.as_bytes());
        hash.update([match self.binding.provenance {
            IndexProvenance::LocalFirstWitness => 1,
            IndexProvenance::Off => 0,
        }]);
        hash.update(self.expected_revision.to_be_bytes());
        hash.update(self.succession_digest.as_bytes());
        for cut in [&self.before, &self.after] {
            hash.update(cut.topology_generation().get().to_be_bytes());
            hash.update((cut.bounds().len() as u32).to_be_bytes());
            for bound in cut.bounds() {
                hash.update(bound.stream.shard_id.to_be_bytes());
                hash.update(bound.stream.owner_epoch.get().to_be_bytes());
                hash.update(bound.last_complete_sequence.get().to_be_bytes());
            }
        }
        Digest256::from_bytes(hash.finalize().into())
    }
}

/// A committed PG bridge has no invented ClickHouse batch or engine receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHandoffReceipt {
    pub handoff: IndexHandoff,
    pub publisher: IndexOwnerIdentity,
    pub committed_at_us: i64,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded revision stays under the retained metadata grant without another heap allocation"
)]
pub enum RetainedIndexRevision {
    Batch(RetainedIndexPlan),
    Handoff(IndexHandoffReceipt),
}

#[cfg(test)]
mod tests {
    use super::*;
    fn before(topology: u64, epoch: u64, through: u64) -> SourceCut {
        SourceCut::new(
            [1; 16].try_into().unwrap(),
            TopologyGeneration::new(topology),
            vec![StreamBound {
                stream: StreamId {
                    shard_id: 7,
                    owner_epoch: OwnerEpoch::new(epoch).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(through),
            }],
        )
        .unwrap()
    }
    fn binding() -> IndexBinding {
        IndexBinding::new(
            [1; 16].try_into().unwrap(),
            [2; 16].try_into().unwrap(),
            [3; 16].try_into().unwrap(),
            [4; 16].try_into().unwrap(),
            Digest256::from_bytes([5; 32]),
            Digest256::from_bytes([6; 32]),
            IndexProvenance::LocalFirstWitness,
        )
        .unwrap()
    }
    fn handoff(revision: u64, cut: SourceCut) -> Result<IndexHandoff, IndexError> {
        IndexHandoff::new(
            binding(),
            revision,
            [8; 16].try_into().unwrap(),
            Digest256::from_bytes([9; 32]),
            cut,
        )
    }
    #[test]
    fn handoff_has_one_literal_identity_and_preserves_closed_prefix() {
        let h = handoff(4, before(0, 1, 12)).unwrap();
        assert_eq!(h.before(), &before(0, 1, 12));
        assert_eq!(h.after().topology_generation().get(), 1);
        assert_eq!(h.after().bounds()[0], h.before().bounds()[0]);
        assert_eq!(h.after().bounds()[1].stream.owner_epoch.get(), 2);
        assert_eq!(h.after().bounds()[1].last_complete_sequence.get(), 0);
        // Independent struct.pack/hashlib vector, not an encoder-derived expectation.
        assert_eq!(
            h.digest().to_string(),
            "33b360afb63dfd64ad3502382b591a14fc804a1d22efa1b7494a3aad52961bf9"
        );
        assert_ne!(h.digest(), handoff(4, before(0, 1, 11)).unwrap().digest());
        assert_ne!(h.digest(), handoff(5, before(0, 1, 12)).unwrap().digest());
    }
    #[test]
    fn handoff_cannot_skip_topology_or_borrow_another_stream() {
        for cut in [
            before(1, 1, 12),
            before(0, 2, 12),
            SourceCut::new(binding().dataset(), TopologyGeneration::new(0), vec![]).unwrap(),
        ] {
            assert!(handoff(4, cut).is_err());
        }
        assert!(handoff(0, before(0, 1, 12)).is_err());
        assert!(handoff(u64::MAX, before(0, 1, 12)).is_err());
        assert!(
            IndexHandoff::new(
                binding(),
                4,
                [8; 16].try_into().unwrap(),
                Digest256::from_bytes([0; 32]),
                before(0, 1, 12)
            )
            .is_err()
        );
    }
    #[test]
    fn captured_heads_are_exactly_one_typed_revision() {
        let mut state = IndexRecoveryState {
            published_revision: 0,
            published_plan: None,
            published_handoff: None,
            pending_plan: None,
        };
        assert!(state.valid_head());
        state.published_revision = 5;
        assert!(!state.valid_head());
        state.published_handoff = Some(Digest256::from_bytes([1; 32]));
        assert!(state.valid_head());
        state.published_plan = Some(Digest256::from_bytes([2; 32]));
        assert!(!state.valid_head());
        state.published_handoff = None;
        assert!(state.valid_head());
        state.published_revision = 0;
        assert!(!state.valid_head());
    }
}
