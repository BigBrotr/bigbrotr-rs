use super::*;
use bigbrotr::runtime::{ProcessEnvelope, WaiterLimits};
use bigbrotr::storage::postgres::{
    IndexBinding, IndexChunk, IndexOwnerIdentity, IndexTarget, RetainedIndexPlan,
};
use bigbrotr_model::{
    AdmissionBatch, Digest256, FactRef, PolicyEpoch, RecoveryGenerationId, TopologyGeneration,
};

fn manifest() -> IngestManifest {
    IngestManifest::new(
        ManifestIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            topology_generation: TopologyGeneration::new(0),
            owner_resource_id: [2; 16].try_into().unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: [3; 16].try_into().unwrap(),
            recovery_generation: RecoveryGenerationId::from_bytes([4; 16]),
            policy_epoch: PolicyEpoch::new(1),
        },
        AllocationNamespaces::from_entries(&[(
            bigbrotr_model::DictionaryKind::VisibilityScope,
            [5; 16].try_into().unwrap(),
        )])
        .unwrap(),
    )
}
fn binding() -> IndexBinding {
    IndexBinding::new(
        [1; 16].try_into().unwrap(),
        [6; 16].try_into().unwrap(),
        [7; 16].try_into().unwrap(),
        [8; 16].try_into().unwrap(),
        Digest256::from_bytes([9; 32]),
        Digest256::from_bytes([10; 32]),
        IndexProvenance::Off,
    )
    .unwrap()
}
fn cut(through: u64) -> SourceCut {
    SourceCut::new(
        manifest().identity().dataset_id,
        TopologyGeneration::new(0),
        vec![StreamBound {
            stream: StreamId {
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
            },
            last_complete_sequence: CompleteThrough::new(through),
        }],
    )
    .unwrap()
}
fn plan(first: u64, revision: u64) -> IndexPlan {
    // Metadata fixtures only: these cannot construct a physical/recovery proof,
    // session, pending native batch or PostgreSQL authority.
    let batch = AdmissionBatch::new(
        (first..=first + 1)
            .map(|sequence| FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: manifest().identity().dataset_id,
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(sequence).unwrap(),
                },
                payload_digest: Digest256::from_bytes([sequence as u8; 32]),
            })
            .collect(),
    )
    .unwrap();
    let chunks = std::array::from_fn(|i| {
        IndexChunk::new(
            IndexTarget::ALL[i],
            1,
            "all".into(),
            Digest256::from_bytes([20 + i as u8; 32]),
            u64::from(i == 0),
            Digest256::from_bytes([40 + i as u8; 32]),
        )
        .unwrap()
    });
    IndexPlan::new(
        binding(),
        revision,
        cut(first - 1),
        cut(first + 1),
        batch,
        chunks,
    )
    .unwrap()
}
fn published() -> RetainedIndexPlan {
    let plan = plan(1, 0);
    let receipt = IndexPublicationReceipt {
        binding: binding(),
        plan_digest: plan.digest(),
        revision: 1,
        cut: cut(2),
        family_counts: std::array::from_fn(|i| plan.chunks()[i].row_count()),
        publisher: IndexOwnerIdentity {
            resource: binding().resource(),
            epoch: OwnerEpoch::new(1).unwrap(),
            incarnation: [11; 16].try_into().unwrap(),
        },
        engine_receipt_digest: Digest256::from_bytes([12; 32]),
        committed_at_us: 17,
        created: false,
    };
    RetainedIndexPlan {
        plan,
        publication: Some(receipt),
    }
}

#[test]
fn recovery_published_cursor_excludes_retained_pending_and_unplanned_backlog() {
    let head = published();
    let pending = plan(3, 1);
    let state = IndexRecoveryState {
        published_revision: 1,
        published_plan: Some(head.plan.digest()),
        pending_plan: Some(pending.digest()),
    };
    let cursor =
        publication::Published::captured(&binding(), *manifest().identity(), state, Some(&head), 6)
            .unwrap();
    assert_eq!(cursor.revision, 1);
    assert_eq!(cursor.cut, cut(2));
    assert_ne!(cursor.cut, *pending.after());
    assert_ne!(cursor.cut, cut(6));
    let empty = IndexRecoveryState {
        published_revision: 0,
        published_plan: None,
        pending_plan: Some(head.plan.digest()),
    };
    let cursor =
        publication::Published::captured(&binding(), *manifest().identity(), empty, None, 6)
            .unwrap();
    assert_eq!(
        (
            cursor.revision,
            cursor.cut.bounds()[0].last_complete_sequence.get()
        ),
        (0, 0)
    );
}

#[test]
fn recovery_published_cursor_refuses_missing_changed_and_foreign_heads() {
    let head = published();
    let state = IndexRecoveryState {
        published_revision: 1,
        published_plan: Some(head.plan.digest()),
        pending_plan: None,
    };
    let inspect = |head: Option<&RetainedIndexPlan>, through| {
        publication::Published::captured(&binding(), *manifest().identity(), state, head, through)
    };
    assert!(inspect(None, 2).is_err());
    assert!(inspect(Some(&head), 1).is_err());
    let mut changed = head.clone();
    changed.publication = None;
    assert!(inspect(Some(&changed), 2).is_err());
    changed = head.clone();
    changed.publication.as_mut().unwrap().cut = cut(4);
    assert!(inspect(Some(&changed), 4).is_err());
    changed = head.clone();
    changed.publication.as_mut().unwrap().revision = 2;
    assert!(inspect(Some(&changed), 2).is_err());
    changed = head.clone();
    changed.plan = plan(3, 1);
    assert!(inspect(Some(&changed), 4).is_err());
    let mut identity = *manifest().identity();
    identity.shard_id = 1;
    assert!(publication::Published::captured(&binding(), identity, state, Some(&head), 2).is_err());
}

#[test]
fn recovery_cursor_advances_only_for_the_exact_new_publication_receipt() {
    // This is only the local metadata cursor. These fixtures cannot construct
    // a session or authorize the PG publication that must precede acceptance.
    let head = published();
    let pending = plan(3, 1);
    let publisher = IndexOwnerIdentity {
        epoch: OwnerEpoch::new(2).unwrap(),
        incarnation: [13; 16].try_into().unwrap(),
        ..head.publication.as_ref().unwrap().publisher
    };
    let receipt = IndexPublicationReceipt {
        binding: binding(),
        plan_digest: pending.digest(),
        revision: 2,
        cut: cut(4),
        family_counts: std::array::from_fn(|i| pending.chunks()[i].row_count()),
        publisher,
        engine_receipt_digest: Digest256::from_bytes([14; 32]),
        committed_at_us: 19,
        created: true,
    };
    let initial = || publication::Published {
        revision: 1,
        cut: cut(2),
    };
    for change in 0..7 {
        let mut refused = receipt.clone();
        match change {
            0 => refused.created = false,
            1 => refused.revision = 3,
            2 => refused.cut = cut(6),
            3 => refused.plan_digest = head.plan.digest(),
            4 => refused.publisher.epoch = OwnerEpoch::new(1).unwrap(),
            5 => refused.publisher.incarnation = [15; 16].try_into().unwrap(),
            6 => refused.family_counts[0] += 1,
            _ => unreachable!(),
        }
        let mut cursor = initial();
        assert!(
            cursor
                .accept(&binding(), publisher, &pending, refused)
                .is_err()
        );
        assert_eq!((cursor.revision, cursor.cut), (1, cut(2)));
    }
    let mut cursor = initial();
    assert!(
        cursor
            .accept(&binding(), publisher, &head.plan, receipt.clone())
            .is_err()
    );
    assert_eq!((cursor.revision, cursor.cut.clone()), (1, cut(2)));
    cursor
        .accept(&binding(), publisher, &pending, receipt)
        .unwrap();
    assert_eq!((cursor.revision, cursor.cut.clone()), (2, cut(4)));
    // The genuinely new backlog remains outside this receipt.
    assert_ne!(cursor.cut, cut(6));
}

#[test]
fn finite_recovery_requires_initial_topology_and_never_selects_a_successor_writer() {
    let original = manifest();
    require_initial_manifest(&original).unwrap();
    let mut identity = *original.identity();
    identity.topology_generation = TopologyGeneration::new(1);
    assert!(
        require_initial_manifest(&IngestManifest::new(
            identity,
            *original.allocation_namespaces()
        ))
        .is_err()
    );
    identity = *original.identity();
    identity.owner_epoch = OwnerEpoch::new(2).unwrap();
    assert!(
        require_initial_manifest(&IngestManifest::for_successor(
            identity,
            *original.allocation_namespaces()
        ))
        .is_err()
    );
    let command = crate::cli::command()
        .try_get_matches_from(["bigbrotr", "index", "catch-up", "--file", "root.toml"])
        .unwrap();
    assert!(
        command
            .subcommand_matches("index")
            .unwrap()
            .subcommand_matches("catch-up")
            .is_some()
    );
    assert!(
        crate::cli::command()
            .try_get_matches_from([
                "bigbrotr",
                "index",
                "catch-up",
                "--file",
                "root.toml",
                "--initialize-index"
            ])
            .is_err()
    );
}

#[test]
fn recovery_capacity_includes_real_holders_pending_replay_and_physical_proof() {
    let profile = Profile {
        delay: Duration::from_secs(1),
        source_records: 64,
        source_bytes: MIB as usize,
        rows: 4,
        wire_bytes: 16 * MIB,
        query_memory: 256 * MIB,
        provenance: IndexProvenance::Off,
        configured: [4, 16 * MIB, 1],
    };
    for (payload, fits) in [(640 * MIB, false), (2 * 1024 * MIB, true)] {
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                payload + 256 * MIB,
                0,
                ResourceLimits::new(payload, 16, 256 * MIB, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let control = grant(&budget, 528 * MIB + MIB / 2 + 8, 0).unwrap();
        let preparation = grant(&budget, 16 * MIB, 0).unwrap();
        let metadata = grant(&budget, MIB, 0).unwrap();
        let native = grant(&budget, 0, 128 * MIB).unwrap();
        let before = budget.snapshot().used();
        assert_eq!(preflight(profile, &budget).is_ok(), fits);
        assert_eq!(budget.snapshot().used(), before);
        assert_eq!(budget.snapshot().active_permits(), 4);
        drop((control, preparation, metadata, native));
    }
}
