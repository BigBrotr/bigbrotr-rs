//! Connected finite bridge publication and typed immutable revision reads.
use super::*;
use crate::storage::postgres::control_owner::{
    InitialHistoryRequest, SuccessorHistoryState, VerifiedSuccessorHistory,
};
use crate::storage::rocks::store::{IngestRecovery, RetainedTerminalKind};

fn source_matches(
    h: &IndexHandoff,
    proof: &VerifiedSuccessorHistory<'_>,
    native: &IngestRecovery,
) -> bool {
    let history = proof.history();
    let lineage = history.lineage();
    let (Some(old), Some(next)) = (lineage.entry(1), lineage.entry(2)) else {
        return false;
    };
    let identity = native.manifest().identity();
    history.state() == SuccessorHistoryState::Terminal
        && history.registered_epoch() == 2
        && history.native_epoch() == 2
        && proof.verified_through() == 2
        && proof.last_epoch().terminal_kind == RetainedTerminalKind::Activated
        && !lineage.has_compacted_initial()
        && lineage.pending_target().is_none()
        && lineage.entries().len() == 2
        && native.manifest().key_schema() == 2
        && identity.owner_epoch.get() == 2
        && identity.topology_generation.get() == 1
        && identity.owner_resource_id == h.ingest_resource()
        && identity.dataset_id == h.binding().dataset()
        && next.manifest() == native.manifest()
        && old.manifest().key_schema() == 1
        && old.manifest().identity().topology_generation.get() == 0
        && old.manifest().identity().owner_resource_id == identity.owner_resource_id
        && old.manifest().identity().dataset_id == identity.dataset_id
        && old.manifest().identity().shard_id == identity.shard_id
        && h.before().bounds()[0].stream.shard_id == identity.shard_id
        && h.before().bounds()[0].last_complete_sequence.get() == old.source_through()
        && next.incoming_handoff_sha256() == *h.succession_digest().as_bytes()
}

impl ControlSession {
    /// Read exactly one published revision. A missing or multiply typed entry
    /// cannot be skipped by paging only through source batch plans.
    pub async fn read_index_revision(
        &mut self,
        binding: &IndexBinding,
        revision: u64,
    ) -> Result<Option<RetainedIndexRevision>, IndexError> {
        if binding.dataset() != self.request.dataset || revision == 0 {
            return Err(IndexError::InvalidInput);
        }
        self.begin_operation().map_err(owner)?;
        let tx = application_transaction(&mut self.client, SCHEMA)
            .await
            .map_err(deployment)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(owner)?;
        binding_matches(&tx, binding).await?;
        let result = read_revision(&tx, binding, revision).await?;
        tx.rollback().await.map_err(pg)?;
        self.available = true;
        Ok(result)
    }

    /// Requires the exact full physical recovery used to acquire this Index
    /// session and the independently replayed terminal native/SQL history. The
    /// history's old observation is rechecked under current locks and policy.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_index_handoff(
        &mut self,
        session: &IndexSession,
        custody: &HeldIndexCustody,
        handoff: &IndexHandoff,
        recovered: &VerifiedIndexRecovery,
        history: &VerifiedSuccessorHistory<'_>,
        native: &IngestRecovery,
        input: InitialHistoryRequest<'_>,
        commit_started: &Cell<bool>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<IndexHandoffReceipt, IndexError> {
        valid_session(self, session, custody)?;
        let (previous, expected, recovery_digest) =
            proofs::recovery(&session.binding, custody, recovered)?;
        if handoff.binding() != &session.binding
            || !source_matches(handoff, history, native)
            || expected.published_revision != handoff.expected_revision()
            || expected.pending_plan.is_some()
            || expected.published_plan.is_none()
            || expected.published_handoff.is_some()
            || previous.epoch.get().checked_add(1) != Some(session.owner.epoch.get())
        {
            return Err(IndexError::Conflict);
        }
        history
            .validate(input.cancelled)
            .map_err(|_| IndexError::Unavailable)?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(history.history(), native, &input, validate)
            .await
            .map_err(|_| IndexError::Unavailable)?;
        require_session(&tx, session).await?;
        binding_matches(&tx, &session.binding).await?;
        if inventory(&tx, &session.binding).await? != expected {
            return Err(IndexError::Conflict);
        }
        let matching: bool = tx
            .query_one(
                "SELECT recovery_digest=$3::bytea FROM control.index_owner_sessions
            WHERE resource_id=$1::bytea AND owner_epoch=$2::text::numeric",
                &[
                    &&session.owner.resource.as_bytes()[..],
                    &session.owner.epoch.get().to_string(),
                    &&recovery_digest.as_bytes()[..],
                ],
            )
            .await
            .map_err(pg)?
            .try_get(0)
            .map_err(pg)?;
        if !matching {
            return Err(IndexError::Conflict);
        }
        let now: i64 = tx
            .query_one(NOW, &[])
            .await
            .map_err(pg)?
            .try_get(0)
            .map_err(pg)?;
        publish_handoff(&tx, session, handoff, now).await?;
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(pg)?;
        require(&tx, handoff.binding().dataset(), current)
            .await
            .map_err(owner)?;
        require_session(&tx, session).await?;
        proofs::recovery(&session.binding, custody, recovered)?;
        history
            .validate(input.cancelled)
            .map_err(|_| IndexError::Unavailable)?;
        super::super::super::initial_history::check_delivery::<()>(
            &mut lease,
            policy,
            input.cancelled,
        )
        .map_err(|_| IndexError::Unavailable)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| IndexError::CommitUnknown)?;
        history
            .validate(input.cancelled)
            .map_err(|_| IndexError::Unavailable)?;
        proofs::live(&session.binding, custody)?;
        super::super::super::initial_history::check_delivery::<()>(
            &mut lease,
            policy,
            input.cancelled,
        )
        .map_err(|_| IndexError::Unavailable)?;
        if self.client.is_closed() {
            return Err(IndexError::Unavailable);
        }
        self.available = true;
        Ok(IndexHandoffReceipt {
            handoff: handoff.clone(),
            publisher: session.owner,
            committed_at_us: now,
            created: true,
        })
    }
}

async fn read_revision(
    tx: &Transaction<'_>,
    binding: &IndexBinding,
    revision: u64,
) -> Result<Option<RetainedIndexRevision>, IndexError> {
    let row = tx.query_opt("SELECT r.sealed,p.plan_digest,h.handoff_digest FROM control.consumer_frontier_revisions r
        LEFT JOIN control.index_frontier_plans p USING(consumer_id,frontier_revision)
        LEFT JOIN control.index_frontier_handoffs h USING(consumer_id,frontier_revision)
        WHERE r.consumer_id=$1::bytea AND r.frontier_revision=$2::text::numeric AND r.consumer_role='index'",
        &[&&binding.resource().as_bytes()[..], &revision.to_string()]).await.map_err(pg)?;
    let Some(row) = row else { return Ok(None) };
    if !row.try_get::<_, bool>(0).map_err(pg)? {
        return Err(IndexError::Conflict);
    }
    match (optional_digest(&row, 1)?, optional_digest(&row, 2)?) {
        (Some(digest), None) => {
            let plan = read_plan(tx, binding, digest)
                .await?
                .ok_or(IndexError::Conflict)?;
            if plan.plan.expected_revision().checked_add(1) != Some(revision)
                || plan.publication.is_none()
            {
                return Err(IndexError::Conflict);
            }
            Ok(Some(RetainedIndexRevision::Batch(plan)))
        }
        (None, Some(digest)) => Ok(Some(RetainedIndexRevision::Handoff(
            read_handoff(tx, binding, revision, digest).await?,
        ))),
        _ => Err(IndexError::Conflict),
    }
}

async fn read_handoff(
    tx: &Transaction<'_>,
    binding: &IndexBinding,
    revision: u64,
    digest: Digest256,
) -> Result<IndexHandoffReceipt, IndexError> {
    tx.query_one(
        "SELECT control.require_index_handoff_metadata($1::bytea,$2::text::numeric)",
        &[&&binding.resource().as_bytes()[..], &revision.to_string()],
    )
    .await
    .map_err(pg)?;
    let row = tx.query_one("SELECT expected_revision::text,ingest_resource,succession_digest,
        owner_epoch::text,owner_incarnation,committed_at_us FROM control.index_frontier_handoffs
        WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric AND generation_id=$3::bytea AND handoff_digest=$4::bytea",
        &[&&binding.resource().as_bytes()[..], &revision.to_string(), &&binding.generation().as_bytes()[..], &&digest.as_bytes()[..]])
        .await.map_err(pg)?;
    let expected = number(&row, 0)?;
    let bounds = tx.query("SELECT shard_id::bigint,owner_epoch::text,last_complete_sequence::text
        FROM control.consumer_frontiers WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric
        ORDER BY shard_id,owner_epoch LIMIT 2", &[&&binding.resource().as_bytes()[..], &expected.to_string()]).await.map_err(pg)?;
    let handoff = IndexHandoff::new(
        *binding,
        expected,
        OwnerResourceId::try_from(bytes::<16>(&row, 1)?).map_err(|_| IndexError::Conflict)?,
        Digest256::from_bytes(bytes(&row, 2)?),
        cut(binding.dataset(), TopologyGeneration::new(0), &bounds)?,
    )?;
    if handoff.digest() != digest || expected.checked_add(1) != Some(revision) {
        return Err(IndexError::Conflict);
    }
    Ok(IndexHandoffReceipt {
        handoff,
        publisher: IndexOwnerIdentity {
            resource: binding.resource(),
            epoch: OwnerEpoch::new(number(&row, 3)?).map_err(|_| IndexError::Conflict)?,
            incarnation: ProcessIncarnation::try_from(bytes::<16>(&row, 4)?)
                .map_err(|_| IndexError::Conflict)?,
        },
        committed_at_us: row.try_get(5).map_err(pg)?,
        created: false,
    })
}

async fn publish_handoff(
    tx: &Transaction<'_>,
    session: &IndexSession,
    h: &IndexHandoff,
    now: i64,
) -> Result<(), IndexError> {
    let b = h.binding();
    let revision = (h.expected_revision() + 1).to_string();
    let previous = h.expected_revision().to_string();
    let shard = i64::from(h.before().bounds()[0].stream.shard_id);
    tx.execute("INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
        VALUES($1::bytea,$2::text::numeric,$3::bytea,'index',1,$4::text::numeric,$1::bytea,$5::text::numeric,$6::bytea,$7::bigint)",
        &[&&b.resource().as_bytes()[..], &revision, &&b.dataset().as_bytes()[..], &previous,
            &session.owner.epoch.get().to_string(), &&session.owner.incarnation.as_bytes()[..], &now]).await.map_err(pg)?;
    tx.execute("INSERT INTO control.index_frontier_handoffs(consumer_id,frontier_revision,expected_revision,dataset_id,
        topology_generation,generation_id,handoff_digest,ingest_resource,succession_digest,source_shard,predecessor_through,
        owner_epoch,owner_incarnation,committed_at_us) VALUES($1::bytea,$2::text::numeric,$3::text::numeric,$4::bytea,1,
        $5::bytea,$6::bytea,$7::bytea,$8::bytea,$9::bigint,$10::text::numeric,$11::text::numeric,$12::bytea,$13::bigint)",
        &[&&b.resource().as_bytes()[..], &revision, &previous, &&b.dataset().as_bytes()[..], &&b.generation().as_bytes()[..],
            &&h.digest().as_bytes()[..], &&h.ingest_resource().as_bytes()[..], &&h.succession_digest().as_bytes()[..], &shard,
            &h.before().bounds()[0].last_complete_sequence.get().to_string(), &session.owner.epoch.get().to_string(),
            &&session.owner.incarnation.as_bytes()[..], &now]).await.map_err(pg)?;
    if tx.execute("INSERT INTO control.consumer_frontiers(consumer_id,frontier_revision,dataset_id,topology_generation,
        shard_id,owner_epoch,last_complete_sequence,attestation_kind,attestation_digest)
        SELECT consumer_id,$2::text::numeric,dataset_id,1,shard_id,owner_epoch,last_complete_sequence,attestation_kind,attestation_digest
        FROM control.consumer_frontiers WHERE consumer_id=$1::bytea AND frontier_revision=$3::text::numeric",
        &[&&b.resource().as_bytes()[..], &revision, &previous]).await.map_err(pg)? != 1 { return Err(IndexError::Conflict) }
    tx.execute("INSERT INTO control.consumer_frontiers(consumer_id,frontier_revision,dataset_id,topology_generation,
        shard_id,owner_epoch,last_complete_sequence) VALUES($1::bytea,$2::text::numeric,$3::bytea,1,$4::bigint,2,0)",
        &[&&b.resource().as_bytes()[..], &revision, &&b.dataset().as_bytes()[..], &shard]).await.map_err(pg)?;
    tx.execute(
        "UPDATE control.consumer_frontier_revisions SET sealed=true WHERE consumer_id=$1::bytea
        AND frontier_revision=$2::text::numeric",
        &[&&b.resource().as_bytes()[..], &revision],
    )
    .await
    .map_err(pg)?;
    if tx.execute("UPDATE control.consumers SET current_revision=$2::text::numeric,current_topology_generation=1
        WHERE consumer_id=$1::bytea AND current_revision=$3::text::numeric AND current_topology_generation=0",
        &[&&b.resource().as_bytes()[..], &revision, &previous]).await.map_err(pg)? != 1 { return Err(IndexError::Conflict) }
    Ok(())
}
