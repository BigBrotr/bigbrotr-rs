//! PG coordination only. All physical evidence enters through proofs.rs.
use super::{proofs::*, *};
use crate::storage::rocks::store::PendingBatch;
use bigbrotr_model::{CompleteThrough, Sequence, StreamBound, StreamId, TopologyGeneration};

const SCHEMA: u32 = 41;
const NOW: &str = "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint";
fn pg(_: tokio_postgres::Error) -> IndexError {
    IndexError::Backend
}
fn owner(error: OwnerError) -> IndexError {
    match error {
        OwnerError::CommitUnknown => IndexError::CommitUnknown,
        OwnerError::IncompatibleDeployment => IndexError::IncompatibleDeployment,
        OwnerError::Backend => IndexError::Backend,
        _ => IndexError::Unavailable,
    }
}
fn deployment(_: InitializeError) -> IndexError {
    IndexError::IncompatibleDeployment
}
fn number(row: &Row, index: usize) -> Result<u64, IndexError> {
    row.try_get::<_, &str>(index)
        .map_err(pg)?
        .parse()
        .map_err(|_| IndexError::Conflict)
}
fn bytes<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], IndexError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(pg)?
        .try_into()
        .map_err(|_| IndexError::Conflict)
}
fn optional_digest(row: &Row, index: usize) -> Result<Option<Digest256>, IndexError> {
    row.try_get::<_, Option<&[u8]>>(index)
        .map_err(pg)?
        .map(|v| {
            v.try_into()
                .map(Digest256::from_bytes)
                .map_err(|_| IndexError::Conflict)
        })
        .transpose()
}
fn valid_session(
    control: &ControlSession,
    session: &IndexSession,
    custody: &HeldIndexCustody,
) -> Result<(), IndexError> {
    if session.binding.dataset != control.request.dataset || session.issuer != control.identity {
        return Err(IndexError::Conflict);
    }
    proofs::live(&session.binding, custody)
}
async fn lock_dataset(tx: &Transaction<'_>, dataset: DatasetId) -> Result<(), IndexError> {
    tx.query_opt(
        "SELECT dataset_id FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
        &[&&dataset.as_bytes()[..]],
    )
    .await
    .map_err(pg)?
    .ok_or(IndexError::Conflict)?;
    Ok(())
}
async fn require_session(tx: &Transaction<'_>, session: &IndexSession) -> Result<(), IndexError> {
    tx.query_one(
        "SELECT control.require_index_owner($1::bytea,$2::bytea,$3::text::numeric,$4::bytea)",
        &[
            &&session.binding.dataset.as_bytes()[..],
            &&session.owner.resource.as_bytes()[..],
            &session.owner.epoch.get().to_string(),
            &&session.owner.incarnation.as_bytes()[..],
        ],
    )
    .await
    .map_err(pg)?;
    Ok(())
}
async fn binding_matches(tx: &Transaction<'_>, b: &IndexBinding) -> Result<(), IndexError> {
    let row = tx
        .query_opt(
            "SELECT dataset_id=$2::bytea AND owner_resource_id=$3::bytea
        AND storage_incarnation=$4::bytea AND table_identity_digest=$5::bytea
        AND custody_binding_digest=$6::bytea AND provenance=$7 AND index_publication_protocol=1
        FROM control.projection_generations WHERE generation_id=$1::bytea",
            &[
                &&b.generation.as_bytes()[..],
                &&b.dataset.as_bytes()[..],
                &&b.resource.as_bytes()[..],
                &&b.storage_incarnation.as_bytes()[..],
                &&b.table_identity_digest.as_bytes()[..],
                &&b.custody_binding_digest.as_bytes()[..],
                &b.provenance.as_str(),
            ],
        )
        .await
        .map_err(pg)?;
    if row
        .ok_or(IndexError::Conflict)?
        .try_get::<_, Option<bool>>(0)
        .map_err(pg)?
        != Some(true)
    {
        return Err(IndexError::Conflict);
    }
    Ok(())
}
async fn inventory(
    tx: &Transaction<'_>,
    b: &IndexBinding,
) -> Result<IndexRecoveryState, IndexError> {
    let row = tx.query_one("SELECT coalesce(c.current_revision,0)::text,
        (SELECT plan_digest FROM control.index_frontier_plans f WHERE f.consumer_id=c.consumer_id AND f.frontier_revision=c.current_revision),
        (SELECT plan_digest FROM control.projection_plans p WHERE p.generation_id=$2::bytea AND p.frontier_revision=coalesce(c.current_revision,0)+1)
        FROM control.consumers c WHERE c.consumer_id=$1::bytea AND c.consumer_role='index' FOR UPDATE OF c",
        &[&&b.resource.as_bytes()[..],&&b.generation.as_bytes()[..]]).await.map_err(pg)?;
    let state = IndexRecoveryState {
        published_revision: number(&row, 0)?,
        published_plan: optional_digest(&row, 1)?,
        pending_plan: optional_digest(&row, 2)?,
    };
    if (state.published_revision == 0) != state.published_plan.is_none() {
        return Err(IndexError::Conflict);
    }
    Ok(state)
}

impl ControlSession {
    /// Fresh installation only. A retained generation always takes recovery,
    /// including after a lost COMMIT. The caller owns and joins the PG driver.
    pub async fn claim_initial_index(
        &mut self,
        binding: &IndexBinding,
        custody: &HeldIndexCustody,
        proof: &VerifiedInstallation,
        commit_started: &Cell<bool>,
    ) -> Result<IndexSession, IndexError> {
        if binding.dataset != self.request.dataset {
            return Err(IndexError::InvalidInput);
        }
        proofs::installation(binding, custody, proof)?;
        self.begin_operation().map_err(owner)?;
        let tx = application_transaction(&mut self.client, SCHEMA)
            .await
            .map_err(deployment)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(owner)?;
        lock_dataset(&tx, binding.dataset).await?;
        if tx
            .query_opt(
                "SELECT resource_id FROM control.owner_epochs WHERE resource_id=$1::bytea LIMIT 1",
                &[&&binding.resource.as_bytes()[..]],
            )
            .await
            .map_err(pg)?
            .is_some()
        {
            return Err(IndexError::Conflict);
        }
        let identity = IndexOwnerIdentity {
            resource: binding.resource,
            epoch: OwnerEpoch::new(1).map_err(|_| IndexError::InvalidInput)?,
            incarnation: self.identity.process_incarnation,
        };
        insert_owner(&tx, binding, identity, self.identity).await?;
        tx.execute("INSERT INTO control.projection_generations(generation_id,dataset_id,storage_incarnation,
            table_identity_digest,owner_resource_id,owner_epoch,owner_incarnation,created_at_us,
            index_publication_protocol,custody_binding_digest,provenance)
            VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,1,$6::bytea,
                (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,1,$7::bytea,$8)",
            &[&&binding.generation.as_bytes()[..],&&binding.dataset.as_bytes()[..],
                &&binding.storage_incarnation.as_bytes()[..],&&binding.table_identity_digest.as_bytes()[..],
                &&binding.resource.as_bytes()[..],&&identity.incarnation.as_bytes()[..],
                &&binding.custody_binding_digest.as_bytes()[..],&binding.provenance.as_str()]).await.map_err(pg)?;
        tx.execute("INSERT INTO control.consumers(consumer_id,dataset_id,consumer_role,created_at_us)
            VALUES($1::bytea,$2::bytea,'index',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)",
            &[&&binding.resource.as_bytes()[..],&&binding.dataset.as_bytes()[..]]).await.map_err(pg)?;
        let session = IndexSession {
            binding: *binding,
            owner: identity,
            issuer: self.identity,
        };
        insert_session(
            &tx,
            &session,
            None,
            IndexRecoveryState {
                published_revision: 0,
                published_plan: None,
                pending_plan: None,
            },
        )
        .await?;
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(pg)?;
        require_session(&tx, &session).await?;
        proofs::installation(binding, custody, proof)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| IndexError::CommitUnknown)?;
        proofs::live(binding, custody)?;
        if self.client.is_closed() {
            return Err(IndexError::Unavailable);
        }
        self.available = true;
        Ok(session)
    }

    /// Rebind retained plans only after positive stable-custody exclusion and
    /// verification of the full published CH prefix. Lease expiry is not proof.
    pub async fn recover_index(
        &mut self,
        binding: &IndexBinding,
        custody: &HeldIndexCustody,
        proof: &VerifiedIndexRecovery,
        commit_started: &Cell<bool>,
    ) -> Result<IndexSession, IndexError> {
        if binding.dataset != self.request.dataset {
            return Err(IndexError::InvalidInput);
        }
        let (previous, expected, digest) = proofs::recovery(binding, custody, proof)?;
        self.begin_operation().map_err(owner)?;
        let tx = application_transaction(&mut self.client, SCHEMA)
            .await
            .map_err(deployment)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(owner)?;
        lock_dataset(&tx, binding.dataset).await?;
        let row = tx.query_opt("SELECT epoch::text,process_incarnation,role_instance_id,state FROM control.owner_epochs
            WHERE resource_id=$1::bytea AND dataset_id=$2::bytea AND role='index'
            ORDER BY epoch DESC LIMIT 1 FOR UPDATE",
            &[&&binding.resource.as_bytes()[..],&&binding.dataset.as_bytes()[..]]).await.map_err(pg)?.ok_or(IndexError::Conflict)?;
        if number(&row, 0)? != previous.epoch.get()
            || bytes::<16>(&row, 1)? != *previous.incarnation.as_bytes()
        {
            return Err(IndexError::Conflict);
        }
        binding_matches(&tx, binding).await?;
        if inventory(&tx, binding).await? != expected {
            return Err(IndexError::Conflict);
        }
        // Attribution cannot be rewritten even when the old chunk creator is
        // several sessions behind. Only the current session is transferred.
        if row.try_get::<_, &str>(3).map_err(pg)? != "released" {
            tx.execute(
                "UPDATE control.owner_epochs SET state='released',
                released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                exclusion_method='storage_fenced',exclusion_digest=$3::bytea
                WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
                &[
                    &&binding.resource.as_bytes()[..],
                    &previous.epoch.get().to_string(),
                    &&digest.as_bytes()[..],
                ],
            )
            .await
            .map_err(pg)?;
        }
        tx.execute("UPDATE control.role_instances SET retired_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
            WHERE role_instance_id=$1::bytea AND retired_at_us IS NULL", &[&&bytes::<16>(&row,2)?[..]]).await.map_err(pg)?;
        let identity = IndexOwnerIdentity {
            resource: binding.resource,
            epoch: OwnerEpoch::new(
                previous
                    .epoch
                    .get()
                    .checked_add(1)
                    .ok_or(IndexError::Exhausted)?,
            )
            .map_err(|_| IndexError::Exhausted)?,
            incarnation: self.identity.process_incarnation,
        };
        insert_owner(&tx, binding, identity, self.identity).await?;
        let session = IndexSession {
            binding: *binding,
            owner: identity,
            issuer: self.identity,
        };
        insert_session(
            &tx,
            &session,
            Some((previous.epoch.get(), digest)),
            expected,
        )
        .await?;
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(pg)?;
        require_session(&tx, &session).await?;
        proofs::recovery(binding, custody, proof)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| IndexError::CommitUnknown)?;
        proofs::live(binding, custody)?;
        if self.client.is_closed() {
            return Err(IndexError::Unavailable);
        }
        self.available = true;
        Ok(session)
    }

    /// Persist the complete deterministic plan before dispatch. `pending` holds
    /// the actual bounded Rocks read and reservation, not caller-made FactRefs.
    pub async fn prepare_index_plan(
        &mut self,
        session: &IndexSession,
        custody: &HeldIndexCustody,
        plan: &IndexPlan,
        pending: &PendingBatch,
        commit_started: &Cell<bool>,
    ) -> Result<PreparedIndexPlan, IndexError> {
        valid_session(self, session, custody)?;
        if plan.binding != session.binding
            || plan.source != IndexSourceBatch::from_batch(pending.batch())
        {
            return Err(IndexError::Conflict);
        }
        self.begin_operation().map_err(owner)?;
        let tx = application_transaction(&mut self.client, SCHEMA)
            .await
            .map_err(deployment)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(owner)?;
        lock_dataset(&tx, plan.binding.dataset).await?;
        require_session(&tx, session).await?;
        binding_matches(&tx, &plan.binding).await?;
        if let Some(retained) = read_plan(&tx, &plan.binding, plan.digest).await? {
            if retained.plan != *plan {
                return Err(IndexError::Conflict);
            }
            require_session(&tx, session).await?;
            proofs::live(&plan.binding, custody)?;
            tx.rollback().await.map_err(pg)?;
            self.available = true;
            return Ok(PreparedIndexPlan { plan: plan.clone() });
        }
        let state = inventory(&tx, &plan.binding).await?;
        if state.published_revision != plan.expected_revision || state.pending_plan.is_some() {
            return Err(IndexError::Conflict);
        }
        let now: i64 = tx
            .query_one(NOW, &[])
            .await
            .map_err(pg)?
            .try_get(0)
            .map_err(pg)?;
        insert_plan(&tx, session, plan, now).await?;
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(pg)?;
        require_session(&tx, session).await?;
        proofs::live(&plan.binding, custody)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| IndexError::CommitUnknown)?;
        proofs::live(&plan.binding, custody)?;
        if self.client.is_closed() {
            return Err(IndexError::Unavailable);
        }
        self.available = true;
        Ok(PreparedIndexPlan { plan: plan.clone() })
    }

    /// Publish all target completions and the next contiguous frontier in one
    /// transaction. Unknown CH work never reaches this method without proof.
    pub async fn publish_index_plan(
        &mut self,
        session: &IndexSession,
        custody: &HeldIndexCustody,
        prepared: &PreparedIndexPlan,
        proof: &VerifiedIndexPlan,
        commit_started: &Cell<bool>,
    ) -> Result<IndexPublicationReceipt, IndexError> {
        valid_session(self, session, custody)?;
        let plan = &prepared.plan;
        if plan.binding != session.binding {
            return Err(IndexError::Conflict);
        }
        let engine = proofs::publication(plan, custody, proof)?;
        self.begin_operation().map_err(owner)?;
        let tx = application_transaction(&mut self.client, SCHEMA)
            .await
            .map_err(deployment)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(owner)?;
        lock_dataset(&tx, plan.binding.dataset).await?;
        require_session(&tx, session).await?;
        binding_matches(&tx, &plan.binding).await?;
        let retained = read_plan(&tx, &plan.binding, plan.digest)
            .await?
            .ok_or(IndexError::Conflict)?;
        if retained.plan != *plan {
            return Err(IndexError::Conflict);
        }
        if let Some(receipt) = retained.publication {
            // Preserve the original publisher/time/receipt, not the new proof's
            // observation time. A historical read grants no physical readiness.
            require_session(&tx, session).await?;
            proofs::live(&plan.binding, custody)?;
            tx.rollback().await.map_err(pg)?;
            self.available = true;
            return Ok(receipt);
        }
        let state = inventory(&tx, &plan.binding).await?;
        if state.published_revision != plan.expected_revision
            || state.pending_plan != Some(plan.digest)
        {
            return Err(IndexError::Conflict);
        }
        let now: i64 = tx
            .query_one(NOW, &[])
            .await
            .map_err(pg)?
            .try_get(0)
            .map_err(pg)?;
        publish(&tx, session, plan, engine, now).await?;
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(pg)?;
        require_session(&tx, session).await?;
        proofs::publication(plan, custody, proof)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| IndexError::CommitUnknown)?;
        proofs::live(&plan.binding, custody)?;
        if self.client.is_closed() {
            return Err(IndexError::Unavailable);
        }
        self.available = true;
        Ok(IndexPublicationReceipt {
            binding: plan.binding,
            plan_digest: plan.digest,
            revision: plan.expected_revision + 1,
            cut: plan.after.clone(),
            family_counts: std::array::from_fn(|i| plan.chunks[i].row_count),
            publisher: session.owner,
            engine_receipt_digest: engine,
            committed_at_us: now,
            created: true,
        })
    }

    /// Scheduling renewal only; the runtime independently owns its policy lease
    /// and native operation grants. Refresh Control first. No expired deadline
    /// or released owner is revived, including after a blocking lock wait.
    pub async fn renew_index(
        &mut self,
        session: &IndexSession,
        custody: &HeldIndexCustody,
        commit_started: &Cell<bool>,
    ) -> Result<(), IndexError> {
        valid_session(self, session, custody)?;
        self.begin_operation().map_err(owner)?;
        let tx = application_transaction(&mut self.client, SCHEMA)
            .await
            .map_err(deployment)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(owner)?;
        lock_dataset(&tx, session.binding.dataset).await?;
        tx.query_one("SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric FOR UPDATE",
            &[&&session.owner.resource.as_bytes()[..],&session.owner.epoch.get().to_string()]).await.map_err(pg)?;
        require_session(&tx, session).await?;
        if tx.execute("WITH observed AS MATERIALIZED (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS now_us)
            UPDATE control.owner_epochs i SET lease_expires_at_us=c.lease_expires_at_us
            FROM control.owner_epochs c,observed WHERE i.resource_id=$1::bytea AND i.epoch=$2::text::numeric
                AND c.resource_id=$3::bytea AND c.epoch=$4::text::numeric
                AND i.lease_expires_at_us>observed.now_us AND c.lease_expires_at_us>observed.now_us",
            &[&&session.owner.resource.as_bytes()[..],&session.owner.epoch.get().to_string(),
                &&session.issuer.resource_id.as_bytes()[..],&session.issuer.epoch.get().to_string()]).await.map_err(pg)? != 1 {
            return Err(IndexError::Unavailable);
        }
        require_session(&tx, session).await?;
        proofs::live(&session.binding, custody)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| IndexError::CommitUnknown)?;
        proofs::live(&session.binding, custody)?;
        if self.client.is_closed() {
            return Err(IndexError::Unavailable);
        }
        self.available = true;
        Ok(())
    }

    /// Exact historical metadata resolution, never a new native capability.
    pub async fn read_index_plan(
        &mut self,
        binding: &IndexBinding,
        digest: Digest256,
    ) -> Result<Option<RetainedIndexPlan>, IndexError> {
        if binding.dataset != self.request.dataset {
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
        let result = read_plan(&tx, binding, digest).await?;
        tx.rollback().await.map_err(pg)?;
        self.available = true;
        Ok(result)
    }

    /// Caller reserves the bounded page and streams the immutable prefix. Empty
    /// page means no further retained plan, not that CH content is absent.
    pub async fn read_index_plans(
        &mut self,
        binding: &IndexBinding,
        after_revision: u64,
        limit: u8,
    ) -> Result<Vec<RetainedIndexPlan>, IndexError> {
        if binding.dataset != self.request.dataset || !(1..=64).contains(&limit) {
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
        let rows = tx
            .query(
                "SELECT plan_digest FROM control.projection_plans WHERE generation_id=$1::bytea
            AND frontier_revision>$2::text::numeric ORDER BY frontier_revision LIMIT $3",
                &[
                    &&binding.generation.as_bytes()[..],
                    &after_revision.to_string(),
                    &i64::from(limit),
                ],
            )
            .await
            .map_err(pg)?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            result.push(
                read_plan(&tx, binding, Digest256::from_bytes(bytes(&row, 0)?))
                    .await?
                    .ok_or(IndexError::Conflict)?,
            );
        }
        tx.rollback().await.map_err(pg)?;
        self.available = true;
        Ok(result)
    }

    pub async fn read_index_recovery_state(
        &mut self,
        binding: &IndexBinding,
    ) -> Result<(IndexOwnerIdentity, IndexRecoveryState), IndexError> {
        if binding.dataset != self.request.dataset {
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
        let row = tx.query_one("SELECT owner_epoch::text,owner_incarnation FROM control.index_owner_sessions
            WHERE resource_id=$1::bytea AND generation_id=$2::bytea ORDER BY owner_epoch DESC LIMIT 1",
            &[&&binding.resource.as_bytes()[..],&&binding.generation.as_bytes()[..]]).await.map_err(pg)?;
        let previous = IndexOwnerIdentity {
            resource: binding.resource,
            epoch: OwnerEpoch::new(number(&row, 0)?).map_err(|_| IndexError::Conflict)?,
            incarnation: ProcessIncarnation::try_from(bytes::<16>(&row, 1)?)
                .map_err(|_| IndexError::Conflict)?,
        };
        let state = inventory(&tx, binding).await?;
        tx.rollback().await.map_err(pg)?;
        self.available = true;
        Ok((previous, state))
    }
}

async fn insert_owner(
    tx: &Transaction<'_>,
    binding: &IndexBinding,
    identity: IndexOwnerIdentity,
    issuer: ingest_registry::ControlOwner,
) -> Result<(), IndexError> {
    let role = fresh_id().map_err(owner)?;
    tx.execute(
        "INSERT INTO control.role_instances VALUES($1::bytea,$2::bytea,'index',
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,NULL)",
        &[&&role[..], &&binding.dataset.as_bytes()[..]],
    )
    .await
    .map_err(pg)?;
    if tx.execute("INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
        process_incarnation,acquired_at_us,lease_expires_at_us,state,index_publication_protocol)
        SELECT $1::bytea,$2::text::numeric,$3::bytea,'index',$4::bytea,$5::bytea,
            (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,lease_expires_at_us,'active',1
        FROM control.owner_epochs WHERE resource_id=$6::bytea AND epoch=$7::text::numeric
            AND state='active' AND lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint",
        &[&&binding.resource.as_bytes()[..],&identity.epoch.get().to_string(),&&binding.dataset.as_bytes()[..],
            &&role[..],&&identity.incarnation.as_bytes()[..],&&issuer.resource_id.as_bytes()[..],&issuer.epoch.get().to_string()])
        .await.map_err(pg)? != 1 { return Err(IndexError::Unavailable); }
    Ok(())
}

async fn insert_session(
    tx: &Transaction<'_>,
    session: &IndexSession,
    recovery: Option<(u64, Digest256)>,
    state: IndexRecoveryState,
) -> Result<(), IndexError> {
    let previous = recovery.map(|v| v.0.to_string());
    let digest = recovery.map(|v| v.1.as_bytes().to_vec());
    let published = state.published_plan.map(|v| v.as_bytes().to_vec());
    let pending = state.pending_plan.map(|v| v.as_bytes().to_vec());
    tx.execute("INSERT INTO control.index_owner_sessions(dataset_id,resource_id,owner_epoch,owner_incarnation,
        generation_id,issuer_resource,issuer_epoch,issuer_incarnation,previous_epoch,recovery_digest,
        recovered_revision,recovered_plan,recovered_pending_plan)
        VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::bytea,$5::bytea,$6::bytea,$7::text::numeric,$8::bytea,
            $9::text::numeric,$10::bytea,$11::text::numeric,$12::bytea,$13::bytea)",
        &[&&session.binding.dataset.as_bytes()[..],&&session.owner.resource.as_bytes()[..],
            &session.owner.epoch.get().to_string(),&&session.owner.incarnation.as_bytes()[..],
            &&session.binding.generation.as_bytes()[..],&&session.issuer.resource_id.as_bytes()[..],
            &session.issuer.epoch.get().to_string(),&&session.issuer.process_incarnation.as_bytes()[..],
            &previous,&digest,&state.published_revision.to_string(),&published,&pending]).await.map_err(pg)?;
    Ok(())
}

async fn insert_plan(
    tx: &Transaction<'_>,
    session: &IndexSession,
    p: &IndexPlan,
    now: i64,
) -> Result<(), IndexError> {
    let b = &p.binding;
    let revision = (p.expected_revision + 1).to_string();
    let topology = p.after.topology_generation().get().to_string();
    tx.execute(
        "INSERT INTO control.projection_plans(generation_id,plan_digest,dataset_id,consumer_id,
        frontier_revision,expected_revision,topology_generation,source_shard,source_owner_epoch,
        source_first,source_last,source_batch_digest,origin_epoch,origin_incarnation,prepared_at_us)
        VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::text::numeric,$6::text::numeric,
            $7::text::numeric,$8::bigint,$9::text::numeric,$10::text::numeric,$11::text::numeric,
            $12::bytea,$13::text::numeric,$14::bytea,$15::bigint)",
        &[
            &&b.generation.as_bytes()[..],
            &&p.digest.as_bytes()[..],
            &&b.dataset.as_bytes()[..],
            &&b.resource.as_bytes()[..],
            &revision,
            &p.expected_revision.to_string(),
            &topology,
            &i64::from(p.source.first().shard_id),
            &p.source.first().owner_epoch.get().to_string(),
            &p.source.first().sequence.get().to_string(),
            &p.source.last().sequence.get().to_string(),
            &&p.source.digest().as_bytes()[..],
            &session.owner.epoch.get().to_string(),
            &&session.owner.incarnation.as_bytes()[..],
            &now,
        ],
    )
    .await
    .map_err(pg)?;
    for chunk in &p.chunks {
        tx.execute("INSERT INTO control.projection_chunks(generation_id,chunk_id,dataset_id,storage_incarnation,
            table_identity_digest,target,target_schema_version,topology_generation,chunk_ordinal,row_count,
            owner_resource_id,owner_epoch,owner_incarnation,prepared_at_us)
            VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,$6,1,$7::text::numeric,
                $8::text::numeric,$9::text::numeric,$10::bytea,$11::text::numeric,$12::bytea,$13::bigint)",
            &[&&b.generation.as_bytes()[..],&&chunk.chunk_id.as_bytes()[..],&&b.dataset.as_bytes()[..],
                &&b.storage_incarnation.as_bytes()[..],&&b.table_identity_digest.as_bytes()[..],
                &chunk.target.as_str(),&topology,&revision,&chunk.row_count.to_string(),
                &&b.resource.as_bytes()[..],&session.owner.epoch.get().to_string(),
                &&session.owner.incarnation.as_bytes()[..],&now]).await.map_err(pg)?;
        tx.execute("INSERT INTO control.projection_plan_chunks(generation_id,plan_digest,target,chunk_id,partition_key,rows_digest)
            VALUES($1::bytea,$2::bytea,$3,$4::bytea,$5,$6::bytea)",
            &[&&b.generation.as_bytes()[..],&&p.digest.as_bytes()[..],&chunk.target.as_str(),
                &&chunk.chunk_id.as_bytes()[..],&chunk.partition,&&chunk.rows_digest.as_bytes()[..]]).await.map_err(pg)?;
        for bound in p.after.bounds() {
            tx.execute("INSERT INTO control.projection_chunk_bounds(generation_id,chunk_id,dataset_id,
                topology_generation,shard_id,owner_epoch,last_complete_sequence)
                VALUES($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::bigint,$6::text::numeric,$7::text::numeric)",
                &[&&b.generation.as_bytes()[..],&&chunk.chunk_id.as_bytes()[..],&&b.dataset.as_bytes()[..],
                    &topology,&i64::from(bound.stream.shard_id),&bound.stream.owner_epoch.get().to_string(),
                    &bound.last_complete_sequence.get().to_string()]).await.map_err(pg)?;
        }
    }
    tx.execute("UPDATE control.projection_plans SET sealed=true WHERE generation_id=$1::bytea AND plan_digest=$2::bytea",
        &[&&b.generation.as_bytes()[..],&&p.digest.as_bytes()[..]]).await.map_err(pg)?;
    Ok(())
}

async fn publish(
    tx: &Transaction<'_>,
    session: &IndexSession,
    p: &IndexPlan,
    engine: Digest256,
    now: i64,
) -> Result<(), IndexError> {
    let b = &p.binding;
    let revision = (p.expected_revision + 1).to_string();
    let previous = (p.expected_revision != 0).then(|| p.expected_revision.to_string());
    let topology = p.after.topology_generation().get().to_string();
    if tx
        .execute(
            "UPDATE control.projection_chunks c SET state='committed',committed_at_us=$3::bigint,
        engine_receipt_digest=$4::bytea FROM control.projection_plan_chunks m
        WHERE c.generation_id=m.generation_id AND c.chunk_id=m.chunk_id
            AND m.generation_id=$1::bytea AND m.plan_digest=$2::bytea AND c.state='prepared'",
            &[
                &&b.generation.as_bytes()[..],
                &&p.digest.as_bytes()[..],
                &now,
                &&engine.as_bytes()[..],
            ],
        )
        .await
        .map_err(pg)?
        != INDEX_TARGET_COUNT as u64
    {
        return Err(IndexError::Conflict);
    }
    tx.execute("INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,
        consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
        VALUES($1::bytea,$2::text::numeric,$3::bytea,'index',$4::text::numeric,$5::text::numeric,
            $1::bytea,$6::text::numeric,$7::bytea,$8::bigint)",
        &[&&b.resource.as_bytes()[..],&revision,&&b.dataset.as_bytes()[..],&topology,&previous,
            &session.owner.epoch.get().to_string(),&&session.owner.incarnation.as_bytes()[..],&now]).await.map_err(pg)?;
    tx.execute("INSERT INTO control.index_frontier_plans(consumer_id,frontier_revision,dataset_id,topology_generation,
        generation_id,plan_digest,owner_epoch,owner_incarnation,engine_receipt_digest,committed_at_us)
        VALUES($1::bytea,$2::text::numeric,$3::bytea,$4::text::numeric,$5::bytea,$6::bytea,
            $7::text::numeric,$8::bytea,$9::bytea,$10::bigint)",
        &[&&b.resource.as_bytes()[..],&revision,&&b.dataset.as_bytes()[..],&topology,
            &&b.generation.as_bytes()[..],&&p.digest.as_bytes()[..],&session.owner.epoch.get().to_string(),
            &&session.owner.incarnation.as_bytes()[..],&&engine.as_bytes()[..],&now]).await.map_err(pg)?;
    for bound in p.after.bounds() {
        let nonzero = bound.last_complete_sequence.get() != 0;
        let attestation = nonzero.then_some("projection-chunk");
        let digest = nonzero.then(|| p.digest.as_bytes().to_vec());
        tx.execute("INSERT INTO control.consumer_frontiers(consumer_id,frontier_revision,dataset_id,topology_generation,
            shard_id,owner_epoch,last_complete_sequence,attestation_kind,attestation_digest)
            VALUES($1::bytea,$2::text::numeric,$3::bytea,$4::text::numeric,$5::bigint,$6::text::numeric,
                $7::text::numeric,$8,$9::bytea)",
            &[&&b.resource.as_bytes()[..],&revision,&&b.dataset.as_bytes()[..],&topology,
                &i64::from(bound.stream.shard_id),&bound.stream.owner_epoch.get().to_string(),
                &bound.last_complete_sequence.get().to_string(),&attestation,&digest]).await.map_err(pg)?;
    }
    tx.execute(
        "UPDATE control.consumer_frontier_revisions SET sealed=true WHERE consumer_id=$1::bytea
        AND frontier_revision=$2::text::numeric",
        &[&&b.resource.as_bytes()[..], &revision],
    )
    .await
    .map_err(pg)?;
    if tx.execute("UPDATE control.consumers SET current_revision=$2::text::numeric,current_topology_generation=$3::text::numeric
        WHERE consumer_id=$1::bytea AND current_revision IS NOT DISTINCT FROM $4::text::numeric",
        &[&&b.resource.as_bytes()[..],&revision,&topology,&previous]).await.map_err(pg)? != 1 {
        return Err(IndexError::Conflict);
    }
    Ok(())
}

async fn read_plan(
    tx: &Transaction<'_>,
    binding: &IndexBinding,
    digest: Digest256,
) -> Result<Option<RetainedIndexPlan>, IndexError> {
    let Some(row) = tx.query_opt("SELECT expected_revision::text,topology_generation::text,
        source_shard::bigint,source_owner_epoch::text,source_first::text,source_last::text,
        source_batch_digest,sealed FROM control.projection_plans WHERE generation_id=$1::bytea AND plan_digest=$2::bytea",
        &[&&binding.generation.as_bytes()[..],&&digest.as_bytes()[..]]).await.map_err(pg)? else { return Ok(None); };
    if !row.try_get::<_, bool>(7).map_err(pg)? {
        return Err(IndexError::Conflict);
    }
    let expected_revision = number(&row, 0)?;
    let topology = TopologyGeneration::new(number(&row, 1)?);
    let first = SourceRecordId {
        dataset_id: binding.dataset,
        shard_id: u32::try_from(row.try_get::<_, i64>(2).map_err(pg)?)
            .map_err(|_| IndexError::Conflict)?,
        owner_epoch: OwnerEpoch::new(number(&row, 3)?).map_err(|_| IndexError::Conflict)?,
        sequence: Sequence::new(number(&row, 4)?).map_err(|_| IndexError::Conflict)?,
    };
    let source = IndexSourceBatch {
        first,
        last: SourceRecordId {
            sequence: Sequence::new(number(&row, 5)?).map_err(|_| IndexError::Conflict)?,
            ..first
        },
        digest: Digest256::from_bytes(bytes(&row, 6)?),
    };
    let rows = tx.query("SELECT m.target,c.target_schema_version,m.partition_key,m.chunk_id,c.row_count::text,m.rows_digest
        FROM control.projection_plan_chunks m JOIN control.projection_chunks c USING(generation_id,chunk_id)
        WHERE m.generation_id=$1::bytea AND m.plan_digest=$2::bytea LIMIT 11",
        &[&&binding.generation.as_bytes()[..],&&digest.as_bytes()[..]]).await.map_err(pg)?;
    if rows.len() != INDEX_TARGET_COUNT {
        return Err(IndexError::Conflict);
    }
    let mut chunks = Vec::with_capacity(INDEX_TARGET_COUNT);
    for target in IndexTarget::ALL {
        let row = rows
            .iter()
            .find(|r| r.try_get::<_, &str>(0).ok() == Some(target.as_str()))
            .ok_or(IndexError::Conflict)?;
        chunks.push(IndexChunk::new(
            target,
            u16::try_from(row.try_get::<_, i32>(1).map_err(pg)?)
                .map_err(|_| IndexError::Conflict)?,
            row.try_get::<_, String>(2).map_err(pg)?,
            Digest256::from_bytes(bytes(row, 3)?),
            number(row, 4)?,
            Digest256::from_bytes(bytes(row, 5)?),
        )?);
    }
    let rows = tx.query("SELECT b.shard_id::bigint,b.owner_epoch::text,b.last_complete_sequence::text
        FROM control.projection_plan_chunks m JOIN control.projection_chunk_bounds b USING(generation_id,chunk_id)
        WHERE m.generation_id=$1::bytea AND m.plan_digest=$2::bytea AND m.target='dictionaries'
        ORDER BY b.shard_id,b.owner_epoch LIMIT 1025",
        &[&&binding.generation.as_bytes()[..],&&digest.as_bytes()[..]]).await.map_err(pg)?;
    let after = cut(binding.dataset, topology, &rows)?;
    let before = if expected_revision == 0 {
        SourceCut::new(
            binding.dataset,
            topology,
            after
                .bounds()
                .iter()
                .map(|b| StreamBound {
                    stream: b.stream,
                    last_complete_sequence: CompleteThrough::new(0),
                })
                .collect(),
        )
        .map_err(|_| IndexError::Conflict)?
    } else {
        let rows = tx.query("SELECT shard_id::bigint,owner_epoch::text,last_complete_sequence::text FROM control.consumer_frontiers
            WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric ORDER BY shard_id,owner_epoch LIMIT 1025",
            &[&&binding.resource.as_bytes()[..],&expected_revision.to_string()]).await.map_err(pg)?;
        cut(binding.dataset, topology, &rows)?
    };
    let plan = IndexPlan::from_metadata(
        *binding,
        expected_revision,
        before,
        after,
        source,
        chunks.try_into().map_err(|_| IndexError::Conflict)?,
    )?;
    if plan.digest != digest {
        return Err(IndexError::Conflict);
    }
    let publication = tx
        .query_opt(
            "SELECT owner_epoch::text,owner_incarnation,engine_receipt_digest,committed_at_us
        FROM control.index_frontier_plans WHERE generation_id=$1::bytea AND plan_digest=$2::bytea",
            &[&&binding.generation.as_bytes()[..], &&digest.as_bytes()[..]],
        )
        .await
        .map_err(pg)?
        .map(|row| {
            Ok(IndexPublicationReceipt {
                binding: *binding,
                plan_digest: digest,
                revision: expected_revision + 1,
                cut: plan.after.clone(),
                family_counts: std::array::from_fn(|i| plan.chunks[i].row_count),
                publisher: IndexOwnerIdentity {
                    resource: binding.resource,
                    epoch: OwnerEpoch::new(number(&row, 0)?).map_err(|_| IndexError::Conflict)?,
                    incarnation: ProcessIncarnation::try_from(bytes::<16>(&row, 1)?)
                        .map_err(|_| IndexError::Conflict)?,
                },
                engine_receipt_digest: Digest256::from_bytes(bytes(&row, 2)?),
                committed_at_us: row.try_get(3).map_err(pg)?,
                created: false,
            })
        })
        .transpose()?;
    Ok(Some(RetainedIndexPlan { plan, publication }))
}

fn cut(
    dataset: DatasetId,
    topology: TopologyGeneration,
    rows: &[Row],
) -> Result<SourceCut, IndexError> {
    if rows.is_empty() || rows.len() > MAX_INDEX_STREAMS {
        return Err(IndexError::Conflict);
    }
    let mut bounds = Vec::with_capacity(rows.len());
    for row in rows {
        bounds.push(StreamBound {
            stream: StreamId {
                shard_id: u32::try_from(row.try_get::<_, i64>(0).map_err(pg)?)
                    .map_err(|_| IndexError::Conflict)?,
                owner_epoch: OwnerEpoch::new(number(row, 1)?).map_err(|_| IndexError::Conflict)?,
            },
            last_complete_sequence: CompleteThrough::new(number(row, 2)?),
        });
    }
    SourceCut::new(dataset, topology, bounds).map_err(|_| IndexError::Conflict)
}
