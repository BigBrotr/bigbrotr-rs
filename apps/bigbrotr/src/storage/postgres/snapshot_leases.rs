//! Actual native barrier -> atomic reader lease, binding and source claims.
//! Receipts are historical metadata, never principal authority or live read grants.
use super::{ControlSession, application_transaction, fresh_id, require};
use crate::storage::postgres::{catalog_guard::CatalogCapture, ingest_registry::ControlOwner};
use bigbrotr_model::{DatasetId, OwnerResourceId, TopologyGeneration};
use bigbrotr_protocol::archive_cut::CutBound;
use std::{cell::Cell, fmt, time::Duration};
use tokio_postgres::{Row, Transaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivePinError {
    InvalidInput,
    InvalidMetadata,
    Prerequisite,
    Conflict,
    ResourceLimit,
    Unavailable,
    Backend,
    CommitUnknown,
}
impl fmt::Display for ArchivePinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive snapshot pin refused: {self:?}")
    }
}
impl std::error::Error for ArchivePinError {}
use ArchivePinError as Error;

pub struct ArchivePinRequest {
    pub lease_id: [u8; 16],
    pub frontier_revision: u64,
    pub ttl: Duration,
}
impl ArchivePinRequest {
    fn ttl_us(&self) -> Result<i64, Error> {
        if self.lease_id == [0; 16] || self.frontier_revision == 0 {
            return Err(Error::InvalidInput);
        }
        i64::try_from(self.ttl.as_micros())
            .ok()
            .filter(|v| (1..=120_000_000).contains(v))
            .ok_or(Error::InvalidInput)
    }
}

/// A lost reply resolves this original identity without extending its expiry.
/// Even an unreleased, apparently unexpired receipt does not grant delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchivePinReceipt {
    pub lease_id: [u8; 16],
    pub catalog_id: OwnerResourceId,
    pub frontier_revision: u64,
    pub snapshot_id: u64,
    pub lease_revision: u64,
    pub acquired_at_us: i64,
    pub updated_at_us: i64,
    pub expires_at_us: i64,
    pub released: bool,
    pub created: bool,
}
/// Immutable selection metadata observed from Control. Neither this value nor
/// its stored frontier acquires a pin, establishes native presence or grants
/// access. Consumers must retain the actual catalog barrier and certified pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveReadSelection {
    pub dataset_id: DatasetId,
    pub catalog_id: OwnerResourceId,
    pub frontier_revision: u64,
    pub snapshot_id: u64,
    pub topology_generation: TopologyGeneration,
    pub bounds: Vec<CutBound>,
}

struct Recorded {
    receipt: ArchivePinReceipt,
    ttl_us: i64,
    current_holder: bool,
}
fn backend(_: tokio_postgres::Error) -> Error {
    Error::Backend
}
fn number(row: &Row, i: usize) -> Result<u64, Error> {
    row.try_get::<_, &str>(i)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(Error::InvalidMetadata)
}

impl ControlSession {
    /// Selected readers require their observation, retention and invoker-helper
    /// privileges before exposing the private listener. This acquires no pin.
    pub async fn require_archive_reader(&mut self) -> Result<(), Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 18)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let valid: bool = tx.query_one(
            "SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege)) AND has_any_column_privilege(CURRENT_USER,'control.consumer_frontier_revisions','UPDATE') AND has_function_privilege(CURRENT_USER,'control.lock_archive_pins(bytea,bytea)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.require_archive_pin_holder(control.snapshot_leases,boolean)','EXECUTE') FROM (VALUES ('control.archive_frontier_snapshots',ARRAY['SELECT']),('control.consumer_frontier_revisions',ARRAY['SELECT']),('control.consumer_frontiers',ARRAY['SELECT']),('control.archive_persistence',ARRAY['SELECT']),('control.archive_pin_recoveries',ARRAY['SELECT']),('control.topology_generations',ARRAY['SELECT']),('control.consumers',ARRAY['SELECT','UPDATE']),('control.snapshot_leases',ARRAY['SELECT','INSERT','UPDATE']),('control.archive_snapshot_pins',ARRAY['SELECT','INSERT']),('control.retention_claims',ARRAY['SELECT','INSERT','UPDATE']),('control.source_streams',ARRAY['SELECT']),('control.topology_streams',ARRAY['SELECT'])) t(name,privileges) CROSS JOIN LATERAL unnest(t.privileges) p(privilege)", &[]
        ).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !valid {
            return Err(Error::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    /// Load one exact sealed revision before capturing its native snapshot. The
    /// bounded immutable rows can be observed without retaining a Control lock
    /// across the later catalog barrier. Pin registration rechecks the binding.
    pub async fn read_archive_selection(
        &mut self,
        catalog: OwnerResourceId,
        revision: u64,
    ) -> Result<Option<ArchiveReadSelection>, Error> {
        if revision == 0 || catalog == self.identity.resource_id {
            return Err(Error::InvalidInput);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 17)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let dataset = self.request.dataset;
        let outcome = async {
            let row = tx.query_opt(
                "SELECT f.snapshot_id,left(f.topology_generation::text,21),r.sealed AND r.dataset_id=f.dataset_id AND r.consumer_role='archive' AND r.topology_generation=f.topology_generation AND f.consumer_id=f.catalog_id FROM control.archive_frontier_snapshots f JOIN control.consumer_frontier_revisions r USING(consumer_id,frontier_revision) WHERE f.dataset_id=$1::bytea AND f.catalog_id=$2::bytea AND f.frontier_revision=$3::text::numeric",
                &[&&dataset.as_bytes()[..], &&catalog.as_bytes()[..], &revision.to_string()],
            ).await.map_err(backend)?;
            let Some(row) = row else { return Ok(None); };
            if !row.try_get::<_, bool>(2).map_err(backend)? {
                return Err(Error::InvalidMetadata);
            }
            let snapshot_id = u64::try_from(row.try_get::<_, i64>(0).map_err(backend)?)
                .ok().filter(|v| *v > 0).ok_or(Error::InvalidMetadata)?;
            let topology_generation = TopologyGeneration::new(number(&row, 1)?);
            let rows = tx.query(
                "SELECT shard_id::bigint,left(owner_epoch::text,21),left(last_complete_sequence::text,21) FROM control.consumer_frontiers WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric ORDER BY shard_id,owner_epoch LIMIT 1025",
                &[&&catalog.as_bytes()[..], &revision.to_string()],
            ).await.map_err(backend)?;
            if rows.len() > 1024 { return Err(Error::ResourceLimit); }
            let mut bounds = Vec::new();
            bounds.try_reserve_exact(rows.len()).map_err(|_| Error::ResourceLimit)?;
            for row in rows {
                let shard_id = u32::try_from(row.try_get::<_, i64>(0).map_err(backend)?)
                    .map_err(|_| Error::InvalidMetadata)?;
                let owner_epoch = number(&row, 1)?;
                if owner_epoch == 0 { return Err(Error::InvalidMetadata); }
                bounds.push(CutBound { shard_id, owner_epoch, complete_through: number(&row, 2)? });
            }
            Ok(Some(ArchiveReadSelection { dataset_id: dataset, catalog_id: catalog,
                frontier_revision: revision, snapshot_id, topology_generation, bounds }))
        }.await;
        match outcome {
            Ok(_) | Err(Error::ResourceLimit) => {
                tx.rollback().await.map_err(backend)?;
                self.available = true;
                outcome
            }
            error => error,
        }
    }

    /// Retire an old holder's obligations only in a fresh recovery-only custody
    /// lifetime. Ordinary readers cannot be launched under that lifetime. Keep
    /// both actual transports retained through join, including unknown COMMIT.
    pub async fn recover_archive_pin(
        &mut self,
        recovery: &crate::storage::archive::LocalPinRecovery,
        manager: &mut tokio_postgres::Client,
        lease_id: [u8; 16],
        duration: Duration,
        commit_started: &Cell<bool>,
    ) -> Result<ArchivePinReceipt, Error> {
        let coordinator = recovery.coordinator();
        let identity = recovery.identity();
        if identity.dataset() != self.request.dataset || coordinator.permits_reader_pins() {
            return Err(Error::InvalidInput);
        }
        // Historical lookup holds no Control transaction while acquiring the
        // catalog barrier. Every immutable value is checked again under lock.
        let expected = self
            .read_archive_pin(identity.catalog(), lease_id)
            .await?
            .ok_or(Error::Conflict)?;
        if expected.released {
            return Ok(expected);
        }
        let capture =
            CatalogCapture::at_snapshot(manager, coordinator, expected.snapshot_id, duration)
                .await
                .map_err(|_| Error::Unavailable)?;
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 18)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock(&tx, self.request.dataset, identity.catalog()).await?;
        let old = read(
            &tx,
            self.request.dataset,
            identity.catalog(),
            lease_id,
            self.identity,
        )
        .await?
        .ok_or(Error::Conflict)?;
        if old.current_holder || old.receipt != expected {
            return Err(Error::Conflict);
        }
        let dataset = &self.request.dataset.as_bytes()[..];
        let catalog_id = identity.catalog();
        let catalog = &catalog_id.as_bytes()[..];
        let lease = &lease_id[..];
        tx.execute("INSERT INTO control.archive_pin_recoveries(lease_id,dataset_id,consumer_id,expected_lease_revision,coordinator_binding_sha256,exclusion_protocol,recoverer_resource,recoverer_epoch,recoverer_incarnation,recovered_at_us) VALUES ($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::bytea,1,$6::bytea,$7::text::numeric,$8::bytea,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)", &[&lease,&dataset,&catalog,&expected.lease_revision.to_string(),&&coordinator.binding_digest().as_bytes()[..],&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
        tx.execute("UPDATE control.retention_claims SET state='released',released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,release_reason='old_reader_excluded' WHERE lease_id=$1::bytea AND state='active'", &[&lease]).await.map_err(backend)?;
        let changed = tx.execute("UPDATE control.snapshot_leases SET state='released',lease_revision=lease_revision+1,updated_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,release_reason='old_reader_excluded' WHERE lease_id=$1::bytea AND lease_revision=$2::text::numeric AND state='active'", &[&lease,&expected.lease_revision.to_string()]).await.map_err(backend)?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        let receipt = read(
            &tx,
            self.request.dataset,
            identity.catalog(),
            lease_id,
            self.identity,
        )
        .await?
        .ok_or(Error::InvalidMetadata)?
        .receipt;
        capture.verify().await.map_err(|_| Error::Unavailable)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| Error::CommitUnknown)?;
        capture.verify().await.map_err(|_| Error::Unavailable)?;
        capture.finish().await.map_err(|_| Error::Unavailable)?;
        self.available = true;
        Ok(receipt)
    }

    /// Caller holds local custody and both actual transports through commit/join.
    /// All claims are retained independently of expiry. Native existence is
    /// checked under the barrier; signed file integrity belongs to the reader.
    pub async fn register_archive_pin(
        &mut self,
        capture: &CatalogCapture<'_>,
        request: &ArchivePinRequest,
        commit_started: &Cell<bool>,
    ) -> Result<ArchivePinReceipt, Error> {
        let ttl_us = request.ttl_us()?;
        let identity = capture.identity();
        if identity.dataset() != self.request.dataset
            || identity.catalog() == self.identity.resource_id
            || !capture.coordinator().permits_reader_pins()
        {
            return Err(Error::InvalidInput);
        }
        capture.verify().await.map_err(|_| Error::Unavailable)?;
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 17)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock(&tx, self.request.dataset, identity.catalog()).await?;
        if let Some(recorded) = read(
            &tx,
            self.request.dataset,
            identity.catalog(),
            request.lease_id,
            self.identity,
        )
        .await?
        {
            if recorded.receipt.frontier_revision != request.frontier_revision
                || recorded.receipt.snapshot_id != capture.snapshot_id()
                || recorded.ttl_us != ttl_us
            {
                return Err(Error::Conflict);
            }
            capture.verify().await.map_err(|_| Error::Unavailable)?;
            tx.rollback().await.map_err(backend)?;
            self.available = true;
            // No renewal, replacement holder or implicit resurrection on retry.
            return Ok(recorded.receipt);
        }
        let dataset_id = identity.dataset();
        let catalog_id = identity.catalog();
        let dataset = &dataset_id.as_bytes()[..];
        let catalog = &catalog_id.as_bytes()[..];
        let revision = request.frontier_revision.to_string();
        let row = tx.query_opt("SELECT f.topology_generation::text, f.snapshot_id=$4::bigint AND r.sealed AND p.coordinator_binding_sha256=$5::bytea FROM control.archive_frontier_snapshots f JOIN control.consumer_frontier_revisions r USING(consumer_id,frontier_revision) JOIN control.archive_persistence p ON p.dataset_id=f.dataset_id AND p.catalog_id=f.catalog_id WHERE f.dataset_id=$1::bytea AND f.catalog_id=$2::bytea AND f.frontier_revision=$3::text::numeric", &[&dataset,&catalog,&revision,&(capture.snapshot_id() as i64),&&capture.coordinator().binding_digest().as_bytes()[..]]).await.map_err(backend)?.ok_or(Error::Conflict)?;
        if !row.try_get::<_, bool>(1).map_err(backend)? {
            return Err(Error::Conflict);
        }
        let topology = number(&row, 0)?.to_string();
        let bounds = tx.query("SELECT shard_id::bigint,owner_epoch::text,last_complete_sequence::text FROM control.consumer_frontiers WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric ORDER BY shard_id,owner_epoch LIMIT 1025", &[&catalog,&revision]).await.map_err(backend)?;
        if bounds.len() > 1024 {
            return Err(Error::ResourceLimit);
        }
        let lease = &request.lease_id[..];
        let owner_resource = &self.identity.resource_id.as_bytes()[..];
        let owner_epoch = self.identity.epoch.get().to_string();
        let incarnation = &self.identity.process_incarnation.as_bytes()[..];
        let inserted = tx.execute("INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,acquired_at_us,updated_at_us,expires_at_us,archive_pin_protocol) SELECT $1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::text::numeric,'role_instance',$6::bytea,'control',$7::bytea,now_us,now_us,least(now_us+$8::bigint,lease_expires_at_us),1 FROM control.owner_epochs CROSS JOIN (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS now_us)t WHERE resource_id=$9::bytea AND epoch=$10::text::numeric", &[&lease,&dataset,&catalog,&revision,&topology,&&self.identity.role_instance_id[..],&incarnation,&ttl_us,&owner_resource,&owner_epoch]).await.map_err(backend)?;
        if inserted != 1 {
            return Err(Error::Conflict);
        }
        tx.execute("INSERT INTO control.archive_snapshot_pins(lease_id,dataset_id,consumer_id,frontier_revision,issuer_resource,issuer_epoch,issuer_incarnation,purpose,requested_ttl_us) VALUES ($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::bytea,$6::text::numeric,$7::bytea,'reader',$8::bigint)", &[&lease,&dataset,&catalog,&revision,&owner_resource,&owner_epoch,&incarnation,&ttl_us]).await.map_err(backend)?;
        for bound in bounds {
            let last = number(&bound, 2)?;
            if last == 0 {
                continue;
            }
            let shard: i64 = bound.try_get(0).map_err(backend)?;
            let epoch = number(&bound, 1)?;
            let claim = fresh_id().map_err(|_| Error::Unavailable)?;
            tx.execute("INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us) VALUES ($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::bigint,$6::text::numeric,1,$7::text::numeric,'reader',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)", &[&&claim[..],&lease,&dataset,&topology,&shard,&epoch.to_string(),&last.to_string()]).await.map_err(backend)?;
        }
        let mut receipt = read(&tx, dataset_id, catalog_id, request.lease_id, self.identity)
            .await?
            .ok_or(Error::InvalidMetadata)?
            .receipt;
        capture.verify().await.map_err(|_| Error::Unavailable)?;
        // Keep time/complete-claim checks deferred until COMMIT, after every wait.
        commit_started.set(true);
        tx.commit().await.map_err(|_| Error::CommitUnknown)?;
        capture.verify().await.map_err(|_| Error::Unavailable)?;
        receipt.created = true;
        self.available = true;
        Ok(receipt)
    }

    /// Historical resolution only. Absence is not proof of rollback; expiry,
    /// current holder, native existence and policy must be checked for delivery.
    pub async fn read_archive_pin(
        &mut self,
        catalog: OwnerResourceId,
        lease: [u8; 16],
    ) -> Result<Option<ArchivePinReceipt>, Error> {
        if lease == [0; 16] || catalog == self.identity.resource_id {
            return Err(Error::InvalidInput);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 17)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let recorded = read(&tx, self.request.dataset, catalog, lease, self.identity).await?;
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(recorded.map(|r| r.receipt))
    }

    /// Explicit release only after this holder's consumers and delivery have
    /// joined. Expiry or a dropped future is insufficient. A different Control
    /// holder cannot release these obligations without separate recovery proof.
    pub async fn release_archive_pin(
        &mut self,
        expected: &ArchivePinReceipt,
        commit_started: &Cell<bool>,
    ) -> Result<ArchivePinReceipt, Error> {
        if expected.lease_id == [0; 16] || expected.catalog_id == self.identity.resource_id {
            return Err(Error::InvalidInput);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 17)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        // Before any row UPDATE/lock: the trigger cannot order a row lock that
        // PostgreSQL has already taken before invoking its BEFORE ROW callback.
        lock(&tx, self.request.dataset, expected.catalog_id).await?;
        let old = read(
            &tx,
            self.request.dataset,
            expected.catalog_id,
            expected.lease_id,
            self.identity,
        )
        .await?
        .ok_or(Error::Conflict)?;
        if old.receipt.frontier_revision != expected.frontier_revision
            || old.receipt.snapshot_id != expected.snapshot_id
        {
            return Err(Error::Conflict);
        }
        if old.receipt.released {
            tx.rollback().await.map_err(backend)?;
            self.available = true;
            return Ok(old.receipt);
        }
        if !old.current_holder || old.receipt.lease_revision != expected.lease_revision {
            return Err(Error::Conflict);
        }
        let lease = &expected.lease_id[..];
        tx.execute("UPDATE control.retention_claims SET state='released',released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,release_reason='reader_joined' WHERE lease_id=$1::bytea AND state='active'", &[&lease]).await.map_err(backend)?;
        let changed = tx.execute("UPDATE control.snapshot_leases SET state='released',lease_revision=lease_revision+1,updated_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,release_reason='reader_joined' WHERE lease_id=$1::bytea AND lease_revision=$2::text::numeric AND state='active'", &[&lease,&expected.lease_revision.to_string()]).await.map_err(backend)?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        let receipt = read(
            &tx,
            self.request.dataset,
            expected.catalog_id,
            expected.lease_id,
            self.identity,
        )
        .await?
        .ok_or(Error::InvalidMetadata)?
        .receipt;
        commit_started.set(true);
        tx.commit().await.map_err(|_| Error::CommitUnknown)?;
        self.available = true;
        Ok(receipt)
    }
}

async fn lock(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    catalog: OwnerResourceId,
) -> Result<(), Error> {
    tx.query_one(
        "SELECT control.lock_archive_pins($1::bytea,$2::bytea)",
        &[&&dataset.as_bytes()[..], &&catalog.as_bytes()[..]],
    )
    .await
    .map_err(backend)?;
    Ok(())
}
async fn read(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    lease: [u8; 16],
    owner: ControlOwner,
) -> Result<Option<Recorded>, Error> {
    let row = tx.query_opt("SELECT l.dataset_id=$2::bytea AND l.consumer_id=$3::bytea AND l.archive_pin_protocol=1 AND p.purpose='reader',l.frontier_revision::text,f.snapshot_id,l.lease_revision::text,l.acquired_at_us,l.updated_at_us,l.expires_at_us,l.state='released',p.requested_ttl_us,p.issuer_resource=$4::bytea AND p.issuer_epoch=$5::text::numeric AND p.issuer_incarnation=$6::bytea FROM control.snapshot_leases l LEFT JOIN control.archive_snapshot_pins p USING(lease_id) LEFT JOIN control.archive_frontier_snapshots f ON f.consumer_id=p.consumer_id AND f.frontier_revision=p.frontier_revision WHERE l.lease_id=$1::bytea", &[&&lease[..],&&dataset.as_bytes()[..],&&catalog.as_bytes()[..],&&owner.resource_id.as_bytes()[..],&owner.epoch.get().to_string(),&&owner.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.try_get::<_, Option<bool>>(0).map_err(backend)? != Some(true) {
        return Err(Error::Conflict);
    }
    let snapshot_id = u64::try_from(row.try_get::<_, i64>(2).map_err(backend)?)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(Error::InvalidMetadata)?;
    Ok(Some(Recorded {
        receipt: ArchivePinReceipt {
            lease_id: lease,
            catalog_id: catalog,
            frontier_revision: number(&row, 1)?,
            snapshot_id,
            lease_revision: number(&row, 3)?,
            acquired_at_us: row.try_get(4).map_err(backend)?,
            updated_at_us: row.try_get(5).map_err(backend)?,
            expires_at_us: row.try_get(6).map_err(backend)?,
            released: row.try_get(7).map_err(backend)?,
            created: false,
        },
        ttl_us: row.try_get(8).map_err(backend)?,
        current_holder: row.try_get(9).map_err(backend)?,
    }))
}
