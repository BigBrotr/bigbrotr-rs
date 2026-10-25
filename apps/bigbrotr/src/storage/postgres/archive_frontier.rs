//! Actual native observation -> fenced, complete immutable Archive frontier.
//! A receipt records publication; it is not a read pin or principal authority.
use super::{
    ControlSession, InitialHistoryRequest, OwnerError, RetainedArchiveFrontier,
    VerifiedSuccessorHistory, application_transaction, fresh_id, initial_history, require,
    successor_history,
};
use crate::storage::{
    archive::RetainedInputs,
    postgres::{
        archive_attempt::ArchiveAttemptError as Error, catalog_guard::CapturedCut,
        ingest_registry::ControlOwner,
    },
    rocks::store::IngestRecovery,
};
use bigbrotr_model::{Digest256, OwnerResourceId};
use bigbrotr_protocol::archive_cut::CutRead;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use tokio_postgres::{Row, Transaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveFrontierReceipt {
    pub operation_id: [u8; 16],
    pub consumer_id: OwnerResourceId,
    pub revision: u64,
    pub snapshot_id: u64,
    pub request_digest: Digest256,
    pub family_counts: [u64; 5],
    pub created: bool,
}
fn backend(_: tokio_postgres::Error) -> Error {
    Error::Backend
}
fn number(row: &Row, i: usize) -> Result<u64, Error> {
    row.try_get::<_, &str>(i)
        .ok()
        .and_then(|v| v.parse().ok())
        .ok_or(Error::InvalidMetadata)
}
fn fixed<const N: usize>(row: &Row, i: usize) -> Result<[u8; N], Error> {
    row.try_get::<_, &[u8]>(i)
        .ok()
        .and_then(|v| v.try_into().ok())
        .ok_or(Error::InvalidMetadata)
}

// Borrowed call context only; the outer frontier owns the one private Check.
struct CompactedPublication<'a, 'h> {
    frontier: &'a RetainedArchiveFrontier,
    proof: &'a VerifiedSuccessorHistory<'h>,
    recovery: &'a IngestRecovery,
    input: InitialHistoryRequest<'a>,
}

impl ControlSession {
    /// Historical immutable result lookup under current Control. This does not
    /// recapture latest, replace the original owner, or acquire/renew a read pin.
    /// A lost reply can thus resolve the same operation after native publication
    /// advances. Absence grants no new operation identity or replay permission.
    pub async fn read_archive_frontier(
        &mut self,
        catalog: OwnerResourceId,
        operation_id: [u8; 16],
    ) -> Result<Option<ArchiveFrontierReceipt>, Error> {
        if operation_id == [0; 16] || catalog == self.identity.resource_id {
            return Err(Error::InvalidInput);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 16)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = tx.query_opt("SELECT dataset_id=$2::bytea AND catalog_id=$3::bytea,frontier_revision::text,snapshot_id,substring(request_digest FROM 1 FOR 33),dictionary_count::text,payload_count::text,eligibility_count::text,receive_count::text,provenance_count::text FROM control.archive_frontier_snapshots WHERE operation_id=$1::bytea", &[&&operation_id[..],&&self.request.dataset.as_bytes()[..],&&catalog.as_bytes()[..]]).await.map_err(backend)?;
        let result = if let Some(row) = row {
            if !row.try_get::<_, bool>(0).map_err(backend)? {
                return Err(Error::Conflict);
            }
            let revision = number(&row, 1)?;
            let snapshot_id = u64::try_from(row.try_get::<_, i64>(2).map_err(backend)?)
                .map_err(|_| Error::InvalidMetadata)?;
            if revision == 0 || snapshot_id == 0 {
                return Err(Error::InvalidMetadata);
            }
            Some(ArchiveFrontierReceipt {
                operation_id,
                consumer_id: catalog,
                revision,
                snapshot_id,
                request_digest: Digest256::from_bytes(fixed(&row, 3)?),
                family_counts: [
                    number(&row, 4)?,
                    number(&row, 5)?,
                    number(&row, 6)?,
                    number(&row, 7)?,
                    number(&row, 8)?,
                ],
                created: false,
            })
        } else {
            None
        };
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(result)
    }
    /// Caller retains CatalogCapture and both actual transports through COMMIT
    /// and join. Complete input handles must be loaded before this short PG
    /// transaction; synchronous local I/O must not block Control supervision.
    pub async fn publish_archive_frontier(
        &mut self,
        captured: &CapturedCut<'_, '_>,
        inputs: &[RetainedInputs],
        operation_id: [u8; 16],
        expected_revision: u64,
        commit_started: &Cell<bool>,
    ) -> Result<ArchiveFrontierReceipt, Error> {
        self.publish_archive_frontier_in(
            captured,
            inputs,
            operation_id,
            expected_revision,
            commit_started,
            None,
            |_, _| Ok(()),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "captured native result, retained history and exact transaction authority"
    )]
    pub async fn publish_compacted_archive_frontier<'a, 'h>(
        &mut self,
        captured: &CapturedCut<'_, '_>,
        inputs: &[RetainedInputs],
        operation_id: [u8; 16],
        expected_revision: u64,
        commit_started: &Cell<bool>,
        frontier: &'a RetainedArchiveFrontier,
        proof: &'a VerifiedSuccessorHistory<'h>,
        recovery: &'a IngestRecovery,
        input: InitialHistoryRequest<'a>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ArchiveFrontierReceipt, Error> {
        self.publish_archive_frontier_in(
            captured,
            inputs,
            operation_id,
            expected_revision,
            commit_started,
            Some(CompactedPublication {
                frontier,
                proof,
                recovery,
                input,
            }),
            validate,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one shared publication kernel with optional private retained-history context"
    )]
    async fn publish_archive_frontier_in(
        &mut self,
        captured: &CapturedCut<'_, '_>,
        inputs: &[RetainedInputs],
        operation_id: [u8; 16],
        expected_revision: u64,
        commit_started: &Cell<bool>,
        history: Option<CompactedPublication<'_, '_>>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ArchiveFrontierReceipt, Error> {
        let read = captured.observed().read();
        let observation = captured.observed().observation();
        let coordinator = captured.coordinator();
        let identity = coordinator.identity();
        if operation_id == [0; 16]
            || read.dataset_id != self.request.dataset
            || read.snapshot_id != Some(observation.snapshot_id)
            || identity.catalog() == self.identity.resource_id
            || inputs.len() != read.receipts.len()
            || inputs.iter().zip(&read.receipts).any(|(input, receipt)| {
                input.expectation() != receipt
                    || input.coordinator_binding() != coordinator.binding_digest()
                    || input.request().catalog() != identity.catalog()
            })
        {
            return Err(Error::InvalidInput);
        }
        let revision = expected_revision.checked_add(1).ok_or(Error::Exhausted)?;
        captured
            .verify_barrier()
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = self.identity;
        let (tx, mut authority) = if let Some(h) = &history {
            let check = h.frontier.require_compacted_context(
                h.proof,
                h.recovery,
                identity.catalog(),
                coordinator.binding_digest(),
            )?;
            let (tx, mut lease, policy) = self
                .begin_checked_successor_history(h.proof.history(), h.recovery, &h.input, validate)
                .await?;
            check
                .require_history(
                    &tx,
                    h.recovery,
                    h.proof.history(),
                    &mut lease,
                    policy,
                    h.input.cancelled,
                )
                .await?;
            (tx, Some((lease, policy)))
        } else {
            self.begin_operation().map_err(|_| Error::Unavailable)?;
            let tx = application_transaction(&mut self.client, 16)
                .await
                .map_err(|_| Error::Prerequisite)?;
            require(&tx, self.request.dataset, current)
                .await
                .map_err(|_| Error::Unavailable)?;
            (tx, None)
        };
        let dataset = &read.dataset_id.as_bytes()[..];
        let catalog_id = identity.catalog();
        let catalog = &catalog_id.as_bytes()[..];
        tx.query_one(
            "SELECT dataset_id FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
            &[&dataset],
        )
        .await
        .map_err(backend)?;
        let receipt = ArchiveFrontierReceipt {
            operation_id,
            consumer_id: identity.catalog(),
            revision,
            snapshot_id: observation.snapshot_id,
            request_digest: observation.request_digest,
            family_counts: observation.family_counts,
            created: true,
        };
        if let Some(existing) = resolve(&tx, &receipt, dataset).await? {
            if let Some(h) = &history {
                h.frontier
                    .validate_compacted(h.proof, h.recovery, h.input.cancelled)?;
                successor_history::require_current(&tx, current).await?;
            }
            captured
                .verify_barrier()
                .await
                .map_err(|_| Error::Unavailable)?;
            if let Some((h, (lease, policy))) = history.as_ref().zip(authority.as_mut()) {
                initial_history::check_delivery(lease, *policy, h.input.cancelled)?;
            }
            tx.rollback().await.map_err(backend)?;
            if let Some((h, (lease, policy))) = history.as_ref().zip(authority.as_mut()) {
                h.frontier
                    .validate_compacted(h.proof, h.recovery, h.input.cancelled)?;
                captured
                    .verify_barrier()
                    .await
                    .map_err(|_| Error::Unavailable)?;
                initial_history::check_delivery(lease, *policy, h.input.cancelled)?;
                if self.client.is_closed() {
                    return Err(Error::Unavailable);
                }
            }
            self.available = true;
            return Ok(existing);
        }
        validate_topology(&tx, read).await?;
        // Match every immutable Control intent, including its original native
        // lifetime. The local handles bind complete admission grouping; the
        // joined observation binds native receipt/files/reference closure.
        for input in inputs {
            let r = input.request();
            let matching: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.archive_attempts WHERE attempt_id=$1::bytea AND dataset_id=$2::bytea AND catalog_id=$3::bytea AND ordinal=$4::text::numeric AND topology_generation=$5::text::numeric AND schema_version=1 AND batch_digest=$6::bytea AND coordinator_incarnation=$7::bytea AND worker_incarnation=$8::bytea AND persistence_protocol=1 AND coordinator_binding_sha256=$9::bytea)", &[&&r.attempt_id()[..],&dataset,&catalog,&r.ordinal().to_string(),&r.topology_generation().get().to_string(),&&r.batch_digest().as_bytes()[..],&&r.coordinator().as_bytes()[..],&&r.worker().as_bytes()[..],&&coordinator.binding_digest().as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(backend)?;
            if !matching {
                return Err(Error::Conflict);
            }
        }
        let epoch = archive_owner(&tx, captured, current).await?;
        let consumer = tx.query_opt("SELECT dataset_id=$2::bytea AND consumer_role='archive',coalesce(current_revision,0)::text FROM control.consumers WHERE consumer_id=$1::bytea FOR UPDATE", &[&catalog,&dataset]).await.map_err(backend)?;
        if let Some(row) = consumer {
            if !row.try_get::<_, bool>(0).map_err(backend)? || number(&row, 1)? != expected_revision
            {
                return Err(Error::Conflict);
            }
        } else {
            if expected_revision != 0 {
                return Err(Error::Conflict);
            }
            tx.execute("INSERT INTO control.consumers(consumer_id,dataset_id,consumer_role,created_at_us) VALUES ($1::bytea,$2::bytea,'archive',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)", &[&catalog,&dataset]).await.map_err(backend)?;
        }
        let rev = revision.to_string();
        let topology = read.topology_generation.get().to_string();
        let incarnation = &current.process_incarnation.as_bytes()[..];
        tx.execute("INSERT INTO control.consumer_frontier_revisions(consumer_id,frontier_revision,dataset_id,consumer_role,topology_generation,previous_revision,owner_resource_id,owner_epoch,owner_incarnation,created_at_us) VALUES ($1::bytea,$2::text::numeric,$3::bytea,'archive',$4::text::numeric,NULLIF($5::text::numeric,0),$1::bytea,$6::text::numeric,$7::bytea,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)", &[&catalog,&rev,&dataset,&topology,&expected_revision.to_string(),&epoch.to_string(),&incarnation]).await.map_err(backend)?;
        let counts = observation.family_counts.map(|n| n.to_string());
        tx.execute("INSERT INTO control.archive_frontier_snapshots(operation_id,consumer_id,frontier_revision,dataset_id,topology_generation,catalog_id,owner_epoch,owner_incarnation,snapshot_id,request_digest,dictionary_count,payload_count,eligibility_count,receive_count,provenance_count) VALUES ($1::bytea,$2::bytea,$3::text::numeric,$4::bytea,$5::text::numeric,$2::bytea,$6::text::numeric,$7::bytea,$8::bigint,$9::bytea,$10::text::numeric,$11::text::numeric,$12::text::numeric,$13::text::numeric,$14::text::numeric)", &[&&operation_id[..],&catalog,&rev,&dataset,&topology,&epoch.to_string(),&incarnation,&(observation.snapshot_id as i64),&&observation.request_digest.as_bytes()[..],&counts[0],&counts[1],&counts[2],&counts[3],&counts[4]]).await.map_err(backend)?;
        for input in inputs {
            tx.execute("INSERT INTO control.archive_frontier_batches VALUES ($1::bytea,$2::text::numeric,$3::bytea)", &[&catalog,&rev,&&input.request().attempt_id()[..]]).await.map_err(backend)?;
        }
        for bound in &read.bounds {
            let digest = if bound.complete_through == 0 {
                None
            } else {
                Some(
                    read.receipts
                        .iter()
                        .find(|r| {
                            r.input_ranges.iter().any(|b| {
                                b.shard_id == bound.shard_id
                                    && b.owner_epoch == bound.owner_epoch
                                    && b.last_sequence == bound.complete_through
                            })
                        })
                        .ok_or(Error::InvalidInput)?
                        .batch_digest,
                )
            };
            let bytes = digest.as_ref().map(|v| &v.as_bytes()[..]);
            tx.execute("INSERT INTO control.consumer_frontiers(consumer_id,frontier_revision,dataset_id,topology_generation,shard_id,owner_epoch,last_complete_sequence,attestation_kind,attestation_digest) VALUES ($1::bytea,$2::text::numeric,$3::bytea,$4::text::numeric,$5::bigint,$6::text::numeric,$7::text::numeric,CASE WHEN $8::bytea IS NULL THEN NULL ELSE 'archive-batch' END,$8::bytea)", &[&catalog,&rev,&dataset,&topology,&i64::from(bound.shard_id),&bound.owner_epoch.to_string(),&bound.complete_through.to_string(),&bytes]).await.map_err(backend)?;
        }
        tx.execute("UPDATE control.consumer_frontier_revisions SET sealed=true WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric", &[&catalog,&rev]).await.map_err(backend)?;
        tx.execute("UPDATE control.consumers SET current_revision=$2::text::numeric,current_topology_generation=$3::text::numeric WHERE consumer_id=$1::bytea", &[&catalog,&rev,&topology]).await.map_err(backend)?;
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(backend)?;
        if let Some(h) = &history {
            h.frontier
                .validate_compacted(h.proof, h.recovery, h.input.cancelled)?;
            successor_history::require_current(&tx, current).await?;
        }
        captured
            .verify_barrier()
            .await
            .map_err(|_| Error::Unavailable)?;
        tx.query_one("SELECT control.require_archive_frontier_owner($1::bytea,$2::bytea,$3::text::numeric,$4::bytea)", &[&dataset,&catalog,&epoch.to_string(),&incarnation]).await.map_err(backend)?;
        if let Some((h, (lease, policy))) = history.as_ref().zip(authority.as_mut()) {
            initial_history::check_delivery(lease, *policy, h.input.cancelled)?;
        }
        commit_started.set(true);
        tx.commit().await.map_err(|_| Error::CommitUnknown)?;
        if let Some((h, (lease, policy))) = history.as_ref().zip(authority.as_mut()) {
            h.frontier
                .validate_compacted(h.proof, h.recovery, h.input.cancelled)?;
            captured
                .verify_barrier()
                .await
                .map_err(|_| Error::Unavailable)?;
            initial_history::check_delivery(lease, *policy, h.input.cancelled)?;
            if self.client.is_closed() {
                return Err(Error::Unavailable);
            }
        }
        self.available = true;
        Ok(receipt)
    }
}

async fn resolve(
    tx: &Transaction<'_>,
    wanted: &ArchiveFrontierReceipt,
    dataset: &[u8],
) -> Result<Option<ArchiveFrontierReceipt>, Error> {
    let counts = wanted.family_counts.map(|n| n.to_string());
    let row = tx.query_opt("SELECT dataset_id=$2::bytea AND consumer_id=$3::bytea AND frontier_revision=$4::text::numeric AND snapshot_id=$5::bigint AND request_digest=$6::bytea AND dictionary_count=$7::text::numeric AND payload_count=$8::text::numeric AND eligibility_count=$9::text::numeric AND receive_count=$10::text::numeric AND provenance_count=$11::text::numeric FROM control.archive_frontier_snapshots WHERE operation_id=$1::bytea", &[&&wanted.operation_id[..],&dataset,&&wanted.consumer_id.as_bytes()[..],&wanted.revision.to_string(),&(wanted.snapshot_id as i64),&&wanted.request_digest.as_bytes()[..],&counts[0],&counts[1],&counts[2],&counts[3],&counts[4]]).await.map_err(backend)?;
    match row {
        None => Ok(None),
        Some(row) if row.try_get::<_, bool>(0).map_err(backend)? => {
            Ok(Some(ArchiveFrontierReceipt {
                created: false,
                ..*wanted
            }))
        }
        Some(_) => Err(Error::Conflict),
    }
}

async fn validate_topology(tx: &Transaction<'_>, read: &CutRead) -> Result<(), Error> {
    let dataset = &read.dataset_id.as_bytes()[..];
    let topology = read.topology_generation.get().to_string();
    let sealed: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.topology_generations WHERE dataset_id=$1::bytea AND generation=$2::text::numeric AND sealed)", &[&dataset,&topology]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !sealed {
        return Err(Error::Conflict);
    }
    // A fixed extra row refuses a topology beyond the protocol cap. Lock before
    // reading closure so a concurrent closer and publisher cannot both validate
    // against invisible state. READ COMMITTED is supplied by the shared adapter.
    let rows = tx.query("SELECT t.shard_id::bigint,t.owner_epoch::text FROM control.topology_streams t JOIN control.source_streams s USING(dataset_id,shard_id,owner_epoch) WHERE t.dataset_id=$1::bytea AND t.topology_generation=$2::text::numeric ORDER BY t.shard_id,t.owner_epoch LIMIT 1025 FOR SHARE OF s", &[&dataset,&topology]).await.map_err(backend)?;
    if rows.len() != read.bounds.len() {
        return Err(Error::Conflict);
    }
    for (row, bound) in rows.iter().zip(&read.bounds) {
        if row.try_get::<_, i64>(0).map_err(backend)? != i64::from(bound.shard_id)
            || number(row, 1)? != bound.owner_epoch
        {
            return Err(Error::Conflict);
        }
        let allowed: bool = tx.query_one("SELECT state<>'closed' OR last_complete_sequence >= $4::text::numeric FROM control.source_streams WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=$3::text::numeric", &[&dataset,&i64::from(bound.shard_id),&bound.owner_epoch.to_string(),&bound.complete_through.to_string()]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !allowed {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

async fn archive_owner(
    tx: &Transaction<'_>,
    cut: &CapturedCut<'_, '_>,
    issuer: ControlOwner,
) -> Result<u64, Error> {
    let coordinator = cut.coordinator();
    let dataset_id = coordinator.identity().dataset();
    let catalog_id = coordinator.identity().catalog();
    let dataset = &dataset_id.as_bytes()[..];
    let catalog = &catalog_id.as_bytes()[..];
    let row = tx.query_opt("SELECT o.epoch::text,substring(o.process_incarnation FROM 1 FOR 17),substring(o.role_instance_id FROM 1 FOR 17),o.state, a.dataset_id=$2::bytea AND a.coordinator_binding_sha256=$3::bytea,a.issuer_resource=$4::bytea AND a.issuer_epoch=$5::text::numeric AND a.issuer_incarnation=$6::bytea,o.lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint FROM control.owner_epochs o LEFT JOIN control.archive_owner_sessions a ON a.catalog_id=o.resource_id AND a.owner_epoch=o.epoch WHERE o.resource_id=$1::bytea ORDER BY o.epoch DESC LIMIT 1 FOR UPDATE OF o", &[&catalog,&dataset,&&coordinator.binding_digest().as_bytes()[..],&&issuer.resource_id.as_bytes()[..],&issuer.epoch.get().to_string(),&&issuer.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    let epoch = if let Some(row) = row {
        if row.try_get::<_, Option<bool>>(4).map_err(backend)? != Some(true) {
            return Err(Error::Conflict);
        }
        let previous = number(&row, 0)?;
        let state: &str = row.try_get(3).map_err(backend)?;
        if state == "active"
            && row.try_get::<_, Option<bool>>(5).map_err(backend)? == Some(true)
            && fixed::<16>(&row, 1)? == *issuer.process_incarnation.as_bytes()
            && row.try_get::<_, bool>(6).map_err(backend)?
        {
            tx.execute("UPDATE control.owner_epochs SET lease_expires_at_us=(SELECT lease_expires_at_us FROM control.owner_epochs WHERE resource_id=$3::bytea AND epoch=$4::text::numeric) WHERE resource_id=$1::bytea AND epoch=$2::text::numeric", &[&catalog,&previous.to_string(),&&issuer.resource_id.as_bytes()[..],&issuer.epoch.get().to_string()]).await.map_err(backend)?;
            return Ok(previous);
        }
        if state != "released" {
            // Both actual commit domains are excluded: current Control fence,
            // retained local custody and a live disabled native owner barrier.
            // Lease age is not the exclusion evidence.
            cut.verify_barrier().await.map_err(|_| Error::Unavailable)?;
            let digest: [u8; 32] = Sha256::new()
                .chain_update(b"bigbrotr/archive-frontier-owner-exclusion/1\0")
                .chain_update(dataset)
                .chain_update(catalog)
                .chain_update(previous.to_be_bytes())
                .chain_update(fixed::<16>(&row, 1)?)
                .chain_update(issuer.resource_id.as_bytes())
                .chain_update(issuer.epoch.get().to_be_bytes())
                .chain_update(issuer.process_incarnation.as_bytes())
                .chain_update(cut.observed().observation().request_digest.as_bytes())
                .finalize()
                .into();
            tx.execute("UPDATE control.owner_epochs SET state='released',released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,exclusion_method='storage_fenced',exclusion_digest=$3::bytea WHERE resource_id=$1::bytea AND epoch=$2::text::numeric", &[&catalog,&previous.to_string(),&&digest[..]]).await.map_err(backend)?;
            tx.execute("UPDATE control.role_instances SET retired_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint WHERE role_instance_id=$1::bytea AND retired_at_us IS NULL", &[&&fixed::<16>(&row,2)?[..]]).await.map_err(backend)?;
        }
        previous.checked_add(1).ok_or(Error::Exhausted)?
    } else {
        1
    };
    let role = fresh_id().map_err(|_| Error::Unavailable)?;
    tx.execute("INSERT INTO control.role_instances VALUES ($1::bytea,$2::bytea,'archive',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,NULL)", &[&&role[..],&dataset]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state,archive_frontier_protocol) SELECT $1::bytea,$2::text::numeric,$3::bytea,'archive',$4::bytea,$5::bytea,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,lease_expires_at_us,'active',1 FROM control.owner_epochs WHERE resource_id=$6::bytea AND epoch=$7::text::numeric", &[&catalog,&epoch.to_string(),&dataset,&&role[..],&&issuer.process_incarnation.as_bytes()[..],&&issuer.resource_id.as_bytes()[..],&issuer.epoch.get().to_string()]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.archive_owner_sessions(dataset_id,catalog_id,owner_epoch,owner_incarnation,coordinator_binding_sha256,persistence_protocol,issuer_resource,issuer_epoch,issuer_incarnation) VALUES ($1::bytea,$2::bytea,$3::text::numeric,$4::bytea,$5::bytea,1,$6::bytea,$7::text::numeric,$4::bytea)", &[&dataset,&catalog,&epoch.to_string(),&&issuer.process_incarnation.as_bytes()[..],&&coordinator.binding_digest().as_bytes()[..],&&issuer.resource_id.as_bytes()[..],&issuer.epoch.get().to_string()]).await.map_err(backend)?;
    Ok(epoch)
}
