//! Shared0501 inventory consumer. Filesystem work never runs in a transaction.
use super::{
    ControlSession, application_transaction,
    browser_sessions::{fixed, lock_dataset, revision},
    fresh_id, require,
};
use crate::{
    export::{
        destination::CheckedDestination,
        execution::{
            ExecutionCustody, ExportJob, ExportLimits, ExportPauseReason, ExportReservation,
            ExportSelection, ExportState, RunningExport, WrittenExport,
        },
    },
    management::{jobs::ExportDestination, sessions::SessionError},
    storage::postgres::catalog_guard::CatalogCapture,
};
use SessionError as Error;
use bigbrotr_model::{
    ArtifactId, DatasetId, DictionaryKind, Digest256, EventId, JobId, Ordinal, OwnerResourceId,
    PrincipalId, QualifiedRef, TopologyGeneration,
};
use bigbrotr_protocol::{archive_cut::CutBound, archive_event::EventQuery};
use std::{cell::Cell, time::Duration};
use tokio_postgres::{Row, Transaction};

fn backend(error: tokio_postgres::Error) -> Error {
    match error.code().map(|c| c.code()) {
        Some("BB001") => Error::Forbidden,
        Some("23514" | "23505") => Error::Conflict,
        Some("42501") => Error::Prerequisite,
        _ => Error::Backend,
    }
}
fn metadata(_: tokio_postgres::Error) -> Error {
    Error::InvalidMetadata
}
fn reusable<T>(outcome: &Result<T, Error>) -> bool {
    matches!(
        outcome,
        Ok(_)
            | Err(Error::Forbidden | Error::Conflict | Error::Capacity | Error::ArchiveUnavailable)
    )
}
async fn commit(tx: Transaction<'_>, started: &Cell<bool>) -> Result<(), Error> {
    started.set(true);
    tx.commit().await.map_err(|error| {
        if error.as_db_error().is_some() {
            backend(error)
        } else {
            Error::CommitUnknown
        }
    })
}
async fn authority(tx: &Transaction<'_>, dataset: DatasetId, job: JobId) -> Result<(), Error> {
    let qualified: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.export_jobs e JOIN control.jobs j USING(job_id) WHERE e.job_id=$1::bytea AND j.dataset_id=$2::bytea AND j.export_job_protocol=1)", &[&&job.as_bytes()[..],&&dataset.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if !qualified {
        return Err(Error::Conflict);
    }
    tx.query_one(
        "SELECT control.require_export_execution_authority($1::bytea)",
        &[&&job.as_bytes()[..]],
    )
    .await
    .map_err(backend)?;
    Ok(())
}

impl ControlSession {
    /// Selected execution requires501 in addition to the existing813 acceptance
    /// and reader prerequisites. No secrets or filesystem paths are inspected.
    pub async fn require_export_execution(&mut self) -> Result<(), Error> {
        self.require_export_jobs().await?;
        self.require_archive_reader()
            .await
            .map_err(|_| Error::Prerequisite)?;
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let valid: bool = tx.query_one("SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege)) AND has_any_column_privilege(CURRENT_USER,'control.principal_grants','UPDATE') AND has_any_column_privilege(CURRENT_USER,'control.local_accounts','UPDATE') AND has_any_column_privilege(CURRENT_USER,'control.artifacts','UPDATE') AND has_function_privilege(CURRENT_USER,'control.require_export_execution_authority(bytea)','EXECUTE') FROM (VALUES ('control.export_execution_units',ARRAY['SELECT','INSERT']),('control.artifacts',ARRAY['SELECT','INSERT']),('control.artifact_files',ARRAY['SELECT','INSERT']),('control.local_accounts',ARRAY['SELECT']),('control.principal_grants',ARRAY['SELECT'])) t(name,privileges) CROSS JOIN LATERAL unnest(t.privileges) p(privilege)", &[]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
        if !valid {
            return Err(Error::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    /// Internal bounded scan. The scheduler advances after denied/refused jobs
    /// and restarts from None only at the end; a paused/cancelling job is absent.
    /// A selected ID alone grants no execution or public visibility.
    pub async fn next_export_job(&mut self, after: Option<JobId>) -> Result<Option<JobId>, Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = tx.query_opt("SELECT substring(job_id FROM 1 FOR 17) FROM control.jobs WHERE dataset_id=$1::bytea AND export_job_protocol=1 AND state IN ('accepted','queued','running') AND ($2::bytea IS NULL OR job_id>$2::bytea) ORDER BY job_id LIMIT 1", &[&&self.request.dataset.as_bytes()[..], &after.as_ref().map(|id| &id.as_bytes()[..])]).await.map_err(backend)?;
        let result = row
            .as_ref()
            .map(|row| JobId::try_from(fixed::<16>(row, 0)?).map_err(|_| Error::InvalidMetadata))
            .transpose()?;
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(result)
    }

    /// Resolve the original job after an ambiguous commit, even with no current
    /// input grant or destination. Historical metadata grants no new file write.
    pub async fn read_export_job(&mut self, job: JobId) -> Result<Option<ExportJob>, Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let outcome = read(&tx, self.request.dataset, job).await;
        if reusable(&outcome) {
            tx.rollback().await.map_err(backend)?;
            self.available = true;
        }
        outcome
    }

    /// First reservation commits accepted->queued and one new artifact identity.
    /// Retries return the original identity/caps; smaller available caps refuse.
    /// This method performs no output dispatch and never adopts an advanced job
    /// without a unit. Cancellation/COMMIT loss resolves by the original job ID.
    pub async fn reserve_export(
        &mut self,
        job: JobId,
        capacity: ExportLimits,
        destination: &CheckedDestination,
        started: &Cell<bool>,
    ) -> Result<ExportJob, Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            authority(&tx, self.request.dataset, job).await?;
            let old = read(&tx, self.request.dataset, job).await?.ok_or(Error::Conflict)?;
            let selection = &old.selection;
            if destination.dataset() != selection.query.dataset || destination.id() != selection.destination.id
                || destination.binding_digest() != selection.destination.binding_sha256 { return Err(Error::Conflict); }
            if !matches!(old.state, ExportState::Accepted | ExportState::Queued | ExportState::Running) { return Err(Error::Conflict); }
            retained(&tx, job).await?;
            if let Some(unit) = old.reservation {
                if !unit.limits.fits(capacity) { return Err(Error::Capacity); }
                return Ok((old, false));
            }
            if old.state != ExportState::Accepted { return Err(Error::Conflict); }
            let artifact = ArtifactId::try_from(fresh_id().map_err(|_| Error::Unavailable)?).map_err(|_| Error::Unavailable)?;
            update_owner(&tx, &old, self.identity, "queued").await?;
            tx.execute("INSERT INTO control.export_execution_units(job_id,artifact_id,max_output_bytes,max_temporary_bytes) VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::text::numeric)", &[&&job.as_bytes()[..],&&artifact.as_bytes()[..],&capacity.maximum().to_string(),&capacity.temporary().to_string()]).await.map_err(backend)?;
            Ok((read(&tx, self.request.dataset, job).await?.ok_or(Error::InvalidMetadata)?, true))
        }.await;
        match outcome {
            Ok((job, true)) => {
                let result = commit(tx, started).await;
                if reusable(&result) {
                    self.available = true;
                }
                result.map(|_| job)
            }
            other => {
                if reusable(&other) {
                    tx.rollback().await.map_err(backend)?;
                    self.available = true;
                }
                other.map(|(job, _)| job)
            }
        }
    }

    /// Rechecks current account/input/export grants and expected job revision.
    /// This follows the reservation's separate COMMIT. Reclaiming a running row
    /// does not exclude an old filesystem worker; prepare() must acquire custody.
    pub async fn start_export(
        &mut self,
        expected: &ExportJob,
        capture: &CatalogCapture<'_>,
        reader_pin: [u8; 16],
        started: &Cell<bool>,
    ) -> Result<RunningExport, Error> {
        if expected.selection.query.dataset != self.request.dataset
            || reader_pin == [0; 16]
            || capture.identity().dataset() != self.request.dataset
            || capture.identity().catalog() != expected.selection.catalog
            || capture.snapshot_id() != expected.selection.query.snapshot_id
        {
            return Err(Error::InvalidInput);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            authority(&tx, self.request.dataset, expected.selection.job).await?;
            let old = read(&tx, self.request.dataset, expected.selection.job).await?.ok_or(Error::Conflict)?;
            if &old != expected || old.reservation.is_none() || !matches!(old.state, ExportState::Queued | ExportState::Running) { return Err(Error::Conflict); }
            retained(&tx, old.selection.job).await?;
            let bound: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.archive_persistence WHERE dataset_id=$1::bytea AND catalog_id=$2::bytea AND coordinator_binding_sha256=$3::bytea)", &[&&self.request.dataset.as_bytes()[..],&&old.selection.catalog.as_bytes()[..],&&capture.coordinator().binding_digest().as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            if !bound { return Err(Error::Conflict); }
            let live: bool = tx.query_one("SELECT control.scope_pin_live($1::bytea,$2::bytea,$3::text::numeric,$4::bytea)", &[&&self.request.dataset.as_bytes()[..],&&old.selection.catalog.as_bytes()[..],&old.selection.frontier_revision.to_string(),&&reader_pin[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            if !live { return Err(Error::Forbidden); }
            update_owner(&tx, &old, self.identity, "running").await?;
            let job = read(&tx, self.request.dataset, old.selection.job).await?.ok_or(Error::InvalidMetadata)?;
            capture.verify().await.map_err(|_| Error::ArchiveUnavailable)?;
            let remaining: i64 = tx.query_one("SELECT least(5000000,least(o.lease_expires_at_us,l.expires_at_us)-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.owner_epochs o CROSS JOIN control.snapshot_leases l WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric AND o.process_incarnation=$3::bytea AND o.state='active' AND l.lease_id=$4::bytea AND l.state='active'", &[&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..],&&reader_pin[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            if remaining <= 0 { return Err(Error::Forbidden); }
            Ok(RunningExport { job, owner: self.identity, catalog_identity: capture.identity().clone(), remaining: Duration::from_micros(remaining as u64) })
        }.await;
        match outcome {
            Ok(running) => {
                let result = commit(tx, started).await;
                if reusable(&result) {
                    self.available = true;
                }
                result.map(|_| running)
            }
            error => {
                if reusable(&error) {
                    tx.rollback().await.map_err(backend)?;
                    self.available = true;
                }
                error
            }
        }
    }

    /// Park a known refusal only after the supervisor has joined every writer
    /// and finished the fresh reader/capture cleanup. The bound filesystem lock
    /// remains held through this transaction and any unknown transport outcome.
    /// Parking grants no input read or output write, so revoked input grants do
    /// not prevent it. Current Control, policy and the exact start still must match.
    pub async fn pause_export(
        &mut self,
        custody: &ExecutionCustody,
        reason: ExportPauseReason,
        started: &Cell<bool>,
    ) -> Result<ExportJob, Error> {
        let expected = &custody.job;
        if expected.selection.query.dataset != self.request.dataset
            || custody.owner != self.identity
            || expected.state != ExportState::Running
            || expected.reservation.is_none()
        {
            return Err(Error::Conflict);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            // Job-only transition: no consumer or pin locks are acquired after
            // this job lock. Deferred source bounds lock last, as in0008.
            tx.query_opt("SELECT job_id FROM control.jobs WHERE job_id=$1::bytea AND dataset_id=$2::bytea FOR UPDATE", &[&&expected.selection.job.as_bytes()[..],&&self.request.dataset.as_bytes()[..]]).await.map_err(backend)?.ok_or(Error::Conflict)?;
            let old = read(&tx, self.request.dataset, expected.selection.job).await?.ok_or(Error::Conflict)?;
            if &old != expected { return Err(Error::Conflict); }
            let changed = tx.execute("UPDATE control.jobs SET state='paused',job_revision=job_revision+1,reason_code=$3::text WHERE job_id=$1::bytea AND job_revision=$2::text::numeric AND state='running' AND owner_resource_id=$4::bytea AND owner_epoch=$5::text::numeric AND owner_incarnation=$6::bytea", &[&&expected.selection.job.as_bytes()[..],&expected.revision.to_string(),&reason.code(),&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
            if changed != 1 { return Err(Error::Conflict); }
            read(&tx, self.request.dataset, expected.selection.job).await?.ok_or(Error::InvalidMetadata)
        }.await;
        match outcome {
            Ok(job) => {
                let result = commit(tx, started).await;
                if reusable(&result) {
                    self.available = true;
                }
                result.map(|_| job)
            }
            error => {
                if reusable(&error) {
                    tx.rollback().await.map_err(backend)?;
                    self.available = true;
                }
                error
            }
        }
    }

    /// Only actual output joined to its original native observation can enter
    /// inventory. Keep its reader pin live through deferred COMMIT checks; pin
    /// release is a later transaction after known completion and worker joins.
    pub async fn complete_export(
        &mut self,
        written: &WrittenExport,
        capture: &CatalogCapture<'_>,
        reader_pin: [u8; 16],
        started: &Cell<bool>,
    ) -> Result<ExportJob, Error> {
        let expected = &written.running.job;
        let selection = &expected.selection;
        if selection.query.dataset != self.request.dataset
            || written.running.owner != self.identity
            || reader_pin == [0; 16]
            || capture.identity() != written.observation.catalog_identity()
            || capture.snapshot_id() != selection.query.snapshot_id
            || capture.identity().catalog() != selection.catalog
        {
            return Err(Error::Conflict);
        }
        capture
            .verify()
            .await
            .map_err(|_| Error::ArchiveUnavailable)?;
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 501)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            authority(&tx, self.request.dataset, selection.job).await?;
            let old = read(&tx, self.request.dataset, selection.job).await?.ok_or(Error::Conflict)?;
            if &old != expected || old.state != ExportState::Running { return Err(Error::Conflict); }
            retained(&tx, selection.job).await?;
            let reservation = old.reservation.ok_or(Error::Conflict)?;
            let stored = written.stored;
            let total = stored.data.bytes.checked_add(stored.manifest_bytes).ok_or(Error::Capacity)?;
            let time: i64 = tx.query_one("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint", &[]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            tx.execute("INSERT INTO control.artifacts(artifact_id,job_id,dataset_id,principal_id,destination_id,destination_binding_sha256,manifest_format,manifest_bytes,manifest_sha256,file_count,total_bytes,verification_pin_id,completed_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,$6::bytea,'bigbrotr-signed-events-export/1',$7::text::numeric,$8::bytea,1,$9::text::numeric,$10::bytea,$11::bigint)", &[&&reservation.artifact.as_bytes()[..],&&selection.job.as_bytes()[..],&&selection.query.dataset.as_bytes()[..],&&selection.principal.as_bytes()[..],&&selection.destination.id.as_bytes()[..],&&selection.destination.binding_sha256.as_bytes()[..],&stored.manifest_bytes.to_string(),&&stored.manifest_sha256.as_bytes()[..],&total.to_string(),&&reader_pin[..],&time]).await.map_err(backend)?;
            tx.execute("INSERT INTO control.artifact_files(artifact_id,file_id,relative_path,format,byte_count,row_count,sha256) VALUES($1::bytea,'events','events.jsonl','nostr-jsonl/1',$2::text::numeric,$3::text::numeric,$4::bytea)", &[&&reservation.artifact.as_bytes()[..],&stored.data.bytes.to_string(),&stored.data.rows.to_string(),&&stored.data.sha256.as_bytes()[..]]).await.map_err(backend)?;
            let changed = tx.execute("UPDATE control.jobs SET state='succeeded',job_revision=job_revision+1,checkpoint_revision=1,result_manifest_digest=$3::bytea,completed_at_us=$4::bigint WHERE job_id=$1::bytea AND job_revision=$2::text::numeric AND state='running' AND owner_resource_id=$5::bytea AND owner_epoch=$6::text::numeric AND owner_incarnation=$7::bytea", &[&&selection.job.as_bytes()[..],&old.revision.to_string(),&&stored.manifest_sha256.as_bytes()[..],&time,&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
            if changed != 1 { return Err(Error::Conflict); }
            capture.verify().await.map_err(|_| Error::ArchiveUnavailable)?;
            read(&tx, self.request.dataset, selection.job).await?.ok_or(Error::InvalidMetadata)
        }.await;
        match outcome {
            Ok(job) => {
                let result = commit(tx, started).await;
                if reusable(&result) {
                    self.available = true;
                }
                result.map(|_| job)
            }
            error => {
                if reusable(&error) {
                    tx.rollback().await.map_err(backend)?;
                    self.available = true;
                }
                error
            }
        }
    }
}

async fn retained(tx: &Transaction<'_>, job: JobId) -> Result<(), Error> {
    // Scheduling expiry is absent. Active durable retention, exact purpose/job
    // and original frontier are required; fresh reader authority is separate.
    let valid: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.export_jobs e JOIN control.archive_snapshot_pins p ON p.lease_id=e.pin_lease_id JOIN control.snapshot_leases l USING(lease_id) WHERE e.job_id=$1::bytea AND p.purpose='export' AND p.export_job_id=e.job_id AND p.dataset_id=e.dataset_id AND p.consumer_id=e.catalog_id AND p.frontier_revision=e.frontier_revision AND l.state='active')", &[&&job.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if valid { Ok(()) } else { Err(Error::Conflict) }
}
async fn update_owner(
    tx: &Transaction<'_>,
    old: &ExportJob,
    owner: super::ingest_registry::ControlOwner,
    state: &str,
) -> Result<(), Error> {
    let changed = tx.execute("UPDATE control.jobs SET state=$3::text,job_revision=job_revision+1,owner_resource_id=$4::bytea,owner_epoch=$5::text::numeric,owner_incarnation=$6::bytea WHERE job_id=$1::bytea AND job_revision=$2::text::numeric", &[&&old.selection.job.as_bytes()[..],&old.revision.to_string(),&state,&&owner.resource_id.as_bytes()[..],&owner.epoch.get().to_string(),&&owner.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(Error::Conflict)
    }
}
fn number(row: &Row, index: usize) -> Result<u64, Error> {
    let text: &str = row.try_get(index).map_err(metadata)?;
    text.parse::<u64>()
        .ok()
        .filter(|v| v.to_string() == text)
        .ok_or(Error::InvalidMetadata)
}
async fn read(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    job: JobId,
) -> Result<Option<ExportJob>, Error> {
    let row = tx.query_opt("SELECT substring(j.principal_id FROM 1 FOR 17),left(j.state,32),left(j.job_revision::text,21),left(j.checkpoint_revision::text,21),substring(j.result_manifest_digest FROM 1 FOR 33),substring(e.catalog_id FROM 1 FOR 17),left(e.frontier_revision::text,21),f.snapshot_id,left(j.input_topology_generation::text,21),substring(e.event_id FROM 1 FOR 33),substring(e.allocation_domain FROM 1 FOR 17),left(e.ordinal::text,21),substring(e.destination_id FROM 1 FOR 17),substring(e.destination_binding_sha256 FROM 1 FOR 33),left(j.config_revision::text,21),left(j.policy_revision::text,21),substring(u.artifact_id FROM 1 FOR 17),left(u.max_output_bytes::text,21),left(u.max_temporary_bytes::text,21),j.input_complete AND e.dataset_id=j.dataset_id AND e.actor_id=j.principal_id AND f.dataset_id=j.dataset_id AND f.catalog_id=e.catalog_id AND f.topology_generation=j.input_topology_generation AND r.sealed AND r.topology_generation=f.topology_generation AND r.consumer_role='archive' FROM control.jobs j JOIN control.export_jobs e USING(job_id) JOIN control.archive_frontier_snapshots f ON f.consumer_id=e.catalog_id AND f.frontier_revision=e.frontier_revision JOIN control.consumer_frontier_revisions r ON r.consumer_id=f.consumer_id AND r.frontier_revision=f.frontier_revision LEFT JOIN control.export_execution_units u USING(job_id) WHERE j.job_id=$1::bytea AND j.dataset_id=$2::bytea AND j.export_job_protocol=1", &[&&job.as_bytes()[..],&&dataset.as_bytes()[..]]).await.map_err(backend)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if !row.try_get::<_, bool>(19).map_err(metadata)? {
        return Err(Error::InvalidMetadata);
    }
    let state = match row.try_get::<_, &str>(1).map_err(metadata)? {
        "accepted" => ExportState::Accepted,
        "queued" => ExportState::Queued,
        "running" => ExportState::Running,
        "paused" => ExportState::Paused,
        "cancel_requested" => ExportState::CancelRequested,
        "succeeded" => ExportState::Succeeded,
        "partial" => ExportState::Partial,
        "failed" => ExportState::Failed,
        "cancelled" => ExportState::Cancelled,
        "skipped" => ExportState::Skipped,
        _ => return Err(Error::InvalidMetadata),
    };
    let result = row
        .try_get::<_, Option<&[u8]>>(4)
        .map_err(metadata)?
        .map(|v| {
            v.try_into()
                .map(Digest256::from_bytes)
                .map_err(|_| Error::InvalidMetadata)
        })
        .transpose()?;
    let reservation = row
        .try_get::<_, Option<&[u8]>>(16)
        .map_err(metadata)?
        .map(|v| {
            let artifact =
                ArtifactId::try_from(<[u8; 16]>::try_from(v).map_err(|_| Error::InvalidMetadata)?)
                    .map_err(|_| Error::InvalidMetadata)?;
            Ok(ExportReservation {
                artifact,
                limits: ExportLimits::new(revision(&row, 17)?, revision(&row, 18)?)
                    .map_err(|_| Error::InvalidMetadata)?,
            })
        })
        .transpose()?;
    if number(&row, 3)? != u64::from(state == ExportState::Succeeded)
        || result.is_some() != (state == ExportState::Succeeded)
        || (matches!(state, ExportState::Running | ExportState::Succeeded) && reservation.is_none())
    {
        return Err(Error::InvalidMetadata);
    }
    let snapshot = u64::try_from(row.try_get::<_, i64>(7).map_err(metadata)?)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(Error::InvalidMetadata)?;
    let catalog: OwnerResourceId = fixed::<16>(&row, 5)?
        .try_into()
        .map_err(|_| Error::InvalidMetadata)?;
    let frontier_revision = revision(&row, 6)?;
    let rows = tx.query("SELECT coalesce(b.shard_id,f.shard_id)::bigint,coalesce(b.owner_epoch,f.owner_epoch)::text,coalesce(b.last_complete_sequence,f.last_complete_sequence)::text,b.shard_id IS NOT NULL AND f.shard_id IS NOT NULL AND b.last_complete_sequence=f.last_complete_sequence FROM (SELECT * FROM control.job_input_bounds WHERE job_id=$1::bytea) b FULL JOIN (SELECT * FROM control.consumer_frontiers WHERE consumer_id=$2::bytea AND frontier_revision=$3::text::numeric) f USING(shard_id,owner_epoch) ORDER BY 1,coalesce(b.owner_epoch,f.owner_epoch) LIMIT 1025", &[&&job.as_bytes()[..],&&catalog.as_bytes()[..],&frontier_revision.to_string()]).await.map_err(backend)?;
    if rows.len() > 1024 {
        return Err(Error::Capacity);
    }
    let mut bounds = Vec::new();
    bounds
        .try_reserve_exact(rows.len())
        .map_err(|_| Error::Capacity)?;
    for row in rows {
        if !row.try_get::<_, bool>(3).map_err(metadata)? {
            return Err(Error::InvalidMetadata);
        }
        bounds.push(CutBound {
            shard_id: u32::try_from(row.try_get::<_, i64>(0).map_err(metadata)?)
                .map_err(|_| Error::InvalidMetadata)?,
            owner_epoch: revision(&row, 1)?,
            complete_through: number(&row, 2)?,
        });
    }
    let query = EventQuery {
        dataset,
        event_id: EventId::from_bytes(fixed::<32>(&row, 9)?),
        scope: QualifiedRef {
            dataset_id: dataset,
            dictionary_kind: DictionaryKind::VisibilityScope,
            allocation_domain: fixed::<16>(&row, 10)?
                .try_into()
                .map_err(|_| Error::InvalidMetadata)?,
            ordinal: Ordinal::new(revision(&row, 11)?).map_err(|_| Error::InvalidMetadata)?,
        },
        topology_generation: TopologyGeneration::new(number(&row, 8)?),
        bounds,
        snapshot_id: snapshot,
    };
    query.validate(1).map_err(|_| Error::InvalidMetadata)?;
    let mut observed = ExportJob {
        selection: ExportSelection {
            job,
            principal: PrincipalId::try_from(fixed::<16>(&row, 0)?)
                .map_err(|_| Error::InvalidMetadata)?,
            query,
            catalog,
            frontier_revision,
            destination: ExportDestination {
                id: fixed::<16>(&row, 12)?
                    .try_into()
                    .map_err(|_| Error::InvalidMetadata)?,
                binding_sha256: Digest256::from_bytes(fixed::<32>(&row, 13)?),
            },
            config_revision: revision(&row, 14)?,
            policy_revision: revision(&row, 15)?,
        },
        state,
        revision: revision(&row, 2)?,
        reservation,
        result,
        files: None,
    };
    observed.files = completed_files(tx, &observed).await?;
    Ok(Some(observed))
}

async fn completed_files(
    tx: &Transaction<'_>,
    job: &ExportJob,
) -> Result<Option<crate::export::files::StoredExport>, Error> {
    let row = tx.query_opt("SELECT substring(a.artifact_id FROM 1 FOR 17),left(a.manifest_bytes::text,21),substring(a.manifest_sha256 FROM 1 FOR 33),left(a.total_bytes::text,21),left(f.byte_count::text,21),left(f.row_count::text,21),substring(f.sha256 FROM 1 FOR 33),a.dataset_id=j.dataset_id AND a.principal_id=j.principal_id AND a.destination_id=e.destination_id AND a.destination_binding_sha256=e.destination_binding_sha256 AND a.manifest_sha256=j.result_manifest_digest AND a.completed_at_us=j.completed_at_us AND a.file_count=1 AND a.manifest_format='bigbrotr-signed-events-export/1' AND f.relative_path='events.jsonl' AND f.format='nostr-jsonl/1' AND (SELECT count(*) FROM control.artifact_files z WHERE z.artifact_id=a.artifact_id)=1 FROM control.artifacts a JOIN control.jobs j USING(job_id) JOIN control.export_jobs e USING(job_id) LEFT JOIN control.artifact_files f ON f.artifact_id=a.artifact_id AND f.file_id='events' WHERE a.job_id=$1::bytea", &[&&job.selection.job.as_bytes()[..]]).await.map_err(backend)?;
    let Some(row) = row else {
        return if job.state == ExportState::Succeeded {
            Err(Error::InvalidMetadata)
        } else {
            Ok(None)
        };
    };
    if job.state != ExportState::Succeeded
        || row.try_get::<_, Option<bool>>(7).map_err(metadata)? != Some(true)
    {
        return Err(Error::InvalidMetadata);
    }
    let unit = job.reservation.ok_or(Error::InvalidMetadata)?;
    let value = crate::export::files::StoredExport {
        data: crate::export::jsonl::JsonlFile {
            bytes: number(&row, 4)?,
            rows: number(&row, 5)?,
            sha256: Digest256::from_bytes(fixed::<32>(&row, 6)?),
        },
        manifest_bytes: revision(&row, 1)?,
        manifest_sha256: Digest256::from_bytes(fixed::<32>(&row, 2)?),
    };
    if fixed::<16>(&row, 0)? != *unit.artifact.as_bytes()
        || value.manifest_bytes > 262144
        || value.data.rows > 1
        || (value.data.rows == 0) != (value.data.bytes == 0)
        || value.data.bytes.checked_add(value.manifest_bytes) != Some(number(&row, 3)?)
        || number(&row, 3)? > unit.limits.maximum()
        || Some(value.manifest_sha256) != job.result
    {
        return Err(Error::InvalidMetadata);
    }
    Ok(Some(value))
}
