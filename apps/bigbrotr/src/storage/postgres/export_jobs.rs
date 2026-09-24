//! Atomic export acceptance. A reply identifies durable work, not permission to
//! execute it. Temporary native-reader custody never owns the job's export pin.
use super::{
    ArchivePinError, ArchivePinRequest, ControlSession, ScopeGrantAllocation,
    application_transaction,
    browser_reauthentication::authenticated_session,
    browser_sessions::*,
    require,
    snapshot_leases::{PinPurpose, insert_pin},
};
use crate::{
    export::destination::CheckedDestination,
    management::{
        credentials::random,
        jobs::{ExportCommand, JobReceipt},
        sessions::{BrowserSecret, SessionError},
    },
    storage::postgres::ingest_registry::ControlOwner,
};
use bigbrotr_model::{CommandKey, DatasetId, JobId};
use std::{cell::Cell, time::Duration};
use tokio_postgres::Transaction;

impl ControlSession {
    pub async fn require_export_jobs(&mut self) -> Result<(), SessionError> {
        if self.export_jobs_available().await? {
            Ok(())
        } else {
            Err(SessionError::Prerequisite)
        }
    }

    /// Probe the optional protocol without referencing813 objects on an older
    /// valid schema. Absence or insufficient job privileges disables this API;
    /// a selected destination makes false a fatal startup prerequisite instead.
    pub async fn export_jobs_available(&mut self) -> Result<bool, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 812)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let present: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE engine='postgres-control' AND migration=813)", &[])
            .await.map_err(backend)?.try_get(0).map_err(metadata)?;
        let valid = if present {
            let valid: bool = tx.query_one("SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege)) AND has_any_column_privilege(CURRENT_USER,'control.source_streams','UPDATE') AND has_function_privilege(CURRENT_USER,'control.export_command_digest(control.export_jobs,control.management_commands)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.command_u64_bytes(control.u64)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.scope_pin_live(bytea,bytea,numeric,bytea)','EXECUTE') FROM (VALUES ('control.jobs',ARRAY['SELECT','INSERT','UPDATE']),('control.job_input_bounds',ARRAY['SELECT','INSERT']),('control.export_jobs',ARRAY['SELECT','INSERT']),('control.management_commands',ARRAY['SELECT','INSERT']),('control.management_audit',ARRAY['SELECT','INSERT']),('control.scope_grants',ARRAY['SELECT'])) t(name,privileges) CROSS JOIN LATERAL unnest(t.privileges) p(privilege)", &[]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            let resume_schema: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE engine='postgres-control' AND migration=814)", &[]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            // The additive transition guard reads this ledger even for existing
            // execution. Refuse missing grants before accepting new job work.
            let resume_guard = if resume_schema {
                tx.query_one("SELECT has_table_privilege(CURRENT_USER,'control.export_resume_commands','SELECT')", &[]).await.map_err(backend)?.try_get::<_,bool>(0).map_err(metadata)?
            } else {
                true
            };
            valid && resume_guard
        } else {
            false
        };
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(valid)
    }

    /// `None` native inputs still allow an authenticated historical retry. New
    /// work requires the actual retained allocation and initialized destination.
    /// Once COMMIT starts, a lost reply must resolve this command key; never
    /// compensate by releasing the newly assigned durable export obligation.
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_export(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &ExportCommand,
        allocation: Option<&ScopeGrantAllocation<'_, '_>>,
        destination: Option<&CheckedDestination>,
        commit_started: &Cell<bool>,
    ) -> Result<JobReceipt, SessionError> {
        command.validate(self.request.dataset)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 813)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = accept(
            &tx,
            self.request.dataset,
            self.identity,
            self.validated_policy,
            token,
            csrf,
            key,
            command,
            allocation,
            destination,
        )
        .await;
        let outcome = match outcome {
            Err(SessionError::ArchiveUnavailable) => Err(SessionError::ArchiveUnavailable),
            other => terminal(&tx, other)?,
        };
        match outcome {
            Ok(receipt) if receipt.created => {
                commit_started.set(true);
                if let Err(error) = tx.commit().await {
                    if let Some(rejection) = error.as_db_error() {
                        if rejection.code().code() == "BB001" {
                            self.available = true;
                            return Err(SessionError::Denied);
                        }
                        return Err(SessionError::Backend);
                    }
                    return Err(SessionError::CommitUnknown);
                }
                // This is an immutable receipt. No live native assertion or
                // successful export result is inferred from the SQL commit.
                self.available = true;
                Ok(receipt)
            }
            outcome => {
                tx.rollback().await.map_err(backend)?;
                self.available = true;
                outcome
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept(
    tx: &Transaction<'_>,
    dataset_id: DatasetId,
    owner: ControlOwner,
    policy: super::PolicySnapshot,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    key: CommandKey,
    command: &ExportCommand,
    allocation: Option<&ScopeGrantAllocation<'_, '_>>,
    destination: Option<&CheckedDestination>,
) -> Result<JobReceipt, SessionError> {
    let current = authenticated_session(
        tx,
        dataset_id,
        &token.session_digest(),
        &csrf.csrf_digest(),
        None,
    )
    .await?;
    let digest = command.digest(dataset_id, current.principal, key)?;
    let dataset = &dataset_id.as_bytes()[..];
    let actor = &current.principal.as_bytes()[..];
    let request_key = &key.as_bytes()[..];
    if let Some(row) = tx.query_opt("SELECT c.dataset_id=$3::bytea AND c.action='job.export.accept' AND c.request_digest=$4::bytea,substring(e.job_id FROM 1 FOR 17),c.accepted_at_us FROM control.management_commands c LEFT JOIN control.export_jobs e USING(actor_id,request_key) WHERE c.actor_id=$1::bytea AND c.request_key=$2::bytea", &[&actor,&request_key,&dataset,&&digest.as_bytes()[..]]).await.map_err(backend)? {
        if !row.try_get::<_,bool>(0).map_err(metadata)? { return Err(SessionError::Conflict); }
        return Ok(JobReceipt { command_key: key,
            job_id: JobId::try_from(fixed::<16>(&row,1)?).map_err(|_| SessionError::InvalidMetadata)?,
            accepted_at_us: row.try_get(2).map_err(metadata)?, created: false });
    }
    if command.expected_config_revision.get() != policy.config_revision
        || command.expected_policy_revision.get() != policy.policy_revision
    {
        return Err(SessionError::Conflict);
    }
    let scope = command.query.scope;
    let permitted: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.principal_grants WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='data.read' AND resource_kind='dataset' AND resource_id=$1::bytea) AND NOT EXISTS(SELECT 1 FROM (VALUES ('history.read'),('export'),('private-export')) wanted(action) WHERE NOT EXISTS(SELECT 1 FROM control.scope_grants g WHERE g.dataset_id=$1::bytea AND g.principal_id=$2::bytea AND g.action=wanted.action AND g.dictionary_kind=4 AND g.allocation_domain=$3::bytea AND g.ordinal=$4::text::numeric AND g.enabled))", &[&dataset,&actor,&&scope.allocation_domain.as_bytes()[..],&scope.ordinal.get().to_string()]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if !permitted {
        return Err(SessionError::Forbidden);
    }
    let allocation = allocation.ok_or(SessionError::ArchiveUnavailable)?;
    let destination = destination.ok_or(SessionError::ArchiveUnavailable)?;
    if destination.dataset() != dataset_id
        || destination.id() != command.destination.id
        || destination.binding_digest() != command.destination.binding_sha256
    {
        return Err(SessionError::Conflict);
    }
    // Filesystem verification and owned custody came from a supervised blocking
    // operation. Never perform filesystem I/O while polling the Control future.
    allocation
        .verify(tx, dataset_id, scope, &command.allocation)
        .await?;
    let read = allocation.visibility.read();
    let event = command.validate(dataset_id)?;
    let time = now(tx).await?;
    if time >= current.expires {
        return Err(SessionError::Denied);
    }
    let audit = random::<16>().map_err(|_| SessionError::Unavailable)?;
    let job_id = JobId::try_from(random::<16>().map_err(|_| SessionError::Unavailable)?)
        .map_err(|_| SessionError::Unavailable)?;
    let lease_id = random::<16>().map_err(|_| SessionError::Unavailable)?;
    let job = &job_id.as_bytes()[..];
    let config = command.expected_config_revision.get().to_string();
    let policy = command.expected_policy_revision.get().to_string();
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision) VALUES($1::bytea,$2::bytea,$3::bytea,'job.export.accept','accepted',$4::bigint,$5::text::numeric,$6::bytea,$7::text::numeric)", &[&&audit[..],&dataset,&actor,&time,&config,&request_key,&policy]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,'job.export.accept',$4::bytea,$5::text::numeric,$6::text::numeric,$7::bytea,$8::bigint)", &[&actor,&request_key,&dataset,&&digest.as_bytes()[..],&config,&policy,&&audit[..],&time]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_topology_generation,export_job_protocol) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,'export',$6::text::numeric,$7::text::numeric,$8::bigint,$9::text::numeric,1)", &[&job,&dataset,&actor,&request_key,&&digest.as_bytes()[..],&config,&policy,&time,&read.topology_generation.get().to_string()]).await.map_err(backend)?;
    for bound in &read.bounds {
        tx.execute("INSERT INTO control.job_input_bounds(job_id,dataset_id,topology_generation,shard_id,owner_epoch,last_complete_sequence) VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::bigint,$5::text::numeric,$6::text::numeric)", &[&job,&dataset,&read.topology_generation.get().to_string(),&i64::from(bound.shard_id),&bound.owner_epoch.to_string(),&bound.complete_through.to_string()]).await.map_err(backend)?;
    }
    // Scheduling lifetime is bounded; retention remains until explicit terminal
    // cleanup. Execution always needs a fresh reader pin and current grants.
    insert_pin(
        tx,
        allocation.capture,
        &ArchivePinRequest {
            lease_id,
            frontier_revision: command.allocation.frontier_revision.get(),
            ttl: Duration::from_secs(30),
        },
        owner,
        PinPurpose::Export(job_id),
    )
    .await
    .map_err(pin_error)?;
    tx.execute("INSERT INTO control.export_jobs(job_id,dataset_id,actor_id,request_key,format,visibility,dictionary_kind,allocation_domain,ordinal,event_id,page_size,catalog_id,frontier_revision,destination_id,destination_binding_sha256,pin_lease_id,allocation_pin_id,session_id,actor_credential_revision,actor_authentication_revision,actor_grant_revision) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,'signed-events-jsonl','private',4,$5::bytea,$6::text::numeric,$7::bytea,$8::smallint,$9::bytea,$10::text::numeric,$11::bytea,$12::bytea,$13::bytea,$14::bytea,$15::bytea,$16::text::numeric,$17::text::numeric,$18::text::numeric)", &[&job,&dataset,&actor,&request_key,&&scope.allocation_domain.as_bytes()[..],&scope.ordinal.get().to_string(),&&event.as_bytes()[..],&(command.query.page_size.unwrap_or(100) as i16),&&command.allocation.catalog_id.as_bytes()[..],&command.allocation.frontier_revision.get().to_string(),&&command.destination.id.as_bytes()[..],&&command.destination.binding_sha256.as_bytes()[..],&&lease_id[..],&&allocation.pin.lease_id[..],&&current.id[..],&revision(&current.account,1)?.to_string(),&revision(&current.account,2)?.to_string(),&revision(&current.account,3)?.to_string()]).await.map_err(backend)?;
    if tx
        .execute(
            "UPDATE control.jobs SET input_complete=true WHERE job_id=$1::bytea",
            &[&job],
        )
        .await
        .map_err(backend)?
        != 1
    {
        return Err(SessionError::InvalidMetadata);
    }
    allocation
        .capture
        .verify()
        .await
        .map_err(|_| SessionError::ArchiveUnavailable)?;
    Ok(JobReceipt {
        command_key: key,
        job_id,
        accepted_at_us: time,
        created: true,
    })
}

fn pin_error(error: ArchivePinError) -> SessionError {
    match error {
        ArchivePinError::InvalidInput => SessionError::InvalidInput,
        ArchivePinError::InvalidMetadata => SessionError::InvalidMetadata,
        ArchivePinError::Prerequisite => SessionError::Prerequisite,
        ArchivePinError::Conflict => SessionError::Conflict,
        ArchivePinError::ResourceLimit => SessionError::Capacity,
        ArchivePinError::Unavailable => SessionError::Unavailable,
        ArchivePinError::Backend => SessionError::Backend,
        ArchivePinError::CommitUnknown => SessionError::CommitUnknown,
    }
}
