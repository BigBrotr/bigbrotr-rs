//! Explicit actor-owned resume; immutable historical retries never dispatch work.
use super::{
    ControlSession, application_transaction, browser_reauthentication::authenticated_session,
    browser_sessions::*, require,
};
use crate::{
    export::destination::CheckedDestination,
    management::{
        credentials::random,
        grants::Revision,
        jobs::{ResumeExportCommand, ResumeExportReceipt},
        sessions::{BrowserSecret, SessionError},
    },
    storage::postgres::ingest_registry::ControlOwner,
};
use bigbrotr_model::{CommandKey, DatasetId, JobId};
use std::cell::Cell;
use tokio_postgres::Transaction;

impl ControlSession {
    /// The command is optional on older schemas; selecting it requires explicit
    /// migration and grants, never implicit DDL or privilege expansion.
    pub async fn require_export_resume(&mut self) -> Result<(), SessionError> {
        self.require_export_execution().await?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 814)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        let valid: bool = tx.query_one("SELECT has_table_privilege(CURRENT_USER,'control.export_resume_commands','SELECT') AND has_table_privilege(CURRENT_USER,'control.export_resume_commands','INSERT') AND has_function_privilege(CURRENT_USER,'control.export_resume_digest(control.export_resume_commands,control.management_commands)','EXECUTE')", &[]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
        if !valid {
            return Err(SessionError::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    /// `None` destination still permits an authenticated historical retry.
    /// The caller retains checked destination custody through Control driver
    /// join on cancellation/unknown COMMIT, as for initial export acceptance.
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_export(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        job: JobId,
        command: &ResumeExportCommand,
        destination: Option<&CheckedDestination>,
        commit_started: &Cell<bool>,
    ) -> Result<ResumeExportReceipt, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 814)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = resume(
            &tx,
            self.request.dataset,
            self.identity,
            self.validated_policy,
            token,
            csrf,
            key,
            job,
            command,
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
async fn resume(
    tx: &Transaction<'_>,
    dataset_id: DatasetId,
    owner: ControlOwner,
    policy: super::PolicySnapshot,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    key: CommandKey,
    job_id: JobId,
    command: &ResumeExportCommand,
    destination: Option<&CheckedDestination>,
) -> Result<ResumeExportReceipt, SessionError> {
    let current = authenticated_session(
        tx,
        dataset_id,
        &token.session_digest(),
        &csrf.csrf_digest(),
        None,
    )
    .await?;
    let digest = command.digest(dataset_id, current.principal, key, job_id);
    let dataset = &dataset_id.as_bytes()[..];
    let actor = &current.principal.as_bytes()[..];
    let request_key = &key.as_bytes()[..];
    let job = &job_id.as_bytes()[..];
    // Current session authentication precedes historical identity resolution.
    // Do not join the old session or require today's destination/input grants.
    if let Some(row) = tx.query_opt("SELECT c.dataset_id=$3::bytea AND c.action='job.export.resume' AND c.request_digest=$4::bytea,substring(r.job_id FROM 1 FOR 17),left((r.expected_job_revision+1)::text,21),c.accepted_at_us FROM control.management_commands c LEFT JOIN control.export_resume_commands r USING(actor_id,request_key) WHERE c.actor_id=$1::bytea AND c.request_key=$2::bytea", &[&actor,&request_key,&dataset,&&digest.as_bytes()[..]]).await.map_err(backend)? {
        if !row.try_get::<_,bool>(0).map_err(metadata)? { return Err(SessionError::Conflict); }
        if fixed::<16>(&row,1)? != *job_id.as_bytes() { return Err(SessionError::InvalidMetadata); }
        // A slow query cannot let even a historical receipt escape after expiry.
        if now(tx).await? >= current.expires { return Err(SessionError::Denied); }
        return Ok(ResumeExportReceipt { command_key:key, job_id,
            job_revision:Revision::try_from(revision(&row,2)?.to_string()).map_err(|_| SessionError::InvalidMetadata)?,
            accepted_at_us:row.try_get(3).map_err(metadata)?, created:false });
    }
    if command.expected_config_revision.get() != policy.config_revision
        || command.expected_policy_revision.get() != policy.policy_revision
    {
        return Err(SessionError::Conflict);
    }
    let next = command
        .expected_job_revision
        .get()
        .checked_add(1)
        .ok_or(SessionError::Capacity)?;
    let next = Revision::try_from(next.to_string()).map_err(|_| SessionError::Capacity)?;
    let detail = tx.query_opt("SELECT substring(e.destination_id FROM 1 FOR 17),substring(e.destination_binding_sha256 FROM 1 FOR 33) FROM control.export_jobs e JOIN control.jobs j USING(job_id) WHERE e.job_id=$1::bytea AND e.dataset_id=$2::bytea AND e.actor_id=$3::bytea AND j.export_job_protocol=1", &[&job,&dataset,&actor]).await.map_err(backend)?.ok_or(SessionError::Conflict)?;
    // This acquires consumer, job and sorted source rows in the owning order,
    // after the already held Control/dataset/account/session. No job lock first.
    tx.query_one(
        "SELECT control.require_export_execution_authority($1::bytea)",
        &[&job],
    )
    .await
    .map_err(|e| {
        if e.code().is_some_and(|c| c.code() == "BB001") {
            SessionError::Forbidden
        } else {
            backend(e)
        }
    })?;
    let selected: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.jobs j JOIN control.export_execution_units u USING(job_id) JOIN control.export_jobs e USING(job_id) JOIN control.snapshot_leases l ON l.lease_id=e.pin_lease_id WHERE j.job_id=$1::bytea AND j.state='paused' AND j.job_revision=$2::text::numeric AND j.checkpoint_revision=0 AND j.result_manifest_digest IS NULL AND l.state='active' AND u.reservation_transaction<>pg_current_xact_id())", &[&job,&command.expected_job_revision.get().to_string()]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if !selected {
        return Err(SessionError::Conflict);
    }
    let destination = destination.ok_or(SessionError::ArchiveUnavailable)?;
    if destination.dataset() != dataset_id
        || destination.id().as_bytes() != &fixed::<16>(&detail, 0)?
        || destination.binding_digest().as_bytes() != &fixed::<32>(&detail, 1)?
    {
        return Err(SessionError::Conflict);
    }
    let time = now(tx).await?;
    if time >= current.expires {
        return Err(SessionError::Denied);
    }
    let audit = random::<16>().map_err(|_| SessionError::Unavailable)?;
    let config = command.expected_config_revision.get().to_string();
    let policy = command.expected_policy_revision.get().to_string();
    let expected = command.expected_job_revision.get().to_string();
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision) VALUES($1::bytea,$2::bytea,$3::bytea,'job.export.resume','accepted',$4::bigint,$5::text::numeric,$6::bytea,$7::text::numeric)", &[&&audit[..],&dataset,&actor,&time,&config,&request_key,&policy]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,'job.export.resume',$4::bytea,$5::text::numeric,$6::text::numeric,$7::bytea,$8::bigint)", &[&actor,&request_key,&dataset,&&digest.as_bytes()[..],&config,&policy,&&audit[..],&time]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.export_resume_commands(actor_id,request_key,dataset_id,job_id,expected_job_revision,session_id,actor_credential_revision,actor_authentication_revision,actor_grant_revision) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::text::numeric,$6::bytea,$7::text::numeric,$8::text::numeric,$9::text::numeric)", &[&actor,&request_key,&dataset,&job,&expected,&&current.id[..],&revision(&current.account,1)?.to_string(),&revision(&current.account,2)?.to_string(),&revision(&current.account,3)?.to_string()]).await.map_err(backend)?;
    let changed = tx.execute("UPDATE control.jobs SET state='queued',job_revision=job_revision+1,reason_code=NULL,owner_resource_id=$3::bytea,owner_epoch=$4::text::numeric,owner_incarnation=$5::bytea WHERE job_id=$1::bytea AND job_revision=$2::text::numeric AND state='paused'", &[&job,&expected,&&owner.resource_id.as_bytes()[..],&owner.epoch.get().to_string(),&&owner.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
    if changed != 1 {
        return Err(SessionError::Conflict);
    }
    Ok(ResumeExportReceipt {
        command_key: key,
        job_id,
        job_revision: next,
        accepted_at_us: time,
        created: true,
    })
}
