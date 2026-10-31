//! Atomic finite acquisition intent. A committed receipt grants no socket or
//! admission authority; dispatch must recheck the live job, source and policy.
use super::{
    ControlSession, PolicySnapshot, application_transaction,
    browser_reauthentication::authenticated_session, browser_sessions::*, require,
};
use crate::{
    acquisition::job::AcquisitionCommand,
    management::{
        credentials::random,
        jobs::JobReceipt,
        sessions::{BrowserSecret, SessionError},
    },
};
use bigbrotr_model::{CommandKey, DatasetId, JobId};
use std::cell::Cell;
use tokio_postgres::Transaction;

impl ControlSession {
    /// The initial listener probes its finite acceptance/status dependencies
    /// after session and scope prerequisites, before binding. No implicit DDL.
    pub async fn require_acquisition_jobs(&mut self) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 22)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require_job_update_schema(&tx).await?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let valid: bool = tx.query_one("
            SELECT bool_and(has_table_privilege(CURRENT_USER, t.name, p.privilege))
                AND has_any_column_privilege(CURRENT_USER, 'control.relay_enrollments', 'UPDATE')
                AND has_function_privilege(CURRENT_USER,
                    'control.acquisition_command_digest(control.acquisition_jobs,control.management_commands)', 'EXECUTE')
                AND has_function_privilege(CURRENT_USER,
                    'control.command_u64_bytes(control.u64)', 'EXECUTE')
            FROM (VALUES
                ('control.jobs', ARRAY['SELECT','INSERT','UPDATE']),
                ('control.acquisition_jobs', ARRAY['SELECT','INSERT']),
                ('control.job_admissions', ARRAY['SELECT','INSERT']),
                ('control.job_input_bounds', ARRAY['SELECT']),
                ('control.topology_streams', ARRAY['SELECT']),
                ('control.acquisition_scopes', ARRAY['SELECT']),
                ('control.relay_enrollments', ARRAY['SELECT']),
                ('control.management_commands', ARRAY['SELECT','INSERT']),
                ('control.management_audit', ARRAY['SELECT','INSERT'])
            ) t(name, privileges)
            CROSS JOIN LATERAL unnest(t.privileges) p(privilege)
        ", &[]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
        if !valid {
            return Err(SessionError::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    /// Authenticate before resolving historical keys. Fresh permission checks
    /// apply only to new work; replay never updates a job or its deadline.
    pub async fn accept_acquisition(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &AcquisitionCommand,
        commit_started: &Cell<bool>,
    ) -> Result<JobReceipt, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 22)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require_job_update_schema(&tx).await?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = accept(
            &tx,
            self.request.dataset,
            self.validated_policy,
            token,
            csrf,
            key,
            command,
        )
        .await;
        let outcome = terminal(&tx, outcome)?;
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
                    // The key, rather than a new acceptance, resolves a lost
                    // reply. Cancellation also leaves this connection poisoned.
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

/// The shared migration lock and verified ledger belong to `application_transaction`.
/// A deployment stopped at0027 must explicitly apply its corrective0028 before
/// ordinary mutations. Earlier deployments retain their existing prerequisites.
pub(super) async fn require_job_update_schema(tx: &Transaction<'_>) -> Result<(), SessionError> {
    let uncorrected: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM control.schema_versions
                WHERE engine = 'postgres-control' AND migration = 27)
             AND NOT EXISTS (SELECT 1 FROM control.schema_versions
                WHERE engine = 'postgres-control' AND migration = 28)",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(metadata)?;
    if uncorrected {
        return Err(SessionError::Prerequisite);
    }
    Ok(())
}

async fn accept(
    tx: &Transaction<'_>,
    dataset_id: DatasetId,
    policy: PolicySnapshot,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    key: CommandKey,
    command: &AcquisitionCommand,
) -> Result<JobReceipt, SessionError> {
    let current = authenticated_session(
        tx,
        dataset_id,
        &token.session_digest(),
        &csrf.csrf_digest(),
        None,
    )
    .await?;
    let digest = command.digest(dataset_id, current.principal, key);
    let dataset = &dataset_id.as_bytes()[..];
    let actor = &current.principal.as_bytes()[..];
    let request_key = &key.as_bytes()[..];
    if let Some(row) = tx
        .query_opt(
            "SELECT c.dataset_id=$3::bytea AND c.action='job.acquisition.accept'
                AND c.request_digest=$4::bytea,
                substring(a.job_id FROM 1 FOR 17), c.accepted_at_us
         FROM control.management_commands c
         LEFT JOIN control.acquisition_jobs a USING(actor_id,request_key)
         WHERE c.actor_id=$1::bytea AND c.request_key=$2::bytea",
            &[&actor, &request_key, &dataset, &&digest.as_bytes()[..]],
        )
        .await
        .map_err(backend)?
    {
        if !row.try_get::<_, bool>(0).map_err(metadata)? {
            return Err(SessionError::Conflict);
        }
        return Ok(JobReceipt {
            command_key: key,
            job_id: JobId::try_from(fixed::<16>(&row, 1)?)
                .map_err(|_| SessionError::InvalidMetadata)?,
            accepted_at_us: row.try_get(2).map_err(metadata)?,
            created: false,
        });
    }
    if command.expected_config_revision.get() != policy.config_revision
        || command.expected_policy_revision.get() != policy.policy_revision
    {
        return Err(SessionError::Conflict);
    }
    let permitted: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.principal_grants
         WHERE dataset_id=$1::bytea AND principal_id=$2::bytea
           AND action='roles.control' AND resource_kind='dataset' AND resource_id=$1::bytea)",
            &[&dataset, &actor],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(metadata)?;
    if !permitted {
        return Err(SessionError::Forbidden);
    }

    // The dataset/account/session locks precede the source lock. Full route,
    // bans, visibility and document semantics are freshly checked at execution.
    let scope = &command.scope_id.as_bytes()[..];
    let configured = tx.query_opt(
        "SELECT e.enabled AND s.policy_revision=d.policy_revision
                AND c.document->>'format'='bigbrotr-product-policy/3'
                AND p.document->>'format'='bigbrotr-product-policy/3'
                AND c.document->'settings'->'sync.enabled'='true'::jsonb
                AND p.document->'settings'->'sync.enabled'='true'::jsonb
         FROM control.acquisition_scopes s
         JOIN control.datasets d USING(dataset_id)
         JOIN control.config_revisions c ON c.dataset_id=d.dataset_id AND c.revision=d.config_revision
         JOIN control.policy_revisions p ON p.dataset_id=d.dataset_id AND p.revision=d.policy_revision
         JOIN control.relay_enrollments e ON e.dataset_id=s.dataset_id
              AND e.endpoint_id=s.endpoint_id AND e.access_scope_id=s.access_scope_id
         WHERE s.dataset_id=$1::bytea AND s.scope_id=$2::bytea FOR SHARE OF e",
        &[&dataset, &scope],
    ).await.map_err(backend)?;
    let configured = configured
        .map(|row| row.try_get::<_, Option<bool>>(0).map_err(metadata))
        .transpose()?
        .flatten()
        .unwrap_or(false);
    if !configured {
        return Err(SessionError::Conflict);
    }

    let time = now(tx).await?;
    if time >= current.expires {
        return Err(SessionError::Denied);
    }
    let duration =
        i64::try_from(command.deadline_us.get()).map_err(|_| SessionError::InvalidInput)?;
    let deadline = time
        .checked_add(duration)
        .ok_or(SessionError::InvalidMetadata)?;
    let audit = random::<16>().map_err(|_| SessionError::Unavailable)?;
    let job_id = JobId::try_from(random::<16>().map_err(|_| SessionError::Unavailable)?)
        .map_err(|_| SessionError::Unavailable)?;
    let job = &job_id.as_bytes()[..];
    let config = command.expected_config_revision.get().to_string();
    let policy = command.expected_policy_revision.get().to_string();

    tx.execute(
        "INSERT INTO control.management_audit
            (audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
         VALUES($1::bytea,$2::bytea,$3::bytea,'job.acquisition.accept','accepted',
                $4::bigint,$5::text::numeric,$6::bytea,$7::text::numeric)",
        &[&&audit[..], &dataset, &actor, &time, &config, &request_key, &policy],
    ).await.map_err(backend)?;
    tx.execute(
        "INSERT INTO control.management_commands
            (actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,
             policy_revision,audit_id,accepted_at_us)
         VALUES($1::bytea,$2::bytea,$3::bytea,'job.acquisition.accept',$4::bytea,
                $5::text::numeric,$6::text::numeric,$7::bytea,$8::bigint)",
        &[
            &actor,
            &request_key,
            &dataset,
            &&digest.as_bytes()[..],
            &config,
            &policy,
            &&audit[..],
            &time,
        ],
    )
    .await
    .map_err(backend)?;
    tx.execute(
        "INSERT INTO control.jobs
            (job_id,dataset_id,principal_id,request_key,request_digest,kind,
             config_revision,policy_revision,created_at_us,acquisition_job_protocol)
         VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,'acquisition',
                $6::text::numeric,$7::text::numeric,$8::bigint,1)",
        &[
            &job,
            &dataset,
            &actor,
            &request_key,
            &&digest.as_bytes()[..],
            &config,
            &policy,
            &time,
        ],
    )
    .await
    .map_err(backend)?;
    tx.execute(
        "INSERT INTO control.acquisition_jobs
            (job_id,dataset_id,actor_id,request_key,scope_id,duration_us,deadline_at_us,session_id,
             actor_credential_revision,actor_authentication_revision,actor_grant_revision)
         VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,$6::bigint,$7::bigint,$8::bytea,
                $9::text::numeric,$10::text::numeric,$11::text::numeric)",
        &[
            &job,
            &dataset,
            &actor,
            &request_key,
            &scope,
            &duration,
            &deadline,
            &&current.id[..],
            &revision(&current.account, 1)?.to_string(),
            &revision(&current.account, 2)?.to_string(),
            &revision(&current.account, 3)?.to_string(),
        ],
    )
    .await
    .map_err(backend)?;
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
    Ok(JobReceipt {
        command_key: key,
        job_id,
        accepted_at_us: time,
        created: true,
    })
}
