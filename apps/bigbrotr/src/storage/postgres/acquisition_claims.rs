//! Queued ownership and historical observation. Neither dispatches a socket or
//! adopts native work. Actual execution rechecks the job at every effect boundary.
use super::{
    ControlSession,
    acquisition_jobs::require_job_update_schema,
    application_transaction,
    browser_sessions::{fixed, lock_dataset, metadata, revision, terminal},
    require,
};
use crate::{
    acquisition::job::{
        AcquisitionCandidate, AcquisitionJob, AcquisitionOwner, AcquisitionScan, JobDuration,
    },
    management::{jobs::JobState, sessions::SessionError as Error},
    runtime::OwnedPermit,
};
use bigbrotr_model::{AcquisitionScopeId, DatasetId, JobId, OwnerEpoch, PrincipalId};
use std::cell::Cell;
use tokio_postgres::{Row, Transaction};

fn backend(error: tokio_postgres::Error) -> Error {
    match error.code().map(|code| code.code()) {
        Some("BB001") => Error::Forbidden,
        Some("23514") => Error::Conflict,
        Some("42501") => Error::Prerequisite,
        _ => Error::Backend,
    }
}

const ADMISSION_CUT: &str = "
    SELECT left(coalesce((SELECT admission_ordinal FROM control.job_admissions
        ORDER BY admission_ordinal DESC LIMIT 1),0)::text,21)";

const SCAN_ADMISSIONS: &str = "
    WITH batch AS MATERIALIZED (
        SELECT job_id,admission_ordinal FROM control.job_admissions
        WHERE admission_ordinal > $1::text::numeric AND admission_ordinal <= $2::text::numeric
        ORDER BY admission_ordinal LIMIT 32
    )
    SELECT substring(b.job_id FROM 1 FOR 17),left(b.admission_ordinal::text,21),
        (SELECT CASE WHEN j.dataset_id=$3::bytea AND j.kind='acquisition'
            AND j.acquisition_job_protocol=1 AND j.checkpoint_revision=0
            AND j.input_complete AND j.input_topology_generation IS NULL
            AND ((j.state='accepted' AND j.job_revision=1
                    AND j.owner_resource_id IS NULL AND j.owner_epoch IS NULL AND j.owner_incarnation IS NULL)
                OR (j.state='queued' AND j.job_revision=2
                    AND j.owner_resource_id=$4::bytea AND j.owner_epoch=$5::text::numeric
                    AND j.owner_incarnation=$6::bytea))
         THEN true ELSE false END FROM control.jobs j WHERE j.job_id=b.job_id)
    FROM batch b ORDER BY b.admission_ordinal";

impl ControlSession {
    /// Execution is selected independently of the private listener. Check its
    /// schema, function and row-lock permissions before publishing startup.
    pub async fn require_acquisition_execution(&mut self) -> Result<(), Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 24)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require_job_update_schema(&tx).await?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let valid: bool = tx
            .query_one(EXECUTION_PRIVILEGES, &[])
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(metadata)?;
        if !valid {
            return Err(Error::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(Error::Unavailable);
        }
        self.available = true;
        Ok(())
    }

    /// Nominate from at most32 admissions, before eligibility filtering. No claim,
    /// source action, lease renewal or scientific progress is produced here.
    pub async fn nominate_acquisition(
        &mut self,
        scan: Option<AcquisitionScan>,
        permit: OwnedPermit,
    ) -> Result<AcquisitionCandidate, Error> {
        if permit.request().units().data_bytes() < AcquisitionScan::REQUIRED_BYTES
            || permit.request().units().items() == 0
        {
            return Err(Error::Capacity);
        }
        if scan.is_some_and(|scan| scan.after_ordinal >= scan.admission_cut) {
            return Err(Error::InvalidInput);
        }
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 23)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let mut scan = match scan {
            Some(scan) => scan,
            None => {
                // Committed admission order is monotone under0815's dataset
                // lock. New admissions cannot enter this fixed pass afterward.
                let row = tx.query_one(ADMISSION_CUT, &[]).await.map_err(backend)?;
                let text: &str = row.try_get(0).map_err(metadata)?;
                let admission_cut = text
                    .parse::<u64>()
                    .ok()
                    .filter(|n| n.to_string() == text)
                    .ok_or(Error::InvalidMetadata)?;
                AcquisitionScan {
                    admission_cut,
                    after_ordinal: 0,
                }
            }
        };
        // Materialize32 indexed admission positions before inspecting jobs.
        // Correlated primary-key lookups avoid a join over unbounded history.
        let rows = tx
            .query(
                SCAN_ADMISSIONS,
                &[
                    &scan.after_ordinal.to_string(),
                    &scan.admission_cut.to_string(),
                    &&self.request.dataset.as_bytes()[..],
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                    &&self.identity.process_incarnation.as_bytes()[..],
                ],
            )
            .await
            .map_err(backend)?;
        if rows.len() > 32 {
            return Err(Error::InvalidMetadata);
        }
        let mut job = None;
        for row in &rows {
            let ordinal = revision(row, 1)?;
            if ordinal <= scan.after_ordinal || ordinal > scan.admission_cut {
                return Err(Error::InvalidMetadata);
            }
            scan.after_ordinal = ordinal;
            if row.try_get::<_, bool>(2).map_err(metadata)? {
                job = Some(
                    fixed::<16>(row, 0)?
                        .try_into()
                        .map_err(|_| Error::InvalidMetadata)?,
                );
                break;
            }
        }
        if job.is_none() && rows.len() < 32 {
            scan.after_ordinal = scan.admission_cut;
        }
        drop(rows);
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(Error::Unavailable);
        }
        self.available = true;
        Ok(AcquisitionCandidate { scan, job })
    }

    /// Internal historical lookup, including after expiry/withdrawal/owner loss.
    /// An external status surface must authenticate and authorize its reader.
    pub async fn read_acquisition_job(
        &mut self,
        job: JobId,
    ) -> Result<Option<AcquisitionJob>, Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 23)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let result = read(&tx, self.request.dataset, job).await?;
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(result)
    }

    /// Claim exactly one accepted job under its original actor, scope and time.
    /// Repeating a same-owner queued claim is read-only but rechecks permission.
    /// A foreign queued/running job requires explicit custody recovery; expiry
    /// or possession of its ID cannot authorize takeover.
    pub async fn claim_acquisition(
        &mut self,
        job: JobId,
        started: &Cell<bool>,
    ) -> Result<AcquisitionJob, Error> {
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 23)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require_job_update_schema(&tx).await?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let mine = AcquisitionOwner {
            resource: self.identity.resource_id,
            epoch: self.identity.epoch,
            incarnation: self.identity.process_incarnation,
        };
        let outcome = queue(&tx, self.request.dataset, job, mine).await;
        match terminal(&tx, outcome)? {
            Ok((claimed, true)) => {
                started.set(true);
                if let Err(error) = tx.commit().await {
                    if error.as_db_error().is_none() {
                        return Err(Error::CommitUnknown);
                    }
                    let error = backend(error);
                    if matches!(error, Error::Forbidden | Error::Conflict) {
                        self.available = true;
                    }
                    return Err(error);
                }
                self.available = true;
                Ok(claimed)
            }
            other => {
                tx.rollback().await.map_err(backend)?;
                self.available = true;
                other.map(|(claimed, _)| claimed)
            }
        }
    }
}

const EXECUTION_PRIVILEGES: &str = "
    SELECT bool_and(has_table_privilege(CURRENT_USER, t.name, 'SELECT'))
        AND bool_and(NOT t.row_lock OR has_any_column_privilege(CURRENT_USER, t.name, 'UPDATE'))
        AND has_table_privilege(CURRENT_USER, 'control.jobs', 'UPDATE')
        AND has_function_privilege(CURRENT_USER,
            'control.require_acquisition_job_authority(bytea,bytea)', 'EXECUTE')
        AND has_function_privilege(CURRENT_USER,
            'control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea)', 'EXECUTE')
        AND has_function_privilege(CURRENT_USER,
            'control.require_relay_source(bytea,bytea,bytea,numeric,bytea)', 'EXECUTE')
        AND has_function_privilege(CURRENT_USER,
            'control.require_acquisition_scope(bytea,bytea,bytea,bytea)', 'EXECUTE')
    FROM (VALUES
        ('control.datasets', true),
        ('control.local_accounts', true),
        ('control.principal_grants', true),
        ('control.relay_enrollments', true),
        ('control.relay_enrollment_changes', false),
        ('control.jobs', true),
        ('control.owner_epochs', false),
        ('control.acquisition_jobs', false),
        ('control.job_admissions', false),
        ('control.acquisition_scopes', false),
        ('control.acquisition_selectors', false),
        ('control.relay_endpoints', false),
        ('control.network_routes', false),
        ('control.relay_access_contexts', false),
        ('control.producer_input_bindings', false),
        ('control.config_revisions', false),
        ('control.policy_revisions', false)
    ) t(name, row_lock)";

async fn queue(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    job: JobId,
    mine: AcquisitionOwner,
) -> Result<(AcquisitionJob, bool), Error> {
    // Authoritative locks precede reading state; no provisional claim is based
    // on a row observed before waiting for account/source/job serialization.
    tx.query_one(
        "SELECT control.require_acquisition_job_authority($1::bytea,$2::bytea)",
        &[&&dataset.as_bytes()[..], &&job.as_bytes()[..]],
    )
    .await
    .map_err(backend)?;
    let old = read(tx, dataset, job).await?.ok_or(Error::Conflict)?;
    if old.checkpoint_revision != 0 {
        return Err(Error::Conflict);
    }
    if old.state == JobState::Queued && old.revision == 2 && old.owner == Some(mine) {
        return Ok((old, false));
    }
    if old.state != JobState::Accepted || old.revision != 1 || old.owner.is_some() {
        return Err(Error::Conflict);
    }
    let changed = tx
        .execute(
            "UPDATE control.jobs SET state='queued',job_revision=job_revision+1,
                owner_resource_id=$3::bytea,owner_epoch=$4::text::numeric,owner_incarnation=$5::bytea
             WHERE dataset_id=$1::bytea AND job_id=$2::bytea
                AND acquisition_job_protocol=1 AND state='accepted' AND job_revision=1",
            &[
                &&dataset.as_bytes()[..],
                &&job.as_bytes()[..],
                &&mine.resource.as_bytes()[..],
                &mine.epoch.get().to_string(),
                &&mine.incarnation.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?;
    if changed != 1 {
        return Err(Error::Conflict);
    }
    let queued = read(tx, dataset, job)
        .await?
        .ok_or(Error::InvalidMetadata)?;
    Ok((queued, true))
}

async fn read(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    job: JobId,
) -> Result<Option<AcquisitionJob>, Error> {
    // Every returned variable-width field is bounded before crossing the driver.
    // The immutable typed detail is not inferred for legacy acquisition rows.
    tx.query_opt(
        "SELECT substring(j.principal_id FROM 1 FOR 17),substring(a.scope_id FROM 1 FOR 17),
            a.duration_us,j.created_at_us,a.deadline_at_us,left(j.config_revision::text,21),
            left(j.policy_revision::text,21),left(j.state,32),left(j.job_revision::text,21),
            left(j.checkpoint_revision::text,21),substring(j.owner_resource_id FROM 1 FOR 17),
            left(j.owner_epoch::text,21),substring(j.owner_incarnation FROM 1 FOR 17),
            j.input_complete AND j.input_topology_generation IS NULL
                AND j.dataset_id=a.dataset_id AND j.principal_id=a.actor_id AND j.request_key=a.request_key
         FROM control.jobs j LEFT JOIN control.acquisition_jobs a USING(job_id)
         WHERE j.dataset_id=$1::bytea AND j.job_id=$2::bytea AND j.kind='acquisition'
            AND j.acquisition_job_protocol=1",
        &[&&dataset.as_bytes()[..], &&job.as_bytes()[..]],
    ).await.map_err(backend)?.as_ref().map(|row| decode(row,dataset,job)).transpose()
}

fn decode(row: &Row, dataset: DatasetId, id: JobId) -> Result<AcquisitionJob, Error> {
    if !row.try_get::<_, bool>(13).map_err(metadata)? {
        return Err(Error::InvalidMetadata);
    }
    let duration = JobDuration::new(
        u64::try_from(row.try_get::<_, i64>(2).map_err(metadata)?)
            .map_err(|_| Error::InvalidMetadata)?,
    )
    .map_err(|_| Error::InvalidMetadata)?;
    let accepted_at_us: i64 = row.try_get(3).map_err(metadata)?;
    let deadline_at_us = row.try_get(4).map_err(metadata)?;
    if accepted_at_us.checked_add(duration.get() as i64) != Some(deadline_at_us) {
        return Err(Error::InvalidMetadata);
    }
    let checkpoint: &str = row.try_get(9).map_err(metadata)?;
    let checkpoint_revision = checkpoint
        .parse::<u64>()
        .ok()
        .filter(|n| n.to_string() == checkpoint)
        .ok_or(Error::InvalidMetadata)?;
    let owner = match (
        row.try_get::<_, Option<&[u8]>>(10).map_err(metadata)?,
        row.try_get::<_, Option<&str>>(11).map_err(metadata)?,
        row.try_get::<_, Option<&[u8]>>(12).map_err(metadata)?,
    ) {
        (None, None, None) => None,
        (Some(_), Some(_), Some(_)) => Some(AcquisitionOwner {
            resource: fixed::<16>(row, 10)?
                .try_into()
                .map_err(|_| Error::InvalidMetadata)?,
            epoch: OwnerEpoch::new(revision(row, 11)?).map_err(|_| Error::InvalidMetadata)?,
            incarnation: fixed::<16>(row, 12)?
                .try_into()
                .map_err(|_| Error::InvalidMetadata)?,
        }),
        _ => return Err(Error::InvalidMetadata),
    };
    Ok(AcquisitionJob {
        id,
        dataset,
        principal: PrincipalId::try_from(fixed::<16>(row, 0)?)
            .map_err(|_| Error::InvalidMetadata)?,
        scope: AcquisitionScopeId::try_from(fixed::<16>(row, 1)?)
            .map_err(|_| Error::InvalidMetadata)?,
        duration,
        accepted_at_us,
        deadline_at_us,
        config_revision: revision(row, 5)?,
        policy_revision: revision(row, 6)?,
        state: JobState::from_code(row.try_get(7).map_err(metadata)?)
            .ok_or(Error::InvalidMetadata)?,
        revision: revision(row, 8)?,
        checkpoint_revision,
        owner,
    })
}
