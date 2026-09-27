//! Actor-owned operational metadata, independent of input/output permissions.
use super::{
    ControlSession, application_transaction, browser_reauthentication::authenticated_session,
    browser_sessions::*, require,
};
use crate::{
    export::execution::ExportState,
    management::{
        jobs::{JobObservation, JobReason},
        sessions::{BrowserSecret, SessionError},
    },
};
use bigbrotr_model::{DatasetId, JobId, PrincipalId};
use std::time::Duration;
use tokio_postgres::{Row, Transaction};

/// A transport cannot construct this authenticated observation. Pair it with
/// the nonrenewable action window started before SQL, including for a missing ID.
pub struct AuthorizedJobStatus {
    observation: Option<JobObservation>,
    remaining: Duration,
}
impl AuthorizedJobStatus {
    pub fn remaining(&self) -> Duration {
        self.remaining
    }
    pub fn into_observation(self) -> Option<JobObservation> {
        self.observation
    }
}

impl ControlSession {
    pub async fn observe_browser_job(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        job: JobId,
    ) -> Result<AuthorizedJobStatus, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 813)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            let current = authenticated_session(
                &tx,
                self.request.dataset,
                &token.session_digest(),
                &csrf.csrf_digest(),
                None,
            )
            .await?;
            require_status(&tx, self.request.dataset, current.principal).await?;
            // Ownership is filtered in SQL, even for administrators. No source
            // selection, destination or payload enters the response.
            let row = tx
                .query_opt(
                    "SELECT substring(job_id FROM 1 FOR 17) AS job_id,
                    left(state,32) AS state, left(job_revision::text,21) AS job_revision,
                    left(checkpoint_revision::text,21) AS checkpoint_revision,
                    created_at_us, completed_at_us,
                    CASE WHEN reason_code IS NULL THEN NULL
                         WHEN reason_code IN ('resource_limit','execution_refused') THEN reason_code
                         ELSE 'other' END AS reason
                 FROM control.jobs
                 WHERE job_id=$1::bytea AND dataset_id=$2::bytea AND principal_id=$3::bytea
                    AND kind='export' AND export_job_protocol=1",
                    &[
                        &&job.as_bytes()[..],
                        &&self.request.dataset.as_bytes()[..],
                        &&current.principal.as_bytes()[..],
                    ],
                )
                .await
                .map_err(backend)?;
            let time = now(&tx).await?;
            let remaining = current
                .expires
                .checked_sub(time)
                .filter(|v| *v > 0)
                .ok_or(SessionError::Denied)?;
            let observation = row.as_ref().map(|row| decode(row, time)).transpose()?;
            Ok(AuthorizedJobStatus {
                observation,
                remaining: Duration::from_micros(remaining as u64),
            })
        }
        .await;
        let outcome = terminal(&tx, outcome)?;
        // No idle touch, job mutation or new durable receipt. Unknown rollback
        // still poisons Control, as for other authenticated reads.
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        outcome
    }
}

pub(super) async fn require_status(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    actor: PrincipalId,
) -> Result<(), SessionError> {
    let permitted: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.principal_grants
         WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='status.read'
            AND resource_kind='dataset' AND resource_id=dataset_id)",
            &[&&dataset.as_bytes()[..], &&actor.as_bytes()[..]],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(metadata)?;
    if permitted {
        Ok(())
    } else {
        Err(SessionError::Forbidden)
    }
}

/// Named aliases keep list/detail projections visibly equivalent while refusing
/// corrupt metadata. SQL bounds every variable-width value before allocation.
pub(super) fn decode(row: &Row, observed_at_us: i64) -> Result<JobObservation, SessionError> {
    let job: &[u8] = row.try_get("job_id").map_err(metadata)?;
    let job_id =
        JobId::try_from(<[u8; 16]>::try_from(job).map_err(|_| SessionError::InvalidMetadata)?)
            .map_err(|_| SessionError::InvalidMetadata)?;
    let number = |column| {
        let text: &str = row.try_get(column).map_err(metadata)?;
        text.parse::<u64>()
            .ok()
            .filter(|n| n.to_string() == text)
            .ok_or(SessionError::InvalidMetadata)
    };
    let job_revision = number("job_revision")?;
    if job_revision == 0 {
        return Err(SessionError::InvalidMetadata);
    }
    let reason = match row.try_get::<_, Option<&str>>("reason").map_err(metadata)? {
        None => None,
        Some("resource_limit") => Some(JobReason::ResourceLimit),
        Some("execution_refused") => Some(JobReason::ExecutionRefused),
        Some("other") => Some(JobReason::Other),
        _ => return Err(SessionError::InvalidMetadata),
    };
    Ok(JobObservation {
        job_id,
        state: ExportState::from_code(row.try_get("state").map_err(metadata)?)
            .ok_or(SessionError::InvalidMetadata)?,
        job_revision,
        checkpoint_revision: number("checkpoint_revision")?,
        created_at_us: row.try_get("created_at_us").map_err(metadata)?,
        completed_at_us: row.try_get("completed_at_us").map_err(metadata)?,
        observed_at_us,
        reason,
    })
}
