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
use bigbrotr_model::JobId;
use std::time::Duration;

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
            let current = authenticated_session(&tx, self.request.dataset, &token.session_digest(), &csrf.csrf_digest(), None).await?;
            // Same dataset/account lock order as current grant mutations. This
            // observes only the actor's work, even for an administrator.
            let permitted: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.principal_grants WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='status.read' AND resource_kind='dataset' AND resource_id=dataset_id)", &[&&self.request.dataset.as_bytes()[..], &&current.principal.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            if !permitted { return Err(SessionError::Forbidden); }
            // Filter ownership in SQL and project closed metadata only. Neither
            // source selection nor destination/payload can enter the response.
            let row = tx.query_opt("SELECT left(state,32),left(job_revision::text,21),left(checkpoint_revision::text,21),created_at_us,completed_at_us,CASE WHEN reason_code IS NULL THEN NULL WHEN reason_code IN ('resource_limit','execution_refused') THEN reason_code ELSE 'other' END FROM control.jobs WHERE job_id=$1::bytea AND dataset_id=$2::bytea AND principal_id=$3::bytea AND kind='export' AND export_job_protocol=1", &[&&job.as_bytes()[..], &&self.request.dataset.as_bytes()[..], &&current.principal.as_bytes()[..]]).await.map_err(backend)?;
            let time = now(&tx).await?;
            let remaining = current.expires.checked_sub(time).filter(|v| *v > 0).ok_or(SessionError::Denied)?;
            let observation = row.map(|row| {
                let reason = match row.try_get::<_, Option<&str>>(5).map_err(metadata)? {
                    None => None,
                    Some("resource_limit") => Some(JobReason::ResourceLimit),
                    Some("execution_refused") => Some(JobReason::ExecutionRefused),
                    Some("other") => Some(JobReason::Other),
                    _ => return Err(SessionError::InvalidMetadata),
                };
                Ok(JobObservation {
                    job_id: job,
                    state: ExportState::from_code(row.try_get(0).map_err(metadata)?).ok_or(SessionError::InvalidMetadata)?,
                    job_revision: revision(&row, 1)?, checkpoint_revision: {
                        let text: &str = row.try_get(2).map_err(metadata)?;
                        text.parse::<u64>().ok().filter(|n| n.to_string() == text).ok_or(SessionError::InvalidMetadata)?
                    },
                    created_at_us: row.try_get(3).map_err(metadata)?, completed_at_us: row.try_get(4).map_err(metadata)?,
                    observed_at_us: time, reason,
                })
            }).transpose()?;
            Ok(AuthorizedJobStatus { observation, remaining: Duration::from_micros(remaining as u64) })
        }.await;
        let outcome = terminal(&tx, outcome)?;
        // No idle touch, job mutation or new durable receipt. Unknown rollback
        // still poisons Control, as for other authenticated reads.
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        outcome
    }
}
