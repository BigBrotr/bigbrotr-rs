//! Bounded actor-owned pages with committed key retention before token delivery.
use super::{
    ControlSession, application_transaction, browser_reauthentication::authenticated_session,
    browser_sessions::*, job_status, require,
};
use crate::management::jobs::{JobKind, JobObservation};
use crate::{
    management::{
        job_list::{
            Continuation, CursorLifetime, JobListQuery, JobListRequest, MAX_SKEW_US,
            PAGE_RESERVATION, Page, PageBody, Position, RESOURCE,
        },
        sessions::{BrowserSecret, SessionError},
    },
    read::{
        cursor::{self, CURSOR_RESERVATION, CursorError, EncodedCursor},
        keyring::Keyring,
    },
    runtime::{ResourceBudget, ResourceRequest},
};
use bigbrotr_model::{DatasetId, PrincipalId};
use std::time::Duration;
use tokio_postgres::Transaction;

pub struct AuthorizedJobPage {
    body: PageBody,
    remaining: Duration,
}
impl AuthorizedJobPage {
    pub fn remaining(&self) -> Duration {
        self.remaining
    }
    pub fn into_body(self) -> PageBody {
        self.body
    }
}

impl ControlSession {
    /// Older valid installations keep their existing APIs. This selected page
    /// consumer additionally needs loaded enrolled material and retention rights.
    pub async fn job_pages_available(&mut self) -> Result<bool, SessionError> {
        if self.enrolled_cursor_keys().is_none() {
            return Ok(false);
        }
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 812)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let present: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM control.schema_versions
             WHERE engine='postgres-control' AND migration=816)",
                &[],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(metadata)?;
        let valid = if present {
            tx.query_one(
                "SELECT has_table_privilege(CURRENT_USER,'control.jobs','SELECT')
                    AND has_table_privilege(CURRENT_USER,'control.job_admissions','SELECT')
                    AND has_table_privilege(CURRENT_USER,'control.cursor_key_transitions','SELECT')
                    AND has_table_privilege(CURRENT_USER,'control.cursor_key_lifecycle','SELECT')
                    AND has_column_privilege(CURRENT_USER,'control.cursor_key_lifecycle','decrypt_until_us','UPDATE')
                    AND has_function_privilege(CURRENT_USER,'control.require_cursor_retention_owner(bytea)','EXECUTE')",
                &[],
            ).await.map_err(backend)?.try_get(0).map_err(metadata)?
        } else {
            false
        };
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(valid)
    }

    pub async fn list_browser_jobs(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        request: &JobListRequest,
        lifetime: CursorLifetime,
        budget: &ResourceBudget,
    ) -> Result<AuthorizedJobPage, SessionError> {
        let permit = budget
            .try_acquire(
                ResourceRequest::data(PAGE_RESERVATION, 1, 0).expect("fixed page reservation"),
            )
            .map_err(|_| SessionError::Capacity)?;
        let keys = self
            .enrolled_cursor_keys
            .clone()
            .ok_or(SessionError::CursorUnavailable)?;
        let dataset = self.request.dataset;
        let policy = self.validated_policy;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 816)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, dataset, policy).await?;
        let mut retained = false;
        let outcome = async {
            let current = authenticated_session(
                &tx,
                dataset,
                &token.session_digest(),
                &csrf.csrf_digest(),
                None,
            )
            .await?;
            job_status::require_status(&tx, dataset, current.principal).await?;
            // The selected endpoint requires an issuer even for a terminal page.
            // Dataset exclusion keeps this selection stable through completion.
            let issuer = active_issuer(&tx, dataset).await?;
            let time = now(&tx).await?;
            // Both grant and issuer lookups may wait. Expiry must precede opening
            // the envelope, including its malformed-token error path.
            if time >= current.expires {
                return Err(SessionError::Denied);
            }
            let issuer = issuer.ok_or(SessionError::CursorUnavailable)?;
            if !keys.key_ids().any(|id| id == issuer.id) {
                return Err(SessionError::CursorUnavailable);
            }
            let grants = revision(&current.account, 3)?;
            let skew = i64::try_from(lifetime.skew_us)
                .ok()
                .filter(|n| (0..=MAX_SKEW_US).contains(n))
                .ok_or(SessionError::InvalidInput)?;
            // No envelope decoding or key-id lookup before current session/CSRF
            // and status.read checks. Cached key bytes alone authorize nothing.
            let previous = if let Some(token) = &request.cursor {
                let opened = cursor::open(&keys, RESOURCE, token, codec_permit(budget)?)
                    .map_err(cursor_error)?;
                let continuation = Continuation::decode(opened.payload())?;
                continuation.validate(
                    current.principal,
                    policy.policy_revision,
                    grants,
                    request.query,
                    time,
                    skew,
                )?;
                let retained_until = continuation
                    .expires_at_us
                    .checked_add(MAX_SKEW_US)
                    .ok_or(SessionError::CursorInvalid)?;
                let valid: bool = tx
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM control.cursor_key_lifecycle
                     WHERE dataset_id=$1::bytea AND key_id=$2::bytea
                        AND first_activation_revision IS NOT NULL AND revoked_revision IS NULL
                        AND decrypt_until_us >= $3::bigint)",
                        &[
                            &&dataset.as_bytes()[..],
                            &&opened.key_id()[..],
                            &retained_until,
                        ],
                    )
                    .await
                    .map_err(backend)?
                    .try_get(0)
                    .map_err(metadata)?;
                if !valid {
                    return Err(SessionError::CursorInvalid);
                }
                Some(continuation)
            } else {
                None
            };
            let cut =
                match &previous {
                    Some(previous) => previous.admission_cut,
                    None => {
                        // Dataset exclusion serializes the cut with admission,
                        // including later jobs with older creation timestamps.
                        let row = tx.query_one(
                        "SELECT coalesce((SELECT admission_ordinal FROM control.job_admissions
                         ORDER BY admission_ordinal DESC LIMIT 1),0)::text", &[],
                    ).await.map_err(backend)?;
                        let text: &str = row.try_get(0).map_err(metadata)?;
                        text.parse::<u64>()
                            .ok()
                            .filter(|n| n.to_string() == text)
                            .ok_or(SessionError::InvalidMetadata)?
                    }
                };
            let scan = scan_jobs(
                &tx,
                dataset,
                current.principal,
                request.query,
                cut,
                previous.as_ref().map(|c| c.position),
            )
            .await?;
            let mut continuation = None;
            let next_cursor = if let Some(position) = scan.resume_after {
                let (issued_at_us, expires_at_us) = match previous.as_ref() {
                    Some(previous) => (previous.issued_at_us, previous.expires_at_us),
                    None => {
                        let ttl = i64::try_from(lifetime.ttl_us)
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or(SessionError::Capacity)?;
                        (time, time.checked_add(ttl).ok_or(SessionError::Capacity)?)
                    }
                };
                let payload = Continuation {
                    actor: current.principal,
                    policy_revision: policy.policy_revision,
                    grant_revision: grants,
                    query: request.query,
                    admission_cut: cut,
                    position,
                    issued_at_us,
                    expires_at_us,
                };
                let (encoded, changed) =
                    retain_cursor(&tx, &keys, &issuer, &payload, budget).await?;
                retained = changed;
                continuation = Some(payload);
                Some(encoded)
            } else {
                None
            };
            let body = Page {
                items: scan.items,
                next_cursor,
                effective_page_size: request.query.page_size(),
                observed_at_us: scan.observed_at_us,
                permit,
            }
            .encode()?;
            // Both writing and already-retained/read-only paths recheck authority
            // after waits. Deferred SQL also checks the lease at writing COMMIT.
            tx.query_one(
                "SELECT control.require_cursor_retention_owner($1::bytea)",
                &[&&dataset.as_bytes()[..]],
            )
            .await
            .map_err(backend)?;
            let final_time = now(&tx).await?;
            for value in previous.iter().chain(continuation.iter()) {
                value.validate(
                    current.principal,
                    policy.policy_revision,
                    grants,
                    request.query,
                    final_time,
                    skew,
                )?;
            }
            let delivery_expires = previous
                .iter()
                .chain(continuation.iter())
                .fold(current.expires, |end, value| {
                    end.min(value.expires_at_us.saturating_add(skew))
                });
            let remaining = delivery_expires
                .checked_sub(final_time)
                .filter(|n| *n > 0)
                .ok_or(SessionError::Denied)?;
            Ok(AuthorizedJobPage {
                body,
                remaining: Duration::from_micros(remaining as u64),
            })
        }
        .await;
        let outcome = terminal(&tx, outcome)?;
        if retained && outcome.is_ok() {
            // Never return a newly issued token on ambiguous COMMIT. Custody is
            // retained by the surrounding driver owner until cancellation joins.
            tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        } else {
            tx.rollback().await.map_err(backend)?;
        }
        self.available = true;
        outcome
    }
}

struct Scan {
    items: Vec<JobObservation>,
    observed_at_us: i64,
    resume_after: Option<Position>,
}

async fn scan_jobs(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    actor: PrincipalId,
    query: JobListQuery,
    cut: u64,
    position: Option<Position>,
) -> Result<Scan, SessionError> {
    let after_time = position.map(|p| p.created_at_us);
    let after_id = position.as_ref().map(|p| &p.job.as_bytes()[..]);
    // MATERIALIZED bounds candidates before membership/cut filtering or
    // admission lookups. A page may be empty but still make progress.
    let rows = tx.query(
        "WITH candidates AS MATERIALIZED (
            SELECT job_id,state,job_revision,checkpoint_revision,created_at_us,completed_at_us,reason_code
            FROM control.jobs
            WHERE dataset_id=$1::bytea AND principal_id=$2::bytea
                AND kind='export' AND export_job_protocol=1
                AND ($3::bigint IS NULL OR created_at_us >= $3::bigint)
                AND ($4::bigint IS NULL OR created_at_us < $4::bigint)
                AND ($5::bigint IS NULL OR (created_at_us,job_id) < ($5::bigint,$6::bytea))
            ORDER BY created_at_us DESC, job_id DESC LIMIT $7::bigint
         )
         SELECT substring(c.job_id FROM 1 FOR 17) AS job_id,
            left(c.state,32) AS state, left(c.job_revision::text,21) AS job_revision,
            left(c.checkpoint_revision::text,21) AS checkpoint_revision,
            c.created_at_us, c.completed_at_us,
            CASE WHEN c.reason_code IS NULL THEN NULL
                 WHEN c.reason_code IN ('resource_limit','execution_refused') THEN c.reason_code
                 ELSE 'other' END AS reason,
            left(a.admission_ordinal::text,21) AS admission_ordinal
         FROM candidates c LEFT JOIN control.job_admissions a USING(job_id)
         ORDER BY c.created_at_us DESC,c.job_id DESC",
        &[&&dataset.as_bytes()[..], &&actor.as_bytes()[..],
          &query.created_from_us(), &query.created_before_us(),
          &after_time, &after_id, &(i64::from(query.page_size())+1)],
    ).await.map_err(backend)?;
    let count = usize::from(query.page_size());
    let more = rows.len() > count;
    let mut items = Vec::new();
    items
        .try_reserve_exact(count)
        .map_err(|_| SessionError::Capacity)?;
    let observed_at_us = now(tx).await?;
    let mut scanned = None;
    for row in rows.iter().take(count) {
        let observation = job_status::decode(row, observed_at_us, JobKind::Export)?;
        let ordinal = revision(row, 7)?;
        scanned = Some(Position {
            created_at_us: observation.created_at_us,
            job: observation.job_id,
        });
        if ordinal <= cut && query.state().is_none_or(|s| s == observation.state) {
            items.push(observation);
        }
    }
    drop(rows);
    Ok(Scan {
        items,
        observed_at_us,
        resume_after: if more {
            Some(scanned.ok_or(SessionError::InvalidMetadata)?)
        } else {
            None
        },
    })
}

struct IssuingKey {
    id: [u8; 16],
    retained_until_us: Option<i64>,
}

async fn active_issuer(
    tx: &Transaction<'_>,
    dataset: DatasetId,
) -> Result<Option<IssuingKey>, SessionError> {
    let row = tx
        .query_opt(
            "SELECT substring(l.key_id FROM 1 FOR 17),l.decrypt_until_us
         FROM control.cursor_key_lifecycle l
         WHERE l.dataset_id=$1::bytea AND l.key_id=(
            SELECT resulting_active_key_id FROM control.cursor_key_transitions
            WHERE dataset_id=$1::bytea ORDER BY revision DESC LIMIT 1)
            AND l.first_activation_revision IS NOT NULL AND l.revoked_revision IS NULL",
            &[&&dataset.as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
    row.map(|row| {
        Ok(IssuingKey {
            id: fixed::<16>(&row, 0)?,
            retained_until_us: row.try_get(1).map_err(metadata)?,
        })
    })
    .transpose()
}

/// Sealing does not release bytes. The caller must positively commit a changed
/// retention obligation, or acknowledge rollback when the existing one suffices.
async fn retain_cursor(
    tx: &Transaction<'_>,
    keys: &Keyring,
    issuer: &IssuingKey,
    payload: &Continuation,
    budget: &ResourceBudget,
) -> Result<(EncodedCursor, bool), SessionError> {
    let until = payload
        .expires_at_us
        .checked_add(MAX_SKEW_US)
        .ok_or(SessionError::Capacity)?;
    let encoded = cursor::seal(
        keys,
        issuer.id,
        RESOURCE,
        &payload.encode()[..],
        codec_permit(budget)?,
    )
    .map_err(|e| {
        if e == CursorError::Invalid {
            SessionError::CursorUnavailable
        } else {
            cursor_error(e)
        }
    })?;
    let mut retained = false;
    if issuer.retained_until_us.is_none_or(|old| old < until) {
        let changed = tx
            .execute(
                "UPDATE control.cursor_key_lifecycle SET decrypt_until_us=$3::bigint
             WHERE dataset_id=$1::bytea AND key_id=$2::bytea
                AND (decrypt_until_us IS NULL OR decrypt_until_us<$3::bigint)",
                &[&&keys.dataset().as_bytes()[..], &&issuer.id[..], &until],
            )
            .await
            .map_err(backend)?;
        if changed != 1 {
            return Err(SessionError::InvalidMetadata);
        }
        retained = true;
    }
    Ok((encoded, retained))
}

fn codec_permit(budget: &ResourceBudget) -> Result<crate::runtime::OwnedPermit, SessionError> {
    budget
        .try_acquire(
            ResourceRequest::data(CURSOR_RESERVATION, 1, 0).expect("fixed cursor reservation"),
        )
        .map_err(|_| SessionError::Capacity)
}
fn cursor_error(error: CursorError) -> SessionError {
    match error {
        CursorError::Invalid => SessionError::CursorInvalid,
        CursorError::Capacity => SessionError::Capacity,
        CursorError::Entropy => SessionError::CursorUnavailable,
    }
}
