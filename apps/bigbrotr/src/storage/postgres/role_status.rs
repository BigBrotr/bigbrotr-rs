//! Observation-only authorization; no session idle touch or durable mutation.
use super::{
    ControlSession, application_transaction, browser_reauthentication::authenticated_session,
    browser_sessions::*, job_status::require_status, require,
};
use crate::management::{
    roles::SnapshotIdentity,
    sessions::{BrowserSecret, SessionError},
};
use std::time::Duration;

/// Only Control constructs this authorization. The transport starts a fixed
/// action window before SQL and binds the returned remaining session duration.
pub struct AuthorizedRoleStatus {
    identity: SnapshotIdentity,
    observed_at_us: i64,
    remaining: Duration,
}
impl AuthorizedRoleStatus {
    pub fn identity(&self) -> SnapshotIdentity {
        self.identity
    }
    pub fn observed_at_us(&self) -> i64 {
        self.observed_at_us
    }
    pub fn remaining(&self) -> Duration {
        self.remaining
    }
}
impl ControlSession {
    pub async fn authorize_browser_role_status(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
    ) -> Result<AuthorizedRoleStatus, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 809)
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
            let time = now(&tx).await?;
            let remaining = current
                .expires
                .checked_sub(time)
                .filter(|v| *v > 0)
                .ok_or(SessionError::Denied)?;
            Ok(AuthorizedRoleStatus {
                identity: SnapshotIdentity {
                    dataset_id: self.request.dataset,
                    process_incarnation: self.request.incarnation,
                    config_revision: self
                        .validated_policy
                        .config_revision
                        .to_string()
                        .try_into()
                        .map_err(|_| SessionError::InvalidMetadata)?,
                    policy_revision: self
                        .validated_policy
                        .policy_revision
                        .to_string()
                        .try_into()
                        .map_err(|_| SessionError::InvalidMetadata)?,
                },
                observed_at_us: time,
                remaining: Duration::from_micros(remaining as u64),
            })
        }
        .await;
        let outcome = terminal(&tx, outcome)?;
        // A missing grant or expired session is a healthy completed refusal.
        // An unknown rollback still poisons Control and must be joined by owner.
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        outcome
    }
}
