//! Session and exact qualified-scope authorization for one finite history read.
use super::{
    ControlSession, application_transaction, browser_reauthentication::authenticated_session,
    browser_sessions::*, require,
};
use crate::management::sessions::{BrowserSecret, SessionError};
use bigbrotr_model::{DictionaryKind, PrincipalId, QualifiedRef};
use std::time::Duration;

/// Not serializable or constructible by a transport. Pair this observation with
/// the nonrenewable local window started BEFORE the authenticated SQL request.
pub struct HistoryAuthorization {
    principal: PrincipalId,
    scope: QualifiedRef,
    observed_at_us: i64,
    remaining: Duration,
}
impl HistoryAuthorization {
    pub fn principal(&self) -> PrincipalId {
        self.principal
    }
    pub fn scope(&self) -> QualifiedRef {
        self.scope
    }
    pub fn observed_at_us(&self) -> i64 {
        self.observed_at_us
    }
    pub fn remaining(&self) -> Duration {
        self.remaining
    }
}
impl ControlSession {
    /// A history read is deliberately not an idle-session heartbeat. The
    /// existing explicit session inspection owns user-activity refresh.
    pub async fn authorize_browser_history(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        scope: QualifiedRef,
    ) -> Result<HistoryAuthorization, SessionError> {
        scope
            .require(self.request.dataset, DictionaryKind::VisibilityScope)
            .map_err(|_| SessionError::InvalidInput)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 811)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            let current = authenticated_session(&tx, self.request.dataset, &token.session_digest(), &csrf.csrf_digest(), None).await?;
            // Grant mutation serializes on the same dataset/account locks. A
            // dataset data.read or principals.manage grant is insufficient.
            let permitted: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.scope_grants WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='history.read' AND dictionary_kind=4 AND allocation_domain=$3::bytea AND ordinal=$4::text::numeric AND enabled)", &[&&self.request.dataset.as_bytes()[..], &&current.principal.as_bytes()[..], &&scope.allocation_domain.as_bytes()[..], &scope.ordinal.get().to_string()]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
            if !permitted { return Err(SessionError::Forbidden); }
            let time = now(&tx).await?;
            let remaining = current.expires.checked_sub(time).filter(|v| *v > 0).ok_or(SessionError::Denied)?;
            Ok(HistoryAuthorization { principal: current.principal, scope, observed_at_us: time, remaining: Duration::from_micros(remaining as u64) })
        }.await;
        let outcome = terminal(&tx, outcome)?;
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        outcome
    }
}
