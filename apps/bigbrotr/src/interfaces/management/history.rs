//! Owned private read command and its final HTTP body consumer.
use super::{Completion, Reply, Response, Result, sql, transport::ConnectionDelivery};
use bigbrotr::{
    management::sessions::{BrowserSecret, SessionError},
    runtime::{OwnedPermit, policy_authority::ActionWindow},
    storage::{
        archive::ArchivedEventQuery,
        postgres::control_owner::{ControlSession, HistoryAuthorization},
    },
};
use bigbrotr_model::read::QueryRequest;
use std::time::Duration;

pub(crate) struct HistoryWork {
    pub(super) completion: Completion,
    pub(super) session: BrowserSecret,
    pub(super) csrf: BrowserSecret,
    pub(super) query: QueryRequest,
    pub(super) delivery: ConnectionDelivery,
}
impl HistoryWork {
    pub fn query(&self) -> &QueryRequest {
        &self.query
    }
    pub fn deadline(&self) -> tokio::time::Instant {
        self.completion.deadline
    }
    pub fn abandoned(&self) -> bool {
        self.completion.abandoned()
    }
    pub fn refuse(self, error: SessionError) -> Response {
        Response(self.completion, Err(error))
    }
    pub async fn authorize(
        &self,
        owner: &mut ControlSession,
        maximum: Duration,
    ) -> Result<std::result::Result<HistoryAuthorization, SessionError>> {
        sql(
            maximum,
            owner.authorize_browser_history(&self.session, &self.csrf, self.query.scope),
        )
        .await
    }
    pub fn bind_action(
        &self,
        window: ActionWindow,
    ) -> std::io::Result<super::delivery::ActionGate> {
        self.delivery.action.bind(window)?;
        Ok(self.delivery.action.clone())
    }
    pub fn deliver(self, body: HistoryBody) -> ConnectionDelivery {
        // Failure drops the owned body without dispatch. The caller still waits
        // for positive socket termination before releasing the durable pin.
        if !self.abandoned() && self.delivery.action.check().is_ok() {
            self.completion.finish(Ok(Reply::History(Box::new(body))));
        }
        self.delivery
    }
}
/// Bytes::from_owner keeps this owner alive through every Hyper buffer clone.
/// Socket completion is the supervisor's independent positive cleanup boundary.
pub(crate) struct HistoryBody {
    pub bytes: Vec<u8>,
    pub _event: ArchivedEventQuery,
    pub _permit: OwnedPermit,
}
impl AsRef<[u8]> for HistoryBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
