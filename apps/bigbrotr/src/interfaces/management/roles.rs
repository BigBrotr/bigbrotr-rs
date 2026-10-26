//! Authenticated completion seam for an observation from the actual supervisor.
use super::{Completion, Reply, Response, transport::ConnectionDelivery};
use bigbrotr::{
    management::{
        roles::{RESPONSE_RESERVATION, RoleSnapshot, SnapshotIdentity},
        sessions::SessionError,
    },
    runtime::{ResourceBudget, ResourceRequest},
    storage::postgres::control_owner::AuthorizedRoleStatus,
};

pub(crate) struct RoleStatusWork {
    pub(super) completion: Completion,
    pub(super) authorization: AuthorizedRoleStatus,
    pub(super) delivery: ConnectionDelivery,
    pub(super) budget: ResourceBudget,
}
impl RoleStatusWork {
    pub fn identity(&self) -> SnapshotIdentity {
        self.authorization.identity()
    }
    pub fn observed_at_us(&self) -> i64 {
        self.authorization.observed_at_us()
    }
    pub fn abandoned(&self) -> bool {
        self.completion.abandoned()
    }
    pub fn refuse(self, error: SessionError) -> Response {
        Response(self.completion, Err(error))
    }
    pub fn complete(self, snapshot: RoleSnapshot) -> Response {
        let result = if self.abandoned() || self.delivery.action.check().is_err() {
            Err(SessionError::Unavailable)
        } else if snapshot.identity() != self.identity()
            || snapshot.observed_at_us() != self.observed_at_us()
        {
            Err(SessionError::PolicyChanged)
        } else {
            self.budget
                .try_acquire(
                    ResourceRequest::data(RESPONSE_RESERVATION, 1, 0)
                        .expect("fixed role response reservation"),
                )
                .map_err(|_| SessionError::Capacity)
                .and_then(|permit| snapshot.encode(permit))
                .map(Reply::RoleStatus)
        };
        Response(self.completion, result)
    }
}
