//! Operational attribution for one arrival; never an acquisition permission.
use bigbrotr_model::{AccessScopeId, EndpointId, RouteId};
use std::num::NonZeroU64;

/// Construct in the trusted socket owner from the source actually used. Keep
/// this tuple even with provenance off; it allocates no archive dictionaries.
/// Its dataset is the enclosing producer reservation's dataset. The current
/// source/policy and committed visibility must still be checked at dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelaySourceBinding {
    pub endpoint_id: EndpointId,
    pub access_scope_id: AccessScopeId,
    pub enrollment_generation: NonZeroU64,
    pub route_id: RouteId,
}
