//! Committed local attribution. Source/job authority remains a separate check.
use crate::{network::RouteSelection, runtime::OwnedPermit};
use bigbrotr_model::{
    ClockQuality, DictionaryEntry, EndpointDescriptor, LocalObservation, ObservedTime,
    QualifiedRef, RouteClass, WitnessContext,
};
use bigbrotr_protocol::endpoint::Endpoint;

/// Expected local identities supplied by the trusted receiving owner. These are
/// full immutable mappings, not caller-selected labels or operational source IDs.
#[derive(Clone, Copy)]
pub struct ObservationBindings<'a> {
    pub visibility: QualifiedRef,
    pub observer: &'a DictionaryEntry,
    pub clock: &'a DictionaryEntry,
    pub witness: Option<WitnessContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationError {
    SourceMismatch,
    ClockMismatch,
}

/// Created only after native mappings and their complete dependency closure
/// have been checked. This does not allocate identities or authorize a socket.
/// Provenance off retains no endpoint, access or method descriptor/reference.
pub struct StoredObservation {
    pub(crate) visibility: QualifiedRef,
    pub(crate) observer: QualifiedRef,
    pub(crate) clock: QualifiedRef,
    pub(crate) observer_route: Option<RouteClass>,
    pub(crate) clock_route: Option<RouteClass>,
    pub(crate) witness: Option<(WitnessContext, EndpointDescriptor)>,
    pub(crate) permit: OwnedPermit,
}
impl StoredObservation {
    pub fn visibility(&self) -> QualifiedRef {
        self.visibility
    }

    /// Bind an actual session sample to the committed local system clock.
    pub fn received(
        &self,
        endpoint: &Endpoint,
        route: RouteSelection,
        sample: super::receive_clock::ReceiveTime,
    ) -> Result<LocalObservation, ObservationError> {
        self.observe(
            endpoint,
            route,
            sample.received_at(),
            sample
                .quality(self.clock)
                .map_err(|_| ObservationError::ClockMismatch)?,
        )
    }

    /// Call with the endpoint/route of the actual guarded source and the clock
    /// sample captured at receive, before verification or admission. This checks
    /// attribution, not current policy. Signed created_at is never used here.
    pub(crate) fn observe(
        &self,
        endpoint: &Endpoint,
        route: RouteSelection,
        received_at: ObservedTime,
        quality: ClockQuality,
    ) -> Result<LocalObservation, ObservationError> {
        let route = match route {
            RouteSelection::Public {} => RouteClass::Public,
            RouteSelection::Private { .. } => RouteClass::Private,
        };
        if self.observer_route.is_some_and(|r| r != route)
            || self.clock_route.is_some_and(|r| r != route)
            || self
                .witness
                .as_ref()
                .is_some_and(|(_, e)| e.url() != endpoint.as_str())
        {
            return Err(ObservationError::SourceMismatch);
        }
        if quality.clock_id() != self.clock {
            return Err(ObservationError::ClockMismatch);
        }
        Ok(LocalObservation {
            received_at,
            observer_id: self.observer,
            clock_quality: quality,
            witness: self.witness.as_ref().map(|(context, _)| *context),
        })
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.permit.request().units().data_bytes()
    }
}
