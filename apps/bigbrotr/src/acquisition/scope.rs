//! Retained collection intent. Loading this data grants no source or admission authority.
use super::relay_source::RelaySourceBinding;
use crate::runtime::OwnedPermit;
use bigbrotr_model::{
    AcquisitionScopeId, QualifiedRef, SignedEvent,
    acquisition::{AcquisitionScope, CollectionSelector, Filter, MAX_FILTERS, Membership},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeEventError {
    InvalidRequest,
    SourceMismatch,
    OutsideCollection,
    EphemeralExcluded,
    OutsideRequest,
}

/// Borrowed immutable intent owned by the actual subscription generation. The
/// socket owner retains the exact wire predicate across reservation and dispatch;
/// a lost mutation is resolved from its original input, never a replacement page.
/// This value checks membership, not current policy, enrollment or socket custody.
#[derive(Clone, Copy)]
pub struct AcquisitionEventContext<'a> {
    scope: &'a StoredAcquisitionScope,
    request: &'a [Filter],
}
impl<'a> AcquisitionEventContext<'a> {
    pub fn new(
        scope: &'a StoredAcquisitionScope,
        request: &'a [Filter],
    ) -> Result<Self, ScopeEventError> {
        if request.is_empty() || request.len() > MAX_FILTERS {
            return Err(ScopeEventError::InvalidRequest);
        }
        Ok(Self { scope, request })
    }

    pub fn scope(self) -> &'a StoredAcquisitionScope {
        self.scope
    }

    pub fn check(
        self,
        source: RelaySourceBinding,
        visibility: QualifiedRef,
        event: &SignedEvent,
    ) -> Result<(), ScopeEventError> {
        let scope = self.scope.scope();
        if scope.endpoint() != source.endpoint_id
            || scope.access() != source.access_scope_id
            || scope.visibility() != visibility
        {
            return Err(ScopeEventError::SourceMismatch);
        }
        match self.scope.selector().classify(event) {
            Membership::OutsideSelector => return Err(ScopeEventError::OutsideCollection),
            Membership::EphemeralExcluded => return Err(ScopeEventError::EphemeralExcluded),
            Membership::Included => {}
        }
        if !self.request.iter().any(|filter| filter.matches(event)) {
            return Err(ScopeEventError::OutsideRequest);
        }
        Ok(())
    }
}

/// The selector keeps its parser reservation until all retained allocations drop.
/// Deliberately not Clone: a consumer borrows the intent under its owner's grant.
pub struct StoredAcquisitionScope {
    identity: AcquisitionScopeId,
    scope: AcquisitionScope,
    selector: CollectionSelector,
    permit: OwnedPermit,
}

impl StoredAcquisitionScope {
    pub(crate) fn new(
        identity: AcquisitionScopeId,
        scope: AcquisitionScope,
        selector: CollectionSelector,
        permit: OwnedPermit,
    ) -> Self {
        Self {
            identity,
            scope,
            selector,
            permit,
        }
    }

    pub fn identity(&self) -> AcquisitionScopeId {
        self.identity
    }

    pub fn scope(&self) -> AcquisitionScope {
        self.scope
    }

    pub fn selector(&self) -> &CollectionSelector {
        &self.selector
    }

    /// Conservative retained reservation, not a measurement of allocator/RSS use.
    pub fn reserved_bytes(&self) -> u64 {
        self.permit.request().units().data_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::relay_source::RelaySourceBinding,
        runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    };
    use bigbrotr_model::{DictionaryKind, Ordinal, PolicyEpoch, QualifiedRef};
    use bigbrotr_protocol::{EventLimits, selector::decode_selector, verify_json};
    use serde_json::Value;

    fn fixture() -> (Value, StoredAcquisitionScope, ResourceBudget) {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/acquisition/selector-events.json"
        ))
        .unwrap();
        let selector =
            decode_selector(&serde_json::to_vec(&fixture["collection"]).unwrap()).unwrap();
        let dataset = [1; 16].try_into().unwrap();
        let scope = AcquisitionScope::new(
            dataset,
            [2; 16].try_into().unwrap(),
            [3; 16].try_into().unwrap(),
            [4; 16].try_into().unwrap(),
            QualifiedRef {
                dataset_id: dataset,
                dictionary_kind: DictionaryKind::VisibilityScope,
                allocation_domain: [5; 16].try_into().unwrap(),
                ordinal: Ordinal::new(1).unwrap(),
            },
            PolicyEpoch::new(1),
        )
        .unwrap();
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                32 * 1024 * 1024,
                8 * 1024 * 1024,
                ResourceLimits::new(8 * 1024 * 1024, 1, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let permit = budget
            .try_acquire(ResourceRequest::data(8 * 1024 * 1024, 1, 0).unwrap())
            .unwrap();
        (
            fixture,
            StoredAcquisitionScope::new([6; 16].try_into().unwrap(), scope, selector, permit),
            budget,
        )
    }

    fn source(scope: &StoredAcquisitionScope) -> RelaySourceBinding {
        RelaySourceBinding {
            endpoint_id: scope.scope().endpoint(),
            access_scope_id: scope.scope().access(),
            enrollment_generation: 1.try_into().unwrap(),
            route_id: [7; 16].try_into().unwrap(),
        }
    }

    #[test]
    fn event_context_checks_full_collection_before_request_and_keeps_scope_custody() {
        let (fixture, scope, budget) = fixture();
        let request = decode_selector(&serde_json::to_vec(&fixture["request"]).unwrap()).unwrap();
        let context = AcquisitionEventContext::new(&scope, request.filters()).unwrap();
        let expected = [
            Ok(()),
            Err(ScopeEventError::EphemeralExcluded),
            Err(ScopeEventError::OutsideCollection),
            Err(ScopeEventError::OutsideCollection),
            Err(ScopeEventError::OutsideCollection),
            Err(ScopeEventError::OutsideRequest),
        ];
        for (record, expected) in fixture["events"].as_array().unwrap().iter().zip(expected) {
            let event = verify_json(
                &serde_json::to_vec(&record["event"]).unwrap(),
                EventLimits::default(),
            )
            .unwrap();
            assert_eq!(
                context.check(source(&scope), scope.scope().visibility(), event.event()),
                expected,
                "{}",
                record["name"]
            );
        }
        assert_eq!(budget.snapshot().active_permits(), 1);
        drop(scope);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }

    #[test]
    fn event_context_refuses_empty_or_excess_requests_and_full_source_mismatch() {
        let (fixture, scope, _) = fixture();
        assert_eq!(
            AcquisitionEventContext::new(&scope, &[]).err(),
            Some(ScopeEventError::InvalidRequest)
        );
        let excess = vec![scope.selector().filters()[0].clone(); 17];
        assert_eq!(
            AcquisitionEventContext::new(&scope, &excess).err(),
            Some(ScopeEventError::InvalidRequest)
        );
        let context = AcquisitionEventContext::new(&scope, scope.selector().filters()).unwrap();
        let event = verify_json(
            &serde_json::to_vec(&fixture["events"][0]["event"]).unwrap(),
            EventLimits::default(),
        )
        .unwrap();
        let original = source(&scope);
        for changed in [
            RelaySourceBinding {
                endpoint_id: [8; 16].try_into().unwrap(),
                ..original
            },
            RelaySourceBinding {
                access_scope_id: [8; 16].try_into().unwrap(),
                ..original
            },
        ] {
            assert_eq!(
                context.check(changed, scope.scope().visibility(), event.event()),
                Err(ScopeEventError::SourceMismatch)
            );
        }
        let original_visibility = scope.scope().visibility();
        for changed in [
            QualifiedRef {
                dataset_id: [8; 16].try_into().unwrap(),
                ..original_visibility
            },
            QualifiedRef {
                allocation_domain: [8; 16].try_into().unwrap(),
                ..original_visibility
            },
            QualifiedRef {
                ordinal: Ordinal::new(2).unwrap(),
                ..original_visibility
            },
            QualifiedRef {
                dictionary_kind: DictionaryKind::Endpoint,
                ..original_visibility
            },
        ] {
            assert_eq!(
                context.check(original, changed, event.event()),
                Err(ScopeEventError::SourceMismatch)
            );
        }
    }
}
