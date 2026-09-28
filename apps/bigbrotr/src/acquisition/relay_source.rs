//! Operational attribution for one arrival; never an acquisition permission.
use crate::network::RouteSelection;
use bigbrotr_model::{AccessScopeId, DictionaryDescriptor, DictionaryEntry, EndpointId, RouteId};
use bigbrotr_protocol::endpoint::Endpoint;
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

/// A conservative source restriction, not proof of committed scope or current
/// enrollment. Those are checked by the actual Control/native consumer. This
/// initial profile has no explicit public query-credential exception.
pub(crate) fn source_visibility_allowed(
    endpoint: &Endpoint,
    route: RouteSelection,
    visibility: &DictionaryEntry,
) -> bool {
    let DictionaryDescriptor::Visibility { class } = visibility.descriptor() else {
        return false;
    };
    class.as_str() != "public"
        || (matches!(route, RouteSelection::Public {}) && !endpoint.as_str().contains('?'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::RouteSelection;
    use bigbrotr_model::{
        DescriptorLabel, DictionaryDescriptor, DictionaryEntry, DictionaryKind, Ordinal,
        QualifiedRef,
    };
    use bigbrotr_protocol::endpoint::Endpoint;

    #[test]
    fn source_visibility_never_infers_public_permission_from_no_auth_or_a_label() {
        let public = RouteSelection::Public {};
        let private = RouteSelection::Private {
            cidr: "127.0.0.1/32".parse().unwrap(),
        };
        for (url, route, class, allowed) in [
            ("wss://relay.example/events", public, "public", true),
            ("wss://relay.example/events", public, "research", true),
            ("wss://relay.example/events?", public, "public", false),
            ("wss://relay.example/events?x=1", public, "public", false),
            ("wss://relay.example/events?x=1", public, "research", true),
            ("ws://127.0.0.1/private", private, "public", false),
            ("ws://127.0.0.1/private", private, "Public", true),
            ("ws://127.0.0.1/private", private, "research", true),
        ] {
            let entry = DictionaryEntry::new(
                QualifiedRef {
                    dataset_id: [1; 16].try_into().unwrap(),
                    dictionary_kind: DictionaryKind::VisibilityScope,
                    allocation_domain: [4; 16].try_into().unwrap(),
                    ordinal: Ordinal::new(1).unwrap(),
                },
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(class.into()).unwrap(),
                },
            )
            .unwrap();
            assert_eq!(
                source_visibility_allowed(&Endpoint::parse(url).unwrap(), route, &entry),
                allowed,
                "{url} {route:?} {class}"
            );
        }
    }
}
