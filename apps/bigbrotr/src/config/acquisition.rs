//! Accepted acquisition policy rebound to the actual deployment roots.
use super::{Resolved, policy::PolicySeed, private_routes};
use bigbrotr::{
    network::{BanSet, DirectPolicy, RouteSelection},
    runtime::policy_document::PolicyDocumentVersion,
    storage::{
        postgres::control_owner::OwnerError,
        rocks::store::{PreparedCertification, PreparedEvent},
    },
};
use bigbrotr_model::{
    EventOrigin, LocalObservation, QualifiedRef,
    acquisition::{CollectionSelector, EphemeralPolicy},
};
use bigbrotr_protocol::endpoint::Endpoint;

/// Called for each complete accepted document while Control holds the policy
/// and enrollment locks. This computes static restrictions, not a socket grant.
#[allow(
    dead_code,
    reason = "actual native probe consumes this owner; installed Sync integration remains pending"
)]
pub(crate) fn authorize_sync_source(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
    endpoint: &Endpoint,
    route: RouteSelection,
) -> Result<DirectPolicy, OwnerError> {
    source_policy(&restore_sync(root, document, digest)?, endpoint, route)
}

/// The finite request owner applies the registered intent before any wire work,
/// using the actual endpoint/route supplied by run_relay_source. No arrival
/// timestamp or witness is invented before receiving an event.
#[allow(
    dead_code,
    reason = "finite native consumer precedes installed Sync integration"
)]
pub(crate) fn authorize_sync_collection(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
    endpoint: &Endpoint,
    route: RouteSelection,
    selector: &CollectionSelector,
) -> Result<DirectPolicy, OwnerError> {
    let accepted = restore_sync(root, document, digest)?;
    require_collection(&accepted, selector)?;
    source_policy(&accepted, endpoint, route)
}

fn require_collection(
    accepted: &Resolved,
    selector: &CollectionSelector,
) -> Result<(), OwnerError> {
    // Explicit intent may narrow collection, but is never silently clamped to a
    // different predicate while keeping the original scope identity.
    if selector.history_since().0 < accepted.number("sync.history_since")
        || (accepted.text("sync.ephemeral") == Some("drop")
            && selector.ephemeral() != EphemeralPolicy::Drop)
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

fn restore_sync(root: &Resolved, document: &str, digest: [u8; 32]) -> Result<Resolved, OwnerError> {
    let accepted = PolicySeed::restore(document, digest, root.clone())
        .map_err(|_| OwnerError::InvalidPolicy)?;
    if accepted.policy_version != PolicyDocumentVersion::V2
        || !accepted.local("sync")
        || !accepted.flag("sync.enabled")
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(accepted)
}

fn source_policy(
    accepted: &Resolved,
    endpoint: &Endpoint,
    route: RouteSelection,
) -> Result<DirectPolicy, OwnerError> {
    if accepted.text("sync.endpoints.mode") == Some("fixed")
        && !accepted
            .value("sync.endpoints.urls")
            .as_array()
            .expect("validated list")
            .iter()
            .any(|url| url.as_str() == Some(endpoint.as_str()))
    {
        return Err(OwnerError::InvalidPolicy);
    }
    let bans = BanSet::parse(accepted.value("relay_policy.banned"))
        .map_err(|_| OwnerError::InvalidPolicy)?;
    if bans.contains(endpoint) {
        return Err(OwnerError::InvalidPolicy);
    }
    let (_, private) = private_routes::parse(accepted.value("network.private.rules"))
        .map_err(|_| OwnerError::InvalidPolicy)?;
    route
        .policy(
            endpoint,
            accepted.flag("network.public.enabled"),
            private.as_ref(),
        )
        .map_err(|_| OwnerError::InvalidPolicy)
}

/// Immutable attribution retained by the trusted receiving owner. This checker
/// does not establish socket custody or resolve dictionaries: the consumer must
/// bind the endpoint/route and full observation references to the actual source,
/// including committed endpoint/access descriptors when provenance is enabled.
/// selector and visibility must come from the same loaded scope borrowed by the
/// operation's AcquisitionEventContext, never an independently supplied predicate.
/// Control invokes these methods for both accepted documents at each fresh
/// operation; historical ambiguous-outcome recovery never uses this permission.
#[allow(
    dead_code,
    reason = "finite native consumer precedes installed Sync integration"
)]
pub(crate) struct SyncArrivalPolicy<'a> {
    pub root: &'a Resolved,
    pub endpoint: &'a Endpoint,
    pub route: RouteSelection,
    pub selector: &'a CollectionSelector,
    pub visibility: QualifiedRef,
    pub observation: LocalObservation,
    pub provenance_enabled: bool,
}

#[allow(
    dead_code,
    reason = "finite native consumer precedes installed Sync integration"
)]
impl SyncArrivalPolicy<'_> {
    pub fn admission(
        &self,
        document: &str,
        digest: [u8; 32],
        event: &PreparedEvent,
    ) -> Result<(), OwnerError> {
        let accepted = self.collection(document, digest)?;
        self.arrival(
            &accepted,
            event.verification_limit_bytes(),
            event.origin(),
            event.visibility_scope_id(),
            event.observation(),
        )
    }

    pub fn certification(
        &self,
        document: &str,
        digest: [u8; 32],
        event: &PreparedCertification,
    ) -> Result<(), OwnerError> {
        let accepted = self.collection(document, digest)?;
        self.arrival(
            &accepted,
            event.verification_limit_bytes(),
            event.origin(),
            event.visibility_scope_id(),
            event.observation(),
        )
    }

    fn collection(&self, document: &str, digest: [u8; 32]) -> Result<Resolved, OwnerError> {
        let accepted = restore_sync(self.root, document, digest)?;
        source_policy(&accepted, self.endpoint, self.route)?;
        require_collection(&accepted, self.selector)?;
        Ok(accepted)
    }

    fn arrival(
        &self,
        accepted: &Resolved,
        verification_limit: usize,
        origin: EventOrigin,
        visibility: QualifiedRef,
        observation: Option<&LocalObservation>,
    ) -> Result<(), OwnerError> {
        let provenance = accepted.text("sync.provenance") == Some("local_first_witness");
        if verification_limit as u64 != accepted.number("ingest.max_event_bytes")
            || origin != EventOrigin::Network
            || visibility != self.visibility
            || observation != Some(&self.observation)
            || self.provenance_enabled != provenance
            || self.observation.witness.is_some() != provenance
        {
            return Err(OwnerError::InvalidPolicy);
        }
        // Invalid signatures/IDs retain source attribution and the exact limit.
        // The native operation owns their explicit rejection; requiring a
        // VerifiedEvent here would incorrectly replace it with a policy error.
        // Collection/request membership is checked by AcquisitionEventContext.
        Ok(())
    }
}

#[cfg(test)]
#[path = "acquisition_tests.rs"]
mod arrival_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Resolved, policy::PolicySeed, resolve};
    use bigbrotr::network::RouteSelection;
    use bigbrotr_protocol::endpoint::Endpoint;
    use serde_json::json;

    fn root() -> Resolved {
        resolve::parse(include_str!("../../../../config/recipes/archive-only.toml")).unwrap()
    }

    fn check(root: &Resolved, accepted: &Resolved, url: &str, route: RouteSelection) -> bool {
        let seed = PolicySeed::from_resolved(accepted).unwrap();
        let endpoint = Endpoint::parse(url).unwrap();
        let target = bigbrotr::network::Target::new(endpoint.clone()).unwrap();
        // The selected policy applies its current-root intersection after DNS;
        // constructing it does not mean that any address has been allowed.
        let address = match route {
            RouteSelection::Public {} => "8.8.8.8",
            RouteSelection::Private { .. } => "127.0.0.1",
        }
        .parse()
        .unwrap();
        authorize_sync_source(root, seed.document(), seed.digest(), &endpoint, route)
            .is_ok_and(|policy| policy.filter(&target, &[address]).is_ok())
    }

    #[test]
    fn sync_source_requires_selected_enabled_service_and_accepted_public_setting() {
        let root = root();
        let route = RouteSelection::Public {};
        assert!(check(&root, &root, "wss://relay.example/events", route));
        let mut accepted = root.clone();
        accepted.values.insert("sync.enabled".into(), json!(false));
        assert!(!check(
            &root,
            &accepted,
            "wss://relay.example/events",
            route
        ));
        accepted = root.clone();
        accepted
            .values
            .insert("network.public.enabled".into(), json!(false));
        assert!(!check(
            &root,
            &accepted,
            "wss://relay.example/events",
            route
        ));
        let mut unselected = root.clone();
        unselected.local_roles.remove("sync");
        assert!(!check(
            &unselected,
            &root,
            "wss://relay.example/events",
            route
        ));
        // Candidate policy is unused after initialization; accepted values win.
        let mut drift = root.clone();
        drift.values.insert("sync.enabled".into(), json!(false));
        drift
            .values
            .insert("network.public.enabled".into(), json!(false));
        assert!(check(&drift, &root, "wss://relay.example/events", route));
    }

    #[test]
    fn sync_fixed_membership_intersects_exact_bans_and_never_widens_enrollment() {
        let root = root();
        let route = RouteSelection::Public {};
        let mut accepted = root.clone();
        accepted
            .values
            .insert("sync.endpoints.mode".into(), json!("fixed"));
        accepted.values.insert(
            "sync.endpoints.urls".into(),
            json!(["wss://relay.example/a?x=1&x=2"]),
        );
        assert!(check(
            &root,
            &accepted,
            "wss://relay.example/a?x=1&x=2",
            route
        ));
        for other in [
            "wss://relay.example/a?x=2&x=1",
            "wss://relay.example/a",
            "ws://relay.example/a?x=1&x=2",
        ] {
            assert!(!check(&root, &accepted, other, route));
        }
        for ban in [
            json!({"endpoint":"wss://relay.example/a?x=1&x=2"}),
            json!({"host":"relay.example"}),
        ] {
            accepted
                .values
                .insert("relay_policy.banned".into(), json!([ban]));
            assert!(!check(
                &root,
                &accepted,
                "wss://relay.example/a?x=1&x=2",
                route
            ));
        }
    }

    #[test]
    fn legacy_policy_never_inherits_empty_ban_authority() {
        let mut accepted = root();
        accepted.policy_version = bigbrotr::runtime::policy_document::PolicyDocumentVersion::V1;
        assert!(!check(
            &root(),
            &accepted,
            "wss://relay.example/",
            RouteSelection::Public {}
        ));
    }

    #[test]
    fn selected_private_route_uses_current_roots_not_a_stale_policy_copy() {
        let mut root = root();
        root.values.insert(
            "network.private.rules".into(),
            json!([
                {"endpoint":"ws://127.0.0.1:7447/private", "cidr":"127.0.0.1/32", "port":7447}
            ]),
        );
        let route = RouteSelection::Private {
            cidr: "127.0.0.1/32".parse().unwrap(),
        };
        assert!(check(&root, &root, "ws://127.0.0.1:7447/private", route));
        assert!(!check(&root, &root, "ws://127.0.0.1:7447/other", route));
        let accepted = root.clone();
        root.values
            .insert("network.private.rules".into(), json!([]));
        assert!(!check(
            &root,
            &accepted,
            "ws://127.0.0.1:7447/private",
            route
        ));
    }
}
