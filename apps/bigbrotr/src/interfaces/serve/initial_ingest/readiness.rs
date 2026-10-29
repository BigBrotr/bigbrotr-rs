//! Bounded observations from the serial native owner, never execution authority.
use super::{
    index::{
        Index,
        source::{Observed, Source as IndexSource},
    },
    owner::NativeOwner,
};
use bigbrotr::{
    management::{grants::Revision, roles::*, sessions::SessionError},
    runtime::policy_authority::{AuthorityClock, AuthorityTick, SuspendAwareClock},
};
use bigbrotr_model::{DatasetId, SourceCut, TopologyGeneration};
use std::{sync::atomic::AtomicBool, time::Duration};

pub(super) const FRESHNESS: Duration = Duration::from_secs(30);

/// Fixed-size metadata fits the existing retained initial-owner reservation.
#[derive(Clone, Copy, Default)]
pub(super) struct Source {
    observed: Option<(Observed, AuthorityTick)>,
}
impl Source {
    pub(super) fn refresh(
        &mut self,
        owner: &NativeOwner,
        cancelled: &AtomicBool,
        live_index: bool,
    ) {
        self.observed = (|| {
            // Preserve the older successor profiles' unknown observation. The
            // explicit live Index lifetime owns the new complete-vector reader.
            if matches!(owner, NativeOwner::Live(_)) && !live_index {
                return None;
            }
            let source = IndexSource::from_owner(owner).ok()?;
            let started = SuspendAwareClock.read().ok()?;
            let head = source.observe(cancelled).ok()?;
            Some((head, started))
        })();
    }
}

#[derive(Clone, Copy)]
pub(super) struct ArchiveObservation {
    pub dataset: DatasetId,
    pub topology: TopologyGeneration,
    pub shard: u32,
    pub revision: u64,
    pub through: u64,
    pub started: AuthorityTick,
}

#[derive(Clone, Copy, Default)]
pub(super) struct ArchiveStatus {
    pub publication: Option<ArchiveObservation>,
    pub verifying: bool,
    pub connection_refused: bool,
}

fn cut(head: Observed) -> Result<SourceCut, SessionError> {
    head.cut().map_err(|_| SessionError::InvalidMetadata)
}

fn age(started: AuthorityTick) -> Option<Duration> {
    SuspendAwareClock.read().ok()?.elapsed_since(started).ok()
}
fn time(at: i64, elapsed: Option<Duration>) -> Result<Option<ObservationTime>, SessionError> {
    elapsed.map(|age| ObservationTime::new(at, age)).transpose()
}
fn freshness(elapsed: Option<Duration>) -> Freshness {
    match elapsed {
        Some(age) if age < FRESHNESS => Freshness::Fresh,
        Some(_) => Freshness::Stale,
        None => Freshness::Unknown,
    }
}
fn capability(kind: CapabilityKind) -> CapabilityObservation {
    CapabilityObservation {
        capability: kind,
        state: CapabilityState::Building,
        reason: StatusReason::NotObserved,
        freshness: Freshness::Unknown,
        time: None,
        published: None,
        generation: None,
        storage_incarnation: None,
    }
}
fn role(kind: RoleKind, state: RoleState, reason: StatusReason) -> RoleObservation {
    RoleObservation {
        role: kind,
        state,
        reason,
    }
}
fn revision(value: u64) -> Result<Revision, SessionError> {
    value.to_string().try_into()
}

/// At most three bounded one- or two-stream cuts are copied. No history-sized collection or
/// engine operation is admitted by a status request. The request's64KiB grant
/// remains held until the separately charged response takes ownership.
pub(super) fn snapshot(
    identity: SnapshotIdentity,
    observed_at_us: i64,
    source: Source,
    sync: bool,
    archive: Option<ArchiveStatus>,
    index: Option<&Index>,
) -> Result<RoleSnapshot, SessionError> {
    let source_age = source.observed.and_then(|(_, start)| age(start));
    let source_fresh = freshness(source_age);
    // A broken clock cannot attach an unaged cut to an unknown observation.
    let current = source
        .observed
        .filter(|_| source_age.is_some())
        .map(|v| v.0);
    let source_reason = match source_fresh {
        Freshness::Fresh => StatusReason::Current,
        Freshness::Stale => StatusReason::VerificationStale,
        Freshness::Unknown => StatusReason::SourceUnavailable,
    };
    let source = SourceObservation {
        cut: current.map(cut).transpose()?,
        freshness: source_fresh,
        time: time(observed_at_us, source_age)?,
        reason: source_reason,
    };
    let ready_source = source_fresh == Freshness::Fresh;
    let mut roles = vec![
        role(RoleKind::Control, RoleState::Ready, StatusReason::Current),
        role(
            RoleKind::Ingest,
            if ready_source {
                RoleState::Idle
            } else {
                RoleState::Degraded
            },
            source_reason,
        ),
    ];
    let mut admission = capability(CapabilityKind::SignedEventAdmission);
    admission.state = if ready_source {
        CapabilityState::Ready
    } else {
        CapabilityState::Unavailable
    };
    admission.reason = source_reason;
    admission.freshness = source_fresh;
    admission.time = time(observed_at_us, source_age)?;
    let mut capabilities = vec![admission];
    if sync {
        roles.push(role(
            RoleKind::Sync,
            if ready_source {
                RoleState::Idle
            } else {
                RoleState::Degraded
            },
            source_reason,
        ));
        let mut acquisition = capability(CapabilityKind::FiniteAcquisition);
        acquisition.state = if ready_source {
            CapabilityState::Ready
        } else {
            CapabilityState::Unavailable
        };
        acquisition.reason = source_reason;
        acquisition.freshness = source_fresh;
        acquisition.time = time(observed_at_us, source_age)?;
        capabilities.push(acquisition);
    }
    if let Some(archive) = archive {
        let mut cap = capability(CapabilityKind::ArchivePublication);
        if let Some(publication) = archive.publication {
            let elapsed = age(publication.started);
            cap.freshness = freshness(elapsed);
            cap.time = time(observed_at_us, elapsed)?;
            let bound = bigbrotr_model::StreamBound {
                stream: bigbrotr_model::StreamId {
                    shard_id: publication.shard,
                    owner_epoch: bigbrotr_model::OwnerEpoch::new(1).unwrap(),
                },
                last_complete_sequence: bigbrotr_model::CompleteThrough::new(publication.through),
            };
            cap.published = Some(PublishedCut {
                revision: revision(publication.revision)?,
                cut: SourceCut::new(publication.dataset, publication.topology, vec![bound])
                    .map_err(|_| SessionError::InvalidMetadata)?,
            });
            (cap.state, cap.reason) = publication_state(
                ready_source,
                cap.freshness,
                current.is_some_and(|head| {
                    head.identity().dataset_id == publication.dataset
                        && head.identity().topology_generation == publication.topology
                        && head.bound() == bound
                }),
            );
        }
        mark_verifying(&mut cap, archive.verifying);
        if archive.connection_refused {
            // Retain the exact last verified cut and its original age. The
            // failed new attempt is neither a publication nor fresh evidence.
            cap.state = CapabilityState::Unavailable;
            cap.reason = StatusReason::DependencyUnavailable;
        }
        roles.push(role(RoleKind::Archive, health(&cap), cap.reason));
        capabilities.push(cap);
    }
    if let Some(index) = index {
        let observed = index.readiness_observation();
        let mut cap = capability(CapabilityKind::IndexPublication);
        if let Some(verified) = observed.verified {
            let elapsed = age(verified.started);
            cap.freshness = if elapsed.is_some_and(|age| age >= observed.freshness) {
                Freshness::Stale
            } else {
                freshness(elapsed)
            };
            cap.time = time(observed_at_us, elapsed)?;
            let verified_cut = cut(verified.source)?;
            let caught_up = current == Some(verified.source)
                && index.published_revision() == verified.revision
                && index.frontier() == Some(&verified_cut);
            cap.published = Some(PublishedCut {
                revision: revision(verified.revision)?,
                cut: verified_cut,
            });
            cap.generation = Some(verified.generation);
            cap.storage_incarnation = Some(verified.storage_incarnation);
            (cap.state, cap.reason) = publication_state(ready_source, cap.freshness, caught_up);
        }
        if observed.parked {
            cap.state = CapabilityState::Unavailable;
            cap.reason = if observed.unresolved {
                StatusReason::RemoteOutcomeUnknown
            } else {
                StatusReason::DependencyUnavailable
            };
        } else if current.is_some_and(|source| !source.within_history_limit())
            || observed.failure.is_some()
        {
            cap.state = CapabilityState::Unavailable;
            cap.reason = StatusReason::DependencyUnavailable;
        } else if (observed.building || observed.active) && cap.state != CapabilityState::Ready {
            cap.state = CapabilityState::Building;
            cap.reason = StatusReason::VerificationInFlight;
        }
        roles.push(role(RoleKind::Index, health(&cap), cap.reason));
        capabilities.push(cap);
    }
    RoleSnapshot::new(identity, observed_at_us, source, roles, capabilities)
}

fn publication_state(
    source: bool,
    fresh: Freshness,
    caught_up: bool,
) -> (CapabilityState, StatusReason) {
    if !source {
        (
            CapabilityState::Unavailable,
            StatusReason::SourceUnavailable,
        )
    } else if fresh != Freshness::Fresh {
        (CapabilityState::Stale, StatusReason::VerificationStale)
    } else if !caught_up {
        (CapabilityState::Building, StatusReason::BehindSource)
    } else {
        (CapabilityState::Ready, StatusReason::Current)
    }
}
fn health(capability: &CapabilityObservation) -> RoleState {
    match capability.state {
        CapabilityState::Ready => RoleState::Idle,
        CapabilityState::Building => RoleState::Starting,
        _ => RoleState::Degraded,
    }
}

fn mark_verifying(capability: &mut CapabilityObservation, verifying: bool) {
    if verifying && capability.state != CapabilityState::Unavailable {
        capability.state = CapabilityState::Building;
        capability.reason = StatusReason::VerificationInFlight;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parked_archive_keeps_the_old_cut_without_claiming_verification_or_control_failure() {
        let identity = SnapshotIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            process_incarnation: [2; 16].try_into().unwrap(),
            config_revision: revision(1).unwrap(),
            policy_revision: revision(1).unwrap(),
        };
        let started = SuspendAwareClock.read().unwrap();
        for publication in [
            None,
            Some(ArchiveObservation {
                dataset: identity.dataset_id,
                topology: TopologyGeneration::new(0),
                shard: 0,
                revision: 2,
                through: 3,
                started,
            }),
        ] {
            let value = serde_json::to_value(
                snapshot(
                    identity,
                    123,
                    Source::default(),
                    true,
                    Some(ArchiveStatus {
                        publication,
                        verifying: false,
                        connection_refused: true,
                    }),
                    None,
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(value["roles"][0]["state"], "ready");
            assert_eq!(value["roles"][3]["role"], "archive");
            assert_eq!(value["roles"][3]["state"], "degraded");
            let archive = &value["capabilities"][2];
            assert_eq!(archive["state"], "unavailable");
            assert_eq!(archive["reason"], "dependency_unavailable");
            if publication.is_some() {
                assert_eq!(archive["published"]["revision"], "2");
                assert_eq!(
                    archive["published"]["cut"],
                    serde_json::json!({
                        "dataset_id": "01010101010101010101010101010101",
                        "topology_generation": "0",
                        "bounds": [{"shard_id": "0", "owner_epoch": "1", "last_complete_sequence": "3"}]
                    })
                );
                assert!(!archive["time"].is_null());
            } else {
                assert!(archive["published"].is_null());
                assert!(archive["time"].is_null());
            }
        }
    }

    #[test]
    fn missing_native_observation_never_implies_empty_or_ready_source() {
        let identity = SnapshotIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            process_incarnation: [2; 16].try_into().unwrap(),
            config_revision: revision(1).unwrap(),
            policy_revision: revision(1).unwrap(),
        };
        let snapshot = snapshot(
            identity,
            123,
            Source::default(),
            true,
            Some(ArchiveStatus::default()),
            None,
        )
        .unwrap();
        let value = serde_json::to_value(snapshot).unwrap();
        assert_eq!(value["source"]["cut"], serde_json::Value::Null);
        assert_eq!(value["source"]["freshness"], "unknown");
        assert_eq!(value["roles"][0]["state"], "ready");
        assert_eq!(value["roles"][1]["state"], "degraded");
        let capabilities = value["capabilities"].as_array().unwrap();
        assert_eq!(capabilities.len(), 3);
        assert_eq!(capabilities[0]["state"], "unavailable");
        assert_eq!(capabilities[1]["state"], "unavailable");
        assert_eq!(capabilities[2]["state"], "building");
        assert!(capabilities.iter().all(|cap| cap["published"].is_null()));
    }

    #[test]
    fn publication_requires_source_fresh_physical_rows_and_matching_cut() {
        assert_eq!(
            publication_state(true, Freshness::Fresh, true),
            (CapabilityState::Ready, StatusReason::Current)
        );
        assert_eq!(
            publication_state(true, Freshness::Fresh, false),
            (CapabilityState::Building, StatusReason::BehindSource)
        );
        assert_eq!(
            publication_state(true, Freshness::Stale, true),
            (CapabilityState::Stale, StatusReason::VerificationStale)
        );
        assert_eq!(
            publication_state(false, Freshness::Fresh, true),
            (
                CapabilityState::Unavailable,
                StatusReason::SourceUnavailable
            )
        );
        assert_eq!(freshness(Some(FRESHNESS)), Freshness::Stale);
        assert_eq!(freshness(None), Freshness::Unknown);
        // A failed catalog driver can still be joining its native consumer.
        // Even a prior fresh successful observation cannot remain Ready while
        // that combined operation services a nested status request.
        let mut archive = capability(CapabilityKind::ArchivePublication);
        archive.state = CapabilityState::Ready;
        archive.reason = StatusReason::Current;
        archive.freshness = Freshness::Fresh;
        mark_verifying(&mut archive, true);
        assert_eq!(archive.state, CapabilityState::Building);
        assert_eq!(archive.reason, StatusReason::VerificationInFlight);
        archive.state = CapabilityState::Unavailable;
        archive.reason = StatusReason::SourceUnavailable;
        mark_verifying(&mut archive, true);
        assert_eq!(archive.state, CapabilityState::Unavailable);
        assert_eq!(archive.reason, StatusReason::SourceUnavailable);
    }
}
