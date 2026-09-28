//! Committed attribution, not source permission. Reuse the real native fixture.
use super::*;
use crate::acquisition::observation::{ObservationBindings, ObservationError};
use crate::network::RouteSelection;
use bigbrotr_protocol::endpoint::Endpoint;

fn bindings(observer: &DictionaryEntry, provenance: bool) -> ObservationBindings<'_> {
    ObservationBindings {
        visibility: reference(DictionaryKind::VisibilityScope, 1),
        observer,
        clock: observer,
        witness: provenance.then_some(WitnessContext {
            endpoint_id: reference(DictionaryKind::Endpoint, 1),
            access_scope_id: reference(DictionaryKind::AccessScope, 1),
            verification_method_id: reference(DictionaryKind::Method, 1),
        }),
    }
}
fn observer() -> DictionaryEntry {
    entries(false).remove(2)
}
fn read_limits() -> CertificationLimits {
    CertificationLimits {
        max_fact_bytes: 8192,
        max_dependencies: 16,
    }
}
fn quality() -> ClockQuality {
    ClockQuality::new(
        reference(DictionaryKind::Observer, 1),
        DurationUs::new(1),
        None,
        0,
    )
    .unwrap()
}

#[test]
fn observation_never_promotes_cross_scope_descriptors_and_supports_a_private_bundle() {
    let local = observer();
    let (_root, mut store, budget, _, _) = setup(false);
    let data = read_limits().required_data_bytes().unwrap();
    let mut input = bindings(&local, false);
    input.visibility = reference(DictionaryKind::VisibilityScope, 2);
    // Both initial visibility mappings and the observer have public envelopes.
    // Even provenance off cannot infer cross-scope descriptor grants.
    assert_eq!(
        store
            .read_observation(input, read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::InvalidInput)
    );
    let mut dictionaries = entries(true);
    for (index, class) in ["private", "public"].into_iter().enumerate() {
        dictionaries[index] = DictionaryEntry::new(
            dictionaries[index].reference(),
            DictionaryDescriptor::Visibility {
                class: label(class),
            },
        )
        .unwrap();
    }
    let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
    // The complete bootstrap envelope now belongs to the private first mapping.
    assert!(
        store
            .read_observation(
                bindings(&local, true),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .unwrap()
            .is_some()
    );
}

#[test]
fn observation_keeps_a_distinct_committed_clock_and_checks_its_route_too() {
    let local = observer();
    let DictionaryDescriptor::Observer(value) = local.descriptor() else {
        panic!()
    };
    let clock = DictionaryEntry::new(
        reference(DictionaryKind::Observer, 2),
        DictionaryDescriptor::Observer(ObserverDescriptor {
            vantage: Some(label("local-clock")),
            route_class: Some(RouteClass::Public),
            ..value.clone()
        }),
    )
    .unwrap();
    let mut dictionaries = entries(false);
    dictionaries.push(clock.clone());
    let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
    let mut input = bindings(&local, false);
    input.clock = &clock;
    let data = read_limits().required_data_bytes().unwrap();
    let context = store
        .read_observation(input, read_limits(), grant(&budget, data, 0), || Ok(()))
        .unwrap()
        .unwrap();
    let endpoint = Endpoint::parse("wss://public.example/").unwrap();
    let mut receive_clock = crate::acquisition::receive_clock::ReceiveClock::new().unwrap();
    let sample = receive_clock.sample().unwrap();
    let observation = context
        .received(&endpoint, RouteSelection::Public {}, sample)
        .unwrap();
    assert_eq!(observation.observer_id, local.reference());
    assert_eq!(observation.clock_quality.clock_id(), clock.reference());
    assert_eq!(observation.received_at, sample.received_at());
    assert_eq!(observation.clock_quality.uncertainty_us(), None);
    assert_eq!(
        context.received(
            &endpoint,
            RouteSelection::Private {
                cidr: "127.0.0.1/32".parse().unwrap()
            },
            sample
        ),
        Err(ObservationError::SourceMismatch)
    );
    drop(context);
    // A separate SystemUtc clock cannot repair a known contradictory observer
    // descriptor. Unknown observer clock attribution remains explicitly allowed.
    for declared in [None, Some(ClockSource::ExternalUtc)] {
        let who = DictionaryEntry::new(
            local.reference(),
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock: declared,
                ..value.clone()
            }),
        )
        .unwrap();
        let mut dictionaries = entries(false);
        dictionaries[2] = who.clone();
        dictionaries.push(clock.clone());
        let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
        let mut input = bindings(&who, false);
        input.clock = &clock;
        let result =
            store.read_observation(input, read_limits(), grant(&budget, data, 0), || Ok(()));
        match declared {
            None => assert!(result.unwrap().is_some()),
            Some(_) => assert_eq!(result.err(), Some(AdmissionError::InvalidInput)),
        }
    }
}

#[test]
fn observation_reopens_committed_bundle_and_matches_the_actual_source() {
    for provenance in [false, true] {
        let (root, store, budget, manifest, native) = setup(provenance);
        drop(store);
        let mut store = IngestStore::reopen(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        let before = store.db.latest_sequence_number();
        let data = read_limits().required_data_bytes().unwrap();
        let local = observer();
        let context = store
            .read_observation(
                bindings(&local, provenance),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(()),
            )
            .unwrap()
            .unwrap();
        let endpoint = Endpoint::parse("wss://public.example/").unwrap();
        let observation = context
            .observe(
                &endpoint,
                RouteSelection::Public {},
                ObservedTime::new(-1),
                quality(),
            )
            .unwrap();
        assert_eq!(observation.received_at, ObservedTime::new(-1));
        assert_eq!(observation.observer_id, local.reference());
        assert_eq!(observation.clock_quality, quality());
        assert_eq!(observation.witness, bindings(&local, provenance).witness);
        assert_eq!(
            context.visibility(),
            reference(DictionaryKind::VisibilityScope, 1)
        );
        let other = Endpoint::parse("wss://public.example/other").unwrap();
        let result = context.observe(
            &other,
            RouteSelection::Public {},
            ObservedTime::new(0),
            quality(),
        );
        assert_eq!(
            result.is_ok(),
            !provenance,
            "off must retain no endpoint restriction or hidden edge"
        );
        let foreign_clock = ClockQuality::new(
            reference(DictionaryKind::Observer, 2),
            DurationUs::new(1),
            None,
            0,
        )
        .unwrap();
        assert_eq!(
            context.observe(
                &endpoint,
                RouteSelection::Public {},
                ObservedTime::new(0),
                foreign_clock
            ),
            Err(ObservationError::ClockMismatch)
        );
        assert_eq!(store.db.latest_sequence_number(), before);
        assert_eq!(budget.snapshot().used().data_bytes(), data);
        drop(context);
        assert_eq!(budget.snapshot().used().data_bytes(), 0);
    }
}

#[test]
fn observation_requires_full_trusted_observer_and_system_clock_descriptors() {
    let (_root, mut store, budget, _, _) = setup(true);
    let before = store.db.latest_sequence_number();
    let data = read_limits().required_data_bytes().unwrap();
    let local = observer();
    let DictionaryDescriptor::Observer(value) = local.descriptor() else {
        panic!()
    };
    let changed = DictionaryEntry::new(
        local.reference(),
        DictionaryDescriptor::Observer(ObserverDescriptor {
            vantage: Some(label("another-observer")),
            ..value.clone()
        }),
    )
    .unwrap();
    for (who, clock) in [(&changed, &local), (&local, &changed)] {
        let mut input = bindings(who, true);
        input.clock = clock;
        assert_eq!(
            store
                .read_observation(input, read_limits(), grant(&budget, data, 0), || Ok(()))
                .err(),
            Some(AdmissionError::InvalidInput)
        );
    }
    let mut foreign = local.clone();
    foreign = DictionaryEntry::new(
        QualifiedRef {
            dataset_id: [77; 16].try_into().unwrap(),
            ..foreign.reference()
        },
        foreign.descriptor().clone(),
    )
    .unwrap();
    assert_eq!(
        store
            .read_observation(
                bindings(&foreign, false),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .err(),
        Some(AdmissionError::InvalidInput)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    drop(store);
    for clock in [None, Some(ClockSource::ExternalUtc)] {
        let mut dictionaries = entries(false);
        let entry = DictionaryEntry::new(
            local.reference(),
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock,
                ..value.clone()
            }),
        )
        .unwrap();
        dictionaries[2] = entry.clone();
        let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
        assert_eq!(
            store
                .read_observation(
                    bindings(&entry, false),
                    read_limits(),
                    grant(&budget, data, 0),
                    || Ok(())
                )
                .err(),
            Some(AdmissionError::InvalidInput)
        );
    }
}

#[test]
fn observation_refuses_protected_or_wrong_visibility_witnesses_and_route_attribution() {
    let local = observer();
    let (_root, mut store, budget, _, _) = setup(true);
    let data = read_limits().required_data_bytes().unwrap();
    let mut input = bindings(&local, true);
    input.witness.as_mut().unwrap().access_scope_id = reference(DictionaryKind::AccessScope, 2);
    assert_eq!(
        store
            .read_observation(input, read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::InvalidInput)
    );
    input = bindings(&local, true);
    input.visibility = reference(DictionaryKind::VisibilityScope, 2);
    assert_eq!(
        store
            .read_observation(input, read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::InvalidInput)
    );
    let mut dictionaries = entries(false);
    let DictionaryDescriptor::Observer(value) = local.descriptor() else {
        panic!()
    };
    let routed = DictionaryEntry::new(
        local.reference(),
        DictionaryDescriptor::Observer(ObserverDescriptor {
            route_class: Some(RouteClass::Private),
            ..value.clone()
        }),
    )
    .unwrap();
    dictionaries[2] = routed.clone();
    let (_root, mut store, budget, _, _) = setup_entries(dictionaries);
    let context = store
        .read_observation(
            bindings(&routed, false),
            read_limits(),
            grant(&budget, data, 0),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
    let endpoint = Endpoint::parse("ws://127.0.0.1:7447/").unwrap();
    assert_eq!(
        context.observe(
            &endpoint,
            RouteSelection::Public {},
            ObservedTime::new(0),
            quality()
        ),
        Err(ObservationError::SourceMismatch)
    );
    assert!(
        context
            .observe(
                &endpoint,
                RouteSelection::Private {
                    cidr: "127.0.0.1/32".parse().unwrap()
                },
                ObservedTime::new(0),
                quality()
            )
            .is_ok()
    );
}

#[test]
fn observation_distinguishes_absence_corruption_limits_and_lost_authority_without_writes() {
    let local = observer();
    let (_root, mut store, budget, _, _) = setup(false);
    let before = store.db.latest_sequence_number();
    let data = read_limits().required_data_bytes().unwrap();
    assert!(
        store
            .read_observation(
                bindings(&local, true),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_observation(
                bindings(&local, false),
                read_limits(),
                grant(&budget, 1, 0),
                || panic!("credit precedes native work")
            )
            .err(),
        Some(AdmissionError::InsufficientReservation)
    );
    let small = CertificationLimits {
        max_fact_bytes: 32,
        ..read_limits()
    };
    assert_eq!(
        store
            .read_observation(
                bindings(&local, false),
                small,
                grant(&budget, data, 0),
                || Ok(())
            )
            .err(),
        Some(AdmissionError::Limit)
    );
    assert!(
        store
            .read_observation(
                bindings(&local, false),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .unwrap()
            .is_some()
    );
    let mut checks = 0;
    assert_eq!(
        store
            .read_observation(
                bindings(&local, false),
                read_limits(),
                grant(&budget, data, 0),
                || {
                    checks += 1;
                    if checks == 1 {
                        Ok(())
                    } else {
                        Err(AdmissionError::AuthorityUnavailable)
                    }
                }
            )
            .err(),
        Some(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(checks, 2);
    store.write_uncertain = true;
    assert_eq!(
        store
            .read_observation(
                bindings(&local, false),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .err(),
        Some(AdmissionError::CommitUnknown)
    );
    store.write_uncertain = false;
    assert_eq!(store.db.latest_sequence_number(), before);
    let key = crate::storage::rocks::keys::IngestKey::Dictionary {
        reference: local.reference(),
    }
    .encode()
    .unwrap();
    let cf = store.db.cf_handle("default").unwrap();
    let mut original = store.db.get_cf(cf, key.as_bytes()).unwrap().unwrap();
    *original.last_mut().unwrap() ^= 1;
    store.db.put_cf(cf, key.as_bytes(), original).unwrap();
    let before = store.db.latest_sequence_number();
    assert_eq!(
        store
            .read_observation(
                bindings(&local, false),
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .err(),
        Some(AdmissionError::StateMismatch)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    assert_eq!(budget.snapshot().used().data_bytes(), 0);
}
