use bigbrotr_model::{
    EventId, NostrTime, PublicKey, Signature, SignedEvent,
    acquisition::{
        CollectionSelector, EphemeralPolicy, Filter, FilterFields, Membership, SelectorError,
    },
};
use std::collections::BTreeMap;

fn filter(fields: FilterFields) -> Filter {
    Filter::new(fields).unwrap()
}

fn selector(filters: Vec<Filter>) -> CollectionSelector {
    CollectionSelector::new(filters, NostrTime(0), EphemeralPolicy::Retain).unwrap()
}

fn event() -> SignedEvent {
    SignedEvent {
        id: EventId::from_bytes([0xaa; 32]),
        pubkey: PublicKey::from_bytes([0x11; 32]),
        created_at: NostrTime(23),
        kind: 1,
        tags: vec![
            vec!["t".into(), "é".into(), "later".into()],
            vec!["x".into()],
        ],
        content: String::new(),
        sig: Signature::from_bytes([0; 64]),
    }
}

#[test]
fn declared_order_and_duplicate_equivalences_share_one_identity() {
    let first = filter(FilterFields {
        ids: Some(vec![
            EventId::from_bytes([0xbb; 32]),
            EventId::from_bytes([0xaa; 32]),
            EventId::from_bytes([0xaa; 32]),
        ]),
        authors: Some(vec![PublicKey::from_bytes([0x11; 32])]),
        kinds: Some(vec![65535, 0, 65535]),
        tags: BTreeMap::from([
            (
                't',
                vec!["é".into(), "".into(), "e\u{301}".into(), "é".into()],
            ),
            ('e', vec!["00".repeat(32)]),
            ('A', vec!["z".into()]),
        ]),
        since: Some(NostrTime(0)),
        until: Some(NostrTime(u64::MAX)),
    });
    let rich = CollectionSelector::new(
        vec![first.clone(), first.clone()],
        NostrTime(23),
        EphemeralPolicy::Drop,
    )
    .unwrap();
    assert_eq!(rich.filters().len(), 1);
    assert_eq!(rich.filters()[0].fields().tags[&'t'], ["", "e\u{301}", "é"]);
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/collection-selector.json"
    ))
    .unwrap();
    assert_eq!(rich.digest().to_string(), vectors["vectors"][1]["sha256"]);
    let unrestricted = selector(vec![filter(FilterFields::default())]);
    assert_eq!(
        unrestricted.digest().to_string(),
        vectors["vectors"][0]["sha256"]
    );
    let other = filter(FilterFields {
        kinds: Some(vec![1]),
        ..FilterFields::default()
    });
    assert_eq!(
        selector(vec![first.clone(), other.clone()]).digest(),
        selector(vec![other, first]).digest()
    );
}

#[test]
fn explicit_policy_values_are_not_erased_by_boolean_simplification() {
    let base = selector(vec![filter(FilterFields::default())]);
    let with_since = selector(vec![filter(FilterFields {
        since: Some(NostrTime(0)),
        ..FilterFields::default()
    })]);
    let with_until = selector(vec![filter(FilterFields {
        until: Some(NostrTime(u64::MAX)),
        ..FilterFields::default()
    })]);
    assert_ne!(base.digest(), with_since.digest());
    assert_ne!(base.digest(), with_until.digest());
    let narrowed = filter(FilterFields {
        kinds: Some(vec![1]),
        ..FilterFields::default()
    });
    assert_ne!(
        base.digest(),
        selector(vec![filter(FilterFields::default()), narrowed]).digest()
    );
    let later = CollectionSelector::new(
        vec![filter(FilterFields::default())],
        NostrTime(1),
        EphemeralPolicy::Retain,
    )
    .unwrap();
    let drop = CollectionSelector::new(
        vec![filter(FilterFields::default())],
        NostrTime(0),
        EphemeralPolicy::Drop,
    )
    .unwrap();
    assert_ne!(base.digest(), later.digest());
    assert_ne!(base.digest(), drop.digest());
}

#[test]
fn union_order_matches_independent_typed_tuple_vector_not_cbor_byte_sorting() {
    let present = filter(FilterFields {
        ids: Some(vec![EventId::from_bytes([0xaa; 32])]),
        ..FilterFields::default()
    });
    let absent = filter(FilterFields {
        kinds: Some(vec![1]),
        ..FilterFields::default()
    });
    let value = selector(vec![present, absent]);
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/collection-selector.json"
    ))
    .unwrap();
    assert_eq!(value.digest().to_string(), vectors["vectors"][2]["sha256"]);
}

#[test]
fn conjunction_union_and_first_tag_value_use_exact_membership() {
    let selected = filter(FilterFields {
        ids: Some(vec![event().id]),
        authors: Some(vec![event().pubkey]),
        kinds: Some(vec![1]),
        tags: BTreeMap::from([('t', vec!["é".into()])]),
        since: Some(NostrTime(23)),
        until: Some(NostrTime(23)),
    });
    assert!(selected.matches(&event()));
    let mut changed = event();
    changed.id = EventId::from_bytes([0xbb; 32]);
    assert!(!selected.matches(&changed));
    changed = event();
    changed.pubkey = PublicKey::from_bytes([0x22; 32]);
    assert!(!selected.matches(&changed));
    changed = event();
    changed.kind = 2;
    assert!(!selected.matches(&changed));
    changed = event();
    changed.created_at = NostrTime(22);
    assert!(!selected.matches(&changed));
    changed.created_at = NostrTime(24);
    assert!(!selected.matches(&changed));
    changed = event();
    changed.tags[0][1] = "e\u{301}".into();
    assert!(!selected.matches(&changed));
    for value in ["later", "", "É"] {
        let tag = filter(FilterFields {
            tags: BTreeMap::from([('t', vec![value.into()])]),
            ..FilterFields::default()
        });
        assert!(!tag.matches(&event()));
    }
    let short = filter(FilterFields {
        tags: BTreeMap::from([('x', vec!["".into()])]),
        ..FilterFields::default()
    });
    assert!(!short.matches(&event()));
    let union = selector(vec![
        selected,
        filter(FilterFields {
            kinds: Some(vec![2]),
            ..FilterFields::default()
        }),
    ]);
    changed = event();
    changed.kind = 2;
    changed.id = EventId::from_bytes([0xff; 32]);
    assert_eq!(union.classify(&changed), Membership::Included);
}

#[test]
fn local_ephemeral_exclusion_is_distinct_from_request_filter_membership() {
    let wire = filter(FilterFields::default());
    let collection =
        CollectionSelector::new(vec![wire.clone()], NostrTime(23), EphemeralPolicy::Drop).unwrap();
    for (kind, expected) in [
        (19999, Membership::Included),
        (20000, Membership::EphemeralExcluded),
        (22222, Membership::EphemeralExcluded),
        (29999, Membership::EphemeralExcluded),
        (30000, Membership::Included),
    ] {
        let mut candidate = event();
        candidate.kind = kind;
        assert!(wire.matches(&candidate));
        assert_eq!(collection.classify(&candidate), expected);
        candidate.created_at = NostrTime(22);
        assert_eq!(collection.classify(&candidate), Membership::OutsideSelector);
    }
    let impossible = selector(vec![filter(FilterFields {
        since: Some(NostrTime(1)),
        until: Some(NostrTime(0)),
        ..FilterFields::default()
    })]);
    for timestamp in [0, 1, u64::MAX] {
        let mut candidate = event();
        candidate.created_at = NostrTime(timestamp);
        assert_eq!(impossible.classify(&candidate), Membership::OutsideSelector);
    }
}

#[test]
fn invalid_or_empty_constraints_are_never_broadened() {
    for fields in [
        FilterFields {
            ids: Some(vec![]),
            ..FilterFields::default()
        },
        FilterFields {
            authors: Some(vec![]),
            ..FilterFields::default()
        },
        FilterFields {
            kinds: Some(vec![]),
            ..FilterFields::default()
        },
        FilterFields {
            tags: BTreeMap::from([('t', vec![])]),
            ..FilterFields::default()
        },
    ] {
        assert_eq!(Filter::new(fields).err(), Some(SelectorError::EmptyList));
    }
    assert_eq!(
        CollectionSelector::new(vec![], NostrTime(0), EphemeralPolicy::Retain).err(),
        Some(SelectorError::EmptyUnion)
    );
    for key in ['1', 'é', '#', '\0'] {
        assert_eq!(
            Filter::new(FilterFields {
                tags: BTreeMap::from([(key, vec!["x".into()])]),
                ..FilterFields::default()
            })
            .err(),
            Some(SelectorError::InvalidTag)
        );
    }
    for key in ['e', 'p'] {
        for value in ["", "00", &"AB".repeat(32), &"gg".repeat(32)] {
            assert_eq!(
                Filter::new(FilterFields {
                    tags: BTreeMap::from([(key, vec![value.into()])]),
                    ..FilterFields::default()
                })
                .err(),
                Some(SelectorError::InvalidTagValue)
            );
        }
    }
}

#[test]
fn profile_work_limits_apply_before_list_normalization() {
    assert!(
        Filter::new(FilterFields {
            kinds: Some(vec![1; 1024]),
            ..FilterFields::default()
        })
        .is_ok()
    );
    assert_eq!(
        Filter::new(FilterFields {
            kinds: Some(vec![1; 1025]),
            ..FilterFields::default()
        })
        .err(),
        Some(SelectorError::ValueLimit)
    );
    assert!(
        CollectionSelector::new(
            vec![filter(FilterFields::default()); 16],
            NostrTime(0),
            EphemeralPolicy::Retain
        )
        .is_ok()
    );
    assert_eq!(
        CollectionSelector::new(
            vec![filter(FilterFields::default()); 17],
            NostrTime(0),
            EphemeralPolicy::Retain
        )
        .err(),
        Some(SelectorError::FilterLimit)
    );
    assert!(
        Filter::new(FilterFields {
            tags: BTreeMap::from([('t', vec!["x".repeat(1024 * 1024)])]),
            ..FilterFields::default()
        })
        .is_ok()
    );
    assert_eq!(
        Filter::new(FilterFields {
            tags: BTreeMap::from([('t', vec!["x".repeat(1024 * 1024 + 1)])]),
            ..FilterFields::default()
        })
        .err(),
        Some(SelectorError::TextLimit)
    );
}

#[test]
fn aggregate_limits_hold_across_conjunctions_before_union_deduplication() {
    let thousand = filter(FilterFields {
        kinds: Some((0..1024).collect()),
        ..FilterFields::default()
    });
    assert!(
        CollectionSelector::new(
            vec![thousand.clone(); 16],
            NostrTime(0),
            EphemeralPolicy::Retain
        )
        .is_ok()
    );
    let extra = filter(FilterFields {
        kinds: Some((0..1024).collect()),
        ids: Some(vec![EventId::from_bytes([1; 32])]),
        ..FilterFields::default()
    });
    let mut filters = vec![thousand; 15];
    filters.push(extra);
    assert_eq!(
        CollectionSelector::new(filters, NostrTime(0), EphemeralPolicy::Retain).err(),
        Some(SelectorError::ValueLimit)
    );
    let half = filter(FilterFields {
        tags: BTreeMap::from([('t', vec!["x".repeat(512 * 1024)])]),
        ..FilterFields::default()
    });
    assert!(
        CollectionSelector::new(vec![half.clone(); 2], NostrTime(0), EphemeralPolicy::Retain)
            .is_ok()
    );
    let extra = filter(FilterFields {
        tags: BTreeMap::from([('t', vec!["x".repeat(512 * 1024 + 1)])]),
        ..FilterFields::default()
    });
    assert_eq!(
        CollectionSelector::new(vec![half, extra], NostrTime(0), EphemeralPolicy::Retain).err(),
        Some(SelectorError::TextLimit)
    );
}
