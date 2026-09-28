//! Actual native mappings; these tests do not grant source or network authority.
use super::{bootstrap_tests::*, *};
use crate::storage::rocks::keys::{ColumnFamily, IngestKey};
use bigbrotr_model::{DictionaryDescriptor, DictionaryKind, ObservedTime, Ordinal, QualifiedRef};

const MIB: u64 = 1024 * 1024;
fn read_limits() -> CertificationLimits {
    CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    }
}
fn bootstrapped(path: &Path, budget: &crate::runtime::ResourceBudget) -> IngestStore {
    let mut store =
        IngestStore::initialize(path, manifest(), native(), grant(budget, 0, 128 * MIB)).unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("restricted"),
                ObservedTime::new(123),
                BootstrapLimits {
                    max_mappings: 8,
                    max_batch_bytes: MIB as usize,
                },
                grant(budget, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    store
}

#[test]
fn visibility_lookup_reopens_exact_mapping_without_writing_or_allocating_source_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    drop(bootstrapped(&path, &budget));
    let mut store =
        IngestStore::reopen(&path, manifest(), native(), grant(&budget, 0, 128 * MIB)).unwrap();
    let before = store.db.latest_sequence_number();
    let data = read_limits().required_data_bytes().unwrap();
    let mapping = store
        .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || Ok(()))
        .unwrap()
        .expect("committed visibility mapping");
    assert_eq!(mapping.entry().reference(), scope());
    assert!(
        matches!(mapping.entry().descriptor(), DictionaryDescriptor::Visibility { class }
        if class.as_str()=="restricted")
    );
    assert_eq!(budget.snapshot().used().data_bytes(), data);
    assert_eq!(store.db.latest_sequence_number(), before);
    drop(mapping);
    assert_eq!(budget.snapshot().used().data_bytes(), 0);
    let absent = bigbrotr_model::QualifiedRef {
        ordinal: Ordinal::new(2).unwrap(),
        ..scope()
    };
    assert!(
        store
            .read_visibility(absent, read_limits(), grant(&budget, data, 0), || Ok(()))
            .unwrap()
            .is_none()
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    assert_eq!(budget.snapshot().used().data_bytes(), 0);
}

#[test]
fn visibility_lookup_refuses_foreign_namespace_and_nonvisibility_kind() {
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    let mut store = bootstrapped(&root.path().join("db"), &budget);
    let data = read_limits().required_data_bytes().unwrap();
    for invalid in [
        bigbrotr_model::QualifiedRef {
            dataset_id: [99; 16].try_into().unwrap(),
            ..scope()
        },
        bigbrotr_model::QualifiedRef {
            dictionary_kind: DictionaryKind::Endpoint,
            ..scope()
        },
    ] {
        assert_eq!(
            store
                .read_visibility(invalid, read_limits(), grant(&budget, data, 0), || Ok(()))
                .err(),
            Some(AdmissionError::InvalidInput)
        );
    }
}

#[test]
fn visibility_lookup_preserves_limit_retry_uncertainty_and_authority_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    let mut store = bootstrapped(&root.path().join("db"), &budget);
    let before = store.db.latest_sequence_number();
    let data = read_limits().required_data_bytes().unwrap();
    assert_eq!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, 1, 0), || panic!(
                "reservation must precede authority"
            ))
            .err(),
        Some(AdmissionError::InsufficientReservation)
    );
    let small = CertificationLimits {
        max_fact_bytes: 32,
        ..read_limits()
    };
    assert_eq!(
        store
            .read_visibility(scope(), small, grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::Limit)
    );
    assert!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || Ok(()))
            .unwrap()
            .is_some()
    );
    store.write_uncertain = true;
    assert_eq!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::CommitUnknown)
    );
    store.write_uncertain = false;
    let mut checks = 0;
    assert_eq!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || {
                checks += 1;
                if checks == 1 {
                    Ok(())
                } else {
                    Err(AdmissionError::AuthorityUnavailable)
                }
            })
            .err(),
        Some(AdmissionError::AuthorityUnavailable)
    );
    assert_eq!(checks, 2);
    assert_eq!(store.db.latest_sequence_number(), before);
    assert_eq!(budget.snapshot().used().data_bytes(), 0);
}

#[test]
fn visibility_lookup_never_confuses_a_broken_mapping_with_an_unallocated_scope() {
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    let mut store = bootstrapped(&root.path().join("db"), &budget);
    let data = read_limits().required_data_bytes().unwrap();
    let key = IngestKey::Dictionary { reference: scope() }
        .encode()
        .unwrap();
    let cf = store.db.cf_handle(key.column_family().name()).unwrap();
    let original = store.db.get_cf(cf, key.as_bytes()).unwrap().unwrap();
    assert_eq!(original.len(), 70);
    let mut fact_key = vec![1, 0x20];
    fact_key.extend_from_slice(&original[2..38]);
    let facts = store.db.cf_handle(ColumnFamily::Facts.name()).unwrap();
    let fact = store.db.get_cf(facts, &fact_key).unwrap().unwrap();
    store.db.delete_cf(facts, &fact_key).unwrap();
    let before = store.db.latest_sequence_number();
    assert_eq!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::StateMismatch)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    store
        .db
        .put_cf(store.db.cf_handle("facts").unwrap(), &fact_key, &fact)
        .unwrap();
    let mut bad = original.clone();
    *bad.last_mut().unwrap() ^= 1;
    store
        .db
        .put_cf(store.db.cf_handle("default").unwrap(), key.as_bytes(), bad)
        .unwrap();
    let before = store.db.latest_sequence_number();
    assert_eq!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::StateMismatch)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    store
        .db
        .put_cf(
            store.db.cf_handle("default").unwrap(),
            key.as_bytes(),
            original,
        )
        .unwrap();
    assert!(
        store
            .read_visibility(scope(), read_limits(), grant(&budget, data, 0), || Ok(()))
            .unwrap()
            .is_some()
    );
}

#[test]
fn visibility_lookup_requires_the_envelopes_visibility_mapping_too() {
    use bigbrotr_model::{DescriptorLabel, DictionaryEntry, EvidenceIntent, EvidenceIntentLimits};
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let second = QualifiedRef {
        ordinal: Ordinal::new(2).unwrap(),
        ..scope()
    };
    let entries = [(scope(), "public"), (second, "private")]
        .into_iter()
        .map(|(r, class)| {
            DictionaryEntry::new(
                r,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(class.into()).unwrap(),
                },
            )
            .unwrap()
        })
        .collect();
    let intent = EvidenceIntent::new(
        1,
        scope(),
        entries,
        EvidenceIntentLimits {
            max_entries: 2,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent,
                ObservedTime::new(123),
                BootstrapLimits {
                    max_mappings: 8,
                    max_batch_bytes: MIB as usize,
                },
                grant(&budget, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let data = read_limits().required_data_bytes().unwrap();
    let read = store
        .read_visibility(second, read_limits(), grant(&budget, data, 0), || Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(read.entry().reference(), second);
    assert!(
        matches!(read.entry().descriptor(), DictionaryDescriptor::Visibility { class } if class.as_str() == "private")
    );
    drop(read);
    let key = IngestKey::Dictionary { reference: scope() }
        .encode()
        .unwrap();
    let cf = store.db.cf_handle("default").unwrap();
    let original = store.db.get_cf(cf, key.as_bytes()).unwrap().unwrap();
    store.db.delete_cf(cf, key.as_bytes()).unwrap();
    let before = store.db.latest_sequence_number();
    assert_eq!(
        store
            .read_visibility(second, read_limits(), grant(&budget, data, 0), || Ok(()))
            .err(),
        Some(AdmissionError::StateMismatch)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    store
        .db
        .put_cf(
            store.db.cf_handle("default").unwrap(),
            key.as_bytes(),
            original,
        )
        .unwrap();
    assert!(
        store
            .read_visibility(second, read_limits(), grant(&budget, data, 0), || Ok(()))
            .unwrap()
            .is_some()
    );
    let foreign_domain = QualifiedRef {
        allocation_domain: [98; 16].try_into().unwrap(),
        ..second
    };
    assert!(
        store
            .read_visibility(
                foreign_domain,
                read_limits(),
                grant(&budget, data, 0),
                || Ok(())
            )
            .unwrap()
            .is_none()
    );
}
