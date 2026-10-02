//! Native append invariants; independent portable-byte checks live in the lab.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request, scope};
use super::*;
use bigbrotr_model::*;

const MIB: u64 = 1024 * 1024;
fn limits() -> AdmissionLimits {
    AdmissionLimits {
        max_batch_bytes: 256 * 1024,
        ..AdmissionLimits::default()
    }
}
fn bootstrap_limits() -> BootstrapLimits {
    BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: 256 * 1024,
    }
}
fn next_request(sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        request_sequence: RequestSequence::new(sequence).unwrap(),
        ..request()
    }
}
fn private_intent() -> EvidenceIntent {
    let private = QualifiedRef {
        ordinal: Ordinal::new(2).unwrap(),
        ..scope()
    };
    EvidenceIntent::new(
        1,
        private,
        vec![
            DictionaryEntry::new(
                private,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("private".into()).unwrap(),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap()
}

#[test]
fn append_preserves_original_bootstrap_and_replays_original_receipts_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let original = intent("public");
    let first = store
        .bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(10),
            bootstrap_limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    let original_bytes = first.receipt().encode_persistent(8192).unwrap();
    drop(first);
    let private = private_intent();
    let second = store
        .append_dictionaries(
            next_request(2),
            &private,
            ObservedTime::new(20),
            limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    let second_bytes = second.receipt().encode_persistent(8192).unwrap();
    assert_eq!(second.receipt().batch().unwrap().first().sequence.get(), 2);
    assert_eq!(second.receipt().dependencies().records().len(), 1);
    drop(second);
    let third = store
        .append_dictionaries(
            next_request(3),
            &private,
            ObservedTime::new(30),
            limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    assert!(
        third.receipt().batch().is_none(),
        "same mapping must not emit another fact"
    );
    drop(third);
    drop(store);
    let mut store =
        IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let before = store.db.latest_sequence_number();
    let first = store
        .bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(999),
            bootstrap_limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        first.receipt().encode_persistent(8192).unwrap(),
        original_bytes
    );
    drop(first);
    let second = store
        .append_dictionaries(
            next_request(2),
            &private,
            ObservedTime::new(999),
            limits(),
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        second.receipt().encode_persistent(8192).unwrap(),
        second_bytes
    );
    drop(second);
    assert_eq!(store.db.latest_sequence_number(), before);
}
