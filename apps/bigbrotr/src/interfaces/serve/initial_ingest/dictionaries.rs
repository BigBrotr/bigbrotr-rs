//! Initial visibility identities; no accepted enrollment or job is rebound.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{InitialBatchRequest, InitialDictionaryAppendRequest},
    rocks::store::{AdmissionLimits, BootstrapLimits},
};
use bigbrotr_model::{
    DescriptorLabel, DictionaryDescriptor, DictionaryEntry, EvidenceIntent, EvidenceIntentLimits,
    ObservedTime, Ordinal, QualifiedRef, RequestSequence,
};

// The one private declaration is small. These bounds reserve decoding, closure
// and synchronized staging too; request2 never needs more than two open slots.
const PRIVATE_LIMITS: AdmissionLimits = AdmissionLimits {
    max_batch_bytes: 64 * 1024,
    max_dependencies: 8,
    max_outstanding_requests: 2,
};

pub(super) fn reserve_private(budget: &ResourceBudget) -> Result<OwnedPermit> {
    grant(
        budget,
        PRIVATE_LIMITS
            .required_dictionary_data_bytes()
            .expect("checked private bounds"),
        PRIVATE_LIMITS
            .required_native_bytes()
            .expect("checked private bounds"),
    )
}

pub(super) async fn initialize(
    custody: &mut Custody,
    spec: &InitialRegistration,
    root: &Resolved,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    bootstrap: OwnedPermit,
    private_grant: OwnedPermit,
) -> Result<(QualifiedRef, QualifiedRef)> {
    let timeout = duration(root, "renewal_timeout");
    let ttl = duration(root, "authorization_ttl");
    let path = Path::new(required(root, "storage.ingest_root")?);
    let public = QualifiedRef {
        dataset_id: spec.dataset_id,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: custody
            .store
            .as_ref()
            .unwrap()
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    let dictionaries = ["public", "private"]
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            DictionaryEntry::new(
                QualifiedRef {
                    ordinal: Ordinal::new(index as u64 + 1).unwrap(),
                    ..public
                },
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(label.into()).unwrap(),
                },
            )
            .unwrap()
        })
        .collect();
    let intent = EvidenceIntent::new(
        1,
        public,
        dictionaries,
        EvidenceIntentLimits {
            max_entries: 2,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .map_err(|e| (3, format!("initial dictionaries refused: {e:?}")))?;
    phase(progress, timeout)?;
    let receipt = bounded(timeout, cancellation, async {
        custody
            .owner
            .bootstrap_initial_dictionaries(
                custody.store.as_mut().unwrap(),
                InitialBatchRequest {
                    ingest_root: path,
                    registration: spec,
                    intent: &intent,
                    recorded_at: ObservedTime::new(spec.created_at_us),
                    limits: BootstrapLimits {
                        max_mappings: 2,
                        max_batch_bytes: MIB as usize,
                    },
                    policy_ttl: ttl,
                    cancelled: &cancellation.requested,
                },
                bootstrap,
                |doc, digest| validate(root, doc, digest),
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    drop(receipt);
    // Preserve both original mappings/input/receipt. Ordinal2's fact is public;
    // ordinal3 explicitly supplies a wholly private dependency root.
    let private = QualifiedRef {
        ordinal: Ordinal::new(3).unwrap(),
        ..public
    };
    let intent = EvidenceIntent::new(
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
            max_entries: 1,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .map_err(|e| (3, format!("initial private scope refused: {e:?}")))?;
    phase(progress, timeout)?;
    let receipt = bounded(timeout, cancellation, async {
        custody
            .owner
            .append_initial_dictionaries(
                custody.store.as_mut().unwrap(),
                InitialDictionaryAppendRequest {
                    ingest_root: path,
                    registration: spec,
                    request_sequence: RequestSequence::new(2).unwrap(),
                    intent: &intent,
                    recorded_at: ObservedTime::new(spec.created_at_us),
                    limits: PRIVATE_LIMITS,
                    policy_ttl: ttl,
                    cancelled: &cancellation.requested,
                },
                private_grant,
                |doc, digest| validate(root, doc, digest),
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    drop(receipt);
    Ok((public, private))
}
