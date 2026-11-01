//! Same-Control OPEN reconciliation with independent clone-only PG injections.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialDictionaryAppendRequest, InitialEventError, InitialPendingQuotaRequest,
    },
    rocks::store::{AdmissionLimits, PendingQuotaError, PendingQuotaStatus},
};
use bigbrotr_model::RequestSequence;

async fn snapshot(
    observer: &tokio_postgres::Client,
    path: &Path,
    phase: &str,
) -> serde_json::Value {
    let state = ingest_snapshot::state(observer, path).await;
    let (rows, sequence) = raw_state(path);
    assert_eq!(state["sequence"].as_u64(), Some(sequence));
    emit(serde_json::json!({"event":"quota-state","phase":phase,"state":state,"rows":rows}));
    state
}

#[allow(
    clippy::too_many_arguments,
    reason = "finite probe retains the genuine owner and originals"
)]
pub async fn run(
    mut owner: ControlSession,
    observer: &tokio_postgres::Client,
    store: IngestStore,
    spec: &InitialRegistration,
    intent: &EvidenceIntent,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    receipt: Vec<u8>,
) {
    assert!(matches!(
        mode,
        "quota-reconcile-pressure"
            | "quota-reconcile-missing-outcome"
            | "quota-reconcile-reserved-ahead"
            | "quota-reconcile-outcome-conflict"
            | "quota-reconcile-foreign-request-tail"
            | "quota-reconcile-foreign-outcome-tail"
    ));
    let cancelled = AtomicBool::new(false);
    let manifest = *store.manifest();
    let seed = snapshot(observer, path, "seed").await;
    drop(store);
    let mut store = IngestStore::reopen(
        path,
        manifest,
        limits(),
        ingest_owner::grant(budget, 128 * MIB),
    )
    .unwrap();
    assert_eq!(
        store.pending_quota_status(),
        PendingQuotaStatus::RecoveryBlocked
    );
    assert_eq!(snapshot(observer, path, "reopened").await, seed);
    // The laboratory modifies only this fresh clone while no PG operation or
    // authority lease is held. Native custody and the original Control stay live.
    let ready = path.with_extension("quota-ready");
    let go = path.with_extension("quota-go");
    assert!(!ready.exists() && !go.exists());
    std::fs::write(ready, b"ready\n").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !go.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "quota fixture gate expired"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(std::fs::read(go).unwrap(), b"go\n");
    let before = snapshot(observer, path, "before-proof").await;
    let admission = AdmissionLimits {
        max_batch_bytes: MIB as usize,
        max_dependencies: 16,
        max_outstanding_requests: u64::from(spec.outstanding_request_limit),
    };
    let result = ingest_owner::reconcile_initial_pending_quota(
        &mut owner,
        &mut store,
        InitialPendingQuotaRequest {
            ingest_root: path,
            registration: spec,
            limits: ingest_recovery::initial_limits(admission),
            policy_ttl: Duration::from_secs(2),
            cancelled: &cancelled,
        },
        budget,
        validate,
    )
    .await;
    emit(serde_json::json!({"event":"quota-proof","mode":mode,
        "result":format!("{result:?}"),"status":format!("{:?}",store.pending_quota_status())}));
    if mode == "quota-reconcile-pressure" {
        assert_eq!(result, Ok(PendingQuotaStatus::Pressure));
        assert_eq!(snapshot(observer, path, "after-proof").await, before);
        // Known exact replay must remain possible immediately under the latch.
        let retry = owner
            .bootstrap_initial_dictionaries(
                &mut store,
                InitialBatchRequest {
                    ingest_root: path,
                    registration: spec,
                    intent,
                    recorded_at: ObservedTime::new(999),
                    limits: BootstrapLimits {
                        max_mappings: 8,
                        max_batch_bytes: MIB as usize,
                    },
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
                },
                grant(budget, 8 * MIB, 4 * MIB),
                validate,
            )
            .await
            .unwrap();
        assert_eq!(retry.receipt().encode_persistent(8192).unwrap(), receipt);
        drop(retry);
        assert_eq!(snapshot(observer, path, "after-known-retry").await, before);
    } else {
        let expected = if mode == "quota-reconcile-outcome-conflict" {
            InitialEventError::Outcome(
                bigbrotr::storage::postgres::producer_outcomes::OutcomeError::Conflict,
            )
        } else {
            InitialEventError::Control(OwnerError::InvalidMetadata)
        };
        assert_eq!(result, Err(expected));
        assert_eq!(
            store.pending_quota_status(),
            PendingQuotaStatus::RecoveryBlocked
        );
        assert_eq!(
            store.observe_pending_quota(),
            Err(PendingQuotaError::RecoveryBlocked)
        );
        assert_eq!(snapshot(observer, path, "after-proof").await, before);
    }
    let scope = intent.visibility_scope_id();
    let fresh = EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                QualifiedRef {
                    ordinal: Ordinal::new(3).unwrap(),
                    ..scope
                },
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("quota-new".into()).unwrap(),
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
    .unwrap();
    let next = owner
        .append_initial_dictionaries(
            &mut store,
            InitialDictionaryAppendRequest {
                ingest_root: path,
                registration: spec,
                request_sequence: RequestSequence::new(2).unwrap(),
                intent: &fresh,
                recorded_at: ObservedTime::new(1000),
                limits: admission,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(
                budget,
                admission.required_dictionary_data_bytes().unwrap(),
                admission.required_native_bytes().unwrap(),
            ),
            validate,
        )
        .await;
    let error = next
        .err()
        .expect("blocked quota must not admit the next original");
    emit(serde_json::json!({"event":"quota-next-refused","error":format!("{error:?}")}));
    if mode == "quota-reconcile-pressure" {
        assert_eq!(error, InitialEventError::Pressure);
    }
    assert_eq!(snapshot(observer, path, "after-next").await, before);
    let status = store.pending_quota_status();
    drop(store);
    // Leave all PG rows available for the independent exact comparison. The
    // parent joins the actual driver; dropping is not a claimed graceful release.
    drop(owner);
    emit(
        serde_json::json!({"event":"quota-complete","mode":mode,"next_input":false,"status":format!("{status:?}")}),
    );
}
