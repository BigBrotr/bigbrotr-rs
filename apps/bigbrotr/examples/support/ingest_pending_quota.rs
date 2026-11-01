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

/// Healthy below-low continuation for the existing append/intern socket modes.
/// All proposals occur before creating the unchanged 30s acquisition job. Their
/// refusals exercise the normal connected path; the real signed socket event is
/// admitted only after the original configured interval, with no extra facts.
#[allow(clippy::too_many_arguments, reason = "one retained healthy fixture")]
pub async fn resume_healthy(
    owner: &mut ControlSession,
    observer: &tokio_postgres::Client,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    next_sequence: u64,
    dictionaries: &EvidenceIntent,
    reconciliation: (tokio::time::Instant, tokio::time::Instant),
) {
    let stable = spec.intake_policy.stable_duration();
    assert!(!stable.is_zero());
    let (started, finished) = reconciliation;
    assert!(started <= finished);
    assert_eq!(
        store.observe_pending_quota().unwrap(),
        PendingQuotaStatus::Pressure
    );
    let pending = store
        .pending_accounting()
        .unwrap()
        .pending_bytes()
        .checked_add(store.pending_reserved_bytes().unwrap())
        .unwrap();
    assert!(pending < spec.intake_policy.pending_low());
    assert_eq!(store.pending_reserved_bytes().unwrap(), 0);
    let scope = dictionaries.visibility_scope_id();
    let fresh = EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                QualifiedRef {
                    ordinal: Ordinal::new(scope.ordinal.get() + 1).unwrap(),
                    ..scope
                },
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("quota-resume-probe".into()).unwrap(),
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
    let admission = AdmissionLimits {
        max_batch_bytes: MIB as usize,
        max_dependencies: 16,
        max_outstanding_requests: u64::from(spec.outstanding_request_limit),
    };
    let cancelled = AtomicBool::new(false);
    let before = ingest_snapshot::state(observer, path).await;
    emit(
        serde_json::json!({"event":"socket-quota-recovery","anchor":"reconciliation-start",
        "reconciliation_finished_us":u64::try_from(finished.duration_since(started).as_micros()).unwrap(),
        "stable_duration_us":u64::try_from(stable.as_micros()).unwrap(),"pending_bytes":pending,"held_bytes":0}),
    );
    for attempt in 0..3u32 {
        tokio::time::sleep_until(started + (stable / 3) * attempt).await;
        // Each refusal must be a normal positive proposal during the original
        // latch, never an attempted mutation after stable-low already completed.
        let attempted_at = started.elapsed();
        assert!(
            attempted_at < stable,
            "fixture missed its pressure interval"
        );
        let result = owner
            .append_initial_dictionaries(
                store,
                InitialDictionaryAppendRequest {
                    ingest_root: path,
                    registration: spec,
                    request_sequence: RequestSequence::new(next_sequence).unwrap(),
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
        let returned_at = started.elapsed();
        assert_eq!(result.err(), Some(InitialEventError::Pressure));
        assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Pressure);
        assert_eq!(store.pending_reserved_bytes().unwrap(), 0);
        let after = ingest_snapshot::state(observer, path).await;
        assert_eq!(after, before, "pressure created native/input/PG effects");
        emit(
            serde_json::json!({"event":"socket-quota-pressure","attempt":attempt+1,
            "started_us":u64::try_from(attempted_at.as_micros()).unwrap(),
            "finished_us":u64::try_from(returned_at.as_micros()).unwrap(),
            "result":"Pressure","held_bytes":0,"state":after}),
        );
    }
    // Binding happened no later than finished. Repeated refused proposals must
    // not shift this original end point. No operation or PG lease crosses sleep.
    tokio::time::sleep_until(finished + stable).await;
    assert_eq!(
        store.observe_pending_quota().unwrap(),
        PendingQuotaStatus::Available
    );
    assert_eq!(store.pending_reserved_bytes().unwrap(), 0);
    assert_eq!(ingest_snapshot::state(observer, path).await, before);
    emit(
        serde_json::json!({"event":"socket-quota-resumed","refusals":3,
        "elapsed_us":u64::try_from(started.elapsed().as_micros()).unwrap()}),
    );
}
