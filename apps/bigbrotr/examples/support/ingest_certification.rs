//! Signed existing-effect consumer; every read is compared with retained state.
use super::*;
use bigbrotr::storage::postgres::control_owner::InitialCertificationRequest;
use bigbrotr::storage::rocks::store::{
    CertificationLimits, CertificationOutcome, PreparedCertification,
};
use sha2::{Digest, Sha256};

async fn state(observer: &tokio_postgres::Client, path: &Path) -> serde_json::Value {
    let (rows, sequence) = raw_state(path);
    let native_sha256 = hex(&Sha256::digest(serde_json::to_vec(&rows).unwrap()));
    let mut control = Vec::new();
    for row in observer
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename",
            &[],
        )
        .await
        .unwrap()
    {
        let table: String = row.get(0);
        // Names come from the isolated fixture catalog; still quote identifiers.
        let quoted = table.replace('"', "\"\"");
        let query = format!(
            "SELECT coalesce(jsonb_agg(v ORDER BY v::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(r)||jsonb_build_object('xmin',xmin::text) v FROM control.\"{quoted}\" r) rows"
        );
        let rows: String = observer.query_one(&query, &[]).await.unwrap().get(0);
        control.push((table, rows));
    }
    assert!(!control.is_empty());
    let mut originals = std::fs::read_dir(path.join("producer-inputs-v1"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let bytes = std::fs::read(entry.path()).unwrap();
            (
                entry.file_name().into_string().unwrap(),
                serde_json::json!({"bytes":bytes.len(),"sha256":hex(&Sha256::digest(bytes))}),
            )
        })
        .collect::<Vec<_>>();
    originals.sort_by(|a, b| a.0.cmp(&b.0));
    serde_json::json!({"native_rows":rows.len(),"native_sha256":native_sha256,"sequence":sequence,
        "control_sha256":hex(&Sha256::digest(serde_json::to_vec(&control).unwrap())),"control_tables":control,"originals":originals})
}

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture carries existing owner handles and exact source grant"
)]
pub async fn run(
    owner: &mut ControlSession,
    observer: &tokio_postgres::Client,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    scope: QualifiedRef,
    lineage: QualifiedRef,
    event: &bigbrotr_model::SignedEvent,
) -> bool {
    if mode == "event-certification-retired" {
        observer.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=1",&[&&spec.producer_slot_id.as_bytes()[..]]).await.unwrap();
    }
    let before = state(observer, path).await;
    emit(serde_json::json!({"event":"certification-before","state":before}));
    let cases: &[&str] = match mode {
        "event-certification" => &[
            "existing",
            "existing",
            "missing-scope",
            "missing-lineage",
            "invalid",
            "size",
            "limit",
            "cancelled",
            "existing",
        ],
        "event-certification-expired" => &["expired"],
        "event-certification-policy" => &["policy"],
        "event-certification-retired" => &["retired"],
        _ => panic!("unknown certification scenario"),
    };
    for case in cases {
        let supplied_scope = if *case == "missing-scope" {
            QualifiedRef {
                ordinal: Ordinal::new(2).unwrap(),
                ..scope
            }
        } else {
            scope
        };
        let supplied_lineage = if *case == "missing-lineage" {
            QualifiedRef {
                ordinal: Ordinal::new(2).unwrap(),
                ..lineage
            }
        } else {
            lineage
        };
        let mut supplied = event.clone();
        if *case == "invalid" {
            supplied.sig = bigbrotr_model::Signature::from_bytes([0; 64]);
        }
        let input = PreparedCertification::new(
            EventIntent::import(1, supplied_scope, supplied, supplied_lineage).unwrap(),
            if *case == "size" { 8 } else { 256 * 1024 },
            grant(budget, MIB, 0),
        )
        .unwrap();
        let cancelled = AtomicBool::new(*case == "cancelled");
        let mut validations = 0;
        let result = owner
            .certify_initial_event(
                store,
                InitialCertificationRequest {
                    ingest_root: path,
                    registration: spec,
                    event: input,
                    provenance_enabled: true,
                    limits: CertificationLimits {
                        max_fact_bytes: if *case == "limit" {
                            32
                        } else {
                            2 * 1024 * 1024
                        },
                        ..CertificationLimits::default()
                    },
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
                },
                grant(budget, 32 * MIB, 0),
                |document, digest, candidate| {
                    validate(document, digest)?;
                    if candidate.visibility_scope_id() != supplied_scope
                        || candidate.origin()
                            != bigbrotr_model::EventOrigin::Import(supplied_lineage)
                        || candidate.observation().is_some()
                        || candidate.verification_limit_bytes()
                            != if *case == "size" { 8 } else { 256 * 1024 }
                        || candidate
                            .verified_event()
                            .is_some_and(|v| v.event().id != event.id)
                    {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    validations += 1;
                    if *case == "policy" {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    if *case == "expired" && validations == 1 {
                        std::thread::sleep(Duration::from_secs(3));
                    }
                    Ok(())
                },
            )
            .await;
        let output = match result {
            Ok(result) => match result.outcome() {
                CertificationOutcome::Existing(proof) => {
                    assert_eq!(*case, "existing");
                    serde_json::json!({"result":"existing","context":proof.protection_context(),"intent_digest":proof.intent_digest(),
                        "dependencies":proof.dependencies().records(),"proof_digest":proof.digest()})
                }
                CertificationOutcome::NeedsAdmission => {
                    assert!(matches!(*case, "missing-scope" | "missing-lineage"));
                    serde_json::json!({"result":"needs-admission"})
                }
                CertificationOutcome::Rejected(reason) => {
                    assert!(matches!(*case, "invalid" | "size"));
                    assert_eq!(
                        *reason,
                        if *case == "size" {
                            bigbrotr::storage::rocks::store::RejectionReason::EventSize
                        } else {
                            bigbrotr::storage::rocks::store::RejectionReason::Signature
                        }
                    );
                    serde_json::json!({"result":"rejected","reason":format!("{reason:?}")})
                }
            },
            Err(error) => {
                let expected=match *case {
                    "limit" => InitialEventError::Native(AdmissionError::Limit),
                    "cancelled" => InitialEventError::Native(AdmissionError::AuthorityUnavailable),
                    "expired" => InitialEventError::Control(OwnerError::Unavailable),
                    "policy" => InitialEventError::Control(OwnerError::InvalidPolicy),
                    "retired" => InitialEventError::Registry(bigbrotr::storage::postgres::ingest_registry::RegistryError::InvalidMetadata),
                    _ => panic!("unexpected certification refusal {error:?}"),
                };
                assert_eq!(error, expected);
                serde_json::json!({"result":"refused","error":format!("{error:?}")})
            }
        };
        emit(serde_json::json!({"event":"certification","case":case,"output":output}));
        assert_eq!(
            state(observer, path).await,
            before,
            "certification changed retained effects"
        );
    }
    emit(serde_json::json!({"event":"certification-after","state":state(observer,path).await}));
    mode == "event-certification"
}
