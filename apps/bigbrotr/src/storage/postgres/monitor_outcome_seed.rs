//! Opt-in connected fixture, private to cfg(test). Never a production copy API.
use super::*;
use crate::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    secrets::SecretFile,
    storage::{
        postgres::{producer_outcomes, producer_requests},
        rocks::{
            RocksNativeLimits, RocksNativeSettings,
            store::{
                AdmissionLimits, AdmissionOutcome, BootstrapLimits, IngestRecovery,
                ProducerRecoveryLimits, RetainedRequestInput,
            },
        },
    },
};
use bigbrotr_model::{
    ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use serde::Deserialize;
use serde_json::Value;
use std::{fs::OpenOptions, io::Write, path::Path, sync::atomic::AtomicBool};
use tokio_postgres::{Config, NoTls};

const MIB: u64 = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    revision: u64,
    document: Value,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Policies {
    config: Document,
    policy: Document,
    deployment_sha256: String,
    roots: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    format: String,
    dataset_id: String,
    instance_id: String,
    producer_slot_id: String,
    manifest_hex: String,
    policies: Policies,
    expected_receipt_hex: String,
    #[serde(default)]
    phase: Option<String>,
}

fn bytes<const N: usize>(text: &str) -> [u8; N] {
    assert!(
        text.len() == 2 * N
            && text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "fixture requires canonical fixed hex"
    );
    let mut result = [0; N];
    for (index, value) in result.iter_mut().enumerate() {
        *value = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).expect("checked hex");
    }
    result
}

fn encoded(text: &str, maximum: usize) -> Vec<u8> {
    assert!(
        text.len() <= 2 * maximum && text.len().is_multiple_of(2),
        "fixture encoding bound"
    );
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| bytes::<1>(std::str::from_utf8(pair).expect("hex"))[0])
        .collect()
}

fn validate(pair: &Document, document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    use crate::runtime::policy_document::PolicyDocumentVersion;
    let expected = serde_json::to_vec(&pair.document).map_err(|_| OwnerError::InvalidPolicy)?;
    let version =
        PolicyDocumentVersion::from_document(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let actual: Value = serde_json::from_str(document).map_err(|_| OwnerError::InvalidPolicy)?;
    let hash: [u8; 32] = Sha256::new()
        .chain_update(version.digest_domain())
        .chain_update(expected)
        .finalize()
        .into();
    if pair.revision != 1
        || actual != pair.document
        || hash != bytes::<32>(&pair.sha256)
        || digest != hash
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

fn validate_either(
    policies: &Policies,
    document: &str,
    digest: [u8; 32],
) -> Result<(), OwnerError> {
    validate(&policies.config, document, digest)
        .or_else(|_| validate(&policies.policy, document, digest))
}

fn grant(budget: &ResourceBudget, data: u64, native: u64) -> crate::runtime::OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(data, 1, native).expect("fixed request"))
        .unwrap_or_else(|_| panic!("seed resource reservation refused"))
}

fn native_limits() -> RocksNativeLimits {
    RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 64,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .expect("fixed native profile")
}

fn limits() -> ProducerRecoveryLimits {
    ProducerRecoveryLimits {
        input: RequestDecodeLimits {
            values: FactDecodeLimits {
                max_encoded_bytes: 16 * MIB as usize,
                max_allocation_bytes: 64 * MIB as usize,
                max_tag_nodes: 4 * MIB as usize,
                max_string_bytes: 16 * MIB as usize,
            },
            max_dictionaries: 256,
        },
        bootstrap: BootstrapLimits {
            max_mappings: 256,
            max_batch_bytes: 4 * MIB as usize,
        },
        event: AdmissionLimits {
            max_dependencies: 256,
            ..AdmissionLimits::default()
        },
    }
}

async fn runtime_role(client: &tokio_postgres::Client) -> i32 {
    let role = client
        .query_one(
            "SELECT current_user='bigbrotr_control' AND session_user='bigbrotr_control'
        AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)
        AND NOT EXISTS(SELECT 1 FROM pg_class WHERE relowner=current_user::regrole)
        AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datdba=current_user::regrole)
        AND NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspowner=current_user::regrole)
        AND NOT EXISTS(SELECT 1 FROM pg_proc WHERE proowner=current_user::regrole)
        AND current_setting('session_replication_role')='origin', pg_backend_pid()",
            &[],
        )
        .await
        .unwrap_or_else(|_| panic!("seed runtime role observation failed"));
    assert!(
        role.get::<_, bool>(0),
        "seed requires ordinary invoker authority"
    );
    role.get(1)
}

#[allow(
    clippy::too_many_arguments,
    reason = "private connected fixture retains distinct authority and resource owners"
)]
async fn resolve(
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    spec: &ingest_registry::InitialRegistration,
    id: ProducerRequestId,
    input: &Input,
    budget: &ResourceBudget,
    progress: &ProgressGrant,
    cancelled: &AtomicBool,
) -> Result<crate::storage::rocks::store::ResolvedProducerRequest, ProducerResolveError> {
    let limits = limits();
    owner
        .resolve_initial_producer_request(
            recovery,
            ProducerResolveRequest {
                ingest_root: Path::new("/var/lib/bigbrotr/ingest"),
                registration: spec,
                request: id,
                limits,
                policy_ttl: Duration::from_secs(5),
                cancelled,
            },
            grant(
                budget,
                RetainedRequestInput::required_data_bytes(limits.input)
                    .expect("input charge")
                    .max(historical_progress_data_bytes(0).expect("history charge")),
                0,
            ),
            grant(
                budget,
                limits.required_data_bytes().expect("outcome charge"),
                0,
            ),
            progress,
            |body, digest| validate_either(&input.policies, body, digest),
        )
        .await
}

async fn seed(input: &Input, budget: &ResourceBudget) -> Value {
    let phase = input.phase.as_deref();
    let guard = matches!(
        phase,
        Some("close" | "borrowed-backend" | "invalid-context" | "resolve")
    );
    assert!(
        (phase.is_none() && input.format == "bigbrotr-monitor-outcome-seed/1")
            || (guard && input.format == "bigbrotr-monitor-connected-guards/1"),
        "seed format"
    );
    let deployment =
        crate::local_file::read(Path::new("/etc/bigbrotr/instance.toml"), 10 * MIB as usize)
            .unwrap_or_else(|_| panic!("seed deployment unavailable"));
    let deployment_hash: [u8; 32] = Sha256::digest(&deployment).into();
    assert!(
        deployment_hash == bytes::<32>(&input.policies.deployment_sha256),
        "seed deployment changed"
    );
    let roots: toml::Value =
        toml::from_str(std::str::from_utf8(&deployment).expect("deployment UTF8"))
            .unwrap_or_else(|_| panic!("seed deployment refused"));
    assert!(
        roots["topology"]["dataset_id"].as_str() == Some(input.dataset_id.as_str())
            && roots["topology"]["instance_id"].as_str() == Some(input.instance_id.as_str())
            && roots["storage"]["ingest_root"].as_str() == Some("/var/lib/bigbrotr/ingest")
            && roots["postgres"]["control"]["endpoint"].as_str() == Some("127.0.0.1:15432")
            && roots["postgres"]["control"]["user"].as_str() == Some("bigbrotr_control")
            && roots["postgres"]["control"]["database"].as_str() == Some("bigbrotr_control"),
        "seed deployment identity differs"
    );
    assert!(
        input.policies.roots["runtime.lease_ttl"] == "60000000"
            && input.policies.roots["runtime.authorization_ttl"] == "5000000",
        "closed seed timing differs"
    );
    let secret_ref = roots["postgres"]["control"]["password_ref"]
        .as_str()
        .expect("protected reference");
    assert!(
        secret_ref == "file:/run/secrets/control",
        "seed credential root differs"
    );
    let password = SecretFile::parse(secret_ref)
        .expect("protected reference")
        .read(65536)
        .unwrap_or_else(|_| panic!("seed credential unavailable"));
    let path = Path::new("/var/lib/bigbrotr/ingest");
    let dataset = bytes::<16>(&input.dataset_id).try_into().expect("dataset");
    let slot = bytes::<16>(&input.producer_slot_id)
        .try_into()
        .expect("slot");
    let mut recovery =
        IngestRecovery::inspect(path, dataset, native_limits(), grant(budget, 0, 128 * MIB))
            .unwrap_or_else(|_| panic!("seed native custody refused"));
    let expected_manifest = encoded(&input.manifest_hex, 266);
    assert!(
        recovery.manifest().encode().as_bytes() == expected_manifest,
        "seed native manifest differs from frozen registration"
    );
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(15432)
        .dbname("bigbrotr_control")
        .user("bigbrotr_control")
        .password(password.as_bytes())
        .connect_timeout(Duration::from_secs(5));
    // Retain each SQL transport allocation through positive driver join.
    let _control_transport = grant(budget, 8 * MIB, 0);
    let (mut client, driver) = config
        .connect(NoTls)
        .await
        .unwrap_or_else(|_| panic!("seed runtime login refused"));
    let driver = tokio::spawn(driver);
    let spec = ingest_registry::load_historical(
        &mut client,
        bytes::<16>(&input.instance_id),
        &recovery,
        ingest_registry::MAX_HISTORICAL_DOCUMENT_BYTES,
        |body, digest| validate_either(&input.policies, body, digest),
    )
    .await
    .unwrap_or_else(|_| panic!("seed historical registration refused"));
    assert!(
        spec.producer_slot_id == slot
            && spec.config_revision == 1
            && spec.policy_revision == 1
            && spec.config_digest.as_bytes() == &bytes::<32>(&input.policies.config.sha256)
            && spec.policy_digest.as_bytes() == &bytes::<32>(&input.policies.policy.sha256),
        "seed historical policy identity differs"
    );
    let request = ClaimRequest {
        dataset,
        instance: bytes::<16>(&input.instance_id)
            .try_into()
            .expect("instance"),
        authority_mode: "managed".into(),
        role_instance_id: fresh_id().expect("fresh role"),
        incarnation: fresh_id()
            .expect("fresh incarnation")
            .try_into()
            .expect("incarnation"),
        lease_ttl: Duration::from_secs(60),
    };
    let (mut owner, policy) = ControlSession::acquire(
        client,
        request,
        |body, digest| validate_either(&input.policies, body, digest),
        &Cell::new(false),
    )
    .await
    .unwrap_or_else(|_| panic!("seed genuine Control claim refused"));
    assert!(
        policy.config_revision == 1
            && policy.policy_revision == 1
            && policy.config_digest == bytes::<32>(&input.policies.config.sha256)
            && policy.policy_digest == bytes::<32>(&input.policies.policy.sha256),
        "seed current policy identity differs"
    );
    let owner_backend = runtime_role(&owner.client).await;
    let cancelled = AtomicBool::new(false);
    if phase.is_none() || phase == Some("close") {
        let closed = owner
            .close_initial_producer(
                &mut recovery,
                ProducerCloseRequest {
                    ingest_root: path,
                    registration: &spec,
                    policy_ttl: Duration::from_secs(5),
                    cancelled: &cancelled,
                },
                grant(
                    budget,
                    historical_progress_data_bytes(4096).expect("closing charge"),
                    4096,
                ),
                |body, digest| validate_either(&input.policies, body, digest),
            )
            .await
            .unwrap_or_else(|_| panic!("seed native/PG closing refused"));
        assert!(
            closed.issued_through() == 4
                && closed.acknowledged_through() == 3
                && closed.reserved_through() == 4,
            "seed prefix differs"
        );
        drop(closed);
    }
    if guard {
        let id = ProducerRequestId {
            producer_slot_id: slot,
            producer_epoch: ProducerEpoch::new(1).expect("epoch"),
            request_sequence: RequestSequence::new(4).expect("request"),
        };
        let prefix = owner.client.query_one(
            "SELECT checkpoint_revision=3 AND complete_through_request=3
                AND dependency_count=9 AND progress_format=1 AND prefix_profile
                AND prefix_active_bytes IS NULL AND prefix_prepared_bytes IS NULL
                AND (SELECT count(*) FROM control.producer_outcomes)=3
                AND NOT EXISTS(SELECT 1 FROM control.producer_outcomes WHERE request_sequence=4)
                AND (SELECT monitor_attempt_id IS NOT NULL AND NOT monitor_resolution
                    FROM control.producer_requests WHERE request_sequence=4)
                AND (SELECT state='closing' AND reserved_through_request=4 FROM control.producer_epochs)
                FROM control.producer_checkpoints", &[]).await.expect("guard selected C3 read");
        assert!(
            prefix.get::<_, bool>(0),
            "guard requires selected typed C3 and absent outcome4"
        );
        if phase == Some("close") {
            drop(recovery);
            owner.release().await.expect("guard closing owner release");
            driver
                .await
                .expect("closing driver join")
                .expect("closing driver result");
            return guard_evidence(owner_backend, None);
        }
        let progress = ProgressGrant::new(grant(
            budget,
            ProgressGrant::REQUIRED_DATA_BYTES,
            ProgressGrant::REQUIRED_NATIVE_BYTES,
        ))
        .expect("guard progress charge");
        let mut borrowed_backend = None;
        if phase == Some("borrowed-backend") {
            let _borrowed_transport = grant(budget, 8 * MIB, 0);
            let (client, borrowed_driver) = config
                .connect(NoTls)
                .await
                .expect("guard borrowed runtime login");
            let borrowed_driver = tokio::spawn(borrowed_driver);
            let b = runtime_role(&client).await;
            assert_ne!(owner_backend, b, "guard requires two actual backends");
            borrowed_backend = Some(b);
            // Deliberately lend attribution alone. A retains its authentic claim,
            // lock and driver; B neither acquires nor forges an owner epoch.
            let mut borrowed = ControlSession {
                client,
                request: ClaimRequest {
                    dataset: owner.request.dataset,
                    instance: owner.request.instance,
                    authority_mode: owner.request.authority_mode.clone(),
                    role_instance_id: owner.request.role_instance_id,
                    incarnation: owner.request.incarnation,
                    lease_ttl: owner.request.lease_ttl,
                },
                identity: owner.identity,
                available: true,
                validated_policy: owner.validated_policy,
                enrolled_cursor_keys: None,
            };
            let result = resolve(
                &mut borrowed,
                &recovery,
                &spec,
                id,
                input,
                budget,
                &progress,
                &cancelled,
            )
            .await;
            assert!(
                matches!(
                    result,
                    Err(ProducerResolveError::Control(OwnerError::Backend))
                ),
                "borrowed backend must fail the actual resolver authority preflight"
            );
            assert!(
                !borrowed.available,
                "failed borrowed session must remain poisoned"
            );
            assert!(
                !borrowed.client.is_closed(),
                "borrowed refusal lost its SQL transport"
            );
            let same: i32 = borrowed
                .client
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .expect("borrowed transport remains live")
                .get(0);
            assert_eq!(same, b, "borrowed refusal changed backend");
            drop(borrowed);
            borrowed_driver
                .await
                .expect("borrowed driver join")
                .expect("borrowed driver result");
            let tx = owner
                .client
                .transaction()
                .await
                .expect("owner authority transaction");
            require(&tx, owner.request.dataset, owner.identity)
                .await
                .expect("genuine owner remains authorized");
            tx.rollback().await.expect("owner authority rollback");
        } else {
            let result = resolve(
                &mut owner, &recovery, &spec, id, input, budget, &progress, &cancelled,
            )
            .await;
            if phase == Some("invalid-context") {
                assert!(
                    matches!(
                        result,
                        Err(ProducerResolveError::Progress(
                            ProducerProgressError::InvalidContext
                        ))
                    ),
                    "selected invalid binding must fail prepare_measurement_recovery without copy-only fallback"
                );
                assert!(
                    !owner.available,
                    "failed resolver session must remain poisoned"
                );
                // No availability reset and no attempted release on this session.
                // The next genuine claim must exclude it after driver join.
                drop(recovery);
                drop(owner);
                driver
                    .await
                    .expect("invalid-context driver join")
                    .expect("invalid-context driver result");
                return guard_evidence(owner_backend, None);
            }
            let resolved = result.expect("guard actual resolver positive completion");
            let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
                panic!("guard requires original durable receipt")
            };
            assert!(
                receipt.encode_persistent(65536).expect("receipt bound")
                    == encoded(&input.expected_receipt_hex, 65536),
                "guard receipt differs from independent original"
            );
            drop(resolved);
        }
        assert!(
            !recovery.native_unavailable(),
            "guard native delivery unknown"
        );
        drop(recovery);
        owner.release().await.expect("guard genuine owner release");
        driver
            .await
            .expect("guard driver join")
            .expect("guard driver result");
        return guard_evidence(owner_backend, borrowed_backend);
    }
    let recorder = owner.identity;
    let (tx, mut lease, epoch) = owner
        .begin_producer_recovery::<crate::storage::rocks::store::ProducerRecoveryError>(
            &recovery,
            path,
            &spec,
            Duration::from_secs(5),
            ingest_registry::RegistryLocks::Producer,
            |body, digest| validate_either(&input.policies, body, digest),
        )
        .await
        .unwrap_or_else(|_| panic!("seed historical authority refused"));
    assert!(epoch.get() == 1, "seed current policy revision changed");
    let limits = limits();
    let input_permit = grant(
        budget,
        RetainedRequestInput::required_data_bytes(limits.input)
            .expect("input charge")
            .max(historical_progress_data_bytes(0).expect("history charge")),
        0,
    );
    producer_progress::recovery::require(
        &tx,
        &recovery,
        recovery.manifest(),
        slot,
        &input_permit,
        || producer_progress::recovery::authority(&mut lease, epoch, &cancelled),
    )
    .await
    .unwrap_or_else(|_| panic!("seed retained C3 authentication refused"));
    let id = ProducerRequestId {
        producer_slot_id: slot,
        producer_epoch: ProducerEpoch::new(1).expect("epoch"),
        request_sequence: RequestSequence::new(4).expect("request"),
    };
    let reservation = producer_requests::read(&tx, dataset, id)
        .await
        .unwrap_or_else(|_| panic!("seed reservation read failed"))
        .expect("original reservation required");
    let resolved = recovery
        .resolve_original_request(
            id,
            reservation.digest,
            limits,
            input_permit,
            grant(
                budget,
                limits.required_data_bytes().expect("outcome charge"),
                0,
            ),
            || producer_progress::recovery::authority(&mut lease, epoch, &cancelled),
        )
        .unwrap_or_else(|_| panic!("seed positive original replay refused"));
    let context = producer_progress::Context {
        dataset,
        slot,
        epoch: 1,
        shard: 0,
        manifest: Sha256::digest(recovery.manifest().encode().as_bytes()).into(),
    };
    assert!(
        producer_progress::supports_measurement_recovery(&tx, context, &resolved)
            .await
            .unwrap_or_else(|_| panic!("seed profile selection failed")),
        "seed requires selected historical Monitor profile"
    );
    let witness = recovery
        .measurement_checkpoint(&resolved)
        .unwrap_or_else(|_| panic!("seed historical witness refused"));
    let progress = ProgressGrant::new(grant(
        budget,
        ProgressGrant::REQUIRED_DATA_BYTES,
        ProgressGrant::REQUIRED_NATIVE_BYTES,
    ))
    .expect("progress charge");
    let _snapshot =
        producer_progress::prepare_measurement_recovery(&tx, context, &witness, &progress)
            .await
            .unwrap_or_else(|_| panic!("seed exact Monitor binding refused"));
    let prefix = tx
        .query_one(
            "SELECT checkpoint_revision=3 AND complete_through_request=3
        AND dependency_count=9 AND progress_format=1 AND prefix_profile
        AND (SELECT count(*) FROM control.producer_outcomes)=3
        AND NOT EXISTS(SELECT 1 FROM control.producer_outcomes WHERE request_sequence=4)
        AND (SELECT config_revision=1 AND policy_revision=1 FROM control.datasets)
        FROM control.producer_checkpoints",
            &[],
        )
        .await
        .unwrap_or_else(|_| panic!("seed C3 observation failed"));
    assert!(
        prefix.get::<_, bool>(0),
        "seed requires exact C3 and absent outcome4"
    );
    let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
        panic!("seed requires original durable receipt")
    };
    assert!(
        receipt.encode_persistent(65536).expect("receipt bound")
            == encoded(&input.expected_receipt_hex, 65536),
        "seed receipt differs from independent original reconstruction"
    );
    producer_outcomes::record_recovered(&tx, &reservation, &resolved, recorder)
        .await
        .unwrap_or_else(|_| panic!("seed original outcome copy refused"));
    witness
        .require()
        .unwrap_or_else(|_| panic!("seed witness became stale"));
    producer_progress::recovery::authority(&mut lease, epoch, &cancelled)
        .unwrap_or_else(|_| panic!("seed authority expired before COMMIT"));
    tx.commit()
        .await
        .unwrap_or_else(|_| panic!("seed outcome COMMIT unknown"));
    assert!(
        !recovery.native_unavailable(),
        "seed native delivery unknown"
    );
    witness
        .require()
        .unwrap_or_else(|_| panic!("seed witness delivery unknown"));
    producer_progress::recovery::authority(&mut lease, epoch, &cancelled)
        .unwrap_or_else(|_| panic!("seed authority delivery unknown"));
    drop(resolved);
    drop(recovery);
    assert!(!owner.client.is_closed(), "seed Control delivery unknown");
    owner.available = true;
    owner
        .release()
        .await
        .unwrap_or_else(|_| panic!("seed Control release unknown"));
    driver
        .await
        .unwrap_or_else(|_| panic!("seed driver panicked"))
        .unwrap_or_else(|_| panic!("seed driver failed"));
    Value::Null
}

fn guard_evidence(owner_backend: i32, borrowed_backend: Option<i32>) -> Value {
    serde_json::json!({"runtime_user":"bigbrotr_control", "session_user":"bigbrotr_control",
        "superuser":false, "owns_objects":false, "replication_role":"origin",
        "owner_backend":owner_backend, "borrowed_backend":borrowed_backend,
        "control_data_bytes":8*MIB, "borrowed_data_bytes":if borrowed_backend.is_some() {8*MIB} else {0}})
}

#[test]
#[ignore = "requires the explicit installed Monitor seed request, runtime PostgreSQL login and exclusive original native store"]
fn seed_exact_existing_outcome() {
    execute_test(false);
}

#[test]
#[ignore = "requires explicit isolated connected guards, actual Control login and exclusive original native store"]
fn resolve_connected_guards() {
    execute_test(true);
}

fn execute_test(guard: bool) {
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            4 * 1024 * MIB,
            512 * MIB,
            ResourceLimits::new(2 * 1024 * MIB, 16, 512 * MIB, 512 * MIB),
            WaiterLimits::new(0, 0),
        )
        .expect("fixture envelope"),
    );
    let _documents = grant(&budget, 128 * MIB, 0);
    let request = std::env::var_os("BIGBROTR_MONITOR_OUTCOME_SEED")
        .expect("explicit protected seed request required");
    let request_ref = format!(
        "file:{}",
        Path::new(&request).to_str().expect("request UTF8 path")
    );
    let raw = SecretFile::parse(&request_ref)
        .expect("protected request reference")
        .read(512 * 1024)
        .unwrap_or_else(|_| panic!("seed request refused"));
    let input: Input = serde_json::from_slice(raw.as_bytes())
        .unwrap_or_else(|_| panic!("seed request shape refused"));
    assert!(
        guard == input.phase.is_some(),
        "explicit fixture test and phase differ"
    );
    let evidence = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(20), seed(&input, &budget))
                .await
                .expect("seed fixture deadline")
        });
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
    let result_path = if guard {
        format!(
            "/var/lib/bigbrotr/monitor-connected-guards-{}-result.json",
            input.phase.as_deref().expect("guard phase")
        )
    } else {
        "/var/lib/bigbrotr/monitor-outcome-seed-result.json".to_owned()
    };
    let mut output = options.open(result_path).expect("fresh protected result");
    let result = if guard {
        serde_json::json!({"format":"bigbrotr-monitor-connected-guards-result/1", "status":"PASS", "phase":input.phase, "evidence":evidence})
    } else {
        serde_json::json!({"format":"bigbrotr-monitor-outcome-seed-result/1", "status":"PASS", "checkpoint":3, "outcome":4, "acknowledged":3})
    };
    serde_json::to_writer(&mut output, &result).expect("fixture result encode");
    output
        .write_all(b"\n")
        .and_then(|()| output.sync_all())
        .expect("fixture result write");
}

#[test]
fn closed_validator_requires_full_frozen_document_digest_and_revision() {
    let raw = include_str!("../../../../../tests/fixtures/configuration/policy-v3.json");
    let pair = Document {
        revision: 1,
        document: serde_json::from_str(raw).expect("literal policy"),
        sha256: "77324b5f6aea113a4e4b81e11d946c0c4e113b051851d398c240de49a7a6322b".into(),
    };
    let digest = bytes::<32>(&pair.sha256);
    assert!(validate(&pair, raw, digest).is_ok());
    let mut altered = pair.document.clone();
    altered["settings"]["monitor.enabled"] = Value::Bool(false);
    assert!(validate(&pair, &altered.to_string(), digest).is_err());
    assert!(validate(&pair, raw, [0; 32]).is_err());
    let wrong_revision = Document {
        revision: 2,
        ..pair
    };
    assert!(validate(&wrong_revision, raw, digest).is_err());
}
