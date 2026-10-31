//! Finite actual-engine consumer: no fixture-supplied historical registration.
use super::*;
use bigbrotr::storage::{postgres::ingest_registry, rocks::store::IngestRecovery};

#[path = "ingest_history_integrity.rs"]
mod integrity;
pub use integrity::{private as private_integrity, seed as seed_integrity};

pub async fn run(
    config: Config,
    mode: &str,
    path: &Path,
    dataset: bigbrotr_model::DatasetId,
    configured_instance: [u8; 16],
) {
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            145 * MIB,
            0,
            ResourceLimits::new(16 * MIB, 8, 128 * MIB, MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let _metadata = grant(&budget, 64 * 1024, 0);
    let recovery =
        IngestRecovery::inspect(path, dataset, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    if mode == "history-hold" {
        emit(serde_json::json!({"event":"native-held"}));
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line, "release\n");
        drop(recovery);
        return;
    }
    // This fixed-fixture reader reserves the remaining existing16MiB payload
    // envelope for transport/validation before connecting. Its16KiB per-body
    // SQL guard bounds LIMIT2's four possible bodies to64KiB total. Attribution
    // scratch and the native permit remain separate; no capacity is increased.
    assert!(
        fixture_document().len() <= 16 * 1024,
        "fixed policy must fit the declared reader cap"
    );
    let _documents = grant(&budget, 16 * MIB - 64 * 1024, 0);
    let root = crate::config::fixture_root(dataset, configured_instance);
    let validate =
        |document: &str, digest| crate::config::validate_fixture_policy(document, digest, &root);
    let (mut client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let document_cap =
        match mode {
            "history-cap-zero" => 0,
            "history-cap-too-large" => ingest_registry::MAX_HISTORICAL_DOCUMENT_BYTES + 1,
            "history-cap-exact" | "history-cap-one-byte-short" => {
                // Boundary input only, not an outcome oracle: measure the complete
                // original documents, then ask the actual reader for that exact
                // ceiling or one byte less. Its answer must still be independent.
                let identity = recovery.manifest().identity();
                let row = client.query_one(
                "SELECT greatest(octet_length(c.document::text),octet_length(p.document::text))
                   FROM control.policy_revisions p JOIN control.config_revisions c
                     ON c.dataset_id=p.dataset_id AND c.revision=p.config_revision
                  WHERE p.dataset_id=$1::bytea AND p.revision=$2::text::numeric",
                &[&&identity.dataset_id.as_bytes()[..], &identity.policy_epoch.get().to_string()],
            ).await.unwrap();
                let bytes = usize::try_from(row.get::<_, i32>(0)).unwrap();
                assert!((2..=16 * 1024).contains(&bytes));
                bytes - usize::from(mode == "history-cap-one-byte-short")
            }
            _ => 16 * 1024,
        };
    let instance = if mode == "history-wrong-instance" {
        [99; 16]
    } else {
        configured_instance
    };
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        ingest_registry::load_historical(&mut client, instance, &recovery, document_cap, validate),
    )
    .await
    .expect("bounded historical lookup");
    let message = match result {
        Err(error) => serde_json::json!({"event":"history-refused", "error":format!("{error:?}")}),
        Ok(s) => serde_json::json!({
            "event":"history-loaded",
            "dataset_id":hex(s.dataset_id.as_bytes()), "instance_id":hex(&s.instance_id),
            "control_resource":hex(s.control.resource_id.as_bytes()),
            "control_epoch":s.control.epoch.get(), "control_role_instance":hex(&s.control.role_instance_id),
            "control_incarnation":hex(s.control.process_incarnation.as_bytes()),
            "ingest_resource":hex(s.ingest_resource_id.as_bytes()),
            "ingest_role_instance":hex(&s.ingest_role_instance_id),
            "ingest_incarnation":hex(s.process_incarnation.as_bytes()), "shard_id":s.shard_id,
            "config_revision":s.config_revision, "policy_revision":s.policy_revision,
            "config_digest":hex(s.config_digest.as_bytes()), "policy_digest":hex(s.policy_digest.as_bytes()),
            "previous_topology":s.previous_topology.map(|v|v.get()),
            "allocation_domains":s.allocation_domains.iter().map(|(kind,id)|
                serde_json::json!([*kind as u16,hex(id.as_bytes())])).collect::<Vec<_>>(),
            "producer_slot":hex(s.producer_slot_id.as_bytes()), "window":s.outstanding_request_limit,
            "intake_policy": {
                "pending_limit":s.intake_policy.pending_limit(),
                "pending_high":s.intake_policy.pending_high(),
                "pending_low":s.intake_policy.pending_low(),
                "max_pending_unit":s.intake_policy.max_pending_unit(),
                "disk_resume_margin":s.intake_policy.disk_resume_margin(),
                "stable_duration_us":u64::try_from(s.intake_policy.stable_duration().as_micros()).unwrap(),
                "request_receipt_retention":s.intake_policy.request_receipt_retention(),
            },
            "created_at_us":s.created_at_us, "lease_expires_at_us":s.lease_expires_at_us,
            "manifest":hex(recovery.manifest().encode().as_bytes()),
        }),
    };
    drop(client);
    crate::ingest_closing::join(driver, false).await;
    drop(recovery);
    emit(message);
}

// The configured instance is a plain nonzero16-byte value, not a model ID type.
pub fn configured_instance(text: &str) -> [u8; 16] {
    assert!(text.len() == 32 && text.bytes().all(|b| b.is_ascii_hexdigit()));
    let bytes = std::array::from_fn(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).unwrap());
    assert_ne!(bytes, [0; 16]);
    bytes
}
