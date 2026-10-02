//! Finite actual-engine consumer: no fixture-supplied historical registration.
use super::*;
use bigbrotr::storage::{postgres::ingest_registry, rocks::store::IngestRecovery};

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
    let (mut client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let instance = if mode == "history-wrong-instance" {
        [99; 16]
    } else {
        configured_instance
    };
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        ingest_registry::load_historical(&mut client, instance, &recovery),
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
            "created_at_us":s.created_at_us, "lease_expires_at_us":s.lease_expires_at_us,
            "manifest":hex(recovery.manifest().encode().as_bytes()),
        }),
    };
    drop(client);
    crate::ingest_closing::join(driver, false).await;
    drop(recovery);
    emit(message);
}
