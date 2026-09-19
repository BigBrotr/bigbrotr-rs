use super::*;
use crate::{
    runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits},
    storage::{
        archive::{ArtifactIdentity, CatalogTls, PreparationLimits, WorkerInstallation},
        postgres::connection_settings::PlaintextBoundary,
    },
};
use bigbrotr_model::{DatasetId, Digest256};
use bigbrotr_protocol::archive_receipt::MAX_RANGES;
use std::{
    path::Path,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

#[test]
fn preflight_sizes_the_complete_native_receipt_request_without_io() {
    use bigbrotr_model::TopologyGeneration;
    use bigbrotr_protocol::archive_receipt::ReceiptRange;
    let mut expected = ReceiptExpectation {
        dataset_id: [1; 16].try_into().unwrap(),
        schema_version: 1,
        topology_generation: TopologyGeneration::new(0),
        batch_digest: Digest256::from_bytes([5; 32]),
        record_count: 420,
        input_ranges: (0..420)
            .map(|shard_id| ReceiptRange {
                shard_id,
                owner_epoch: 1,
                first_sequence: 1,
                last_sequence: 1,
            })
            .collect(),
        attempt_id: [9; 16],
        publication_ordinal: 1,
    };
    let route = CatalogRoute {
        address: "127.0.0.1".parse().unwrap(),
        port: 443,
        database: "archive",
        tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
    };
    let short = CatalogIdentity::local(
        expected.dataset_id,
        [90; 16].try_into().unwrap(),
        "/archive/",
    )
    .unwrap();
    let credential = || CatalogReaderCredentials {
        login: "reader",
        password: "private-fixture",
    };
    assert_eq!(
        route.validate_receipt(&expected, &short, credential()),
        Ok(())
    );
    let native_only =
        CatalogIdentity::native_only_local(expected.dataset_id, short.catalog(), short.root())
            .unwrap();
    assert_eq!(
        route.validate_receipt(&expected, &native_only, credential()),
        Err(ReceiptError::InvalidInput)
    );
    assert!(serde_json::to_vec(&expected).unwrap().len() < MAX_INPUT_BYTES);
    let long = CatalogIdentity::local(
        expected.dataset_id,
        short.catalog(),
        &format!("/{}/", "a".repeat(4000)),
    )
    .unwrap();
    assert_eq!(
        route.validate_receipt(&expected, &long, credential()),
        Err(ReceiptError::ResourceLimit)
    );
    expected
        .input_ranges
        .extend((420..1024).map(|shard_id| ReceiptRange {
            shard_id,
            owner_epoch: 1,
            first_sequence: 1,
            last_sequence: 1,
        }));
    expected.record_count = 1024;
    expected.validate().unwrap();
    assert_eq!(
        route.validate_receipt(&expected, &short, credential()),
        Err(ReceiptError::ResourceLimit)
    );
}

#[test]
fn receipt_reply_rejects_duplicate_conflicting_fields_and_trailing_values() {
    let expected: ReceiptExpectation = serde_json::from_value(serde_json::json!({
        "dataset_id":"01".repeat(16),"schema_version":1,"topology_generation":"0",
        "batch_digest":"05".repeat(32),"record_count":1,
        "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":1}],
        "attempt_id":([9;16]),"publication_ordinal":1
    }))
    .unwrap();
    let reply = serde_json::json!({"format":"bigbrotr-native-receipt/1","status":"present",
        "batch_digest":expected.batch_digest,"attempt_id":expected.attempt_id,"publication_ordinal":1}).to_string();
    assert_eq!(
        decode_response(reply.as_bytes(), &expected),
        Ok(ReceiptObservation::Present)
    );
    for extra in [
        "\"status\":\"absent\"",
        "\"status\":\"present\"",
        "\"publication_ordinal\":1",
        "\"unknown\":true",
    ] {
        let invalid = format!("{},{}{}", &reply[..reply.len() - 1], extra, "}");
        assert_eq!(
            decode_response(invalid.as_bytes(), &expected),
            Err(ReceiptError::ObservationUnknown),
            "{extra}"
        );
    }
    assert_eq!(
        decode_response(format!("{reply}{{}}").as_bytes(), &expected),
        Err(ReceiptError::ObservationUnknown)
    );
}

#[test]
fn invalid_or_unfunded_expectations_refuse_before_any_launch_or_grant() {
    let original: ReceiptExpectation = serde_json::from_value(serde_json::json!({
        "dataset_id":"01".repeat(16),"schema_version":1,"topology_generation":"0",
        "batch_digest":"05".repeat(32),"record_count":3,
        "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
        "attempt_id":([9;16]),"publication_ordinal":1
    }))
    .unwrap();
    let installation = WorkerInstallation {
        root: "/nonexistent-archive-installation".into(),
        executable: ArtifactIdentity {
            bytes: 1,
            sha256: Digest256::from_bytes([1; 32]),
        },
    };
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            513 << 20,
            0,
            ResourceLimits::new(0, 1, 512 << 20, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let cancel = AtomicBool::new(false);
    for case in [
        "range-limit",
        "identity",
        "catalog",
        "allocation",
        "scan-zero",
        "scan-overflow",
        "credential",
    ] {
        let mut wanted = original.clone();
        let mut dataset = wanted.dataset_id;
        let mut maximum = 16;
        let mut reserve = 384 << 20;
        let mut login = "readonly";
        match case {
            "range-limit" => wanted.input_ranges = vec![wanted.input_ranges[0]; MAX_RANGES + 1],
            "identity" => wanted.publication_ordinal = 0,
            "catalog" => dataset = DatasetId::try_from([2; 16]).unwrap(),
            "allocation" => reserve = 1,
            "scan-zero" => maximum = 0,
            "scan-overflow" => maximum = usize::MAX,
            "credential" => login = "",
            _ => unreachable!(),
        }
        let identity =
            CatalogIdentity::local(dataset, [90; 16].try_into().unwrap(), "/archive/").unwrap();
        let result = Preparation {
            installation: &installation,
            temp_root: Path::new("/nonexistent-receipt-job"),
            limits: PreparationLimits {
                memory_bytes: 128 << 20,
                temp_bytes: 64 << 20,
                threads: 1,
                max_records: maximum,
                max_encoded_bytes: 1 << 20,
                native_reserve_bytes: reserve,
            },
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: &cancel,
        }
        .inspect_receipt(
            &wanted,
            &identity,
            &CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port: 5432,
                database: "archive",
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            },
            CatalogReaderCredentials {
                login,
                password: "private-fixture",
            },
            &budget,
        );
        assert_eq!(
            result,
            Err(if matches!(case, "range-limit" | "allocation") {
                ReceiptError::ResourceLimit
            } else {
                ReceiptError::InvalidInput
            }),
            "{case}"
        );
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
