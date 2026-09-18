//! Isolated real parent receipt observation; input is a trusted lab fixture.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits},
    storage::{
        archive::{
            ArtifactIdentity, CatalogReaderCredentials, CatalogRoute, CatalogTls, Preparation,
            PreparationLimits, WorkerInstallation,
        },
        postgres::{catalog_guard::CatalogIdentity, connection_settings::PlaintextBoundary},
    },
};
use bigbrotr_model::OwnerResourceId;
use bigbrotr_protocol::archive_receipt::ReceiptExpectation;
use serde::Deserialize;
use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    expected: ReceiptExpectation,
    catalog_id: OwnerResourceId,
    data_root: String,
    database: String,
    port: u16,
    role: String,
    max_receipts: usize,
    reserve_bytes: u64,
    deadline_ms: u64,
    cancel_when_blocked: bool,
}
fn main() {
    let input: Input = serde_json::from_slice(&fs::read("/fixture/input.json").unwrap()).unwrap();
    assert!(matches!(
        input.role.as_str(),
        "bigbrotr_receipt_reader" | "bigbrotr_receipt_denied"
    ));
    let ready: serde_json::Value =
        serde_json::from_slice(&fs::read("/out/native-tests/ready.json").unwrap()).unwrap();
    let installation = WorkerInstallation {
        root: "/out/archive-worker".into(),
        executable: ArtifactIdentity {
            bytes: fs::metadata("/out/archive-worker/bigbrotr-archive-worker")
                .unwrap()
                .len(),
            sha256: ready["worker"].as_str().unwrap().parse().unwrap(),
        },
    };
    let identity = CatalogIdentity::local(
        input.expected.dataset_id,
        input.catalog_id,
        &input.data_root,
    )
    .unwrap();
    let password = Zeroizing::new(fs::read_to_string(format!("/secrets/{}", input.role)).unwrap());
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
    let done = AtomicBool::new(false);
    let start = Instant::now();
    let result = std::thread::scope(|scope| {
        if input.cancel_when_blocked {
            scope.spawn(|| {
                while !done.load(Ordering::Relaxed) {
                    if Path::new("/fixture/cancel").exists() {
                        cancel.store(true, Ordering::Relaxed);
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
        }
        let result = Preparation {
            installation: &installation,
            temp_root: Path::new("/job"),
            limits: PreparationLimits {
                memory_bytes: 128 << 20,
                temp_bytes: 64 << 20,
                threads: 1,
                max_records: input.max_receipts,
                max_encoded_bytes: 1 << 20,
                native_reserve_bytes: input.reserve_bytes,
            },
            deadline: start + Duration::from_millis(input.deadline_ms),
            cancelled: &cancel,
        }
        .inspect_receipt(
            &input.expected,
            &identity,
            &CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port: input.port,
                database: &input.database,
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            },
            CatalogReaderCredentials {
                login: &input.role,
                password: &password,
            },
            &budget,
        );
        done.store(true, Ordering::Relaxed);
        result
    });
    println!(
        "{}",
        serde_json::json!({"outcome":format!("{result:?}"),"elapsed_ms":start.elapsed().as_millis(),
        "active_permits":budget.snapshot().active_permits(),"cancelled":cancel.load(Ordering::Relaxed)})
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
}
