//! Isolated actual private parent/worker hydration. Trusted fixture authority only;
//! this is not an installed operator command, public read API or source-cut grant.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits},
    storage::{
        archive::{
            ArchiveEventError, ArtifactIdentity, CatalogReaderCredentials, CatalogRoute,
            CatalogTls, LifetimeError, LocalCoordinator, Preparation, PreparationError,
            PreparationLimits, WorkerInstallation,
        },
        postgres::{catalog_guard::CatalogIdentity, connection_settings::PlaintextBoundary},
    },
};
use bigbrotr_model::OwnerResourceId;
use bigbrotr_protocol::archive_event::EventRead;
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    expected: EventRead,
    catalog_id: OwnerResourceId,
    data_root: String,
    database: String,
    max_row_bytes: usize,
    deadline_ms: u64,
    cancel_file: Option<PathBuf>,
}
fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    assert_eq!(args.len(), 3);
    let input: Input = serde_json::from_slice(&fs::read(&args[1]).unwrap()).unwrap();
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
    let identity =
        CatalogIdentity::local(input.expected.dataset, input.catalog_id, &input.data_root).unwrap();
    let password = Zeroizing::new(fs::read_to_string("/secrets/bigbrotr_receipt_reader").unwrap());
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            1025 << 20,
            0,
            ResourceLimits::new(512 << 20, 4, 512 << 20, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let cancelled = AtomicBool::new(false);
    let done = AtomicBool::new(false);
    let started = Instant::now();
    let state = Path::new(&input.data_root)
        .parent()
        .unwrap()
        .join("archive-state");
    let coordinator = LocalCoordinator::open(&identity, &state);
    let result = std::thread::scope(|scope| {
        if let Some(path) = &input.cancel_file {
            scope.spawn(|| {
                while !done.load(Ordering::Relaxed) {
                    if path.exists() {
                        cancelled.store(true, Ordering::Relaxed);
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
        }
        let result = coordinator
            .as_ref()
            .map_err(|e| ArchiveEventError::Preparation(PreparationError::Custody(*e)))
            .and_then(|coordinator| {
                Preparation {
                    installation: &installation,
                    temp_root: Path::new(&args[2]),
                    limits: PreparationLimits {
                        memory_bytes: 128 << 20,
                        temp_bytes: 64 << 20,
                        threads: 1,
                        max_records: 2,
                        max_encoded_bytes: input.max_row_bytes,
                        native_reserve_bytes: 384 << 20,
                    },
                    deadline: started + Duration::from_millis(input.deadline_ms),
                    cancelled: &cancelled,
                }
                .read_event(
                    &input.expected,
                    coordinator,
                    &CatalogRoute {
                        address: "127.0.0.1".parse().unwrap(),
                        port: 5432,
                        database: &input.database,
                        tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
                    },
                    CatalogReaderCredentials {
                        login: "bigbrotr_receipt_reader",
                        password: &password,
                    },
                    &budget,
                )
            });
        done.store(true, Ordering::Relaxed);
        result
    });
    drop(coordinator);
    let observed = result.is_ok();
    if observed {
        assert!(
            matches!(
                LocalCoordinator::open(&identity, &state),
                Err(LifetimeError::Occupied)
            ),
            "decoded body must retain custody after the coordinator drops"
        );
    }
    let output = match result {
        Ok(observation) => {
            assert_eq!(
                budget.snapshot().active_permits(),
                1,
                "decoded body must retain its grant"
            );
            serde_json::json!({"status":"observed","snapshot_id":observation.snapshot_id().to_string(),"event":observation.event().event()})
        }
        Err(error) => serde_json::json!({"status":"refused","error":format!("{error:?}")}),
    };
    assert_eq!(budget.snapshot().active_permits(), 0);
    if observed {
        drop(LocalCoordinator::open(&identity, &state).unwrap());
    }
    println!(
        "{}",
        serde_json::json!({"result":output,"elapsed_ms":started.elapsed().as_millis(),"cancelled":cancelled.load(Ordering::Relaxed),"active_permits":budget.snapshot().active_permits()})
    );
}
