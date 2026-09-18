//! Isolated real PG acceptance. Native initialization and writer processes are
//! external to this probe; retained Control input is explicitly synthetic.
use bigbrotr::storage::postgres::{
    archive_attempt::ArchiveAttemptRequest,
    catalog_guard::{self, Activation, CatalogIdentity, GuardError},
    control_owner::{ClaimRequest, ControlSession},
};
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, Sequence, SourceRecordId, TopologyGeneration,
};
use std::{cell::Cell, time::Duration};
use tokio_postgres::{Client, Config, NoTls};

async fn connect(database: &str, role: &str) -> (Client, tokio::task::JoinHandle<()>) {
    let mut c = Config::new();
    c.host("postgres")
        .dbname(database)
        .user(role)
        .password(std::fs::read(format!("/secrets/{role}")).unwrap())
        .connect_timeout(Duration::from_secs(5));
    let (client, driver) = c.connect(NoTls).await.unwrap();
    (
        client,
        tokio::spawn(async move {
            let _ = driver.await;
        }),
    )
}
fn dataset() -> DatasetId {
    DatasetId::try_from([1; 16]).unwrap()
}
fn catalog() -> OwnerResourceId {
    OwnerResourceId::try_from([90; 16]).unwrap()
}
fn request(n: u8, predecessor: u64) -> ArchiveAttemptRequest {
    let input = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([7; 32]),
    }])
    .unwrap()];
    ArchiveAttemptRequest::new(
        [n; 16],
        catalog(),
        ProcessIncarnation::try_from([80; 16]).unwrap(),
        ProcessIncarnation::try_from([n + 1; 16]).unwrap(),
        predecessor,
        &ArchiveBatch::new(1, TopologyGeneration::new(0), &input).unwrap(),
    )
    .unwrap()
}
async fn run() {
    let mode = std::env::args().nth(1).unwrap();
    let (mut admin, driver) = connect("catalog", "postgres").await;
    let identity = CatalogIdentity::local(dataset(), catalog(), "/out/archive/").unwrap();
    let commit = Cell::new(false);
    match mode.as_str() {
        "install" => {
            assert!(
                catalog_guard::install(&mut admin, &identity, "bootstrap", &commit)
                    .await
                    .unwrap()
            );
            assert!(commit.get());
            commit.set(false);
            assert!(
                !catalog_guard::install(&mut admin, &identity, "bootstrap", &commit)
                    .await
                    .unwrap()
            );
            assert!(!commit.get());
            catalog_guard::inspect(&mut admin, &identity).await.unwrap();
            for wrong in [
                CatalogIdentity::local(
                    DatasetId::try_from([3; 16]).unwrap(),
                    catalog(),
                    "/out/archive/",
                )
                .unwrap(),
                CatalogIdentity::local(
                    dataset(),
                    OwnerResourceId::try_from([4; 16]).unwrap(),
                    "/out/archive/",
                )
                .unwrap(),
                CatalogIdentity::local(dataset(), catalog(), "/out/other/").unwrap(),
            ] {
                assert!(catalog_guard::inspect(&mut admin, &wrong).await.is_err());
            }
            let (old, d) = connect("catalog", "bootstrap").await;
            for sql in [
                "CREATE TABLE public.forbidden(v int)",
                "DELETE FROM public.ducklake_metadata WHERE false",
                "ALTER TABLE public.ducklake_metadata DISABLE TRIGGER ALL",
            ] {
                assert_eq!(
                    old.batch_execute(sql)
                        .await
                        .unwrap_err()
                        .code()
                        .unwrap()
                        .code(),
                    "42501"
                );
            }
            drop(old);
            d.await.unwrap();
            // Structural corruption is detected without silently repairing it.
            for (change, restore) in [
                (
                    "ALTER TABLE public.ducklake_metadata DISABLE TRIGGER bigbrotr_writer_guard",
                    "ALTER TABLE public.ducklake_metadata ENABLE TRIGGER bigbrotr_writer_guard",
                ),
                (
                    "CREATE TABLE public.unexpected(value bigint)",
                    "DROP TABLE public.unexpected",
                ),
            ] {
                admin.batch_execute(change).await.unwrap();
                assert!(catalog_guard::inspect(&mut admin, &identity).await.is_err());
                admin.batch_execute(restore).await.unwrap();
                catalog_guard::inspect(&mut admin, &identity).await.unwrap();
            }
        }
        "activate-first" | "activate-second" | "replay-first" => {
            let second = mode == "activate-second";
            let (client, control_driver) = connect("attempts", "bigbrotr_control").await;
            let (mut control, _) = ControlSession::acquire(
                client,
                ClaimRequest {
                    dataset: dataset(),
                    instance: DatasetId::try_from([2; 16]).unwrap(),
                    authority_mode: "managed".into(),
                    role_instance_id: [if second {
                        102
                    } else if mode == "replay-first" {
                        104
                    } else {
                        100
                    }; 16],
                    incarnation: ProcessIncarnation::try_from(
                        [if second {
                            103
                        } else if mode == "replay-first" {
                            105
                        } else {
                            101
                        }; 16],
                    )
                    .unwrap(),
                    lease_ttl: Duration::from_secs(60),
                },
                |_, _| Ok(()),
                &Cell::new(false),
            )
            .await
            .unwrap();
            let mut receipt = control
                .register_archive_attempt(
                    &request(if second { 22 } else { 20 }, u64::from(second)),
                    &Cell::new(false),
                )
                .await
                .unwrap();
            // Public observational fields cannot alter the privately retained
            // tuple used for authority. The adapter must ignore these changes.
            receipt.ordinal = 999;
            receipt.latest_issued = true;
            let expected = if mode == "replay-first" {
                Activation::Observed {
                    current: false,
                    enabled: false,
                }
            } else {
                Activation::Activated
            };
            assert_eq!(
                catalog_guard::activate(&mut admin, &identity, &receipt, &commit)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(commit.get(), mode != "replay-first");
            commit.set(false);
            assert_eq!(
                catalog_guard::activate(&mut admin, &identity, &receipt, &commit)
                    .await
                    .unwrap(),
                Activation::Observed {
                    current: mode != "replay-first",
                    enabled: mode != "replay-first"
                }
            );
            assert!(!commit.get());
            catalog_guard::inspect(&mut admin, &identity).await.unwrap();
            control.release().await.unwrap();
            control_driver.await.unwrap();
        }
        "inspect" => {
            catalog_guard::inspect(&mut admin, &identity).await.unwrap();
        }
        "refuse-damaged" => {
            admin.batch_execute("ALTER TABLE public.ducklake_metadata ADD COLUMN unexpected bigint; ALTER TABLE public.ducklake_metadata DROP COLUMN unexpected").await.unwrap();
            assert_eq!(
                catalog_guard::inspect(&mut admin, &identity)
                    .await
                    .unwrap_err(),
                GuardError::IncompatibleLayout
            );
        }
        _ => panic!("unknown mode"),
    }
    drop(admin);
    driver.await.unwrap();
    println!("{}", serde_json::json!({"status":"PASS","case":mode}));
}
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(90), run())
                .await
                .unwrap()
        });
}
