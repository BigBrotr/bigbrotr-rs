//! Isolated real adapter/native/output fixture, not the installed scheduler.
mod memory;
mod resume;
mod rollback;
use bigbrotr::{
    export::{
        destination::LocalDestination,
        execution::{ExportLimits, ExportState},
    },
    management::{
        credentials::HASH_RESERVATION,
        jobs::ExportCommand,
        sessions::{SessionError, SessionLifetime},
    },
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::{
        archive::{
            ArtifactIdentity, CatalogReaderCredentials, CatalogRoute, CatalogTls, LocalCoordinator,
            Preparation, PreparationLimits, WorkerInstallation,
        },
        postgres::{
            catalog_guard::{self, CatalogCapture, CatalogIdentity},
            connection_settings::{ConnectionSettings, PlaintextBoundary, TlsSettings},
            control_owner::{
                ArchivePinRequest, ClaimRequest, ControlSession, OwnerError, ScopeGrantAllocation,
            },
            transport,
        },
    },
};
use bigbrotr_model::{CommandKey, DatasetId, JobId};
use bigbrotr_protocol::archive_visibility::VisibilityRead;
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};
use tokio_postgres::Client;
use zeroize::Zeroizing;

fn dataset() -> DatasetId {
    [1; 16].try_into().unwrap()
}
fn random() -> [u8; 16] {
    let mut value = [0; 16];
    getrandom::fill(&mut value).unwrap();
    value
}
fn case(name: &str) {
    println!("{}", serde_json::json!({"case":name,"status":"PASS"}));
}
fn validate(document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    let expected = include_str!("../archive-engine/receipt-policy.json");
    if serde_json::from_str::<serde_json::Value>(document).unwrap()
        != serde_json::from_str::<serde_json::Value>(expected).unwrap()
        || digest
            != <[u8; 32]>::from(
                Sha256::new()
                    .chain_update(b"bigbrotr/product-policy/1\0")
                    .chain_update(expected)
                    .finalize(),
            )
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}
async fn connection(database: &str, role: &str) -> (Client, transport::Driver) {
    let mut secret = tempfile::NamedTempFile::new_in("/out").unwrap();
    secret
        .write_all(&fs::read(format!("/secrets/{role}")).unwrap())
        .unwrap();
    let reference = format!("file:{}", secret.path().display());
    let prepared = ConnectionSettings {
        endpoint: "127.0.0.1:5432",
        database,
        user: role,
        password: bigbrotr::secrets::SecretFile::parse(&reference).unwrap(),
        connect_timeout: Duration::from_secs(5),
        tls: TlsSettings::LocalPlaintext {
            boundary: PlaintextBoundary::Loopback,
        },
    }
    .prepare()
    .unwrap();
    drop(secret);
    transport::connect(prepared).await.unwrap()
}
fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            1409 << 20,
            0,
            ResourceLimits::new(768 << 20, 8, 640 << 20, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn preparation<'a>(
    installation: &'a WorkerInstallation,
    temp: &'a Path,
    cancelled: &'a AtomicBool,
) -> Preparation<'a> {
    Preparation {
        installation,
        temp_root: temp,
        limits: PreparationLimits {
            memory_bytes: 128 << 20,
            temp_bytes: 64 << 20,
            threads: 1,
            max_records: 2,
            max_encoded_bytes: 1 << 20,
            native_reserve_bytes: 512 << 20,
        },
        deadline: Instant::now() + Duration::from_secs(4),
        cancelled,
    }
}
fn directory(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mode = std::env::args().nth(1).unwrap();
    if mode == "measure" {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 3 {
            eprintln!("usage: export-execution-probe measure /lab/case.json");
            std::process::exit(2);
        }
        if let Err(error) = memory::run(Path::new(&args[2])).await {
            eprintln!("export measurement refused: {error}");
            std::process::exit(4);
        }
        return;
    }
    assert!(matches!(
        mode.as_str(),
        "migrate" | "denied" | "run" | "resolve" | "rollback" | "resume" | "resume-denied"
    ));
    if mode == "migrate" {
        let (mut admin, driver) = connection("bbpub_control_normal", "postgres").await;
        let driver = tokio::spawn(driver);
        bigbrotr::storage::postgres::migration_runner::run(
            &mut admin,
            501,
            true,
            &Cell::new(false),
        )
        .await
        .unwrap();
        drop(admin);
        driver.await.unwrap().unwrap();
        case("current-rust-migrator-applies501-to-accepted813-job");
        return;
    }
    let (client, control_connection) = connection("bbpub_control_normal", "bigbrotr_control").await;
    let driver = tokio::spawn(control_connection);
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: dataset(),
            instance: [2; 16].try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: random(),
            incarnation: random().try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    if matches!(mode.as_str(), "resume" | "resume-denied") {
        resume::run(owner, driver, mode == "resume-denied").await;
        return;
    }
    if mode == "denied" {
        assert_eq!(
            owner.require_export_execution().await,
            Err(SessionError::Prerequisite)
        );
        drop(owner);
        driver.await.unwrap().unwrap();
        case("missing-execution-row-lock-privilege-refused");
        return;
    }
    if mode == "resolve" {
        // No coordinator, destination or native installation is opened in this
        // fresh process. Current data.read has been revoked by the outer fixture.
        let expected: serde_json::Value =
            serde_json::from_slice(&fs::read("/out/execution-result.json").unwrap()).unwrap();
        for expected in expected.as_array().unwrap() {
            let job: JobId = expected["job"].as_str().unwrap().parse().unwrap();
            let observed = owner.read_export_job(job).await.unwrap().unwrap();
            assert_eq!(observed.state(), ExportState::Succeeded);
            assert_eq!(
                observed.reservation().unwrap().artifact().to_string(),
                expected["artifact"]
            );
            assert_eq!(observed.result().unwrap().to_string(), expected["manifest"]);
            assert_eq!(
                observed.files().unwrap().data.rows,
                expected["rows"].as_u64().unwrap()
            );
        }
        assert!(owner.next_export_job(None).await.unwrap().is_none());
        owner.release().await.unwrap();
        driver.await.unwrap().unwrap();
        case("fresh-control-resolves-completion-without-input-grants-or-files");
        return;
    }
    owner.require_export_execution().await.unwrap();
    let first = owner.next_export_job(None).await.unwrap().unwrap();
    assert!(owner.next_export_job(Some(first)).await.unwrap().is_none());
    let original = owner.read_export_job(first).await.unwrap().unwrap();
    assert_eq!(original.state(), ExportState::Accepted);
    assert_eq!(original.selection().query().snapshot_id, 3);
    assert_eq!(original.selection().query().bounds.len(), 1);
    assert_eq!(original.selection().query().bounds[0].complete_through, 10);
    let destination = Arc::new(
        LocalDestination::open(
            Path::new("/out/signed_normal/export-destination"),
            dataset(),
            original.selection().destination().id,
        )
        .unwrap(),
    );
    let checked = destination.clone().check_for_acceptance().unwrap();
    let limits = ExportLimits::new(1 << 20, 2 << 20).unwrap();
    let started = Cell::new(false);
    let queued = owner
        .reserve_export(first, limits, &checked, &started)
        .await
        .unwrap();
    assert!(started.get());
    assert_eq!(queued.state(), ExportState::Queued);
    let retry = Cell::new(false);
    assert_eq!(
        owner
            .reserve_export(first, limits, &checked, &retry)
            .await
            .unwrap(),
        queued
    );
    assert!(!retry.get());
    assert_eq!(
        owner
            .reserve_export(
                first,
                ExportLimits::new(512 << 10, 1 << 20).unwrap(),
                &checked,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Capacity)
    );
    case("separate-reservation-commit-and-identical-retry-retain-original-caps");
    let identity = CatalogIdentity::local(
        dataset(),
        original.selection().catalog(),
        "/out/signed_normal/kind=7/",
    )
    .unwrap();
    let coordinator =
        LocalCoordinator::open(&identity, Path::new("/out/signed_normal/archive-state")).unwrap();
    owner
        .require_archive_coordinator(&coordinator)
        .await
        .unwrap();
    let (mut manager, connection) = connection("bbpub_normal", "postgres").await;
    let mut catalog_driver = tokio::spawn(coordinator.retain_connection(connection).unwrap());
    let catalog_pid: i32 = manager
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    catalog_guard::inspect_reader(&mut manager, &identity, "bigbrotr_receipt_reader")
        .await
        .unwrap();
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
    let password = Zeroizing::new(fs::read_to_string("/secrets/bigbrotr_receipt_reader").unwrap());
    let route = CatalogRoute {
        address: "127.0.0.1".parse().unwrap(),
        port: 5432,
        database: "bbpub_normal",
        tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
    };
    let credentials = || CatalogReaderCredentials {
        login: "bigbrotr_receipt_reader",
        password: &password,
    };
    let cancelled = AtomicBool::new(false);
    let budget = budget();
    let capture =
        CatalogCapture::at_snapshot(&mut manager, &coordinator, 3, Duration::from_secs(120))
            .await
            .unwrap();
    let pin = owner
        .register_archive_pin(
            &capture,
            &ArchivePinRequest {
                lease_id: random(),
                frontier_revision: 1,
                ttl: Duration::from_secs(30),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    let visibility_temp = Path::new("/out/execution-visibility");
    directory(visibility_temp);
    let query = original.selection().query();
    let visibility = preparation(&installation, visibility_temp, &cancelled)
        .read_visibility(
            VisibilityRead {
                dataset: query.dataset,
                scope: query.scope,
                topology_generation: query.topology_generation,
                bounds: query.bounds.clone(),
                snapshot_id: query.snapshot_id,
            },
            coordinator.reader_custody().unwrap(),
            &route,
            credentials(),
            &budget,
        )
        .unwrap();
    let challenge = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap();
    let mut hashing = budget
        .try_acquire(ResourceRequest::data(HASH_RESERVATION, 1, 0).unwrap())
        .unwrap();
    let verified = challenge
        .verify(b"Independent owner password 2026", &mut hashing)
        .unwrap();
    drop(hashing);
    let session = owner
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../identity-contract/signed-fixture.json")).unwrap();
    let command:ExportCommand=serde_json::from_value(serde_json::json!({"format":"signed-events-jsonl",
        "query":{"version":1,"resource":"event_history","scope":query.scope,"filters":[{"ids":[fixture["events"]["E_public"]["id"]]}]},
        "allocation":{"catalog_id":original.selection().catalog(),"frontier_revision":"1"},"destination":original.selection().destination(),
        "expected_config_revision":"1","expected_policy_revision":"1"})).unwrap();
    let positive = owner
        .accept_export(
            &session.session,
            &session.csrf,
            CommandKey::try_from(random()).unwrap(),
            &command,
            Some(&ScopeGrantAllocation {
                capture: &capture,
                visibility: &visibility,
                pin: &pin,
            }),
            Some(&checked),
            &Cell::new(false),
        )
        .await
        .unwrap();
    owner
        .revoke_browser_session(&session.session, &session.csrf)
        .await
        .unwrap();
    drop(visibility);
    let positive = owner
        .reserve_export(positive.job_id, limits, &checked, &Cell::new(false))
        .await
        .unwrap();
    let mut results = Vec::new();
    let mut custodies = Vec::new();
    for (index, job) in [queued, positive].into_iter().enumerate() {
        let started_at = Instant::now();
        let running = owner
            .start_export(&job, &capture, pin.lease_id, &Cell::new(false))
            .await
            .unwrap();
        assert_eq!(running.job().state(), ExportState::Running);
        let remaining = running.remaining();
        // Fixture execution is finite and checked against the returned bound;
        // installed signal/renewal supervision is a separate runtime gate.
        assert!(matches!(
            owner
                .start_export(&job, &capture, pin.lease_id, &Cell::new(false))
                .await,
            Err(SessionError::Conflict)
        ));
        let prepared = running.prepare(destination.clone()).unwrap();
        custodies.push(prepared.custody());
        let temp = std::path::PathBuf::from(format!("/out/execution-query-{index}"));
        directory(&temp);
        let observed = preparation(&installation, &temp, &cancelled)
            .query_event(
                job.selection().query().clone(),
                coordinator.reader_custody().unwrap(),
                &route,
                credentials(),
                &budget,
            )
            .unwrap();
        assert_eq!(observed.event().is_some(), index == 1);
        let written = prepared.write(observed, &cancelled).unwrap();
        assert!(started_at.elapsed() < remaining);
        assert_eq!(written.files().data.rows, index as u64);
        let old_export_pin = owner
            .read_export_job(job.selection().job())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old_export_pin.state(), ExportState::Running);
        if mode == "rollback" {
            rollback::exercise(
                &mut owner,
                &written,
                &capture,
                pin.lease_id,
                catalog_pid,
                &mut catalog_driver,
            )
            .await;
            drop(written);
            owner
                .release_archive_pin(&pin, &Cell::new(false))
                .await
                .unwrap();
            assert!(capture.finish().await.is_err());
            drop(manager);
            // exercise collected the actual catalog driver before unlocking.
            owner.release().await.unwrap();
            driver.await.unwrap().unwrap();
            drop(custodies);
            drop(destination);
            case("catalog-loss-rollback-reader-release-and-control-driver-join");
            return;
        }
        let complete = owner
            .complete_export(&written, &capture, pin.lease_id, &Cell::new(false))
            .await
            .unwrap();
        assert_eq!(complete.state(), ExportState::Succeeded);
        assert_eq!(complete.files(), Some(written.files()));
        assert!(matches!(
            owner
                .start_export(&complete, &capture, pin.lease_id, &Cell::new(false))
                .await,
            Err(SessionError::Conflict)
        ));
        results.push(serde_json::json!({"job":complete.selection().job(),"artifact":complete.reservation().unwrap().artifact(),"manifest":complete.result().unwrap(),"rows":complete.files().unwrap().data.rows}));
        drop(written);
    }
    fs::write(
        "/out/execution-result.json",
        serde_json::to_vec_pretty(&results).unwrap(),
    )
    .unwrap();
    case("native-empty-and-signed-event-output-complete-after-browser-logout");
    assert!(owner.next_export_job(None).await.unwrap().is_none());
    owner
        .release_archive_pin(&pin, &Cell::new(false))
        .await
        .unwrap();
    capture.finish().await.unwrap();
    drop(manager);
    catalog_driver.await.unwrap().unwrap();
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    drop(custodies);
    drop(destination);
    case("known-completion-reader-release-and-actual-driver-joins");
}
