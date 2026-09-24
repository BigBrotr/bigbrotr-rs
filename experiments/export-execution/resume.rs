//! Real Control adapter on the independently prepared paused7 SQL fixture.
use super::*;
use bigbrotr::management::{jobs::ResumeExportCommand, sessions::BrowserSecret};

fn request(revision: u64) -> ResumeExportCommand {
    serde_json::from_value(
        serde_json::json!({"expected_job_revision":revision.to_string(),
        "expected_config_revision":"1","expected_policy_revision":"1"}),
    )
    .unwrap()
}

pub async fn run(
    mut owner: ControlSession,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    denied: bool,
) {
    if denied {
        assert!(!owner.export_jobs_available().await.unwrap());
        owner.release().await.unwrap();
        driver.await.unwrap().unwrap();
        case("resume-ledger-select-required-before-existing-job-writes");
        return;
    }
    owner.require_export_resume().await.unwrap();
    let (admin, connection) = connection("bbpub_control_normal", "postgres").await;
    let admin_driver = tokio::spawn(connection);
    let id: Vec<u8> = admin
        .query_one(
            "SELECT job_id FROM control.jobs WHERE export_job_protocol=1",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let job = JobId::try_from(<[u8; 16]>::try_from(id).unwrap()).unwrap();
    let before = owner.read_export_job(job).await.unwrap().unwrap();
    assert_eq!(before.state(), ExportState::Paused);
    assert_eq!(before.revision(), 7);
    let destination = Arc::new(
        LocalDestination::open(
            Path::new("/out/signed_normal/export-destination"),
            dataset(),
            before.selection().destination().id,
        )
        .unwrap(),
    );
    let checked = destination.clone().check_for_acceptance().unwrap();
    let budget = budget();
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
    let old_key = CommandKey::try_from([0xe4; 16]).unwrap();
    let old = request(4);
    let first = owner
        .resume_export(
            &session.session,
            &session.csrf,
            old_key,
            job,
            &old,
            None,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(!first.created && first.job_revision.get() == 5);
    assert_eq!(owner.read_export_job(job).await.unwrap().unwrap(), before);
    case("fresh-session-resolves-old-receipt-after-second-pause-and-session-cleanup");
    let grant: String = admin.query_one("DELETE FROM control.principal_grants WHERE action='data.read' RETURNING to_jsonb(principal_grants)::text", &[]).await.unwrap().get(0);
    admin.execute("DELETE FROM control.principal_grants WHERE action IN ('roles.control','status.read','principals.manage')", &[]).await.unwrap();
    let started = Cell::new(false);
    let retry = owner
        .resume_export(
            &session.session,
            &session.csrf,
            old_key,
            job,
            &old,
            None,
            &started,
        )
        .await
        .unwrap();
    assert_eq!(retry, first);
    assert!(!started.get());
    assert_eq!(owner.read_export_job(job).await.unwrap().unwrap(), before);
    let new_key = CommandKey::try_from([0xf1; 16]).unwrap();
    let command = request(7);
    assert_eq!(
        owner
            .resume_export(
                &session.session,
                &session.csrf,
                new_key,
                job,
                &command,
                Some(&checked),
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Forbidden)
    );
    assert_eq!(
        owner
            .resume_export(
                &session.session,
                &session.csrf,
                old_key,
                job,
                &command,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    case("retry-needs-no-current-input-destination-or-role-control-and-cannot-requeue");
    admin.execute("INSERT INTO control.principal_grants SELECT * FROM jsonb_populate_record(NULL::control.principal_grants,$1::text::jsonb)", &[&grant]).await.unwrap();
    assert_eq!(
        owner
            .resume_export(
                &session.session,
                &session.csrf,
                new_key,
                job,
                &command,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::ArchiveUnavailable)
    );
    assert_eq!(
        owner
            .resume_export(
                &session.session,
                &session.csrf,
                new_key,
                job,
                &request(6),
                Some(&checked),
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    let wrong_csrf = BrowserSecret::parse_hex("f0".repeat(32).as_bytes()).unwrap();
    assert_eq!(
        owner
            .resume_export(
                &session.session,
                &wrong_csrf,
                new_key,
                job,
                &command,
                Some(&checked),
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Denied)
    );
    assert_eq!(owner.read_export_job(job).await.unwrap().unwrap(), before);
    case("new-resume-refuses-missing-destination-stale-revision-and-invalid-csrf");
    let started = Cell::new(false);
    let accepted = owner
        .resume_export(
            &session.session,
            &session.csrf,
            new_key,
            job,
            &command,
            Some(&checked),
            &started,
        )
        .await
        .unwrap();
    assert!(started.get() && accepted.created && accepted.job_revision.get() == 8);
    let after = owner.read_export_job(job).await.unwrap().unwrap();
    assert_eq!(after.state(), ExportState::Queued);
    assert_eq!(after.revision(), 8);
    assert_eq!(after.reservation(), before.reservation());
    assert_eq!(after.selection(), before.selection());
    let started = Cell::new(false);
    let retry = owner
        .resume_export(
            &session.session,
            &session.csrf,
            new_key,
            job,
            &command,
            None,
            &started,
        )
        .await
        .unwrap();
    assert!(!started.get() && !retry.created);
    assert_eq!(retry.job_revision, accepted.job_revision);
    assert_eq!(retry.accepted_at_us, accepted.accepted_at_us);
    assert_eq!(owner.read_export_job(job).await.unwrap().unwrap(), after);
    case("resume-commits-once-without-roles-control-status-or-manager-and-preserves-unit");
    owner
        .revoke_browser_session(&session.session, &session.csrf)
        .await
        .unwrap();
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    drop(checked);
    drop(destination);
    drop(admin);
    admin_driver.await.unwrap().unwrap();
    case("resume-control-driver-joined-before-destination-release");
}
