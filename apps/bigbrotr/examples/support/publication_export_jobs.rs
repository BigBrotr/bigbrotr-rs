//! Actual accepted job/pin, independent SQL and native source fixture.
use super::*;
use bigbrotr::{
    export::destination::LocalDestination,
    management::{
        jobs::ExportCommand,
        sessions::{IssuedSession, SessionError},
    },
    storage::postgres::control_owner::ScopeGrantAllocation,
};
use bigbrotr_model::{CommandKey, DestinationId};

pub async fn prepare(control: &str) {
    let (mut admin, driver) = connect(control, "postgres").await;
    migration_runner::run(&mut admin, 813, true, &Cell::new(false))
        .await
        .unwrap();
    admin.batch_execute("GRANT SELECT,INSERT,UPDATE ON control.jobs TO bigbrotr_control; GRANT SELECT,INSERT ON control.job_input_bounds,control.export_jobs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control").await.unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
}
async fn state(admin: &Client) -> String {
    admin.query_one("SELECT jsonb_build_object('jobs',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY job_id) FROM control.jobs t),'details',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY job_id) FROM control.export_jobs t),'pins',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY lease_id) FROM control.archive_snapshot_pins t),'leases',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY lease_id) FROM control.snapshot_leases t),'claims',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY claim_id) FROM control.retention_claims t),'commands',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.management_commands t),'audit',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY audit_id) FROM control.management_audit t))::text", &[]).await.unwrap().get(0)
}
pub async fn run(
    owner: &mut ControlSession,
    session: &IssuedSession,
    allocation: &ScopeGrantAllocation<'_, '_>,
    admin: &Client,
    home: &Path,
) {
    let destination = LocalDestination::initialize(
        &directory(home.join("export-destination")),
        dataset(),
        DestinationId::try_from([0xe1; 16]).unwrap(),
    )
    .unwrap();
    let command: ExportCommand = serde_json::from_value(serde_json::json!({
        "format":"signed-events-jsonl",
        "query":{"version":1,"resource":"event_history","scope":allocation.visibility.read().scope,
            "filters":[{"ids":["ab".repeat(32)]}]},
        "allocation":{"catalog_id":"5a".repeat(16),"frontier_revision":"1"},
        "destination":{"id":destination.id(),"binding_sha256":destination.binding_digest()},
        "expected_config_revision":"1","expected_policy_revision":"1"}))
    .unwrap();
    let key = CommandKey::try_from([0xe2; 16]).unwrap();
    let before = state(admin).await;
    assert_eq!(
        owner
            .accept_export(
                &session.session,
                &session.csrf,
                key,
                &command,
                None,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::ArchiveUnavailable)
    );
    assert_eq!(state(admin).await, before);
    owner.require_export_jobs().await.unwrap();
    admin
        .batch_execute(include_str!(
            "../../../../experiments/archive-engine/export_job_omissions.sql"
        ))
        .await
        .unwrap();
    let committed = Cell::new(false);
    let receipt = owner
        .accept_export(
            &session.session,
            &session.csrf,
            key,
            &command,
            Some(allocation),
            Some(&destination),
            &committed,
        )
        .await
        .unwrap();
    assert!(receipt.created && committed.get());
    let trials: i64 = admin
        .query_one("SELECT count(*) FROM public.export_fixture_results", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(trials, 13);
    admin.batch_execute("DROP TRIGGER a_export_omissions ON control.management_audit; DROP FUNCTION public.export_fixture_omissions(); DROP FUNCTION public.export_fixture_state()").await.unwrap();
    let row = admin.query_one("SELECT j.state,j.input_complete,j.job_revision::text,e.pin_lease_id<>e.allocation_pin_id,p.export_job_id=j.job_id,l.state,(SELECT count(*) FROM control.job_input_bounds b WHERE b.job_id=j.job_id AND b.shard_id=0 AND b.owner_epoch=1 AND b.last_complete_sequence=10),(SELECT count(*) FROM control.retention_claims c WHERE c.lease_id=e.pin_lease_id AND c.state='active' AND c.first_sequence=1 AND c.last_sequence=10 AND c.purpose='export') FROM control.jobs j JOIN control.export_jobs e USING(job_id) JOIN control.archive_snapshot_pins p ON p.lease_id=e.pin_lease_id JOIN control.snapshot_leases l ON l.lease_id=e.pin_lease_id WHERE j.job_id=$1::bytea", &[&&receipt.job_id.as_bytes()[..]]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, bool>(1),
            row.get::<_, String>(2),
            row.get::<_, bool>(3),
            row.get::<_, bool>(4),
            row.get::<_, String>(5),
            row.get::<_, i64>(6),
            row.get::<_, i64>(7)
        ),
        (
            "accepted".into(),
            true,
            "1".into(),
            true,
            true,
            "active".into(),
            1,
            1
        )
    );
    let before = state(admin).await;
    let retry = owner
        .accept_export(
            &session.session,
            &session.csrf,
            key,
            &command,
            None,
            None,
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert_eq!(retry.job_id, receipt.job_id);
    assert_eq!(retry.accepted_at_us, receipt.accepted_at_us);
    assert!(!retry.created);
    let mut conflicting = command.clone();
    conflicting.query.page_size = Some(99);
    assert_eq!(
        owner
            .accept_export(
                &session.session,
                &session.csrf,
                key,
                &conflicting,
                None,
                None,
                &Cell::new(false)
            )
            .await,
        Err(SessionError::Conflict)
    );
    assert_eq!(state(admin).await, before);
    emit(
        serde_json::json!({"case":"export-atomic-native-acceptance-and-historical-retry","status":"PASS","job":receipt}),
    );
    // The accepted job deliberately retains its export obligation after this
    // fixture's ordinary reader releases. This is not terminal-job recovery.
}
