//! Finite accepted jobs through the actual source socket and rollback delivery.
use super::*;
use bigbrotr::{
    acquisition::job::{AcquisitionCommand, AcquisitionJobBinding, JobDuration},
    management::sessions::IssuedSession,
    network::TransportError,
    storage::{
        postgres::{control_owner::ScopeIntent, producer_sources::SourceBindingError},
        rocks::store::StoredVisibility,
    },
};
use bigbrotr_model::PolicyEpoch;
use bigbrotr_protocol::selector::decode_selector;
use std::{
    future::{Future, poll_fn},
    task::Poll,
};
use tokio::time::sleep_until;

struct Received<'a> {
    bytes: [u8; 6],
    dropped: &'a Cell<bool>,
}
impl Drop for Received<'_> {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

const REMAINING: &str = "
    SELECT a.deadline_at_us - now.us, o.lease_expires_at_us - now.us
    FROM control.acquisition_jobs a
    JOIN control.owner_epochs o ON o.dataset_id = a.dataset_id AND o.role = 'control'
    CROSS JOIN LATERAL (
        SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint AS us
    ) now
    WHERE a.job_id = $1::bytea AND o.epoch = 1";

async fn remaining(observer: &tokio_postgres::Client, binding: AcquisitionJobBinding) -> [i64; 2] {
    let row = observer
        .query_one(REMAINING, &[&&binding.job.as_bytes()[..]])
        .await
        .unwrap();
    [row.get(0), row.get(1)]
}

async fn gate(path: &Path, extension: &str) {
    let end = Instant::now() + Duration::from_secs(10);
    while !path.with_extension(extension).exists() {
        assert!(Instant::now() < end, "job transport gate expired");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture borrows the existing owner, native visibility and socket laboratory"
)]
pub async fn run(
    owner: &mut ControlSession,
    observer: &tokio_postgres::Client,
    connector: &DirectConnector,
    visibility: &StoredVisibility,
    source: RelaySourceBinding,
    root: &crate::config::Resolved,
    path: &Path,
    mode: &str,
    listener: &TcpListener,
    session: &IssuedSession,
) {
    let case = mode.strip_prefix("source-transport-job-").unwrap();
    assert!(matches!(
        case,
        "pass" | "pending" | "held" | "ready" | "omitted" | "unclaimed" | "expired"
    ));
    let selector = decode_selector(
        br#"{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}"#,
    )
    .unwrap();
    let scope = owner
        .register_acquisition_scope(
            ScopeIntent {
                selector: &selector,
                endpoint: source.endpoint_id,
                access: source.access_scope_id,
                visibility: visibility.entry().reference(),
                policy_epoch: PolicyEpoch::new(1),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    // Acceptance follows slow native/login/enrollment setup. The product sets
    // the original deadline; the fixture never injects or extends a timestamp.
    let receipt = owner
        .accept_acquisition(
            &session.session,
            &session.csrf,
            [77; 16].try_into().unwrap(),
            &AcquisitionCommand {
                scope_id: scope.identity,
                deadline_us: JobDuration::new(2_000_000).unwrap(),
                expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
                expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(receipt.created);
    if case != "unclaimed" {
        owner
            .claim_acquisition(receipt.job_id, &Cell::new(false))
            .await
            .unwrap();
    }
    let binding = AcquisitionJobBinding {
        job: receipt.job_id,
        scope: scope.identity,
    };
    let before = ingest_snapshot::state(observer, path).await;
    let backend = observer
        .query_one(
            "SELECT control_backend_pid, control_backend_start::text
             FROM control.owner_epochs WHERE role='control' AND epoch=1",
            &[],
        )
        .await
        .unwrap();
    emit(serde_json::json!({"event":"job-source-ready", "case":case,
        "job":binding.job, "scope":binding.scope, "pid":backend.get::<_,i32>(0),
        "backend_start":backend.get::<_,String>(1), "state":before}));
    gate(path, "go").await;
    if case == "expired" {
        let end = Instant::now() + Duration::from_secs(3);
        while remaining(observer, binding).await[0] > 0 {
            assert!(Instant::now() < end);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let times = remaining(observer, binding).await;
    assert!(times[1] > 5_000_000, "Control must outlive the operation");
    assert!(if case == "expired" {
        times[0] <= 0
    } else {
        times[0] > 1_000_000
    });
    emit(serde_json::json!({"event":"job-source-start", "remaining_us":times}));
    let started = Instant::now();
    let returned = Cell::new(false);
    let dropped = Cell::new(false);
    let refusal = matches!(case, "omitted" | "unclaimed" | "expired");
    let result = {
        let receive = owner.run_relay_source(
            RelaySourceRequest {
                acquisition: (case != "omitted").then_some(binding),
                source,
                visibility,
                policy_ttl: Duration::from_secs(5),
                deadline: started + Duration::from_secs(5),
            },
            connector,
            |doc, digest, url, route| {
                crate::config::acquisition::authorize_sync_source(root, doc, digest, url, route)
            },
            async |stream| {
                stream.write_all(b"request").await?;
                let mut bytes = [0; 6];
                stream.read_exact(&mut bytes).await?;
                returned.set(true);
                emit(serde_json::json!({"event":"job-source-value", "elapsed_us":started.elapsed().as_micros()}));
                Ok(Received { bytes, dropped: &dropped })
            },
        );
        tokio::pin!(receive);
        let resume = sleep_until(started + Duration::from_secs(3));
        tokio::pin!(resume);
        // Pause only delivery after the callback; the actual PostgreSQL driver
        // keeps polling and receives C/Z before the original job expires.
        let scheduled = poll_fn(|cx| {
            if case == "ready" && returned.get() && resume.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            receive.as_mut().poll(cx)
        });
        if refusal {
            let result = timeout(Duration::from_secs(1), scheduled).await.unwrap();
            assert!(
                timeout(Duration::from_millis(40), listener.accept())
                    .await
                    .is_err()
            );
            result
        } else {
            let peer = async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 7];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"request");
                if case != "pending" {
                    stream.write_all(b"answer").await.unwrap();
                }
                assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
                emit(
                    serde_json::json!({"event":"job-source-peer-closed", "elapsed_us":started.elapsed().as_micros()}),
                );
            };
            let (result, ()) = timeout(Duration::from_secs(6), async {
                tokio::join!(scheduled, peer)
            })
            .await
            .unwrap();
            result
        }
    };
    let expected = match case {
        "pass" => None,
        "pending" => Some(RelaySourceError::Transport(TransportError::Authority)),
        "held" | "ready" => Some(RelaySourceError::Control(OwnerError::Unavailable)),
        "omitted" | "unclaimed" => Some(RelaySourceError::Source(SourceBindingError::Conflict)),
        "expired" => Some(RelaySourceError::Source(SourceBindingError::Unavailable)),
        _ => unreachable!(),
    };
    match result {
        Ok(value) => {
            assert_eq!(expected, None);
            assert_eq!(&value.bytes, b"answer");
            drop(value);
        }
        Err(error) => assert_eq!(Some(error), expected),
    }
    assert_eq!(returned.get(), matches!(case, "pass" | "held" | "ready"));
    assert_eq!(dropped.get(), returned.get());
    let times = remaining(observer, binding).await;
    assert!(times[1] > 5_000_000);
    if matches!(case, "held" | "ready" | "expired") {
        assert!(times[0] <= 0);
    }
    // A conservative local lease may close a pending socket just before the
    // server deadline. It still must refuse well before the outer five seconds.
    assert!(started.elapsed() < Duration::from_secs(5));
    let after = ingest_snapshot::state(observer, path).await;
    assert_eq!(after, before);
    emit(
        serde_json::json!({"event":"job-source-finished", "case":case,
        "elapsed_us":started.elapsed().as_micros(), "remaining_us":times,
        "returned":returned.get(), "disposed":dropped.get(), "state":after}),
    );
    gate(path, "done").await;
}
