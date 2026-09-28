//! Finite source transactions against externally observed concurrent writers.
//! Writers are privileged clone-only injections, not another Control owner.
use super::*;
use bigbrotr::{
    acquisition::relay_source::RelaySourceBinding,
    network::{DirectConnector, RouteSelection},
    storage::{
        postgres::{
            control_owner::{RelaySourceError, RelaySourceRequest},
            producer_sources::SourceBindingError,
        },
        rocks::store::StoredVisibility,
    },
};
use bigbrotr_protocol::endpoint::Endpoint;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

const CURRENT_DOCUMENTS: &str = "
    SELECT 0 AS origin, c.document::text, c.document_digest
    FROM control.config_revisions c
    JOIN control.datasets d ON c.revision=d.config_revision
    UNION ALL
    SELECT 1, p.document::text, p.document_digest
    FROM control.policy_revisions p
    JOIN control.datasets d ON p.revision=d.policy_revision
    ORDER BY origin";
const OWNER_BACKEND: &str = "
    SELECT control_backend_pid, control_backend_start::text
    FROM control.owner_epochs WHERE role='control' AND epoch=1";

async fn gate(path: &Path, suffix: &str) {
    let file = path.with_extension(suffix);
    let end = Instant::now() + Duration::from_secs(15);
    while !file.exists() {
        assert!(Instant::now() < end, "source race gate expired");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn require_valid_ban(
    observer: &tokio_postgres::Client,
    root: &crate::config::Resolved,
    policy_only: bool,
) {
    // A malformed document or digest would produce the same InvalidPolicy error.
    // Both actual accepted documents must independently allow this unbanned URL.
    let rows = observer.query(CURRENT_DOCUMENTS, &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let origin: i32 = row.get(0);
        let document: &str = row.get(1);
        let digest: [u8; 32] = row.get::<_, &[u8]>(2).try_into().unwrap();
        crate::config::acquisition::authorize_sync_source(
            root,
            document,
            digest,
            &Endpoint::parse("wss://unbanned.example/").unwrap(),
            RouteSelection::Public {},
        )
        .unwrap();
        let matched = crate::config::acquisition::authorize_sync_source(
            root,
            document,
            digest,
            &Endpoint::parse("ws://127.0.0.1:7447/private").unwrap(),
            RouteSelection::Public {},
        );
        assert_eq!(
            matched.err(),
            if (origin == 1) == policy_only {
                Some(OwnerError::InvalidPolicy)
            } else {
                None
            }
        );
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture retains actual owner and committed scope without a second setup"
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
) {
    let schedule = mode.strip_prefix("source-transport-race-").unwrap();
    assert!(matches!(
        schedule,
        "withdraw"
            | "generation"
            | "ban"
            | "rollback"
            | "source-withdraw"
            | "source-ban"
            | "ban-other"
    ));
    let source_first = schedule.starts_with("source-");
    let policy = schedule.ends_with("ban");
    let backend = observer.query_one(OWNER_BACKEND, &[]).await.unwrap();
    emit(
        serde_json::json!({"event":"race-ready","pid":backend.get::<_,i32>(0),
        "backend_start":backend.get::<_,String>(1),"binding":{
            "endpoint":hex(source.endpoint_id.as_bytes()),"access":hex(source.access_scope_id.as_bytes()),
            "generation":source.enrollment_generation.get(),"route":hex(source.route_id.as_bytes())},
        "state":ingest_snapshot::state(observer,path).await}),
    );
    gate(path, "go").await;
    for phase in 0..if source_first { 2 } else { 1 } {
        if phase == 1 {
            gate(path, "retry").await;
        }
        let success = phase == 0 && (source_first || matches!(schedule, "rollback" | "ban-other"));
        let receive = owner.run_relay_source(
            RelaySourceRequest {
                acquisition: None,
                source,
                visibility,
                policy_ttl: Duration::from_secs(5),
                deadline: Instant::now() + Duration::from_secs(5),
            },
            connector,
            |doc, digest, url, route| {
                crate::config::acquisition::authorize_sync_source(root, doc, digest, url, route)
            },
            async |stream| {
                stream.write_all(b"request").await?;
                let mut bytes = [0; 6];
                stream.read_exact(&mut bytes).await?;
                Ok(bytes)
            },
        );
        if success {
            let peer = async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 7];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"request");
                emit(serde_json::json!({"event":"race-peer-ready"}));
                if source_first {
                    gate(path, "peer").await;
                }
                stream.write_all(b"answer").await.unwrap();
                assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
                emit(serde_json::json!({"event":"race-peer-closed"}));
            };
            let (result, ()) = timeout(Duration::from_secs(6), async {
                tokio::join!(receive, peer)
            })
            .await
            .unwrap();
            assert_eq!(result.unwrap(), *b"answer");
        } else {
            let expected = if policy {
                RelaySourceError::Control(OwnerError::InvalidPolicy)
            } else {
                RelaySourceError::Source(SourceBindingError::Unavailable)
            };
            assert_eq!(receive.await.unwrap_err(), expected);
            assert!(
                timeout(Duration::from_millis(40), listener.accept())
                    .await
                    .is_err()
            );
            if policy {
                require_valid_ban(observer, root, schedule == "ban").await;
            }
        }
        emit(
            serde_json::json!({"event":"race-finished","phase":phase,"received":success,
            "state":ingest_snapshot::state(observer,path).await}),
        );
    }
    gate(path, "done").await;
}
