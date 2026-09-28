//! Real Control/native visibility, current product policy and a bounded socket.
use super::*;
use bigbrotr::{
    acquisition::relay_source::RelaySourceBinding,
    management::{
        credentials::HASH_RESERVATION, grants::Revision, relay_enrollment::EnrollmentCommand,
        sessions::SessionLifetime,
    },
    network::{DirectConnector, DnsResolver, RouteSelection, TlsPolicy},
    storage::{
        postgres::control_owner::{
            EnrollmentRoutes, InitialVisibilityRequest, RelaySourceError, RelaySourceRequest,
        },
        rocks::store::CertificationLimits,
    },
};
use bigbrotr_protocol::endpoint::Endpoint;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

const OWNER_BACKEND: &str = "
    SELECT control_backend_pid, control_backend_start::text,
        lease_expires_at_us - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint
    FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric";
const TERMINATE_BACKEND: &str = "
    SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_stat_activity
    WHERE pid=$1 AND backend_start::text=$2";

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture retains actual owner/native/driver handles"
)]
pub async fn run(
    mut owner: ControlSession,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    scope: QualifiedRef,
) {
    assert!(
        mode.starts_with("source-transport-race-")
            || matches!(
                mode,
                "source-transport"
                    | "source-transport-policy-refused"
                    | "source-transport-owner-loss"
                    | "source-transport-cleanup-pass"
                    | "source-transport-cleanup-expired"
                    | "source-transport-cleanup-timeout"
                    | "source-transport-cleanup-ready"
            )
    );
    let owner_loss = mode == "source-transport-owner-loss";
    let policy_refused = mode == "source-transport-policy-refused";
    let validate = |doc: &str, digest| validate_version(doc, digest, true);
    let listener = TcpListener::bind("127.0.0.1:7447").await.unwrap();
    let endpoint = Endpoint::parse("ws://127.0.0.1:7447/private").unwrap();
    let deployment = include_str!("../../../../config/recipes/archive-only.toml")
        .replace(
            "22222222222222222222222222222222",
            &hex(spec.dataset_id.as_bytes()),
        )
        .replace("11111111111111111111111111111111", &hex(&spec.instance_id));
    let root = crate::config::resolve::parse(&format!(
        "{deployment}\n[network.private]\nrules=[{{endpoint='ws://127.0.0.1:7447/private',cidr='127.0.0.1/32',port=7447}}]\n"
    )).unwrap();
    assert_eq!(
        root.text("topology.dataset_id"),
        Some(hex(spec.dataset_id.as_bytes()).as_str())
    );
    assert_eq!(
        root.text("topology.instance_id"),
        Some(hex(&spec.instance_id).as_str())
    );
    let ceiling = bigbrotr::network::DirectPolicy::private(vec![
        bigbrotr::network::PrivateRule::endpoint(endpoint.clone(), "127.0.0.1/32", 7447).unwrap(),
    ])
    .unwrap();
    let hash_budget = ResourceBudget::new(
        ProcessEnvelope::new(
            HASH_RESERVATION,
            0,
            ResourceLimits::new(HASH_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let verified = owner
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap()
        .verify(
            b"Independent owner password 2026",
            &mut grant(&hash_budget, HASH_RESERVATION, 0),
        )
        .unwrap();
    let session = owner
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    let registered = owner
        .register_relay_endpoint(&endpoint, &Cell::new(false))
        .await
        .unwrap();
    let cancelled = AtomicBool::new(false);
    let limits = CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    };
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connector = DirectConnector::new(
        DnsResolver::new(dns.local_addr().unwrap(), budget.clone()).unwrap(),
        TlsPolicy::new(&[]).unwrap(),
        budget.clone(),
    );
    for ordinal in [1u64, 2] {
        let visibility = QualifiedRef {
            ordinal: Ordinal::new(ordinal).unwrap(),
            ..scope
        };
        let receipt = owner
            .accept_relay_enrollment(
                &session.session,
                &session.csrf,
                [ordinal as u8; 16].try_into().unwrap(),
                &EnrollmentCommand {
                    endpoint_id: registered.identity,
                    visibility,
                    route: RouteSelection::Private {
                        cidr: "127.0.0.1/32".parse().unwrap(),
                    },
                    enabled: true,
                    expected_generation: None,
                    expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
                    expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
                },
                EnrollmentRoutes {
                    public_enabled: true,
                    private_ceiling: Some(&ceiling),
                },
                &Cell::new(false),
            )
            .await
            .unwrap();
        let committed = owner
            .read_initial_visibility(
                &mut store,
                InitialVisibilityRequest {
                    ingest_root: path,
                    registration: spec,
                    reference: visibility,
                    limits,
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
                },
                grant(budget, limits.required_data_bytes().unwrap(), 0),
                validate,
            )
            .await
            .unwrap()
            .unwrap();
        let before = ingest_snapshot::state(observer, path).await;
        let source = RelaySourceBinding {
            endpoint_id: registered.identity,
            access_scope_id: receipt.access_scope_id,
            enrollment_generation: receipt.generation.get().try_into().unwrap(),
            route_id: receipt.route_id,
        };
        if ordinal == 2 && mode.starts_with("source-transport-cleanup-") {
            crate::ingest_source_cleanup::run(
                &mut owner, observer, &connector, &committed, source, &root, path, mode, &listener,
            )
            .await;
            continue;
        }
        if ordinal == 2 && mode.starts_with("source-transport-race-") {
            crate::ingest_source_races::run(
                &mut owner, observer, &connector, &committed, source, &root, path, mode, &listener,
            )
            .await;
            continue;
        }
        let mut validations = 0;
        let started = Instant::now();
        let receive = owner.run_relay_source(
            RelaySourceRequest {
                source,
                visibility: &committed,
                policy_ttl: Duration::from_secs(2),
                deadline: Instant::now() + Duration::from_secs(2),
            },
            &connector,
            |doc, digest, url, route| {
                let policy = crate::config::acquisition::authorize_sync_source(
                    &root, doc, digest, url, route,
                )?;
                validations += 1;
                if policy_refused && validations == 2 {
                    return Err(OwnerError::InvalidPolicy);
                }
                Ok(policy)
            },
            async |stream| {
                stream.write_all(b"request").await?;
                let mut bytes = [0; 6];
                stream.read_exact(&mut bytes).await?;
                Ok(bytes)
            },
        );
        if ordinal == 1 {
            assert_eq!(receive.await.err(), Some(RelaySourceError::Visibility));
            assert!(
                timeout(Duration::from_millis(40), listener.accept())
                    .await
                    .is_err()
            );
        } else if policy_refused {
            assert_eq!(
                receive.await.err(),
                Some(RelaySourceError::Control(OwnerError::InvalidPolicy))
            );
            assert_eq!(validations, 2);
            assert!(
                timeout(Duration::from_millis(40), listener.accept())
                    .await
                    .is_err()
            );
        } else {
            let peer = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 7];
                socket.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"request");
                if owner_loss {
                    let identity = observer
                        .query_one(
                            OWNER_BACKEND,
                            &[
                                &&spec.control.resource_id.as_bytes()[..],
                                &spec.control.epoch.get().to_string(),
                            ],
                        )
                        .await
                        .unwrap();
                    let pid: i32 = identity.get(0);
                    let backend_start: String = identity.get(1);
                    assert!(identity.get::<_, i64>(2) > 2_000_000);
                    emit(
                        serde_json::json!({"event":"source-backend-loss","pid":pid,"backend_start":identity.get::<_,String>(1)}),
                    );
                    assert!(
                        observer
                            .query_one(TERMINATE_BACKEND, &[&pid, &backend_start])
                            .await
                            .unwrap()
                            .get::<_, bool>(0)
                    );
                    // A normal two-second expiry cannot explain the following
                    // half-second close: termination leaves over one second.
                    assert!(started.elapsed() < Duration::from_secs(1));
                    assert_eq!(
                        timeout(Duration::from_millis(500), socket.read(&mut [0; 1]))
                            .await
                            .unwrap()
                            .unwrap(),
                        0
                    );
                } else {
                    socket.write_all(b"answer").await.unwrap();
                    assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
                }
            };
            let (result, ()) = timeout(Duration::from_secs(3), async {
                tokio::join!(receive, peer)
            })
            .await
            .unwrap();
            if owner_loss {
                assert_eq!(
                    result.err(),
                    Some(RelaySourceError::Control(OwnerError::Backend))
                );
                assert_eq!(
                    owner.poll_policy(validate).await,
                    Err(OwnerError::Unavailable)
                );
            } else {
                assert_eq!(result.unwrap(), *b"answer");
            }
        }
        let after = ingest_snapshot::state(observer, path).await;
        assert_eq!(after, before);
        emit(
            serde_json::json!({"event":"source-transport","ordinal":ordinal,"state":after,"result":if ordinal==1 {"refused-public"} else if policy_refused {"refused-second-policy"} else if owner_loss {"closed-after-backend-loss"} else {"received-private"}}),
        );
    }
    assert!(
        timeout(Duration::from_millis(40), dns.accept())
            .await
            .is_err()
    );
    if owner_loss
        || mode.starts_with("source-transport-cleanup-")
        || mode.starts_with("source-transport-race-")
    {
        drop(owner);
    } else {
        owner.release().await.unwrap();
    }
    crate::ingest_closing::join(driver, owner_loss).await;
}
