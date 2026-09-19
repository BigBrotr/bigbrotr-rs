//! Actual application/migrator lock schedules and lost binding COMMIT replies.
//! Every barrier observes PostgreSQL's blocking graph, not elapsed time.
use super::*;
use tokio::sync::oneshot;
use tokio_postgres::Client;

async fn pid(client: &Client) -> i32 {
    client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0)
}
async fn owner_pid(client: &Client, owner: &ControlSession) -> i32 {
    client.query_one("SELECT control_backend_pid FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric", &[&&owner.identity().resource_id.as_bytes()[..], &owner.identity().epoch.get().to_string()]).await.unwrap().get(0)
}
async fn blocked(client: &Client, waiter: i32, holder: i32, event: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = client.query_opt("SELECT a.backend_start::text,a.wait_event,pg_blocking_pids(a.pid) FROM pg_stat_activity a WHERE a.pid=$1 AND a.wait_event_type='Lock' AND $2=ANY(pg_blocking_pids(a.pid))", &[&waiter,&holder]).await.unwrap();
            if let Some(row) = row {
                let observed: String = row.get(1);
                assert_eq!(observed, event);
                let locks: String = client.query_one("SELECT coalesce(json_agg(t ORDER BY pid,locktype,mode),'[]')::text FROM (SELECT pid,locktype,mode,granted,classid::text,objid::text FROM pg_locks WHERE pid=ANY($1::integer[]) AND (locktype='advisory' OR NOT granted)) t", &[&vec![waiter,holder]]).await.unwrap().get(0);
                return serde_json::json!({"waiter":waiter,"holder":holder,"backend_start":row.get::<_,String>(0),"event":observed,"blockers":row.get::<_,Vec<i32>>(2),"locks":serde_json::from_str::<serde_json::Value>(&locks).unwrap()});
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap()
}
async fn grants(client: &Client) {
    client.batch_execute("GRANT SELECT,INSERT ON control.archive_coordinators TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control").await.unwrap();
}

async fn gone(client: &Client, backend: i32) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
                    &[&backend],
                )
                .await
                .unwrap()
                .get(0);
            if !exists {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

pub async fn races(local: &LocalCoordinator, request: &ArchiveAttemptRequest) {
    for old_first in [true, false] {
        let database = if old_first {
            "bbprotocol_old_first"
        } else {
            "bbprotocol_migration_first"
        };
        prepare(database, false).await;
        let (mut owner, driver) = claim(database, local, 40).await;
        // Ordinary require refuses missing migration10. This subcase does not
        // exercise issuance; a fresh claim follows its poisoned operation.
        assert_eq!(
            owner.require_archive_coordinator(local).await,
            Err(ArchiveAttemptError::Prerequisite)
        );
        close(owner, driver).await;
        let (mut owner, driver) = claim(database, local, 42).await;
        let (observer, observer_driver) = connect(database, "postgres").await;
        let (mut migrator, migration_driver) = connect(database, "postgres").await;
        // Provision read access at table creation so this race reaches the new
        // issuance guard, independently of a later administrator GRANT race.
        migrator.batch_execute("ALTER DEFAULT PRIVILEGES IN SCHEMA control GRANT SELECT ON TABLES TO bigbrotr_control").await.unwrap();
        let (mut blocker, blocker_driver) = connect(database, "postgres").await;
        let owner_pid = owner_pid(&observer, &owner).await;
        let migration_pid = pid(&migrator).await;
        let blocker_pid = pid(&blocker).await;
        let held = blocker.transaction().await.unwrap();
        held.batch_execute(if old_first {
            "LOCK TABLE control.archive_attempts IN ACCESS EXCLUSIVE MODE"
        } else {
            "LOCK TABLE control.archive_attempts IN ACCESS SHARE MODE"
        })
        .await
        .unwrap();
        let (go, wait) = oneshot::channel();
        let migration_commit = Cell::new(false);
        let issuance_commit = Cell::new(false);
        let (migration, issuance, locks) = if old_first {
            tokio::join!(
                async {
                    wait.await.unwrap();
                    migration_runner::run(&mut migrator, 10, true, &migration_commit).await
                },
                owner.register_archive_attempt(request, &issuance_commit),
                async {
                    let first = blocked(&observer, owner_pid, blocker_pid, "relation").await;
                    go.send(()).unwrap();
                    let second = blocked(&observer, migration_pid, owner_pid, "advisory").await;
                    held.rollback().await.unwrap();
                    vec![first, second]
                }
            )
        } else {
            tokio::join!(
                migration_runner::run(&mut migrator, 10, true, &migration_commit),
                async {
                    wait.await.unwrap();
                    owner
                        .register_archive_attempt(request, &issuance_commit)
                        .await
                },
                async {
                    let first = blocked(&observer, migration_pid, blocker_pid, "relation").await;
                    go.send(()).unwrap();
                    let second = blocked(&observer, owner_pid, migration_pid, "advisory").await;
                    held.rollback().await.unwrap();
                    vec![first, second]
                }
            )
        };
        assert_eq!(migration.unwrap().applied, 1);
        assert!(migration_commit.get());
        assert_eq!(issuance_commit.get(), old_first);
        if old_first {
            assert!(issuance.unwrap().created);
        } else {
            assert_eq!(issuance, Err(ArchiveAttemptError::Prerequisite));
        }
        close(owner, driver).await;
        grants(&migrator).await;
        let before = snapshot(database).await;
        assert_eq!(before["bindings"], serde_json::json!([]));
        assert_eq!(
            before["attempts"].as_array().unwrap().len(),
            usize::from(old_first)
        );
        let (mut owner, driver) = claim(database, local, 44).await;
        let result = owner
            .initialize_archive_coordinator(local, &Cell::new(false))
            .await;
        if old_first {
            assert_eq!(result, Err(ArchiveAttemptError::Conflict));
            assert!(before["attempts"][0]["coordinator_binding_sha256"].is_null());
            assert_eq!(snapshot(database).await, before);
        } else {
            assert_eq!(result, Ok(true));
            assert_eq!(
                snapshot(database).await["bindings"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        }
        close(owner, driver).await;
        drop(observer);
        drop(migrator);
        drop(blocker);
        observer_driver.await.unwrap().unwrap();
        migration_driver.await.unwrap().unwrap();
        blocker_driver.await.unwrap().unwrap();
        recorded(
            if old_first {
                "old-issuer-lock-precedes-migration"
            } else {
                "migration-lock-precedes-old-issuer"
            },
            serde_json::json!({"before_binding":before,"locks":locks,"final":snapshot(database).await}),
        );
    }
    cancelled(local).await;
}

async fn cancelled(local: &LocalCoordinator) {
    let database = "bbprotocol_cancelled";
    prepare(database, true).await;
    let before = snapshot(database).await;
    let (mut owner, driver) = claim(database, local, 46).await;
    let (observer, observer_driver) = connect(database, "postgres").await;
    let (mut blocker, blocker_driver) = connect(database, "postgres").await;
    let owner_pid = owner_pid(&observer, &owner).await;
    let blocker_pid = pid(&blocker).await;
    let held = blocker.transaction().await.unwrap();
    held.batch_execute("LOCK TABLE control.archive_coordinators IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let commit = Cell::new(false);
    let locks;
    {
        let operation = owner.initialize_archive_coordinator(local, &commit);
        tokio::pin!(operation);
        locks = tokio::select! {
            result = &mut operation => panic!("blocked initializer completed: {result:?}"),
            locks = blocked(&observer, owner_pid, blocker_pid, "relation") => locks,
        };
        // Drop the real in-flight API future; it cannot restore availability.
    }
    assert!(!commit.get());
    assert_eq!(
        owner.require_archive_coordinator(local).await,
        Err(ArchiveAttemptError::Unavailable)
    );
    drop(owner);
    held.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    gone(&observer, owner_pid).await;
    assert_eq!(snapshot(database).await, before);
    let (mut owner, driver) = claim(database, local, 48).await;
    assert_eq!(
        owner.require_archive_coordinator(local).await,
        Err(ArchiveAttemptError::Prerequisite)
    );
    close(owner, driver).await;
    assert_eq!(snapshot(database).await, before);
    drop(observer);
    drop(blocker);
    observer_driver.await.unwrap().unwrap();
    blocker_driver.await.unwrap().unwrap();
    recorded(
        "cancelled-blocked-binding-preserves-empty-history",
        serde_json::json!({"locks":locks,"final":before}),
    );
}

pub async fn commit_loss(mode: &str, port: u16) {
    assert!(matches!(mode, "before" | "after"));
    let database = if mode == "before" {
        "bbprotocol_commit_before"
    } else {
        "bbprotocol_commit_after"
    };
    prepare(database, true).await;
    let home = directory(format!("/out/coordinator-protocol-{mode}").into());
    let data = directory(home.join("data"));
    let identity = CatalogIdentity::local(
        dataset(),
        [90; 16].try_into().unwrap(),
        &format!("{}/", data.display()),
    )
    .unwrap();
    drop(LocalInitialization::create(&identity, [3; 16]).unwrap());
    let local = LocalCoordinator::initialize(&identity, &directory(home.join("state"))).unwrap();
    let (mut owner, driver) = claim_port(database, &local, 50, port).await;
    let (observer, observer_driver) = connect(database, "postgres").await;
    let backend = owner_pid(&observer, &owner).await;
    let original = owner.identity();
    let commit = Cell::new(false);
    assert_eq!(
        owner.initialize_archive_coordinator(&local, &commit).await,
        Err(ArchiveAttemptError::CommitUnknown)
    );
    assert!(commit.get());
    assert_eq!(
        owner.require_archive_coordinator(&local).await,
        Err(ArchiveAttemptError::Unavailable)
    );
    drop(owner);
    let driver_error = driver.await.unwrap().is_err();
    gone(&observer, backend).await;
    drop(observer);
    observer_driver.await.unwrap().unwrap();
    // Actual new owner claim serializes against the old backend's Control fence.
    let (mut owner, driver) = claim(database, &local, 52).await;
    let before = snapshot(database).await;
    assert!(before["attempts"].as_array().unwrap().is_empty());
    assert_eq!(
        before["bindings"].as_array().unwrap().len(),
        usize::from(mode == "after")
    );
    let commit = Cell::new(false);
    assert_eq!(
        owner.initialize_archive_coordinator(&local, &commit).await,
        Ok(mode == "before")
    );
    assert_eq!(commit.get(), mode == "before");
    owner.require_archive_coordinator(&local).await.unwrap();
    let after = snapshot(database).await;
    assert_eq!(after["bindings"].as_array().unwrap().len(), 1);
    if mode == "after" {
        assert_eq!(before, after);
        assert_eq!(
            after["bindings"][0]["issuer_epoch"].as_u64(),
            Some(original.epoch.get())
        );
    } else {
        assert_eq!(
            after["bindings"][0]["issuer_epoch"].as_u64(),
            Some(owner.identity().epoch.get())
        );
    }
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    recorded(
        mode,
        serde_json::json!({"before_retry":before,"after_retry":after,"driver_error":driver_error}),
    );
}
