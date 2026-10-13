//! Finite renewal refusals retain the local writer until final native closure.
//! The Control replacement below is a fixture, never writer adoption or retry.
use super::*;

pub(super) fn is_renewal(mode: &str) -> bool {
    matches!(
        mode,
        "successor-live-renew-control"
            | "successor-live-renew-expired"
            | "successor-live-renew-before"
            | "successor-live-renew-after"
    )
}

pub(super) async fn renewal(
    probe: &Probe,
    session: (ControlSession, Driver),
    writer: LiveInitialWriter,
    previous: &InitialRegistration,
    mut backend: Backend,
    mode: &str,
) {
    assert!(is_renewal(mode));
    let (mut owner, mut driver) = session;
    let different_control = mode == "successor-live-renew-control";
    let expired = mode == "successor-live-renew-expired";
    let lost_commit = matches!(
        mode,
        "successor-live-renew-before" | "successor-live-renew-after"
    );
    assert_eq!(probe.initial_proxy_port.is_some(), lost_commit);
    if different_control {
        // Keep exclusive native custody, but genuinely release and join the
        // preparing Control before acquiring its next epoch in another session.
        probe.snapshot("live-renew-before-switch").await;
        probe
            .end_control("renewal-previous", owner, driver, true)
            .await;
        joined("renewal-previous");
        probe.require_backend_absent(&backend).await;
        (owner, driver) = probe.acquire("renewal-fresh", previous, 2).await;
        backend = probe.observe_backend_at(&owner, "renewal-fresh").await;
    } else {
        // Claim/positive replay/prepare use COMMIT1..12, activation13 and this
        // actual Control refresh14. Only renewal15 crosses the selected fault.
        bounded(owner.refresh(|doc, digest| probe.validate(doc, digest)))
            .await
            .unwrap();
        probe.snapshot("live-control-refreshed").await;
        if expired {
            wait_expired(probe, &owner, &writer).await;
            probe.snapshot("live-renew-expired").await;
        }
    }
    probe.snapshot("live-renew-before").await;
    let commit_started = Cell::new(false);
    let result = bounded(owner.renew_live_initial_ingest(
        &writer,
        probe.policy_ttl(),
        |doc, digest| probe.validate(doc, digest),
        &commit_started,
    ))
    .await;
    emit(
        serde_json::json!({"event":"live-renewal-result","mode":mode,
        "control_epoch":owner.identity().epoch.get(),"commit_started":commit_started.get(),
        "error":result.as_ref().err().map(|error|format!("{error:?}"))}),
    );
    probe.snapshot("live-renew-refused").await;
    let expected = if different_control {
        OwnerError::InvalidInput
    } else if expired {
        OwnerError::Backend
    } else {
        OwnerError::CommitUnknown
    };
    assert_eq!(result, Err(expected));
    assert_eq!(commit_started.get(), lost_commit);
    if different_control {
        bounded(owner.refresh(|doc, digest| probe.validate(doc, digest)))
            .await
            .unwrap();
        emit(serde_json::json!({"event":"live-renewal-usable",
            "control_epoch":owner.identity().epoch.get(),"usable":true}));
        probe.snapshot("live-renew-control-usable").await;
    } else {
        if expired {
            let observed = scheduling(probe, &owner, &writer).await;
            observed.emit("after-refusal", owner.identity().epoch.get());
            assert!(observed.ingest <= observed.now && observed.now < observed.control);
        }
        probe.check_poisoned(&mut owner).await;
        probe.snapshot("live-renew-unavailable").await;
    }
    drop(writer);
    let stage = if lost_commit { "unknown" } else { "final" };
    probe
        .end_control(stage, owner, driver, different_control)
        .await;
    // The existing unknown-transport path emits its joined failed driver. The
    // two ordinary transport paths positively emit their clean joined driver.
    if !lost_commit {
        joined(stage);
    }
    probe.require_backend_absent(&backend).await;
    probe.snapshot("final").await;
    emit(serde_json::json!({"event":"live-complete","mode":mode,
        "native":"closed","control":if different_control {"released-and-joined"} else {"dropped-and-joined"},
        "writer":false,"admission":false}));
}

fn joined(stage: &str) {
    emit(serde_json::json!({"event":"successor-driver","stage":stage,
        "failed":false,"joined":true}));
}

struct Scheduling {
    now: i64,
    ingest: i64,
    control: i64,
}
impl Scheduling {
    fn emit(&self, phase: &str, control_epoch: u64) {
        emit(
            serde_json::json!({"event":"live-renewal-clock","phase":phase,
            "control_epoch":control_epoch,"now_us":self.now,
            "ingest_lease_expires_at_us":self.ingest,"control_lease_expires_at_us":self.control}),
        );
    }
}

async fn scheduling(
    probe: &Probe,
    owner: &ControlSession,
    writer: &LiveInitialWriter,
) -> Scheduling {
    let control = owner.identity();
    let target = writer.manifest().identity();
    let row = bounded(probe.observer.query_one(
        "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            i.lease_expires_at_us,c.lease_expires_at_us,c.state='active' AND i.state='active'
         FROM control.owner_epochs c CROSS JOIN control.owner_epochs i
         WHERE c.resource_id=$1::bytea AND c.epoch=$2::text::numeric AND c.process_incarnation=$3::bytea
           AND i.resource_id=$4::bytea AND i.epoch=2 AND i.process_incarnation=$5::bytea",
        &[&&control.resource_id.as_bytes()[..], &control.epoch.get().to_string(),
          &&control.process_incarnation.as_bytes()[..], &&target.owner_resource_id.as_bytes()[..],
          &&target.process_incarnation.as_bytes()[..]],
    )).await.unwrap();
    assert!(row.get::<_, bool>(3));
    Scheduling {
        now: row.get(0),
        ingest: row.get(1),
        control: row.get(2),
    }
}

async fn wait_expired(probe: &Probe, owner: &ControlSession, writer: &LiveInitialWriter) {
    let before = scheduling(probe, owner, writer).await;
    before.emit("before-wait", owner.identity().epoch.get());
    assert!(before.now < before.ingest && before.ingest < before.control);
    bounded(async {
        loop {
            let observed = scheduling(probe, owner, writer).await;
            assert_eq!(observed.ingest, before.ingest);
            assert_eq!(observed.control, before.control);
            assert!(observed.now < observed.control);
            if observed.now >= observed.ingest {
                observed.emit("expired", owner.identity().epoch.get());
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
}
