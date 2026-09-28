//! Actual rollback/late-delivery schedules. External proxy and SQL observations
//! determine fault placement; the product has no test hooks.
use super::*;
use bigbrotr::{
    acquisition::relay_source::RelaySourceBinding,
    network::{DirectConnector, TransportError},
    storage::{
        postgres::control_owner::{RelaySourceError, RelaySourceRequest},
        rocks::store::StoredVisibility,
    },
};
use std::{
    future::{Future, poll_fn},
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, sleep_until, timeout},
};

struct Received<'a> {
    bytes: [u8; 6],
    dropped: &'a Cell<bool>,
}
impl Drop for Received<'_> {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture borrows actual custody without duplicating source setup"
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
    let backend = observer.query_one(
        "SELECT control_backend_pid,control_backend_start::text FROM control.owner_epochs WHERE role='control' AND epoch=1", &[]
    ).await.unwrap();
    emit(
        serde_json::json!({"event":"cleanup-ready","pid":backend.get::<_,i32>(0),
        "backend_start":backend.get::<_,String>(1),"state":ingest_snapshot::state(observer,path).await}),
    );
    let gate = path.with_extension("go");
    let gate_deadline = Instant::now() + Duration::from_secs(20);
    while !gate.exists() {
        assert!(Instant::now() < gate_deadline, "cleanup gate expired");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let before = ingest_snapshot::state(observer, path).await;
    let late_ready = mode == "source-transport-cleanup-ready";
    let deadline_case = late_ready || mode == "source-transport-cleanup-timeout";
    let started = Instant::now();
    let deadline = started
        + if deadline_case {
            Duration::from_millis(750)
        } else {
            Duration::from_secs(5)
        };
    let returned = Cell::new(false);
    let dropped = Cell::new(false);
    let (result, ()) = {
        let receive = owner.run_relay_source(
            RelaySourceRequest {
                source,
                visibility,
                policy_ttl: Duration::from_secs(5),
                deadline,
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
                emit(serde_json::json!({"event":"cleanup-value","elapsed_us":started.elapsed().as_micros()}));
                Ok(Received { bytes, dropped: &dropped })
            },
        );
        tokio::pin!(receive);
        let resume = sleep_until(started + Duration::from_millis(1500));
        tokio::pin!(resume);
        // After callback completion the first poll starts actual rollback and yields.
        // Only subsequent polling of this source future pauses; its real PG driver
        // keeps receiving the released C and Z frames during this interval.
        let scheduled = poll_fn(|cx| {
            if late_ready && returned.get() && resume.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            receive.as_mut().poll(cx)
        });
        let peer = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 7];
            stream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"request");
            stream.write_all(b"answer").await.unwrap();
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
            emit(
                serde_json::json!({"event":"cleanup-peer-closed","elapsed_us":started.elapsed().as_micros()}),
            );
        };
        timeout(Duration::from_secs(6), async {
            tokio::join!(scheduled, peer)
        })
        .await
        .unwrap()
    };
    assert!(
        returned.get(),
        "must reach data delivery before cleanup fault"
    );
    let elapsed_us = started.elapsed().as_micros();
    let expected = match mode {
        "source-transport-cleanup-pass" => None,
        "source-transport-cleanup-expired" => {
            Some(RelaySourceError::Control(OwnerError::Unavailable))
        }
        "source-transport-cleanup-timeout" | "source-transport-cleanup-ready" => {
            Some(RelaySourceError::Transport(TransportError::Deadline))
        }
        _ => panic!("unexpected cleanup mode"),
    };
    match result {
        Ok(value) => {
            assert_eq!(expected, None);
            assert_eq!(&value.bytes, b"answer");
            drop(value);
        }
        Err(error) => assert_eq!(Some(error), expected),
    }
    assert!(dropped.get(), "received data must be disposed on refusal");
    if mode == "source-transport-cleanup-timeout" {
        assert_eq!(
            owner.poll_policy(|d, h| validate_version(d, h, true)).await,
            Err(OwnerError::Unavailable)
        );
    }
    let after = ingest_snapshot::state(observer, path).await;
    assert_eq!(after, before);
    emit(
        serde_json::json!({"event":"cleanup-finished","mode":mode,"elapsed_us":elapsed_us,"disposed":dropped.get(),"state":after}),
    );
    let done = path.with_extension("done");
    let end = Instant::now() + Duration::from_secs(10);
    while !done.exists() {
        assert!(Instant::now() < end, "cleanup retirement gate expired");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
