use super::support::{self, Connection};
use bigbrotr::{
    network::{ReceivedMessage, RelayWebSocket, TransportError, WebSocketError},
    runtime::ResourceLimits,
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

async fn closed(stream: &mut tokio::net::TcpStream) {
    match timeout(Duration::from_secs(1), stream.read(&mut [0; 1]))
        .await
        .unwrap()
    {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("expected socket closure without application output: {other:?}"),
    }
}

#[tokio::test]
async fn cancelling_partial_receive_poison_reuse_and_releases_buffers() {
    for partial in [&[0x81][..], &[0x81, 5, b'a'][..], &[0x01, 1, b'a'][..]] {
        let Connection {
            listener,
            resources,
            connector,
            attempt,
            cancel: _cancel,
        } = support::connection().await;
        let address = listener.local_addr().unwrap();
        let peer = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let response = support::handshake(&mut stream, address).await;
            stream
                .write_all(&[response.as_bytes(), partial].concat())
                .await
                .unwrap();
            closed(&mut stream).await;
        };
        let consumer = connector.run(attempt, async |stream| {
            let mut socket = RelayWebSocket::connect(stream, &resources, 32)
                .await
                .unwrap();
            assert_eq!(resources.snapshot().active_permits(), 2);
            assert!(
                timeout(Duration::from_millis(30), socket.receive())
                    .await
                    .is_err()
            );
            assert!(matches!(
                socket.receive().await,
                Err(WebSocketError::Unavailable)
            ));
            assert_eq!(
                socket.send_text("forbidden").await,
                Err(WebSocketError::Unavailable)
            );
            drop(socket);
            assert_eq!(resources.snapshot().active_permits(), 1);
            Ok(())
        });
        let (result, ()) = tokio::join!(consumer, peer);
        result.unwrap();
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn revocation_is_checked_inside_the_protocol_callback_without_yield() {
    for cancel_handle in [false, true] {
        let Connection {
            listener,
            resources,
            connector,
            attempt,
            cancel,
        } = support::connection().await;
        let address = listener.local_addr().unwrap();
        let live = AtomicBool::new(true);
        let owner_live = || live.load(Ordering::Acquire);
        let attempt = attempt.with_liveness_check(&owner_live);
        let peer = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let response = support::handshake(&mut stream, address).await;
            stream
                .write_all(&[response.as_bytes(), &[0x81, 1, b'A', 0x81, 1, b'B']].concat())
                .await
                .unwrap();
            closed(&mut stream).await;
        };
        let mut observed = false;
        let consumer = connector.run(attempt, async |stream| {
            let mut socket = RelayWebSocket::connect(stream, &resources, 32)
                .await
                .unwrap();
            assert!(matches!(
                socket.receive().await.unwrap(),
                Some(ReceivedMessage::Text("A"))
            ));
            let retained_cancel = if cancel_handle {
                drop(cancel);
                None
            } else {
                live.store(false, Ordering::Release);
                Some(cancel)
            };
            // No callback return or timer tick may substitute for protocol checks.
            assert!(matches!(
                socket.receive().await,
                Err(WebSocketError::Unavailable)
            ));
            live.store(true, Ordering::Release);
            assert_eq!(
                socket.send_text("forbidden").await,
                Err(WebSocketError::Unavailable)
            );
            observed = true;
            drop(retained_cancel);
            Ok(())
        });
        let (result, ()) = tokio::join!(consumer, peer);
        assert_eq!(
            result,
            Err(if cancel_handle {
                TransportError::Cancelled
            } else {
                TransportError::Authority
            })
        );
        assert!(observed);
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test]
async fn protocol_budget_refusal_precedes_http_and_retains_socket_owner() {
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        closed(&mut stream).await;
    };
    let consumer = connector.run(attempt, async |stream| {
        // Lower after literal-address resolution and socket admission; otherwise
        // the DNS reservation refuses before this protocol boundary is reached.
        resources
            .set_limits(ResourceLimits::new(4 * 1024 * 1024, 1, 0, 0))
            .unwrap();
        assert!(matches!(
            RelayWebSocket::connect(stream, &resources, 32).await,
            Err(WebSocketError::Budget)
        ));
        assert_eq!(resources.snapshot().active_permits(), 1);
        Ok(())
    });
    let (result, ()) = timeout(Duration::from_secs(1), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn outer_cancellation_closes_partial_handshake_and_partial_frame() {
    for during_handshake in [false, true] {
        let Connection {
            listener,
            resources,
            connector,
            attempt,
            cancel,
        } = support::connection().await;
        let address = listener.local_addr().unwrap();
        let (ready, entered) = tokio::sync::oneshot::channel();
        let (opened, upgraded) = tokio::sync::oneshot::channel();
        let peer = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let response = support::handshake(&mut stream, address).await;
            if during_handshake {
                stream.write_all(b"HTTP/1.1 101 Switching").await.unwrap();
            } else {
                stream
                    .write_all(&[response.as_bytes(), &[0x81, 5, b'a']].concat())
                    .await
                    .unwrap();
                upgraded.await.unwrap();
            }
            ready.send(()).unwrap();
            closed(&mut stream).await;
        };
        let cancellation = async {
            entered.await.unwrap();
            drop(cancel);
        };
        let consumer = connector.run(attempt, async |stream| {
            let mut socket = RelayWebSocket::connect(stream, &resources, 32)
                .await
                .unwrap();
            opened.send(()).unwrap();
            let _ = socket.receive().await;
            panic!("partial protocol work returned before outer cancellation")
        });
        let (result, (), ()) = timeout(Duration::from_secs(1), async {
            tokio::join!(consumer, peer, cancellation)
        })
        .await
        .unwrap();
        assert_eq!(result, Err::<(), _>(TransportError::Cancelled));
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test]
async fn cancelling_actual_blocked_send_retains_budget_and_forbids_reuse() {
    const CAP: usize = 2 * 1024 * 1024;
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection_with_receive_buffer(Some(16 * 1024)).await;
    let address = listener.local_addr().unwrap();
    let (drain, blocked) = tokio::sync::oneshot::channel();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        // No application reads until the producer has actually returned Pending
        // through the timeout and dropped that send future.
        let completed = blocked.await.unwrap();
        let mut bytes = 0;
        let mut buffer = [0; 16384];
        loop {
            let count = timeout(Duration::from_secs(1), stream.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            if count == 0 {
                break;
            }
            bytes += count;
            assert!(bytes <= 16 * CAP);
        }
        assert!(bytes >= completed * CAP);
        assert!(
            bytes < (completed + 1) * CAP,
            "pending send was fully delivered"
        );
        eprintln!("blocked-send: completed_messages={completed}, wire_bytes={bytes}, cap={CAP}");
    };
    let consumer = connector.run(attempt, async |stream| {
        let mut socket = RelayWebSocket::connect(stream, &resources, CAP)
            .await
            .unwrap();
        let payload = "x".repeat(CAP - 14);
        let mut completed = 0;
        loop {
            assert!(
                completed < 16,
                "fixture did not establish socket backpressure"
            );
            match timeout(Duration::from_millis(30), socket.send_text(&payload)).await {
                Ok(result) => {
                    result.unwrap();
                    completed += 1;
                }
                Err(_) => break,
            }
        }
        assert_eq!(resources.snapshot().active_permits(), 2);
        assert_eq!(
            resources.snapshot().used().data_bytes(),
            4 * 1024 * 1024 + 8 * CAP as u64 + 1024 * 1024
        );
        assert_eq!(
            socket.send_text("forbidden").await,
            Err(WebSocketError::Unavailable)
        );
        drop(socket);
        assert_eq!(resources.snapshot().active_permits(), 1);
        drain.send(completed).unwrap();
        Ok(())
    });
    let (result, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}
