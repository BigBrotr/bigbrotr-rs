//! Real owned circuits for diagnostic capture; no scientific dispatch authority.
use super::support::{self, Connection};
use bigbrotr::network::{
    ConnectSubmission, FirstIoTarget, ObservedSessionLimit, ObservedStop, ReceivedMessage,
    RelayWebSocket, TransportError, TransportTrace, WebSocketError,
};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

async fn client_frame(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let first = stream.read_u8().await.unwrap();
    let second = stream.read_u8().await.unwrap();
    assert_ne!(second & 128, 0);
    assert!(second & 127 < 126);
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).await.unwrap();
    let mut value = vec![0; usize::from(second & 127)];
    stream.read_exact(&mut value).await.unwrap();
    for (n, byte) in value.iter_mut().enumerate() {
        *byte ^= mask[n % 4];
    }
    (first, value)
}

#[tokio::test]
async fn observed_literals_keep_decoder_anchors_and_all_control_teardown_bytes() {
    const REQUEST: &str = r#"["REQ","probe",{"kinds":[1]}]"#;
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let trace = TransportTrace::new();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        // Frames are sent before reading REQ. Their later decoder-delivery
        // anchors must never be described as kernel/network arrival times.
        stream
            .write_all(
                &[
                    response.as_bytes(),
                    &[0x89, 1, b'p', 0x01, 1, b'A', 0x80, 1, b'B', 0x82, 1, b'X'],
                ]
                .concat(),
            )
            .await
            .unwrap();
        assert_eq!(
            client_frame(&mut stream).await,
            (0x81, REQUEST.as_bytes().to_vec())
        );
        assert_eq!(client_frame(&mut stream).await, (0x8a, b"p".to_vec()));
        assert_eq!(client_frame(&mut stream).await, (0x88, vec![]));
        stream.write_all(&[0x88, 0]).await.unwrap();
        stream.shutdown().await.unwrap();
    };
    let consumer = connector.run_observed(attempt, &trace, async |stream| {
        let mut socket = RelayWebSocket::connect_observed(stream, &resources).await?;
        assert!(trace.snapshot().first_message_delivered.is_none());
        socket.send_text(REQUEST).await?;
        assert!(matches!(
            socket.receive().await?,
            Some(ReceivedMessage::Ping(b"p"))
        ));
        assert!(matches!(
            socket.receive().await?,
            Some(ReceivedMessage::Text("AB"))
        ));
        assert!(matches!(
            socket.receive().await?,
            Some(ReceivedMessage::Binary(b"X"))
        ));
        socket.close().await?;
        Ok(())
    });
    let (result, ()) = timeout(Duration::from_secs(2), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    result.unwrap();
    let captured = trace.snapshot();
    let first = captured.first_io.unwrap();
    assert_eq!(first.target, FirstIoTarget::RelayConnect);
    assert!(matches!(
        first.submission,
        ConnectSubmission::Connected | ConnectSubmission::InProgress
    ));
    assert_eq!(captured.resolver_connect_calls, 0);
    assert_eq!(captured.relay_connect_calls, 1);
    assert!(captured.tls_first_lower_read.is_none() && captured.tls_first_lower_write.is_none());
    assert!(
        captured.websocket_upgrade_validated.unwrap().monotonic
            <= captured.first_text_write_flushed.unwrap().monotonic
    );
    assert!(
        captured.first_text_write_flushed.unwrap().monotonic
            <= captured.first_message_delivered.unwrap().monotonic
    );
    let ws = captured.websocket;
    assert_eq!(
        (
            ws.rx_physical_frames_started,
            ws.rx_complete_headers,
            ws.rx_logical_messages_delivered
        ),
        (5, 5, 4)
    );
    assert_eq!(ws.rx_declared_ws_framed_bytes, Some(14));
    assert_eq!(
        (
            ws.rx_ws_framed_bytes_read,
            ws.rx_ws_framed_bytes_consumed,
            ws.rx_ws_framed_bytes_forwarded
        ),
        (14, 14, 14)
    );
    assert_eq!(
        ws.tx_ws_framed_bytes_written,
        REQUEST.len() as u64 + 6 + 7 + 6
    );
    assert!(captured.stop.is_none());
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn observed_logical_cap_includes_unsolicited_controls_before_parse() {
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let trace = TransportTrace::new();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream
            .write_all(&[response.as_bytes(), &[0x8a, 0].repeat(33)].concat())
            .await
            .unwrap();
        let closed = stream.read(&mut [0; 1]).await;
        assert!(
            matches!(closed, Ok(0))
                || matches!(closed,Err(error) if error.kind()==io::ErrorKind::ConnectionReset)
        );
    };
    let consumer = connector.run_observed(attempt, &trace, async |stream| {
        let mut socket = RelayWebSocket::connect_observed(stream, &resources).await?;
        for _ in 0..32 {
            assert!(matches!(
                socket.receive().await?,
                Some(ReceivedMessage::Pong(b""))
            ));
        }
        assert!(matches!(
            socket.receive().await,
            Err(WebSocketError::SessionLimit)
        ));
        Ok(())
    });
    let (result, ()) = timeout(Duration::from_secs(2), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    result.unwrap();
    let captured = trace.snapshot();
    assert_eq!(captured.websocket.rx_logical_messages_delivered, 32);
    assert_eq!(captured.websocket.rx_physical_frames_started, 32);
    assert_eq!(captured.websocket.rx_ws_framed_bytes_forwarded, 64);
    assert!(captured.websocket.rx_ws_framed_bytes_read <= 66);
    assert_eq!(
        captured.stop,
        Some(ObservedStop::Session(
            ObservedSessionLimit::RxLogicalMessages
        ))
    );
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn observed_rx_cap_does_not_read_a_later_header_as_received_budget() {
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let trace = TransportTrace::new();
    let payload = 2 * 1024 * 1024 - 10;
    let mut frames = Vec::new();
    for _ in 0..2 {
        frames.extend([0x82, 127]);
        frames.extend((payload as u64).to_be_bytes());
        frames.extend(vec![b'x'; payload]);
    }
    frames.extend([0x8a, 0]);
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        let sent = stream.write_all(&frames).await;
        match sent {
            Ok(()) => {
                let closed = stream.read(&mut [0; 1]).await;
                assert!(
                    matches!(closed, Ok(0))
                        || matches!(closed,Err(error) if error.kind()==io::ErrorKind::ConnectionReset)
                );
            }
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            )),
        }
    };
    let consumer = connector.run_observed(attempt,&trace,async |stream| {
        let mut socket = RelayWebSocket::connect_observed(stream,&resources).await?;
        for _ in 0..2 {
            assert!(matches!(socket.receive().await?,Some(ReceivedMessage::Binary(value)) if value.len()==2097142 && value.iter().all(|byte| *byte==b'x')));
        }
        assert!(matches!(socket.receive().await,Err(WebSocketError::SessionLimit)));
        Ok(())
    });
    let (result, ()) = timeout(Duration::from_secs(4), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    result.unwrap();
    let captured = trace.snapshot();
    assert_eq!(
        (
            captured.websocket.rx_physical_frames_started,
            captured.websocket.rx_complete_headers,
            captured.websocket.rx_logical_messages_delivered
        ),
        (2, 2, 2)
    );
    assert_eq!(
        captured.websocket.rx_declared_ws_framed_bytes,
        Some(4194304)
    );
    assert_eq!(
        (
            captured.websocket.rx_ws_framed_bytes_read,
            captured.websocket.rx_ws_framed_bytes_consumed,
            captured.websocket.rx_ws_framed_bytes_forwarded
        ),
        (4194304, 4194304, 4194304)
    );
    assert_eq!(
        captured.stop,
        Some(ObservedStop::Session(ObservedSessionLimit::RxWsFramedBytes))
    );
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn first_syscall_capture_survives_stalled_future_drop_and_cancellation() {
    for cancel_now in [false, true] {
        let Connection {
            listener,
            resources,
            connector,
            attempt,
            cancel,
        } = support::connection().await;
        let trace = TransportTrace::new();
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let peer = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            ready.send(()).unwrap();
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
        };
        let control = async {
            let future = connector.run_observed(attempt, &trace, async |_stream| {
                std::future::pending::<io::Result<()>>().await
            });
            let mut future = Box::pin(future);
            tokio::select! { _ = waiting => {}, result = &mut future => panic!("stalled operation completed: {result:?}") }
            assert!(trace.snapshot().first_io.is_some());
            if cancel_now {
                drop(cancel);
                assert!(matches!(
                    (&mut future).await,
                    Err(TransportError::Cancelled)
                ));
            }
            drop(future);
        };
        timeout(Duration::from_secs(2), async {
            tokio::join!(control, peer)
        })
        .await
        .unwrap();
        let captured = trace.snapshot();
        assert_eq!(
            captured.first_io.unwrap().target,
            FirstIoTarget::RelayConnect
        );
        assert_eq!(
            captured.stop,
            if cancel_now {
                Some(ObservedStop::Cancelled)
            } else {
                None
            }
        );
        assert!(
            captured.websocket_upgrade_validated.is_none()
                && captured.first_message_delivered.is_none()
        );
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test]
async fn observed_tls_stall_has_lower_wire_proof_after_future_construction() {
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::tls_connection().await;
    let trace = TransportTrace::new();
    let (hello, seen) = tokio::sync::oneshot::channel();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(stream.read_u8().await.unwrap(), 0x16);
        hello.send(()).unwrap();
        let mut chunk = [0; 4096];
        while stream.read(&mut chunk).await.unwrap() != 0 {}
    };
    let control = async {
        let mut future = Box::pin(connector.run_observed(attempt, &trace, async |_stream| Ok(())));
        assert!(trace.snapshot().tls_first_lower_write.is_none());
        tokio::select! { _ = seen => {}, result = &mut future => panic!("TLS stall completed: {result:?}") }
        assert!(trace.snapshot().tls_first_lower_write.is_some());
        assert!(trace.snapshot().tls_completed.is_none());
        drop(future);
    };
    timeout(Duration::from_secs(2), async {
        tokio::join!(control, peer)
    })
    .await
    .unwrap();
    assert!(trace.snapshot().tls_first_lower_write.is_some());
    assert_eq!(resources.snapshot().active_permits(), 0);
}
