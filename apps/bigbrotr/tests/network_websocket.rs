#[path = "network_websocket/lifetime.rs"]
mod lifetime;
#[path = "network_websocket/nostr.rs"]
mod nostr;
#[path = "network_websocket/support.rs"]
mod support;
#[path = "network_websocket/tls.rs"]
mod tls;

use bigbrotr::network::{ReceivedMessage, RelayWebSocket, WebSocketError};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

async fn receive(frames: &[u8], cap: usize) -> Result<String, WebSocketError> {
    exchange(frames, cap, Operation::Receive).await
}

#[derive(Clone, Copy)]
enum Operation<'a> {
    Receive,
    Binary {
        expected: &'a [u8],
    },
    Close {
        answer: bool,
    },
    Control {
        opcode: u8,
        reply: &'a [u8],
        then_text: bool,
    },
    Send {
        payload: &'a str,
        expected: bool,
    },
}

async fn exchange(
    frames: &[u8],
    cap: usize,
    operation: Operation<'_>,
) -> Result<String, WebSocketError> {
    exchange_response(frames, cap, operation, |response| response).await
}

async fn exchange_response(
    frames: &[u8],
    cap: usize,
    operation: Operation<'_>,
    response_edit: impl FnOnce(String) -> String,
) -> Result<String, WebSocketError> {
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let (acknowledge, acknowledged) = tokio::sync::oneshot::channel();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = response_edit(support::handshake(&mut stream, address).await);
        // One write permits coalescing; it does not assert how TCP splits reads.
        stream
            .write_all(&[response.as_bytes(), frames].concat())
            .await
            .unwrap();
        if let Operation::Close { answer } = operation {
            let mut close = [0; 6];
            stream.read_exact(&mut close).await.unwrap();
            assert_eq!(&close[..2], &[0x88, 0x80]);
            if answer {
                // Four incoming bytes fit cap6; the client already sent its
                // empty six-byte Close and must not send an eight-byte echo.
                stream.write_all(&[0x88, 2, 3, 232]).await.unwrap();
            }
            stream.shutdown().await.unwrap();
        }
        if let Operation::Control { opcode, reply, .. } = operation {
            let mut header = [0; 6];
            timeout(Duration::from_millis(500), stream.read_exact(&mut header))
                .await
                .unwrap()
                .unwrap();
            let reply_opcode = if opcode == 9 { 10 } else { 8 };
            let payload = reply;
            assert_eq!(
                &header[..2],
                &[0x80 | reply_opcode, 0x80 | payload.len() as u8]
            );
            let mut received = vec![0; payload.len()];
            stream.read_exact(&mut received).await.unwrap();
            for (index, byte) in received.iter_mut().enumerate() {
                *byte ^= header[2 + index % 4];
            }
            assert_eq!(received, payload);
            acknowledge.send(()).unwrap();
            if opcode == 8 {
                stream.shutdown().await.unwrap();
            }
        }
        if let Operation::Send {
            payload,
            expected: true,
        } = operation
        {
            assert_eq!(stream.read_u8().await.unwrap(), 0x81);
            let (tag, extended) = if payload.len() < 126 {
                (payload.len() as u8, 0)
            } else if payload.len() < 65536 {
                (126, 2)
            } else {
                (127, 8)
            };
            assert_eq!(stream.read_u8().await.unwrap(), 0x80 | tag);
            match extended {
                2 => assert_eq!(stream.read_u16().await.unwrap() as usize, payload.len()),
                8 => assert_eq!(stream.read_u64().await.unwrap() as usize, payload.len()),
                _ => {}
            }
            let mut mask = [0; 4];
            stream.read_exact(&mut mask).await.unwrap();
            let mut data = vec![0; payload.len()];
            stream.read_exact(&mut data).await.unwrap();
            for (index, byte) in data.iter_mut().enumerate() {
                *byte ^= mask[index % 4];
            }
            assert_eq!(data, payload.as_bytes());
        }
        match timeout(Duration::from_secs(2), stream.read(&mut [0; 1]))
            .await
            .unwrap()
        {
            Ok(0) => {}
            // Refusing a malformed/oversized response may leave unread TCP
            // bytes. A reset is closure; a completed Close still requires EOF.
            Err(error)
                if matches!(operation, Operation::Receive)
                    && error.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => panic!("expected peer closure, got {result:?}"),
        }
    };
    let consumer = connector.run(attempt, async |stream| {
        let result = async {
            let mut socket = RelayWebSocket::connect(stream, &resources, cap).await?;
            if let Operation::Binary { expected } = operation {
                assert!(matches!(socket.receive().await?, Some(ReceivedMessage::Binary(bytes)) if bytes == expected));
                return Ok("binary".to_owned());
            }
            if let Operation::Close { .. } = operation {
                socket.close().await?;
                return Ok("closed".to_owned());
            }
            if let Operation::Send { payload, .. } = operation {
                socket.send_text(payload).await?;
                return Ok("sent".to_owned());
            }
            if let Operation::Control {
                opcode,
                reply,
                then_text,
            } = operation
            {
                match (opcode, socket.receive().await?) {
                    (9, Some(ReceivedMessage::Ping(bytes))) => assert_eq!(bytes, b"p"),
                    (8, Some(ReceivedMessage::Close { code, reason })) => {
                        assert_eq!(
                            code,
                            if reply.is_empty() {
                                None
                            } else {
                                Some(u16::from_be_bytes([reply[0], reply[1]]))
                            }
                        );
                        assert_eq!(
                            reason.as_bytes(),
                            if reply.is_empty() { &[] } else { &reply[2..] }
                        );
                    }
                    _ => panic!("wrong control outcome"),
                }
                // Do not poll receive again until the independent peer observes
                // the reply. Otherwise the next upstream read hides missing flush.
                timeout(Duration::from_millis(500), acknowledged)
                    .await
                    .unwrap()
                    .unwrap();
                if opcode == 8 {
                    assert!(socket.receive().await?.is_none());
                }
                if !then_text {
                    return Ok("control observed".to_owned());
                }
            }
            match socket.receive().await? {
                Some(ReceivedMessage::Text(text)) => Ok(text.to_owned()),
                _ => panic!("expected a text message or a bounded refusal"),
            }
        }
        .await;
        Ok(result)
    });
    let (result, ()) = tokio::join!(consumer, peer);
    assert_eq!(resources.snapshot().active_permits(), 0);
    result.unwrap()
}

#[tokio::test]
async fn outgoing_close_completes_only_after_peer_handshake() {
    assert_eq!(
        exchange(&[], 6, Operation::Close { answer: true })
            .await
            .unwrap(),
        "closed"
    );
    assert_eq!(
        exchange(&[], 6, Operation::Close { answer: false }).await,
        Err(WebSocketError::Protocol)
    );
}

#[tokio::test]
async fn handshake_lf_only_keeps_the_same_frame_boundary() {
    assert_eq!(
        exchange_response(&[0x81, 2, b'o', b'k'], 6, Operation::Receive, |response| {
            response.replace("\r\n", "\n")
        })
        .await
        .unwrap(),
        "ok"
    );
    // A CRLF-only preflight would consume the delimiter inside this frame as
    // HTTP and expose the over-limit frame through the upstream handshake tail.
    assert_eq!(
        exchange_response(
            &[0x81, 6, b'\r', b'\n', b'\r', b'\n', b'x', b'x'],
            6,
            Operation::Receive,
            |response| response.replace("\r\n", "\n")
        )
        .await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn handshake_refuses_unsolicited_and_duplicate_negotiation_headers() {
    for header in [
        "Sec-WebSocket-Extensions: permessage-deflate",
        "Sec-WebSocket-Extensions:",
        "Sec-WebSocket-Protocol: nostr",
        "Sec-WebSocket-Protocol:",
        "Upgrade: websocket",
        "Connection: Upgrade",
    ] {
        assert_eq!(
            exchange_response(&[], 16, Operation::Receive, |response| {
                response.replace("\r\n\r\n", &format!("\r\n{header}\r\n\r\n"))
            })
            .await,
            Err(WebSocketError::Handshake),
            "{header}"
        );
    }
    for duplicate in [false, true] {
        assert_eq!(
            exchange_response(&[], 16, Operation::Receive, |response| {
                let accept = response
                    .lines()
                    .find(|line| line.starts_with("Sec-WebSocket-Accept:"))
                    .unwrap();
                if duplicate {
                    response.replace("\r\n\r\n", &format!("\r\n{accept}\r\n\r\n"))
                } else {
                    response.replace(accept, "Sec-WebSocket-Accept: invalid")
                }
            })
            .await,
            Err(WebSocketError::Handshake)
        );
    }
}

#[tokio::test]
async fn handshake_bounds_exact_http_bytes() {
    for (length, expected) in [
        (65536, Ok("ok".to_owned())),
        (65537, Err(WebSocketError::Handshake)),
    ] {
        assert_eq!(
            exchange_response(&[0x81, 2, b'o', b'k'], 16, Operation::Receive, |response| {
                let padding = "x".repeat(length - response.len() - "X-Pad: \r\n".len());
                let response =
                    response.replace("\r\n\r\n", &format!("\r\nX-Pad: {padding}\r\n\r\n"));
                assert_eq!(response.len(), length);
                response
            })
            .await,
            expected
        );
    }
}

#[tokio::test]
async fn handshake_bounds_header_count() {
    for (extra, expected) in [
        (121, Ok("ok".to_owned())),
        (122, Err(WebSocketError::Handshake)),
    ] {
        assert_eq!(
            exchange_response(&[0x81, 2, b'o', b'k'], 16, Operation::Receive, |response| {
                let headers = "X-Extra: x\r\n".repeat(extra);
                response.replace("\r\n\r\n", &format!("\r\n{headers}\r\n"))
            })
            .await,
            expected
        );
    }
}

#[tokio::test]
async fn handshake_does_not_follow_redirects() {
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = second.local_addr().unwrap();
    assert_eq!(exchange_response(&[], 16, Operation::Receive, |_| {
        format!("HTTP/1.1 302 Found\r\nLocation: ws://{address}/redirect\r\nContent-Length: 0\r\n\r\n")
    }).await, Err(WebSocketError::Handshake));
    assert!(
        timeout(Duration::from_millis(100), second.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn accepts_exact_framed_message_and_preserves_request_identity() {
    let frames = [vec![0x81, 14], vec![b'x'; 14]].concat();
    assert_eq!(receive(&frames, 16).await.unwrap(), "x".repeat(14));
}

#[tokio::test]
async fn fragmented_binary_preserves_non_utf8_bytes() {
    assert_eq!(
        exchange(
            &[0x02, 1, 0xff, 0x80, 2, 0, 0xfe],
            7,
            Operation::Binary {
                expected: &[0xff, 0, 0xfe]
            }
        )
        .await
        .unwrap(),
        "binary"
    );
}

#[tokio::test]
async fn selected_two_mib_cap_accepts_exact_wire_size_and_refuses_header_only_overflow() {
    const CAP: usize = 2 * 1024 * 1024;
    let frames = [
        vec![0x81, 127],
        ((CAP - 10) as u64).to_be_bytes().to_vec(),
        vec![b'x'; CAP - 10],
    ]
    .concat();
    assert_eq!(receive(&frames, CAP).await.unwrap(), "x".repeat(CAP - 10));
    let overflow = [vec![0x81, 127], ((CAP - 9) as u64).to_be_bytes().to_vec()].concat();
    assert_eq!(
        receive(&overflow, CAP).await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn rejects_payload_that_fits_only_when_header_is_ignored() {
    let frames = [vec![0x81, 16], vec![b'x'; 16]].concat();
    assert_eq!(
        receive(&frames, 16).await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn empty_continuations_consume_message_budget() {
    let frames = [vec![0x01, 0], [0, 0].repeat(7), vec![0x80, 0]].concat();
    assert_eq!(
        receive(&frames, 16).await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn rejects_declared_oversize_without_waiting_for_payload() {
    // Header declares65536 bytes. The peer supplies no payload and waits for EOF.
    let frames = [0x81, 127, 0, 0, 0, 0, 0, 1, 0, 0];
    assert_eq!(
        receive(&frames, 16).await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn counts_received_nonminimal_extended_header() {
    let frames = [vec![0x81, 126, 0, 14], vec![b'x'; 14]].concat();
    assert_eq!(
        receive(&frames, 16).await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn control_ping_is_flushed_before_consumer_resumes() {
    assert_eq!(
        exchange(
            &[0x89, 1, b'p'],
            16,
            Operation::Control {
                opcode: 9,
                reply: b"p",
                then_text: false
            }
        )
        .await
        .unwrap(),
        "control observed"
    );
}

#[tokio::test]
async fn control_close_is_flushed_then_peer_eof_completes() {
    assert_eq!(
        exchange(
            &[0x88, 2, 3, 232],
            16,
            Operation::Control {
                opcode: 8,
                reply: &[3, 232],
                then_text: false
            }
        )
        .await
        .unwrap(),
        "control observed"
    );
}

#[tokio::test]
async fn control_automatic_pong_includes_client_mask_in_limit() {
    let frames = [vec![0x89, 14], vec![b'p'; 14]].concat();
    assert_eq!(
        receive(&frames, 16).await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn control_invalid_close_is_not_rewritten_as_remote_protocol_close() {
    // 1005 is reserved for local absence of a close code, never valid on wire.
    assert_eq!(
        receive(&[0x88, 2, 3, 237], 64).await,
        Err(WebSocketError::Protocol)
    );
}

#[tokio::test]
async fn outgoing_exact_limit_includes_mask_and_header() {
    for (length, cap) in [(10, 16), (125, 131), (126, 134), (65536, 65550)] {
        let payload = "x".repeat(length);
        assert_eq!(
            exchange(
                &[],
                cap,
                Operation::Send {
                    payload: &payload,
                    expected: true
                }
            )
            .await
            .unwrap(),
            "sent"
        );
    }
}

#[tokio::test]
async fn outgoing_over_limit_sends_no_frame() {
    for (length, cap) in [(11, 16), (125, 130), (126, 133), (65536, 65549)] {
        let payload = "x".repeat(length);
        assert_eq!(
            exchange(
                &[],
                cap,
                Operation::Send {
                    payload: &payload,
                    expected: false
                }
            )
            .await,
            Err(WebSocketError::MessageLimit)
        );
    }
}

#[tokio::test]
async fn control_close_preserves_original_code_and_reason() {
    for reply in [&b""[..], &[3, 232, b'o', b'k'][..]] {
        let frames = [&[0x88, reply.len() as u8][..], reply].concat();
        assert_eq!(
            exchange(
                &frames,
                32,
                Operation::Control {
                    opcode: 8,
                    reply,
                    then_text: false
                }
            )
            .await
            .unwrap(),
            "control observed"
        );
    }
}

#[tokio::test]
async fn control_ping_does_not_reset_fragmented_message_budget() {
    // Data consumes12+5=17 bytes; the independent ping is not a new data message.
    let frames = [
        vec![0x01, 10],
        vec![b'x'; 10],
        vec![0x89, 1, b'p', 0x80, 3], // The over-budget continuation supplies no body.,
    ]
    .concat();
    assert_eq!(
        exchange(
            &frames,
            16,
            Operation::Control {
                opcode: 9,
                reply: b"p",
                then_text: true
            }
        )
        .await,
        Err(WebSocketError::MessageLimit)
    );
}

#[tokio::test]
async fn malformed_frames_refuse_before_becoming_messages() {
    for frame in [
        &[0x81, 0x80, 1, 2, 3, 4][..], // Masked server.
        &[0xc1, 0][..],                // Unsolicited RSV1.
        &[0x80, 0][..],                // Continuation without a data message.
        &[0x01, 0, 0x81, 0][..],       // New data message before final continuation.
        &[0x09, 0][..],                // Fragmented control.
        &[0x89, 126, 0, 126][..],      // Oversized control, no payload.
        &[0x88, 1][..],                // Invalid close length, no payload.
        &[0x81, 1, 0xff][..],          // Invalid text UTF8.
        &[0x88, 3, 3, 232, 0xff][..],  // Invalid close reason UTF8.
    ] {
        assert_eq!(
            receive(frame, 256).await,
            Err(WebSocketError::Protocol),
            "frame {frame:?}"
        );
    }
}
