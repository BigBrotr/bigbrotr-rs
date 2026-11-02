use super::*;
use crate::{
    network::{
        CancelAttempt,
        lifetime::Gate,
        stream::{Guarded, Wire},
    },
    runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
};
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use bigbrotr_protocol::endpoint::Endpoint;
use futures_util::Stream;
use std::{future::poll_fn, task::Waker, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

async fn streams() -> (DirectStream<'static>, TcpStream, CancelAttempt) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (socket, peer) = tokio::join!(TcpStream::connect(address), listener.accept());
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let incarnation = ProcessIncarnation::try_from([2; 16]).unwrap();
    let epoch = PolicyEpoch::new(1);
    let mut tracker = PolicyLeaseTracker::new(
        dataset,
        incarnation,
        epoch,
        PolicyLeaseTtl::new(Duration::from_secs(5)).unwrap(),
    )
    .unwrap();
    let ticket = tracker.begin_refresh().unwrap();
    tracker
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset,
                process_incarnation: incarnation,
                policy_epoch: epoch,
                ttl: Duration::from_secs(5),
            },
        )
        .unwrap();
    let window = tracker
        .begin_action(epoch)
        .unwrap()
        .complete(Duration::from_secs(5))
        .unwrap();
    let (gate, cancel) = Gate::new(window, Instant::now() + Duration::from_secs(5));
    let stream = DirectStream {
        inner: Guarded {
            socket: Wire::Plain(Guarded {
                socket: socket.unwrap(),
                gate: gate.clone(),
                capture_tls_io: false,
            }),
            gate,
            capture_tls_io: false,
        },
        endpoint: Endpoint::parse(&format!("ws://{address}/")).unwrap(),
        peer: address,
    };
    (stream, peer.unwrap().0, cancel)
}

// These tests inspect retained input directly to prove buffering actually occurred.
// Integration fixtures separately exercise the authenticated HTTP handshake.
const RESPONSE: &[u8] =
    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";

#[tokio::test]
async fn actual_http_tail_stays_owned_and_revocation_refuses_it() {
    let (mut stream, mut peer, cancel) = streams().await;
    let frames = [0x81, 1, b'A', 0x81, 1, b'B'];
    peer.write_all(&[RESPONSE, &frames].concat()).await.unwrap();
    let mut input = Input::new(&mut stream, 16);
    let mut header = vec![0; RESPONSE.len()];
    input.read_exact(&mut header).await.unwrap();
    assert_eq!(header, RESPONSE);
    assert_eq!(
        &input.bytes[input.position..],
        &frames,
        "fixture must observe the real coalesced tail"
    );
    input.enable();
    let mut first = [0; 3];
    input.read_exact(&mut first).await.unwrap();
    assert_eq!(first, [0x81, 1, b'A']);
    assert_eq!(&input.bytes[input.position..], &[0x81, 1, b'B']);
    drop(cancel);
    assert_eq!(
        input.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn fully_buffered_empty_fragments_yield_before_completion_and_observe_cancellation() {
    for revoke in [false, true] {
        let (mut stream, mut peer, cancel) = streams().await;
        let frames = [vec![0x01, 0], [0, 0].repeat(998), vec![0x80, 0]].concat();
        peer.write_all(&[RESPONSE, &frames].concat()).await.unwrap();
        let mut input = Input::new(&mut stream, frames.len());
        let mut header = vec![0; RESPONSE.len()];
        input.read_exact(&mut header).await.unwrap();
        assert_eq!(
            &input.bytes[input.position..],
            &frames,
            "all fragments must already be buffered"
        );
        input.enable();
        let mut socket = WebSocketStream::from_raw_socket(input, Role::Client, None).await;
        let mut context = Context::from_waker(Waker::noop());
        assert!(
            Pin::new(&mut socket).poll_next(&mut context).is_pending(),
            "ready frames monopolized one poll"
        );
        let retained_cancel = if revoke {
            drop(cancel);
            None
        } else {
            Some(cancel)
        };
        let message = poll_fn(|cx| Pin::new(&mut socket).poll_next(cx))
            .await
            .unwrap();
        if revoke {
            assert!(
                matches!(message, Err(tokio_tungstenite::tungstenite::Error::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied)
            );
        } else {
            assert!(matches!(message.unwrap(), Message::Text(text) if text.is_empty()));
        }
        drop(retained_cancel);
    }
}

#[tokio::test]
async fn observed_physical_frame_cap_counts_fragments_before_codec_forwarding() {
    use crate::network::{ObservedSessionLimit, ObservedStop, TransportTrace};
    for count in [64usize, 65] {
        let (mut stream, mut peer, _cancel) = streams().await;
        let mut frames = vec![0x01, 0];
        frames.extend([0, 0].repeat(count - 2));
        frames.extend([0x80, 0]);
        peer.write_all(&[RESPONSE, &frames].concat()).await.unwrap();
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let mut input = Input::with_observation(&mut stream, 2 * 1024 * 1024, Some(trace.clone()));
        input
            .read_exact(&mut vec![0; RESPONSE.len()])
            .await
            .unwrap();
        input.enable();
        let mut socket = WebSocketStream::from_raw_socket(input, Role::Client, None).await;
        let result = poll_fn(|cx| Pin::new(&mut socket).poll_next(cx))
            .await
            .unwrap();
        let captured = trace.snapshot();
        assert_eq!(captured.websocket.rx_physical_frames_started, count as u64);
        assert_eq!(captured.websocket.rx_complete_headers, 64);
        assert_eq!(captured.websocket.rx_ws_framed_bytes_forwarded, 128);
        assert_eq!(captured.websocket.rx_ws_payload_bytes_consumed, 0);
        assert!(
            captured.websocket.rx_ws_framed_bytes_read
                >= captured.websocket.rx_ws_framed_bytes_consumed
        );
        assert!(captured.websocket.rx_ws_framed_bytes_read <= frames.len() as u64);
        if count == 64 {
            assert!(matches!(result.unwrap(),Message::Text(text) if text.is_empty()));
            assert_eq!(captured.websocket.rx_ws_framed_bytes_consumed, 128);
            assert!(captured.stop.is_none());
        } else {
            assert!(result.is_err());
            assert_eq!(captured.websocket.rx_ws_framed_bytes_consumed, 129);
            assert_eq!(
                captured.stop,
                Some(ObservedStop::Session(
                    ObservedSessionLimit::RxPhysicalFrames
                ))
            );
        }
        drop(socket);
        drop(stream);
        let closed = tokio::time::timeout(Duration::from_secs(1), peer.read(&mut [0; 1]))
            .await
            .unwrap();
        assert!(
            matches!(closed, Ok(0))
                || matches!(closed,Err(error) if error.kind()==io::ErrorKind::ConnectionReset)
        );
    }
}

#[tokio::test]
async fn observed_huge_claim_and_rejected_control_never_become_received_payload() {
    use crate::network::{ObservedSessionLimit, ObservedStop, TransportTrace};
    let huge = 1u64 << 40;
    let mut claim = vec![0x82, 127];
    claim.extend(huge.to_be_bytes());
    for (frames, declared, consumed_payload) in [
        (claim, huge + 10, 0),
        (vec![0xc9, 0], 2, 0),
        (vec![0x8b, 0], 2, 0),
        (vec![0x88, 2, 3, 237], 4, 2),
    ] {
        let (mut stream, mut peer, _cancel) = streams().await;
        peer.write_all(&[RESPONSE, &frames].concat()).await.unwrap();
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let mut input = Input::with_observation(&mut stream, 2 * 1024 * 1024, Some(trace.clone()));
        input
            .read_exact(&mut vec![0; RESPONSE.len()])
            .await
            .unwrap();
        input.enable();
        let mut socket = WebSocketStream::from_raw_socket(input, Role::Client, None).await;
        assert!(
            poll_fn(|cx| Pin::new(&mut socket).poll_next(cx))
                .await
                .unwrap()
                .is_err()
        );
        let captured = trace.snapshot();
        assert_eq!(captured.websocket.rx_physical_frames_started, 1);
        assert_eq!(captured.websocket.rx_complete_headers, 1);
        assert_eq!(
            captured.websocket.rx_declared_ws_framed_bytes,
            Some(declared)
        );
        assert_eq!(
            captured.websocket.rx_ws_framed_bytes_read,
            frames.len() as u64
        );
        assert_eq!(
            captured.websocket.rx_ws_framed_bytes_consumed,
            frames.len() as u64
        );
        assert_eq!(captured.websocket.rx_ws_framed_bytes_forwarded, 0);
        assert_eq!(
            captured.websocket.rx_ws_payload_bytes_consumed,
            consumed_payload
        );
        assert_eq!(captured.websocket.tx_ws_framed_bytes_written, 0);
        if declared > 4 * 1024 * 1024 {
            assert_eq!(
                captured.websocket.rx_last_declared_payload_bytes,
                Some(huge)
            );
            assert_eq!(
                captured.stop,
                Some(ObservedStop::Session(ObservedSessionLimit::RxWsFramedBytes))
            );
        }
    }
}

#[tokio::test]
async fn observed_payload_consumption_includes_partial_fragments_and_rejected_close() {
    use crate::network::TransportTrace;
    for (frames, declared, consumed_payload, forwarded) in [
        (vec![0x82, 5, b'A', b'B'], 7, 2, 4),
        (vec![0x01, 2, b'A', b'B', 0x80, 4, b'C', b'D'], 10, 4, 8),
        (vec![0x89, 3, b'p'], 5, 1, 3),
        (vec![0x88, 4, 3], 6, 1, 0),
        (vec![0x88, 3, 3, 232, 0xff], 5, 3, 5),
    ] {
        let (mut stream, mut peer, _cancel) = streams().await;
        peer.write_all(&[RESPONSE, &frames].concat()).await.unwrap();
        peer.shutdown().await.unwrap();
        let trace = TransportTrace::new();
        trace
            .begin(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let mut input = Input::with_observation(&mut stream, 2 * 1024 * 1024, Some(trace.clone()));
        input
            .read_exact(&mut vec![0; RESPONSE.len()])
            .await
            .unwrap();
        input.enable();
        let mut socket = WebSocketStream::from_raw_socket(input, Role::Client, None).await;
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                poll_fn(|cx| Pin::new(&mut socket).poll_next(cx))
            )
            .await
            .unwrap()
            .unwrap()
            .is_err()
        );
        let captured = trace.snapshot().websocket;
        assert_eq!(captured.rx_declared_ws_framed_bytes, Some(declared));
        assert_eq!(captured.rx_ws_framed_bytes_read, frames.len() as u64);
        assert_eq!(captured.rx_ws_framed_bytes_consumed, frames.len() as u64);
        assert_eq!(captured.rx_ws_payload_bytes_consumed, consumed_payload);
        assert_eq!(captured.rx_ws_framed_bytes_forwarded, forwarded);
    }
}

#[tokio::test]
async fn observed_tx_counter_counts_only_writes_and_preserves_the_exact_cap() {
    use crate::network::{ObservedSessionLimit, ObservedStop, TransportTrace};
    let (mut stream, mut peer, _cancel) = streams().await;
    peer.write_all(RESPONSE).await.unwrap();
    let trace = TransportTrace::new();
    trace
        .begin(Instant::now() + Duration::from_secs(5))
        .unwrap();
    let mut input = Input::with_observation(&mut stream, 2 * 1024 * 1024, Some(trace.clone()));
    input
        .read_exact(&mut vec![0; RESPONSE.len()])
        .await
        .unwrap();
    input.enable();
    // Literal masked client Binary:8-byte header +65528-byte payload =64KiB.
    let frame = [vec![0x82, 0xfe, 0xff, 0xf8, 0, 0, 0, 0], vec![b'x'; 65528]].concat();
    let consumer = async {
        input.write_all(&frame).await.unwrap();
        assert!(input.write_all(&[0x89, 0x80, 0, 0, 0, 0]).await.is_err());
        drop(input);
        drop(stream);
    };
    let peer_read = async {
        let mut actual = vec![0; 65536];
        peer.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, frame);
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(consumer, peer_read)
    })
    .await
    .unwrap();
    let captured = trace.snapshot();
    assert_eq!(captured.websocket.tx_ws_framed_bytes_written, 65536);
    assert_eq!(
        captured.stop,
        Some(ObservedStop::Session(ObservedSessionLimit::TxWsFramedBytes))
    );
}
