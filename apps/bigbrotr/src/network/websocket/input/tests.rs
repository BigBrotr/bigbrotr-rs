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
            }),
            gate,
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
