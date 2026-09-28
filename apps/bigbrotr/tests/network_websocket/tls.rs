use super::support::{self, Connection};
use bigbrotr::network::{ReceivedMessage, RelayWebSocket};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

#[tokio::test]
async fn websocket_over_verified_tls_keeps_http_and_frame_boundaries() {
    const CERT: &[u8] =
        include_bytes!("../../../../tests/fixtures/relay-lifecycle/transport/server.der");
    const KEY: &[u8] =
        include_bytes!("../../../../tests/fixtures/relay-lifecycle/transport/server-test-key.der");
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![CERT.to_vec().into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(KEY.to_vec()).into(),
    )
    .unwrap();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::tls_connection().await;
    let address = listener.local_addr().unwrap();
    let peer = async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(socket).await.unwrap();
        assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
        let response = support::handshake(&mut stream, address).await;
        // Split a UTF8 codepoint across data frames and insert an independent Pong.
        // Whole data message is 11 framed bytes, at the exact configured cap.
        stream
            .write_all(
                &[
                    response.as_bytes(),
                    &[
                        0x01, 2, b'A', 0xe2, 0x8a, 1, b'p', 0x80, 5, 0x82, 0xac, b'B', b'C', b'D',
                    ],
                ]
                .concat(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();
        let mut close = [0; 6];
        stream.read_exact(&mut close).await.unwrap();
        assert_eq!(&close[..2], &[0x88, 0x80]);
        stream.write_all(&[0x88, 0]).await.unwrap();
        stream.shutdown().await.unwrap();
    };
    let consumer = connector.run(attempt, async |stream| {
        let mut socket = RelayWebSocket::connect(stream, &resources, 11)
            .await
            .unwrap();
        assert!(matches!(
            socket.receive().await.unwrap(),
            Some(ReceivedMessage::Pong(b"p"))
        ));
        assert!(matches!(
            socket.receive().await.unwrap(),
            Some(ReceivedMessage::Text("A€BCD"))
        ));
        socket.close().await.unwrap();
        Ok(())
    });
    let (result, ()) = timeout(Duration::from_secs(2), async {
        tokio::join!(consumer, peer)
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}
