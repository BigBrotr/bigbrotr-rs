use super::support::{self, Connection};
use bigbrotr::{
    network::{ReceivedMessage, RelayWebSocket},
    runtime::ResourceRequest,
};
use bigbrotr_protocol::{
    EventError, EventLimits, verify_json,
    wire::{RelayMessage, decode_relay},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, time::timeout};

// Literal RFC 6455 server framing, independent of the product's WebSocket codec.
fn text_frame(text: &str) -> Vec<u8> {
    let mut frame = vec![0x81];
    if text.len() < 126 {
        frame.push(text.len() as u8);
    } else {
        frame.push(126);
        frame.extend_from_slice(&u16::try_from(text.len()).unwrap().to_be_bytes());
    }
    frame.extend_from_slice(text.as_bytes());
    frame
}

#[tokio::test]
async fn signed_socket_corpus_checks_every_payload_and_preserves_eose_hints() {
    use tokio::io::AsyncReadExt;
    const REQUEST: &str = r#"["REQ","wire-1",{}]"#;
    const EVENT_ID: &str = "a669974f5d6f76e4668b0be874435f14be799c04f0e70e5ec3d0967f1babf6bb";
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    let valid = &corpus["events"]["E_private"];
    let alternate = &corpus["events"]["E_public"];
    assert_eq!(valid["id"], EVENT_ID);
    assert_eq!(alternate["id"], EVENT_ID);
    assert_ne!(valid["sig"], alternate["sig"]);
    let mut changed = valid.clone();
    changed["content"] = json!("tampered but claims the same ID");
    let mut forged = valid.clone();
    forged["sig"] = json!("00".repeat(64));
    let original = serde_json::to_string_pretty(valid).unwrap();
    let duplicate = format!("{{\"content\":\"tampered\",{}", &original[1..]);
    let events = [
        original.clone(),
        serde_json::to_string(&changed).unwrap(),
        serde_json::to_string(&forged).unwrap(),
        serde_json::to_string(alternate).unwrap(),
        duplicate,
        original,
    ];
    let mut frames = Vec::new();
    // Receiving AUTH does not imply a request refusal. No signing is needed to
    // verify the subsequent public EVENT values in this component scenario.
    frames.extend(text_frame(r#"["AUTH","challenge"]"#));
    frames.extend(text_frame(r#"["NOTICE","generic hint"]"#));
    for event in events {
        frames.extend(text_frame(&format!("[\"EVENT\",\"wire-1\",{event}]")));
    }
    frames.extend(text_frame(
        r#"["EOSE","wire-1",["finish","more","auth","future","future"]]"#,
    ));

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
        stream.write_all(response.as_bytes()).await.unwrap();
        let mut header = [0; 6];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..2], &[0x81, 0x80 | REQUEST.len() as u8]);
        let mut received = vec![0; REQUEST.len()];
        stream.read_exact(&mut received).await.unwrap();
        for (index, byte) in received.iter_mut().enumerate() {
            *byte ^= header[2 + index % 4];
        }
        assert_eq!(received, REQUEST.as_bytes());
        // Chunking crosses JSON/token/frame boundaries. It does not presume how
        // TCP coalesces the separate writes on either native target.
        for chunk in frames.chunks(31) {
            stream.write_all(chunk).await.unwrap();
        }
        let mut close = [0; 6];
        stream.read_exact(&mut close).await.unwrap();
        // No AUTH response or other frame is permitted between REQ and Close.
        assert_eq!(&close[..2], &[0x88, 0x80]);
        stream.write_all(&[0x88, 0]).await.unwrap();
        stream.shutdown().await.unwrap();
    };
    let consumer = connector.run(attempt, async |stream| {
        let limits = EventLimits::default();
        // Both the decoded envelope and the verifier's owned event can coexist.
        // Their grant is additional to the live WebSocket buffer reservation.
        let _decode = resources
            .try_acquire(ResourceRequest::data(2 * limits.parse_bytes as u64, 1, 0).unwrap())
            .unwrap();
        let mut socket = RelayWebSocket::connect(stream, &resources, 32 * 1024)
            .await
            .unwrap();
        socket.send_text(REQUEST).await.unwrap();
        let mut outcomes = Vec::new();
        let mut controls = Vec::new();
        loop {
            let Some(ReceivedMessage::Text(text)) = socket.receive().await.unwrap() else {
                panic!("expected complete Nostr text message");
            };
            let envelope = decode_relay(text.as_bytes(), limits).unwrap();
            match envelope.message() {
                RelayMessage::Auth { challenge } => {
                    assert_eq!(challenge, "challenge");
                    controls.push("AUTH");
                }
                RelayMessage::Notice { message } => {
                    assert_eq!(message, "generic hint");
                    controls.push("NOTICE");
                }
                RelayMessage::Event { subscription, json } => {
                    assert_eq!(subscription, "wire-1");
                    match verify_json(json.get().as_bytes(), limits) {
                        Ok(verified) => {
                            let expected = if outcomes.len() == 3 {
                                alternate
                            } else {
                                valid
                            };
                            assert_eq!(serde_json::to_value(verified.event()).unwrap(), *expected);
                            outcomes.push(None);
                        }
                        Err(error) => outcomes.push(Some(error)),
                    }
                }
                RelayMessage::Eose {
                    subscription,
                    hints,
                } => {
                    assert_eq!(subscription, "wire-1");
                    assert_eq!(
                        hints.as_ref().unwrap(),
                        &["finish", "more", "auth", "future", "future"]
                    );
                    break;
                }
                _ => panic!("unexpected message"),
            }
        }
        assert_eq!(controls, ["AUTH", "NOTICE"]);
        assert_eq!(
            outcomes,
            [
                None,
                Some(EventError::EventIdMismatch),
                Some(EventError::InvalidSignature),
                None,
                Some(EventError::InvalidJson),
                None
            ]
        );
        socket.close().await.unwrap();
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
