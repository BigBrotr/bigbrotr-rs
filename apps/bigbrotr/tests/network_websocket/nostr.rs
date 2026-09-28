use super::support::{self, Connection};
use bigbrotr::{
    network::{ReceivedMessage, RelayWebSocket},
    runtime::ResourceRequest,
};
use bigbrotr_model::acquisition::Membership;
use bigbrotr_protocol::{
    EventError, EventLimits,
    selector::decode_selector,
    verify_json,
    wire::{RelayMessage, decode_relay},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

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

// Only the small, complete masked text frames used by these independent peers.
async fn read_request(stream: &mut (impl AsyncRead + Unpin)) -> Vec<u8> {
    let mut header = [0; 2];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0], 0x81);
    assert_ne!(header[1] & 0x80, 0);
    let length = match header[1] & 0x7f {
        126 => stream.read_u16().await.unwrap() as usize,
        127 => panic!("unexpected large fixture request"),
        short => short as usize,
    };
    assert!(length <= 4096);
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).await.unwrap();
    let mut received = vec![0; length];
    stream.read_exact(&mut received).await.unwrap();
    for (index, byte) in received.iter_mut().enumerate() {
        *byte ^= mask[index % 4];
    }
    received
}

#[tokio::test]
async fn signed_socket_corpus_checks_every_payload_and_preserves_eose_hints() {
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
        assert_eq!(read_request(&mut stream).await, REQUEST.as_bytes());
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

#[tokio::test]
async fn signed_socket_separates_collection_exclusion_from_request_violation() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/acquisition/selector-events.json"
    ))
    .unwrap();
    let rows = corpus["events"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
    let mut frames = Vec::new();
    for row in rows {
        frames.extend(text_frame(&format!(
            "[\"EVENT\",\"scope-1\",{}]",
            row["event"]
        )));
    }
    frames.extend(text_frame(r#"["EOSE","scope-1"]"#));
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
        let request: Value = serde_json::from_slice(&read_request(&mut stream).await).unwrap();
        // Independent literal: never generated by the production selector.
        assert_eq!(
            request,
            json!(["REQ", "scope-1", {
                "authors": ["79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"],
                "#t": ["allowed"], "since": 23, "until": 23
            }])
        );
        stream.write_all(&frames).await.unwrap();
        let mut close = [0; 6];
        stream.read_exact(&mut close).await.unwrap();
        assert_eq!(&close[..2], &[0x88, 0x80]);
        stream.write_all(&[0x88, 0]).await.unwrap();
        stream.shutdown().await.unwrap();
    };
    let consumer = connector.run(attempt, async |stream| {
        let limits = EventLimits::default();
        // Two selector parses/retained values, envelope and verified event. This
        // 64MiB test budget does not qualify the installed 32MiB producer grant.
        let _memory = resources
            .try_acquire(ResourceRequest::data(4 * limits.parse_bytes as u64, 1, 0).unwrap())
            .unwrap();
        let collection =
            decode_selector(&serde_json::to_vec(&corpus["collection"]).unwrap()).unwrap();
        let request = decode_selector(&serde_json::to_vec(&corpus["request"]).unwrap()).unwrap();
        assert_eq!(request.filters().len(), 1);
        let wire = serde_json::to_string(&json!(["REQ", "scope-1", request.filters()[0]])).unwrap();
        let mut socket = RelayWebSocket::connect(stream, &resources, 32 * 1024)
            .await
            .unwrap();
        socket.send_text(&wire).await.unwrap();
        let mut position = 0;
        loop {
            let Some(ReceivedMessage::Text(text)) = socket.receive().await.unwrap() else {
                panic!("expected Nostr text");
            };
            let envelope = decode_relay(text.as_bytes(), limits).unwrap();
            match envelope.message() {
                RelayMessage::Event { subscription, json } => {
                    assert_eq!(subscription, "scope-1");
                    let expected = &rows[position];
                    let verified = verify_json(json.get().as_bytes(), limits).unwrap();
                    assert_eq!(
                        serde_json::to_value(verified.event()).unwrap(),
                        expected["event"]
                    );
                    let membership = match collection.classify(verified.event()) {
                        Membership::Included => "included",
                        Membership::OutsideSelector => "outside-selector",
                        Membership::EphemeralExcluded => "ephemeral-excluded",
                    };
                    assert_eq!(membership, expected["collection"]);
                    assert_eq!(
                        request.filters()[0].matches(verified.event()),
                        expected["request"].as_bool().unwrap()
                    );
                    position += 1;
                }
                RelayMessage::Eose {
                    subscription,
                    hints,
                } => {
                    assert_eq!(subscription, "scope-1");
                    assert!(hints.is_none());
                    break;
                }
                _ => panic!("unexpected message"),
            }
        }
        assert_eq!(position, 6);
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
