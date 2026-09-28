#[path = "network_websocket/support.rs"]
#[allow(dead_code)]
mod support;
use bigbrotr::acquisition::session::{RelaySession, SessionMessage};
use bigbrotr_model::acquisition::{Filter, FilterFields};
use bigbrotr_protocol::{EventLimits, wire::RelayMessage};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn request(stream: &mut tokio::net::TcpStream) -> Value {
    assert_eq!(stream.read_u8().await.unwrap(), 0x81);
    let second = stream.read_u8().await.unwrap();
    assert_ne!(second & 0x80, 0);
    let count = match second & 0x7f {
        126 => usize::from(stream.read_u16().await.unwrap()),
        127 => panic!("oversized fixture command"),
        n => usize::from(n),
    };
    assert!(count < 4096);
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).await.unwrap();
    let mut bytes = vec![0; count];
    stream.read_exact(&mut bytes).await.unwrap();
    for (i, b) in bytes.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    serde_json::from_slice(&bytes).unwrap()
}
async fn frame(stream: &mut tokio::net::TcpStream, text: &str) {
    if text.len() < 126 {
        stream.write_all(&[0x81, text.len() as u8]).await.unwrap();
    } else {
        stream.write_all(&[0x81, 126]).await.unwrap();
        stream
            .write_u16(u16::try_from(text.len()).unwrap())
            .await
            .unwrap();
    }
    stream.write_all(text.as_bytes()).await.unwrap();
}

async fn closed_socket(stream: &mut tokio::net::TcpStream) {
    // Dropping a connection with unread hostile data may cause TCP RST rather
    // than FIN. Both close custody; receiving any further application byte fails.
    match stream.read(&mut [0; 1]).await {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        result => panic!("socket did not close without more output: {result:?}"),
    }
}

#[tokio::test]
async fn retired_handle_cannot_close_its_replacement_at_the_actual_wire_boundary() {
    let support::Connection {
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
        let first = request(&mut stream).await;
        assert_eq!(first[0], "REQ");
        assert_eq!(first[2], serde_json::json!({"kinds":[1],"limit":0}));
        let old = first[1].as_str().unwrap();
        assert_eq!(old.len(), 48);
        assert_eq!(
            request(&mut stream).await,
            serde_json::json!(["CLOSE", old])
        );
        let second = request(&mut stream).await;
        let new = second[1].as_str().unwrap();
        assert_ne!(old, new);
        assert_eq!(new.len(), 48);
        assert_eq!(second[2], serde_json::json!({"kinds":[2]}));
        frame(&mut stream, &format!(r#"["EOSE","{old}"]"#)).await;
        frame(&mut stream, &format!(r#"["CLOSED","{old}","stale"]"#)).await;
        frame(&mut stream, &format!(r#"["EOSE","{new}",["more","auth"]]"#)).await;
        // The stale close attempt must emit nothing; the next frame is the
        // current generation's CLOSE, followed by the WebSocket close handshake.
        assert_eq!(
            request(&mut stream).await,
            serde_json::json!(["CLOSE", new])
        );
        let mut close = [0; 6];
        stream.read_exact(&mut close).await.unwrap();
        assert_eq!(&close[..2], &[0x88, 0x80]);
        stream.write_all(&[0x88, 0]).await.unwrap();
        stream.shutdown().await.unwrap();
    };
    let consumer = connector.run(attempt, async |stream| {
        let first = [Filter::new(FilterFields {
            kinds: Some(vec![1]),
            ..Default::default()
        })
        .unwrap()];
        let second = [Filter::new(FilterFields {
            kinds: Some(vec![2]),
            ..Default::default()
        })
        .unwrap()];
        let mut session = RelaySession::connect(
            stream,
            &resources,
            EventLimits::default(),
            1.try_into().unwrap(),
        )
        .await?;
        let old = session.open(&first, Some(0)).await?;
        assert!(session.close_request(old).await?);
        let new = session.open(&second, None).await?;
        assert!(!session.close_request(old).await?);
        let SessionMessage::Relay(stale) = session.receive().await?.unwrap() else {
            panic!("relay message")
        };
        assert_eq!(stale.sequence, 1);
        assert!(stale.request.is_none());
        let SessionMessage::Relay(stale) = session.receive().await?.unwrap() else {
            panic!("relay message")
        };
        assert_eq!(stale.sequence, 2);
        assert!(stale.request.is_none());
        let SessionMessage::Relay(current) = session.receive().await?.unwrap() else {
            panic!("relay message")
        };
        assert_eq!(current.sequence, 3);
        let request = current.request.unwrap();
        assert_eq!(request.id(), new);
        assert!(request.filters() == second);
        let RelayMessage::Eose { hints, .. } = current.envelope.message() else {
            panic!("EOSE")
        };
        assert_eq!(hints.as_ref().unwrap(), &["more", "auth"]);
        assert!(session.close_request(new).await?);
        session.close().await?;
        Ok(())
    });
    let (result, ()) = tokio::join!(consumer, peer);
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn multiplexed_frames_keep_their_exact_predicate_and_closed_retires_only_its_request() {
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    let signed = &corpus["events"]["E_private"];
    let event = serde_json::to_string(signed).unwrap();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        let first = request(&mut stream).await;
        let second = request(&mut stream).await;
        let one = first[1].as_str().unwrap();
        let two = second[1].as_str().unwrap();
        assert_ne!(one, two);
        assert_eq!(first[2], serde_json::json!({"kinds":[1]}));
        assert_eq!(second[2], serde_json::json!({"kinds":[2],"limit":3}));
        frame(&mut stream, &format!(r#"["EVENT","{one}",{event}]"#)).await;
        frame(&mut stream, &format!(r#"["EVENT","unknown",{event}]"#)).await;
        frame(&mut stream, &format!(r#"["EOSE","{two}",["finish"]]"#)).await;
        frame(
            &mut stream,
            &format!(r#"["CLOSED","{one}","auth-required"]"#),
        )
        .await;
        frame(&mut stream, &format!(r#"["EVENT","{one}",{event}]"#)).await;
        assert_eq!(
            request(&mut stream).await,
            serde_json::json!(["CLOSE", two])
        );
        let mut close = [0; 6];
        stream.read_exact(&mut close).await.unwrap();
        assert_eq!(&close[..2], &[0x88, 0x80]);
        stream.write_all(&[0x88, 0]).await.unwrap();
        stream.shutdown().await.unwrap();
    };
    let consumer = connector.run(attempt, async |stream| {
        let one = [Filter::new(FilterFields {
            kinds: Some(vec![1]),
            ..Default::default()
        })
        .unwrap()];
        let two = [Filter::new(FilterFields {
            kinds: Some(vec![2]),
            ..Default::default()
        })
        .unwrap()];
        let mut session = RelaySession::connect(
            stream,
            &resources,
            EventLimits::default(),
            2.try_into().unwrap(),
        )
        .await?;
        let first = session.open(&one, None).await?;
        let second = session.open(&two, Some(3)).await?;
        assert_eq!(
            session.open(&one, None).await,
            Err(bigbrotr::acquisition::session::SessionError::Capacity)
        );
        for (index, expected) in [Some(first), None, Some(second), Some(first), None]
            .into_iter()
            .enumerate()
        {
            let SessionMessage::Relay(received) = session.receive().await?.unwrap() else {
                panic!("relay")
            };
            assert_eq!(received.sequence, index as u64 + 1);
            assert_eq!(received.request.map(|r| r.id()), expected);
            if index == 0 {
                let request = received.request.unwrap();
                assert!(request.filters() == one);
                assert_eq!(request.limit(), None);
                let RelayMessage::Event { json, .. } = received.envelope.message() else {
                    panic!("EVENT")
                };
                let _verification = resources
                    .try_acquire(
                        bigbrotr::runtime::ResourceRequest::data(8 * 1024 * 1024, 1, 0).unwrap(),
                    )
                    .unwrap();
                let verified =
                    bigbrotr_protocol::verify_json(json.get().as_bytes(), EventLimits::default())
                        .unwrap();
                assert_eq!(serde_json::to_value(verified.event()).unwrap(), *signed);
                assert!(request.filters()[0].matches(verified.event()));
            }
            if index == 2 {
                assert!(received.request.unwrap().filters() == two);
                assert_eq!(received.request.unwrap().limit(), Some(3));
            }
        }
        assert!(!session.close_request(first).await?);
        assert!(session.close_request(second).await?);
        session.close().await?;
        Ok(())
    });
    let (result, ()) = tokio::join!(consumer, peer);
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn cancelled_partial_frame_retires_the_session_and_closes_the_owned_socket() {
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let address = listener.local_addr().unwrap();
    let (sent, ready) = tokio::sync::oneshot::channel();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let response = support::handshake(&mut stream, address).await;
        stream.write_all(response.as_bytes()).await.unwrap();
        let _ = request(&mut stream).await;
        stream.write_all(&[0x81, 100, b'[']).await.unwrap();
        sent.send(()).unwrap();
        closed_socket(&mut stream).await;
    };
    let consumer = connector.run(attempt, async |stream| {
        let filters = [Filter::new(FilterFields::default()).unwrap()];
        let mut session = RelaySession::connect(
            stream,
            &resources,
            EventLimits::default(),
            1.try_into().unwrap(),
        )
        .await?;
        let id = session.open(&filters, None).await?;
        ready.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), session.receive())
                .await
                .is_err()
        );
        use bigbrotr::acquisition::session::SessionError;
        assert_eq!(
            session.close_request(id).await,
            Err(SessionError::Unavailable)
        );
        assert_eq!(
            session.open(&filters, None).await,
            Err(SessionError::Unavailable)
        );
        assert!(matches!(
            session.receive().await,
            Err(SessionError::Unavailable)
        ));
        drop(session);
        Ok(())
    });
    let (result, ()) = tokio::join!(consumer, peer);
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn malformed_or_binary_input_cannot_be_cleared_by_a_following_eose() {
    for malformed in [
        vec![
            0x81, 9, b'[', b'"', b'E', b'V', b'E', b'N', b'T', b'"', b']',
        ],
        vec![0x82, 1, b'x'],
    ] {
        let support::Connection {
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
            let req = request(&mut stream).await;
            let id = req[1].as_str().unwrap();
            stream.write_all(&malformed).await.unwrap();
            frame(&mut stream, &format!(r#"["EOSE","{id}"]"#)).await;
            closed_socket(&mut stream).await;
        };
        let consumer = connector.run(attempt, async |stream| {
            let filters = [Filter::new(FilterFields::default()).unwrap()];
            let mut session = RelaySession::connect(
                stream,
                &resources,
                EventLimits::default(),
                1.try_into().unwrap(),
            )
            .await?;
            session.open(&filters, None).await?;
            use bigbrotr::acquisition::session::SessionError;
            match malformed[0] {
                0x81 => assert!(matches!(
                    session.receive().await,
                    Err(SessionError::Envelope(_))
                )),
                0x82 => assert!(matches!(
                    session.receive().await,
                    Err(SessionError::BinaryMessage)
                )),
                _ => unreachable!(),
            }
            assert!(matches!(
                session.receive().await,
                Err(SessionError::Unavailable)
            ));
            drop(session);
            Ok(())
        });
        let (result, ()) = tokio::join!(consumer, peer);
        result.unwrap();
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test]
async fn parser_registry_credit_is_required_before_the_websocket_handshake() {
    use bigbrotr::runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits};
    let support::Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel: _cancel,
    } = support::connection().await;
    let insufficient = ResourceBudget::new(
        ProcessEnvelope::new(
            1024,
            0,
            ResourceLimits::new(1024, 4, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        closed_socket(&mut stream).await;
    };
    let consumer = connector.run(attempt, async |stream| {
        assert!(matches!(
            RelaySession::connect(
                stream,
                &insufficient,
                EventLimits::default(),
                1.try_into().unwrap()
            )
            .await,
            Err(bigbrotr::acquisition::session::SessionError::Budget)
        ));
        assert_eq!(insufficient.snapshot().active_permits(), 0);
        Ok(())
    });
    let (result, ()) = tokio::join!(consumer, peer);
    result.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}
