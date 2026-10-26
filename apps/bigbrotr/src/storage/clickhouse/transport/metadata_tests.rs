//! Actual selected-client metadata controls against a tiny loopback HTTP peer.
//! These exercise the vendor patch with validation enabled; no engine is started.
use super::*;
use clickhouse::error::Error;
use hyper_util::client::legacy::connect::HttpConnector;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[derive(Debug, Deserialize, Serialize, clickhouse::Row, PartialEq)]
struct One {
    id: u64,
}

fn string(bytes: &mut Vec<u8>, value: &str) {
    let mut length = value.len();
    while length >= 128 {
        bytes.push((length as u8 & 127) | 128);
        length >>= 7;
    }
    bytes.push(length as u8);
    bytes.extend_from_slice(value.as_bytes());
}

fn header(names: &[&str], types: &[&str]) -> Vec<u8> {
    assert_eq!(names.len(), types.len());
    assert!(names.len() < 128);
    let mut bytes = vec![names.len() as u8];
    for value in names.iter().chain(types) {
        string(&mut bytes, value);
    }
    bytes
}

async fn peer(body: Vec<u8>) -> (clickhouse::Client, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        super::tests::request(&mut socket).await;
        let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    });
    let client = bounded_client(HttpConnector::new(), NonZeroUsize::new(16 * 1024).unwrap())
        .with_url(format!("http://{address}"));
    (client, server)
}

fn run(future: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), future).await.unwrap() });
}

#[test]
fn actual_cursor_refuses_declared_count_and_malformed_metadata() {
    run(async {
        let mut count = vec![0xff; 9];
        count.push(1); // u64::MAX: no attacker-sized allocation is attempted.
        let long_name = "x".repeat(clickhouse::_priv::RBWNAT_MAX_NAME_BYTES + 1);
        let nested = format!("{}UInt64{}", "Array(".repeat(9), ")".repeat(9));
        for bytes in [count, vec![2], header(&["id"], &["SimpleAggregateFunction("]),
            header(&["id"], &[&nested]), header(&[&long_name], &["UInt64"])] {
            let (client, server) = peer(bytes).await;
            let error = client.query("SELECT id").fetch_one::<One>().await.unwrap_err();
            assert!(matches!(error, Error::InvalidColumnsHeader(_)), "{error}");
            server.await.unwrap();
        }
    });
}

#[test]
fn valid_header_keeps_row_validation_and_decoding() {
    run(async {
        let mut bytes = header(&["id"], &["UInt64"]);
        bytes.extend_from_slice(&42u64.to_le_bytes());
        let (client, server) = peer(bytes).await;
        assert_eq!(client.query("SELECT id").fetch_one::<One>().await.unwrap(), One { id: 42 });
        server.await.unwrap();
        let mut bytes = header(&["id"], &["Int64"]);
        bytes.extend_from_slice(&42i64.to_le_bytes());
        let (client, server) = peer(bytes).await;
        assert!(client.query("SELECT id").fetch_one::<One>().await.is_err());
        server.await.unwrap();
    });
}

#[test]
fn malformed_inner_type_cannot_match_smaller_rust_tuple() {
    #[derive(Debug, Deserialize, clickhouse::Row, PartialEq)]
    struct OneTuple {
        id: (u64,),
    }
    run(async {
        for ty in ["Tuple(UInt64, `(`)", "Tuple(UInt64,)", "Tuple(UInt64, )"] {
            let mut bytes = header(&["id"], &[ty]);
            bytes.extend_from_slice(&42u64.to_le_bytes());
            let (client, server) = peer(bytes).await;
            let result = client.query("SELECT id").fetch_one::<OneTuple>().await;
            server.await.unwrap();
            assert!(matches!(result, Err(Error::InvalidColumnsHeader(_))), "{ty}: {result:?}");
        }
        let mut bytes = header(&["id"], &["Tuple(UInt64)"]);
        bytes.extend_from_slice(&42u64.to_le_bytes());
        let (client, server) = peer(bytes).await;
        assert_eq!(client.query("SELECT id").fetch_one::<OneTuple>().await.unwrap(), OneTuple { id: (42,) });
        server.await.unwrap();
    });
}

#[test]
fn expected_count_preserves_primitive_vector_and_tuple_rows() {
    run(async {
        let mut bytes = header(&["id"], &["UInt64"]);
        bytes.extend_from_slice(&42u64.to_le_bytes());
        let (client, server) = peer(bytes).await;
        assert_eq!(client.query("SELECT id").fetch_one::<u64>().await.unwrap(), 42);
        server.await.unwrap();

        let mut bytes = header(&["ids"], &["Array(UInt64)"]);
        bytes.push(2);
        bytes.extend_from_slice(&42u64.to_le_bytes());
        bytes.extend_from_slice(&43u64.to_le_bytes());
        let (client, server) = peer(bytes).await;
        assert_eq!(client.query("SELECT ids").fetch_one::<Vec<u64>>().await.unwrap(), vec![42, 43]);
        server.await.unwrap();

        let mut bytes = header(&["id", "kind"], &["UInt64", "UInt16"]);
        bytes.extend_from_slice(&42u64.to_le_bytes());
        bytes.extend_from_slice(&7u16.to_le_bytes());
        let (client, server) = peer(bytes).await;
        assert_eq!(client.query("SELECT id, kind").fetch_one::<(u64, u16)>().await.unwrap(), (42, 7));
        server.await.unwrap();
    });
}

fn describe(count: usize, ty: &str) -> Vec<u8> {
    let names = ["name", "type", "default_type", "default_expression", "comment",
        "codec_expression", "ttl_expression"];
    let mut bytes = header(&names, &["String"; 7]);
    for _ in 0..count {
        for value in ["id", ty, "", "", "", "", ""] {
            string(&mut bytes, value);
        }
    }
    bytes
}

#[test]
fn insert_describe_reuses_bounded_parser_and_bounds_metadata_rows() {
    run(async {
        let (client, server) = peer(describe(1, "UInt64")).await;
        // Constructing INSERT fetches DESCRIBE; dropping it sends no insert body.
        let insert = client.insert::<One>("table").await.unwrap();
        drop(insert);
        server.await.unwrap();
        let (client, server) = peer(describe(1, "SimpleAggregateFunction(")).await;
        let error = client.insert::<One>("table").await.err().unwrap();
        assert!(matches!(error, Error::InvalidColumnsHeader(_)), "{error}");
        server.await.unwrap();
        let (client, server) = peer(describe(clickhouse::_priv::RBWNAT_MAX_COLUMNS + 1, "UInt64")).await;
        let error = client.insert::<One>("table").await.err().unwrap();
        assert!(matches!(error, Error::BadResponse(ref message) if message == "insert metadata exceeds bounded column profile"));
        server.await.unwrap();
    });
}
