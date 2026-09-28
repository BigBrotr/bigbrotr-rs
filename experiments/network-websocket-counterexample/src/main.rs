//! Isolated exact-version dependency counterexample; not product code.
use std::{io::Write, net::{TcpListener, TcpStream}, thread, time::Duration};
use tungstenite::{WebSocket, protocol::{Role, WebSocketConfig}};

fn receive(frames: Vec<u8>) -> bool {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(&frames).unwrap();
    });
    let stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let config = WebSocketConfig::default()
        .read_buffer_size(32)
        .max_message_size(Some(16))
        .max_frame_size(Some(16));
    let mut socket = WebSocket::from_raw_socket(stream, Role::Client, Some(config));
    let accepted = socket.read().is_ok();
    peer.join().unwrap();
    accepted
}

fn main() {
    let exact: Vec<u8> = [vec![0x81, 14], vec![b'x'; 14]].concat();
    let oversized: Vec<u8> = [vec![0x81, 16], vec![b'x'; 16]].concat();
    // Empty text start, seven nonfinal continuations, one final continuation.
    // Payload is empty, but the message consumes18 framing bytes.
    let fragmented: Vec<u8> = [vec![0x01, 0], vec![0, 0].repeat(7), vec![0x80, 0]].concat();
    let positive = receive(exact);
    let payload_only = receive(oversized);
    let zero_fragments = receive(fragmented);
    println!("{{\"configured_limit\":16,\"exact_wire16_accepted\":{positive},\"wire18_payload16_accepted\":{payload_only},\"wire18_payload0_accepted\":{zero_fragments}}}");
    assert!(positive, "fixture positive failed");
    assert!(!payload_only && !zero_fragments, "upstream payload limits do not satisfy the framing-inclusive product contract");
}
