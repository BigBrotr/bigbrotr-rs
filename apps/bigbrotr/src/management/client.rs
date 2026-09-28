//! Finite direct HTTP operations for the trusted-host CLI profile.
//! One owned connection per explicit operation; no connector, pool or retries.
pub mod session_file;
pub mod validation;

use axum::{
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
};
use hyper::body::Body as HttpBody;
use hyper_util::rt::TokioIo;
use std::{
    future::poll_fn,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    time::Duration,
};
use zeroize::Zeroizing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientError {
    Input,
    SessionFile,
    Busy,
    PublicationUnknown,
    Transport,
    Response,
}
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never include peer text, paths, request URIs, credentials or tokens.
        write!(f, "management client refused: {self:?}")
    }
}
impl std::error::Error for ClientError {}

#[derive(Clone, Debug)]
pub struct LoopbackOrigin {
    address: SocketAddr,
    authority: String,
}
impl LoopbackOrigin {
    pub fn parse(text: &str) -> Result<Self, ClientError> {
        let authority = text.strip_prefix("http://").ok_or(ClientError::Input)?;
        if authority.len() > 64 {
            return Err(ClientError::Input);
        }
        let address = authority
            .parse::<SocketAddr>()
            .or_else(|_| {
                authority
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .map(|ip| SocketAddr::new(ip, 80))
            })
            .map_err(|_| ClientError::Input)?;
        let result = Self::from_address(address)?;
        if authority != result.authority && authority != address.to_string() {
            return Err(ClientError::Input);
        }
        Ok(result)
    }
    fn from_address(address: SocketAddr) -> Result<Self, ClientError> {
        if !address.ip().is_loopback()
            || address.port() == 0
            || matches!(address, SocketAddr::V6(a) if a.scope_id() != 0 || a.flowinfo() != 0)
        {
            return Err(ClientError::Input);
        }
        let authority = if address.port() == 80 {
            match address.ip() {
                IpAddr::V4(ip) => ip.to_string(),
                IpAddr::V6(ip) => format!("[{ip}]"),
            }
        } else {
            address.to_string()
        };
        Ok(Self { address, authority })
    }
    pub fn as_string(&self) -> String {
        format!("http://{}", self.authority)
    }
    fn authority(&self) -> &str {
        &self.authority
    }
}

pub struct Response {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Zeroizing<Vec<u8>>,
}

pub const RESPONSE_LIMIT: usize = 128 * 1024;
const SMALL_RESPONSE_LIMIT: usize = 16 * 1024;
pub const NETWORK_DEADLINE: Duration = Duration::from_secs(6);

/// Timeout drops the request AND its connection together. No spawned driver can
/// outlive this future or retain a socket after cancellation. The body is bounded
/// independently of Content-Length, including chunked and close-delimited input.
pub async fn exchange(
    origin: &LoopbackOrigin,
    request: Request<Body>,
) -> Result<Response, ClientError> {
    tokio::time::timeout(NETWORK_DEADLINE, exchange_inner(origin, request))
        .await
        .map_err(|_| ClientError::Transport)?
}
async fn exchange_inner(
    origin: &LoopbackOrigin,
    request: Request<Body>,
) -> Result<Response, ClientError> {
    // Only this module's closed callers build origin-form requests.
    if request.uri().scheme().is_some() || request.uri().authority().is_some() {
        return Err(ClientError::Input);
    }
    let page = request.method() == Method::GET && request.uri().path() == "/management/v1/jobs";
    let socket = tokio::net::TcpStream::connect(origin.address)
        .await
        .map_err(|_| ClientError::Transport)?;
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_headers(32)
        .max_buf_size(16 * 1024)
        .handshake(TokioIo::new(socket))
        .await
        .map_err(|_| ClientError::Transport)?;
    let receive = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| ClientError::Transport)?;
        let (parts, mut incoming) = response.into_parts();
        if parts.status.is_redirection()
            || parts.status.is_informational()
            || parts.headers.contains_key("content-encoding")
        {
            return Err(ClientError::Response);
        }
        let limit = if page && parts.status == StatusCode::OK {
            RESPONSE_LIMIT
        } else {
            SMALL_RESPONSE_LIMIT
        };
        if incoming
            .size_hint()
            .upper()
            .is_some_and(|n| n > limit as u64)
        {
            return Err(ClientError::Response);
        }
        let mut body = Zeroizing::new(Vec::new());
        body.try_reserve_exact(limit + 1)
            .map_err(|_| ClientError::Response)?;
        while let Some(frame) = poll_fn(|cx| Pin::new(&mut incoming).poll_frame(cx)).await {
            let frame = frame.map_err(|_| ClientError::Transport)?;
            let data = frame.into_data().map_err(|_| ClientError::Response)?;
            if data.len() > limit - body.len() {
                return Err(ClientError::Response);
            }
            body.extend_from_slice(&data);
        }
        Ok(Response {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    };
    tokio::pin!(receive, connection);
    // A complete connection may precede consumption of its last buffered body.
    // Then collect that body; otherwise dropping connection closes it directly.
    tokio::select! {
        result = &mut receive => result,
        _ = &mut connection => receive.await,
    }
}

pub fn request(
    origin: &LoopbackOrigin,
    method: Method,
    path: &str,
) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(path)
        .header("host", origin.authority())
        .header("origin", origin.as_string())
        .header("connection", "close")
        .header("accept", "application/json")
}
