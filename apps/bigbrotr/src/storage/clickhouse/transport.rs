//! Stage bounded HTTP response bytes until actual peer EOF, before exposing any
//! header/body to Hyper and the ClickHouse client's existing framing/row checks.
//! Framed body completion alone must not dispatch the next writer query.
//! Connections are not pooled: this allowance belongs to one response, including
//! its HTTP framing. Compression must remain disabled on the resulting client.
use hyper::{
    Uri,
    rt::{Read, ReadBuf, ReadBufCursor, Write},
};
use hyper_util::{
    client::legacy::{
        Client,
        connect::{Connected, Connection},
    },
    rt::TokioExecutor,
};
use std::{
    error::Error,
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tower_service::Service;

type BoxError = Box<dyn Error + Send + Sync>;

pub(super) fn bounded_client<C>(connector: C, response_bytes: NonZeroUsize) -> clickhouse::Client
where
    C: Service<Uri> + Clone + Send + Sync + 'static,
    C::Response: Read + Write + Connection + Unpin + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
{
    let http = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(0)
        .http1_max_buf_size(16 * 1024)
        .build(CappedConnector {
            inner: connector,
            response_bytes,
        });
    clickhouse::Client::with_http_client(http)
        .with_header("Connection", "close")
        .with_compression(clickhouse::Compression::None)
        .with_validation(true)
}

#[derive(Clone)]
struct CappedConnector<C> {
    inner: C,
    response_bytes: NonZeroUsize,
}
impl<C> Service<Uri> for CappedConnector<C>
where
    C: Service<Uri>,
    C::Future: Send + 'static,
    C::Response: 'static,
    C::Error: Into<BoxError>,
{
    type Response = CappedIo<C::Response>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        // The adapter admits a separate complete wire allowance before creating
        // this client. Refuse allocation before even invoking the connector:
        // its error remains a definite preconnection, zero-request failure.
        let mut staged = Vec::new();
        if staged.try_reserve_exact(self.response_bytes.get()).is_err() {
            return Box::pin(async {
                Err(io::Error::other("ClickHouse response staging allocation unavailable").into())
            });
        }
        let connected = self.inner.call(uri);
        let remaining = self.response_bytes.get();
        Box::pin(async move {
            Ok(CappedIo {
                inner: connected.await.map_err(Into::into)?,
                remaining,
                staged,
                consumed: 0,
                peer_eof: false,
            })
        })
    }
}

struct CappedIo<T> {
    inner: T,
    remaining: usize,
    staged: Vec<u8>,
    consumed: usize,
    peer_eof: bool,
}
impl<T: Connection> Connection for CappedIo<T> {
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}
impl<T: Read + Unpin> Read for CappedIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut destination: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if destination.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if !this.peer_eof {
            // This fixed initialized slice fits the already funded lane stack.
            // At the exact cap, probe one guard byte: it is never staged or
            // forwarded. Zero is actual peer EOF; any byte refuses immediately.
            let mut scratch = [0; 8192];
            let available = if this.remaining == 0 {
                1
            } else {
                scratch.len().min(this.remaining)
            };
            let mut buffer = ReadBuf::new(&mut scratch[..available]);
            ready!(Pin::new(&mut this.inner).poll_read(cx, buffer.unfilled()))?;
            let bytes = buffer.filled();
            if bytes.is_empty() {
                this.peer_eof = true;
            } else {
                if this.remaining == 0 {
                    return Poll::Ready(Err(io::Error::other(
                        "ClickHouse response byte limit exceeded",
                    )));
                }
                this.remaining -= bytes.len();
                this.staged.extend_from_slice(bytes);
                // Yield between bounded reads so writes and cancellation can
                // progress. Returning an empty successful read would invent EOF.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        let available = destination
            .remaining()
            .min(this.staged.len() - this.consumed);
        destination.put_slice(&this.staged[this.consumed..this.consumed + available]);
        this.consumed += available;
        if this.consumed == this.staged.len() {
            // Residual legacy driver tasks must not retain an allowance while
            // the same operation starts its next fresh unpooled connection.
            this.staged = Vec::new();
            this.consumed = 0;
        }
        Poll::Ready(Ok(()))
    }
}
impl<T: Write + Unpin> Write for CappedIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, buffers)
    }
}

#[cfg(test)]
#[path = "transport/metadata_tests.rs"]
mod metadata_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_util::client::legacy::connect::HttpConnector;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    pub(super) async fn request(socket: &mut tokio::net::TcpStream) {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            assert!(header.len() < 8192);
            header.push(socket.read_u8().await.unwrap());
        }
        let text = std::str::from_utf8(&header).unwrap();
        assert!(text.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("connection")
                    && value.trim().eq_ignore_ascii_case("close")
            })
        }));
        let length: usize = text
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .unwrap()
            .1
            .trim()
            .parse()
            .unwrap();
        assert!(length <= 8192);
        socket.read_exact(&mut vec![0; length]).await.unwrap();
    }

    #[test]
    fn error_body_is_cut_off_before_unbounded_client_collection() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                request(&mut socket).await;
                socket
                    .write_all(
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 65536\r\n\r\n",
                    )
                    .await
                    .unwrap();
                // The peer intentionally never completes its advertised body.
                // The client must fail at its byte allowance without waiting for EOF.
                let _ = socket.write_all(&[b'x'; 8192]).await;
                let mut byte = [0];
                let _ = socket.read(&mut byte).await;
            });
            let client = bounded_client(HttpConnector::new(), NonZeroUsize::new(512).unwrap())
                .with_url(format!("http://{address}"));
            let result =
                tokio::time::timeout(Duration::from_secs(5), client.query("SELECT 1").execute())
                    .await
                    .expect("response cap must not depend on peer completion");
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().len() < 256);
            drop(client);
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        });
    }

    #[test]
    fn response_allowance_is_fresh_on_each_unpooled_connection() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                for _ in 0..2 {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    request(&mut socket).await;
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n1\n")
                        .await
                        .unwrap();
                    socket.shutdown().await.unwrap();
                    let mut byte = [0];
                    assert_eq!(
                        tokio::time::timeout(Duration::from_secs(5), socket.read(&mut byte))
                            .await
                            .unwrap()
                            .unwrap(),
                        0,
                        "completed connections must not remain pooled"
                    );
                }
            });
            let client = bounded_client(HttpConnector::new(), NonZeroUsize::new(64).unwrap())
                .with_url(format!("http://{address}"));
            for _ in 0..2 {
                tokio::time::timeout(Duration::from_secs(5), client.query("SELECT 1").execute())
                    .await
                    .unwrap()
                    .unwrap();
            }
            server.await.unwrap();
        });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn complete_frame_at_wire_cap_waits_for_actual_peer_eof() {
        for response in [
            &b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n1\n"[..],
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n1\n\r\n0\r\n\r\n"[..],
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (sent, observe_sent) = tokio::sync::oneshot::channel();
            let (finish, observe_finish) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                request(&mut socket).await;
                socket.write_all(response).await.unwrap();
                sent.send(()).unwrap();
                observe_finish.await.unwrap();
                socket.shutdown().await.unwrap();
            });
            let client = bounded_client(
                HttpConnector::new(),
                NonZeroUsize::new(response.len()).unwrap(),
            )
            .with_url(format!("http://{address}"));
            let mut query = tokio::spawn(async move { client.query("SELECT 1").execute().await });
            tokio::time::timeout(Duration::from_secs(5), observe_sent)
                .await
                .unwrap()
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut query)
                    .await
                    .is_err(),
                "complete framing without peer EOF must not complete the query"
            );
            finish.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), query)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn truncated_frames_and_server_errors_stay_errors_after_peer_eof() {
        for response in [
            &b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n1"[..],
            &b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n1\n\r\n"[..],
            &b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\n\r\nfail"[..],
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                request(&mut socket).await;
                socket.write_all(response).await.unwrap();
                socket.shutdown().await.unwrap();
            });
            let client = bounded_client(HttpConnector::new(), NonZeroUsize::new(512).unwrap())
                .with_url(format!("http://{address}"));
            let result =
                tokio::time::timeout(Duration::from_secs(5), client.query("SELECT 1").execute())
                    .await
                    .unwrap();
            assert!(result.is_err(), "truncated/error frame cannot succeed");
            server.await.unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn staging_allocation_refusal_never_invokes_inner_connector() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        #[derive(Clone)]
        struct ObservedConnector {
            inner: HttpConnector,
            calls: Arc<AtomicUsize>,
        }
        impl Service<Uri> for ObservedConnector {
            type Response = <HttpConnector as Service<Uri>>::Response;
            type Error = <HttpConnector as Service<Uri>>::Error;
            type Future = <HttpConnector as Service<Uri>>::Future;

            fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                self.inner.poll_ready(cx)
            }
            fn call(&mut self, uri: Uri) -> Self::Future {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.inner.call(uri)
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let connector = ObservedConnector {
            inner: HttpConnector::new(),
            calls: Arc::clone(&calls),
        };
        // Vec's deterministic capacity overflow cannot allocate or invoke the
        // connector. Production PhysicalLimits rejects this impossible bound.
        let client = bounded_client(connector, NonZeroUsize::new(usize::MAX).unwrap())
            .with_url("http://127.0.0.1:0");
        let result =
            tokio::time::timeout(Duration::from_secs(5), client.query("SELECT 1").execute())
                .await
                .unwrap();
        assert!(matches!(
            result,
            Err(clickhouse::error::Error::Network(ref error))
                if error.downcast_ref::<hyper_util::client::legacy::Error>()
                    .is_some_and(|error| error.is_connect())
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
