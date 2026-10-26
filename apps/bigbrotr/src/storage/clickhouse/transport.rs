//! Bound HTTP response bytes before the ClickHouse client's error/header/row
//! buffers. The pinned client otherwise collects an error body without a limit.
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
        let connected = self.inner.call(uri);
        let remaining = self.response_bytes.get();
        Box::pin(async move {
            Ok(CappedIo {
                inner: connected.await.map_err(Into::into)?,
                remaining,
            })
        })
    }
}

struct CappedIo<T> {
    inner: T,
    remaining: usize,
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
        if this.remaining == 0 {
            return Poll::Ready(Err(io::Error::other(
                "ClickHouse response byte limit exceeded",
            )));
        }
        // A small initialized staging slice avoids unsafe cursor manipulation.
        // Never read an over-limit suffix and then mistake it for valid input.
        let mut scratch = [0; 8192];
        let available = scratch
            .len()
            .min(destination.remaining())
            .min(this.remaining);
        let mut buffer = ReadBuf::new(&mut scratch[..available]);
        ready!(Pin::new(&mut this.inner).poll_read(cx, buffer.unfilled()))?;
        this.remaining -= buffer.filled().len();
        destination.put_slice(buffer.filled());
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
mod tests {
    use super::*;
    use hyper_util::client::legacy::connect::HttpConnector;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn request(socket: &mut tokio::net::TcpStream) {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            assert!(header.len() < 8192);
            header.push(socket.read_u8().await.unwrap());
        }
        let length: usize = std::str::from_utf8(&header)
            .unwrap()
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
}
