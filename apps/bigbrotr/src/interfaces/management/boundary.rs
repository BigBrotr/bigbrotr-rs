use axum::http::{HeaderMap, Method, StatusCode, Uri};
use std::net::SocketAddr;

/// Exact browser authority for the explicit trusted-host loopback HTTP profile.
/// Cookies are host scoped, never isolated by port. Origin checks prevent browser
/// cross-origin commands; they do not authenticate other services on that host.
#[derive(Clone)]
pub(super) struct BrowserBoundary {
    authority: String,
    explicit_authority: String,
    origin: String,
}
impl BrowserBoundary {
    pub(super) fn loopback(address: SocketAddr) -> Result<Self, &'static str> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err("private HTTP requires a loopback address and explicit nonzero port");
        }
        let explicit_authority = address.to_string();
        let authority = if address.port() == 80 {
            match address {
                SocketAddr::V4(v) => v.ip().to_string(),
                SocketAddr::V6(v) => format!("[{}]", v.ip()),
            }
        } else {
            explicit_authority.clone()
        };
        let origin = format!("http://{authority}");
        Ok(Self {
            authority,
            explicit_authority,
            origin,
        })
    }

    pub(super) fn check(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Result<(), StatusCode> {
        if uri.scheme().is_some() || uri.authority().is_some() {
            return Err(StatusCode::BAD_REQUEST);
        }
        // No proxy/Forwarded/Referer fallback and no permissive origin parsing.
        // HTTP clients may spell the default port in Host; a browser Origin must
        // use its canonical serialization, which omits that port.
        let host = single(headers, "host").ok_or(StatusCode::FORBIDDEN)?;
        if host != self.authority.as_bytes() && host != self.explicit_authority.as_bytes() {
            return Err(StatusCode::FORBIDDEN);
        }
        match single(headers, "origin") {
            Some(value) if value == self.origin.as_bytes() => Ok(()),
            None if !headers.contains_key("origin")
                && matches!(*method, Method::GET | Method::HEAD) =>
            {
                Ok(())
            }
            _ => Err(StatusCode::FORBIDDEN),
        }
    }
}

pub(super) fn single<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a [u8]> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    Some(value.as_bytes())
}
