//! Pure `bigbrotr-endpoint/1` identity normalization. This grants no dial authority.

use std::{fmt, net::Ipv4Addr};

/// Maximum UTF-8 bytes in one configured or discovered endpoint.
pub const MAX_ENDPOINT_BYTES: usize = 8192;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Endpoint(String);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InvalidEndpoint;

impl fmt::Display for InvalidEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid bounded ws/wss endpoint; credentials, fragments and ambiguous addresses are forbidden")
    }
}

impl std::error::Error for InvalidEndpoint {}

impl Endpoint {
    pub fn parse(input: &str) -> Result<Self, InvalidEndpoint> {
        if input.is_empty()
            || input.len() > MAX_ENDPOINT_BYTES
            || input.chars().any(|c| c.is_control() || c.is_whitespace())
            || input.contains(['\\', '#'])
        {
            return Err(InvalidEndpoint);
        }
        validate_escapes(input)?;
        let (scheme, rest) = input.split_once("://").ok_or(InvalidEndpoint)?;
        if !scheme.eq_ignore_ascii_case("ws") && !scheme.eq_ignore_ascii_case("wss") {
            return Err(InvalidEndpoint);
        }
        let authority = rest.split(['/', '?']).next().ok_or(InvalidEndpoint)?;
        if authority.is_empty() || authority.contains(['@', '%']) {
            return Err(InvalidEndpoint);
        }
        let raw_host = if authority.starts_with('[') {
            let end = authority.find(']').ok_or(InvalidEndpoint)?;
            &authority[..=end]
        } else {
            authority.split(':').next().ok_or(InvalidEndpoint)?
        };
        // WHATWG deliberately accepts abbreviated/octal/hex IPv4. Identity v1 does not.
        if matches!(url::Host::parse(raw_host), Ok(url::Host::Ipv4(_)))
            && raw_host.parse::<Ipv4Addr>().is_err()
        {
            return Err(InvalidEndpoint);
        }
        let (without_query, query) = input
            .split_once('?')
            .map_or((input, None), |(a, b)| (a, Some(b)));
        let normalized_path_input = normalize_path_escapes(without_query);
        let mut parsed = url::Url::parse(&normalized_path_input).map_err(|_| InvalidEndpoint)?;
        if parsed.port() == Some(0) || parsed.host_str().is_none() {
            return Err(InvalidEndpoint);
        }
        if let Some(url::Host::Domain(host)) = parsed.host()
            && let Some(stripped) = host.strip_suffix('.')
        {
            if stripped.is_empty() || stripped.ends_with('.') {
                return Err(InvalidEndpoint);
            }
            let stripped = stripped.to_owned();
            parsed
                .set_host(Some(&stripped))
                .map_err(|_| InvalidEndpoint)?;
        }
        let mut output = parsed.to_string();
        // Query bytes and ordering are identity, including an explicitly empty query.
        if let Some(query) = query {
            output.push('?');
            output.push_str(query);
        }
        if output.len() > MAX_ENDPOINT_BYTES {
            return Err(InvalidEndpoint);
        }
        Ok(Self(output))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn redacted(&self) -> String {
        let end = self.0.find("://").expect("validated scheme") + 3;
        let authority_end = self.0[end..].find('/').map_or(self.0.len(), |n| end + n);
        format!("{}/[redacted]", &self.0[..authority_end])
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Endpoint").field(&self.redacted()).finish()
    }
}

fn validate_escapes(input: &str) -> Result<(), InvalidEndpoint> {
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if !bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                || !bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit)
            {
                return Err(InvalidEndpoint);
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    Ok(())
}

fn normalize_path_escapes(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            // Checked once above; only ASCII escape digits are consumed here.
            let high = chars.next().unwrap();
            let low = chars.next().unwrap();
            let value = (high.to_digit(16).unwrap() * 16 + low.to_digit(16).unwrap()) as u8;
            if value.is_ascii_alphanumeric() || b"-._~".contains(&value) {
                result.push(char::from(value));
            } else {
                result.push('%');
                result.push(high.to_ascii_uppercase());
                result.push(low.to_ascii_uppercase());
            }
        } else {
            result.push(c);
        }
    }
    result
}
