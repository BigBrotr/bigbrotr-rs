//! Deployment destination ceilings. Parsing does no I/O and confers no authority.
use bigbrotr::network::{DirectPolicy, PrivateRule};
use bigbrotr_protocol::endpoint::Endpoint;
use serde_json::{Value, json};

/// Preserve the single transport validator for hostname/CIDR/port semantics.
/// The normalized value is diagnostic configuration, never stored product policy.
/// Duplicate rules consume capacity but do not change the permitted destination set.
pub(super) fn parse(value: &Value) -> Result<(Value, Option<DirectPolicy>), &'static str> {
    let entries = value.as_array().ok_or("expected private rule array")?;
    if entries.len() > 256 {
        return Err("private rules exceed 256 entries");
    }
    let mut normalized = Vec::with_capacity(entries.len());
    let mut rules = Vec::with_capacity(entries.len());
    for entry in entries {
        let fields = entry.as_object().ok_or("expected closed private rule")?;
        let endpoint = fields.get("endpoint");
        let hostname = fields.get("hostname");
        if fields.len() != 3 || endpoint.is_some() == hostname.is_some() {
            return Err("private rule requires exactly endpoint or hostname, cidr and port");
        }
        let cidr = fields
            .get("cidr")
            .and_then(Value::as_str)
            .ok_or("private rule requires CIDR text")?;
        let port = fields
            .get("port")
            .and_then(Value::as_u64)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port != 0)
            .ok_or("private rule requires integer port in 1..65535")?;
        let (rule, normalized_subject) = if let Some(endpoint) = endpoint {
            let endpoint = endpoint.as_str().ok_or("private endpoint must be text")?;
            let endpoint = Endpoint::parse(endpoint)
                .map_err(|_| "invalid private endpoint (value withheld)")?;
            let subject = json!(endpoint.as_str());
            (
                PrivateRule::endpoint(endpoint, cidr, port)
                    .map_err(|_| "invalid private destination rule (values withheld)")?,
                ("endpoint", subject),
            )
        } else {
            let hostname = hostname
                .and_then(Value::as_str)
                .ok_or("private hostname must be text")?;
            (
                PrivateRule::hostname(hostname, cidr, port)
                    .map_err(|_| "invalid private destination rule (values withheld)")?,
                ("hostname", json!(hostname)),
            )
        };
        rules.push(rule);
        let (key, subject) = normalized_subject;
        normalized.push(json!({key:subject,"cidr":cidr,"port":port}));
    }
    let policy = if rules.is_empty() {
        None
    } else {
        Some(DirectPolicy::private(rules).map_err(|_| "invalid private rule count")?)
    };
    Ok((Value::Array(normalized), policy))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr::network::Target;

    #[test]
    fn parsed_ceiling_keeps_endpoint_path_query_port_and_address_intersection() {
        let (normalized, policy) = parse(&json!([
            {"endpoint":"WS://127.0.0.1:7447/a/../private?x=1&x=2", "cidr":"127.0.0.1/32", "port":7447},
            {"hostname":"relay.example", "cidr":"10.0.0.0/8", "port":443}
        ])).unwrap();
        assert_eq!(
            normalized[0]["endpoint"],
            "ws://127.0.0.1:7447/private?x=1&x=2"
        );
        let policy = policy.unwrap();
        for (url, addresses, expected) in [
            (
                "ws://127.0.0.1:7447/private?x=1&x=2",
                vec!["127.0.0.1"],
                vec!["127.0.0.1"],
            ),
            (
                "ws://127.0.0.1:7447/private?x=2&x=1",
                vec!["127.0.0.1"],
                vec![],
            ),
            (
                "ws://127.0.0.1:7447/public?x=1&x=2",
                vec!["127.0.0.1"],
                vec![],
            ),
            (
                "ws://127.0.0.1:7448/private?x=1&x=2",
                vec!["127.0.0.1"],
                vec![],
            ),
            (
                "wss://relay.example/any-path",
                vec!["10.0.0.1", "127.0.0.1", "8.8.8.8"],
                vec!["10.0.0.1"],
            ),
            ("wss://other.example/", vec!["10.0.0.1"], vec![]),
            ("wss://relay.example:444/", vec!["10.0.0.1"], vec![]),
        ] {
            let target = Target::new(Endpoint::parse(url).unwrap()).unwrap();
            let addresses: Vec<_> = addresses.iter().map(|s| s.parse().unwrap()).collect();
            let result = policy.filter(&target, &addresses);
            if expected.is_empty() {
                assert!(result.is_err(), "{url}");
            } else {
                let expected: Vec<std::net::IpAddr> =
                    expected.iter().map(|s| s.parse().unwrap()).collect();
                assert_eq!(result.unwrap().addresses(), expected, "{url}");
            }
        }
        assert!(parse(&json!([])).unwrap().1.is_none());
    }
}
