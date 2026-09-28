//! Current policy data only. A ban set neither resolves nor grants a route.
use bigbrotr_protocol::endpoint::Endpoint;
use serde_json::{Value, json};
use std::{cmp::Ordering, fmt, net::IpAddr};

#[derive(Eq, PartialEq, Ord, PartialOrd)]
enum Subject {
    Endpoint(Endpoint),
    Host(String),
}

/// Sorted, normalized exact subjects. Caller owns the policy parse reservation
/// and must bind this data to current accepted authority before using it.
pub struct BanSet(Vec<Subject>);

impl fmt::Debug for BanSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BanSet")
            .field("subjects", &self.0.len())
            .finish()
    }
}

impl BanSet {
    pub fn parse(value: &Value) -> Result<Self, &'static str> {
        let rules = value.as_array().ok_or("expected ban rule array")?;
        if rules.len() > 100_000 {
            return Err("ban array exceeds 100000 entries");
        }
        let mut subjects = Vec::new();
        subjects
            .try_reserve_exact(rules.len())
            .map_err(|_| "ban allocation unavailable")?;
        let mut input_bytes = 0_usize;
        for rule in rules {
            let object = rule
                .as_object()
                .filter(|o| o.len() == 1)
                .ok_or("ban rule requires exactly endpoint or host")?;
            let (kind, value) = object.iter().next().expect("one member");
            let text = value.as_str().ok_or("ban subject must be a string")?;
            input_bytes = input_bytes
                .checked_add(text.len())
                .filter(|&n| n <= 10 * 1024 * 1024)
                .ok_or("ban subjects exceed document byte limit")?;
            subjects.push(match kind.as_str() {
                "endpoint" => {
                    Subject::Endpoint(Endpoint::parse(text).map_err(|_| "invalid ban endpoint")?)
                }
                "host" => Subject::Host(parse_host(text)?),
                _ => return Err("ban rule requires exactly endpoint or host"),
            });
        }
        subjects.sort_unstable();
        subjects.dedup();
        Ok(Self(subjects))
    }

    pub fn normalized_value(&self) -> Value {
        Value::Array(
            self.0
                .iter()
                .map(|subject| match subject {
                    Subject::Endpoint(endpoint) => json!({"endpoint":endpoint.as_str()}),
                    Subject::Host(host) => json!({"host":host}),
                })
                .collect(),
        )
    }

    pub fn contains(&self, endpoint: &Endpoint) -> bool {
        if self
            .0
            .binary_search_by(|subject| match subject {
                Subject::Endpoint(banned) => banned.cmp(endpoint),
                Subject::Host(_) => Ordering::Greater,
            })
            .is_ok()
        {
            return true;
        }
        let url = url::Url::parse(endpoint.as_str()).expect("normalized endpoint");
        let Ok(host) = host_key(url.host().expect("normalized host")) else {
            return false;
        };
        self.0
            .binary_search_by(|subject| match subject {
                Subject::Endpoint(_) => Ordering::Less,
                Subject::Host(banned) => banned.cmp(&host),
            })
            .is_ok()
    }
}

fn parse_host(input: &str) -> Result<String, &'static str> {
    if input.len() > 8192
        || input.contains(['/', '?', '#', '@', '\\', '%'])
        || (input.contains(':') && !(input.starts_with('[') && input.ends_with(']')))
    {
        return Err("invalid exact ban host");
    }
    // Reuse the versioned endpoint normalizer for IDNA, root dots and strict
    // literal addresses. This synthetic URL never becomes a registry identity.
    let endpoint =
        Endpoint::parse(&format!("wss://{input}/")).map_err(|_| "invalid exact ban host")?;
    let url = url::Url::parse(endpoint.as_str()).expect("normalized endpoint");
    host_key(url.host().expect("normalized host"))
}

fn host_key(host: url::Host<&str>) -> Result<String, &'static str> {
    match host {
        url::Host::Domain(host) => {
            super::policy::dns_hostname(host).map_err(|_| "invalid exact ban hostname")?;
            Ok(host.to_owned())
        }
        url::Host::Ipv4(address) => Ok(address.to_string()),
        url::Host::Ipv6(address) => match super::address::canonical(IpAddr::V6(address)) {
            IpAddr::V4(address) => Ok(address.to_string()),
            IpAddr::V6(address) => Ok(format!("[{address}]")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_endpoint_and_host_bans_have_distinct_matching_boundaries() {
        let bans = BanSet::parse(&json!([
            {"endpoint":"WSS://RELAY.example.:443/x/../private?a=1&a=2"},
            {"host":"BÜCHER.example."}, {"host":"[::ffff:127.0.0.1]"},
            {"host":"[2001:db8::1]"}, {"host":"relay.onion"}
        ]))
        .unwrap();
        for endpoint in [
            "wss://relay.example/private?a=1&a=2",
            "ws://xn--bcher-kva.example:9000/anything?private=true",
            "wss://127.0.0.1:7447/",
            "ws://[::ffff:7f00:1]/",
            "ws://[2001:db8::1]:443/",
            "wss://relay.onion/",
        ] {
            assert!(
                bans.contains(&Endpoint::parse(endpoint).unwrap()),
                "{endpoint}"
            );
        }
        for endpoint in [
            "ws://relay.example/private?a=1&a=2",
            "wss://relay.example:9000/private?a=1&a=2",
            "wss://relay.example/private/?a=1&a=2",
            "wss://relay.example/private?a=2&a=1",
            "wss://relay.example/private?a=1&a=2&b=3",
            "wss://sub.xn--bcher-kva.example/",
            "ws://localhost/",
            "ws://127.0.0.2/",
            "ws://[2001:db8::2]/",
            "wss://sub.relay.onion/",
        ] {
            assert!(
                !bans.contains(&Endpoint::parse(endpoint).unwrap()),
                "{endpoint}"
            );
        }
        // Host canonicalization does not change endpoint identity v1.
        let endpoint_only =
            BanSet::parse(&json!([{"endpoint":"ws://[::ffff:127.0.0.1]/"}])).unwrap();
        assert!(!endpoint_only.contains(&Endpoint::parse("ws://127.0.0.1/").unwrap()));
    }

    #[test]
    fn ban_bounds_apply_before_deduplication_and_dns_labels_are_bounded() {
        let rule = json!({"host":"relay.example"});
        let mut rules = vec![rule.clone(); 100_000];
        assert_eq!(
            BanSet::parse(&json!(rules)).unwrap().normalized_value(),
            json!([rule])
        );
        rules.push(json!({"host":"relay.example"}));
        assert!(BanSet::parse(&json!(rules)).is_err());
        assert!(BanSet::parse(&json!([{"host":format!("{}.example", "a".repeat(63))}])).is_ok());
        assert!(BanSet::parse(&json!([{"host":format!("{}.example", "a".repeat(64))}])).is_err());
        let hostname = [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61),
        ]
        .join(".");
        assert_eq!(hostname.len(), 253);
        assert!(BanSet::parse(&json!([{"host":hostname}])).is_ok());
        assert!(BanSet::parse(&json!([{"host":format!("{hostname}d")}])).is_err());
    }
}
