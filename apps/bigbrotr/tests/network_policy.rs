use bigbrotr::network::{DirectPolicy, PolicyError, PrivateRule, Target};
use bigbrotr_protocol::endpoint::Endpoint;
use std::net::IpAddr;

fn target(url: &str) -> Target {
    Target::new(Endpoint::parse(url).unwrap()).unwrap()
}

fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}

#[test]
fn public_route_filters_every_answer_and_canonicalizes_mapped_addresses() {
    let policy = DirectPolicy::public();
    let endpoint = target("wss://Example.COM:443/A%2fb?token=private&x=1");
    let answers = [
        ip("127.0.0.1"),
        ip("1.1.1.1"),
        ip("::ffff:10.0.0.1"),
        ip("::ffff:1.1.1.1"),
        ip("2606:4700:4700::1111"),
    ];
    let selected = policy.filter(&endpoint, &answers).unwrap();
    assert_eq!(
        selected.addresses(),
        &[ip("1.1.1.1"), ip("2606:4700:4700::1111")]
    );
    assert_eq!(selected.excluded(), 2);
    assert_eq!(endpoint.host(), "example.com");
    assert_eq!(endpoint.port(), 443);
    assert_eq!(
        endpoint.endpoint().as_str(),
        "wss://example.com/A%2Fb?token=private&x=1"
    );
    assert!(!format!("{endpoint:?}").contains("private"));
}

#[test]
fn public_route_refuses_special_purpose_and_unallocated_addresses() {
    let endpoint = target("ws://relay.example/");
    let policy = DirectPolicy::public();
    // Literal expectations: special-purpose registries and global IPv6 allocations.
    // No DNS, packet delivery, or reachability is inferred from this pure test.
    for value in [
        "0.0.0.1",
        "10.255.255.255",
        "100.64.0.0",
        "100.127.255.255",
        "127.255.255.255",
        "169.254.169.254",
        "172.16.0.0",
        "172.31.255.255",
        "192.0.0.9",
        "192.0.2.1",
        "192.31.196.1",
        "192.52.193.1",
        "192.88.99.1",
        "192.168.1.1",
        "192.175.48.1",
        "198.18.0.0",
        "198.19.255.255",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "239.255.255.255",
        "240.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "::127.0.0.1",
        "::ffff:169.254.169.254",
        "64:ff9b::7f00:1",
        "64:ff9b:1::1",
        "100::1",
        "100:0:0:1::1",
        "2001::1",
        "2001:1::1",
        "2001:2::1",
        "2001:20::1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "2620:4f:8000::1",
        "3ffe::1",
        "3fff::1",
        "5f00::1",
        "fc00::1",
        "fe80::1",
        "fec0::1",
        "ff02::1",
        "2001:1000::1",
        "2003:4000::1",
        "2500::1",
        "2d00::1",
        "3000::1",
    ] {
        assert_eq!(
            policy.filter(&endpoint, &[ip(value)]),
            Err(PolicyError::NoAllowedAddress),
            "{value}"
        );
    }
    for value in [
        "1.1.1.1",
        "100.63.255.255",
        "100.128.0.0",
        "172.15.255.255",
        "172.32.0.0",
        "198.17.255.255",
        "198.20.0.0",
        "223.255.255.255",
        "2001:4860:4860::8888",
        "2001:200::1",
        "2003:3fff::1",
        "2400::1",
        "2410::1",
        "2606:4700::1",
        "2610::1",
        "2620::1",
        "2630::1",
        "2800::1",
        "2a00::1",
        "2a10::1",
        "2c00::1",
    ] {
        assert_eq!(
            policy.filter(&endpoint, &[ip(value)]).unwrap().addresses(),
            &[ip(value)],
            "{value}"
        );
    }
}

#[test]
fn private_route_requires_the_same_endpoint_cidr_and_port() {
    let endpoint = Endpoint::parse("ws://relay.example:7777/private?key=one").unwrap();
    let rule = PrivateRule::endpoint(endpoint.clone(), "127.0.0.0/8", 7777).unwrap();
    let policy = DirectPolicy::private(vec![rule]).unwrap();
    let selected = policy
        .filter(
            &Target::new(endpoint).unwrap(),
            &[ip("127.0.0.2"), ip("1.1.1.1")],
        )
        .unwrap();
    assert_eq!(selected.addresses(), &[ip("127.0.0.2")]);
    for url in [
        "ws://relay.example:7778/private?key=one",
        "ws://relay.example:7777/private?key=two",
        "ws://other.example:7777/private?key=one",
        "wss://relay.example:7777/private?key=one",
    ] {
        assert_eq!(
            policy.filter(&target(url), &[ip("127.0.0.2")]),
            Err(PolicyError::NoAllowedAddress)
        );
    }
    assert_eq!(
        DirectPolicy::public().filter(
            &target("ws://relay.example:7777/private?key=one"),
            &[ip("127.0.0.2")]
        ),
        Err(PolicyError::NoAllowedAddress)
    );
}

#[test]
fn hostname_rule_is_exact_but_allows_paths_within_its_port_and_cidr() {
    let policy = DirectPolicy::private(vec![
        PrivateRule::hostname("relay.example", "10.2.0.0/16", 443).unwrap(),
    ])
    .unwrap();
    for path in ["a", "b?query=one"] {
        let endpoint = target(&format!("wss://relay.example/{path}"));
        assert_eq!(
            policy
                .filter(&endpoint, &[ip("::ffff:10.2.3.4")])
                .unwrap()
                .addresses(),
            &[ip("10.2.3.4")]
        );
    }
    for url in [
        "wss://sub.relay.example/",
        "wss://relay.example.evil/",
        "wss://relay.example:444/",
    ] {
        assert_eq!(
            policy.filter(&target(url), &[ip("10.2.3.4")]),
            Err(PolicyError::NoAllowedAddress)
        );
    }
    assert_eq!(
        policy.filter(&target("wss://relay.example/"), &[ip("10.3.0.1")]),
        Err(PolicyError::NoAllowedAddress)
    );
}

#[test]
fn overlay_names_are_rejected_before_any_direct_resolution_even_with_private_rules() {
    for host in [
        "relay.onion",
        "onion",
        "relay.i2p",
        "i2p",
        "relay.loki",
        "loki",
        "relay.snode",
        "snode",
    ] {
        assert_eq!(
            Target::new(Endpoint::parse(&format!("wss://{host}/")).unwrap()),
            Err(PolicyError::OverlayRouteRequired)
        );
        assert!(PrivateRule::hostname(host, "127.0.0.1/32", 443).is_err());
    }
    assert!(Target::new(Endpoint::parse("wss://onion.example/").unwrap()).is_ok());
}

#[test]
fn link_local_requires_an_explicit_single_address_and_unicast_is_always_required() {
    let endpoint = target("ws://metadata.example:8888/");
    let broad = DirectPolicy::private(vec![
        PrivateRule::hostname("metadata.example", "169.254.0.0/16", 8888).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        broad.filter(&endpoint, &[ip("169.254.169.254")]),
        Err(PolicyError::NoAllowedAddress)
    );
    let exact = DirectPolicy::private(vec![
        PrivateRule::hostname("metadata.example", "169.254.169.254/32", 8888).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        exact
            .filter(&endpoint, &[ip("169.254.169.254")])
            .unwrap()
            .addresses(),
        &[ip("169.254.169.254")]
    );
    for (cidr, value) in [
        ("0.0.0.0/0", "0.0.0.0"),
        ("0.0.0.0/0", "255.255.255.255"),
        ("0.0.0.0/0", "224.0.0.1"),
        ("::/0", "::"),
        ("::/0", "ff02::1"),
    ] {
        let policy = DirectPolicy::private(vec![
            PrivateRule::hostname("metadata.example", cidr, 8888).unwrap(),
        ])
        .unwrap();
        assert_eq!(
            policy.filter(&endpoint, &[ip(value)]),
            Err(PolicyError::NoAllowedAddress)
        );
    }
}

#[test]
fn policy_input_is_bounded_and_ambiguous_rules_are_refused() {
    for cidr in [
        "127.0.0.1/8",
        "127.0.0.0/33",
        "::/129",
        "::ffff:127.0.0.0/104",
        "127.0.0.0/-1",
        "127.0.0.0/08",
        "127.0.0.0",
        " 127.0.0.0/8",
    ] {
        assert!(
            PrivateRule::hostname("relay.example", cidr, 80).is_err(),
            "{cidr}"
        );
    }
    for host in [
        "*.example",
        "user@relay.example",
        "relay.example/path",
        "relay.example:80",
        "relay.example.",
        "Relay.Example",
        "",
        "127.0.0.1",
    ] {
        assert!(
            PrivateRule::hostname(host, "127.0.0.0/8", 80).is_err(),
            "{host}"
        );
    }
    assert!(PrivateRule::hostname("relay.example", "127.0.0.0/8", 0).is_err());
    assert!(
        PrivateRule::endpoint(
            Endpoint::parse("ws://relay.example:88/").unwrap(),
            "127.0.0.0/8",
            80
        )
        .is_err()
    );
    assert!(DirectPolicy::private(vec![]).is_err());
    let rule = PrivateRule::hostname("relay.example", "127.0.0.0/8", 80).unwrap();
    assert!(DirectPolicy::private(vec![rule; 257]).is_err());
    assert_eq!(
        DirectPolicy::public().filter(&target("ws://relay.example/"), &vec![ip("1.1.1.1"); 65]),
        Err(PolicyError::AnswerLimit)
    );
    assert_eq!(
        DirectPolicy::public().filter(&target("ws://relay.example/"), &[]),
        Err(PolicyError::NoAllowedAddress)
    );
}

#[test]
fn cloud_metadata_exceptions_require_single_addresses_even_inside_a_private_cidr() {
    let endpoint = target("ws://metadata.example/");
    for (broad, exact, value) in [
        ("fc00::/7", "fd00:ec2::254/128", "fd00:ec2::254"),
        ("fc00::/7", "fd00:ec2::23/128", "fd00:ec2::23"),
        ("fc00::/7", "fd20:ce::254/128", "fd20:ce::254"),
        ("100.64.0.0/10", "100.100.100.200/32", "100.100.100.200"),
        ("168.63.0.0/16", "168.63.129.16/32", "168.63.129.16"),
    ] {
        let private = |cidr| {
            DirectPolicy::private(vec![
                PrivateRule::hostname("metadata.example", cidr, 80).unwrap(),
            ])
            .unwrap()
        };
        assert_eq!(
            private(broad).filter(&endpoint, &[ip(value)]),
            Err(PolicyError::NoAllowedAddress),
            "{value}"
        );
        assert_eq!(
            private(exact)
                .filter(&endpoint, &[ip(value)])
                .unwrap()
                .addresses(),
            &[ip(value)]
        );
        assert_eq!(
            DirectPolicy::public().filter(&endpoint, &[ip(value)]),
            Err(PolicyError::NoAllowedAddress)
        );
    }
}

#[test]
fn literal_targets_cannot_substitute_an_unrelated_address() {
    let endpoint = target("wss://[::ffff:1.1.1.1]/");
    assert_eq!(endpoint.literal_address(), Some(ip("1.1.1.1")));
    assert_eq!(
        DirectPolicy::public().filter(&endpoint, &[ip("8.8.8.8")]),
        Err(PolicyError::NoAllowedAddress)
    );
    assert_eq!(
        DirectPolicy::public()
            .filter(&endpoint, &[ip("1.1.1.1")])
            .unwrap()
            .addresses(),
        &[ip("1.1.1.1")]
    );
}
