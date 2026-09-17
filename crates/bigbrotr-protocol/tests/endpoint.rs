use bigbrotr_protocol::endpoint::Endpoint;

#[test]
fn endpoint_identity_keeps_route_distinctions_and_query_bytes() {
    for (raw, expected) in [
        ("WSS://EXAMPLE.COM.:443", "wss://example.com/"),
        (
            "ws://example.com:80/a/../b/%7e/%2f//",
            "ws://example.com/b/~/%2F//",
        ),
        ("wss://bücher.example/a", "wss://xn--bcher-kva.example/a"),
        ("wss://[2001:0db8::1]:443", "wss://[2001:db8::1]/"),
        (
            "wss://example.com/path?x=%7e&x=1+2",
            "wss://example.com/path?x=%7e&x=1+2",
        ),
        ("wss://example.com?", "wss://example.com/?"),
    ] {
        let endpoint = Endpoint::parse(raw).unwrap();
        assert_eq!(endpoint.as_str(), expected);
        assert_eq!(Endpoint::parse(expected).unwrap(), endpoint);
    }
    for pair in [
        ["ws://example.com/", "wss://example.com/"],
        ["wss://example.com/a", "wss://example.com/a/"],
        ["wss://example.com/", "wss://example.com/?"],
        ["wss://example.com/%2f", "wss://example.com//"],
    ] {
        assert_ne!(
            Endpoint::parse(pair[0]).unwrap(),
            Endpoint::parse(pair[1]).unwrap()
        );
    }
}

#[test]
fn browser_url_repairs_are_rejected_before_identity_creation() {
    for raw in [
        "https://example.com/",
        "example.com",
        "wss:example.com",
        "wss:///example.com",
        "wss://user:pass@example.com/",
        "wss://@example.com",
        "wss://example.com/#frag",
        "wss://example.com/#",
        " wss://example.com/",
        "wss://exam\nple.com/",
        "wss://example.com/a b",
        "wss://example.com\\private",
        "wss://example.com/%",
        "wss://example.com/%zz",
        "wss://example.com/?x=%a",
        "wss://127.1/",
        "wss://0x7f000001/",
        "wss://0177.0.0.1/",
        "wss://2130706433/",
        "wss://[fe80::1%25en0]/",
        "wss://example.com:0/",
        "wss://example.com:65536/",
    ] {
        assert!(Endpoint::parse(raw).is_err(), "accepted {raw:?}");
    }
}

#[test]
fn diagnostic_format_never_discloses_path_or_query() {
    let endpoint = Endpoint::parse("wss://private.example/secret/path?token=secret").unwrap();
    assert_eq!(endpoint.redacted(), "wss://private.example/[redacted]");
    assert!(!format!("{endpoint:?}").contains("secret"));
    assert!(Endpoint::parse(&format!("wss://example.com/{}", "a".repeat(8192))).is_err());
}
