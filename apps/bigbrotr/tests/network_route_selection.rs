use bigbrotr::network::{DirectPolicy, PrivateRule, RouteSelection, Target};
use bigbrotr_protocol::endpoint::Endpoint;
use std::net::IpAddr;

fn endpoint(url: &str) -> Endpoint {
    Endpoint::parse(url).unwrap()
}
fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}
fn route(cidr: &str) -> RouteSelection {
    serde_json::from_value(serde_json::json!({"class":"private","cidr":cidr})).unwrap()
}

#[test]
fn selected_route_intersects_roots_and_binds_the_complete_endpoint() {
    let ceiling = DirectPolicy::private(vec![
        PrivateRule::hostname("relay.example", "10.0.0.0/8", 443).unwrap(),
        PrivateRule::hostname("relay.example", "127.0.0.0/8", 443).unwrap(),
        PrivateRule::hostname("other.example", "192.168.0.0/16", 443).unwrap(),
    ])
    .unwrap();
    let url = endpoint("wss://relay.example/path?x=1&x=2");
    let selected = route("10.2.0.0/16")
        .policy(&url, true, Some(&ceiling))
        .unwrap();
    let target = Target::new(url).unwrap();
    let answers = [
        ip("10.2.1.1"),
        ip("10.3.1.1"),
        ip("127.0.0.1"),
        ip("192.168.1.1"),
    ];
    assert_eq!(
        selected.filter(&target, &answers).unwrap().addresses(),
        &[ip("10.2.1.1")]
    );
    for url in [
        "wss://relay.example/other?x=1&x=2",
        "wss://relay.example/path?x=2&x=1",
        "wss://relay.example:444/path?x=1&x=2",
        "wss://other.example/path?x=1&x=2",
    ] {
        assert!(
            selected
                .filter(&Target::new(endpoint(url)).unwrap(), &answers)
                .is_err(),
            "{url}"
        );
    }
    assert!(
        route("0.0.0.0/0")
            .policy(target.endpoint(), true, None)
            .is_err()
    );
    assert!(
        route("10.0.0.0/8")
            .policy(target.endpoint(), true, Some(&DirectPolicy::public()))
            .is_err()
    );
}

#[test]
fn exact_selection_never_turns_a_broad_root_into_a_metadata_exception() {
    let url = endpoint("ws://metadata.example/");
    for (broad, exact, value) in [
        ("169.254.0.0/16", "169.254.169.254/32", "169.254.169.254"),
        ("fc00::/7", "fd00:ec2::254/128", "fd00:ec2::254"),
    ] {
        let roots = |cidr| {
            DirectPolicy::private(vec![
                PrivateRule::hostname("metadata.example", cidr, 80).unwrap(),
            ])
            .unwrap()
        };
        let target = Target::new(url.clone()).unwrap();
        for (selected, root) in [(exact, broad), (broad, exact)] {
            let policy = route(selected)
                .policy(&url, false, Some(&roots(root)))
                .unwrap();
            assert!(policy.filter(&target, &[ip(value)]).is_err());
        }
        let policy = route(exact)
            .policy(&url, false, Some(&roots(exact)))
            .unwrap();
        assert_eq!(
            policy.filter(&target, &[ip(value)]).unwrap().addresses(),
            &[ip(value)]
        );
    }
}

#[test]
fn public_selection_stays_separate_and_is_also_endpoint_bound() {
    let url = endpoint("wss://relay.example/one");
    let selection: RouteSelection = serde_json::from_str(r#"{"class":"public"}"#).unwrap();
    assert!(selection.policy(&url, false, None).is_err());
    let policy = selection.policy(&url, true, None).unwrap();
    assert_eq!(
        policy
            .filter(&Target::new(url).unwrap(), &[ip("1.1.1.1")])
            .unwrap()
            .addresses(),
        &[ip("1.1.1.1")]
    );
    assert!(
        policy
            .filter(
                &Target::new(endpoint("wss://relay.example/two")).unwrap(),
                &[ip("1.1.1.1")]
            )
            .is_err()
    );
}

#[test]
fn route_spelling_is_closed_bounded_and_has_fixed_independent_bytes() {
    for text in [
        r#"{"class":"private"}"#,
        r#"{"class":"private","cidr":"10.0.0.1/8"}"#,
        r#"{"class":"public","cidr":"10.0.0.0/8"}"#,
        r#"{"class":"tor"}"#,
        r#"{"class":"private","cidr":"::ffff:10.0.0.0/104"}"#,
    ] {
        assert!(
            serde_json::from_str::<RouteSelection>(text).is_err(),
            "{text}"
        );
    }
    assert_eq!(
        route("10.2.0.0/16").canonical_bytes(),
        vec![1, 4, 16, 10, 2, 0, 0]
    );
    let ipv6 = route("FD12:0000:0000:0000:0000:0000:0000:0000/16");
    assert_eq!(
        serde_json::to_value(ipv6).unwrap(),
        serde_json::json!({"class":"private","cidr":"fd12::/16"})
    );
    assert_eq!(
        ipv6.canonical_bytes(),
        vec![
            1, 6, 16, 0xfd, 0x12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ]
    );
    let public: RouteSelection = serde_json::from_str(r#"{"class":"public"}"#).unwrap();
    assert_eq!(public.canonical_bytes(), vec![0]);
    assert!(!format!("{ipv6:?}").contains("fd12"));
}

#[test]
fn fresh_enrollment_requires_complete_containment_in_one_matching_root() {
    let url = endpoint("wss://relay.example/private?x=1");
    let roots = DirectPolicy::private(vec![
        PrivateRule::hostname("relay.example", "10.2.0.0/16", 443).unwrap(),
        PrivateRule::hostname("other.example", "10.0.0.0/8", 443).unwrap(),
        PrivateRule::hostname("relay.example", "10.0.0.0/8", 444).unwrap(),
    ])
    .unwrap();
    for cidr in ["10.2.0.0/16", "10.2.1.0/24", "10.2.1.1/32"] {
        assert!(
            route(cidr)
                .validate_enrollment(&url, false, Some(&roots))
                .is_ok()
        );
    }
    for cidr in ["10.0.0.0/8", "10.3.0.0/16", "::/0"] {
        assert!(
            route(cidr)
                .validate_enrollment(&url, true, Some(&roots))
                .is_err()
        );
    }
    let metadata = DirectPolicy::private(vec![
        PrivateRule::hostname("relay.example", "169.254.0.0/16", 443).unwrap(),
    ])
    .unwrap();
    assert!(
        route("169.254.169.254/32")
            .validate_enrollment(&url, true, Some(&metadata))
            .is_err()
    );
    // A static selected policy cannot be passed back as a replacement ceiling.
    let selected = route("10.2.1.0/24")
        .policy(&url, false, Some(&roots))
        .unwrap();
    assert!(
        route("10.2.0.0/16")
            .policy(&url, false, Some(&selected))
            .is_err()
    );
}
