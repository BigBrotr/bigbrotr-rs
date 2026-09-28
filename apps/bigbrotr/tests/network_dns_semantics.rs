use bigbrotr::network::{DnsError, DnsFamilyOutcome, DnsResolution, DnsResolver, Target};
use bigbrotr::runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{net::IpAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::{Instant, timeout},
};

// Literal RFC1035 wire fixtures: no production or upstream DNS encoder/parser.
fn name(host: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for label in host.split('.') {
        bytes.push(label.len() as u8);
        bytes.extend_from_slice(label.as_bytes());
    }
    bytes.push(0);
    bytes
}

fn query_identity(bytes: &[u8]) -> (String, u16, usize) {
    let mut labels = Vec::new();
    let mut at = 12;
    while bytes[at] != 0 {
        let length = usize::from(bytes[at]);
        labels.push(std::str::from_utf8(&bytes[at + 1..at + 1 + length]).unwrap());
        at += 1 + length;
    }
    (
        labels.join("."),
        u16::from_be_bytes([bytes[at + 1], bytes[at + 2]]),
        at + 5,
    )
}

fn record(host: &str, kind: u16, data: &[u8]) -> Vec<u8> {
    let mut bytes = name(host);
    bytes.extend_from_slice(&kind.to_be_bytes());
    bytes.extend_from_slice(&[0, 1, 0, 0, 0, 60]);
    bytes.extend_from_slice(&(data.len() as u16).to_be_bytes());
    bytes.extend_from_slice(data);
    bytes
}

fn response(query: &[u8], rcode: u8, records: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = vec![
        query[0],
        query[1],
        0x81,
        0x80 | rcode,
        0,
        1,
        0,
        records.len() as u8,
        0,
        0,
        0,
        0,
    ];
    bytes.extend_from_slice(&query[12..query_identity(query).2]);
    for rr in records {
        bytes.extend_from_slice(rr);
    }
    bytes
}

async fn run(
    host: &str,
    mut respond: impl FnMut(&[u8]) -> Vec<u8> + Send + 'static,
) -> (Result<DnsResolution, DnsError>, Vec<(String, u16)>) {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resources = ResourceBudget::new(
        ProcessEnvelope::new(
            4 * 1024 * 1024,
            0,
            ResourceLimits::new(4 * 1024 * 1024, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let resolver = DnsResolver::new(server.local_addr().unwrap(), resources.clone()).unwrap();
    let (stop, mut stopped) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut queries = Vec::new();
        loop {
            let (mut socket, _) = tokio::select! {
                accepted = server.accept() => accepted.unwrap(),
                _ = &mut stopped => break,
            };
            let length = socket.read_u16().await.unwrap();
            let mut query = vec![0; usize::from(length)];
            socket.read_exact(&mut query).await.unwrap();
            let (host, kind, _) = query_identity(&query);
            queries.push((host, kind));
            let answer = respond(&query);
            socket.write_u16(answer.len() as u16).await.unwrap();
            socket.write_all(&answer).await.unwrap();
        }
        queries
    });
    let target = Target::new(Endpoint::parse(&format!("wss://{host}/")).unwrap()).unwrap();
    let result = resolver
        .resolve(&target, Instant::now() + Duration::from_secs(2))
        .await;
    stop.send(()).unwrap();
    let queries = timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
    (result, queries)
}

#[tokio::test(flavor = "current_thread")]
async fn unrelated_srv_cannot_redirect_address_resolution() {
    let (result, queries) = run("fixture.example", |query| {
        let (host, kind, _) = query_identity(query);
        let records = if host == "fixture.example" {
            let mut srv = vec![0, 0, 0, 0, 1, 187];
            srv.extend_from_slice(&name("other.example"));
            vec![record("attacker.example", 33, &srv)]
        } else {
            vec![record(
                &host,
                kind,
                if kind == 1 { &[1, 1, 1, 1] } else { &[0; 16] },
            )]
        };
        response(query, 0, &records)
    })
    .await;
    assert_eq!(
        queries,
        vec![
            ("fixture.example".into(), 1),
            ("fixture.example".into(), 28)
        ]
    );
    assert_eq!(result.unwrap_err(), DnsError::NoRecords);
}

#[tokio::test(flavor = "current_thread")]
async fn negative_and_unknown_rcodes_cannot_return_addresses() {
    for code in [3, 15] {
        let (result, _) = run("fixture.example", move |query| {
            let (host, kind, _) = query_identity(query);
            response(
                query,
                code,
                &[record(
                    &host,
                    kind,
                    if kind == 1 { &[1, 1, 1, 1] } else { &[0; 16] },
                )],
            )
        })
        .await;
        assert_eq!(result.unwrap_err(), DnsError::InvalidResponse);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cname_owner_cannot_also_supply_address_data() {
    let (result, _) = run("fixture.example", |query| {
        response(
            query,
            0,
            &[
                record("fixture.example", 5, &name("alias.example")),
                record("fixture.example", 1, &[1, 1, 1, 1]),
                record("alias.example", 1, &[8, 8, 8, 8]),
            ],
        )
    })
    .await;
    assert_eq!(result.unwrap_err(), DnsError::InvalidResponse);
}

#[tokio::test(flavor = "current_thread")]
async fn cross_packet_cycles_are_not_reported_as_absence() {
    let (result, queries) = run("fixture.example", |query| {
        let (host, _, _) = query_identity(query);
        let alias = if host == "fixture.example" {
            "alias.example"
        } else {
            "fixture.example"
        };
        response(query, 0, &[record(&host, 5, &name(alias))])
    })
    .await;
    assert_eq!(result.unwrap_err(), DnsError::InvalidResponse);
    assert_eq!(queries.len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cname_depth_exhaustion_is_a_limit_not_absence() {
    let mut hop = 0;
    let (result, _) = run("fixture.example", move |query| {
        let (host, _, _) = query_identity(query);
        hop += 1;
        response(
            query,
            0,
            &[record(&host, 5, &name(&format!("alias{hop}.example")))],
        )
    })
    .await;
    assert_eq!(result.unwrap_err(), DnsError::ResponseLimit);
}

#[tokio::test(flavor = "current_thread")]
async fn special_local_names_are_refused_without_synthesizing_dns_observations() {
    for host in [
        "localhost",
        "host.localhost",
        "1.0.0.127.in-addr.arpa",
        "name.invalid",
    ] {
        let (result, queries) = run(host, |query| response(query, 0, &[])).await;
        assert_eq!(result.unwrap_err(), DnsError::NamePolicy, "{host}");
        assert!(queries.is_empty());
    }
    let (result, queries) = run("fixture.example", |query| {
        response(
            query,
            0,
            &[record("fixture.example", 5, &name("localhost"))],
        )
    })
    .await;
    assert_eq!(result.unwrap_err(), DnsError::NamePolicy);
    assert_eq!(queries.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn additional_only_addresses_do_not_establish_an_answer() {
    let (result, _) = run("fixture.example", |query| {
        let (host, kind, _) = query_identity(query);
        let mut bytes = response(
            query,
            0,
            &[record(
                &host,
                kind,
                if kind == 1 { &[1, 1, 1, 1] } else { &[0; 16] },
            )],
        );
        if kind == 1 {
            bytes[7] = 0;
            bytes[11] = 1;
        }
        bytes
    })
    .await;
    let result = result.unwrap();
    assert_eq!(result.ipv4, DnsFamilyOutcome::NoRecords);
    assert_eq!(result.ipv6, DnsFamilyOutcome::Answered);
    assert_eq!(result.addresses(), &["::".parse::<IpAddr>().unwrap()]);
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_header_bit_is_malformed_even_when_the_codec_discards_it() {
    let (result, queries) = run("fixture.example", |query| {
        let mut bytes = response(query, 0, &[]);
        bytes[3] |= 0x40;
        bytes
    })
    .await;
    assert_eq!(result.unwrap_err(), DnsError::InvalidResponse);
    assert_eq!(queries.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn negative_cname_and_server_failure_keep_distinct_outcomes() {
    for code in [3, 2, 5] {
        let (result, queries) = run("fixture.example", move |query| {
            response(
                query,
                code,
                &[record("fixture.example", 5, &name("alias.example"))],
            )
        })
        .await;
        assert_eq!(
            result.unwrap_err(),
            if code == 3 {
                DnsError::NoRecords
            } else {
                DnsError::Unavailable
            }
        );
        assert_eq!(
            queries,
            vec![
                ("fixture.example".into(), 1),
                ("fixture.example".into(), 28)
            ]
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn eight_cross_packet_aliases_per_family_resolve_before_the_limit() {
    let (result, queries) = run("fixture.example", |query| {
        let (host, kind, _) = query_identity(query);
        let hop: u8 = if host == "fixture.example" {
            0
        } else {
            host.strip_prefix("alias")
                .unwrap()
                .strip_suffix(".example")
                .unwrap()
                .parse()
                .unwrap()
        };
        let rr = if hop == 8 {
            record(
                &host,
                kind,
                if kind == 1 { &[1, 1, 1, 1] } else { &[0; 16] },
            )
        } else {
            record(&host, 5, &name(&format!("alias{}.example", hop + 1)))
        };
        response(query, 0, &[rr])
    })
    .await;
    let result = result.unwrap();
    assert_eq!(
        result.addresses(),
        &["1.1.1.1".parse::<IpAddr>().unwrap(), "::".parse().unwrap()]
    );
    assert_eq!(queries.len(), 18);
    for family in [1, 28] {
        let expected: Vec<_> = std::iter::once(("fixture.example".to_owned(), family))
            .chain((1..=8).map(|n| (format!("alias{n}.example"), family)))
            .collect();
        assert_eq!(
            queries
                .iter()
                .filter(|(_, kind)| *kind == family)
                .cloned()
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn packet_order_cannot_change_the_terminal_alias() {
    let (result, queries) = run("fixture.example", |query| {
        let (_, kind, _) = query_identity(query);
        response(
            query,
            0,
            &[
                record(
                    "final.example",
                    kind,
                    if kind == 1 { &[8, 8, 8, 8] } else { &[0; 16] },
                ),
                record("alias.example", 5, &name("final.example")),
                record("fixture.example", 5, &name("alias.example")),
            ],
        )
    })
    .await;
    assert_eq!(
        result.unwrap().addresses(),
        &["8.8.8.8".parse::<IpAddr>().unwrap(), "::".parse().unwrap()]
    );
    assert_eq!(queries.len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn address_count_is_bounded_across_both_families() {
    for total in [64, 65] {
        let (result, _) = run("fixture.example", move |query| {
            let (host, kind, _) = query_identity(query);
            let count = if kind == 1 { 63 } else { total - 63 };
            let records = (0..count)
                .map(|_| {
                    record(
                        &host,
                        kind,
                        if kind == 1 { &[1, 1, 1, 1] } else { &[0; 16] },
                    )
                })
                .collect::<Vec<_>>();
            response(query, 0, &records)
        })
        .await;
        if total == 64 {
            assert_eq!(result.unwrap().addresses().len(), 64);
        } else {
            assert_eq!(result.unwrap_err(), DnsError::ResponseLimit);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exact_frame_bound_accepts_valid_data_with_unrelated_unknown_records() {
    let (result, _) = run("fixture.example", |query| {
        let (host, kind, _) = query_identity(query);
        let mut records = vec![record(
            &host,
            kind,
            if kind == 1 { &[1, 1, 1, 1] } else { &[0; 16] },
        )];
        let size = response(query, 0, &records).len() + record("other.example", 65000, &[]).len();
        records.push(record("other.example", 65000, &vec![0; 4096 - size]));
        let bytes = response(query, 0, &records);
        assert_eq!(bytes.len(), 4096);
        bytes
    })
    .await;
    assert_eq!(result.unwrap().addresses().len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_negative_aliases_cannot_hide_behind_another_family() {
    for case in 0..3 {
        let (result, queries) = run("fixture.example", move |query| {
            let (_, kind, _) = query_identity(query);
            if kind == 28 {
                return response(query, 0, &[record("fixture.example", 28, &[0; 16])]);
            }
            let aliases = match case {
                0 => vec![
                    record("fixture.example", 5, &name("alias.example")),
                    record("alias.example", 5, &name("fixture.example")),
                ],
                1 => vec![
                    record("fixture.example", 5, &name("one.example")),
                    record("fixture.example", 5, &name("two.example")),
                ],
                2 => (0..9)
                    .map(|hop| {
                        let owner = if hop == 0 {
                            "fixture.example".into()
                        } else {
                            format!("alias{hop}.example")
                        };
                        record(&owner, 5, &name(&format!("alias{}.example", hop + 1)))
                    })
                    .collect(),
                _ => unreachable!(),
            };
            response(query, 3, &aliases)
        })
        .await;
        assert_eq!(
            result.unwrap_err(),
            if case == 2 {
                DnsError::ResponseLimit
            } else {
                DnsError::InvalidResponse
            }
        );
        assert_eq!(queries.len(), 1);
    }
}
