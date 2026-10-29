use super::*;
use crate::storage::{archive::CatalogTls, postgres::connection_settings::PlaintextBoundary};
use bigbrotr_model::Digest256;
use bigbrotr_protocol::archive_receipt::{
    MAX_INPUT_BYTES, ReceiptExpectation, ReceiptRange,
    group::{FORMAT, Member},
};

fn common() -> ReceiptExpectation {
    serde_json::from_value(
        serde_json::json!({"dataset_id":"01".repeat(16),"schema_version":1,
        "topology_generation":"0","batch_digest":"05".repeat(32),"record_count":3,
        "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
        "attempt_id":([8;16]),"publication_ordinal":3}),
    )
    .unwrap()
}
fn members() -> [Member; 3] {
    [
        Member {
            attempt_id: [8; 16],
            publication_ordinal: 3,
        },
        Member {
            attempt_id: [9; 16],
            publication_ordinal: 4,
        },
        Member {
            attempt_id: [10; 16],
            publication_ordinal: 5,
        },
    ]
}

#[test]
fn grouped_reply_rejects_wrong_digest_unlisted_crossed_partial_and_extra_output() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    for selected in [None, Some(0), Some(1), Some(2)] {
        let reply = GroupObservation {
            format: FORMAT.into(),
            request_digest: read.digest().unwrap(),
            present: selected.map(|i| members[i]),
        };
        assert_eq!(
            decode_response(&serde_json::to_vec(&reply).unwrap(), read),
            Ok(selected)
        );
    }
    let original = serde_json::to_value(GroupObservation {
        format: FORMAT.into(),
        request_digest: read.digest().unwrap(),
        present: Some(members[2]),
    })
    .unwrap();
    for field in [
        "digest",
        "unlisted",
        "crossed",
        "missing",
        "extra",
        "member-extra",
        "format",
    ] {
        let mut invalid = original.clone();
        match field {
            "digest" => {
                invalid["request_digest"] =
                    serde_json::to_value(Digest256::from_bytes([6; 32])).unwrap()
            }
            "unlisted" => invalid["present"]["attempt_id"] = serde_json::json!(([11; 16])),
            "crossed" => invalid["present"]["publication_ordinal"] = 4.into(),
            "missing" => {
                invalid.as_object_mut().unwrap().remove("present");
            }
            "extra" => invalid["status"] = "present".into(),
            "member-extra" => invalid["present"]["authority"] = true.into(),
            "format" => invalid["format"] = "bigbrotr-native-receipt/1".into(),
            _ => unreachable!(),
        }
        assert_eq!(
            decode_response(&serde_json::to_vec(&invalid).unwrap(), read),
            Err(ReceiptError::ObservationUnknown),
            "{field}"
        );
    }
    let reply = original.to_string();
    for invalid in [
        format!("{reply}{{}}"),
        reply[..reply.len() - 1].to_owned(),
        format!("{},\"present\":null}}", &reply[..reply.len() - 1]),
    ] {
        assert_eq!(
            decode_response(invalid.as_bytes(), read),
            Err(ReceiptError::ObservationUnknown)
        );
    }
}

#[test]
fn grouped_wire_preflight_charges_full_common_members_route_and_credentials() {
    let route = CatalogRoute {
        address: "127.0.0.1".parse().unwrap(),
        port: 5432,
        database: "archive",
        tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
    };
    let mut common = common();
    let members = members();
    let identity =
        CatalogIdentity::local(common.dataset_id, [2; 16].try_into().unwrap(), "/archive/")
            .unwrap();
    let credential = || CatalogReaderCredentials {
        login: "reader",
        password: "fixture-only",
    };
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    assert_eq!(
        route.validate_receipt_group(read, &identity, credential()),
        Ok(())
    );
    assert_eq!(
        route.validate_receipt_group(
            read,
            &identity,
            CatalogReaderCredentials {
                login: "reader",
                password: ""
            }
        ),
        Err(ReceiptError::InvalidInput)
    );
    assert_eq!(
        route.validate_receipt_group(
            GroupRead {
                common: &common,
                members: &[]
            },
            &identity,
            credential()
        ),
        Err(ReceiptError::InvalidInput)
    );
    common.input_ranges = (0..64)
        .map(|shard_id| ReceiptRange {
            shard_id,
            owner_epoch: 1,
            first_sequence: 1,
            last_sequence: 1,
        })
        .collect();
    common.record_count = 64;
    common.publication_ordinal = u64::MAX - 127;
    common.attempt_id = [255; 16];
    common.attempt_id[0] = 0;
    let members: Vec<_> = (0..128)
        .map(|i| {
            let mut id = [255; 16];
            id[0] = i;
            Member {
                attempt_id: id,
                publication_ordinal: u64::MAX - 127 + u64::from(i),
            }
        })
        .collect();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    read.validate().unwrap();
    assert_eq!(
        route.validate_receipt_group(read, &identity, credential()),
        Ok(())
    );
    let long = CatalogIdentity::local(
        common.dataset_id,
        identity.catalog(),
        &format!("/{}/", "a".repeat(4000)),
    )
    .unwrap();
    let password = "p".repeat(4096);
    let login = "r".repeat(4096);
    assert_eq!(
        route.validate_receipt_group(
            read,
            &long,
            CatalogReaderCredentials {
                login: &login,
                password: &password
            }
        ),
        Err(ReceiptError::ResourceLimit)
    );
    // The refusal comes from the unchanged full-wire cap, not a range count increase.
    assert!(serde_json::to_vec(&read).unwrap().len() < MAX_INPUT_BYTES);
}
