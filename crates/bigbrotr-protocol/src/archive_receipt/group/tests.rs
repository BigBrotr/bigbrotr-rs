use super::*;

fn common() -> ReceiptExpectation {
    serde_json::from_value(serde_json::json!({
        "dataset_id":"01".repeat(16),"schema_version":1,"topology_generation":"0",
        "batch_digest":"05".repeat(32),"record_count":3,
        "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
        "attempt_id":([8;16]),"publication_ordinal":3
    }))
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
fn group_digest_binds_literal_common_semantics_and_ordered_identities() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    // Independent Python hashlib over the documented big-endian binary layout.
    assert_eq!(
        read.digest().unwrap().to_string(),
        "ed7e2bd636182e77e8da1ee6a00143b1b74bb007b13cdad873d970e31d59e935"
    );
    for field in 0..6 {
        let mut changed = common.clone();
        match field {
            0 => changed.dataset_id = [2; 16].try_into().unwrap(),
            1 => changed.topology_generation = bigbrotr_model::TopologyGeneration::new(1),
            2 => changed.batch_digest = Digest256::from_bytes([6; 32]),
            3 => {
                changed.record_count = 4;
                changed.input_ranges[0].last_sequence = 4;
            }
            4 => changed.input_ranges[0].shard_id = 1,
            5 => {
                changed.input_ranges[0].first_sequence = 2;
                changed.input_ranges[0].last_sequence = 4;
            }
            _ => unreachable!(),
        }
        let altered = GroupRead {
            common: &changed,
            members: &members,
        };
        assert_ne!(altered.digest().unwrap(), read.digest().unwrap());
        assert!(!read.same_semantics(&changed));
    }
    let mut changed = members;
    changed[2].attempt_id = [11; 16];
    assert_ne!(
        GroupRead {
            common: &common,
            members: &changed
        }
        .digest()
        .unwrap(),
        read.digest().unwrap()
    );
    changed[2].publication_ordinal = 6;
    assert_ne!(
        GroupRead {
            common: &common,
            members: &changed
        }
        .digest()
        .unwrap(),
        read.digest().unwrap()
    );
    assert_ne!(
        GroupRead {
            common: &common,
            members: &members[..2]
        }
        .digest()
        .unwrap(),
        read.digest().unwrap()
    );
}

#[test]
fn group_members_refuse_empty_duplicate_crossed_unordered_and_over_limit() {
    let common = common();
    let original = members();
    for invalid in [
        vec![],
        vec![original[1]],
        vec![original[0], original[0]],
        vec![original[0], original[2], original[1]],
        vec![
            original[0],
            Member {
                attempt_id: [0; 16],
                publication_ordinal: 4,
            },
        ],
        vec![
            original[0],
            Member {
                attempt_id: [9; 16],
                publication_ordinal: 0,
            },
        ],
        vec![
            original[0],
            Member {
                attempt_id: [9; 16],
                publication_ordinal: 3,
            },
        ],
        vec![
            original[0],
            Member {
                attempt_id: [8; 16],
                publication_ordinal: 4,
            },
        ],
    ] {
        assert_eq!(
            GroupRead {
                common: &common,
                members: &invalid
            }
            .validate(),
            Err(ExpectationError::InvalidIdentity)
        );
    }
    let mut maximum: Vec<_> = (0..MAX_MEMBERS)
        .map(|i| Member {
            attempt_id: [(i + 8) as u8; 16],
            publication_ordinal: i as u64 + 3,
        })
        .collect();
    assert_eq!(
        GroupRead {
            common: &common,
            members: &maximum
        }
        .validate(),
        Ok(())
    );
    maximum.push(Member {
        attempt_id: [200; 16],
        publication_ordinal: 131,
    });
    assert_eq!(
        GroupRead {
            common: &common,
            members: &maximum
        }
        .validate(),
        Err(ExpectationError::ResourceLimit)
    );
}

#[test]
fn group_reply_requires_the_exact_digest_and_one_complete_admitted_identity() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    for index in [None, Some(0), Some(1), Some(2)] {
        let observation = GroupObservation {
            format: FORMAT.into(),
            request_digest: read.digest().unwrap(),
            present: index.map(|i| members[i]),
        };
        assert_eq!(observation.validate(read), Ok(index));
    }
    let mut observation = GroupObservation {
        format: FORMAT.into(),
        request_digest: read.digest().unwrap(),
        present: Some(members[2]),
    };
    observation.present.as_mut().unwrap().publication_ordinal = members[1].publication_ordinal;
    assert!(observation.validate(read).is_err());
    observation.present = Some(Member {
        attempt_id: [11; 16],
        publication_ordinal: 6,
    });
    assert!(observation.validate(read).is_err());
    observation.present = None;
    observation.request_digest = Digest256::from_bytes([0; 32]);
    assert!(observation.validate(read).is_err());
    observation.request_digest = read.digest().unwrap();
    observation.format = "bigbrotr-native-receipt/1".into();
    assert!(observation.validate(read).is_err());
}
