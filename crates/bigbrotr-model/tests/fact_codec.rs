use bigbrotr_model::{
    Digest256, Fact, FactPayload,
    codec::{CodecError, FactDecodeLimits},
};
use sha2::{Digest, Sha256};

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn digest(bytes: &[u8]) -> Digest256 {
    Digest256::from_bytes(Sha256::digest(bytes).into())
}
fn fixtures() -> Vec<(String, Vec<u8>, Digest256)> {
    let core: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/model/core-fingerprints.json"
    ))
    .unwrap();
    let original: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/golden-vectors.json"
    ))
    .unwrap();
    core["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .chain(original["vectors"].as_array().unwrap())
        .filter_map(|row| {
            let input = bytes(row["preimage_hex"].as_str().unwrap());
            input.starts_with(b"bigbrotr/fact/1\0").then(|| {
                (
                    row["name"].as_str().unwrap().into(),
                    input,
                    row["sha256"].as_str().unwrap().parse().unwrap(),
                )
            })
        })
        .collect()
}
fn decode(input: &[u8]) -> Result<Fact, CodecError> {
    Fact::decode_preimage(input, digest(input), FactDecodeLimits::default())
}

#[test]
fn frozen_facts_decode_losslessly_and_reencode_exactly() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 5);
    for (name, input, expected) in fixtures {
        let fact = Fact::decode_preimage(&input, expected, FactDecodeLimits::default()).unwrap();
        assert_eq!(fact.payload_digest(), expected);
        assert_eq!(fact.encode_preimage(input.len()).unwrap(), input);
        assert!(matches!(
            fact.encode_preimage(input.len() - 1),
            Err(CodecError::InputLimit)
        ));
        assert_eq!(
            fact.source_record_id().dataset_id.to_string(),
            "01".repeat(16)
        );
        assert_eq!(fact.visibility_scope_id().dictionary_kind as u16, 4);
        assert_eq!(
            fact.source_record_id().sequence.get(),
            if name == "fact-eligibility" { 1 } else { 2 }
        );
        assert_eq!(
            fact.recorded_at().get(),
            if name == "fact-eligibility" {
                -1
            } else {
                i64::MIN
            }
        );
        assert_eq!(
            fact.payload().event_id().unwrap().to_string(),
            "aa".repeat(32)
        );
        match fact.payload() {
            FactPayload::Dictionary(_) => panic!("event fixture contains a dictionary"),
            FactPayload::EventPayload(value) => {
                assert_eq!(value.body.created_at.0, u64::MAX);
                assert_eq!(value.body.pubkey.to_string(), "bb".repeat(32));
                assert_eq!(value.body.kind, 1);
                assert_eq!(value.body.tags, [vec!["x"], vec!["x", ""], vec!["x", ""]]);
                assert_eq!(value.body.content, "é\n");
                assert_eq!(value.event_ordinal.dictionary_kind as u16, 8);
            }
            FactPayload::EventEligibility(value) => {
                assert_eq!(value.sig.to_string(), "cc".repeat(64));
                assert_eq!(value.first_eligible_at.get(), -1);
                assert_eq!(
                    value.origin.as_str(),
                    if name == "eligibility-import" {
                        "import"
                    } else {
                        "network"
                    }
                );
            }
            FactPayload::EventReceive(value) => {
                assert_eq!(value.received_at.get(), -1);
                assert_eq!(value.clock_quality.uncertainty_us(), None);
                assert_eq!(value.corrects.unwrap().sequence.get(), 1);
            }
            FactPayload::Provenance(value) => {
                assert_eq!(value.received_at.get(), -1);
                assert_eq!(value.clock_quality.resolution_us().get(), 1);
                assert_eq!(value.verification_method_id.dictionary_kind as u16, 5);
                assert_eq!(value.corrects.unwrap().sequence.get(), 1);
            }
        }
    }
}

#[test]
fn retained_header_reader_matches_literal_frozen_source_identities() {
    for (name, input, _) in fixtures() {
        let source = Fact::retained_source_record_id(&input, input.len()).unwrap();
        assert_eq!(source.dataset_id.to_string(), "01".repeat(16));
        assert_eq!(source.shard_id, 0);
        assert_eq!(source.owner_epoch.get(), 1);
        assert_eq!(
            source.sequence.get(),
            if name == "fact-eligibility" { 1 } else { 2 }
        );
        assert_eq!(
            Fact::retained_source_record_id(&input, input.len() - 1),
            Err(CodecError::InputLimit)
        );
    }
}

#[test]
fn retained_header_is_not_a_payload_integrity_or_validity_certificate() {
    let (_, input, _) = fixtures().into_iter().next().unwrap();
    let source = Fact::retained_source_record_id(&input, input.len()).unwrap();
    let mut corrupt = input.clone();
    // Preserve the fixed identity/profile but destroy the scientific payload.
    corrupt.truncate(input.len() - 20);
    assert_eq!(
        Fact::retained_source_record_id(&corrupt, input.len()),
        Ok(source)
    );
    assert!(decode(&corrupt).is_err());
    let kind = input
        .windows(13)
        .position(|s| s == b"event-payload")
        .unwrap();
    for (position, replacement) in [
        (b"bigbrotr/fact/1\0".len() - 2, b'2'),
        (kind, b'x'),
        (kind + 13, 2),
    ] {
        let mut unknown = input.clone();
        unknown[position] = replacement;
        assert_eq!(
            Fact::retained_source_record_id(&unknown, input.len()),
            Err(CodecError::IncompatibleSchema)
        );
    }
}

#[test]
fn incompatible_noncanonical_and_truncated_input_never_becomes_a_fact() {
    let corpus = fixtures();
    for (name, input, _) in &corpus {
        for length in 0..input.len() {
            assert!(
                decode(&input[..length]).is_err(),
                "{name} truncated at {length}"
            );
        }
    }
    let input = &corpus[0].1;
    let prefix = b"bigbrotr/fact/1\0".len();
    let mut noncanonical = input.clone();
    noncanonical.splice(prefix..prefix + 1, [0x98, 6]);
    assert!(matches!(
        decode(&noncanonical),
        Err(CodecError::NonCanonical)
    ));
    // Minicbor intentionally accepts these equivalent values. The closed
    // profile must reject their non-shortest heads at every primitive boundary.
    let mut wide_bytes = input.clone();
    wide_bytes.splice(prefix + 2..prefix + 3, [0x58, 0x10]);
    assert!(matches!(decode(&wide_bytes), Err(CodecError::NonCanonical)));
    let mut wide_text = input.clone();
    let label = wide_text
        .windows(13)
        .position(|s| s == b"event-payload")
        .unwrap();
    wide_text.splice(label - 1..label, [0x78, 13]);
    assert!(matches!(decode(&wide_text), Err(CodecError::NonCanonical)));
    let mut wide_integer = input.clone();
    let authored = wide_integer
        .windows(10)
        .position(|s| s == [0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1])
        .unwrap();
    wide_integer.splice(authored + 9..authored + 10, [0x18, 1]);
    assert!(matches!(
        decode(&wide_integer),
        Err(CodecError::NonCanonical)
    ));
    let mut trailing = input.clone();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    let mut indefinite = input.clone();
    indefinite[prefix] = 0x9f;
    assert!(decode(&indefinite).is_err());
    let mut unknown_kind = input.clone();
    let kind = unknown_kind
        .windows(13)
        .position(|s| s == b"event-payload")
        .unwrap();
    unknown_kind[kind] = b'x';
    assert!(matches!(
        decode(&unknown_kind),
        Err(CodecError::IncompatibleSchema)
    ));
    let mut unknown_version = input.clone();
    unknown_version[kind + 13] = 2;
    assert!(matches!(
        decode(&unknown_version),
        Err(CodecError::IncompatibleSchema)
    ));
    let mut profile = input.clone();
    profile[prefix - 2] = b'2';
    assert!(matches!(
        decode(&profile),
        Err(CodecError::IncompatibleSchema)
    ));
    let mut arity = input.clone();
    arity[prefix] = 0x85;
    assert!(decode(&arity).is_err());
    let mut unicode = input.clone();
    let position = unicode.windows(2).position(|s| s == [0xc3, 0xa9]).unwrap();
    unicode[position] = 0xff;
    assert!(decode(&unicode).is_err());
    assert!(matches!(
        Fact::decode_preimage(
            input,
            Digest256::from_bytes([0; 32]),
            FactDecodeLimits::default()
        ),
        Err(CodecError::DigestMismatch)
    ));
}

#[test]
fn decode_enforces_namespace_nullability_and_reserved_clock_bits() {
    let corpus = fixtures();
    let input = |name: &str| corpus.iter().find(|row| row.0 == name).unwrap().1.clone();
    let mut foreign = input("event-payload");
    let scope = bytes(&("8450".to_owned() + &"01".repeat(16) + "0450"));
    let position = foreign
        .windows(scope.len())
        .position(|part| part == scope)
        .unwrap();
    foreign[position + 2..position + 18].fill(9);
    assert!(matches!(decode(&foreign), Err(CodecError::InvalidFact(_))));
    let mut flags = input("event-receive-correction");
    let clock =
        bytes(&("848450".to_owned() + &"01".repeat(16) + "0250" + &"02".repeat(16) + "0101f600"));
    let position = flags
        .windows(clock.len())
        .position(|part| part == clock)
        .unwrap();
    flags[position + clock.len() - 1] = 8;
    assert!(matches!(
        decode(&flags),
        Err(CodecError::IncompatibleSchema)
    ));
    let mut missing_lineage = input("fact-eligibility");
    let position = missing_lineage
        .windows(7)
        .position(|part| part == b"network")
        .unwrap();
    missing_lineage.splice(position - 1..position + 7, bytes("66696d706f7274"));
    assert!(decode(&missing_lineage).is_err());
    let mut retained_network_lineage = input("eligibility-import");
    let position = retained_network_lineage
        .windows(6)
        .position(|part| part == b"import")
        .unwrap();
    retained_network_lineage.splice(position - 1..position + 6, bytes("676e6574776f726b"));
    assert!(decode(&retained_network_lineage).is_err());
}

#[test]
fn untrusted_lengths_and_expansion_are_bounded_before_reservation() {
    let fixtures = fixtures();
    let input = &fixtures[0].1;
    let mut limits = FactDecodeLimits {
        max_encoded_bytes: input.len() - 1,
        ..Default::default()
    };
    assert!(matches!(
        Fact::decode_preimage(input, digest(input), limits),
        Err(CodecError::InputLimit)
    ));
    limits = FactDecodeLimits {
        max_allocation_bytes: 0,
        ..Default::default()
    };
    assert!(matches!(
        Fact::decode_preimage(input, digest(input), limits),
        Err(CodecError::AllocationLimit)
    ));
    // The literal corpus has three tag Vecs, five Strings, three tag UTF-8
    // bytes and three content bytes. This counts requested storage, not RSS.
    let requested = 3 * std::mem::size_of::<Vec<String>>() + 5 * std::mem::size_of::<String>() + 6;
    limits.max_allocation_bytes = requested;
    assert!(Fact::decode_preimage(input, digest(input), limits).is_ok());
    limits.max_allocation_bytes = requested - 1;
    assert!(matches!(
        Fact::decode_preimage(input, digest(input), limits),
        Err(CodecError::AllocationLimit)
    ));
    limits.max_allocation_bytes = 0;
    let eligibility = fixtures
        .iter()
        .find(|(name, _, _)| name == "fact-eligibility")
        .unwrap();
    assert!(Fact::decode_preimage(&eligibility.1, eligibility.2, limits).is_ok());
    limits = FactDecodeLimits {
        max_tag_nodes: 7,
        ..Default::default()
    };
    assert!(matches!(
        Fact::decode_preimage(input, digest(input), limits),
        Err(CodecError::TagLimit)
    ));
    limits.max_tag_nodes = 8;
    assert!(Fact::decode_preimage(input, digest(input), limits).is_ok());
    limits = FactDecodeLimits {
        max_string_bytes: 2,
        ..Default::default()
    };
    assert!(matches!(
        Fact::decode_preimage(input, digest(input), limits),
        Err(CodecError::StringLimit)
    ));
    let mut huge = input.clone();
    let tags = huge
        .windows(4)
        .position(|s| s == [0x83, 0x81, 0x61, b'x'])
        .unwrap();
    huge.splice(
        tags..tags + 1,
        [0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    );
    assert!(matches!(decode(&huge), Err(CodecError::TagLimit)));
}

// Independent walker for these frozen definite-length fixtures only; never
// used by the product decoder or fed arbitrary input. Enumerates equivalent
// primitive encodings so acceptance cannot rely on a codec default.
fn heads(bytes: &[u8], position: &mut usize, found: &mut Vec<(usize, usize, u8, u64)>) {
    let start = *position;
    let head = bytes[*position];
    *position += 1;
    let extra = match head & 31 {
        n @ 0..=23 => {
            found.push((start, *position, head & 0xe0, n.into()));
            0
        }
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => panic!("closed fixture expected"),
    };
    let mut argument = u64::from(head & 31);
    if extra > 0 {
        argument = 0;
        for _ in 0..extra {
            argument = argument * 256 + u64::from(bytes[*position]);
            *position += 1;
        }
        found.push((start, *position, head & 0xe0, argument));
    }
    match head & 0xe0 {
        0x00 | 0x20 | 0xe0 => (),
        0x40 | 0x60 => *position += argument as usize,
        0x80 => {
            for _ in 0..argument {
                heads(bytes, position, found);
            }
        }
        _ => panic!("closed fixture expected"),
    }
}
#[test]
fn every_primitive_head_rejects_larger_equal_value_widths() {
    let mut checked = 0;
    for (_, input, _) in fixtures() {
        let mut position = b"bigbrotr/fact/1\0".len();
        let mut found = Vec::new();
        heads(&input, &mut position, &mut found);
        assert_eq!(position, input.len());
        for (start, end, major, value) in found {
            if major == 0xe0 {
                continue;
            }
            let width = end - start;
            for extra in [1usize, 2, 4, 8] {
                if extra < width {
                    continue;
                }
                let info = match extra {
                    1 => 24,
                    2 => 25,
                    4 => 26,
                    _ => 27,
                };
                let mut modified = input.clone();
                let mut replacement = vec![major | info];
                replacement.extend_from_slice(&value.to_be_bytes()[8 - extra..]);
                modified.splice(start..end, replacement);
                assert_eq!(decode(&modified), Err(CodecError::NonCanonical));
                checked += 1;
            }
        }
    }
    eprintln!("canonical width probes: {checked}");
}

#[test]
fn missing_nullable_and_scalar_domain_controls() {
    let corpus = fixtures();
    let input = &corpus.iter().find(|v| v.0 == "event-payload").unwrap().1;
    let prefix = b"bigbrotr/fact/1\0".len();
    for position in [prefix + 19, prefix + 20, prefix + 21] {
        // source shard / epoch / sequence
        for replacement in [
            vec![0xf6],
            vec![0xfb, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![0xc2, 0x40],
            vec![0x20],
            vec![0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ] {
            let mut modified = input.clone();
            modified.splice(position..position + 1, replacement.clone());
            let result = decode(&modified);
            // u64::MAX remains valid for epoch/sequence, but not u32 shard.
            if position != prefix + 19 && replacement[0] == 0x1b {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }
    for (_, input, _) in corpus {
        let mut position = b"bigbrotr/fact/1\0".len();
        let mut found = Vec::new();
        heads(&input, &mut position, &mut found);
        for (position, _, major, value) in found {
            if major == 0xe0 && value == 22 {
                let mut missing = input.clone();
                missing.remove(position);
                assert!(decode(&missing).is_err());
                let mut undefined = input.clone();
                undefined[position] = 0xf7;
                assert!(decode(&undefined).is_err());
            }
        }
    }
}
