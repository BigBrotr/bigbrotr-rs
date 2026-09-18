use bigbrotr_model::{AdmissionBatch, Fact, codec::FactDecodeLimits};
use bigbrotr_protocol::archive_batch::{BatchReader, Error, Limits, write_batch};
use std::{io::Cursor, sync::atomic::AtomicBool};

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/archive/single.json")).unwrap()
}
fn bytes(value: &serde_json::Value, name: &str) -> Vec<u8> {
    value[name]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn limits() -> Limits {
    Limits {
        max_records: 16,
        max_encoded_bytes: 1024 * 1024,
        fact: FactDecodeLimits::default(),
    }
}
#[test]
fn frozen_handoff_decodes_complete_membership_and_writes_identical_bytes() {
    let v = fixture();
    let wire = bytes(&v, "wire_hex");
    let payload = bytes(&v, "preimage_hex");
    let cancel = AtomicBool::new(false);
    let mut reader = BatchReader::new(Cursor::new(&wire), limits(), &cancel).unwrap();
    let fact = reader.next_fact(&cancel).unwrap().unwrap();
    assert_eq!(fact.source_record_id().sequence.get(), 2);
    assert_eq!(fact.payload_digest().to_string(), v["fact_digest"]);
    assert!(reader.next_fact(&cancel).unwrap().is_none());
    let batch = reader.finish(&cancel).unwrap();
    assert_eq!(batch.digest().to_string(), v["batch_digest"]);
    let mut actual = Vec::new();
    write_batch(&batch, &[payload], &mut actual, limits(), &cancel).unwrap();
    assert_eq!(actual, wire);
}
#[test]
fn encoder_matches_independent_wire() {
    let v = fixture();
    let payload = bytes(&v, "preimage_hex");
    let fact = Fact::decode_preimage(
        &payload,
        v["fact_digest"].as_str().unwrap().parse().unwrap(),
        FactDecodeLimits::default(),
    )
    .unwrap();
    let batch = AdmissionBatch::new(vec![fact.fact_ref()]).unwrap();
    let mut out = Vec::new();
    write_batch(
        &batch,
        &[payload],
        &mut out,
        limits(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(out, bytes(&v, "wire_hex"));
}

fn decode(wire: &[u8], limits: Limits) -> Result<AdmissionBatch, Error> {
    let cancel = AtomicBool::new(false);
    let mut reader = BatchReader::new(Cursor::new(wire), limits, &cancel)?;
    while reader.next_fact(&cancel)?.is_some() {}
    reader.finish(&cancel)
}

#[test]
fn incomplete_corrupt_foreign_and_trailing_input_cannot_finish() {
    let v = fixture();
    let wire = bytes(&v, "wire_hex");
    for end in 0..wire.len() {
        assert_eq!(
            decode(&wire[..end], limits()),
            Err(Error::Incomplete),
            "cut {end}"
        );
    }
    for position in [8, 24, 28, 36, 48, 80, 260] {
        let mut corrupt = wire.clone();
        corrupt[position] ^= 1;
        assert_eq!(
            decode(&corrupt, limits()),
            Err(Error::Integrity),
            "byte {position}"
        );
    }
    let mut trailing = wire.clone();
    trailing.push(0);
    assert_eq!(decode(&trailing, limits()), Err(Error::Malformed));
    let mut version = wire.clone();
    version[7] = 2;
    assert_eq!(decode(&version, limits()), Err(Error::IncompatibleSchema));
    // The fact profile is checked before its digest. Preserve that category.
    let mut profile = wire.clone();
    profile[130] ^= 1;
    assert_eq!(decode(&profile, limits()), Err(Error::IncompatibleSchema));
    let mut count = wire.clone();
    count[44..48].copy_from_slice(&0u32.to_be_bytes());
    assert_eq!(decode(&count, limits()), Err(Error::Malformed));
    let mut overflow = wire;
    overflow[36..44].copy_from_slice(&u64::MAX.to_be_bytes());
    overflow[44..48].copy_from_slice(&2u32.to_be_bytes());
    assert_eq!(decode(&overflow, limits()), Err(Error::Malformed));
}

#[test]
fn bounds_precede_payload_reads_and_failed_reader_cannot_recover() {
    let v = fixture();
    let wire = bytes(&v, "wire_hex");
    let exact = Limits {
        max_records: 1,
        max_encoded_bytes: 231,
        fact: FactDecodeLimits {
            max_encoded_bytes: 231,
            ..FactDecodeLimits::default()
        },
    };
    assert!(decode(&wire, exact).is_ok());
    for smaller in [
        Limits {
            max_encoded_bytes: 230,
            ..exact
        },
        Limits {
            fact: FactDecodeLimits {
                max_allocation_bytes: 1,
                ..exact.fact
            },
            ..exact
        },
    ] {
        assert_eq!(decode(&wire, smaller), Err(Error::Limit));
    }
    let mut huge = wire[..116].to_vec();
    huge[112..116].copy_from_slice(&u32::MAX.to_be_bytes());
    let cancel = AtomicBool::new(false);
    let mut reader = BatchReader::new(Cursor::new(huge), limits(), &cancel).unwrap();
    assert!(matches!(reader.next_fact(&cancel), Err(Error::Limit)));
    assert!(matches!(reader.next_fact(&cancel), Err(Error::Incomplete)));
    assert_eq!(reader.finish(&cancel), Err(Error::Incomplete));
    assert!(matches!(
        BatchReader::new(Cursor::new(&wire), limits(), &AtomicBool::new(true)),
        Err(Error::Cancelled)
    ));
    let mut reader = BatchReader::new(Cursor::new(&wire), limits(), &cancel).unwrap();
    assert!(matches!(
        reader.next_fact(&AtomicBool::new(true)),
        Err(Error::Cancelled)
    ));
    assert_eq!(reader.finish(&cancel), Err(Error::Incomplete));
    let reader = BatchReader::new(Cursor::new(&wire), limits(), &cancel).unwrap();
    assert_eq!(reader.finish(&cancel), Err(Error::Incomplete));
}

#[test]
fn encoder_refuses_changed_or_missing_preimages_without_partial_output() {
    let v = fixture();
    let payload = bytes(&v, "preimage_hex");
    let fact = Fact::decode_preimage(
        &payload,
        v["fact_digest"].as_str().unwrap().parse().unwrap(),
        FactDecodeLimits::default(),
    )
    .unwrap();
    let batch = AdmissionBatch::new(vec![fact.fact_ref()]).unwrap();
    let mut output = Vec::new();
    let cancel = AtomicBool::new(false);
    assert_eq!(
        write_batch(&batch, &[], &mut output, limits(), &cancel),
        Err(Error::Incomplete)
    );
    let mut changed = payload.clone();
    changed[130] ^= 1;
    assert_eq!(
        write_batch(&batch, &[changed], &mut output, limits(), &cancel),
        Err(Error::Integrity)
    );
    assert_eq!(
        write_batch(
            &batch,
            &[payload],
            &mut output,
            Limits {
                max_encoded_bytes: 230,
                ..limits()
            },
            &cancel
        ),
        Err(Error::Limit)
    );
    assert!(output.is_empty());
}
