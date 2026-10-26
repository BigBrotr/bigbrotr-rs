// BigBrotr regressions for the untrusted metadata boundary.
use super::*;

fn header(name: &[u8], ty: &[u8]) -> Vec<u8> {
    let mut bytes = vec![1];
    for field in [name, ty] {
        put_leb128(&mut bytes, field.len() as u64);
        bytes.extend_from_slice(field);
    }
    bytes
}

#[test]
fn declared_count_is_checked_before_capacity_allocation() {
    // The upstream capacity-overflow reproducer never requests actual allocation.
    let maximal = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1];
    assert!(parse_rbwnat_columns_header(maximal.as_slice()).is_err());
    for bytes in [maximal.as_slice(), &[2]] {
        let error = parse_rbwnat_columns_header_for_row(bytes, 1).unwrap_err();
        assert!(matches!(error, TypesError::TypeParsingError(_)));
    }
    for count in [0, RBWNAT_MAX_COLUMNS + 1] {
        assert!(parse_rbwnat_columns_header_for_row(&[1][..], count).is_err());
    }
}

#[test]
fn fragmented_header_retries_without_accepting_a_partial_header() {
    let bytes = header(b"id", b"UInt64");
    for length in 0..bytes.len() {
        assert!(matches!(
            parse_rbwnat_columns_header_for_row(&bytes[..length], 1),
            Err(TypesError::NotEnoughData(_))
        ));
    }
    assert_eq!(
        parse_rbwnat_columns_header_for_row(bytes.as_slice(), 1).unwrap(),
        vec![Column::new("id".into(), DataTypeNode::UInt64)]
    );
}

#[test]
fn names_types_and_declared_lengths_have_finite_bounds() {
    for bytes in [
        header(b"", b"UInt64"),
        header(b"id", b""),
        header(&[b'x'; RBWNAT_MAX_NAME_BYTES + 1], b"UInt64"),
        header(b"id", &[b'x'; RBWNAT_MAX_TYPE_BYTES + 1]),
        header(&[0xff], b"UInt64"),
        header(b"id", &[0xff]),
    ] {
        assert!(matches!(
            parse_rbwnat_columns_header_for_row(bytes.as_slice(), 1),
            Err(TypesError::TypeParsingError(_))
        ));
    }
    // Oversized declared length is refused without waiting for the missing bytes.
    let mut bytes = vec![1];
    put_leb128(&mut bytes, u64::MAX);
    assert!(matches!(
        parse_rbwnat_columns_header_for_row(bytes.as_slice(), 1),
        Err(TypesError::TypeParsingError(_))
    ));
    let bytes = header(&[b'x'; RBWNAT_MAX_NAME_BYTES], b"UInt64");
    assert!(parse_rbwnat_columns_header_for_row(bytes.as_slice(), 1).is_ok());
}

#[test]
fn depth_and_framing_refuse_malformed_input_without_panics() {
    let nested = |depth| format!("{}String{}", "Array(".repeat(depth), ")".repeat(depth));
    assert!(DataTypeNode::new_bounded(&nested(RBWNAT_MAX_TYPE_DEPTH)).is_ok());
    assert!(DataTypeNode::new_bounded(&nested(RBWNAT_MAX_TYPE_DEPTH + 1)).is_err());
    for malformed in [
        "SimpleAggregateFunction(",
        "SimpleAggregateFunctioné",
        "ArrayéString)",
        "Array(String",
        "Array(String))",
        "DateTime64(3x)",
        "NullableéUInt64)",
        "Tuple(UInt64",
        "Enum8('open = 1)",
        "Timejunk",
        "DateTime('UTC)",
    ] {
        assert!(DataTypeNode::new_bounded(malformed).is_err(), "{malformed}");
    }
    for valid in [
        "FixedString(64)",
        "Array(Array(String))",
        "Nullable(UInt64)",
        "Array(Tuple(FixedString(16), UInt16, FixedString(16), UInt64))",
        "Enum8('é(((((((((' = 1)",
        "SimpleAggregateFunction(sum, UInt64)",
    ] {
        assert!(DataTypeNode::new_bounded(valid).is_ok(), "{valid}");
    }
}

#[test]
fn truncated_and_unicode_wrapper_framing_never_panics() {
    for valid in [
        "SimpleAggregateFunction(sum, UInt64)",
        "Array(Nullable(String))",
        "Enum8('é' = 1)",
        "DateTime64(3, 'UTC')",
        "Map(String, UInt64)",
        "Tuple(UInt64, Array(String))",
        "LowCardinality(String)",
        "FixedString(64)",
    ] {
        for (end, _) in valid.char_indices() {
            let _ = DataTypeNode::new_bounded(&valid[..end]);
        }
        for (at, _) in valid.char_indices() {
            let mut changed = valid.to_owned();
            changed.insert(at, 'é');
            let _ = DataTypeNode::new_bounded(&changed);
        }
    }
}
