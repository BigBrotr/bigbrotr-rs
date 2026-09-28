use bigbrotr_model::{
    NostrTime,
    acquisition::{Filter, FilterFields},
};
use bigbrotr_protocol::wire::{RequestEncodeError, encode_request};

#[test]
fn exact_request_union_preserves_bounds_escaping_and_explicit_zero_limit() {
    let filter = Filter::new(FilterFields {
        kinds: Some(vec![2, 1]),
        tags: [('t', vec!["a\"\\\n雪".to_owned()])].into(),
        since: Some(NostrTime(0)),
        until: Some(NostrTime(u64::MAX)),
        ..Default::default()
    })
    .unwrap();
    let any = Filter::new(FilterFields::default()).unwrap();
    let expected = r##"["REQ","r\"1",{"kinds":[1,2],"#t":["a\"\\\n雪"],"since":0,"until":18446744073709551615,"limit":0},{"limit":0}]"##;
    let encoded = encode_request(
        "r\"1",
        &[filter.clone(), any.clone()],
        Some(0),
        expected.len(),
    )
    .unwrap();
    assert_eq!(encoded, expected);
    assert_eq!(
        encode_request("r\"1", &[filter, any], Some(0), expected.len() - 1),
        Err(RequestEncodeError::EncodedLimit)
    );
}

#[test]
fn absent_limit_differs_from_zero_and_all_unions_and_identifiers_are_bounded() {
    let filters = [Filter::new(FilterFields::default()).unwrap()];
    assert_eq!(
        encode_request("r", &filters, None, 100).unwrap(),
        r#"["REQ","r",{}]"#
    );
    assert_eq!(
        encode_request("r", &filters, Some(0), 100).unwrap(),
        r#"["REQ","r",{"limit":0}]"#
    );
    assert_eq!(
        encode_request("r", &[], None, 100),
        Err(RequestEncodeError::InvalidRequest)
    );
    assert_eq!(
        encode_request("r", &vec![filters[0].clone(); 17], None, 1000),
        Err(RequestEncodeError::InvalidRequest)
    );
    assert_eq!(
        encode_request("", &filters, None, 100),
        Err(RequestEncodeError::InvalidRequest)
    );
    assert!(encode_request(&"雪".repeat(64), &filters, None, 1000).is_ok());
    assert_eq!(
        encode_request(&"雪".repeat(65), &filters, None, 1000),
        Err(RequestEncodeError::InvalidRequest)
    );
}
