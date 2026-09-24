//! Independent signed fixtures and literal portable bytes, not self-roundtrips.
use bigbrotr::export::jsonl::{JsonlError, write_exact_event};
use bigbrotr_protocol::{EventLimits, VerifiedEvent, verify_json};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
    sync::atomic::{AtomicBool, Ordering},
};

fn event(name: &str) -> VerifiedEvent {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    verify_json(
        &serde_json::to_vec(&fixture["events"][name]["event"]).unwrap(),
        EventLimits::default(),
    )
    .unwrap()
}

#[test]
fn exact_signed_fields_and_lf_match_independent_bytes() {
    let input = event("timestamp-zero");
    let expected = concat!(
        "{\"id\":\"e258cc73c1ed89e28f28705e91a79dcb0c62f0ee4a61756730525c03dd68283c\",",
        "\"pubkey\":\"c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5\",",
        "\"created_at\":0,\"kind\":0,\"tags\":[],\"content\":\"\",",
        "\"sig\":\"4fd1163b0c418a2c3ff7caf8707ad4ec1cae51f92273e0c885f136bca90a4035f5d633503b767c439a0ff8019a7e4c5274cf9442576c3bffa80b23a7a8301583\"}\n"
    ).as_bytes();
    let mut output = Vec::new();
    let receipt = write_exact_event(
        &mut output,
        input.event().id,
        Some(&input),
        expected.len() as u64,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(output, expected);
    assert_eq!(receipt.bytes, expected.len() as u64);
    assert_eq!(receipt.rows, 1);
    assert_eq!(
        receipt.sha256.as_bytes(),
        &<[u8; 32]>::from(Sha256::digest(expected))
    );
}

#[test]
fn unsigned_extremes_preserve_order_empty_values_and_escaping() {
    let input = event("unsigned-maximum");
    let expected = concat!(
        "{\"id\":\"d77fac87d3251e321410ada19ad9f0534b5ccf1813202ccfcdbaa9637d3fe280\",",
        "\"pubkey\":\"79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798\",",
        "\"created_at\":18446744073709551615,\"kind\":65535,",
        "\"tags\":[[\"\"],[\"x\"],[\"x\",\"\"],[\"t\",\"é\",\"a,b\"],[\"t\",\"e\u{301}\"],[\"t\",\"é\"]],",
        "\"content\":\"{\\\"value\\\":\\\"still text\\\"}\\n\\\"\\\\/\\b\\t\\n\\f\\r\\u0000\\u0001é𝄞\u{2028}\",",
        "\"sig\":\"fe03109bd891ea72d6ee48c2c7425eef1780301a6fc8f7ce5cf2b21fd6f3cddf320348c8956fe0c7b338ebe56e3398c390bb9549dcf168d50cdf29e96e93ebd2\"}\n"
    ).as_bytes();
    let mut output = Vec::new();
    let receipt = write_exact_event(
        &mut output,
        input.event().id,
        Some(&input),
        expected.len() as u64,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(output, expected);
    assert_eq!(output.iter().filter(|&&b| b == b'\n').count(), 1);
    assert_eq!(receipt.bytes, expected.len() as u64);
    assert_eq!(
        receipt.sha256.as_bytes(),
        &<[u8; 32]>::from(Sha256::digest(expected))
    );
}

#[test]
fn bound_includes_lf_and_refuses_before_any_output() {
    let input = event("timestamp-zero");
    let mut output = Vec::new();
    assert_eq!(
        write_exact_event(
            &mut output,
            input.event().id,
            Some(&input),
            input.normalized_bytes() as u64,
            &AtomicBool::new(false)
        ),
        Err(JsonlError::ResourceLimit)
    );
    assert!(output.is_empty());
}

#[test]
fn absence_has_an_explicit_empty_file_digest() {
    let input = event("timestamp-zero");
    let mut output = Vec::new();
    let receipt = write_exact_event(
        &mut output,
        input.event().id,
        None,
        0,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(receipt.rows, 0);
    assert_eq!(receipt.bytes, 0);
    assert_eq!(
        receipt.sha256.to_string(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn wrong_event_or_preexisting_cancellation_never_writes() {
    let input = event("timestamp-zero");
    let other = event("unsigned-maximum");
    let mut output = Vec::new();
    assert_eq!(
        write_exact_event(
            &mut output,
            other.event().id,
            Some(&input),
            u64::MAX,
            &AtomicBool::new(false)
        ),
        Err(JsonlError::WrongEvent)
    );
    assert_eq!(
        write_exact_event(
            &mut output,
            input.event().id,
            Some(&input),
            u64::MAX,
            &AtomicBool::new(true)
        ),
        Err(JsonlError::Cancelled)
    );
    assert_eq!(
        write_exact_event(
            &mut output,
            input.event().id,
            None,
            0,
            &AtomicBool::new(true)
        ),
        Err(JsonlError::Cancelled)
    );
    assert!(output.is_empty());
}

struct ShortWriter<'a> {
    bytes: Vec<u8>,
    fail_at: usize,
    interrupt: bool,
    cancel: Option<&'a AtomicBool>,
}
impl Write for ShortWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.interrupt {
            self.interrupt = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.bytes.len() >= self.fail_at {
            return Err(io::ErrorKind::StorageFull.into());
        }
        let count = bytes.len().min(3).min(self.fail_at - self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..count]);
        if let Some(cancel) = self.cancel {
            cancel.store(true, Ordering::Release);
        }
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        panic!("encoder must not claim storage durability")
    }
}

#[test]
fn hashes_actual_short_writes_and_never_returns_partial_success() {
    let input = event("timestamp-zero");
    let mut writer = ShortWriter {
        bytes: Vec::new(),
        fail_at: usize::MAX,
        interrupt: true,
        cancel: None,
    };
    let receipt = write_exact_event(
        &mut writer,
        input.event().id,
        Some(&input),
        u64::MAX,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(receipt.bytes as usize, writer.bytes.len());
    assert_eq!(
        receipt.sha256.as_bytes(),
        &<[u8; 32]>::from(Sha256::digest(&writer.bytes))
    );
    writer.bytes.clear();
    writer.fail_at = 17;
    assert_eq!(
        write_exact_event(
            &mut writer,
            input.event().id,
            Some(&input),
            u64::MAX,
            &AtomicBool::new(false)
        ),
        Err(JsonlError::Output)
    );
    assert_eq!(writer.bytes.len(), 17);
    let cancel = AtomicBool::new(false);
    writer.bytes.clear();
    writer.cancel = Some(&cancel);
    assert_eq!(
        write_exact_event(
            &mut writer,
            input.event().id,
            Some(&input),
            u64::MAX,
            &cancel
        ),
        Err(JsonlError::Cancelled)
    );
    assert!(writer.bytes.len() <= 3);
}

#[test]
fn cancellation_after_the_last_lf_still_withholds_the_receipt() {
    struct CancelAtLf<'a> {
        bytes: Vec<u8>,
        cancel: &'a AtomicBool,
    }
    impl Write for CancelAtLf<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            if bytes.contains(&b'\n') {
                self.cancel.store(true, Ordering::Release);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let input = event("timestamp-zero");
    let cancel = AtomicBool::new(false);
    let mut output = CancelAtLf {
        bytes: Vec::new(),
        cancel: &cancel,
    };
    assert_eq!(
        write_exact_event(
            &mut output,
            input.event().id,
            Some(&input),
            u64::MAX,
            &cancel
        ),
        Err(JsonlError::Cancelled)
    );
    assert_eq!(output.bytes.len(), input.normalized_bytes() + 1);
    assert_eq!(output.bytes.last(), Some(&b'\n'));
}
