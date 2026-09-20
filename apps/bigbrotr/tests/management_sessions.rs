use bigbrotr::management::sessions::{BrowserSecret, SessionLifetime};
use std::time::Duration;

#[test]
fn browser_secrets_have_one_closed_encoding_and_redacted_debug() {
    let secret = BrowserSecret::parse_hex(&[b'a'; 64]).unwrap();
    assert_eq!(&*secret.expose_hex(), &"a".repeat(64));
    assert_eq!(format!("{secret:?}"), "BrowserSecret([redacted])");
    for invalid in [&[b'A'; 64][..], &[b'a'; 63], &[b'a'; 65], &[b'g'; 64]] {
        assert!(BrowserSecret::parse_hex(invalid).is_err());
    }
}

#[test]
fn session_lifetimes_are_explicit_checked_and_preserve_subsecond_precision() {
    let defaults = SessionLifetime::default();
    assert_eq!(defaults.maximum(), Duration::from_secs(8 * 3600));
    assert_eq!(defaults.idle(), Duration::from_secs(30 * 60));
    assert!(SessionLifetime::new(Duration::ZERO, Duration::ZERO).is_err());
    assert!(SessionLifetime::new(Duration::from_secs(1), Duration::from_secs(2)).is_err());
    assert!(SessionLifetime::new(Duration::MAX, Duration::from_secs(1)).is_err());
    assert!(SessionLifetime::new(Duration::from_secs(1), Duration::from_nanos(1)).is_err());
    let precise =
        SessionLifetime::new(Duration::from_micros(15), Duration::from_micros(11)).unwrap();
    assert_eq!(precise.maximum(), Duration::from_micros(15));
    assert_eq!(precise.idle(), Duration::from_micros(11));
}
