use super::*;

#[test]
fn signed_microseconds_preserve_pre_epoch_values_and_refuse_overflow() {
    assert_eq!(
        observed(Timespec {
            tv_sec: 0,
            tv_nsec: 999
        }),
        Ok(ObservedTime::new(0))
    );
    assert_eq!(
        observed(Timespec {
            tv_sec: -1,
            tv_nsec: 999_999_999
        }),
        Ok(ObservedTime::new(-1))
    );
    assert_eq!(
        observed(Timespec {
            tv_sec: -1,
            tv_nsec: 0
        }),
        Ok(ObservedTime::new(-1_000_000))
    );
    assert_eq!(
        observed(Timespec {
            tv_sec: i64::MAX,
            tv_nsec: 0
        }),
        Err(ClockReadError)
    );
    assert_eq!(
        observed(Timespec {
            tv_sec: 0,
            tv_nsec: -1
        }),
        Err(ClockReadError)
    );
    assert_eq!(
        observed(Timespec {
            tv_sec: 0,
            tv_nsec: 1_000_000_000
        }),
        Err(ClockReadError)
    );
}

#[test]
fn resolution_rounds_up_and_unknown_uncertainty_never_becomes_zero() {
    for (ns, expected) in [(1, 1), (1000, 1), (1001, 2), (999_999_999, 1_000_000)] {
        assert_eq!(
            resolution(Timespec {
                tv_sec: 0,
                tv_nsec: ns
            }),
            Ok(DurationUs::new(expected))
        );
    }
    assert_eq!(
        resolution(Timespec {
            tv_sec: 0,
            tv_nsec: 0
        }),
        Err(ClockReadError)
    );
    assert_eq!(
        resolution(Timespec {
            tv_sec: -1,
            tv_nsec: 0
        }),
        Err(ClockReadError)
    );
    let mut clock = ReceiveClock {
        resolution: DurationUs::new(2),
        previous: None,
    };
    let first = clock.record(ObservedTime::new(100));
    let rollback = clock.record(ObservedTime::new(99));
    let next = clock.record(ObservedTime::new(101));
    let id = bigbrotr_model::QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: bigbrotr_model::DictionaryKind::Observer,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: bigbrotr_model::Ordinal::new(1).unwrap(),
    };
    assert_eq!(first.quality(id).unwrap().anomaly_flags(), 4);
    assert_eq!(rollback.quality(id).unwrap().anomaly_flags(), 5);
    assert_eq!(next.quality(id).unwrap().anomaly_flags(), 4);
    assert_eq!(rollback.received_at(), ObservedTime::new(99));
    assert_eq!(first.received_at(), ObservedTime::new(100));
    assert_eq!(first.quality(id).unwrap().uncertainty_us(), None);
    assert_eq!(
        first.quality(id).unwrap().resolution_us(),
        DurationUs::new(2)
    );
}

#[test]
fn realtime_sampling_is_bracketed_by_independent_standard_clock_reads() {
    let mut clock = ReceiveClock::new().unwrap();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let sample = clock.sample().unwrap();
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    assert!((before..=after).contains(&(sample.received_at().get() as u128)));
}
