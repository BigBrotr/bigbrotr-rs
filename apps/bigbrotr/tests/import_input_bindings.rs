use bigbrotr::acquisition::import::{RecordPosition, SourceIdentity};
use bigbrotr::storage::postgres::input_bindings::{ImportBatch, ImportJobBinding};

fn job() -> ImportJobBinding {
    ImportJobBinding {
        id: [7; 16],
        request_digest: [8; 32],
        expected_revision: 3,
        config_revision: 1,
        policy_revision: 1,
    }
}
fn positions() -> [RecordPosition; 2] {
    let source = SourceIdentity {
        bytes: 706,
        sha256: [3; 32],
    };
    [
        RecordPosition {
            source,
            line: 1,
            byte_start: 0,
            byte_end: 353,
            sha256: [4; 32],
        },
        RecordPosition {
            source,
            line: 2,
            byte_start: 353,
            byte_end: 706,
            sha256: [4; 32],
        },
    ]
}
#[test]
fn identical_records_remain_two_distinct_obligations() {
    let records = positions();
    let batch = ImportBatch::new(job(), [9; 32], 0, &records).unwrap();
    assert_eq!(batch.record_count(), 2);
    assert_eq!(batch.first_record(), 1);
}
#[test]
fn incomplete_source_ranges_and_mixed_sources_are_refused() {
    for change in 0..6 {
        let mut records = positions();
        match change {
            0 => records[1].line = 3,
            1 => records[1].byte_start = 354,
            2 => records[1].source.sha256[0] ^= 1,
            3 => records[1].byte_end = 707,
            4 => records[0].line = 0,
            _ => records[0].byte_end = records[0].byte_start,
        }
        assert!(
            ImportBatch::new(job(), [9; 32], 0, &records).is_err(),
            "case {change}"
        );
    }
    assert!(ImportBatch::new(job(), [9; 32], 0, &[]).is_err());
}
#[test]
fn later_batch_is_not_required_to_start_at_source_zero() {
    let records = positions();
    let batch = ImportBatch::new(job(), [9; 32], 0, &records[1..]).unwrap();
    assert_eq!(batch.first_record(), 2);
    assert_eq!(batch.record_count(), 1);
}
#[test]
fn finite_batch_cannot_expand_the_retained_request_window() {
    let mut records = Vec::new();
    for n in 0..65 {
        records.push(RecordPosition {
            source: SourceIdentity {
                bytes: 65,
                sha256: [1; 32],
            },
            line: n + 1,
            byte_start: n,
            byte_end: n + 1,
            sha256: [2; 32],
        });
    }
    assert!(ImportBatch::new(job(), [9; 32], 0, &records[..64]).is_ok());
    assert!(ImportBatch::new(job(), [9; 32], 0, &records).is_err());
}

#[test]
fn logical_line_number_requires_space_for_prior_delimiters() {
    for line in [2, u64::MAX] {
        let mut records = positions();
        records[0].line = line;
        assert!(ImportBatch::new(job(), [9; 32], 0, &records[..1]).is_err());
    }
}
