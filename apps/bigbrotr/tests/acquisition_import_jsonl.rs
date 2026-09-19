use bigbrotr::acquisition::import::{
    ImportSourceError, JsonlReader, ScratchSpace, SourceIdentity, SourceSnapshot,
};
use bigbrotr::runtime::{
    OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use bigbrotr_protocol::{EventError, EventLimits};
use sha2::{Digest, Sha256};
use std::{fs, sync::atomic::AtomicBool};

const EVENT: &[u8] = include_bytes!("../../../tests/fixtures/ingest/import-event.jsonl");
static CANCEL: AtomicBool = AtomicBool::new(false);

fn memory(bytes: u64) -> OwnedPermit {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
    .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
    .unwrap()
}

fn capture(bytes: &[u8]) -> (tempfile::TempDir, SourceSnapshot, ScratchSpace) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::write(&source, bytes).unwrap();
    let scratch = ScratchSpace::new(dir.path(), bytes.len() as u64, 2).unwrap();
    let snapshot = SourceSnapshot::capture(&source, &scratch, None, &CANCEL).unwrap();
    (dir, snapshot, scratch)
}

fn reader(snapshot: SourceSnapshot, cap: usize) -> JsonlReader {
    JsonlReader::new(
        snapshot,
        EventLimits {
            wire_bytes: cap,
            ..EventLimits::default()
        },
        memory(16 * 1024 * 1024),
    )
    .unwrap()
}

#[test]
fn captured_file_is_not_changed_by_rewriting_or_replacing_source() {
    let (dir, snapshot, scratch) = capture(EVENT);
    assert_eq!(
        snapshot.identity(),
        SourceIdentity {
            bytes: EVENT.len() as u64,
            sha256: Sha256::digest(EVENT).into()
        }
    );
    fs::write(dir.path().join("source"), vec![b'x'; EVENT.len()]).unwrap();
    fs::rename(dir.path().join("source"), dir.path().join("replaced")).unwrap();
    fs::write(dir.path().join("source"), b"different").unwrap();
    assert_eq!(
        fs::read_dir(dir.path()).unwrap().count(),
        2,
        "no named staging file"
    );
    let mut decoder = reader(snapshot, 4096);
    let record = decoder.next_record(&CANCEL).unwrap().unwrap();
    assert_eq!(
        record.result().as_ref().unwrap().event().content,
        "import then network"
    );
    assert_eq!(scratch.used_bytes(), EVENT.len() as u64);
    drop(record);
    drop(decoder);
    assert_eq!(scratch.used_bytes(), 0);
}

#[test]
fn expected_whole_source_identity_refuses_same_size_change_and_releases_quota() {
    let (dir, snapshot, scratch) = capture(EVENT);
    let identity = snapshot.identity();
    drop(snapshot);
    let path = dir.path().join("source");
    let mut changed = EVENT.to_vec();
    changed[30] ^= 1;
    fs::write(&path, changed).unwrap();
    assert!(matches!(
        SourceSnapshot::capture(&path, &scratch, Some(identity), &CANCEL),
        Err(ImportSourceError::SourceChanged)
    ));
    assert_eq!(scratch.used_bytes(), 0);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    fs::write(&path, EVENT).unwrap();
    assert!(SourceSnapshot::capture(&path, &scratch, Some(identity), &CANCEL).is_ok());
}

#[test]
fn shared_scratch_limit_counts_live_snapshots_and_cancellation_leaves_source() {
    let (dir, snapshot, scratch) = capture(EVENT);
    let path = dir.path().join("source");
    assert!(matches!(
        SourceSnapshot::capture(&path, &scratch, None, &CANCEL),
        Err(ImportSourceError::ScratchLimit { .. })
    ));
    drop(snapshot);
    assert!(matches!(
        SourceSnapshot::capture(&path, &scratch, None, &AtomicBool::new(true)),
        Err(ImportSourceError::Cancelled)
    ));
    assert_eq!(scratch.used_bytes(), 0);
    assert_eq!(fs::read(path).unwrap(), EVENT);
}

#[test]
fn source_must_be_a_regular_nonsymlink_file() {
    use std::os::unix::fs::symlink;
    let (dir, snapshot, scratch) = capture(EVENT);
    drop(snapshot);
    let link = dir.path().join("link");
    symlink(dir.path().join("source"), &link).unwrap();
    assert!(SourceSnapshot::capture(&link, &scratch, None, &CANCEL).is_err());
    assert!(matches!(
        SourceSnapshot::capture(dir.path(), &scratch, None, &CANCEL),
        Err(ImportSourceError::NotRegular)
    ));
    let fifo = dir.path().join("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    assert!(matches!(
        SourceSnapshot::capture(&fifo, &scratch, None, &CANCEL),
        Err(ImportSourceError::NotRegular)
    ));
}

#[test]
fn positions_include_crlf_and_final_unterminated_record_and_raw_hash() {
    let event = &EVENT[..EVENT.len() - 1];
    let mut bytes = event.to_vec();
    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(event);
    let (_dir, snapshot, _scratch) = capture(&bytes);
    let source = snapshot.identity();
    let mut decoder = reader(snapshot, 4096);
    let first_end = (event.len() + 2) as u64;
    for (line, start, end) in [(1, 0, first_end), (2, first_end, bytes.len() as u64)] {
        let record = decoder.next_record(&CANCEL).unwrap().unwrap();
        let position = record.position();
        assert_eq!(
            (position.line, position.byte_start, position.byte_end),
            (line, start, end)
        );
        assert_eq!(position.source, source);
        assert_eq!(
            position.sha256,
            <[u8; 32]>::from(Sha256::digest(&bytes[start as usize..end as usize]))
        );
        assert_eq!(
            serde_json::to_value(record.result().as_ref().unwrap().event()).unwrap(),
            serde_json::from_slice::<serde_json::Value>(EVENT).unwrap()
        );
    }
    assert!(decoder.next_record(&CANCEL).unwrap().is_none());
    assert!(decoder.next_record(&CANCEL).unwrap().is_none());
}

#[test]
fn oversized_line_is_drained_before_valid_following_record() {
    let mut bytes = vec![b'x'; 128 * 1024 + 3];
    bytes.push(b'\n');
    let end = bytes.len() as u64;
    bytes.extend_from_slice(EVENT);
    let (_dir, snapshot, _scratch) = capture(&bytes);
    let mut decoder = reader(snapshot, 4096);
    let record = decoder.next_record(&CANCEL).unwrap().unwrap();
    assert_eq!(
        record.result().as_ref().unwrap_err(),
        &EventError::WireLimit
    );
    assert_eq!(record.position().byte_end, end);
    assert_eq!(
        record.position().sha256,
        <[u8; 32]>::from(Sha256::digest(&bytes[..end as usize]))
    );
    drop(record);
    let record = decoder.next_record(&CANCEL).unwrap().unwrap();
    assert!(record.result().is_ok());
    assert_eq!(record.position().line, 2);
    assert_eq!(record.position().byte_start, end);
}

#[test]
fn malformed_duplicate_key_forged_signature_and_blank_records_are_counted_separately() {
    let event = std::str::from_utf8(EVENT).unwrap();
    let duplicate = event.replacen("{", "{\"kind\":1,", 1);
    let forged = event.replacen("ab9e62de", "cb9e62de", 1);
    let bytes = format!("{{\n{duplicate}{forged}\n{event}");
    let (_dir, snapshot, _scratch) = capture(bytes.as_bytes());
    let mut decoder = reader(snapshot, 4096);
    for (index, error) in [
        Some(EventError::InvalidJson),
        Some(EventError::InvalidJson),
        Some(EventError::InvalidSignature),
        Some(EventError::InvalidJson),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let record = decoder.next_record(&CANCEL).unwrap().unwrap();
        assert_eq!(record.position().line, index as u64 + 1);
        assert_eq!(record.result().as_ref().err().copied(), error);
    }
    assert!(decoder.next_record(&CANCEL).unwrap().is_none());
}

#[test]
fn decoder_preserves_independently_signed_extreme_fields() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    let event = &corpus["events"]["unsigned-maximum"]["event"];
    let bytes = serde_json::to_vec(event).unwrap();
    let (_dir, snapshot, _scratch) = capture(&bytes);
    let mut decoder = reader(snapshot, 4096);
    let record = decoder.next_record(&CANCEL).unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(record.result().as_ref().unwrap().event()).unwrap(),
        *event
    );
}

#[test]
fn empty_file_is_zero_records_and_exact_limit_accepts_final_record() {
    let (_dir, snapshot, _scratch) = capture(b"");
    assert!(
        reader(snapshot, 4096)
            .next_record(&CANCEL)
            .unwrap()
            .is_none()
    );
    let event = &EVENT[..EVENT.len() - 1];
    let (_dir, snapshot, _scratch) = capture(event);
    assert!(
        reader(snapshot, event.len())
            .next_record(&CANCEL)
            .unwrap()
            .unwrap()
            .result()
            .is_ok()
    );
}

#[test]
fn insufficient_memory_and_cancelled_decoder_do_not_emit_record_positions() {
    let (_dir, snapshot, scratch) = capture(EVENT);
    assert!(matches!(
        JsonlReader::new(snapshot, EventLimits::default(), memory(1)),
        Err(ImportSourceError::MemoryLimit)
    ));
    assert_eq!(scratch.used_bytes(), 0);
    let (_dir, snapshot, _scratch) = capture(EVENT);
    let mut decoder = reader(snapshot, 4096);
    assert!(matches!(
        decoder.next_record(&AtomicBool::new(true)),
        Err(ImportSourceError::Cancelled)
    ));
    assert!(matches!(
        decoder.next_record(&CANCEL),
        Err(ImportSourceError::ReaderStopped)
    ));
}

#[test]
fn simultaneous_captures_share_one_byte_and_file_allowance() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::write(&source, EVENT).unwrap();
    for (bytes, files) in [(2 * EVENT.len() as u64, 8), (8 * EVENT.len() as u64, 2)] {
        let scratch = ScratchSpace::new(dir.path(), bytes, files).unwrap();
        let start = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let scratch = scratch.clone();
                let source = source.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    SourceSnapshot::capture(&source, &scratch, None, &CANCEL)
                })
            })
            .collect();
        // Join handles retain successful snapshots until every attempt finishes.
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 2);
        assert!(
            results
                .iter()
                .filter_map(|r| r.as_ref().err())
                .all(|e| matches!(e, ImportSourceError::ScratchLimit { .. }))
        );
        assert_eq!(scratch.used_bytes(), 2 * EVENT.len() as u64);
        drop(results);
        assert_eq!(scratch.used_bytes(), 0);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[test]
fn zero_byte_sources_still_consume_file_slots() {
    let (dir, first, scratch) = capture(b"");
    let second =
        SourceSnapshot::capture(&dir.path().join("source"), &scratch, None, &CANCEL).unwrap();
    assert!(matches!(
        SourceSnapshot::capture(&dir.path().join("source"), &scratch, None, &CANCEL),
        Err(ImportSourceError::ScratchLimit { .. })
    ));
    drop((first, second));
    assert!(SourceSnapshot::capture(&dir.path().join("source"), &scratch, None, &CANCEL).is_ok());
}

#[test]
fn parser_resource_refusal_is_distinct_from_invalid_input_and_next_line_survives() {
    let bytes = [EVENT, EVENT].concat();
    let (_dir, snapshot, _scratch) = capture(&bytes);
    let mut decoder = JsonlReader::new(
        snapshot,
        EventLimits {
            parse_bytes: 1,
            ..EventLimits::default()
        },
        memory(16 * 1024 * 1024),
    )
    .unwrap();
    for line in [1, 2] {
        let record = decoder.next_record(&CANCEL).unwrap().unwrap();
        assert_eq!(
            record.result().as_ref().err(),
            Some(&EventError::ArenaLimit)
        );
        assert_eq!(record.position().line, line);
    }
    assert!(decoder.next_record(&CANCEL).unwrap().is_none());
}
