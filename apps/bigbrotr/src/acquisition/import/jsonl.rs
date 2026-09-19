use super::source::{
    ImportSourceError, ScratchPermit, SourceIdentity, SourceSnapshot, check_cancelled,
};
use crate::runtime::OwnedPermit;
use bigbrotr_protocol::{EventError, EventLimits, VerifiedEvent, verify_json};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::atomic::AtomicBool,
};

/// Source-local position; job namespace, shard membership, scope and obligation
/// identity must be bound separately before dispatch. Never an ingest sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordPosition {
    pub source: SourceIdentity,
    pub line: u64,
    pub byte_start: u64,
    pub byte_end: u64,
    /// Hash of all raw record bytes, including CR/LF when present.
    pub sha256: [u8; 32],
}

/// Borrowing the reader's permit prevents the next parse or releasing memory
/// while this event is retained. A later admission consumer must reserve its own
/// storage before copying the event into an independently owned request.
///
/// Retained event allocations prevent parsing the next event under the same grant:
/// ```compile_fail
/// use bigbrotr::acquisition::import::JsonlReader;
/// use std::sync::atomic::AtomicBool;
/// fn overlapping(mut reader: JsonlReader, cancelled: &AtomicBool) {
///     let first = reader.next_record(cancelled).unwrap().unwrap();
///     assert!(first.result().is_ok());
///     let second = reader.next_record(cancelled).unwrap();
/// }
/// ```
/// The grant cannot be dropped while the last record still owns an event:
/// ```compile_fail
/// use bigbrotr::acquisition::import::JsonlReader;
/// use std::sync::atomic::AtomicBool;
/// fn released(mut reader: JsonlReader, cancelled: &AtomicBool) {
///     let first = reader.next_record(cancelled).unwrap().unwrap();
///     assert!(first.result().is_ok());
///     drop(reader);
/// }
/// ```
pub struct JsonlRecord<'reader> {
    position: RecordPosition,
    result: Result<VerifiedEvent, EventError>,
    _permit: &'reader OwnedPermit,
}
impl Drop for JsonlRecord<'_> {
    fn drop(&mut self) {
        // Drop checking keeps the permit borrowed until the owned event is
        // destroyed. Without Drop, NLL ends the borrow at its last *use* while
        // the event's allocation can remain live until the enclosing scope ends.
    }
}
impl JsonlRecord<'_> {
    pub fn position(&self) -> RecordPosition {
        self.position
    }
    pub fn result(&self) -> &Result<VerifiedEvent, EventError> {
        &self.result
    }
}

pub struct JsonlReader {
    input: BufReader<File>,
    _scratch: ScratchPermit,
    source: SourceIdentity,
    limits: EventLimits,
    buffer: Vec<u8>,
    memory: OwnedPermit,
    line: u64,
    offset: u64,
    stopped: bool,
}
type DecodedRecord = (RecordPosition, Result<VerifiedEvent, EventError>);
impl JsonlReader {
    pub fn new(
        snapshot: SourceSnapshot,
        limits: EventLimits,
        memory: OwnedPermit,
    ) -> Result<Self, ImportSourceError> {
        limits
            .validate()
            .map_err(|_| ImportSourceError::InvalidLimits)?;
        let required = limits
            .wire_bytes
            .checked_add(limits.parse_bytes)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or(ImportSourceError::MemoryLimit)?;
        if memory.request().units().data_bytes() < required as u64
            || memory.request().units().items() < 1
        {
            return Err(ImportSourceError::MemoryLimit);
        }
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(limits.wire_bytes)
            .map_err(|_| ImportSourceError::MemoryLimit)?;
        Ok(Self {
            input: BufReader::with_capacity(16 * 1024, snapshot.file),
            _scratch: snapshot.scratch,
            source: snapshot.identity,
            limits,
            buffer,
            memory,
            line: 0,
            offset: 0,
            stopped: false,
        })
    }

    /// LF terminates a record; CR remains JSON whitespace and counts toward the
    /// wire cap. A nonempty final EOF unit is one record. Blank lines are invalid
    /// records. I/O failure/cancellation stops this reader without emitting a
    /// completed position for a partially read unit.
    pub fn next_record(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<Option<JsonlRecord<'_>>, ImportSourceError> {
        if self.stopped {
            return Err(ImportSourceError::ReaderStopped);
        }
        let result = self.read_record(cancelled);
        match result {
            Ok(Some((position, result))) => Ok(Some(JsonlRecord {
                position,
                result,
                _permit: &self.memory,
            })),
            Ok(None) => Ok(None),
            Err(error) => {
                self.stopped = true;
                Err(error)
            }
        }
    }

    fn read_record(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<Option<DecodedRecord>, ImportSourceError> {
        let start = self.offset;
        let mut hash = Sha256::new();
        let mut oversized = false;
        self.buffer.clear();
        loop {
            check_cancelled(cancelled)?;
            let available = self.input.fill_buf()?;
            if available.is_empty() {
                if self.offset != self.source.bytes {
                    return Err(ImportSourceError::SourceChanged);
                }
                if self.offset == start {
                    return Ok(None);
                }
                break;
            }
            let end = available.iter().position(|byte| *byte == b'\n');
            let payload = end.unwrap_or(available.len());
            let consumed = payload + usize::from(end.is_some());
            hash.update(&available[..consumed]);
            if !oversized {
                if payload > self.limits.wire_bytes - self.buffer.len() {
                    oversized = true;
                    self.buffer.clear();
                } else {
                    self.buffer.extend_from_slice(&available[..payload]);
                }
            }
            self.offset = self
                .offset
                .checked_add(consumed as u64)
                .filter(|n| *n <= self.source.bytes)
                .ok_or(ImportSourceError::SourceChanged)?;
            self.input.consume(consumed);
            if end.is_some() {
                break;
            }
        }
        check_cancelled(cancelled)?;
        let result = if oversized {
            Err(EventError::WireLimit)
        } else {
            verify_json(&self.buffer, self.limits)
        };
        check_cancelled(cancelled)?;
        self.line = self
            .line
            .checked_add(1)
            .ok_or(ImportSourceError::SourceChanged)?;
        Ok(Some((
            RecordPosition {
                source: self.source,
                line: self.line,
                byte_start: start,
                byte_end: self.offset,
                sha256: hash.finalize().into(),
            },
            result,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::import::ScratchSpace,
        runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    };
    fn reader() -> (tempfile::TempDir, JsonlReader) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source");
        std::fs::write(&path, b"{\"kind\":1}\n").unwrap();
        let scratch = ScratchSpace::new(dir.path(), 1024, 1).unwrap();
        let snapshot =
            SourceSnapshot::capture(&path, &scratch, None, &AtomicBool::new(false)).unwrap();
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                16 * 1024 * 1024,
                0,
                ResourceLimits::new(16 * 1024 * 1024, 1, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let memory = budget
            .try_acquire(ResourceRequest::data(16 * 1024 * 1024, 1, 0).unwrap())
            .unwrap();
        (
            dir,
            JsonlReader::new(snapshot, EventLimits::default(), memory).unwrap(),
        )
    }

    #[test]
    fn truncated_captured_storage_does_not_emit_a_completed_rejection() {
        let (_dir, mut reader) = reader();
        // Inject a storage fault through private access unavailable to callers.
        reader.input.get_mut().set_len(3).unwrap();
        assert!(matches!(
            reader.next_record(&AtomicBool::new(false)),
            Err(ImportSourceError::SourceChanged)
        ));
        assert!(matches!(
            reader.next_record(&AtomicBool::new(false)),
            Err(ImportSourceError::ReaderStopped)
        ));
    }

    #[test]
    fn actual_read_error_stops_instead_of_becoming_eof_or_invalid_json() {
        let (dir, mut reader) = reader();
        reader.input = BufReader::new(File::open(dir.path()).unwrap());
        assert!(matches!(
            reader.next_record(&AtomicBool::new(false)),
            Err(ImportSourceError::Io(_))
        ));
        assert!(matches!(
            reader.next_record(&AtomicBool::new(false)),
            Err(ImportSourceError::ReaderStopped)
        ));
    }
}
