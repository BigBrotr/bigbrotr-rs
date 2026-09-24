//! The bounded exact-ID export unit. Encoding is not durability or publication.
use bigbrotr_model::{Digest256, EventId};
use bigbrotr_protocol::VerifiedEvent;
use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonlError {
    ResourceLimit,
    Cancelled,
    WrongEvent,
    Output,
}

/// Describes bytes accepted by the writer, not fsync or a completed job. The
/// filesystem consumer must synchronize files/directories before using this in
/// its final manifest and committing the job checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonlFile {
    pub bytes: u64,
    pub rows: u64,
    pub sha256: Digest256,
}

pub fn write_exact_event(
    writer: impl Write,
    expected: EventId,
    event: Option<&VerifiedEvent>,
    maximum: u64,
    cancelled: &AtomicBool,
) -> Result<JsonlFile, JsonlError> {
    if cancelled.load(Ordering::Acquire) {
        return Err(JsonlError::Cancelled);
    }
    // The verifier already counted this exact SignedEvent serialization. Include
    // the LF before any output, so a lowered limit cannot emit a partial row.
    let bytes = match event {
        Some(event) => {
            if event.event().id != expected {
                return Err(JsonlError::WrongEvent);
            }
            u64::try_from(event.normalized_bytes())
                .ok()
                .and_then(|v| v.checked_add(1))
                .filter(|&v| v <= maximum)
                .ok_or(JsonlError::ResourceLimit)?
        }
        None => 0,
    };
    let mut output = Output {
        writer,
        cancelled,
        maximum: bytes,
        bytes: 0,
        hash: Sha256::new(),
        failure: None,
    };
    if let Some(event) = event {
        serde_json::to_writer(&mut output, event.event())
            .map_err(|_| output.failure.unwrap_or(JsonlError::Output))?;
        output
            .write_all(b"\n")
            .map_err(|_| output.failure.unwrap_or(JsonlError::Output))?;
    }
    if cancelled.load(Ordering::Acquire) {
        return Err(JsonlError::Cancelled);
    }
    if output.bytes != bytes {
        return Err(JsonlError::Output);
    }
    Ok(JsonlFile {
        bytes,
        rows: u64::from(event.is_some()),
        sha256: Digest256::from_bytes(output.hash.finalize().into()),
    })
}

struct Output<'a, W> {
    writer: W,
    cancelled: &'a AtomicBool,
    maximum: u64,
    bytes: u64,
    hash: Sha256,
    failure: Option<JsonlError>,
}
impl<W: Write> Write for Output<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let failure = if self.cancelled.load(Ordering::Acquire) {
            Some(JsonlError::Cancelled)
        } else if bytes.len() as u64 > self.maximum.saturating_sub(self.bytes) {
            Some(JsonlError::ResourceLimit)
        } else {
            None
        };
        if let Some(failure) = failure {
            self.failure = Some(failure);
            return Err(io::Error::other("export output refused"));
        }
        let written = self.writer.write(bytes)?;
        self.hash.update(&bytes[..written]);
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
