//! The existing projection-chunk/1 fingerprint, streamed without row buffers.
use super::{IndexTarget, ReferenceTuple, RowsError, SCHEMA_VERSION, SourceTuple};
use bigbrotr_model::{Digest256, ProjectionGenerationId, SourceCut};
use minicbor::{Encoder, encode::Write};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, convert::Infallible};

pub(super) mod sealed {
    pub trait Sealed {}
}

/// Implemented only by this module's closed initial row schemas. The fingerprint
/// row value is an array in serialized column order, with binary identities,
/// native integer domains, null optional values and unchanged text/arrays.
pub trait ProjectionRow: sealed::Sealed {
    fn projection_generation(&self) -> [u8; 16];
    fn source(&self) -> SourceTuple;
    fn discriminator(&self) -> u64;
    fn supports(target: IndexTarget) -> bool;
    #[doc(hidden)]
    fn encode_value(&self, value: &mut ValueWriter) -> Result<(), RowsError>;
}

struct HashWriter(Sha256);
impl Write for HashWriter {
    type Error = Infallible;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0.update(bytes);
        Ok(())
    }
}

/// Opaque closed primitive writer used by ProjectionRow. In addition to canonical
/// hashing it counts the exact uncompressed RowBinary value bytes (not transport
/// headers, compression or engine buffers), without allocating an output buffer.
#[doc(hidden)]
pub struct ValueWriter {
    encoder: Encoder<HashWriter>,
    wire_bytes: u64,
}

macro_rules! encode {
    ($value:expr) => {
        $value.expect("primitive canonical encoding into SHA256 is infallible")
    };
}

fn length_bytes(length: u64) -> u64 {
    let bits = u64::BITS - length.leading_zeros();
    u64::from(bits.max(1).div_ceil(7))
}

impl ValueWriter {
    fn charge(&mut self, bytes: u64) -> Result<(), RowsError> {
        self.wire_bytes = self
            .wire_bytes
            .checked_add(bytes)
            .ok_or(RowsError::SizeOverflow)?;
        Ok(())
    }
    pub(super) fn array(&mut self, length: u64) {
        encode!(self.encoder.array(length));
    }
    fn null(&mut self) {
        encode!(self.encoder.null());
    }
    pub(super) fn bytes<const N: usize>(&mut self, value: &[u8; N]) -> Result<(), RowsError> {
        encode!(self.encoder.bytes(value));
        self.charge(N as u64)
    }
    pub(super) fn u16(&mut self, value: &u16) -> Result<(), RowsError> {
        encode!(self.encoder.u16(*value));
        self.charge(2)
    }
    pub(super) fn u32(&mut self, value: &u32) -> Result<(), RowsError> {
        encode!(self.encoder.u32(*value));
        self.charge(4)
    }
    pub(super) fn u64(&mut self, value: &u64) -> Result<(), RowsError> {
        encode!(self.encoder.u64(*value));
        self.charge(8)
    }
    pub(super) fn i64(&mut self, value: &i64) -> Result<(), RowsError> {
        encode!(self.encoder.i64(*value));
        self.charge(8)
    }
    pub(super) fn text(&mut self, value: &str) -> Result<(), RowsError> {
        encode!(self.encoder.str(value));
        self.charge(value.len() as u64)?;
        self.charge(length_bytes(value.len() as u64))
    }
    pub(super) fn source(&mut self, value: &SourceTuple) -> Result<(), RowsError> {
        self.array(4);
        self.bytes(&value.0)?;
        self.u32(&value.1)?;
        self.u64(&value.2)?;
        self.u64(&value.3)
    }
    pub(super) fn reference(&mut self, value: &ReferenceTuple) -> Result<(), RowsError> {
        self.array(4);
        self.bytes(&value.0)?;
        self.u16(&value.1)?;
        self.bytes(&value.2)?;
        self.u64(&value.3)
    }
    pub(super) fn tags(&mut self, value: &[Vec<String>]) -> Result<(), RowsError> {
        self.array(value.len() as u64);
        self.charge(length_bytes(value.len() as u64))?;
        for tag in value {
            self.array(tag.len() as u64);
            self.charge(length_bytes(tag.len() as u64))?;
            for text in tag {
                self.text(text)?;
            }
        }
        Ok(())
    }
    pub(super) fn optional_bytes<const N: usize>(
        &mut self,
        value: &Option<[u8; N]>,
    ) -> Result<(), RowsError> {
        self.charge(1)?;
        match value {
            Some(value) => self.bytes(value),
            None => {
                self.null();
                Ok(())
            }
        }
    }
    pub(super) fn optional_text(&mut self, value: &Option<Cow<'_, str>>) -> Result<(), RowsError> {
        self.charge(1)?;
        match value {
            Some(value) => self.text(value),
            None => {
                self.null();
                Ok(())
            }
        }
    }
    pub(super) fn optional_u16(&mut self, value: &Option<u16>) -> Result<(), RowsError> {
        self.charge(1)?;
        match value {
            Some(value) => self.u16(value),
            None => {
                self.null();
                Ok(())
            }
        }
    }
    pub(super) fn optional_u64(&mut self, value: &Option<u64>) -> Result<(), RowsError> {
        self.charge(1)?;
        match value {
            Some(value) => self.u64(value),
            None => {
                self.null();
                Ok(())
            }
        }
    }
    pub(super) fn optional_source(&mut self, value: &Option<SourceTuple>) -> Result<(), RowsError> {
        // Array length0/1 has one byte on the wire; canonical meaning is null or
        // the complete SourceRecordId, not an untyped array of corrections.
        self.charge(1)?;
        match value {
            Some(value) => self.source(value),
            None => {
                self.null();
                Ok(())
            }
        }
    }
    pub(super) fn optional_reference(
        &mut self,
        value: &Option<ReferenceTuple>,
    ) -> Result<(), RowsError> {
        self.charge(1)?;
        match value {
            Some(value) => self.reference(value),
            None => {
                self.null();
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSummary {
    pub target: IndexTarget,
    pub row_count: u64,
    pub rows_digest: Digest256,
    /// Exact serialized value bytes before compression. Driver/native buffers,
    /// response bounds and decoded readback allocations need separate grants.
    pub row_binary_bytes: u64,
}

pub struct ChunkHasher<'a> {
    generation: [u8; 16],
    target: IndexTarget,
    cut: &'a SourceCut,
    expected_rows: u64,
    rows: u64,
    last: Option<(SourceTuple, u64)>,
    value: ValueWriter,
}

impl<'a> ChunkHasher<'a> {
    /// One profile-1 partition ('all'), one chunk ordinal (0) per target. The
    /// caller pre-counts the bounded source before starting this streaming hash.
    /// Even zero rows hash the complete generation/target/schema/cut header.
    pub fn new(
        generation: ProjectionGenerationId,
        target: IndexTarget,
        cut: &'a SourceCut,
        expected_rows: u64,
    ) -> Self {
        let mut encoder = Encoder::new(HashWriter(Sha256::new()));
        encode!(
            encoder
                .writer_mut()
                .write_all(b"bigbrotr/projection-chunk/1\0")
        );
        encode!(encoder.array(6));
        encode!(encoder.bytes(generation.as_bytes()));
        encode!(encoder.str(target.as_str()));
        encode!(encoder.u16(SCHEMA_VERSION));
        encode!(encoder.array(3));
        encode!(encoder.bytes(cut.dataset_id().as_bytes()));
        encode!(encoder.u64(cut.topology_generation().get()));
        encode!(encoder.array(cut.bounds().len() as u64));
        for bound in cut.bounds() {
            encode!(encoder.array(3));
            encode!(encoder.u32(bound.stream.shard_id));
            encode!(encoder.u64(bound.stream.owner_epoch.get()));
            encode!(encoder.u64(bound.last_complete_sequence.get()));
        }
        encode!(encoder.u64(0));
        encode!(encoder.array(expected_rows));
        Self {
            generation: *generation.as_bytes(),
            target,
            cut,
            expected_rows,
            rows: 0,
            last: None,
            value: ValueWriter {
                encoder,
                wire_bytes: 0,
            },
        }
    }

    pub const fn target(&self) -> IndexTarget {
        self.target
    }

    /// Readback must remove only byte-identical physical copies and order the
    /// remaining full rows by source/discriminator. Two different values with
    /// one immutable identity are refused, never collapsed by an engine merge.
    pub fn push<R: ProjectionRow>(&mut self, row: &R) -> Result<(), RowsError> {
        if !R::supports(self.target) {
            return Err(RowsError::WrongTarget);
        }
        if row.projection_generation() != self.generation {
            return Err(RowsError::WrongGeneration);
        }
        if self.rows >= self.expected_rows {
            return Err(RowsError::RowCount);
        }
        let source = row.source();
        let in_cut = source.0 == *self.cut.dataset_id().as_bytes()
            && source.3 != 0
            && self.cut.bounds().iter().any(|bound| {
                bound.stream.shard_id == source.1
                    && bound.stream.owner_epoch.get() == source.2
                    && source.3 <= bound.last_complete_sequence.get()
            });
        if !in_cut {
            return Err(RowsError::OutsideCut);
        }
        let identity = (source, row.discriminator());
        if self.last.is_some_and(|last| last >= identity) {
            return Err(RowsError::UnorderedRows);
        }
        // The discriminator's relation is the exact target name. All rows for
        // one source/target sort by the numeric zero-based posting ordinal;
        // canonical CBOR unsigned encoding has the same lexicographic order.
        let encoder = &mut self.value.encoder;
        encode!(encoder.array(3));
        encode!(encoder.array(4));
        encode!(encoder.bytes(&source.0));
        encode!(encoder.u32(source.1));
        encode!(encoder.u64(source.2));
        encode!(encoder.u64(source.3));
        encode!(encoder.array(2));
        encode!(encoder.str(self.target.as_str()));
        encode!(encoder.u64(row.discriminator()));
        row.encode_value(&mut self.value)?;
        self.rows += 1;
        self.last = Some(identity);
        Ok(())
    }

    pub fn finish(self) -> Result<ChunkSummary, RowsError> {
        if self.rows != self.expected_rows {
            return Err(RowsError::RowCount);
        }
        Ok(ChunkSummary {
            target: self.target,
            row_count: self.rows,
            rows_digest: Digest256::from_bytes(
                self.value.encoder.into_writer().0.finalize().into(),
            ),
            row_binary_bytes: self.value.wire_bytes,
        })
    }
}
