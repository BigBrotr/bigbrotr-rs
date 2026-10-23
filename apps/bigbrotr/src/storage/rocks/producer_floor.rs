//! Portable closed-producer floor data, never permission to reclaim or replay.
use std::fmt;

pub const CLOSED_PRODUCER_FLOOR_BYTES: usize = 139;

/// The key supplies producer slot/epoch. PostgreSQL must authenticate every
/// field against its immutable history before this data can authorize anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedProducerFloorFields {
    pub issued_through: u64,
    pub manifest_sha256: [u8; 32],
    pub source_through: u64,
    pub closure_digest: [u8; 32],
    pub checkpoint_revision: u64,
    pub checkpoint_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedProducerFloor(ClosedProducerFloorFields);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedProducerFloorError;
impl fmt::Display for ClosedProducerFloorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid closed-producer floor")
    }
}
impl std::error::Error for ClosedProducerFloorError {}

impl ClosedProducerFloor {
    pub fn new(fields: ClosedProducerFloorFields) -> Result<Self, ClosedProducerFloorError> {
        if fields.issued_through == 0
            || fields.issued_through > 64
            || fields.checkpoint_revision == 0
        {
            return Err(ClosedProducerFloorError);
        }
        Ok(Self(fields))
    }
    pub fn fields(&self) -> &ClosedProducerFloorFields {
        &self.0
    }
    pub fn encode(&self) -> [u8; CLOSED_PRODUCER_FLOOR_BYTES] {
        let mut bytes = [0; CLOSED_PRODUCER_FLOOR_BYTES];
        bytes[..2].copy_from_slice(&[3, 0x31]);
        for at in [2, 10, 18] {
            bytes[at..at + 8].copy_from_slice(&self.0.issued_through.to_be_bytes());
        }
        bytes[26] = 1;
        bytes[27..59].copy_from_slice(&self.0.manifest_sha256);
        bytes[59..67].copy_from_slice(&self.0.source_through.to_be_bytes());
        bytes[67..99].copy_from_slice(&self.0.closure_digest);
        bytes[99..107].copy_from_slice(&self.0.checkpoint_revision.to_be_bytes());
        bytes[107..139].copy_from_slice(&self.0.checkpoint_digest);
        bytes
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ClosedProducerFloorError> {
        if bytes.len() != CLOSED_PRODUCER_FLOOR_BYTES || bytes[..2] != [3, 0x31] || bytes[26] != 1 {
            return Err(ClosedProducerFloorError);
        }
        let number = |at| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("fixed floor"));
        let issued = number(2);
        if number(10) != issued || number(18) != issued {
            return Err(ClosedProducerFloorError);
        }
        Self::new(ClosedProducerFloorFields {
            issued_through: issued,
            manifest_sha256: bytes[27..59].try_into().expect("fixed floor"),
            source_through: number(59),
            closure_digest: bytes[67..99].try_into().expect("fixed floor"),
            checkpoint_revision: number(99),
            checkpoint_digest: bytes[107..139].try_into().expect("fixed floor"),
        })
    }
}
