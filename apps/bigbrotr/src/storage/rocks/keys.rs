//! Closed version-1 keys for the ingest owner's three RocksDB column families.
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, Digest256, EventId, Ordinal, OwnerEpoch,
    ProducerEpoch, ProducerRequestId, ProducerSlotId, QualifiedRef, RequestSequence, Sequence,
    SourceRecordId,
};
use std::fmt;

const FORMAT_VERSION: u8 = 1;
pub const MAX_INGEST_KEY_BYTES: usize = 218;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFamily {
    Default,
    Facts,
    Receipts,
}

impl ColumnFamily {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Facts => "facts",
            Self::Receipts => "receipts",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KeyDomain {
    Manifest = 0x01,
    SourceStream = 0x02,
    AllocationStream = 0x03,
    Dictionary = 0x04,
    DictionaryReverseCandidate = 0x05,
    EventPayload = 0x06,
    EventEligibility = 0x07,
    EventReceive = 0x08,
    Provenance = 0x09,
    Fact = 0x20,
    Request = 0x30,
    ProducerState = 0x31,
}

impl KeyDomain {
    pub const fn prefix(self) -> [u8; 2] {
        [FORMAT_VERSION, self as u8]
    }

    pub const fn column_family(self) -> ColumnFamily {
        match self {
            Self::Manifest
            | Self::SourceStream
            | Self::AllocationStream
            | Self::Dictionary
            | Self::DictionaryReverseCandidate
            | Self::EventPayload
            | Self::EventEligibility
            | Self::EventReceive
            | Self::Provenance => ColumnFamily::Default,
            Self::Fact => ColumnFamily::Facts,
            Self::Request | Self::ProducerState => ColumnFamily::Receipts,
        }
    }

    const fn encoded_len(self) -> usize {
        match self {
            Self::Manifest => 2,
            Self::SourceStream => 2 + 16 + 4 + 8,
            Self::AllocationStream => 2 + 16 + 2 + 16,
            Self::Dictionary => 2 + 42,
            Self::DictionaryReverseCandidate => 2 + 16 + 2 + 32 + 42,
            Self::EventPayload => 2 + 16 + 32,
            Self::EventEligibility | Self::EventReceive => 2 + 16 + 32 + 42,
            Self::Provenance => 2 + 16 + 32 + 4 * 42,
            Self::Fact => 2 + 16 + 4 + 8 + 8,
            Self::Request => 2 + 16 + 8 + 8,
            Self::ProducerState => 2 + 16 + 8,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, KeyError> {
        match tag {
            0x01 => Ok(Self::Manifest),
            0x02 => Ok(Self::SourceStream),
            0x03 => Ok(Self::AllocationStream),
            0x04 => Ok(Self::Dictionary),
            0x05 => Ok(Self::DictionaryReverseCandidate),
            0x06 => Ok(Self::EventPayload),
            0x07 => Ok(Self::EventEligibility),
            0x08 => Ok(Self::EventReceive),
            0x09 => Ok(Self::Provenance),
            0x20 => Ok(Self::Fact),
            0x30 => Ok(Self::Request),
            0x31 => Ok(Self::ProducerState),
            _ => Err(KeyError::UnknownDomain),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestKey {
    Manifest,
    SourceStream {
        dataset_id: DatasetId,
        shard_id: u32,
        owner_epoch: OwnerEpoch,
    },
    AllocationStream {
        dataset_id: DatasetId,
        dictionary_kind: DictionaryKind,
        allocation_domain: AllocationDomainId,
    },
    Dictionary {
        reference: QualifiedRef,
    },
    DictionaryReverseCandidate {
        dataset_id: DatasetId,
        dictionary_kind: DictionaryKind,
        descriptor_digest: Digest256,
        reference: QualifiedRef,
    },
    EventPayload {
        dataset_id: DatasetId,
        event_id: EventId,
    },
    EventEligibility {
        dataset_id: DatasetId,
        event_id: EventId,
        visibility_scope: QualifiedRef,
    },
    EventReceive {
        dataset_id: DatasetId,
        event_id: EventId,
        visibility_scope: QualifiedRef,
    },
    Provenance {
        dataset_id: DatasetId,
        event_id: EventId,
        endpoint_id: QualifiedRef,
        observer_id: QualifiedRef,
        access_scope_id: QualifiedRef,
        visibility_scope_id: QualifiedRef,
    },
    Fact {
        source_record_id: SourceRecordId,
    },
    Request {
        request_id: ProducerRequestId,
    },
    ProducerState {
        producer_slot_id: ProducerSlotId,
        producer_epoch: ProducerEpoch,
    },
}

impl IngestKey {
    pub const fn domain(&self) -> KeyDomain {
        match self {
            Self::Manifest => KeyDomain::Manifest,
            Self::SourceStream { .. } => KeyDomain::SourceStream,
            Self::AllocationStream { .. } => KeyDomain::AllocationStream,
            Self::Dictionary { .. } => KeyDomain::Dictionary,
            Self::DictionaryReverseCandidate { .. } => KeyDomain::DictionaryReverseCandidate,
            Self::EventPayload { .. } => KeyDomain::EventPayload,
            Self::EventEligibility { .. } => KeyDomain::EventEligibility,
            Self::EventReceive { .. } => KeyDomain::EventReceive,
            Self::Provenance { .. } => KeyDomain::Provenance,
            Self::Fact { .. } => KeyDomain::Fact,
            Self::Request { .. } => KeyDomain::Request,
            Self::ProducerState { .. } => KeyDomain::ProducerState,
        }
    }

    pub fn encode(&self) -> Result<EncodedKey, KeyError> {
        self.validate()?;
        let domain = self.domain();
        let mut writer = KeyWriter::new();
        writer.bytes(&domain.prefix());
        match *self {
            Self::Manifest => {}
            Self::SourceStream {
                dataset_id,
                shard_id,
                owner_epoch,
            } => {
                writer.bytes(dataset_id.as_bytes());
                writer.u32(shard_id);
                writer.u64(owner_epoch.get());
            }
            Self::AllocationStream {
                dataset_id,
                dictionary_kind,
                allocation_domain,
            } => {
                writer.bytes(dataset_id.as_bytes());
                writer.u16(dictionary_kind as u16);
                writer.bytes(allocation_domain.as_bytes());
            }
            Self::Dictionary { reference } => writer.reference(reference),
            Self::DictionaryReverseCandidate {
                dataset_id,
                dictionary_kind,
                descriptor_digest,
                reference,
            } => {
                writer.bytes(dataset_id.as_bytes());
                writer.u16(dictionary_kind as u16);
                writer.bytes(descriptor_digest.as_bytes());
                writer.reference(reference);
            }
            Self::EventPayload {
                dataset_id,
                event_id,
            } => {
                writer.bytes(dataset_id.as_bytes());
                writer.bytes(event_id.as_bytes());
            }
            Self::EventEligibility {
                dataset_id,
                event_id,
                visibility_scope,
            }
            | Self::EventReceive {
                dataset_id,
                event_id,
                visibility_scope,
            } => {
                writer.bytes(dataset_id.as_bytes());
                writer.bytes(event_id.as_bytes());
                writer.reference(visibility_scope);
            }
            Self::Provenance {
                dataset_id,
                event_id,
                endpoint_id,
                observer_id,
                access_scope_id,
                visibility_scope_id,
            } => {
                writer.bytes(dataset_id.as_bytes());
                writer.bytes(event_id.as_bytes());
                writer.reference(endpoint_id);
                writer.reference(observer_id);
                writer.reference(access_scope_id);
                writer.reference(visibility_scope_id);
            }
            Self::Fact { source_record_id } => writer.source(source_record_id),
            Self::Request { request_id } => writer.request(request_id),
            Self::ProducerState {
                producer_slot_id,
                producer_epoch,
            } => {
                writer.bytes(producer_slot_id.as_bytes());
                writer.u64(producer_epoch.get());
            }
        }
        debug_assert_eq!(writer.len, domain.encoded_len());
        Ok(writer.finish(domain))
    }

    fn validate(&self) -> Result<(), KeyError> {
        match *self {
            Self::DictionaryReverseCandidate {
                dataset_id,
                dictionary_kind,
                reference,
                ..
            } => require(reference, dataset_id, dictionary_kind),
            Self::EventEligibility {
                dataset_id,
                visibility_scope,
                ..
            }
            | Self::EventReceive {
                dataset_id,
                visibility_scope,
                ..
            } => require(
                visibility_scope,
                dataset_id,
                DictionaryKind::VisibilityScope,
            ),
            Self::Provenance {
                dataset_id,
                endpoint_id,
                observer_id,
                access_scope_id,
                visibility_scope_id,
                ..
            } => {
                require(endpoint_id, dataset_id, DictionaryKind::Endpoint)?;
                require(observer_id, dataset_id, DictionaryKind::Observer)?;
                require(access_scope_id, dataset_id, DictionaryKind::AccessScope)?;
                require(
                    visibility_scope_id,
                    dataset_id,
                    DictionaryKind::VisibilityScope,
                )
            }
            _ => Ok(()),
        }
    }
}

fn require(
    reference: QualifiedRef,
    dataset_id: DatasetId,
    dictionary_kind: DictionaryKind,
) -> Result<(), KeyError> {
    if reference.dataset_id != dataset_id {
        Err(KeyError::ForeignDataset)
    } else if reference.dictionary_kind != dictionary_kind {
        Err(KeyError::WrongDictionaryKind)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedKey {
    bytes: [u8; MAX_INGEST_KEY_BYTES],
    len: usize,
    domain: KeyDomain,
}

impl EncodedKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    pub const fn domain(&self) -> KeyDomain {
        self.domain
    }

    pub const fn column_family(&self) -> ColumnFamily {
        self.domain.column_family()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    WrongLength,
    UnsupportedVersion,
    UnknownDomain,
    WrongColumnFamily,
    ZeroIdentity,
    ZeroCounter,
    UnknownDictionaryKind,
    ForeignDataset,
    WrongDictionaryKind,
}

impl fmt::Display for KeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WrongLength => "ingest key has the wrong length",
            Self::UnsupportedVersion => "ingest key uses an unsupported format version",
            Self::UnknownDomain => "ingest key uses an unknown domain tag",
            Self::WrongColumnFamily => "ingest key domain is in the wrong column family",
            Self::ZeroIdentity => "ingest key contains an all-zero allocated identity",
            Self::ZeroCounter => "ingest key contains a zero allocated counter",
            Self::UnknownDictionaryKind => "ingest key contains an unknown dictionary kind",
            Self::ForeignDataset => "ingest key combines references from different datasets",
            Self::WrongDictionaryKind => "ingest key reference has the wrong dictionary kind",
        })
    }
}

impl std::error::Error for KeyError {}

pub fn decode_key(column_family: ColumnFamily, bytes: &[u8]) -> Result<IngestKey, KeyError> {
    if bytes.len() < 2 {
        return Err(KeyError::WrongLength);
    }
    if bytes[0] != FORMAT_VERSION {
        return Err(KeyError::UnsupportedVersion);
    }
    let domain = KeyDomain::from_tag(bytes[1])?;
    if domain.column_family() != column_family {
        return Err(KeyError::WrongColumnFamily);
    }
    if bytes.len() != domain.encoded_len() {
        return Err(KeyError::WrongLength);
    }
    let mut reader = KeyReader { bytes, position: 2 };
    let key = match domain {
        KeyDomain::Manifest => IngestKey::Manifest,
        KeyDomain::SourceStream => IngestKey::SourceStream {
            dataset_id: reader.dataset()?,
            shard_id: reader.u32()?,
            owner_epoch: OwnerEpoch::new(reader.u64()?).map_err(|_| KeyError::ZeroCounter)?,
        },
        KeyDomain::AllocationStream => IngestKey::AllocationStream {
            dataset_id: reader.dataset()?,
            dictionary_kind: reader.kind()?,
            allocation_domain: reader.allocation_domain()?,
        },
        KeyDomain::Dictionary => IngestKey::Dictionary {
            reference: reader.reference()?,
        },
        KeyDomain::DictionaryReverseCandidate => IngestKey::DictionaryReverseCandidate {
            dataset_id: reader.dataset()?,
            dictionary_kind: reader.kind()?,
            descriptor_digest: Digest256::from_bytes(reader.fixed()?),
            reference: reader.reference()?,
        },
        KeyDomain::EventPayload => IngestKey::EventPayload {
            dataset_id: reader.dataset()?,
            event_id: EventId::from_bytes(reader.fixed()?),
        },
        KeyDomain::EventEligibility => IngestKey::EventEligibility {
            dataset_id: reader.dataset()?,
            event_id: EventId::from_bytes(reader.fixed()?),
            visibility_scope: reader.reference()?,
        },
        KeyDomain::EventReceive => IngestKey::EventReceive {
            dataset_id: reader.dataset()?,
            event_id: EventId::from_bytes(reader.fixed()?),
            visibility_scope: reader.reference()?,
        },
        KeyDomain::Provenance => IngestKey::Provenance {
            dataset_id: reader.dataset()?,
            event_id: EventId::from_bytes(reader.fixed()?),
            endpoint_id: reader.reference()?,
            observer_id: reader.reference()?,
            access_scope_id: reader.reference()?,
            visibility_scope_id: reader.reference()?,
        },
        KeyDomain::Fact => IngestKey::Fact {
            source_record_id: reader.source()?,
        },
        KeyDomain::Request => IngestKey::Request {
            request_id: reader.request()?,
        },
        KeyDomain::ProducerState => IngestKey::ProducerState {
            producer_slot_id: reader.producer_slot()?,
            producer_epoch: ProducerEpoch::new(reader.u64()?).map_err(|_| KeyError::ZeroCounter)?,
        },
    };
    debug_assert_eq!(reader.position, bytes.len());
    key.validate()?;
    Ok(key)
}

struct KeyWriter {
    bytes: [u8; MAX_INGEST_KEY_BYTES],
    len: usize,
}

impl KeyWriter {
    const fn new() -> Self {
        Self {
            bytes: [0; MAX_INGEST_KEY_BYTES],
            len: 0,
        }
    }

    fn bytes(&mut self, value: &[u8]) {
        let end = self.len + value.len();
        self.bytes[self.len..end].copy_from_slice(value);
        self.len = end;
    }

    fn u16(&mut self, value: u16) {
        self.bytes(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }

    fn reference(&mut self, value: QualifiedRef) {
        self.bytes(value.dataset_id.as_bytes());
        self.u16(value.dictionary_kind as u16);
        self.bytes(value.allocation_domain.as_bytes());
        self.u64(value.ordinal.get());
    }

    fn source(&mut self, value: SourceRecordId) {
        self.bytes(value.dataset_id.as_bytes());
        self.u32(value.shard_id);
        self.u64(value.owner_epoch.get());
        self.u64(value.sequence.get());
    }

    fn request(&mut self, value: ProducerRequestId) {
        self.bytes(value.producer_slot_id.as_bytes());
        self.u64(value.producer_epoch.get());
        self.u64(value.request_sequence.get());
    }

    fn finish(self, domain: KeyDomain) -> EncodedKey {
        EncodedKey {
            bytes: self.bytes,
            len: self.len,
            domain,
        }
    }
}

struct KeyReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl KeyReader<'_> {
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], KeyError> {
        let end = self.position.checked_add(N).ok_or(KeyError::WrongLength)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(KeyError::WrongLength)?;
        self.position = end;
        value.try_into().map_err(|_| KeyError::WrongLength)
    }

    fn u16(&mut self) -> Result<u16, KeyError> {
        Ok(u16::from_be_bytes(self.fixed()?))
    }

    fn u32(&mut self) -> Result<u32, KeyError> {
        Ok(u32::from_be_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64, KeyError> {
        Ok(u64::from_be_bytes(self.fixed()?))
    }

    fn dataset(&mut self) -> Result<DatasetId, KeyError> {
        DatasetId::try_from(self.fixed()?).map_err(|_| KeyError::ZeroIdentity)
    }

    fn allocation_domain(&mut self) -> Result<AllocationDomainId, KeyError> {
        AllocationDomainId::try_from(self.fixed()?).map_err(|_| KeyError::ZeroIdentity)
    }

    fn producer_slot(&mut self) -> Result<ProducerSlotId, KeyError> {
        ProducerSlotId::try_from(self.fixed()?).map_err(|_| KeyError::ZeroIdentity)
    }

    fn kind(&mut self) -> Result<DictionaryKind, KeyError> {
        DictionaryKind::try_from(self.u16()?).map_err(|_| KeyError::UnknownDictionaryKind)
    }

    fn reference(&mut self) -> Result<QualifiedRef, KeyError> {
        Ok(QualifiedRef {
            dataset_id: self.dataset()?,
            dictionary_kind: self.kind()?,
            allocation_domain: self.allocation_domain()?,
            ordinal: Ordinal::new(self.u64()?).map_err(|_| KeyError::ZeroCounter)?,
        })
    }

    fn source(&mut self) -> Result<SourceRecordId, KeyError> {
        Ok(SourceRecordId {
            dataset_id: self.dataset()?,
            shard_id: self.u32()?,
            owner_epoch: OwnerEpoch::new(self.u64()?).map_err(|_| KeyError::ZeroCounter)?,
            sequence: Sequence::new(self.u64()?).map_err(|_| KeyError::ZeroCounter)?,
        })
    }

    fn request(&mut self) -> Result<ProducerRequestId, KeyError> {
        Ok(ProducerRequestId {
            producer_slot_id: self.producer_slot()?,
            producer_epoch: ProducerEpoch::new(self.u64()?).map_err(|_| KeyError::ZeroCounter)?,
            request_sequence: RequestSequence::new(self.u64()?)
                .map_err(|_| KeyError::ZeroCounter)?,
        })
    }
}
