//! Closed core fact and dictionary preimages only. Limits precede attacker-directed allocation;
//! exact re-encoding refuses noncanonical CBOR without a second output buffer.
//! This is not an archive manifest, general CBOR codec or authority certificate.
use crate::{
    AccessClass, AccessScopeDescriptor, AllocationDomainId, ClockQuality, ClockSource, DatasetId,
    DatasetLineageDescriptor, DescriptorLabel, DictionaryDescriptor, DictionaryEntry,
    DictionaryKind, Digest256, DurationUs, EndpointDescriptor, EventBody, EventEligibility,
    EventId, EventOrigin, EventPayload, EventReceive, Fact, FactPayload, NostrTime, ObservedTime,
    ObserverDescriptor, Ordinal, OwnerEpoch, Provenance, PublicKey, QualifiedRef, RouteClass,
    Sequence, Signature, SourceRecordId,
    dictionary::{
        DictionaryError, EVENT_VERIFICATION_NIPS_REVISION, MAX_DESCRIPTOR_ENDPOINT_BYTES,
        MAX_DESCRIPTOR_LABEL_BYTES,
    },
    fact::FactError,
    fingerprint,
};
use minicbor::{Decoder, Encoder, encode::Write};
use sha2::{Digest, Sha256};
use std::{fmt, mem::size_of};

const FACT_PREFIX: &[u8] = b"bigbrotr/fact/1\0";
const DICTIONARY_PREFIX: &[u8] = b"bigbrotr/dictionary/1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    InputLimit,
    AllocationLimit,
    TagLimit,
    StringLimit,
    AllocationFailed,
    Malformed,
    InvalidValue,
    IncompatibleSchema,
    NonCanonical,
    DigestMismatch,
    InvalidFact(FactError),
    InvalidDictionary(DictionaryError),
}
impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InputLimit => "persistent preimage exceeds the encoded byte limit",
            Self::AllocationLimit => "persistent value exceeds the requested allocation budget",
            Self::TagLimit => "fact tag structure exceeds the node limit",
            Self::StringLimit => "persistent string exceeds the UTF-8 byte limit",
            Self::AllocationFailed => "unable to reserve memory for the persistent value",
            Self::Malformed => "malformed closed persistent preimage",
            Self::InvalidValue => "invalid persistent scalar or reference",
            Self::IncompatibleSchema => "unsupported persistent profile, kind, version or enum",
            Self::NonCanonical => "persistent preimage is not exact profile-1 canonical CBOR",
            Self::DigestMismatch => "persistent bytes do not match the supplied digest",
            Self::InvalidFact(_) => "fact references violate structural invariants",
            Self::InvalidDictionary(_) => "dictionary descriptor violates structural invariants",
        })
    }
}
impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidFact(error) => Some(error),
            Self::InvalidDictionary(error) => Some(error),
            _ => None,
        }
    }
}
impl From<minicbor::decode::Error> for CodecError {
    fn from(_: minicbor::decode::Error) -> Self {
        Self::Malformed
    }
}

/// Local decode limits. An adapter must additionally reserve its shared runtime
/// grant, input buffer and concurrent work. Requested Vec/String bytes exclude
/// allocator overhead, so this is not a process RSS ceiling.
#[derive(Debug, Clone, Copy)]
pub struct FactDecodeLimits {
    pub max_encoded_bytes: usize,
    pub max_allocation_bytes: usize,
    /// Tag arrays plus their string elements, including empty strings/arrays.
    pub max_tag_nodes: usize,
    /// Retained content/tag/descriptor strings; fixed schema labels are borrowed.
    pub max_string_bytes: usize,
}
impl Default for FactDecodeLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 2 * 1024 * 1024,
            max_allocation_bytes: 8 * 1024 * 1024,
            max_tag_nodes: 65_536,
            max_string_bytes: 256 * 1024,
        }
    }
}

/// The same input/allocation/string budget applies to a standalone dictionary.
/// No tag nodes are present; descriptor-specific caps additionally apply.
pub type DictionaryDecodeLimits = FactDecodeLimits;

struct CountWriter {
    count: usize,
    limit: usize,
}
impl Write for CountWriter {
    type Error = CodecError;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.count = self
            .count
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or(CodecError::InputLimit)?;
        Ok(())
    }
}
struct CompareWriter<'a>(&'a [u8]);
impl Write for CompareWriter<'_> {
    type Error = CodecError;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = self.0.strip_prefix(bytes).ok_or(CodecError::NonCanonical)?;
        Ok(())
    }
}

impl Fact {
    /// Encode the exact fingerprint preimage, including its domain prefix. This
    /// carries all fact fields except the self-digest, which the storage owner
    /// binds separately. Size is checked before allocating the output buffer.
    pub fn encode_preimage(&self, max_encoded_bytes: usize) -> Result<Vec<u8>, CodecError> {
        let mut count = Encoder::new(CountWriter {
            count: 0,
            limit: max_encoded_bytes,
        });
        fingerprint::fact(self, &mut count).map_err(|_| CodecError::InputLimit)?;
        let size = count.into_writer().count;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| CodecError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        fingerprint::fact(self, &mut encoder).map_err(|_| CodecError::Malformed)?;
        if !encoder.into_writer().is_empty() {
            return Err(CodecError::Malformed);
        }
        Ok(bytes)
    }

    /// Decode only implemented v1 core facts. A matching digest establishes byte
    /// integrity relative to the caller's expected value, never its authority.
    /// It does not verify signatures, policy, dictionary mappings or causality.
    pub fn decode_preimage(
        bytes: &[u8],
        expected_digest: Digest256,
        limits: FactDecodeLimits,
    ) -> Result<Self, CodecError> {
        if bytes.len() > limits.max_encoded_bytes {
            return Err(CodecError::InputLimit);
        }
        let input = bytes
            .strip_prefix(FACT_PREFIX)
            .ok_or(CodecError::IncompatibleSchema)?;
        if Digest256::from_bytes(Sha256::digest(bytes).into()) != expected_digest {
            return Err(CodecError::DigestMismatch);
        }
        let mut reader = FactReader {
            decoder: Decoder::new(input),
            limits,
            allocated: 0,
            tag_nodes: 0,
        };
        let fact = reader.fact()?;
        if reader.decoder.position() != input.len() {
            return Err(CodecError::NonCanonical);
        }
        let mut encoder = Encoder::new(CompareWriter(bytes));
        fingerprint::fact(&fact, &mut encoder).map_err(|_| CodecError::NonCanonical)?;
        if !encoder.into_writer().0.is_empty() {
            return Err(CodecError::NonCanonical);
        }
        Ok(fact)
    }
}

impl DictionaryEntry {
    /// Canonical descriptor equality bytes without the allocated reference.
    /// Operational reverse lookup candidates must compare these complete bytes;
    /// their hash is an accelerator, never a scientific identity or authority.
    pub fn encode_descriptor(&self, max_encoded_bytes: usize) -> Result<Vec<u8>, CodecError> {
        let mut count = Encoder::new(CountWriter {
            count: 0,
            limit: max_encoded_bytes,
        });
        fingerprint::dictionary_descriptor(self, &mut count).map_err(|_| CodecError::InputLimit)?;
        let size = count.into_writer().count;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| CodecError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        fingerprint::dictionary_descriptor(self, &mut encoder)
            .map_err(|_| CodecError::Malformed)?;
        if !encoder.into_writer().is_empty() {
            return Err(CodecError::Malformed);
        }
        Ok(bytes)
    }

    /// Encode the standalone dictionary-domain preimage, distinct from the fact
    /// envelope containing the same mapping. Count before output reservation.
    pub fn encode_preimage(&self, max_encoded_bytes: usize) -> Result<Vec<u8>, CodecError> {
        let mut count = Encoder::new(CountWriter {
            count: 0,
            limit: max_encoded_bytes,
        });
        fingerprint::dictionary(self, &mut count).map_err(|_| CodecError::InputLimit)?;
        let size = count.into_writer().count;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| CodecError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        fingerprint::dictionary(self, &mut encoder).map_err(|_| CodecError::Malformed)?;
        if !encoder.into_writer().is_empty() {
            return Err(CodecError::Malformed);
        }
        Ok(bytes)
    }
    /// Validate closed bytes and integrity, not current allocation, URL
    /// normalization, secret classification, publication permission or authority.
    pub fn decode_preimage(
        bytes: &[u8],
        expected_digest: Digest256,
        limits: DictionaryDecodeLimits,
    ) -> Result<Self, CodecError> {
        if bytes.len() > limits.max_encoded_bytes {
            return Err(CodecError::InputLimit);
        }
        let input = bytes
            .strip_prefix(DICTIONARY_PREFIX)
            .ok_or(CodecError::IncompatibleSchema)?;
        if Digest256::from_bytes(Sha256::digest(bytes).into()) != expected_digest {
            return Err(CodecError::DigestMismatch);
        }
        let mut reader = FactReader {
            decoder: Decoder::new(input),
            limits,
            allocated: 0,
            tag_nodes: 0,
        };
        let value = reader.dictionary()?;
        if reader.decoder.position() != input.len() {
            return Err(CodecError::NonCanonical);
        }
        let mut encoder = Encoder::new(CompareWriter(bytes));
        fingerprint::dictionary(&value, &mut encoder).map_err(|_| CodecError::NonCanonical)?;
        if !encoder.into_writer().0.is_empty() {
            return Err(CodecError::NonCanonical);
        }
        Ok(value)
    }
}

struct FactReader<'a> {
    decoder: Decoder<'a>,
    limits: FactDecodeLimits,
    allocated: usize,
    tag_nodes: usize,
}
impl FactReader<'_> {
    fn array(&mut self, length: u64) -> Result<(), CodecError> {
        if self.decoder.array()? != Some(length) {
            return Err(CodecError::Malformed);
        }
        Ok(())
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.decoder
            .bytes()?
            .try_into()
            .map_err(|_| CodecError::InvalidValue)
    }
    fn reference(&mut self) -> Result<QualifiedRef, CodecError> {
        self.array(4)?;
        Ok(QualifiedRef {
            dataset_id: DatasetId::try_from(self.fixed()?).map_err(|_| CodecError::InvalidValue)?,
            dictionary_kind: DictionaryKind::try_from(self.decoder.u16()?)
                .map_err(|_| CodecError::IncompatibleSchema)?,
            allocation_domain: AllocationDomainId::try_from(self.fixed()?)
                .map_err(|_| CodecError::InvalidValue)?,
            ordinal: Ordinal::new(self.decoder.u64()?).map_err(|_| CodecError::InvalidValue)?,
        })
    }
    fn source(&mut self) -> Result<SourceRecordId, CodecError> {
        self.array(4)?;
        Ok(SourceRecordId {
            dataset_id: DatasetId::try_from(self.fixed()?).map_err(|_| CodecError::InvalidValue)?,
            shard_id: self.decoder.u32()?,
            owner_epoch: OwnerEpoch::new(self.decoder.u64()?)
                .map_err(|_| CodecError::InvalidValue)?,
            sequence: Sequence::new(self.decoder.u64()?).map_err(|_| CodecError::InvalidValue)?,
        })
    }
    fn nullable<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, CodecError>,
    ) -> Result<Option<T>, CodecError> {
        if self.decoder.datatype()? == minicbor::data::Type::Null {
            self.decoder.null()?;
            Ok(None)
        } else {
            read(self).map(Some)
        }
    }
    fn clock(&mut self) -> Result<ClockQuality, CodecError> {
        self.array(4)?;
        let id = self.reference()?;
        let resolution = DurationUs::new(self.decoder.u64()?);
        let uncertainty = self.nullable(|reader| Ok(DurationUs::new(reader.decoder.u64()?)))?;
        let flags = self.decoder.u32()?;
        if flags & !7 != 0 {
            return Err(CodecError::IncompatibleSchema);
        }
        ClockQuality::new(id, resolution, uncertainty, flags).map_err(|_| CodecError::InvalidValue)
    }
    fn origin(&mut self) -> Result<EventOrigin, CodecError> {
        match self.decoder.str()? {
            "network" => {
                self.decoder.null()?;
                Ok(EventOrigin::Network)
            }
            "import" => Ok(EventOrigin::Import(self.reference()?)),
            _ => Err(CodecError::IncompatibleSchema),
        }
    }
    fn charge(&mut self, bytes: usize) -> Result<(), CodecError> {
        self.allocated = self
            .allocated
            .checked_add(bytes)
            .filter(|n| *n <= self.limits.max_allocation_bytes)
            .ok_or(CodecError::AllocationLimit)?;
        Ok(())
    }
    fn text(&mut self) -> Result<String, CodecError> {
        self.text_bounded(self.limits.max_string_bytes)
    }
    fn text_bounded(&mut self, maximum: usize) -> Result<String, CodecError> {
        let value = self.decoder.str()?;
        if value.len() > self.limits.max_string_bytes.min(maximum) {
            return Err(CodecError::StringLimit);
        }
        self.charge(value.len())?;
        let mut text = String::new();
        text.try_reserve_exact(value.len())
            .map_err(|_| CodecError::AllocationFailed)?;
        text.push_str(value);
        Ok(text)
    }
    fn tag_array<T>(&mut self) -> Result<(Vec<T>, usize), CodecError> {
        let length = self.decoder.array()?.ok_or(CodecError::Malformed)?;
        let length = usize::try_from(length).map_err(|_| CodecError::TagLimit)?;
        self.tag_nodes = self
            .tag_nodes
            .checked_add(length)
            .filter(|n| *n <= self.limits.max_tag_nodes)
            .ok_or(CodecError::TagLimit)?;
        // Each declared child needs at least one byte still in the input. This
        // refuses impossible lengths even if a caller supplied very large caps.
        if length > self.decoder.input().len() - self.decoder.position() {
            return Err(CodecError::Malformed);
        }
        self.charge(
            length
                .checked_mul(size_of::<T>())
                .ok_or(CodecError::AllocationLimit)?,
        )?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(length)
            .map_err(|_| CodecError::AllocationFailed)?;
        Ok((values, length))
    }
    fn body(&mut self) -> Result<EventBody, CodecError> {
        let event_id = EventId::from_bytes(self.fixed()?);
        let pubkey = PublicKey::from_bytes(self.fixed()?);
        let created_at = NostrTime(self.decoder.u64()?);
        let kind = self.decoder.u16()?;
        let (mut tags, count) = self.tag_array()?;
        for _ in 0..count {
            let (mut tag, count) = self.tag_array()?;
            for _ in 0..count {
                tag.push(self.text()?);
            }
            tags.push(tag);
        }
        let content = self.text()?;
        Ok(EventBody {
            event_id,
            pubkey,
            created_at,
            kind,
            tags,
            content,
        })
    }
    fn map(&mut self, length: u64) -> Result<(), CodecError> {
        if self.decoder.map()? != Some(length) {
            return Err(CodecError::Malformed);
        }
        Ok(())
    }
    fn literal(&mut self, expected: &str) -> Result<(), CodecError> {
        if self.decoder.str()? != expected {
            return Err(CodecError::IncompatibleSchema);
        }
        Ok(())
    }
    fn label(&mut self) -> Result<DescriptorLabel, CodecError> {
        DescriptorLabel::new(self.text_bounded(MAX_DESCRIPTOR_LABEL_BYTES)?)
            .map_err(CodecError::InvalidDictionary)
    }
    fn dictionary(&mut self) -> Result<DictionaryEntry, CodecError> {
        self.array(5)?;
        let reference = self.reference()?;
        if self.decoder.u16()? != 1 {
            return Err(CodecError::IncompatibleSchema);
        }
        let schema = self.decoder.str()?;
        if self.decoder.u16()? != 1 {
            return Err(CodecError::IncompatibleSchema);
        }
        let descriptor = match (reference.dictionary_kind, schema) {
            (DictionaryKind::Event, "event") => {
                self.map(1)?;
                self.literal("event_id")?;
                DictionaryDescriptor::Event(EventId::from_bytes(self.fixed()?))
            }
            (DictionaryKind::VisibilityScope, "visibility") => {
                self.map(1)?;
                self.literal("class")?;
                DictionaryDescriptor::Visibility {
                    class: self.label()?,
                }
            }
            (DictionaryKind::Endpoint, "endpoint") => {
                self.map(1)?;
                self.literal("url")?;
                DictionaryDescriptor::Endpoint(
                    EndpointDescriptor::new(self.text_bounded(MAX_DESCRIPTOR_ENDPOINT_BYTES)?)
                        .map_err(CodecError::InvalidDictionary)?,
                )
            }
            (DictionaryKind::Observer, "observer") => {
                self.map(7)?;
                self.literal("clock")?;
                let clock = self.nullable(|r| match r.decoder.str()? {
                    "system-utc" => Ok(ClockSource::SystemUtc),
                    "external-utc" => Ok(ClockSource::ExternalUtc),
                    _ => Err(CodecError::IncompatibleSchema),
                })?;
                self.literal("vantage")?;
                let vantage = self.nullable(Self::label)?;
                self.literal("software")?;
                let software = self.nullable(Self::label)?;
                self.literal("method_id")?;
                let method_id = self.nullable(Self::reference)?;
                self.literal("public_key")?;
                let public_key = self.nullable(|r| Ok(PublicKey::from_bytes(r.fixed()?)))?;
                self.literal("route_class")?;
                let route_class = self.nullable(|r| match r.decoder.str()? {
                    "public" => Ok(RouteClass::Public),
                    "private" => Ok(RouteClass::Private),
                    "tor" => Ok(RouteClass::Tor),
                    "i2p" => Ok(RouteClass::I2p),
                    "lokinet" => Ok(RouteClass::Lokinet),
                    _ => Err(CodecError::IncompatibleSchema),
                })?;
                self.literal("software_version")?;
                let software_version = self.nullable(Self::label)?;
                DictionaryDescriptor::Observer(ObserverDescriptor {
                    clock,
                    vantage,
                    software,
                    method_id,
                    public_key,
                    route_class,
                    software_version,
                })
            }
            (DictionaryKind::AccessScope, "access-scope") => {
                self.map(3)?;
                self.literal("class")?;
                let class = match self.decoder.str()? {
                    "public" => AccessClass::Public,
                    "protected" => AccessClass::Protected,
                    _ => return Err(CodecError::IncompatibleSchema),
                };
                self.literal("label")?;
                let label = self.nullable(Self::label)?;
                self.literal("visibility_scope_id")?;
                let visibility_scope_id = self.reference()?;
                DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                    class,
                    label,
                    visibility_scope_id,
                })
            }
            (DictionaryKind::Method, "event-verification") => {
                self.map(7)?;
                for (key, value) in [
                    ("input", "nip01-event/1"),
                    ("algorithm", "sha256-bip340"),
                    ("capability", "event-id-and-signature"),
                    ("output_unit", "verification-result"),
                    ("failure_taxonomy", "bigbrotr-nip01-verifier/1"),
                ] {
                    self.literal(key)?;
                    self.literal(value)?;
                }
                self.literal("algorithm_version")?;
                if self.decoder.u16()? != 1 {
                    return Err(CodecError::IncompatibleSchema);
                }
                self.literal("protocol_revision")?;
                self.literal(EVENT_VERIFICATION_NIPS_REVISION)?;
                DictionaryDescriptor::EventVerification
            }
            (DictionaryKind::DatasetLineage, "dataset-lineage") => {
                self.map(4)?;
                self.literal("format")?;
                let format = self.label()?;
                self.literal("format_version")?;
                let format_version = self.decoder.u16()?;
                self.literal("source_dataset_id")?;
                let source_dataset_id = self.nullable(|r| {
                    DatasetId::try_from(r.fixed()?).map_err(|_| CodecError::InvalidValue)
                })?;
                self.literal("source_artifact_digest")?;
                let source_artifact_digest = Digest256::from_bytes(self.fixed()?);
                DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                    format,
                    format_version,
                    source_dataset_id,
                    source_artifact_digest,
                })
            }
            _ => return Err(CodecError::IncompatibleSchema),
        };
        DictionaryEntry::new(reference, descriptor).map_err(CodecError::InvalidDictionary)
    }
    fn fact(&mut self) -> Result<Fact, CodecError> {
        self.array(6)?;
        let id = self.source()?;
        let kind = self.decoder.str()?;
        if !matches!(
            kind,
            "event-payload" | "event-eligibility" | "event-receive" | "provenance" | "dictionary"
        ) || self.decoder.u16()? != 1
        {
            return Err(CodecError::IncompatibleSchema);
        }
        let scope = self.reference()?;
        let recorded_at = ObservedTime::new(self.decoder.i64()?);
        let payload = match kind {
            "dictionary" => FactPayload::Dictionary(self.dictionary()?),
            "event-payload" => {
                self.array(7)?;
                FactPayload::EventPayload(EventPayload {
                    body: self.body()?,
                    event_ordinal: self.reference()?,
                })
            }
            "event-eligibility" => {
                self.array(6)?;
                FactPayload::EventEligibility(EventEligibility {
                    event_id: EventId::from_bytes(self.fixed()?),
                    body_digest: Digest256::from_bytes(self.fixed()?),
                    sig: Signature::from_bytes(self.fixed()?),
                    first_eligible_at: ObservedTime::new(self.decoder.i64()?),
                    origin: self.origin()?,
                })
            }
            "event-receive" => {
                self.array(5)?;
                FactPayload::EventReceive(EventReceive {
                    event_id: EventId::from_bytes(self.fixed()?),
                    received_at: ObservedTime::new(self.decoder.i64()?),
                    observer_id: self.reference()?,
                    clock_quality: self.clock()?,
                    corrects: self.nullable(Self::source)?,
                })
            }
            "provenance" => {
                self.array(8)?;
                FactPayload::Provenance(Provenance {
                    event_id: EventId::from_bytes(self.fixed()?),
                    endpoint_id: self.reference()?,
                    observer_id: self.reference()?,
                    access_scope_id: self.reference()?,
                    received_at: ObservedTime::new(self.decoder.i64()?),
                    clock_quality: self.clock()?,
                    verification_method_id: self.reference()?,
                    corrects: self.nullable(Self::source)?,
                })
            }
            _ => return Err(CodecError::IncompatibleSchema),
        };
        Fact::new(id, scope, recorded_at, payload).map_err(CodecError::InvalidFact)
    }
}
