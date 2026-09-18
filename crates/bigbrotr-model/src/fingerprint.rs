//! Closed profile-1 recipes, not a general CBOR serializer. Only named typed
//! fields can reach the codec; no generic values, floats or extension bags.
use crate::{
    AdmissionBatch, ClockQuality, DictionaryDescriptor, DictionaryEntry, Digest256, EventBody,
    EventId, EventIntent, EventOrigin, EvidenceIntent, EvidenceIntentError, ExistingProof, Fact,
    FactPayload, NostrTime, ProducerRequestId, PublicKey, QualifiedRef, SignedEvent,
    SourceRecordId,
};
use minicbor::{
    Encoder,
    encode::{Error, Write},
};
use sha2::{Digest, Sha256};
use std::convert::Infallible;

struct HashWriter(Sha256);
impl Write for HashWriter {
    type Error = Infallible;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0.update(bytes);
        Ok(())
    }
}

struct CountWriter {
    count: usize,
    limit: usize,
}
impl Write for CountWriter {
    type Error = EvidenceIntentError;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.count = self
            .count
            .checked_add(bytes.len())
            .filter(|count| *count <= self.limit)
            .ok_or(EvidenceIntentError::EncodedIntentLimit)?;
        Ok(())
    }
}

fn hash(
    recipe: impl FnOnce(&mut Encoder<HashWriter>) -> Result<(), Error<Infallible>>,
) -> Digest256 {
    let mut encoder = Encoder::new(HashWriter(Sha256::new()));
    // These closed recipes call only primitive encoder methods. Their only
    // error path is Write::Error; the SHA-256 writer is infallible.
    recipe(&mut encoder).expect("closed fingerprint recipe cannot fail its digest writer");
    Digest256::from_bytes(encoder.into_writer().0.finalize().into())
}

struct BodyFields<'a> {
    id: EventId,
    pubkey: PublicKey,
    created_at: NostrTime,
    kind: u16,
    tags: &'a [Vec<String>],
    content: &'a str,
}
impl<'a> From<&'a SignedEvent> for BodyFields<'a> {
    fn from(event: &'a SignedEvent) -> Self {
        Self {
            id: event.id,
            pubkey: event.pubkey,
            created_at: event.created_at,
            kind: event.kind,
            tags: &event.tags,
            content: &event.content,
        }
    }
}
impl<'a> From<&'a EventBody> for BodyFields<'a> {
    fn from(event: &'a EventBody) -> Self {
        Self {
            id: event.event_id,
            pubkey: event.pubkey,
            created_at: event.created_at,
            kind: event.kind,
            tags: &event.tags,
            content: &event.content,
        }
    }
}
fn body_fields<W: Write>(
    event: BodyFields<'_>,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .bytes(event.id.as_bytes())?
        .bytes(event.pubkey.as_bytes())?
        .u64(event.created_at.0)?
        .u16(event.kind)?
        .array(event.tags.len() as u64)?;
    for tag in event.tags {
        encoder.array(tag.len() as u64)?;
        for value in tag {
            encoder.str(value)?;
        }
    }
    encoder.str(event.content)?.ok()
}
fn body<W: Write>(event: &SignedEvent, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    body_value(event.into(), encoder)
}
fn body_value<W: Write>(
    event: BodyFields<'_>,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/body/1\0")
        .map_err(Error::write)?;
    encoder.array(6)?;
    body_fields(event, encoder)
}

fn source<W: Write>(id: SourceRecordId, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    encoder
        .array(4)?
        .bytes(id.dataset_id.as_bytes())?
        .u32(id.shard_id)?
        .u64(id.owner_epoch.get())?
        .u64(id.sequence.get())?
        .ok()
}

fn reference<W: Write>(
    value: QualifiedRef,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .array(4)?
        .bytes(value.dataset_id.as_bytes())?
        .u16(value.dictionary_kind as u16)?
        .bytes(value.allocation_domain.as_bytes())?
        .u64(value.ordinal.get())?
        .ok()
}
fn clock<W: Write>(value: ClockQuality, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    encoder.array(4)?;
    reference(value.clock_id(), encoder)?;
    encoder.u64(value.resolution_us().get())?;
    match value.uncertainty_us() {
        Some(uncertainty) => {
            encoder.u64(uncertainty.get())?;
        }
        None => {
            encoder.null()?;
        }
    }
    encoder.u32(value.anomaly_flags())?.ok()
}
fn origin<W: Write>(value: EventOrigin, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    encoder.str(value.as_str())?;
    match value.import_lineage() {
        Some(lineage) => reference(lineage, encoder),
        None => encoder.null()?.ok(),
    }
}
fn correction<W: Write>(
    value: Option<SourceRecordId>,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    match value {
        Some(id) => source(id, encoder),
        None => encoder.null()?.ok(),
    }
}
fn optional_text<W: Write>(
    value: Option<&str>,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    match value {
        Some(value) => encoder.str(value)?.ok(),
        None => encoder.null()?.ok(),
    }
}

pub(crate) fn dictionary_payload<W: Write>(
    value: &DictionaryEntry,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder.array(5)?;
    reference(value.reference(), encoder)?;
    encoder
        .u16(value.normalization_version())?
        .str(value.descriptor_schema())?
        .u16(value.descriptor_version())?;
    // These exact key sequences are sorted by complete deterministic CBOR bytes.
    // No runtime map or sort can discard duplicates or add unnamed fields.
    match value.descriptor() {
        DictionaryDescriptor::Event(id) => {
            encoder.map(1)?.str("event_id")?.bytes(id.as_bytes())?;
        }
        DictionaryDescriptor::Visibility { class } => {
            encoder.map(1)?.str("class")?.str(class.as_str())?;
        }
        DictionaryDescriptor::Endpoint(value) => {
            encoder.map(1)?.str("url")?.str(value.url())?;
        }
        DictionaryDescriptor::Observer(value) => {
            encoder.map(7)?.str("clock")?;
            optional_text(value.clock.map(|v| v.as_str()), encoder)?;
            encoder.str("vantage")?;
            optional_text(value.vantage.as_ref().map(|v| v.as_str()), encoder)?;
            encoder.str("software")?;
            optional_text(value.software.as_ref().map(|v| v.as_str()), encoder)?;
            encoder.str("method_id")?;
            match value.method_id {
                Some(id) => reference(id, encoder)?,
                None => {
                    encoder.null()?;
                }
            }
            encoder.str("public_key")?;
            match value.public_key {
                Some(key) => {
                    encoder.bytes(key.as_bytes())?;
                }
                None => {
                    encoder.null()?;
                }
            }
            encoder.str("route_class")?;
            optional_text(value.route_class.map(|v| v.as_str()), encoder)?;
            encoder.str("software_version")?;
            optional_text(value.software_version.as_ref().map(|v| v.as_str()), encoder)?;
        }
        DictionaryDescriptor::AccessScope(value) => {
            encoder
                .map(3)?
                .str("class")?
                .str(value.class.as_str())?
                .str("label")?;
            optional_text(value.label.as_ref().map(|v| v.as_str()), encoder)?;
            encoder.str("visibility_scope_id")?;
            reference(value.visibility_scope_id, encoder)?;
        }
        DictionaryDescriptor::EventVerification => {
            encoder
                .map(7)?
                .str("input")?
                .str("nip01-event/1")?
                .str("algorithm")?
                .str("sha256-bip340")?
                .str("capability")?
                .str("event-id-and-signature")?
                .str("output_unit")?
                .str("verification-result")?
                .str("failure_taxonomy")?
                .str("bigbrotr-nip01-verifier/1")?
                .str("algorithm_version")?
                .u16(1)?
                .str("protocol_revision")?
                .str(crate::dictionary::EVENT_VERIFICATION_NIPS_REVISION)?;
        }
        DictionaryDescriptor::DatasetLineage(value) => {
            encoder
                .map(4)?
                .str("format")?
                .str(value.format.as_str())?
                .str("format_version")?
                .u16(value.format_version)?
                .str("source_dataset_id")?;
            match value.source_dataset_id {
                Some(id) => {
                    encoder.bytes(id.as_bytes())?;
                }
                None => {
                    encoder.null()?;
                }
            }
            encoder
                .str("source_artifact_digest")?
                .bytes(value.source_artifact_digest.as_bytes())?;
        }
    }
    Ok(())
}
pub(crate) fn dictionary<W: Write>(
    value: &DictionaryEntry,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/dictionary/1\0")
        .map_err(Error::write)?;
    dictionary_payload(value, encoder)
}
impl DictionaryEntry {
    /// Full qualified mapping integrity, not semantic equality across allocation
    /// domains and not authority to use or publish the descriptor.
    pub fn digest(&self) -> Digest256 {
        hash(|encoder| dictionary(self, encoder))
    }
}

pub(crate) fn fact<W: Write>(
    value: &Fact,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/fact/1\0")
        .map_err(Error::write)?;
    encoder.array(6)?;
    source(value.source_record_id(), encoder)?;
    encoder
        .str(value.record_kind())?
        .u16(value.record_schema_version())?;
    reference(value.visibility_scope_id(), encoder)?;
    encoder.i64(value.recorded_at().get())?;
    match value.payload() {
        FactPayload::Dictionary(value) => dictionary_payload(value, encoder)?,
        FactPayload::EventPayload(value) => {
            encoder.array(7)?;
            body_fields((&value.body).into(), encoder)?;
            reference(value.event_ordinal, encoder)?;
        }
        FactPayload::EventEligibility(value) => {
            encoder
                .array(6)?
                .bytes(value.event_id.as_bytes())?
                .bytes(value.body_digest.as_bytes())?
                .bytes(value.sig.as_bytes())?
                .i64(value.first_eligible_at.get())?;
            origin(value.origin, encoder)?;
        }
        FactPayload::EventReceive(value) => {
            encoder
                .array(5)?
                .bytes(value.event_id.as_bytes())?
                .i64(value.received_at.get())?;
            reference(value.observer_id, encoder)?;
            clock(value.clock_quality, encoder)?;
            correction(value.corrects, encoder)?;
        }
        FactPayload::Provenance(value) => {
            encoder.array(8)?.bytes(value.event_id.as_bytes())?;
            reference(value.endpoint_id, encoder)?;
            reference(value.observer_id, encoder)?;
            reference(value.access_scope_id, encoder)?;
            encoder.i64(value.received_at.get())?;
            clock(value.clock_quality, encoder)?;
            reference(value.verification_method_id, encoder)?;
            correction(value.corrects, encoder)?;
        }
    }
    Ok(())
}

fn event_intent<W: Write>(
    value: &EventIntent,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder.array(6)?.u16(value.admission_policy_version())?;
    reference(value.visibility_scope_id(), encoder)?;
    encoder.array(7)?;
    body_fields(value.event().into(), encoder)?;
    encoder.bytes(value.event().sig.as_bytes())?;
    origin(value.origin(), encoder)?;
    match value.observation() {
        None => {
            encoder.null()?;
        }
        Some(local) => {
            encoder.array(4)?.i64(local.received_at.get())?;
            reference(local.observer_id, encoder)?;
            clock(local.clock_quality, encoder)?;
            match local.witness {
                None => {
                    encoder.null()?;
                }
                Some(witness) => {
                    encoder.array(3)?;
                    reference(witness.endpoint_id, encoder)?;
                    reference(witness.access_scope_id, encoder)?;
                    reference(witness.verification_method_id, encoder)?;
                }
            }
        }
    }
    Ok(())
}

fn evidence_intent<W: Write>(
    value: &EvidenceIntent,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder.array(3)?.u16(value.admission_policy_version())?;
    reference(value.visibility_scope_id(), encoder)?;
    encoder.array(value.dictionaries().len() as u64)?;
    for dictionary in value.dictionaries() {
        encoder.array(3)?.str("dictionary")?.u16(1)?;
        dictionary_payload(dictionary, encoder)?;
    }
    Ok(())
}

pub(crate) fn evidence_intent_encoded_len(
    value: &EvidenceIntent,
    max_encoded_bytes: usize,
) -> Result<usize, EvidenceIntentError> {
    let mut encoder = Encoder::new(CountWriter {
        count: 0,
        limit: max_encoded_bytes,
    });
    evidence_intent(value, &mut encoder).map_err(|_| EvidenceIntentError::EncodedIntentLimit)?;
    Ok(encoder.into_writer().count)
}

fn evidence_request<W: Write>(
    id: ProducerRequestId,
    intent: &EvidenceIntent,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/producer-request/1\0")
        .map_err(Error::write)?;
    encoder
        .array(4)?
        .array(3)?
        .bytes(id.producer_slot_id.as_bytes())?
        .u64(id.producer_epoch.get())?
        .u64(id.request_sequence.get())?
        .str("admit-evidence")?
        .u16(1)?;
    evidence_intent(intent, encoder)
}
fn request<W: Write>(
    id: Option<ProducerRequestId>,
    intent: &EventIntent,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/producer-request/1\0")
        .map_err(Error::write)?;
    encoder.array(4)?.array(3)?;
    match id {
        Some(id) => {
            encoder
                .bytes(id.producer_slot_id.as_bytes())?
                .u64(id.producer_epoch.get())?
                .u64(id.request_sequence.get())?
                .str("admit")?;
        }
        None => {
            // The reserved proof-only identity cannot be constructed as a
            // ProducerRequestId, so it cannot accidentally enter strict Admit.
            encoder
                .bytes(&[0; 16])?
                .u64(0)?
                .u64(0)?
                .str("certify-existing")?;
        }
    }
    encoder.u16(1)?;
    event_intent(intent, encoder)
}

fn admission<W: Write>(
    batch: &AdmissionBatch,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/admission-batch/1\0")
        .map_err(Error::write)?;
    let first = batch.first();
    encoder
        .array(6)?
        .bytes(first.dataset_id.as_bytes())?
        .u32(first.shard_id)?
        .u64(first.owner_epoch.get())?
        .u64(first.sequence.get())?
        .u64(batch.last().sequence.get())?
        .array(batch.records().len() as u64)?;
    for record in batch.records() {
        encoder.array(2)?;
        source(record.source_record_id, encoder)?;
        encoder.bytes(record.payload_digest.as_bytes())?;
    }
    Ok(())
}

impl SignedEvent {
    /// Structural shared-payload fingerprint, excluding signature and envelope.
    /// This does not verify the event; admission must use the protocol verifier.
    pub fn body_digest(&self) -> Digest256 {
        hash(|encoder| body(self, encoder))
    }
}
impl EventBody {
    pub fn body_digest(&self) -> Digest256 {
        hash(|encoder| body_value(self.into(), encoder))
    }
}
impl Fact {
    /// Binds the entire fact envelope and typed payload, excluding this digest.
    pub fn payload_digest(&self) -> Digest256 {
        hash(|encoder| fact(self, encoder))
    }
}
impl EventIntent {
    pub fn request_digest(&self, id: ProducerRequestId) -> Digest256 {
        hash(|encoder| request(Some(id), self, encoder))
    }
    pub fn certification_digest(&self) -> Digest256 {
        hash(|encoder| request(None, self, encoder))
    }
}
impl EvidenceIntent {
    /// Digest of the exact strict mutation request. This is an integrity key,
    /// not proof of producer authority, allocation, policy or durable admission.
    pub fn request_digest(&self, id: ProducerRequestId) -> Digest256 {
        hash(|encoder| evidence_request(id, self, encoder))
    }

    /// Encode the same preimage consumed by `request_digest`. The caller's
    /// full-request limit is checked before fallible output allocation.
    pub fn encode_request_preimage(
        &self,
        id: ProducerRequestId,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, EvidenceIntentError> {
        let mut count = Encoder::new(CountWriter {
            count: 0,
            limit: max_encoded_bytes,
        });
        evidence_request(id, self, &mut count)
            .map_err(|_| EvidenceIntentError::EncodedRequestLimit)?;
        let size = count.into_writer().count;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| EvidenceIntentError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        evidence_request(id, self, &mut encoder)
            .map_err(|_| EvidenceIntentError::EncodingFailure)?;
        if !encoder.into_writer().is_empty() {
            return Err(EvidenceIntentError::EncodingFailure);
        }
        Ok(bytes)
    }
}
impl AdmissionBatch {
    /// Semantic membership fingerprint, not proof of an engine commit.
    pub fn digest(&self) -> Digest256 {
        hash(|encoder| admission(self, encoder))
    }
}

fn existing_proof<W: Write>(
    proof: &ExistingProof,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/existing-proof/1\0")
        .map_err(Error::write)?;
    let context = proof.protection_context();
    encoder
        .array(7)?
        .bytes(context.dataset_id.as_bytes())?
        .u64(context.topology_generation.get())?
        .str(context.protection_class.as_str())?
        .bytes(context.recovery_generation.as_bytes())?
        .u64(context.policy_epoch.get())?
        .bytes(proof.intent_digest().as_bytes())?
        .array(proof.dependencies().records().len() as u64)?;
    for record in proof.dependencies().records() {
        encoder.array(2)?;
        source(record.source_record_id, encoder)?;
        encoder.bytes(record.payload_digest.as_bytes())?;
    }
    Ok(())
}
impl ExistingProof {
    /// Operational integrity key, not a signature or an authorization token.
    pub fn digest(&self) -> Digest256 {
        hash(|encoder| existing_proof(self, encoder))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatasetId, FactRef, NostrTime, OwnerEpoch, Sequence, Signature};

    struct Bytes(Vec<u8>);
    impl Write for Bytes {
        type Error = Infallible;
        fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
            self.0.extend_from_slice(bytes);
            Ok(())
        }
    }
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn existing_proof_bytes_match_independent_vector() {
        use crate::{
            PolicyEpoch, ProtectionClass, ProtectionContext, RecoveryGenerationId,
            SourceDependencies, TopologyGeneration,
        };
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../experiments/identity-contract/golden-vectors.json"
        ))
        .unwrap();
        let vector = |name| {
            vectors["vectors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["name"] == name)
                .unwrap()
        };
        let dataset_id = DatasetId::try_from([1; 16]).unwrap();
        let proof = ExistingProof::new(
            ProtectionContext {
                dataset_id,
                topology_generation: TopologyGeneration::new(1),
                protection_class: ProtectionClass::LocalSynced,
                recovery_generation: RecoveryGenerationId::from_bytes([3; 16]),
                policy_epoch: PolicyEpoch::new(1),
            },
            vector("certification-intent")["sha256"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            SourceDependencies::new(
                dataset_id,
                vec![FactRef {
                    source_record_id: SourceRecordId {
                        dataset_id,
                        shard_id: 0,
                        owner_epoch: OwnerEpoch::new(1).unwrap(),
                        sequence: Sequence::new(1).unwrap(),
                    },
                    payload_digest: vector("fact-eligibility")["sha256"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap(),
                }],
            )
            .unwrap(),
        )
        .unwrap();
        let mut encoder = Encoder::new(Bytes(Vec::new()));
        existing_proof(&proof, &mut encoder).unwrap();
        assert_eq!(
            hex(&encoder.into_writer().0),
            vector("existing-proof")["preimage_hex"]
        );
        assert_eq!(
            proof.digest().to_string(),
            vector("existing-proof")["sha256"]
        );
    }

    #[test]
    fn complete_core_fact_and_request_bytes_match_independent_vectors() {
        use crate::{
            AllocationDomainId, DictionaryKind, DurationUs, EventEligibility, EventPayload,
            EventReceive, LocalObservation, ObservedTime, Ordinal, ProducerEpoch, ProducerSlotId,
            Provenance, RequestSequence, WitnessContext,
        };
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/model/core-fingerprints.json"
        ))
        .unwrap();
        let original: serde_json::Value = serde_json::from_str(include_str!(
            "../../../experiments/identity-contract/golden-vectors.json"
        ))
        .unwrap();
        let reference = |kind| QualifiedRef {
            dataset_id: DatasetId::try_from([1; 16]).unwrap(),
            dictionary_kind: kind,
            allocation_domain: AllocationDomainId::try_from([2; 16]).unwrap(),
            ordinal: Ordinal::new(1).unwrap(),
        };
        let source_id = |sequence| SourceRecordId {
            dataset_id: reference(DictionaryKind::VisibilityScope).dataset_id,
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(sequence).unwrap(),
        };
        let event = SignedEvent {
            id: EventId::from_bytes([0xaa; 32]),
            pubkey: PublicKey::from_bytes([0xbb; 32]),
            created_at: NostrTime(u64::MAX),
            kind: 1,
            tags: vec![
                vec!["x".into()],
                vec!["x".into(), "".into()],
                vec!["x".into(), "".into()],
            ],
            content: "é\n".into(),
            sig: Signature::from_bytes([0xcc; 64]),
        };
        let scope = reference(DictionaryKind::VisibilityScope);
        let observer = reference(DictionaryKind::Observer);
        let clock = ClockQuality::new(observer, DurationUs::new(1), None, 0).unwrap();
        let check = |name: &str, bytes: &[u8], digest: Digest256| {
            let vector = vectors["vectors"]
                .as_array()
                .unwrap()
                .iter()
                .chain(original["vectors"].as_array().unwrap())
                .find(|row| row["name"] == name)
                .unwrap();
            assert_eq!(hex(bytes), vector["preimage_hex"], "{name}");
            assert_eq!(digest.to_string(), vector["sha256"], "{name}");
        };
        let payloads = [
            (
                "event-payload",
                FactPayload::EventPayload(EventPayload {
                    body: EventBody::from(event.clone()),
                    event_ordinal: reference(DictionaryKind::Event),
                }),
            ),
            (
                "event-receive-correction",
                FactPayload::EventReceive(EventReceive {
                    event_id: event.id,
                    received_at: ObservedTime::new(-1),
                    observer_id: observer,
                    clock_quality: clock,
                    corrects: Some(source_id(1)),
                }),
            ),
            (
                "provenance-correction",
                FactPayload::Provenance(Provenance {
                    event_id: event.id,
                    endpoint_id: reference(DictionaryKind::Endpoint),
                    observer_id: observer,
                    access_scope_id: reference(DictionaryKind::AccessScope),
                    received_at: ObservedTime::new(-1),
                    clock_quality: clock,
                    verification_method_id: reference(DictionaryKind::Method),
                    corrects: Some(source_id(1)),
                }),
            ),
            (
                "eligibility-import",
                FactPayload::EventEligibility(EventEligibility {
                    event_id: event.id,
                    body_digest: event.body_digest(),
                    sig: event.sig,
                    first_eligible_at: ObservedTime::new(-1),
                    origin: EventOrigin::Import(reference(DictionaryKind::DatasetLineage)),
                }),
            ),
            (
                "fact-eligibility",
                FactPayload::EventEligibility(EventEligibility {
                    event_id: event.id,
                    body_digest: event.body_digest(),
                    sig: event.sig,
                    first_eligible_at: ObservedTime::new(-1),
                    origin: EventOrigin::Network,
                }),
            ),
        ];
        for (name, payload) in payloads {
            let (id, time) = if name == "fact-eligibility" {
                (source_id(1), -1)
            } else {
                (source_id(2), i64::MIN)
            };
            let value = Fact::new(id, scope, ObservedTime::new(time), payload).unwrap();
            let mut encoder = Encoder::new(Bytes(Vec::new()));
            fact(&value, &mut encoder).unwrap();
            check(name, &encoder.into_writer().0, value.payload_digest());
        }
        let id = ProducerRequestId {
            producer_slot_id: ProducerSlotId::try_from([3; 16]).unwrap(),
            producer_epoch: ProducerEpoch::new(1).unwrap(),
            request_sequence: RequestSequence::new(1).unwrap(),
        };
        let observation = LocalObservation {
            received_at: ObservedTime::new(-1),
            observer_id: observer,
            clock_quality: clock,
            witness: None,
        };
        let intents = [
            (
                "request-import",
                Some(id),
                EventIntent::import(
                    1,
                    scope,
                    event.clone(),
                    reference(DictionaryKind::DatasetLineage),
                )
                .unwrap(),
            ),
            (
                "request-network-provenance",
                Some(id),
                EventIntent::network(
                    1,
                    scope,
                    event.clone(),
                    LocalObservation {
                        witness: Some(WitnessContext {
                            endpoint_id: reference(DictionaryKind::Endpoint),
                            access_scope_id: reference(DictionaryKind::AccessScope),
                            verification_method_id: reference(DictionaryKind::Method),
                        }),
                        ..observation
                    },
                )
                .unwrap(),
            ),
            (
                "producer-request",
                Some(id),
                EventIntent::network(1, scope, event.clone(), observation).unwrap(),
            ),
            (
                "certification-intent",
                None,
                EventIntent::network(1, scope, event, observation).unwrap(),
            ),
        ];
        for (name, id, value) in intents {
            let mut encoder = Encoder::new(Bytes(Vec::new()));
            request(id, &value, &mut encoder).unwrap();
            let digest = match id {
                Some(id) => value.request_digest(id),
                None => value.certification_digest(),
            };
            check(name, &encoder.into_writer().0, digest);
        }
    }

    #[test]
    fn multi_record_membership_and_endpoints_cross_integer_width_boundary() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/model/admission-two-records.json"
        ))
        .unwrap();
        let dataset = fixture["dataset_id"].as_str().unwrap().parse().unwrap();
        let records = fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: dataset,
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(row["sequence"].as_str().unwrap().parse().unwrap())
                        .unwrap(),
                },
                payload_digest: row["payload_digest"].as_str().unwrap().parse().unwrap(),
            })
            .collect();
        let batch = AdmissionBatch::new(records).unwrap();
        let mut out = Encoder::new(Bytes(Vec::new()));
        admission(&batch, &mut out).unwrap();
        assert_eq!(hex(&out.into_writer().0), fixture["preimage_hex"]);
        assert_eq!(batch.digest().to_string(), fixture["sha256"]);
        let truncated = AdmissionBatch::new(vec![batch.records()[0]]).unwrap();
        assert_ne!(truncated.digest(), batch.digest());
    }

    #[test]
    fn exact_body_and_admission_bytes_match_independently_assembled_vectors() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../experiments/identity-contract/golden-vectors.json"
        ))
        .unwrap();
        let row = |name: &str| {
            fixture["vectors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["name"] == name)
                .unwrap()
        };
        let value = &row("body-u64-ordered-tags")["value"];
        let event = SignedEvent {
            id: value[0]["$bytes"].as_str().unwrap().parse().unwrap(),
            pubkey: value[1]["$bytes"].as_str().unwrap().parse().unwrap(),
            created_at: NostrTime(value[2]["$int"].as_str().unwrap().parse().unwrap()),
            kind: value[3].as_u64().unwrap().try_into().unwrap(),
            tags: serde_json::from_value(value[4].clone()).unwrap(),
            content: value[5].as_str().unwrap().into(),
            sig: Signature::from_bytes([0; 64]),
        };
        let mut out = Encoder::new(Bytes(Vec::new()));
        body(&event, &mut out).unwrap();
        assert_eq!(
            hex(&out.into_writer().0),
            row("body-u64-ordered-tags")["preimage_hex"]
        );
        let batch = AdmissionBatch::new(vec![FactRef {
            source_record_id: SourceRecordId {
                dataset_id: DatasetId::try_from([1; 16]).unwrap(),
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
                sequence: Sequence::new(1).unwrap(),
            },
            payload_digest: row("fact-eligibility")["sha256"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        }])
        .unwrap();
        let mut out = Encoder::new(Bytes(Vec::new()));
        admission(&batch, &mut out).unwrap();
        assert_eq!(
            hex(&out.into_writer().0),
            row("admission-batch")["preimage_hex"]
        );
    }
}
