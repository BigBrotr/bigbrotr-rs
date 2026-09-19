//! Recovery decoding for exact retained mutation requests; no authority or verification.
//! Reuses the fingerprint recipe and closed fact readers. It never substitutes a
//! new request ID or current attribution for the producer's original bytes.
use super::{CodecError, CompareWriter, CountWriter, FactDecodeLimits, FactReader};
use crate::{
    Digest256, EventIntent, EventOrigin, EvidenceIntent, EvidenceIntentError, EvidenceIntentLimits,
    LocalObservation, ObservedTime, ProducerEpoch, ProducerRequestId, ProducerSlotId,
    RequestSequence, Signature, SignedEvent, WitnessContext, fingerprint,
};
use minicbor::{Decoder, Encoder};
use sha2::{Digest, Sha256};
use std::mem::size_of;

const PREFIX: &[u8] = b"bigbrotr/producer-request/1\0";

#[derive(Debug, PartialEq, Eq)]
// One recovered request is transferred with its byte grant. Keep its fixed
// metadata inline rather than add an infallible heap allocation during decode.
#[allow(clippy::large_enum_variant)]
pub enum MutationIntent {
    Event(EventIntent),
    Evidence(EvidenceIntent),
}
#[derive(Debug, Clone, Copy)]
pub struct RequestDecodeLimits {
    pub values: FactDecodeLimits,
    pub max_dictionaries: usize,
}
impl Default for RequestDecodeLimits {
    fn default() -> Self {
        Self {
            values: FactDecodeLimits::default(),
            max_dictionaries: 256,
        }
    }
}

impl EventIntent {
    /// Exact strict request preimage; count before reserving the output. The
    /// caller retains grants for both original input and this overlapping copy.
    pub fn encode_request_preimage(
        &self,
        id: ProducerRequestId,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, CodecError> {
        let mut count = Encoder::new(CountWriter {
            count: 0,
            limit: max_encoded_bytes,
        });
        fingerprint::request(Some(id), self, &mut count).map_err(|_| CodecError::InputLimit)?;
        let size = count.into_writer().count;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| CodecError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        fingerprint::request(Some(id), self, &mut encoder).map_err(|_| CodecError::Malformed)?;
        if !encoder.into_writer().is_empty() {
            return Err(CodecError::Malformed);
        }
        Ok(bytes)
    }
}

/// Digest/shape/canonical equality only. A recovered signed request must still
/// pass actual cryptographic verification and current policy before dispatch.
/// Proof-only certification has no producer request ID and is refused here.
pub fn decode_request(
    bytes: &[u8],
    expected_digest: Digest256,
    limits: RequestDecodeLimits,
) -> Result<(ProducerRequestId, MutationIntent), CodecError> {
    if bytes.len() > limits.values.max_encoded_bytes {
        return Err(CodecError::InputLimit);
    }
    let input = bytes
        .strip_prefix(PREFIX)
        .ok_or(CodecError::IncompatibleSchema)?;
    if Digest256::from_bytes(Sha256::digest(bytes).into()) != expected_digest {
        return Err(CodecError::DigestMismatch);
    }
    let mut reader = FactReader {
        decoder: Decoder::new(input),
        limits: limits.values,
        allocated: 0,
        tag_nodes: 0,
    };
    reader.array(4)?;
    reader.array(3)?;
    let id = ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from(reader.fixed()?)
            .map_err(|_| CodecError::InvalidValue)?,
        producer_epoch: ProducerEpoch::new(reader.decoder.u64()?)
            .map_err(|_| CodecError::InvalidValue)?,
        request_sequence: RequestSequence::new(reader.decoder.u64()?)
            .map_err(|_| CodecError::InvalidValue)?,
    };
    let operation = reader.decoder.str()?;
    if reader.decoder.u16()? != 1 {
        return Err(CodecError::IncompatibleSchema);
    }
    let intent = match operation {
        "admit" => MutationIntent::Event(reader.event_intent()?),
        "admit-evidence" => {
            MutationIntent::Evidence(reader.evidence_intent(limits.max_dictionaries)?)
        }
        _ => return Err(CodecError::IncompatibleSchema),
    };
    if reader.decoder.position() != input.len() {
        return Err(CodecError::NonCanonical);
    }
    let mut encoder = Encoder::new(CompareWriter(bytes));
    match &intent {
        MutationIntent::Event(intent) => fingerprint::request(Some(id), intent, &mut encoder),
        MutationIntent::Evidence(intent) => fingerprint::evidence_request(id, intent, &mut encoder),
    }
    .map_err(|_| CodecError::NonCanonical)?;
    if !encoder.into_writer().0.is_empty() {
        return Err(CodecError::NonCanonical);
    }
    Ok((id, intent))
}

impl FactReader<'_> {
    fn event_intent(&mut self) -> Result<EventIntent, CodecError> {
        self.array(6)?;
        let policy = self.decoder.u16()?;
        let scope = self.reference()?;
        self.array(7)?;
        let body = self.body()?;
        let event = SignedEvent {
            id: body.event_id,
            pubkey: body.pubkey,
            created_at: body.created_at,
            kind: body.kind,
            tags: body.tags,
            content: body.content,
            sig: Signature::from_bytes(self.fixed()?),
        };
        let origin = self.origin()?;
        let observation = self.nullable(|r| {
            r.array(4)?;
            let received_at = ObservedTime::new(r.decoder.i64()?);
            let observer_id = r.reference()?;
            let clock_quality = r.clock()?;
            let witness = r.nullable(|r| {
                r.array(3)?;
                Ok(WitnessContext {
                    endpoint_id: r.reference()?,
                    access_scope_id: r.reference()?,
                    verification_method_id: r.reference()?,
                })
            })?;
            Ok(LocalObservation {
                received_at,
                observer_id,
                clock_quality,
                witness,
            })
        })?;
        match (origin, observation) {
            (EventOrigin::Network, Some(local)) => {
                EventIntent::network(policy, scope, event, local)
            }
            (EventOrigin::Import(lineage), None) => {
                EventIntent::import(policy, scope, event, lineage)
            }
            _ => return Err(CodecError::InvalidValue),
        }
        .map_err(CodecError::InvalidFact)
    }
    fn evidence_intent(&mut self, max_dictionaries: usize) -> Result<EvidenceIntent, CodecError> {
        self.array(3)?;
        let policy = self.decoder.u16()?;
        let scope = self.reference()?;
        let count = self.decoder.array()?.ok_or(CodecError::Malformed)?;
        let count = usize::try_from(count)
            .ok()
            .filter(|n| *n <= max_dictionaries)
            .ok_or(CodecError::InputLimit)?;
        if count > self.decoder.input().len() - self.decoder.position() {
            return Err(CodecError::Malformed);
        }
        // Constructor uniqueness scratch overlaps the complete decoded entries.
        let scratch = count
            .checked_mul(size_of::<crate::QualifiedRef>())
            .ok_or(CodecError::AllocationLimit)?;
        let storage = count
            .checked_mul(size_of::<crate::DictionaryEntry>())
            .and_then(|n| n.checked_add(scratch))
            .ok_or(CodecError::AllocationLimit)?;
        self.charge(storage)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| CodecError::AllocationFailed)?;
        for _ in 0..count {
            self.array(3)?;
            self.literal("dictionary")?;
            if self.decoder.u16()? != 1 {
                return Err(CodecError::IncompatibleSchema);
            }
            entries.push(self.dictionary()?);
        }
        EvidenceIntent::new(
            policy,
            scope,
            entries,
            EvidenceIntentLimits {
                max_entries: max_dictionaries,
                max_encoded_intent_bytes: self.limits.max_encoded_bytes,
                max_validation_scratch_bytes: scratch,
            },
        )
        .map_err(|e| match e {
            EvidenceIntentError::AllocationFailed => CodecError::AllocationFailed,
            EvidenceIntentError::EntryCountLimit
            | EvidenceIntentError::EncodedIntentLimit
            | EvidenceIntentError::EncodedRequestLimit => CodecError::InputLimit,
            EvidenceIntentError::ValidationScratchLimit => CodecError::AllocationLimit,
            _ => CodecError::InvalidValue,
        })
    }
}
