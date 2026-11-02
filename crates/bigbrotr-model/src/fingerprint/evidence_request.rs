//! Closed AdmitEvidence alternatives share one request envelope and byte budget.
use super::{dictionary_payload, hash, measurement_payload, reference};
use crate::{
    Digest256, EvidenceIntent, EvidenceIntentError, MeasurementEvidenceIntent, ProducerRequestId,
};
use minicbor::{
    Encoder,
    encode::{Error, Write},
};

enum EvidenceRequest<'a> {
    Dictionaries(&'a EvidenceIntent),
    Measurement(&'a MeasurementEvidenceIntent),
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
impl EvidenceRequest<'_> {
    fn intent<W: Write>(&self, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
        match self {
            Self::Dictionaries(value) => {
                encoder.array(3)?.u16(value.admission_policy_version())?;
                reference(value.visibility_scope_id(), encoder)?;
                encoder.array(value.dictionaries().len() as u64)?;
                for dictionary in value.dictionaries() {
                    encoder.array(3)?.str("dictionary")?.u16(1)?;
                    dictionary_payload(dictionary, encoder)?;
                }
            }
            Self::Measurement(value) => {
                encoder.array(4)?.u16(value.admission_policy_version())?;
                reference(value.visibility_scope_id(), encoder)?;
                encoder.str("measurement/1")?;
                measurement_payload(value.measurement(), encoder)?;
            }
        }
        Ok(())
    }
    fn encode<W: Write>(
        &self,
        id: ProducerRequestId,
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
        self.intent(encoder)
    }
    fn intent_len(&self, limit: usize) -> Result<usize, EvidenceIntentError> {
        let mut encoder = Encoder::new(CountWriter { count: 0, limit });
        self.intent(&mut encoder)
            .map_err(|_| EvidenceIntentError::EncodedIntentLimit)?;
        Ok(encoder.into_writer().count)
    }
    fn request_len(
        &self,
        id: ProducerRequestId,
        limit: usize,
    ) -> Result<usize, EvidenceIntentError> {
        let mut encoder = Encoder::new(CountWriter { count: 0, limit });
        self.encode(id, &mut encoder)
            .map_err(|_| EvidenceIntentError::EncodedRequestLimit)?;
        Ok(encoder.into_writer().count)
    }
    fn preimage(
        &self,
        id: ProducerRequestId,
        limit: usize,
    ) -> Result<Vec<u8>, EvidenceIntentError> {
        let size = self.request_len(id, limit)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| EvidenceIntentError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        self.encode(id, &mut encoder)
            .map_err(|_| EvidenceIntentError::EncodingFailure)?;
        if !encoder.into_writer().is_empty() {
            return Err(EvidenceIntentError::EncodingFailure);
        }
        Ok(bytes)
    }
}

pub(crate) fn evidence_intent_encoded_len(
    value: &EvidenceIntent,
    limit: usize,
) -> Result<usize, EvidenceIntentError> {
    EvidenceRequest::Dictionaries(value).intent_len(limit)
}
pub(crate) fn measurement_intent_encoded_len(
    value: &MeasurementEvidenceIntent,
    limit: usize,
) -> Result<usize, EvidenceIntentError> {
    EvidenceRequest::Measurement(value).intent_len(limit)
}
pub(crate) fn evidence_request<W: Write>(
    id: ProducerRequestId,
    value: &EvidenceIntent,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    EvidenceRequest::Dictionaries(value).encode(id, encoder)
}
pub(crate) fn measurement_request<W: Write>(
    id: ProducerRequestId,
    value: &MeasurementEvidenceIntent,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    EvidenceRequest::Measurement(value).encode(id, encoder)
}

impl EvidenceIntent {
    /// Exact input integrity; never producer authority or durable admission.
    pub fn request_digest(&self, id: ProducerRequestId) -> Digest256 {
        hash(|encoder| evidence_request(id, self, encoder))
    }
    /// Count before allocating the full request preimage.
    pub fn request_preimage_len(
        &self,
        id: ProducerRequestId,
        max_encoded_bytes: usize,
    ) -> Result<usize, EvidenceIntentError> {
        EvidenceRequest::Dictionaries(self).request_len(id, max_encoded_bytes)
    }
    /// Caller retains grants for original input and this overlapping copy.
    pub fn encode_request_preimage(
        &self,
        id: ProducerRequestId,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, EvidenceIntentError> {
        EvidenceRequest::Dictionaries(self).preimage(id, max_encoded_bytes)
    }
}
impl MeasurementEvidenceIntent {
    /// Binds attempt identity, attribution and every observed payload field.
    pub fn request_digest(&self, id: ProducerRequestId) -> Digest256 {
        hash(|encoder| measurement_request(id, self, encoder))
    }
    pub fn request_preimage_len(
        &self,
        id: ProducerRequestId,
        max_encoded_bytes: usize,
    ) -> Result<usize, EvidenceIntentError> {
        EvidenceRequest::Measurement(self).request_len(id, max_encoded_bytes)
    }
    pub fn encode_request_preimage(
        &self,
        id: ProducerRequestId,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, EvidenceIntentError> {
        EvidenceRequest::Measurement(self).preimage(id, max_encoded_bytes)
    }
}
