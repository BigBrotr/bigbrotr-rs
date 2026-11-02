//! Exact passive measurement input to AdmitEvidence. This is an input value,
//! never proof of execution, producer authority, allocation or durable admission.
use crate::{DictionaryKind, EvidenceIntentError, Measurement, QualifiedRef, fingerprint};

/// One compound attempt, with its original occurrence/clock attribution. The
/// owner assigns the eventual source record and recorded time after admission.
/// Dictionary-only bootstrap/append remains a separate, nonempty input type.
#[derive(Debug, PartialEq, Eq)]
pub struct MeasurementEvidenceIntent {
    admission_policy_version: u16,
    visibility_scope_id: QualifiedRef,
    measurement: Measurement,
    encoded_intent_bytes: usize,
}
impl MeasurementEvidenceIntent {
    /// The caller funds the already constructed measurement, this inline value,
    /// and any later request encoding for their overlapping lifetimes.
    pub fn new(
        admission_policy_version: u16,
        visibility_scope_id: QualifiedRef,
        measurement: Measurement,
        max_encoded_intent_bytes: usize,
    ) -> Result<Self, EvidenceIntentError> {
        if visibility_scope_id.dictionary_kind != DictionaryKind::VisibilityScope {
            return Err(EvidenceIntentError::WrongVisibilityScope);
        }
        if measurement.context().endpoint_id().dataset_id != visibility_scope_id.dataset_id {
            return Err(EvidenceIntentError::ForeignDataset);
        }
        let mut value = Self {
            admission_policy_version,
            visibility_scope_id,
            measurement,
            encoded_intent_bytes: 0,
        };
        value.encoded_intent_bytes =
            fingerprint::measurement_intent_encoded_len(&value, max_encoded_intent_bytes)?;
        Ok(value)
    }

    pub const fn admission_policy_version(&self) -> u16 {
        self.admission_policy_version
    }
    pub const fn visibility_scope_id(&self) -> QualifiedRef {
        self.visibility_scope_id
    }
    pub fn measurement(&self) -> &Measurement {
        &self.measurement
    }
    pub const fn encoded_intent_bytes(&self) -> usize {
        self.encoded_intent_bytes
    }
    pub fn into_parts(self) -> (u16, QualifiedRef, Measurement) {
        (
            self.admission_policy_version,
            self.visibility_scope_id,
            self.measurement,
        )
    }
}
