//! Read-only preparation of fixed dictionary references for the initial writer.
use super::{AdmissionError, AdmissionLimits, IngestStore};
use crate::runtime::OwnedPermit;
use bigbrotr_model::{DictionaryDescriptor, EvidenceIntent, QualifiedRef};
use std::sync::atomic::AtomicBool;

pub struct DictionaryPreparation<'a> {
    _store: &'a IngestStore,
    _cancelled: &'a AtomicBool,
}

pub struct PreparedDictionaries {
    intent: EvidenceIntent,
    _permit: OwnedPermit,
}
impl PreparedDictionaries {
    pub fn intent(&self) -> &EvidenceIntent {
        &self.intent
    }
}
impl IngestStore {
    pub fn prepare_dictionaries<'a>(
        &'a self,
        _visibility: QualifiedRef,
        _limits: AdmissionLimits,
        _permit: OwnedPermit,
        _cancelled: &'a AtomicBool,
    ) -> Result<DictionaryPreparation<'a>, AdmissionError> {
        Err(AdmissionError::InvalidInput)
    }
}
impl DictionaryPreparation<'_> {
    pub fn intern(
        &mut self,
        _descriptor: DictionaryDescriptor,
    ) -> Result<QualifiedRef, AdmissionError> {
        Err(AdmissionError::InvalidInput)
    }
    pub fn finish(self) -> Result<PreparedDictionaries, AdmissionError> {
        Err(AdmissionError::InvalidInput)
    }
}
