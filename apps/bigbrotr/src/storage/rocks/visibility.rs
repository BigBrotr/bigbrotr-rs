//! Committed visibility data; neither the label nor this result grants access.
use super::{AdmissionError, CertificationLimits, IngestStore, admission::ReadBudget};
use crate::runtime::OwnedPermit;
use bigbrotr_model::{DictionaryEntry, DictionaryKind, QualifiedRef};

pub struct StoredVisibility {
    entry: DictionaryEntry,
    _permit: OwnedPermit,
}
impl StoredVisibility {
    pub fn entry(&self) -> &DictionaryEntry {
        &self.entry
    }
}

impl IngestStore {
    /// Exclusive owner access gives one committed view. Reuse admission's exact
    /// index, digest and transitive dictionary checks; no identity is allocated.
    pub(in crate::storage) fn read_visibility(
        &mut self,
        reference: QualifiedRef,
        limits: CertificationLimits,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<Option<StoredVisibility>, AdmissionError> {
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        let limits = limits.reads()?;
        authority()?;
        if self.write_uncertain {
            return Err(AdmissionError::CommitUnknown);
        }
        if reference.dataset_id != self.manifest.identity().dataset_id
            || reference.dictionary_kind != DictionaryKind::VisibilityScope
        {
            return Err(AdmissionError::InvalidInput);
        }
        let mut reads = ReadBudget::new(limits);
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(limits.max_dependencies)
            .map_err(|_| AdmissionError::Allocation)?;
        let entry =
            self.lookup_dictionary_dependency(reference, limits, &mut reads, &mut dependencies)?;
        // On absence, Option::map drops its captured permit immediately. Retire
        // scratch first so other budget users cannot race its deallocation.
        drop(dependencies);
        authority()?;
        Ok(entry.map(|entry| StoredVisibility {
            entry,
            _permit: permit,
        }))
    }
}
