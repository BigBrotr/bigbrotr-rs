//! Dictionary-only AdmitEvidence intent for bounded native bootstrap.
//! Construction proves shape and namespace consistency, never allocation,
//! policy, producer authority, persistence or admission.
use crate::{DictionaryEntry, DictionaryKind, QualifiedRef, fingerprint};
use std::{fmt, mem::size_of};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceIntentError {
    EmptyEntries,
    EntryCountLimit,
    EncodedIntentLimit,
    EncodedRequestLimit,
    ValidationScratchLimit,
    AllocationFailed,
    WrongVisibilityScope,
    ForeignDataset,
    DuplicateDefinition,
    EncodingFailure,
}
impl fmt::Display for EvidenceIntentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyEntries => "evidence intent requires at least one dictionary mapping",
            Self::EntryCountLimit => "evidence intent exceeds the caller's entry-count limit",
            Self::EncodedIntentLimit => {
                "evidence intent exceeds the caller's encoded-intent byte limit"
            }
            Self::EncodedRequestLimit => {
                "evidence request exceeds the caller's encoded-request byte limit"
            }
            Self::ValidationScratchLimit => {
                "evidence intent exceeds the caller's validation-scratch byte limit"
            }
            Self::AllocationFailed => "unable to reserve bounded evidence-intent storage",
            Self::WrongVisibilityScope => {
                "evidence intent scope is not a visibility dictionary reference"
            }
            Self::ForeignDataset => {
                "evidence intent scope and dictionary mappings require one dataset"
            }
            Self::DuplicateDefinition => {
                "evidence intent defines the same qualified mapping more than once"
            }
            Self::EncodingFailure => "closed evidence-intent encoding failed",
        })
    }
}
impl std::error::Error for EvidenceIntentError {}

/// Per-intent local limits supplied by the caller. The validation scratch charge
/// is requested `Vec<QualifiedRef>` element storage, excluding allocator overhead;
/// none of these fields is a process RSS or shared-runtime grant. The caller must
/// reserve the original descriptors, this temporary scratch and later encoded
/// output against its shared grant for their actual overlapping lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceIntentLimits {
    pub max_entries: usize,
    pub max_encoded_intent_bytes: usize,
    pub max_validation_scratch_bytes: usize,
}

/// A caller-ordered dictionary-only AdmitEvidence intent. The storage owner must
/// still validate prepared allocations, current policy, producer authority,
/// retained dependencies and the atomic durable outcome.
#[derive(Debug, PartialEq, Eq)]
pub struct EvidenceIntent {
    admission_policy_version: u16,
    visibility_scope_id: QualifiedRef,
    dictionaries: Vec<DictionaryEntry>,
    encoded_intent_bytes: usize,
}
impl EvidenceIntent {
    pub fn new(
        admission_policy_version: u16,
        visibility_scope_id: QualifiedRef,
        dictionaries: Vec<DictionaryEntry>,
        limits: EvidenceIntentLimits,
    ) -> Result<Self, EvidenceIntentError> {
        if dictionaries.is_empty() {
            return Err(EvidenceIntentError::EmptyEntries);
        }
        if dictionaries.len() > limits.max_entries {
            return Err(EvidenceIntentError::EntryCountLimit);
        }
        if visibility_scope_id.dictionary_kind != DictionaryKind::VisibilityScope {
            return Err(EvidenceIntentError::WrongVisibilityScope);
        }
        if dictionaries
            .iter()
            .any(|entry| entry.reference().dataset_id != visibility_scope_id.dataset_id)
        {
            return Err(EvidenceIntentError::ForeignDataset);
        }

        dictionaries
            .len()
            .checked_mul(size_of::<QualifiedRef>())
            .filter(|bytes| *bytes <= limits.max_validation_scratch_bytes)
            .ok_or(EvidenceIntentError::ValidationScratchLimit)?;
        let mut references = Vec::new();
        references
            .try_reserve_exact(dictionaries.len())
            .map_err(|_| EvidenceIntentError::AllocationFailed)?;
        references.extend(dictionaries.iter().map(DictionaryEntry::reference));
        references.sort_unstable();
        if references.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(EvidenceIntentError::DuplicateDefinition);
        }
        drop(references);

        let mut value = Self {
            admission_policy_version,
            visibility_scope_id,
            dictionaries,
            encoded_intent_bytes: 0,
        };
        value.encoded_intent_bytes =
            fingerprint::evidence_intent_encoded_len(&value, limits.max_encoded_intent_bytes)?;
        Ok(value)
    }

    pub const fn admission_policy_version(&self) -> u16 {
        self.admission_policy_version
    }
    pub const fn visibility_scope_id(&self) -> QualifiedRef {
        self.visibility_scope_id
    }
    pub fn dictionaries(&self) -> &[DictionaryEntry] {
        &self.dictionaries
    }
    pub const fn encoded_intent_bytes(&self) -> usize {
        self.encoded_intent_bytes
    }
    pub fn into_parts(self) -> (u16, QualifiedRef, Vec<DictionaryEntry>) {
        (
            self.admission_policy_version,
            self.visibility_scope_id,
            self.dictionaries,
        )
    }
}
