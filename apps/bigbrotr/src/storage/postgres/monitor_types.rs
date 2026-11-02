//! Fixed dispatch intent. These values name context; only the private token in
//! monitor.rs grants one guarded dispatch, and no value proves execution.
use super::OwnerError;
use crate::acquisition::relay_source::RelaySourceBinding;
use bigbrotr_model::{
    DatasetId, DictionaryKind, DurationUs, MeasurementAttemptId, MeasurementEvidenceIntent,
    Ordinal, QualifiedRef,
    measurement::{MeasurementContext, MeasurementSubject, PassiveReadProfile},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorIntent {
    pub(super) attempt: MeasurementAttemptId,
    pub(super) source: RelaySourceBinding,
    pub(super) context: MeasurementContext,
    pub(super) clock: QualifiedRef,
    pub(super) visibility: QualifiedRef,
    pub(super) profile: PassiveReadProfile,
}
impl MonitorIntent {
    /// The trusted producer separately binds these committed mappings to its
    /// actual endpoint, direct route, observer and no-auth access. Names alone
    /// do not establish dictionary allocation, visibility or socket authority.
    pub fn new(
        attempt: MeasurementAttemptId,
        source: RelaySourceBinding,
        context: MeasurementContext,
        clock: QualifiedRef,
        visibility: QualifiedRef,
        profile: PassiveReadProfile,
    ) -> Result<Self, OwnerError> {
        let value = Self {
            attempt,
            source,
            context,
            clock,
            visibility,
            profile,
        };
        for (reference, kind) in value.references().into_iter().zip(KINDS) {
            reference
                .require(visibility.dataset_id, kind)
                .map_err(|_| OwnerError::InvalidInput)?;
        }
        Ok(value)
    }
    pub fn attempt_id(self) -> MeasurementAttemptId {
        self.attempt
    }
    pub fn source(self) -> RelaySourceBinding {
        self.source
    }
    pub fn context(self) -> MeasurementContext {
        self.context
    }
    pub fn clock_id(self) -> QualifiedRef {
        self.clock
    }
    pub fn visibility(self) -> QualifiedRef {
        self.visibility
    }
    pub fn profile(self) -> PassiveReadProfile {
        self.profile
    }
    pub fn dataset(self) -> DatasetId {
        self.visibility.dataset_id
    }
    pub(super) fn references(self) -> [QualifiedRef; 7] {
        [
            self.context.endpoint_id(),
            self.context.observer_id,
            self.context.route_id,
            self.context.access_scope_id,
            self.context.method_id,
            self.clock,
            self.visibility,
        ]
    }
    /// Operational protocol1: seven full refs, each dataset16/kind2/domain16/u64be8.
    /// This fixed SQL attribution is separate from the scientific CBOR codec.
    pub(in crate::storage::postgres) fn context_bytes(self) -> [u8; 294] {
        context_bytes(self.references())
    }
    pub(super) fn decode(
        attempt: MeasurementAttemptId,
        source: RelaySourceBinding,
        bytes: &[u8],
        authority_us: u64,
    ) -> Result<Self, OwnerError> {
        if bytes.len() != 294 {
            return Err(OwnerError::InvalidMetadata);
        }
        let mut refs = [None; 7];
        for ((target, bytes), kind) in refs.iter_mut().zip(bytes.as_chunks::<42>().0).zip(KINDS) {
            let kind_bytes: [u8; 2] = bytes[16..18]
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)?;
            if u16::from_be_bytes(kind_bytes) != kind as u16 {
                return Err(OwnerError::InvalidMetadata);
            }
            *target = Some(QualifiedRef {
                dataset_id: <[u8; 16]>::try_from(&bytes[..16])
                    .map_err(|_| OwnerError::InvalidMetadata)?
                    .try_into()
                    .map_err(|_| OwnerError::InvalidMetadata)?,
                dictionary_kind: kind,
                allocation_domain: <[u8; 16]>::try_from(&bytes[18..34])
                    .map_err(|_| OwnerError::InvalidMetadata)?
                    .try_into()
                    .map_err(|_| OwnerError::InvalidMetadata)?,
                ordinal: Ordinal::new(u64::from_be_bytes(
                    bytes[34..42]
                        .try_into()
                        .map_err(|_| OwnerError::InvalidMetadata)?,
                ))
                .map_err(|_| OwnerError::InvalidMetadata)?,
            });
        }
        let [endpoint, observer, route, access, method, clock, visibility] =
            refs.map(|r| r.expect("seven fixed slots"));
        Self::new(
            attempt,
            source,
            MeasurementContext {
                subject: MeasurementSubject::RelayEndpoint(endpoint),
                observer_id: observer,
                route_id: route,
                access_scope_id: access,
                method_id: method,
            },
            clock,
            visibility,
            PassiveReadProfile::first(DurationUs::new(authority_us))
                .map_err(|_| OwnerError::InvalidMetadata)?,
        )
    }
}
pub(super) fn context_bytes(references: [QualifiedRef; 7]) -> [u8; 294] {
    let mut bytes = [0; 294];
    for (slot, reference) in bytes.as_chunks_mut::<42>().0.iter_mut().zip(references) {
        slot[..16].copy_from_slice(reference.dataset_id.as_bytes());
        slot[16..18].copy_from_slice(&(reference.dictionary_kind as u16).to_be_bytes());
        slot[18..34].copy_from_slice(reference.allocation_domain.as_bytes());
        slot[34..].copy_from_slice(&reference.ordinal.get().to_be_bytes());
    }
    bytes
}
const KINDS: [DictionaryKind; 7] = [
    DictionaryKind::Endpoint,
    DictionaryKind::Observer,
    DictionaryKind::Route,
    DictionaryKind::AccessScope,
    DictionaryKind::Method,
    DictionaryKind::Observer,
    DictionaryKind::VisibilityScope,
];

/// Immutable historical binding loaded/created under Control. Copying it never
/// grants network work or fresh evidence admission; those recheck current owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorEvidenceBinding {
    pub(super) intent: MonitorIntent,
}
impl MonitorEvidenceBinding {
    pub fn intent(self) -> MonitorIntent {
        self.intent
    }
    pub fn matches(self, evidence: &MeasurementEvidenceIntent) -> bool {
        let value = evidence.measurement();
        value.attempt_id() == self.intent.attempt
            && value.context() == self.intent.context
            && value.profile() == self.intent.profile
            && value.clock_quality().clock_id() == self.intent.clock
            && evidence.visibility_scope_id() == self.intent.visibility
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::{ClockQuality, Measurement, measurement::MeasurementState};
    use std::num::NonZeroU64;
    fn reference(kind: DictionaryKind, n: u64) -> QualifiedRef {
        QualifiedRef {
            dataset_id: [1; 16].try_into().unwrap(),
            dictionary_kind: kind,
            allocation_domain: [9; 16].try_into().unwrap(),
            ordinal: Ordinal::new(n).unwrap(),
        }
    }
    fn intent() -> MonitorIntent {
        MonitorIntent::new(
            [7; 16].try_into().unwrap(),
            RelaySourceBinding {
                endpoint_id: [2; 16].try_into().unwrap(),
                access_scope_id: [3; 16].try_into().unwrap(),
                enrollment_generation: NonZeroU64::new(11).unwrap(),
                route_id: [4; 16].try_into().unwrap(),
            },
            MeasurementContext {
                subject: MeasurementSubject::RelayEndpoint(reference(DictionaryKind::Endpoint, 1)),
                observer_id: reference(DictionaryKind::Observer, 2),
                route_id: reference(DictionaryKind::Route, 3),
                access_scope_id: reference(DictionaryKind::AccessScope, 4),
                method_id: reference(DictionaryKind::Method, 5),
            },
            reference(DictionaryKind::Observer, 6),
            reference(DictionaryKind::VisibilityScope, 7),
            PassiveReadProfile::first(DurationUs::new(5_000_000)).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn operational_context_has_literal_full_namespaces_and_refuses_substitution() {
        let value = intent();
        let bytes = value.context_bytes();
        // Independently specified fixed protocol layout: seventh reference is
        // visibility, not a method/clock shortcut or operational RouteId.
        assert_eq!(&bytes[252..268], &[1; 16]);
        assert_eq!(&bytes[268..270], &[0, 4]);
        assert_eq!(&bytes[270..286], &[9; 16]);
        assert_eq!(&bytes[286..294], &[0, 0, 0, 0, 0, 0, 0, 7]);
        assert_eq!(
            MonitorIntent::decode(value.attempt, value.source, &bytes, 5_000_000).unwrap(),
            value
        );
        for offset in [0, 16, 17, 126, 142, 143, 252, 268, 269] {
            let mut wrong = bytes;
            wrong[offset] ^= 0x80;
            assert!(
                MonitorIntent::decode(value.attempt, value.source, &wrong, 5_000_000).is_err(),
                "offset {offset}"
            );
        }
        assert!(
            MonitorIntent::decode(value.attempt, value.source, &bytes[..293], 5_000_000).is_err()
        );
        for ceiling in [0, 5_000_001, u64::MAX] {
            assert!(MonitorIntent::decode(value.attempt, value.source, &bytes, ceiling).is_err());
        }
    }
    #[test]
    fn evidence_keeps_clock_identity_but_allows_occurrence_quality_improvement() {
        let value = intent();
        let binding = MonitorEvidenceBinding { intent: value };
        for (resolution, uncertainty, flags) in [(1000, None, 1), (1, Some(DurationUs::new(2)), 0)]
        {
            let measurement = Measurement::new(
                value.attempt,
                value.context,
                value.profile,
                ClockQuality::new(value.clock, DurationUs::new(resolution), uncertainty, flags)
                    .unwrap(),
                MeasurementState::ExecutionUnknown,
            )
            .unwrap();
            let evidence =
                MeasurementEvidenceIntent::new(1, value.visibility, measurement, 8192).unwrap();
            assert!(binding.matches(&evidence));
            let mut changed = value;
            changed.clock = reference(DictionaryKind::Observer, 99);
            assert!(!MonitorEvidenceBinding { intent: changed }.matches(&evidence));
            changed = value;
            changed.profile = PassiveReadProfile::first(DurationUs::new(4_000_000)).unwrap();
            assert!(!MonitorEvidenceBinding { intent: changed }.matches(&evidence));
            changed = value;
            changed.visibility = reference(DictionaryKind::VisibilityScope, 99);
            assert!(!MonitorEvidenceBinding { intent: changed }.matches(&evidence));
            changed = value;
            changed.context.route_id = reference(DictionaryKind::Route, 99);
            assert!(!MonitorEvidenceBinding { intent: changed }.matches(&evidence));
        }
    }
}
