//! Retire only the exact empty initial attempt after positive predecessor replay.
//! The portable handoff and this read context cannot grant admission authority.
use super::{
    BootstrapError, BootstrapLimits, EmptyTargetState, EncodedHandoff, HistoricalInitial,
    IngestManifest, IngestRecovery, InitialHandoffRecord, OwnedPermit, Prepared, ProducerEpoch,
    ProducerSlotId, ResolvedSource, frame,
};
use sha2::{Digest, Sha256};

/// Exact OPEN/zero or CLOSING/zero target, with its predecessor read context.
/// Its borrow keeps the actual native owner alive throughout positive replay.
#[derive(Clone, Copy)]
pub(in crate::storage) struct EmptyInitialAttempt<'a> {
    history: HistoricalInitial<'a>,
}
impl<'a> EmptyInitialAttempt<'a> {
    /// Shared predecessor interpretation for the existing read-only resolver.
    /// Its explicit target discriminator still forbids handoff reconstruction
    /// once closing; there is no conversion to mutation or admission authority.
    pub(in crate::storage) fn historical(&self) -> &HistoricalInitial<'a> {
        &self.history
    }
    pub(in crate::storage) fn is_closing(&self) -> bool {
        self.history.target == EmptyTargetState::Closing
    }

    /// Recompute every original outcome before making a local owned plan. The
    /// caller drops the source borrow before mutable dispatch and retains its
    /// separate metadata grant. No original receipt, input or digest is rewritten.
    pub(in crate::storage) fn reconstruct_retirement(
        &self,
        source: &ResolvedSource<'_>,
    ) -> Result<EmptyInitialRetirement, BootstrapError> {
        self.history.validate_source(source)?;
        let mut digest = Sha256::new();
        digest.update(b"bigbrotr/initial-empty-attempt-closure/1\0");
        digest.update(Sha256::digest(self.history.record.encode().as_bytes()));
        digest.update(Sha256::digest(
            self.historical().target_manifest().encode().as_bytes(),
        ));
        digest.update(self.historical().producer_slot_id().as_bytes());
        digest.update(2_u64.to_be_bytes());
        digest.update(0_u64.to_be_bytes());
        digest.update(0_u64.to_be_bytes());
        Ok(EmptyInitialRetirement {
            record: self.history.record,
            native_sequence: self.history.sequence,
            native_root: self.history.root,
            target: self.history.target,
            closure_digest: digest.finalize().into(),
        })
    }
}

/// Positive predecessor proof and exact empty target, bound to one native root
/// and sequence. This data has no writer conversion or portable constructor.
#[derive(Clone, Debug)]
pub struct EmptyInitialRetirement {
    record: InitialHandoffRecord,
    native_sequence: u64,
    native_root: (u64, u64),
    target: EmptyTargetState,
    closure_digest: [u8; 32],
}
impl EmptyInitialRetirement {
    pub fn previous_manifest(&self) -> &IngestManifest {
        self.record.previous_manifest()
    }
    pub fn target_manifest(&self) -> &IngestManifest {
        self.record.next_manifest()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.record.producer_slot_id()
    }
    /// Closure of the empty attempted source, distinct from the original cut.
    pub fn closure_digest(&self) -> [u8; 32] {
        self.closure_digest
    }
    pub fn handoff(&self) -> EncodedHandoff {
        self.record.encode()
    }
    pub fn initially_closing(&self) -> bool {
        self.target == EmptyTargetState::Closing
    }
}

/// Exact native closing observation retaining its operation grant. PostgreSQL
/// retirement remains a separate checked transition; this cannot supply an ACK.
pub struct ObservedEmptyInitialRetirement {
    retirement: EmptyInitialRetirement,
    _permit: OwnedPermit,
}
impl ObservedEmptyInitialRetirement {
    pub fn retirement(&self) -> &EmptyInitialRetirement {
        &self.retirement
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn empty_initial_attempt(
        &self,
        handoff: &[u8],
    ) -> Result<EmptyInitialAttempt<'_>, BootstrapError> {
        if self.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        let record = InitialHandoffRecord::decode(handoff)?;
        let marker = self
            .store
            .producer_state(
                record.producer_slot_id(),
                ProducerEpoch::new(2).expect("initial empty attempt"),
            )?
            .ok_or(BootstrapError::StateMismatch)?;
        let history = HistoricalInitial {
            recovery: self,
            record,
            sequence: self.store.db.latest_sequence_number(),
            root: self.store.handoff_root_identity()?,
            target: if marker.closing {
                EmptyTargetState::Closing
            } else {
                EmptyTargetState::Open
            },
        };
        // The decoded marker selects one exact state, not an allowance for any
        // closing producer. Validate all zero counters, bytes and target detail.
        history.validate()?;
        Ok(EmptyInitialAttempt { history })
    }

    /// Read-only preflight before connected coordination or native dispatch.
    pub(in crate::storage) fn validate_empty_initial_retirement(
        &self,
        retirement: &EmptyInitialRetirement,
    ) -> Result<(), BootstrapError> {
        self.empty_initial_retirement_state(retirement).map(|_| ())
    }

    fn empty_initial_retirement_state(
        &self,
        retirement: &EmptyInitialRetirement,
    ) -> Result<EmptyTargetState, BootstrapError> {
        if self.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if self.manifest() != retirement.target_manifest()
            || self.store.handoff_root_identity()? != retirement.native_root
        {
            return Err(BootstrapError::StateMismatch);
        }
        let sequence = self.store.db.latest_sequence_number();
        let target = if sequence == retirement.native_sequence {
            retirement.target
        } else if retirement.target == EmptyTargetState::Open
            && retirement.native_sequence.checked_add(1) == Some(sequence)
        {
            EmptyTargetState::Closing
        } else {
            return Err(BootstrapError::StateMismatch);
        };
        self.store
            .require_handoff_state(&retirement.record, Some(target))?;
        Ok(target)
    }

    /// Close this one unactivated empty attempt. FFI is synchronous and cannot
    /// be cancelled by dropping an async caller. After uncertain dispatch or
    /// failed result delivery, keep/join custody and positively reconstruct.
    pub(in crate::storage) fn close_empty_initial_attempt(
        &mut self,
        retirement: &EmptyInitialRetirement,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedEmptyInitialRetirement, BootstrapError> {
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        authority()?;
        if self.empty_initial_retirement_state(retirement)? == EmptyTargetState::Open {
            let mut batch = Prepared::new(BootstrapLimits {
                max_mappings: 1,
                max_batch_bytes: 1024,
            })?;
            let mut closing = [0; 27];
            closing[..2].copy_from_slice(&[2, 0x31]);
            closing[26] = 1;
            batch.put(retirement.record.producer_key(), frame(&[], &[&closing])?)?;
            self.store.commit_prepared(batch, &mut authority)?;
        }
        authority()?;
        Ok(ObservedEmptyInitialRetirement {
            retirement: retirement.clone(),
            _permit: permit,
        })
    }
}
