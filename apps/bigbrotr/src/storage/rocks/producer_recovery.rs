//! Positive original-request resolution under exclusive historical ownership.
use super::{
    ActivatedInitial, AdmissionError, AdmissionLimits, AdmissionOutcome, BootstrapError,
    BootstrapLimits, IngestRecovery, NativeOutcomeCopy, RequestInputError, StoredOutcome,
};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::manifest::IngestManifest;
use bigbrotr_model::{
    Digest256, ProducerRequestId,
    codec::request::{MutationIntent, RequestDecodeLimits},
};

#[derive(Debug, Clone, Copy)]
pub struct ProducerRecoveryLimits {
    pub input: RequestDecodeLimits,
    pub bootstrap: BootstrapLimits,
    pub event: AdmissionLimits,
}
impl ProducerRecoveryLimits {
    /// Shared replay validators retain only one branch at a time. Input decoding
    /// owns a separate grant; native pins stay under the exclusive store grant.
    pub fn required_data_bytes(self) -> Result<u64, AdmissionError> {
        Ok(self
            .bootstrap
            .required_data_bytes()?
            .max(self.event.required_dictionary_replay_data_bytes()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerRecoveryError {
    Native(AdmissionError),
    Input(RequestInputError),
    ProducerOpen,
    /// This first recovery path cannot attest a different retained generation.
    UnsupportedContext,
    /// No positive retained outcome was established. This is never rollback.
    Unresolved,
}
impl From<AdmissionError> for ProducerRecoveryError {
    fn from(value: AdmissionError) -> Self {
        Self::Native(value)
    }
}
impl std::fmt::Display for ProducerRecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer outcome resolution unavailable: {self:?}")
    }
}
impl std::error::Error for ProducerRecoveryError {}

pub struct ResolvedProducerRequest {
    outcome: StoredOutcome,
    input_bytes: u32,
    event: bool,
    native_root: (u64, u64),
    native_sequence: u64,
    lineage_inputs: Option<[u8; 32]>,
}

/// Read interpretation is explicit and cannot replace the owner's singleton or
/// enter a mutation operation. Historical construction belongs to its checked view.
#[derive(Clone, Copy)]
pub(super) struct ReplayContext<'a> {
    pub manifest: &'a IngestManifest,
    pub current_manifest: &'a IngestManifest,
}

/// Original epoch1 interpretation and checked activated epoch2 recovery have
/// different fences. Neither read profile can grant admission or source closure.
pub(super) enum RecoveryContext<'a> {
    Initial(ReplayContext<'a>),
    Activated(&'a ActivatedInitial),
    Lineage {
        lineage: &'a super::RetainedLineage,
        epoch: u64,
        cancelled: &'a std::sync::atomic::AtomicBool,
    },
    Successor {
        owner: &'a super::NativeSuccessorRecovery,
        epoch: u64,
        cancelled: &'a std::sync::atomic::AtomicBool,
    },
}
impl ResolvedProducerRequest {
    pub fn outcome(&self) -> &AdmissionOutcome {
        self.outcome.outcome()
    }
    pub(in crate::storage) fn native_outcome(&self) -> &NativeOutcomeCopy {
        self.outcome.native_outcome()
    }
    pub(in crate::storage) fn input_bytes(&self) -> u32 {
        self.input_bytes
    }
    pub(in crate::storage) fn is_event(&self) -> bool {
        self.event
    }
    pub(super) fn require_lineage(
        &self,
        lineage: &super::RetainedLineage,
    ) -> Result<(), BootstrapError> {
        if self.lineage_inputs != Some(lineage.input_stamp()) {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
    pub(super) fn require_native_context(
        &self,
        recovery: &IngestRecovery,
    ) -> Result<(), BootstrapError> {
        if recovery.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if recovery.store.handoff_root_identity()? != self.native_root
            || recovery.store.db.latest_sequence_number() != self.native_sequence
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
}

impl IngestRecovery {
    /// No commit/staging path exists here. Missing detail remains unresolved,
    /// including a reservation beyond the issued counter. This operation cannot
    /// supply an absence proof, ACK permission or a contiguous recovery prefix.
    #[allow(
        clippy::too_many_arguments,
        reason = "separate retained input and outcome grants"
    )]
    pub(in crate::storage) fn resolve_original_request(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
        self.resolve_original_request_in(
            request,
            digest,
            limits,
            input_permit,
            outcome_permit,
            authority,
            RecoveryContext::Initial(ReplayContext {
                manifest: self.manifest(),
                current_manifest: self.manifest(),
            }),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "explicit historical read context and owned grants"
    )]
    pub(super) fn resolve_original_request_in(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
        profile: RecoveryContext<'_>,
    ) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
        if outcome_permit.request().units().data_bytes() < limits.required_data_bytes()?
            || outcome_permit.request().units().items() == 0
        {
            return Err(AdmissionError::InsufficientReservation.into());
        }
        authority()?;
        if self.store.write_uncertain {
            return Err(AdmissionError::CommitUnknown.into());
        }
        let native_root = self
            .store
            .handoff_root_identity()
            .map_err(AdmissionError::from)?;
        let native_sequence = self.store.db.latest_sequence_number();
        let context = match &profile {
            RecoveryContext::Initial(context) => {
                if context.manifest.key_schema() != 1
                    || request.producer_epoch.get() != 1
                    || context.manifest.identity().owner_epoch.get() != 1
                {
                    return Err(AdmissionError::InvalidInput.into());
                }
                *context
            }
            RecoveryContext::Activated(view) => {
                view.validate(self).map_err(AdmissionError::from)?;
                if request.producer_epoch.get() != 2
                    || request.producer_slot_id != view.producer_slot_id()
                {
                    return Err(AdmissionError::InvalidInput.into());
                }
                ReplayContext {
                    manifest: view.manifest(),
                    current_manifest: view.manifest(),
                }
            }
            RecoveryContext::Lineage {
                lineage,
                epoch,
                cancelled,
            } => {
                lineage
                    .validate(self, cancelled)
                    .map_err(AdmissionError::from)?;
                let entry = lineage.entry(*epoch).ok_or(AdmissionError::InvalidInput)?;
                if request.producer_epoch.get() != *epoch
                    || request.producer_slot_id != lineage.producer_slot_id()
                {
                    return Err(AdmissionError::InvalidInput.into());
                }
                ReplayContext {
                    manifest: entry.manifest(),
                    current_manifest: lineage.native_head().manifest(),
                }
            }
            RecoveryContext::Successor {
                owner,
                epoch,
                cancelled,
            } => {
                owner
                    .validate(self, cancelled)
                    .map_err(AdmissionError::from)?;
                let entry = owner.entry(*epoch).ok_or(AdmissionError::InvalidInput)?;
                if request.producer_epoch.get() != *epoch
                    || request.producer_slot_id != owner.producer_slot_id()
                {
                    return Err(AdmissionError::InvalidInput.into());
                }
                ReplayContext {
                    manifest: entry.manifest(),
                    current_manifest: owner.head().manifest(),
                }
            }
        };
        if self.manifest() != context.current_manifest {
            return Err(AdmissionError::InvalidInput.into());
        }
        self.store
            .require_unretired_request(request, context.manifest)?;
        let state = self
            .store
            .producer_state(request.producer_slot_id, request.producer_epoch)
            .map_err(AdmissionError::from)?
            .ok_or(AdmissionError::StateMismatch)?;
        if !state.closing {
            return Err(ProducerRecoveryError::ProducerOpen);
        }
        if state.retired != 0 || state.issued > super::RETAINED_PRODUCER_REQUEST_LIMIT {
            return Err(AdmissionError::StateMismatch.into());
        }
        if request.request_sequence.get() > state.issued {
            return Err(ProducerRecoveryError::Unresolved);
        }
        let retained = self
            .store
            .read_request_input(request, digest, limits.input, input_permit)
            .map_err(ProducerRecoveryError::Input)?;
        let input_bytes = retained.encoded_bytes();
        let (_, intent, _input_permit) = retained.into_parts();
        let event = matches!(&intent, MutationIntent::Event(_));
        authority()?;
        let (outcome, copy) = match intent {
            MutationIntent::Evidence(ref intent) => {
                let receipt = if request.producer_epoch.get() == 1
                    && context.manifest.identity().owner_epoch.get() == 1
                    && request.request_sequence.get() == 1
                {
                    self.store
                        .replay_bootstrap_in(request, intent, limits.bootstrap, context)
                        .map_err(AdmissionError::from)?
                } else {
                    self.store.replay_dictionary_append_in(
                        request,
                        intent,
                        limits.event,
                        context,
                        match &profile {
                            RecoveryContext::Lineage { lineage, .. } => Some(*lineage),
                            RecoveryContext::Successor { owner, .. } => Some(owner.lineage()),
                            _ => None,
                        },
                    )?
                }
                .ok_or(ProducerRecoveryError::Unresolved)?;
                let copy = NativeOutcomeCopy::receipt(&receipt, context.manifest)
                    .map_err(AdmissionError::from)?;
                (AdmissionOutcome::Durable(receipt), copy)
            }
            MutationIntent::Event(intent) => {
                let scope = intent.visibility_scope_id();
                let origin = intent.origin();
                let observation = intent.observation().copied();
                // This fixed supported profile bounds revalidation; it is not
                // a new current-policy admission decision. Stored rejections
                // remain their original rejection, including their policy epoch.
                let verified =
                    super::admission::verify_event(intent.into_event(), 4 * 1024 * 1024)?;
                let observed = self
                    .store
                    .replay_event_in(
                        request,
                        digest,
                        verified.as_ref().ok().map(|event| {
                            super::receipt_effects::ReplayEventIntent {
                                event: event.event(),
                                scope,
                                origin,
                                observation,
                            }
                        }),
                        None,
                        limits.event,
                        &mut super::admission::ReadBudget::new(limits.event),
                        context,
                    )?
                    .ok_or(ProducerRecoveryError::Unresolved)?;
                if let (
                    RecoveryContext::Lineage { .. } | RecoveryContext::Successor { .. },
                    AdmissionOutcome::Durable(receipt),
                ) = (&profile, &observed.0)
                {
                    let event = verified
                        .as_ref()
                        .map_err(|_| AdmissionError::StateMismatch)?;
                    self.store.require_lineage_event_batch(
                        receipt,
                        super::receipt_effects::ReplayEventIntent {
                            event: event.event(),
                            scope,
                            origin,
                            observation,
                        },
                        limits.event,
                    )?;
                }
                observed
            }
        };
        if let AdmissionOutcome::Durable(receipt) = &outcome {
            let identity = context.manifest.identity();
            let protection = receipt.protection_context();
            if protection.topology_generation != identity.topology_generation
                || protection.recovery_generation != identity.recovery_generation
            {
                return Err(ProducerRecoveryError::UnsupportedContext);
            }
            // Policy belongs to the historical decision, not today's recovery
            // authority. Never rewrite it from the manifest/current PG snapshot.
            if let RecoveryContext::Activated(view) = &profile {
                view.require_receipt_bounds(receipt)?;
            }
        }
        authority()?;
        let resolved = ResolvedProducerRequest {
            outcome: StoredOutcome::new(outcome, copy, outcome_permit),
            input_bytes,
            event,
            native_root,
            native_sequence,
            lineage_inputs: match &profile {
                RecoveryContext::Lineage { lineage, .. } => Some(lineage.input_stamp()),
                RecoveryContext::Successor { owner, .. } => Some(owner.lineage().input_stamp()),
                _ => None,
            },
        };
        resolved
            .require_native_context(self)
            .map_err(AdmissionError::from)?;
        Ok(resolved)
    }
}
