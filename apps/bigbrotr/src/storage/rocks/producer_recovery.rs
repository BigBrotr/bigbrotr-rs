//! Positive original-request resolution under exclusive historical ownership.
use super::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, BootstrapLimits, IngestRecovery,
    NativeOutcomeCopy, RequestInputError, StoredOutcome,
};
use crate::runtime::OwnedPermit;
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
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
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
        if request.producer_epoch.get() != 1 || self.manifest().identity().owner_epoch.get() != 1 {
            return Err(AdmissionError::InvalidInput.into());
        }
        let state = self
            .store
            .producer_state(request.producer_slot_id, request.producer_epoch)
            .map_err(AdmissionError::from)?
            .ok_or(AdmissionError::StateMismatch)?;
        if !state.closing {
            return Err(ProducerRecoveryError::ProducerOpen);
        }
        if state.acknowledged != 0 || state.retired != 0 {
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
        authority()?;
        let (outcome, copy) = match intent {
            MutationIntent::Evidence(ref intent) => {
                let receipt = if request.request_sequence.get() == 1 {
                    self.store
                        .replay_bootstrap(request, intent, limits.bootstrap)
                        .map_err(AdmissionError::from)?
                } else {
                    self.store
                        .replay_dictionary_append(request, intent, limits.event)?
                }
                .ok_or(ProducerRecoveryError::Unresolved)?;
                let copy = NativeOutcomeCopy::receipt(&receipt, self.manifest())
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
                self.store
                    .replay_event(
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
                    )?
                    .ok_or(ProducerRecoveryError::Unresolved)?
            }
        };
        if let AdmissionOutcome::Durable(receipt) = &outcome {
            let context = receipt.protection_context();
            let identity = self.manifest().identity();
            if context.topology_generation != identity.topology_generation
                || context.recovery_generation != identity.recovery_generation
            {
                return Err(ProducerRecoveryError::UnsupportedContext);
            }
            // Policy belongs to the historical decision, not today's recovery
            // authority. Never rewrite it from the manifest/current PG snapshot.
        }
        authority()?;
        Ok(ResolvedProducerRequest {
            outcome: StoredOutcome::new(outcome, copy, outcome_permit),
            input_bytes,
        })
    }
}
