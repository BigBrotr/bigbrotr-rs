//! Sequential original-request replay shared by explicit recovery and succession.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::ProducerResolveRequest,
    rocks::store::{RecoveryAcknowledgementResolution, ResolvedProducerRequest, SourceResolution},
};
use bigbrotr_model::{ProducerEpoch, ProducerRequestId, RequestSequence};

#[derive(Clone, Copy)]
pub(in crate::interfaces::serve::initial_ingest) enum Mode {
    Resolve,
    InspectRetirement,
}

/// The same positive replay feeds either source closure or ACK-only completion.
/// No counter supplied by the caller can replace either opaque accumulator.
pub(in crate::interfaces::serve::initial_ingest) enum Prefix<'a, 'native> {
    Source(&'a mut SourceResolution<'native>),
    Acknowledgement(&'a mut RecoveryAcknowledgementResolution<'native>),
}
impl Prefix<'_, '_> {
    fn record(&mut self, original: &ResolvedProducerRequest) -> Result<()> {
        match self {
            Self::Source(prefix) => prefix.record(original),
            Self::Acknowledgement(prefix) => prefix.record(original),
        }
        .map_err(|e| (4, e.to_string()))
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::interfaces::serve::initial_ingest) async fn originals(
    root: &Resolved,
    budget: &ResourceBudget,
    progress: &watch::Sender<Progress>,
    cancellation: &Cancellation,
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    spec: &InitialRegistration,
    reserved: u64,
    mut prefix: Option<Prefix<'_, '_>>,
    mode: Mode,
    mut validate: impl FnMut(&str, [u8; 32]) -> std::result::Result<(), OwnerError>,
) -> Result<u64> {
    let timeout = duration(root, "renewal_timeout");
    let path = Path::new(required(root, "storage.ingest_root")?);
    let mut resolved = 0_u64;
    for sequence in 1..=reserved {
        phase(progress, timeout)?;
        bounded(timeout, cancellation, async {
            owner.refresh(&mut validate).await.map_err(owner_error)
        })
        .await?;
        let (input, outcome) = replay_grants(budget)?;
        phase(progress, timeout)?;
        let result = bounded(timeout, cancellation, async {
            let request = ProducerResolveRequest {
                ingest_root: path,
                registration: spec,
                request: ProducerRequestId {
                    producer_slot_id: spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).expect("initial producer"),
                    request_sequence: RequestSequence::new(sequence).expect("nonzero sequence"),
                },
                limits: limits(),
                policy_ttl: duration(root, "authorization_ttl"),
                cancelled: &cancellation.requested,
            };
            let result = match mode {
                Mode::InspectRetirement => {
                    owner
                        .inspect_initial_retirement_request(
                            recovery,
                            request,
                            input,
                            outcome,
                            &mut validate,
                        )
                        .await
                }
                Mode::Resolve => {
                    owner
                        .resolve_initial_producer_request(
                            recovery,
                            request,
                            input,
                            outcome,
                            &mut validate,
                        )
                        .await
                }
            };
            result.map_err(|e| (4, e.to_string()))
        })
        .await?;
        if let Some(prefix) = &mut prefix {
            prefix.record(&result)?;
        }
        drop(result);
        resolved += 1;
    }
    Ok(resolved)
}
