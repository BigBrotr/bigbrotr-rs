//! Intern actual attribution once, then reuse its committed native mappings.
use super::*;
use bigbrotr::{
    acquisition::observation::ObservationBindings,
    storage::{
        postgres::control_owner::{
            LiveSuccessorDictionaryAppendRequest, LiveSuccessorObservationRequest,
        },
        rocks::store::AdmissionError,
    },
};
use bigbrotr_model::{
    AccessClass, AccessScopeDescriptor, ClockSource, DictionaryDescriptor, EndpointDescriptor,
    ObserverDescriptor, WitnessContext,
};

impl Sync {
    pub(super) async fn observation(
        &mut self,
        work: &mut Dispatch<'_, '_>,
        scope: &StoredAcquisitionScope,
        source: &StoredRelaySource,
        provenance: bool,
    ) -> Result<Option<StoredObservation>> {
        // Attribution descriptors are small even at the maximum endpoint size.
        // Use a separate bound from event admission, so repeated observations
        // do not retain a full event batch's scratch while receiving a socket.
        let limits = AdmissionLimits {
            max_batch_bytes: 64 * 1024,
            max_dependencies: 8,
            max_outstanding_requests: u64::from(work.writer.limit(work.context)?),
        };
        let visible = scope.scope().visibility();
        let prepared = work.trace.run(
            Stage::ObservationPreparation,
            None,
            None,
            || -> Result<_> {
                let mut prepared = match work.writer.prepare_dictionaries(
                    visible,
                    limits,
                    grant(
                        work.budget,
                        limits
                            .required_dictionary_preparation_data_bytes()
                            .map_err(error)?,
                        0,
                    )?,
                    &work.cancel.requested,
                ) {
                    Ok(prepared) => prepared,
                    // Some valid descriptors have a different fact envelope (the
                    // original private2 bootstrap is in public1). This initial
                    // preparation cannot use their closure. Yield without changing
                    // it; corruption, uncertain writes and resource errors stay fatal.
                    Err(AdmissionError::InvalidInput) => return Ok(None),
                    Err(e) => return Err(error(e)),
                };
                let observer = prepared
                    .intern(DictionaryDescriptor::Observer(ObserverDescriptor {
                        clock: Some(ClockSource::SystemUtc),
                        vantage: None,
                        software: None,
                        method_id: None,
                        public_key: None,
                        route_class: None,
                        software_version: None,
                    }))
                    .map_err(error)?;
                let witness = if provenance {
                    Some(WitnessContext {
                        endpoint_id: prepared
                            .intern(DictionaryDescriptor::Endpoint(
                                EndpointDescriptor::new(source.endpoint().as_str().into())
                                    .map_err(error)?,
                            ))
                            .map_err(error)?,
                        access_scope_id: prepared
                            .intern(DictionaryDescriptor::AccessScope(AccessScopeDescriptor {
                                class: AccessClass::Public,
                                label: None,
                                visibility_scope_id: visible,
                            }))
                            .map_err(error)?,
                        verification_method_id: prepared
                            .intern(DictionaryDescriptor::EventVerification)
                            .map_err(error)?,
                    })
                } else {
                    None
                };
                let prepared = prepared.finish().map_err(error)?;
                Ok(Some((prepared, observer, witness)))
            },
        )?;
        let Some((prepared, observer, witness)) = prepared else {
            return Ok(None);
        };
        let observer = prepared
            .intent()
            .dictionaries()
            .iter()
            .find(|entry| entry.reference() == observer)
            .expect("interned observer");
        let bindings = ObservationBindings {
            visibility: visible,
            observer,
            clock: observer,
            witness,
        };
        let reads = CertificationLimits {
            max_fact_bytes: 64 * 1024,
            max_dependencies: 8,
        };
        let path = Path::new(
            work.root
                .text("storage.ingest_root")
                .expect("validated path"),
        );
        let lifecycle = work.writer.profile()?;
        if !work.safe_point(self, false).await? {
            return Ok(None);
        }
        let ttl = work.remaining()?;
        let existing = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::ObservationLookup,
            None,
            None,
            work.writer.observation(
                work.commands.owner,
                work.context,
                LiveSuccessorObservationRequest {
                    ingest_root: path,
                    bindings,
                    limits: reads,
                    policy_ttl: ttl,
                    cancelled: &work.cancel.requested,
                },
                grant(work.budget, reads.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate_sync(work.root, lifecycle, d, h),
            ),
        )
        .await?
        .map_err(error)?;
        if let Some(existing) = existing {
            return Ok(Some(existing));
        }
        if !work.writer.can_reserve(work.context, self.next_request)? {
            // This new descriptor set needs a mutation slot. A full initial
            // window defers it without ending HTTP, renewal or existing reads.
            return Ok(None);
        }
        let checkpoint = ProgressGrant::new(grant(
            work.budget,
            ProgressGrant::REQUIRED_DATA_BYTES,
            ProgressGrant::REQUIRED_NATIVE_BYTES,
        )?)
        .map_err(error)?;
        if !work.safe_point(self, false).await? {
            return Ok(None);
        }
        let ttl = work.remaining()?;
        let appended = commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::ObservationAppend,
            None,
            Some(self.next_request),
            work.writer.append(
                work.commands.owner,
                work.context,
                LiveSuccessorDictionaryAppendRequest {
                    ingest_root: path,
                    request_sequence: RequestSequence::new(self.next_request).map_err(error)?,
                    intent: prepared.intent(),
                    recorded_at: recorded()?,
                    limits,
                    policy_ttl: ttl,
                    cancelled: &work.cancel.requested,
                },
                grant(
                    work.budget,
                    limits.required_dictionary_data_bytes().map_err(error)?,
                    limits.required_native_bytes().map_err(error)?,
                )?,
                &checkpoint,
                |d, h| validate_sync(work.root, lifecycle, d, h),
            ),
        )
        .await?
        .map_err(error)?;
        drop(appended);
        self.next_request += 1;
        if !work.safe_point(self, false).await? {
            return Ok(None);
        }
        let ttl = work.remaining()?;
        commands::sync_operation(
            work.trace,
            work.deadline,
            &work.cancel.requested,
            Stage::ObservationRelookup,
            None,
            None,
            work.writer.observation(
                work.commands.owner,
                work.context,
                LiveSuccessorObservationRequest {
                    ingest_root: path,
                    bindings,
                    limits: reads,
                    policy_ttl: ttl,
                    cancelled: &work.cancel.requested,
                },
                grant(work.budget, reads.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate_sync(work.root, lifecycle, d, h),
            ),
        )
        .await?
        .map_err(error)?
        .ok_or_else(|| error("committed observation missing after append"))
        .map(Some)
    }
}
