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

impl Dispatch<'_> {
    pub(super) async fn observation(
        &mut self,
        scope: &StoredAcquisitionScope,
        source: &StoredRelaySource,
        provenance: bool,
        next_request: &mut u64,
    ) -> Result<Option<StoredObservation>> {
        // Attribution descriptors are small even at the maximum endpoint size.
        // Use a separate bound from event admission, so repeated observations
        // do not retain a full event batch's scratch while receiving a socket.
        let limits = AdmissionLimits {
            max_batch_bytes: 64 * 1024,
            max_dependencies: 8,
            max_outstanding_requests: u64::from(self.writer.limit(self.context)?),
        };
        let visible = scope.scope().visibility();
        let mut prepared = match self.writer.prepare_dictionaries(
            visible,
            limits,
            grant(
                self.budget,
                limits
                    .required_dictionary_preparation_data_bytes()
                    .map_err(error)?,
                0,
            )?,
            &self.cancel.requested,
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
            self.root
                .text("storage.ingest_root")
                .expect("validated path"),
        );
        let lifecycle = self.writer.profile()?;
        let ttl = self.remaining()?;
        let existing = self
            .writer
            .observation(
                self.owner,
                self.context,
                LiveSuccessorObservationRequest {
                    ingest_root: path,
                    bindings,
                    limits: reads,
                    policy_ttl: ttl,
                    cancelled: &self.cancel.requested,
                },
                grant(self.budget, reads.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate_sync(self.root, lifecycle, d, h),
            )
            .await
            .map_err(error)?;
        if let Some(existing) = existing {
            return Ok(Some(existing));
        }
        if *next_request > u64::from(self.writer.limit(self.context)?) {
            // This new descriptor set needs a mutation slot. A full initial
            // window defers it without ending HTTP, renewal or existing reads.
            return Ok(None);
        }
        let ttl = self.remaining()?;
        let appended = self
            .writer
            .append(
                self.owner,
                self.context,
                LiveSuccessorDictionaryAppendRequest {
                    ingest_root: path,
                    request_sequence: RequestSequence::new(*next_request).map_err(error)?,
                    intent: prepared.intent(),
                    recorded_at: recorded()?,
                    limits,
                    policy_ttl: ttl,
                    cancelled: &self.cancel.requested,
                },
                grant(
                    self.budget,
                    limits.required_dictionary_data_bytes().map_err(error)?,
                    limits.required_native_bytes().map_err(error)?,
                )?,
                |d, h| validate_sync(self.root, lifecycle, d, h),
            )
            .await
            .map_err(error)?;
        drop(appended);
        *next_request += 1;
        let ttl = self.remaining()?;
        self.writer
            .observation(
                self.owner,
                self.context,
                LiveSuccessorObservationRequest {
                    ingest_root: path,
                    bindings,
                    limits: reads,
                    policy_ttl: ttl,
                    cancelled: &self.cancel.requested,
                },
                grant(self.budget, reads.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate_sync(self.root, lifecycle, d, h),
            )
            .await
            .map_err(error)?
            .ok_or_else(|| error("committed observation missing after append"))
            .map(Some)
    }
}
