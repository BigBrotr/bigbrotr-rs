//! Intern actual attribution once, then reuse its committed native mappings.
use super::*;
use bigbrotr::{
    acquisition::observation::ObservationBindings,
    storage::postgres::control_owner::{InitialDictionaryAppendRequest, InitialObservationRequest},
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
    ) -> Result<StoredObservation> {
        // Attribution descriptors are small even at the maximum endpoint size.
        // Use a separate bound from event admission, so repeated observations
        // do not retain a full event batch's scratch while receiving a socket.
        let limits = AdmissionLimits {
            max_batch_bytes: 64 * 1024,
            max_dependencies: 8,
            max_outstanding_requests: u64::from(self.spec.outstanding_request_limit),
        };
        let visible = scope.scope().visibility();
        let mut prepared = self
            .store
            .prepare_dictionaries(
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
            )
            .map_err(error)?;
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
        let ttl = self.remaining()?;
        let existing = self
            .owner
            .read_initial_observation(
                self.store,
                InitialObservationRequest {
                    ingest_root: path,
                    registration: self.spec,
                    bindings,
                    limits: reads,
                    policy_ttl: ttl,
                    cancelled: &self.cancel.requested,
                },
                grant(self.budget, reads.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate(self.root, d, h),
            )
            .await
            .map_err(error)?;
        if let Some(existing) = existing {
            return Ok(existing);
        }
        if *next_request > u64::from(self.spec.outstanding_request_limit) {
            return Err(error(
                "initial producer window full before observation append",
            ));
        }
        let ttl = self.remaining()?;
        let appended = self
            .owner
            .append_initial_dictionaries(
                self.store,
                InitialDictionaryAppendRequest {
                    ingest_root: path,
                    registration: self.spec,
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
                |d, h| validate(self.root, d, h),
            )
            .await
            .map_err(error)?;
        drop(appended);
        *next_request += 1;
        let ttl = self.remaining()?;
        self.owner
            .read_initial_observation(
                self.store,
                InitialObservationRequest {
                    ingest_root: path,
                    registration: self.spec,
                    bindings,
                    limits: reads,
                    policy_ttl: ttl,
                    cancelled: &self.cancel.requested,
                },
                grant(self.budget, reads.required_data_bytes().map_err(error)?, 0)?,
                |d, h| validate(self.root, d, h),
            )
            .await
            .map_err(error)?
            .ok_or_else(|| error("committed observation missing after append"))
    }
}
