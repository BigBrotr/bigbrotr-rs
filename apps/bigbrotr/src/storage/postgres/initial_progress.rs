//! Opt-in progress wrappers share the original native/authority transaction kernels.
use super::*;

impl ControlSession {
    pub async fn certify_initial_event_with_progress(
        &mut self,
        store: &mut IngestStore,
        input: InitialCertificationRequest<'_>,
        permit: OwnedPermit,
        progress: &SyncProgress,
        validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        self.certify_native_event(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialCertificationRequest {
                ingest_root: input.ingest_root,
                event: input.event,
                relay_source: input.relay_source,
                acquisition: input.acquisition,
                provenance_enabled: input.provenance_enabled,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn certify_live_initial_event_with_progress(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialCertificationRequest<'_>,
        permit: OwnedPermit,
        progress: &SyncProgress,
        validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        self.certify_native_event(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn certify_live_successor_event_with_progress(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorCertificationRequest<'_>,
        permit: OwnedPermit,
        progress: &SyncProgress,
        validate: impl FnMut(&str, [u8; 32], &PreparedCertification) -> Result<(), OwnerError>,
    ) -> Result<StoredCertification, InitialEventError> {
        self.certify_native_event(
            NativeOwner::successor(&mut writer.native, writer.check.as_ref()),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn admit_initial_event_with_progress(
        &mut self,
        store: &mut IngestStore,
        input: InitialEventRequest<'_>,
        permit: OwnedPermit,
        progress: &SyncProgress,
        validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        self.admit_native_event(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialEventRequest {
                ingest_root: input.ingest_root,
                event: input.event,
                relay_source: input.relay_source,
                acquisition: input.acquisition,
                provenance_enabled: input.provenance_enabled,
                recorded_at: input.recorded_at,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn admit_live_initial_event_with_progress(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialEventRequest<'_>,
        permit: OwnedPermit,
        progress: &SyncProgress,
        validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        self.admit_native_event(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn admit_live_successor_event_with_progress(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorEventRequest<'_>,
        permit: OwnedPermit,
        progress: &SyncProgress,
        validate: impl FnMut(&str, [u8; 32], &PreparedEvent) -> Result<(), OwnerError>,
    ) -> Result<StoredOutcome, InitialEventError> {
        self.admit_native_event(
            NativeOwner::successor(&mut writer.native, writer.check.as_ref()),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn append_initial_dictionaries_with_progress(
        &mut self,
        store: &mut IngestStore,
        input: InitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        progress: &ProgressGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_dictionaries(
            NativeOwner::Store(store),
            NativeAuthority::Initial(input.registration),
            LiveInitialDictionaryAppendRequest {
                ingest_root: input.ingest_root,
                request_sequence: input.request_sequence,
                intent: input.intent,
                recorded_at: input.recorded_at,
                limits: input.limits,
                policy_ttl: input.policy_ttl,
                cancelled: input.cancelled,
            },
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn append_live_initial_dictionaries_with_progress(
        &mut self,
        writer: &mut LiveInitialWriter,
        input: LiveInitialDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        progress: &ProgressGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_dictionaries(
            NativeOwner::Store(writer.native.store_mut()),
            NativeAuthority::Live(&writer.binding),
            input,
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn append_live_successor_dictionaries_with_progress(
        &mut self,
        writer: &mut LiveSuccessorWriter,
        input: LiveSuccessorDictionaryAppendRequest<'_>,
        permit: OwnedPermit,
        progress: &ProgressGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialEventError> {
        self.append_native_dictionaries(
            NativeOwner::successor(&mut writer.native, writer.check.as_ref()),
            NativeAuthority::Successor(&writer.binding),
            input,
            permit,
            Some(progress),
            validate,
        )
        .await
    }

    pub async fn bootstrap_initial_dictionaries(
        &mut self,
        store: &mut IngestStore,
        input: InitialBatchRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialBatchError> {
        self.bootstrap_native_dictionaries(store, input, permit, None, validate)
            .await
    }

    pub async fn bootstrap_initial_dictionaries_with_progress(
        &mut self,
        store: &mut IngestStore,
        input: InitialBatchRequest<'_>,
        permit: OwnedPermit,
        progress: &ProgressGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<StoredReceipt, InitialBatchError> {
        self.bootstrap_native_dictionaries(store, input, permit, Some(progress), validate)
            .await
    }
}
