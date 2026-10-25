//! Explicit virgin-history boundary for cooperating local Archive coordinators.
//! The caller retains actual driver custody; this is not catalog recovery.
use super::{
    ControlSession, InitialHistoryRequest, OwnerError, RetainedArchiveFrontier,
    VerifiedSuccessorHistory, application_transaction, initial_history, require, successor_history,
};
use crate::storage::{
    archive::{LocalCoordinator, RetainedInputs},
    postgres::archive_attempt::{self, ArchiveAttemptError, ArchiveAttemptReceipt},
    rocks::store::IngestRecovery,
};
use bigbrotr_model::{DatasetId, Digest256, OwnerResourceId};
use std::cell::Cell;
use tokio_postgres::Transaction;

fn backend(_: tokio_postgres::Error) -> ArchiveAttemptError {
    ArchiveAttemptError::Backend
}

impl ControlSession {
    /// Explicit compacted positive-history issuance. The caller retains the
    /// same outer frontier keeper and both actual transports through teardown.
    #[allow(
        clippy::too_many_arguments,
        reason = "retained inputs, history, exact authority and commit custody"
    )]
    pub async fn register_compacted_retained_archive_attempt(
        &mut self,
        frontier: &RetainedArchiveFrontier,
        proof: &VerifiedSuccessorHistory<'_>,
        recovery: &IngestRecovery,
        inputs: &RetainedInputs,
        input: InitialHistoryRequest<'_>,
        commit_started: &Cell<bool>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ArchiveAttemptReceipt, ArchiveAttemptError> {
        let request = inputs.request();
        if request.dataset() != self.request.dataset
            || request.catalog() == self.identity.resource_id
        {
            return Err(ArchiveAttemptError::InvalidInput);
        }
        let check = frontier.require_compacted_context(
            proof,
            recovery,
            request.catalog(),
            inputs.coordinator_binding(),
        )?;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_history(proof.history(), recovery, &input, validate)
            .await?;
        check
            .require_history(
                &tx,
                recovery,
                proof.history(),
                &mut lease,
                policy,
                input.cancelled,
            )
            .await?;
        if !binding(
            &tx,
            request.dataset(),
            request.catalog(),
            inputs.coordinator_binding(),
        )
        .await?
        {
            return Err(ArchiveAttemptError::Prerequisite);
        }
        let receipt =
            archive_attempt::register_in(&tx, request, current, Some(inputs.coordinator_binding()))
                .await?;
        frontier.validate_compacted(proof, recovery, input.cancelled)?;
        successor_history::require_current(&tx, current).await?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if receipt.created {
            commit_started.set(true);
            tx.commit()
                .await
                .map_err(|_| ArchiveAttemptError::CommitUnknown)?;
        } else {
            tx.rollback().await.map_err(backend)?;
        }
        frontier.validate_compacted(proof, recovery, input.cancelled)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ArchiveAttemptError::Unavailable);
        }
        self.available = true;
        Ok(receipt)
    }

    /// Explicit initialization only. Existing pre-protocol attempts refuse;
    /// identical committed binding retries preserve original issuer attribution.
    /// A lost response requires a new fenced session and the same local binding.
    pub async fn initialize_archive_coordinator(
        &mut self,
        coordinator: &LocalCoordinator,
        commit_started: &Cell<bool>,
    ) -> Result<bool, ArchiveAttemptError> {
        self.archive_coordinator(coordinator, true, commit_started)
            .await
    }

    /// Ordinary recovery only checks an existing binding. Missing is not fresh.
    pub async fn require_archive_coordinator(
        &mut self,
        coordinator: &LocalCoordinator,
    ) -> Result<(), ArchiveAttemptError> {
        self.archive_coordinator(coordinator, false, &Cell::new(false))
            .await?;
        Ok(())
    }

    async fn archive_coordinator(
        &mut self,
        coordinator: &LocalCoordinator,
        initialize: bool,
        commit_started: &Cell<bool>,
    ) -> Result<bool, ArchiveAttemptError> {
        let identity = coordinator.identity();
        if identity.dataset() != self.request.dataset
            || identity.catalog() == self.identity.resource_id
        {
            return Err(ArchiveAttemptError::InvalidInput);
        }
        self.begin_operation()
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 10)
            .await
            .map_err(|_| ArchiveAttemptError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        // The Control fence precedes the dataset row everywhere. The following
        // statement sees completed prior issuance; no unlocked emptiness test.
        tx.query_one(
            "SELECT dataset_id FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
            &[&&identity.dataset().as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
        let present = binding(
            &tx,
            identity.dataset(),
            identity.catalog(),
            coordinator.binding_digest(),
        )
        .await?;
        if present {
            tx.rollback().await.map_err(backend)?;
            self.available = true;
            return Ok(false);
        }
        if !initialize {
            return Err(ArchiveAttemptError::Prerequisite);
        }
        let old: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM control.archive_attempts WHERE dataset_id=$1::bytea)",
                &[&&identity.dataset().as_bytes()[..]],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        if old {
            return Err(ArchiveAttemptError::Conflict);
        }
        tx.execute("INSERT INTO control.archive_coordinators(dataset_id,catalog_id,binding_sha256,protocol_version,issuer_resource,issuer_epoch,issuer_incarnation,initialized_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,1,$4::bytea,$5::text::numeric,$6::bytea,(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint)", &[&&identity.dataset().as_bytes()[..],&&identity.catalog().as_bytes()[..],&&coordinator.binding_digest().as_bytes()[..],&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]])
            .await.map_err(backend)?;
        commit_started.set(true);
        tx.commit()
            .await
            .map_err(|_| ArchiveAttemptError::CommitUnknown)?;
        self.available = true;
        Ok(true)
    }

    /// Issuance from complete, synchronized input linkage. The retained token
    /// owns its allocation and local lock throughout this actual Control call.
    pub async fn register_retained_archive_attempt(
        &mut self,
        inputs: &RetainedInputs,
        commit_started: &Cell<bool>,
    ) -> Result<ArchiveAttemptReceipt, ArchiveAttemptError> {
        let request = inputs.request();
        if request.dataset() != self.request.dataset
            || request.catalog() == self.identity.resource_id
        {
            return Err(ArchiveAttemptError::InvalidInput);
        }
        self.begin_operation()
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 15)
            .await
            .map_err(|_| ArchiveAttemptError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        if !binding(
            &tx,
            request.dataset(),
            request.catalog(),
            inputs.coordinator_binding(),
        )
        .await?
        {
            return Err(ArchiveAttemptError::Prerequisite);
        }
        let result = archive_attempt::register_bound(
            tx,
            request,
            self.identity,
            inputs.coordinator_binding(),
            commit_started,
        )
        .await?;
        self.available = true;
        Ok(result)
    }
}

/// Project one fixed boolean, never an unbounded persisted value. Missing and
/// different binding have distinct outcomes; neither is permission to initialize.
async fn binding(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    digest: Digest256,
) -> Result<bool, ArchiveAttemptError> {
    let row = tx.query_opt("SELECT catalog_id=$2::bytea AND binding_sha256=$3::bytea AND protocol_version=1 FROM control.archive_coordinators WHERE dataset_id=$1::bytea", &[&&dataset.as_bytes()[..],&&catalog.as_bytes()[..],&&digest.as_bytes()[..]])
        .await.map_err(backend)?;
    match row {
        None => Ok(false),
        Some(row) if row.try_get::<_, bool>(0).map_err(backend)? => Ok(true),
        Some(_) => Err(ArchiveAttemptError::Conflict),
    }
}
