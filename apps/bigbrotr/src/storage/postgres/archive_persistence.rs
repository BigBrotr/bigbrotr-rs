//! Explicit fresh-only local persistence participation. No archive read/GC grant.
use super::{ControlSession, application_transaction, require};
use crate::storage::postgres::{
    archive_attempt::ArchiveAttemptError, catalog_guard::CatalogCapture,
};
use std::cell::Cell;

fn backend(error: tokio_postgres::Error) -> ArchiveAttemptError {
    if error.code() == Some(&tokio_postgres::error::SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) {
        ArchiveAttemptError::Prerequisite
    } else {
        ArchiveAttemptError::Backend
    }
}
impl ControlSession {
    /// The same initialized binding resolves a lost reply without replacing its
    /// issuer/time. A missing binding requires virgin Control AND native history
    /// under retained local/catalog exclusion; unrelated orphans are never adopted.
    pub async fn initialize_archive_persistence(
        &mut self,
        capture: &CatalogCapture<'_>,
        commit_started: &Cell<bool>,
    ) -> Result<bool, ArchiveAttemptError> {
        let identity = capture.identity();
        if identity.dataset() != self.request.dataset
            || identity.catalog() == self.identity.resource_id
        {
            return Err(ArchiveAttemptError::InvalidInput);
        }
        capture
            .verify()
            .await
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        self.begin_operation()
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 15)
            .await
            .map_err(|_| ArchiveAttemptError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        tx.query_one(
            "SELECT dataset_id FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
            &[&&identity.dataset().as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
        let row = tx.query_opt("SELECT catalog_id=$2::bytea AND coordinator_binding_sha256=$3::bytea AND protocol_version=1 AND protection_class='local-synced' AND baseline_snapshot=1 FROM control.archive_persistence WHERE dataset_id=$1::bytea", &[&&identity.dataset().as_bytes()[..], &&identity.catalog().as_bytes()[..], &&capture.coordinator().binding_digest().as_bytes()[..]]).await.map_err(backend)?;
        if let Some(row) = row {
            if !row.try_get::<_, bool>(0).map_err(backend)? {
                return Err(ArchiveAttemptError::Conflict);
            }
            capture
                .verify()
                .await
                .map_err(|_| ArchiveAttemptError::Unavailable)?;
            tx.rollback().await.map_err(backend)?;
            self.available = true;
            return Ok(false);
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
        capture
            .require_virgin()
            .await
            .map_err(|error| match error {
                crate::storage::postgres::catalog_guard::GuardError::ExistingData => {
                    ArchiveAttemptError::Conflict
                }
                _ => ArchiveAttemptError::Unavailable,
            })?;
        tx.execute("INSERT INTO control.archive_persistence(dataset_id,catalog_id,coordinator_binding_sha256,protocol_version,protection_class,baseline_snapshot,issuer_resource,issuer_epoch,issuer_incarnation,initialized_at_us) VALUES ($1::bytea,$2::bytea,$3::bytea,1,'local-synced',1,$4::bytea,$5::text::numeric,$6::bytea,(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint)", &[&&identity.dataset().as_bytes()[..],&&identity.catalog().as_bytes()[..],&&capture.coordinator().binding_digest().as_bytes()[..],&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
        capture
            .verify()
            .await
            .map_err(|_| ArchiveAttemptError::Unavailable)?;
        commit_started.set(true);
        tx.commit()
            .await
            .map_err(|_| ArchiveAttemptError::CommitUnknown)?;
        self.available = true;
        Ok(true)
    }
}
