//! One actual catalog barrier while the co-located coordinator retains custody.
use super::{CatalogIdentity, GuardError, backend, begin, inspection, lock_inspection};
use crate::storage::archive::{LocalCoordinator, ObservedCut};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, Transaction};

/// This borrow retains local exclusion; the connection's actual transport must
/// also hold coordinator custody until joined, including on cancellation.
/// Publication/maintenance use the same catalog owner row. No table SHARE locks
/// may be acquired after it: native DML takes its table lock before the row lock.
pub struct CatalogCapture<'a> {
    tx: Transaction<'a>,
    coordinator: &'a LocalCoordinator,
    snapshot: u64,
    started: Instant,
    deadline: Instant,
}

/// A native cut actually observed inside this live catalog barrier. It still
/// requires registered persistence provenance and an atomic PG frontier/pin.
pub struct CapturedCut<'a, 'capture> {
    capture: &'a CatalogCapture<'capture>,
    observed: ObservedCut,
}
impl CapturedCut<'_, '_> {
    pub fn observed(&self) -> &ObservedCut {
        &self.observed
    }
    pub fn coordinator(&self) -> &LocalCoordinator {
        self.capture.coordinator
    }
    pub async fn verify_barrier(&self) -> Result<(), GuardError> {
        self.capture.verify().await
    }
}

impl<'a> CatalogCapture<'a> {
    pub async fn open(
        client: &'a mut Client,
        coordinator: &'a LocalCoordinator,
        duration: Duration,
    ) -> Result<Self, GuardError> {
        if duration.is_zero() || duration > Duration::from_secs(120) {
            return Err(GuardError::InvalidInput);
        }
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or(GuardError::InvalidInput)?;
        let tx = begin(client, false).await?;
        lock_inspection(&tx).await?;
        inspection::installed(&tx, coordinator.identity()).await?;
        // A still-enabled native lifetime must be sealed by its owning recovery
        // path first. Capturing never disables or reissues another writer.
        inspection::require(
            &tx,
            "SELECT NOT enabled FROM bigbrotr_archive.owner WHERE singleton",
            &[],
            GuardError::Conflict,
        )
        .await?;
        let snapshot: i64 = tx.query_one("SELECT snapshot_id FROM public.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1", &[]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        let snapshot = u64::try_from(snapshot).map_err(|_| GuardError::IncompatibleLayout)?;
        if snapshot == 0 || Instant::now() >= deadline {
            return Err(GuardError::Conflict);
        }
        // The parent's native deadline is at most120s. This finite server limit
        // allows native work to run without letting an abandoned barrier persist.
        // A fresh query checks the actual session before downstream durable work.
        tx.batch_execute("SET LOCAL idle_in_transaction_session_timeout='125s'")
            .await
            .map_err(backend)?;
        Ok(Self {
            tx,
            coordinator,
            snapshot,
            started: Instant::now(),
            deadline,
        })
    }
    pub fn identity(&self) -> &CatalogIdentity {
        self.coordinator.identity()
    }
    pub const fn snapshot_id(&self) -> u64 {
        self.snapshot
    }
    pub fn coordinator(&self) -> &LocalCoordinator {
        self.coordinator
    }
    pub(crate) async fn require_virgin(&self) -> Result<(), GuardError> {
        self.verify().await?;
        if self.snapshot != 1 {
            return Err(GuardError::ExistingData);
        }
        inspection::require(
            &self.tx,
            "SELECT NOT EXISTS(SELECT 1 FROM bigbrotr_archive.attempts)",
            &[],
            GuardError::ExistingData,
        )
        .await?;
        super::scientific::virgin(&self.tx, self.identity()).await?;
        self.verify().await
    }
    pub async fn verify(&self) -> Result<(), GuardError> {
        if Instant::now() >= self.deadline {
            return Err(GuardError::Conflict);
        }
        inspection::require(&self.tx, "SELECT NOT enabled AND (SELECT max(snapshot_id) FROM public.ducklake_snapshot)=$1::bigint FROM bigbrotr_archive.owner WHERE singleton", &[&(self.snapshot as i64)], GuardError::Conflict).await?;
        if Instant::now() >= self.deadline {
            return Err(GuardError::Conflict);
        }
        Ok(())
    }
    pub async fn accept(&self, observed: ObservedCut) -> Result<CapturedCut<'_, 'a>, GuardError> {
        if observed.catalog() != self.identity()
            || observed.observation().snapshot_id != self.snapshot
            || observed.started() < self.started
        {
            return Err(GuardError::Conflict);
        }
        self.verify().await?;
        Ok(CapturedCut {
            capture: self,
            observed,
        })
    }
    pub async fn finish(self) -> Result<(), GuardError> {
        self.tx.rollback().await.map_err(backend)
    }
}
