//! Current Control supervision while an independent catalog/native worker runs.
//! Historical lineage proves inputs; this owner separately refreshes authority.
use super::*;
use bigbrotr::runtime::policy_authority::{PolicyLeaseTracker, PolicyLeaseTtl};
use bigbrotr::storage::postgres::control_owner::PolicySnapshot;

pub(super) struct Authority<'a> {
    pub(super) root: &'a Resolved,
    pub(super) progress: &'a watch::Sender<Progress>,
    pub(super) cancellation: &'a Cancellation,
    dataset: DatasetId,
    policy: PolicySnapshot,
    lease: PolicyLeaseTracker,
    due: Instant,
}

impl<'a> Authority<'a> {
    pub(super) async fn history(
        &mut self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        proof: &VerifiedSuccessorHistory<'_>,
        frontier: &RetainedArchiveFrontier,
    ) -> Result<()> {
        if frontier.has_compacted_history() {
            let root = self.root;
            let input = request(root, self.cancellation)?;
            self.operation(owner.recheck_compacted_archive_history(
                frontier,
                proof,
                recovery,
                input,
                |d, h| validate(root, d, h),
            ))
            .await?;
        }
        self.local(|cancel| proof.validate(cancel).map_err(error))
    }

    pub(super) fn new(
        root: &'a Resolved,
        progress: &'a watch::Sender<Progress>,
        cancellation: &'a Cancellation,
        owner: &ControlSession,
        dataset: DatasetId,
        policy: PolicySnapshot,
    ) -> Result<Self> {
        Ok(Self {
            root,
            progress,
            cancellation,
            dataset,
            policy,
            lease: PolicyLeaseTracker::new(
                dataset,
                owner.identity().process_incarnation,
                PolicyEpoch::new(policy.policy_revision),
                PolicyLeaseTtl::new(duration(root, "authorization_ttl")).map_err(error)?,
            )
            .map_err(error)?,
            due: Instant::now(),
        })
    }

    pub(super) async fn refresh(&mut self, owner: &mut ControlSession) -> Result<()> {
        let ttl = duration(self.root, "authorization_ttl");
        let timeout = duration(self.root, "renewal_timeout").min(ttl);
        phase(self.progress, timeout)?;
        let ticket = self.lease.begin_refresh().map_err(error)?;
        let started = Instant::now();
        let root = self.root;
        let snapshot = bounded(timeout, self.cancellation, async {
            owner
                .refresh(|document, digest| validate(root, document, digest))
                .await
                .map_err(owner_error)
        })
        .await?;
        if snapshot != self.policy {
            return Err(error("accepted policy changed during Archive catch-up"));
        }
        self.lease
            .complete_refresh(
                &ticket,
                super::super::super::response(
                    self.dataset,
                    owner.identity().process_incarnation,
                    snapshot,
                    ttl,
                ),
            )
            .map_err(error)?;
        self.due = Instant::now()
            + (self.lease.remaining_validity().map_err(error)? / 2)
                .min(duration(root, "heartbeat_interval").saturating_sub(started.elapsed()));
        Ok(())
    }

    pub(super) async fn operation<T, E: std::fmt::Debug>(
        &mut self,
        work: impl Future<Output = std::result::Result<T, E>>,
    ) -> Result<T> {
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(error)?;
        let timeout = duration(self.root, "renewal_timeout")
            .min(self.lease.remaining_validity().map_err(error)? / 2);
        phase(self.progress, timeout)?;
        let result = bounded(timeout, self.cancellation, async {
            work.await.map_err(error)
        })
        .await?;
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(error)?;
        Ok(result)
    }

    /// The enclosing owner already runs on a joined blocking thread. Bound one
    /// local read by the current lease; its independent parent watchdog remains
    /// live. A slow read cannot reset the deadline or deliver expired evidence.
    pub(super) fn local<T>(&mut self, work: impl FnOnce(&AtomicBool) -> Result<T>) -> Result<T> {
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(error)?;
        let timeout = duration(self.root, "renewal_timeout")
            .min(self.lease.remaining_validity().map_err(error)? / 2);
        let deadline = Instant::now() + timeout;
        phase(self.progress, timeout)?;
        if self.cancellation.requested.load(Ordering::Acquire) {
            return Err(error("cancelled before local Archive work"));
        }
        let result = work(&self.cancellation.requested)?;
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(error)?;
        if self.cancellation.requested.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(error("local Archive work expired before delivery"));
        }
        Ok(result)
    }

    /// Only an independent manager operation belongs here. The original
    /// deadline stays fixed while current Control renewal remains serialized.
    pub(super) async fn external<T>(
        &mut self,
        owner: &mut ControlSession,
        deadline: Instant,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::pin!(work);
        loop {
            self.lease
                .check_delivery(self.lease.known_epoch())
                .map_err(error)?;
            phase(
                self.progress,
                deadline
                    .min(self.due + duration(self.root, "renewal_timeout"))
                    .saturating_duration_since(Instant::now()),
            )?;
            tokio::select! {
                biased;
                _ = self.cancellation.wait() => return Err(error("Archive catch-up cancelled")),
                _ = tokio::time::sleep_until(deadline) => return Err(error("Archive catch-up deadline")),
                _ = tokio::time::sleep_until(self.due) => self.refresh(owner).await?,
                result = &mut work => {
                    self.lease.check_delivery(self.lease.known_epoch()).map_err(error)?;
                    return result;
                },
            }
        }
    }

    /// A failed renewal, cancellation or timeout must still collect the actual
    /// native child before the caller can release its native/Control custody.
    pub(super) async fn blocking<T: Send + 'static>(
        &mut self,
        owner: &mut ControlSession,
        deadline: Instant,
        work: impl FnOnce(&AtomicBool) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(error)?;
        if self.cancellation.requested.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(error("Archive catch-up dispatch cancelled or expired"));
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let child_cancel = cancel.clone();
        let mut child = tokio::task::spawn_blocking(move || work(&child_cancel));
        let result = self
            .external(owner, deadline, async { Ok((&mut child).await) })
            .await;
        cancel.store(true, Ordering::Release);
        match result {
            Ok(terminal) => terminal.map_err(error)?,
            Err(failure) => {
                if tokio::time::timeout(
                    duration(self.root, "graceful_shutdown_timeout"),
                    &mut child,
                )
                .await
                .is_err()
                {
                    // The supervising parent also retains Control's actual
                    // driver. Neither task may abandon an unjoined native call.
                    std::process::exit(4);
                }
                Err(failure)
            }
        }
    }
}
