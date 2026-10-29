//! Serialize current authority while independently owned ClickHouse work runs.
use super::*;
use bigbrotr::runtime::policy_authority::{PolicyLeaseTracker, PolicyLeaseTtl};
use std::future::Future;
use tokio::time::Instant as Timer;

pub(in crate::interfaces::serve::initial_ingest::index) struct Authority<'a> {
    root: &'a Resolved,
    progress: &'a watch::Sender<Progress>,
    cancellation: &'a Cancellation,
    policy: PolicySnapshot,
    profile: Profile,
    lease: PolicyLeaseTracker,
    due: Timer,
    live_initial: Option<&'a bigbrotr::storage::postgres::control_owner::LiveInitialWriter>,
}
impl<'a> Authority<'a> {
    pub(in crate::interfaces::serve::initial_ingest::index) fn new(
        root: &'a Resolved,
        progress: &'a watch::Sender<Progress>,
        cancellation: &'a Cancellation,
        owner: &ControlSession,
        policy: PolicySnapshot,
        profile: Profile,
    ) -> Result<Self> {
        Ok(Self {
            root,
            progress,
            cancellation,
            policy,
            profile,
            lease: PolicyLeaseTracker::new(
                required(root, "topology.dataset_id")?
                    .parse()
                    .map_err(error)?,
                owner.identity().process_incarnation,
                PolicyEpoch::new(policy.policy_revision),
                PolicyLeaseTtl::new(duration(root, "authorization_ttl")).map_err(error)?,
            )
            .map_err(error)?,
            due: Timer::now(),
            live_initial: None,
        })
    }

    pub(in crate::interfaces::serve::initial_ingest::index) fn with_live_initial(
        mut self,
        writer: &'a bigbrotr::storage::postgres::control_owner::LiveInitialWriter,
    ) -> Self {
        self.live_initial = Some(writer);
        self
    }

    pub(in crate::interfaces::serve::initial_ingest::index) async fn refresh(
        &mut self,
        owner: &mut ControlSession,
        session: Option<&IndexSession>,
        state: &IndexState,
    ) -> Result<()> {
        let ttl = duration(self.root, "authorization_ttl");
        let timeout = duration(self.root, "renewal_timeout").min(ttl);
        phase(self.progress, timeout)?;
        let ticket = self.lease.begin_refresh().map_err(error)?;
        let started = Timer::now();
        let snapshot = bounded(timeout, self.cancellation, async {
            owner
                .refresh(|d, h| validate(self.root, self.profile, d, h))
                .await
                .map_err(owner_error)
        })
        .await?;
        if snapshot != self.policy {
            return Err(error("accepted policy changed during Index catch-up"));
        }
        self.lease
            .complete_refresh(
                &ticket,
                super::super::super::super::response(
                    required(self.root, "topology.dataset_id")?
                        .parse()
                        .map_err(error)?,
                    owner.identity().process_incarnation,
                    snapshot,
                    ttl,
                ),
            )
            .map_err(error)?;
        if let Some(writer) = self.live_initial {
            let root = self.root;
            let profile = self.profile;
            self.operation(owner.renew_live_initial_ingest(
                writer,
                ttl,
                |d, h| validate(root, profile, d, h),
                &Cell::new(false),
            ))
            .await?;
        }
        if let Some(session) = session {
            self.operation(owner.renew_index(session, state.custody(), &Cell::new(false)))
                .await?;
        }
        self.due = Timer::now()
            + (self.lease.remaining_validity().map_err(error)? / 2)
                .min(duration(self.root, "heartbeat_interval").saturating_sub(started.elapsed()));
        Ok(())
    }

    pub(in crate::interfaces::serve::initial_ingest::index) async fn operation<
        T,
        E: std::fmt::Debug,
    >(
        &mut self,
        work: impl Future<Output = std::result::Result<T, E>>,
    ) -> Result<T> {
        self.live()?;
        let timeout = duration(self.root, "renewal_timeout")
            .min(self.lease.remaining_validity().map_err(error)? / 2);
        phase(self.progress, timeout)?;
        let result = bounded(timeout, self.cancellation, async {
            work.await.map_err(error)
        })
        .await?;
        self.live()?;
        Ok(result)
    }

    fn live(&mut self) -> Result<()> {
        if self.cancellation.requested.load(Ordering::Acquire) {
            return Err(error("Index catch-up cancelled"));
        }
        self.lease
            .check_delivery(self.lease.known_epoch())
            .map_err(error)
    }

    pub(in crate::interfaces::serve::initial_ingest::index) fn local<T>(
        &mut self,
        work: impl FnOnce(&AtomicBool) -> Result<T>,
    ) -> Result<T> {
        self.live()?;
        let timeout = duration(self.root, "renewal_timeout")
            .min(self.lease.remaining_validity().map_err(error)? / 2);
        phase(self.progress, timeout)?;
        let deadline = Timer::now() + timeout;
        let result = work(&self.cancellation.requested)?;
        self.live()?;
        if Timer::now() >= deadline {
            return Err(error("local proof expired before delivery"));
        }
        Ok(result)
    }

    /// Poll only the actual executor join. On any error Drop cancels and joins
    /// while the parent watchdog and PG driver independently retain custody.
    pub(in crate::interfaces::serve::initial_ingest::index) async fn join<T: Send + 'static>(
        &mut self,
        owner: &mut ControlSession,
        session: Option<&IndexSession>,
        state: &IndexState,
        adapter: &IndexAdapter,
        mut operation: IndexOperation<T>,
        deadline: Instant,
    ) -> Result<JoinedIndexOperation<std::result::Result<T, PhysicalError>>> {
        loop {
            self.live()?;
            if Instant::now() >= deadline {
                return Err(error("physical operation deadline"));
            }
            if Timer::now() >= self.due {
                self.refresh(owner, session, state).await?;
            }
            if let Some(joined) = operation.try_join().map_err(error)? {
                self.live()?;
                if Instant::now() >= deadline || adapter.unresolved_query().is_some() {
                    return Err(error("physical query outcome unresolved"));
                }
                match joined.outcome() {
                    IndexOperationOutcome::Completed(Ok(_)) => return Ok(joined),
                    IndexOperationOutcome::Completed(Err(reason)) => return Err(error(reason)),
                    _ => return Err(error("physical executor did not complete positively")),
                }
            }
            phase(
                self.progress,
                Timer::from_std(deadline)
                    .min(self.due + duration(self.root, "renewal_timeout"))
                    .saturating_duration_since(Timer::now()),
            )?;
            tokio::select! {
                biased;
                _ = self.cancellation.wait() => return Err(error("Index catch-up cancelled")),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {},
            }
        }
    }
}
