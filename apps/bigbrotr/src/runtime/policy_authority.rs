//! Local authority lifetime, not principal grants or an authenticated Control adapter.
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use std::{fmt, sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError {
    InvalidTtl,
    Unavailable,
    Expired,
    WrongDataset,
    WrongIncarnation,
    WrongRefresh,
    StalePolicy,
    RefreshInFlight,
    RefreshTimedOut,
    Exhausted,
    ClockUnavailable,
    ClockRegression,
    ClockRange,
}
impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidTtl => {
                "policy lease TTL must be positive and at most the configured five-second ceiling"
            }
            Self::Unavailable => "policy authority unavailable",
            Self::Expired => "policy authority expired",
            Self::WrongDataset => "policy dataset mismatch",
            Self::WrongIncarnation => "policy incarnation mismatch",
            Self::WrongRefresh => "foreign or completed policy refresh",
            Self::StalePolicy => "policy epoch is stale",
            Self::RefreshInFlight => "policy refresh is already owned",
            Self::RefreshTimedOut => "policy refresh timed out",
            Self::Exhausted => "policy refresh sequence exhausted",
            Self::ClockUnavailable => "authority clock unavailable",
            Self::ClockRegression => "authority clock regressed",
            Self::ClockRange => "authority deadline out of range",
        })
    }
}
impl std::error::Error for LeaseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyLeaseTtl(Duration);
impl PolicyLeaseTtl {
    pub fn new(value: Duration) -> Result<Self, LeaseError> {
        if value.is_zero() || value > Duration::from_secs(5) {
            return Err(LeaseError::InvalidTtl);
        }
        Ok(Self(value))
    }
}

/// Opaque local clock coordinate: never UTC, serialized input or a stored lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthorityTick(Duration);
mod sealed {
    pub trait Sealed {}
}

/// Sealed to the qualified provider; tests alone may inject a controlled clock.
pub trait AuthorityClock: sealed::Sealed {
    fn read(&self) -> Result<AuthorityTick, LeaseError>;
}
#[derive(Debug, Clone, Copy)]
pub struct SuspendAwareClock;
impl sealed::Sealed for SuspendAwareClock {}
impl AuthorityClock for SuspendAwareClock {
    fn read(&self) -> Result<AuthorityTick, LeaseError> {
        // Darwin CLOCK_MONOTONIC includes sleep; Linux needs CLOCK_BOOTTIME.
        // std::Instant deliberately does not promise suspend-aware elapsed time.
        #[cfg(target_os = "linux")]
        let clock = rustix::time::DynamicClockId::Boottime;
        #[cfg(target_os = "macos")]
        let clock = rustix::time::DynamicClockId::Known(rustix::time::ClockId::Monotonic);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let value = rustix::time::clock_gettime_dynamic(clock)
                .map_err(|_| LeaseError::ClockUnavailable)?;
            if value.tv_sec < 0 || !(0..1_000_000_000).contains(&value.tv_nsec) {
                return Err(LeaseError::ClockRange);
            }
            Ok(AuthorityTick(Duration::new(
                value.tv_sec as u64,
                value.tv_nsec as u32,
            )))
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        Err(LeaseError::ClockUnavailable)
    }
}

/// Plain response data. Only the authenticated adapter may submit it; constructing
/// this object cannot authenticate Control or establish an action/resource grant.
#[derive(Debug)]
pub struct LeaseResponse {
    pub dataset: DatasetId,
    pub process_incarnation: ProcessIncarnation,
    pub policy_epoch: PolicyEpoch,
    pub ttl: Duration,
}

/// Locally created request correlation. Not cloneable or deserializable authority.
#[derive(Debug)]
pub struct RefreshTicket {
    tracker: Arc<()>,
    sequence: u64,
    dataset: DatasetId,
    incarnation: ProcessIncarnation,
}
impl RefreshTicket {
    pub const fn request_sequence(&self) -> u64 {
        self.sequence
    }
    pub const fn dataset(&self) -> DatasetId {
        self.dataset
    }
    pub const fn process_incarnation(&self) -> ProcessIncarnation {
        self.incarnation
    }
}
#[derive(Debug, Clone, Copy)]
struct PendingRefresh {
    sequence: u64,
    started: AuthorityTick,
    timed_out: bool,
}
#[derive(Debug)]
struct InstalledLease {
    epoch: PolicyEpoch,
    deadline: AuthorityTick,
}

/// A transport-side time bound copied from a checked lease, never an action
/// grant. The owning supervisor must invalidate its transport gate on policy
/// change or shutdown. Each actual write samples the suspend-aware clock again.
pub struct DeliveryWindow<C: AuthorityClock = SuspendAwareClock> {
    clock: C,
    last_tick: AuthorityTick,
    deadline: AuthorityTick,
    epoch: PolicyEpoch,
    failed: Option<LeaseError>,
}
impl<C: AuthorityClock> DeliveryWindow<C> {
    pub const fn epoch(&self) -> PolicyEpoch {
        self.epoch
    }
    pub fn check(&mut self) -> Result<(), LeaseError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        let result = self.clock.read().and_then(|now| {
            if now < self.last_tick {
                return Err(LeaseError::ClockRegression);
            }
            self.last_tick = now;
            if now >= self.deadline {
                return Err(LeaseError::Expired);
            }
            Ok(())
        });
        if let Err(error) = result {
            self.failed = Some(error);
        }
        result
    }
    /// Same-policy refresh preserves the transport's highest clock sample. A
    /// expired/failed window cannot be revived even by a freshly minted window.
    pub fn renew(&mut self, mut next: Self) -> Result<(), LeaseError> {
        let result = (|| {
            self.check()?;
            if next.epoch != self.epoch {
                return Err(LeaseError::StalePolicy);
            }
            next.last_tick = next.last_tick.max(self.last_tick);
            next.check()?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                *self = next;
                Ok(())
            }
            Err(error) => {
                self.failed = Some(error);
                Err(error)
            }
        }
    }
}

impl<C: AuthorityClock + Clone> PolicyLeaseTracker<C> {
    /// Start before the authenticated action lookup, so waiting for its reply
    /// consumes both the Control lease and the action's remaining lifetime.
    pub fn begin_action(&mut self, epoch: PolicyEpoch) -> Result<PendingAction<C>, LeaseError> {
        let window = self.delivery_window(epoch)?;
        let started = window.last_tick;
        Ok(PendingAction { window, started })
    }
    pub fn delivery_window(
        &mut self,
        admitted_epoch: PolicyEpoch,
    ) -> Result<DeliveryWindow<C>, LeaseError> {
        self.check_delivery(admitted_epoch)?;
        Ok(DeliveryWindow {
            clock: self.clock.clone(),
            last_tick: self.last_tick,
            deadline: self.lease.as_ref().expect("checked lease").deadline,
            epoch: admitted_epoch,
            failed: None,
        })
    }
}

pub struct PendingAction<C: AuthorityClock = SuspendAwareClock> {
    window: DeliveryWindow<C>,
    started: AuthorityTick,
}
/// A nonrenewable time bound. The caller separately owns the authenticated
/// principal/action/resource binding; this value alone is never a grant.
pub struct ActionWindow<C: AuthorityClock = SuspendAwareClock>(DeliveryWindow<C>);
impl<C: AuthorityClock> PendingAction<C> {
    pub fn complete(mut self, remaining: Duration) -> Result<ActionWindow<C>, LeaseError> {
        let deadline = self
            .started
            .0
            .checked_add(remaining)
            .ok_or(LeaseError::ClockRange)?;
        self.window.deadline = self.window.deadline.min(AuthorityTick(deadline));
        self.window.check()?;
        Ok(ActionWindow(self.window))
    }
}
impl<C: AuthorityClock> ActionWindow<C> {
    pub fn check(&mut self) -> Result<(), LeaseError> {
        self.0.check()
    }
}

/// One refresh and one lease per owner. Mutable access serializes all transitions.
/// Every policy-authorized output/retry must call `check_delivery` immediately
/// before dispatch. A finite local diagnostic may report a completed observation
/// after cleanup when its contract grants no continuing authority; its revision
/// identifies the checked observation, not authority at diagnostic delivery.
#[derive(Debug)]
pub struct PolicyLeaseTracker<C: AuthorityClock = SuspendAwareClock> {
    clock: C,
    last_tick: AuthorityTick,
    clock_failed: Option<LeaseError>,
    identity: Arc<()>,
    dataset: DatasetId,
    incarnation: ProcessIncarnation,
    known_epoch: PolicyEpoch,
    maximum_ttl: PolicyLeaseTtl,
    last_sequence: u64,
    pending: Option<PendingRefresh>,
    lease: Option<InstalledLease>,
}
impl PolicyLeaseTracker {
    pub fn new(
        dataset: DatasetId,
        incarnation: ProcessIncarnation,
        known_epoch: PolicyEpoch,
        maximum_ttl: PolicyLeaseTtl,
    ) -> Result<Self, LeaseError> {
        Self::with_clock(
            dataset,
            incarnation,
            known_epoch,
            maximum_ttl,
            SuspendAwareClock,
        )
    }
}
impl<C: AuthorityClock> PolicyLeaseTracker<C> {
    fn with_clock(
        dataset: DatasetId,
        incarnation: ProcessIncarnation,
        known_epoch: PolicyEpoch,
        maximum_ttl: PolicyLeaseTtl,
        clock: C,
    ) -> Result<Self, LeaseError> {
        let last_tick = clock.read()?;
        Ok(Self {
            clock,
            last_tick,
            clock_failed: None,
            identity: Arc::new(()),
            dataset,
            incarnation,
            known_epoch,
            maximum_ttl,
            last_sequence: 0,
            pending: None,
            lease: None,
        })
    }

    pub fn begin_refresh(&mut self) -> Result<RefreshTicket, LeaseError> {
        let started = self.sample()?;
        if self.pending.is_some() {
            return Err(LeaseError::RefreshInFlight);
        }
        let Some(sequence) = self.last_sequence.checked_add(1) else {
            self.lease = None;
            return Err(LeaseError::Exhausted);
        };
        self.last_sequence = sequence;
        self.pending = Some(PendingRefresh {
            sequence,
            started,
            timed_out: false,
        });
        Ok(RefreshTicket {
            tracker: Arc::clone(&self.identity),
            sequence,
            dataset: self.dataset,
            incarnation: self.incarnation,
        })
    }

    /// The adapter calls this only after this exact underlying request is terminal.
    pub fn complete_refresh(
        &mut self,
        ticket: &RefreshTicket,
        response: LeaseResponse,
    ) -> Result<(), LeaseError> {
        let pending = self.require_pending(ticket)?;
        let now = self.sample()?;
        self.pending = None;
        if response.dataset != self.dataset {
            return Err(LeaseError::WrongDataset);
        }
        if response.process_incarnation != self.incarnation {
            return Err(LeaseError::WrongIncarnation);
        }
        // Even an unusable late response can reveal an authenticated newer epoch.
        self.advance_epoch(response.policy_epoch);
        if response.policy_epoch != self.known_epoch {
            return Err(LeaseError::StalePolicy);
        }
        if pending.timed_out {
            return Err(LeaseError::RefreshTimedOut);
        }
        let ttl = PolicyLeaseTtl::new(response.ttl)?;
        if ttl.0 > self.maximum_ttl.0 {
            return Err(LeaseError::InvalidTtl);
        }
        let deadline = AuthorityTick(
            pending
                .started
                .0
                .checked_add(ttl.0)
                .ok_or(LeaseError::ClockRange)?,
        );
        if now >= deadline {
            return Err(LeaseError::Expired);
        }
        self.lease = Some(InstalledLease {
            epoch: response.policy_epoch,
            deadline,
        });
        Ok(())
    }

    /// Caller timeout does not prove a stopped driver: retain the occupied slot.
    /// Its later response is terminal cleanup only and cannot install a lease.
    pub fn refresh_timed_out(&mut self, ticket: &RefreshTicket) -> Result<(), LeaseError> {
        self.require_pending(ticket)?;
        self.sample()?;
        self.pending
            .as_mut()
            .expect("matched pending refresh")
            .timed_out = true;
        Ok(())
    }

    /// Only terminal failure/join or positive exclusion permits releasing the slot.
    pub fn refresh_failed(&mut self, ticket: &RefreshTicket) -> Result<(), LeaseError> {
        self.require_pending(ticket)?;
        self.pending = None;
        self.sample()?;
        Ok(())
    }

    pub fn observe_epoch(
        &mut self,
        dataset: DatasetId,
        epoch: PolicyEpoch,
    ) -> Result<(), LeaseError> {
        if dataset != self.dataset {
            return Err(LeaseError::WrongDataset);
        }
        self.sample()?;
        self.advance_epoch(epoch);
        Ok(())
    }

    pub const fn known_epoch(&self) -> PolicyEpoch {
        self.known_epoch
    }

    /// Checks the epoch under which the queued output was admitted. A new lease
    /// never relabels old output as authorized under a different policy.
    pub fn check_delivery(&mut self, admitted_epoch: PolicyEpoch) -> Result<(), LeaseError> {
        self.remaining_for(admitted_epoch).map(|_| ())
    }

    /// Scheduling hint measured with the same suspend-aware clock as delivery.
    /// This duration is not authority: dispatch must still recheck the lease.
    pub fn remaining_validity(&mut self) -> Result<Duration, LeaseError> {
        self.remaining_for(self.known_epoch)
    }

    fn remaining_for(&mut self, admitted_epoch: PolicyEpoch) -> Result<Duration, LeaseError> {
        let now = self.sample()?;
        if admitted_epoch != self.known_epoch {
            return Err(LeaseError::StalePolicy);
        }
        let lease = self.lease.as_ref().ok_or(LeaseError::Unavailable)?;
        if lease.epoch != admitted_epoch {
            return Err(LeaseError::StalePolicy);
        }
        if now >= lease.deadline {
            return Err(LeaseError::Expired);
        }
        Ok(lease.deadline.0 - now.0)
    }

    fn advance_epoch(&mut self, epoch: PolicyEpoch) {
        if epoch > self.known_epoch {
            self.known_epoch = epoch;
            self.lease = None;
        }
    }

    fn require_pending(&self, ticket: &RefreshTicket) -> Result<PendingRefresh, LeaseError> {
        let pending = self.pending.ok_or(LeaseError::WrongRefresh)?;
        // The retained Arc prevents identity reuse even if an old tracker is dropped.
        if !Arc::ptr_eq(&self.identity, &ticket.tracker) || pending.sequence != ticket.sequence {
            return Err(LeaseError::WrongRefresh);
        }
        Ok(pending)
    }

    fn sample(&mut self) -> Result<AuthorityTick, LeaseError> {
        if let Some(error) = self.clock_failed {
            return Err(error);
        }
        let result = self.clock.read().and_then(|now| {
            if now < self.last_tick {
                Err(LeaseError::ClockRegression)
            } else {
                Ok(now)
            }
        });
        match result {
            Ok(now) => {
                self.last_tick = now;
                Ok(now)
            }
            Err(error) => {
                self.lease = None;
                if let Some(pending) = &mut self.pending {
                    pending.timed_out = true;
                }
                self.clock_failed = Some(error);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/policy_lifetime/cases.rs"]
mod tests;
