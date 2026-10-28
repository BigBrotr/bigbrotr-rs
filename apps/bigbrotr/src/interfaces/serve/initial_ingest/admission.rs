//! Pre-effect finite-work admission. No issued operation is retried here.
use super::*;
use bigbrotr::runtime::{AcquireError, BudgetSnapshot};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(30);

pub(super) fn reserve(
    budget: &ResourceBudget,
    limits: ResourceLimits,
    domain: &'static str,
) -> Result<Admission<ReservedBudget>> {
    match budget.try_reserve(limits) {
        Ok(reservation) => Ok(Admission::Granted(reservation)),
        Err(AcquireError::Unavailable) => Ok(Admission::Deferred(Refusal {
            domain,
            requested: limits,
            observed: budget.snapshot(),
        })),
        Err(error) => Err((
            3,
            format!("initial finite-work reservation refused: {error:?}"),
        )),
    }
}
pub(super) enum Admission<T> {
    Granted(T),
    Deferred(Refusal),
}
#[derive(Clone, Copy)]
pub(super) struct Refusal {
    pub domain: &'static str,
    pub requested: ResourceLimits,
    /// Sampled after the atomic refusal. Other owners may have released work;
    /// this is not the original failing lock-protected deficit.
    pub observed: BudgetSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Role {
    Archive,
    Sync,
}

/// Two fixed scheduling slots, with no socket, input, attempt or grant held.
/// Renewal never resets their first-denial deadline. A grant still needs all
/// original operation/job/authority checks; this wait cannot extend them.
#[derive(Default)]
pub(super) struct Deferred {
    archive: Option<Instant>,
    sync: Option<Instant>,
}
impl Deferred {
    fn slot(&mut self, role: Role) -> &mut Option<Instant> {
        match role {
            Role::Archive => &mut self.archive,
            Role::Sync => &mut self.sync,
        }
    }
    pub(super) fn waiting(&mut self, role: Role, now: Instant) -> bool {
        let slot = self.slot(role);
        let first = slot.is_none();
        slot.get_or_insert(now);
        first
    }
    pub(super) fn admitted(&mut self, role: Role) {
        self.slot(role).take();
    }
    pub(super) fn any(&self) -> bool {
        self.archive.is_some() || self.sync.is_some()
    }
    pub(super) fn allows(&self, role: Role) -> bool {
        let first = match (self.archive, self.sync) {
            (None, None) => return true,
            (Some(_), None) => Role::Archive,
            (None, Some(_)) => Role::Sync,
            (Some(a), Some(s)) => {
                if a <= s {
                    Role::Archive
                } else {
                    Role::Sync
                }
            }
        };
        first == role
    }
    pub(super) fn check(&self, now: Instant) -> Result<()> {
        if [self.archive, self.sync]
            .into_iter()
            .flatten()
            .any(|started| now.saturating_duration_since(started) >= WAIT)
        {
            return Err((
                4,
                "initial pre-effect capacity wait expired; no deferred operation was issued".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_effect_wait_does_not_reset_at_polling_or_renewal_and_expires_before_grant() {
        let start = Instant::now();
        let mut waiting = Deferred::default();
        waiting.waiting(Role::Sync, start);
        for elapsed in [1, 15, 29] {
            waiting.waiting(Role::Sync, start + Duration::from_secs(elapsed));
            waiting.check(start + Duration::from_secs(elapsed)).unwrap();
        }
        assert!(waiting.check(start + WAIT).is_err());
        assert!(waiting.any());
        assert!(!waiting.allows(Role::Archive));
    }

    #[test]
    fn oldest_deferred_role_precedes_new_work_and_keeps_the_other_debt() {
        let start = Instant::now();
        let mut waiting = Deferred::default();
        assert!(waiting.allows(Role::Archive) && waiting.allows(Role::Sync));
        waiting.waiting(Role::Sync, start);
        waiting.waiting(Role::Archive, start + sync::INTERVAL);
        assert!(waiting.any());
        assert!(waiting.allows(Role::Sync));
        assert!(!waiting.allows(Role::Archive));
        waiting.admitted(Role::Sync);
        assert!(waiting.any());
        assert!(waiting.allows(Role::Archive));
        assert!(!waiting.allows(Role::Sync));
        waiting.admitted(Role::Archive);
        assert!(!waiting.any());
    }

    #[test]
    fn actual_held_capacity_defers_without_issuing_and_returns_only_after_drop() {
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                100,
                0,
                ResourceLimits::new(100, 4, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let index = budget
            .try_acquire(ResourceRequest::data(70, 1, 0).unwrap())
            .unwrap();
        let request = ResourceLimits::new(60, 2, 0, 0);
        assert!(matches!(
            reserve(&budget, request, "application").unwrap(),
            Admission::Deferred(_)
        ));
        assert_eq!(budget.snapshot().used().data_bytes(), 70);
        assert_eq!(budget.snapshot().active_permits(), 1);
        assert!(reserve(&budget, ResourceLimits::new(101, 1, 0, 0), "application").is_err());
        drop(index);
        let Admission::Granted(granted) = reserve(&budget, request, "application").unwrap() else {
            panic!("released capacity")
        };
        let completion = granted
            .budget()
            .try_acquire(ResourceRequest::data(60, 2, 0).unwrap())
            .unwrap();
        drop(granted);
        assert_eq!(budget.snapshot().used().data_bytes(), 60);
        drop(completion);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
