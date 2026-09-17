//! Identity and timing checks; neither grants ownership nor excludes an old writer.
use bigbrotr_model::{DatasetId, OwnerEpoch, ProcessIncarnation};
use std::{fmt, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncarnationUnavailable;
impl fmt::Display for IncarnationUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fresh process incarnation unavailable")
    }
}
impl std::error::Error for IncarnationUnavailable {}

pub fn fresh_process_incarnation() -> Result<ProcessIncarnation, IncarnationUnavailable> {
    incarnation_from(getrandom::fill)
}

fn incarnation_from(
    fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> Result<ProcessIncarnation, IncarnationUnavailable> {
    let mut bytes = [0; 16];
    // Partial/error output is unusable. No retry loop, host name or PID fallback.
    fill(&mut bytes).map_err(|_| IncarnationUnavailable)?;
    ProcessIncarnation::try_from(bytes).map_err(|_| IncarnationUnavailable)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterRole {
    Control,
    Ingest,
    Archive,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterIdentity {
    dataset: DatasetId,
    role: WriterRole,
    epoch: OwnerEpoch,
    incarnation: ProcessIncarnation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterMismatch {
    Dataset,
    Role,
    Epoch,
    Incarnation,
}
impl fmt::Display for WriterMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dataset => "writer dataset mismatch",
            Self::Role => "writer role mismatch",
            Self::Epoch => "writer epoch mismatch",
            Self::Incarnation => "writer incarnation mismatch",
        })
    }
}
impl std::error::Error for WriterMismatch {}

impl WriterIdentity {
    pub const fn new(
        dataset: DatasetId,
        role: WriterRole,
        epoch: OwnerEpoch,
        incarnation: ProcessIncarnation,
    ) -> Self {
        Self {
            dataset,
            role,
            epoch,
            incarnation,
        }
    }

    /// Compare to the identity established by the actual fenced storage owner.
    /// Equality alone cannot authorize a commit or establish positive exclusion.
    pub fn require_same(self, candidate: Self) -> Result<(), WriterMismatch> {
        if self.dataset != candidate.dataset {
            Err(WriterMismatch::Dataset)
        } else if self.role != candidate.role {
            Err(WriterMismatch::Role)
        } else if self.epoch != candidate.epoch {
            Err(WriterMismatch::Epoch)
        } else if self.incarnation != candidate.incarnation {
            Err(WriterMismatch::Incarnation)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingError {
    ZeroDuration,
    Overflow,
    UnsafeRenewalWindow,
}
impl fmt::Display for TimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroDuration => "ownership timing durations must be positive",
            Self::Overflow => "ownership timing overflow",
            Self::UnsafeRenewalWindow => {
                "heartbeat plus renewal timeout must be below TTL minus safety margin"
            }
        })
    }
}
impl std::error::Error for TimingError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnershipTiming {
    lease_ttl: Duration,
    heartbeat_interval: Duration,
    renewal_timeout: Duration,
    safety_margin: Duration,
    graceful_shutdown_timeout: Duration,
}
impl OwnershipTiming {
    pub fn new(
        lease_ttl: Duration,
        heartbeat_interval: Duration,
        renewal_timeout: Duration,
        safety_margin: Duration,
        graceful_shutdown_timeout: Duration,
    ) -> Result<Self, TimingError> {
        if [
            lease_ttl,
            heartbeat_interval,
            renewal_timeout,
            safety_margin,
            graceful_shutdown_timeout,
        ]
        .contains(&Duration::ZERO)
        {
            return Err(TimingError::ZeroDuration);
        }
        let renewal = heartbeat_interval
            .checked_add(renewal_timeout)
            .ok_or(TimingError::Overflow)?;
        let safe = lease_ttl
            .checked_sub(safety_margin)
            .ok_or(TimingError::UnsafeRenewalWindow)?;
        if renewal >= safe {
            return Err(TimingError::UnsafeRenewalWindow);
        }
        Ok(Self {
            lease_ttl,
            heartbeat_interval,
            renewal_timeout,
            safety_margin,
            graceful_shutdown_timeout,
        })
    }
    pub const fn lease_ttl(self) -> Duration {
        self.lease_ttl
    }
    pub const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }
    pub const fn renewal_timeout(self) -> Duration {
        self.renewal_timeout
    }
    pub const fn safety_margin(self) -> Duration {
        self.safety_margin
    }
    pub const fn graceful_shutdown_timeout(self) -> Duration {
        self.graceful_shutdown_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn entropy_failure_and_zero_are_redacted_refusals() {
        for result in [
            incarnation_from(|bytes| {
                bytes.fill(0xab);
                Err(getrandom::Error::UNSUPPORTED)
            }),
            incarnation_from(|_| Ok(())),
        ] {
            assert_eq!(result, Err(IncarnationUnavailable));
            assert_eq!(
                result.unwrap_err().to_string(),
                "fresh process incarnation unavailable"
            );
        }
    }
    #[test]
    fn actual_os_draws_are_nonzero_and_not_reused() {
        let first = fresh_process_incarnation().unwrap();
        let second = fresh_process_incarnation().unwrap();
        assert_ne!(first, second);
        assert_ne!(first.as_bytes(), &[0; 16]);
        for role in [WriterRole::Control, WriterRole::Index] {
            let writer = WriterIdentity::new(
                DatasetId::try_from([1; 16]).unwrap(),
                role,
                OwnerEpoch::new(1).unwrap(),
                first,
            );
            assert_eq!(writer.require_same(writer), Ok(()));
        }
    }
}
