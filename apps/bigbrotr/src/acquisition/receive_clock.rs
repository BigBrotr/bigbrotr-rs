//! Local realtime attribution. Scheduling and authority use their separate clocks.
use bigbrotr_model::{ClockQuality, DurationUs, ObservedTime, QualifiedRef, evidence::ClockError};
use rustix::time::{ClockId, Timespec, clock_getres, clock_gettime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockReadError;

/// A sample at message completion, before parsing or signature verification.
/// Fields are private so public callers cannot relabel arbitrary signed times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveTime {
    received_at: ObservedTime,
    resolution: DurationUs,
    anomaly_flags: u32,
}
impl ReceiveTime {
    pub fn received_at(self) -> ObservedTime {
        self.received_at
    }
    pub fn quality(self, clock: QualifiedRef) -> Result<ClockQuality, ClockError> {
        ClockQuality::new(clock, self.resolution, None, self.anomaly_flags)
    }
}

pub struct ReceiveClock {
    resolution: DurationUs,
    previous: Option<ObservedTime>,
}
impl ReceiveClock {
    pub fn new() -> Result<Self, ClockReadError> {
        Ok(Self {
            resolution: resolution(clock_getres(ClockId::Realtime))?,
            previous: None,
        })
    }
    pub fn sample(&mut self) -> Result<ReceiveTime, ClockReadError> {
        Ok(self.record(observed(clock_gettime(ClockId::Realtime))?))
    }
    fn record(&mut self, received_at: ObservedTime) -> ReceiveTime {
        // Detect regressions between this owner's consecutive samples, without
        // clamping the original time. No NTP synchronization state is inferred.
        let rollback = self.previous.is_some_and(|previous| received_at < previous);
        self.previous = Some(received_at);
        ReceiveTime {
            received_at,
            resolution: self.resolution,
            anomaly_flags: u32::from(rollback) | (u32::from(self.resolution.get() > 1) << 2),
        }
    }
}

fn nanoseconds(value: Timespec) -> Result<i128, ClockReadError> {
    if !(0..1_000_000_000).contains(&value.tv_nsec) {
        return Err(ClockReadError);
    }
    Ok(i128::from(value.tv_sec) * 1_000_000_000 + i128::from(value.tv_nsec))
}
fn observed(value: Timespec) -> Result<ObservedTime, ClockReadError> {
    // Floor normalized negative instants too: one ns before epoch is -1us.
    i64::try_from(nanoseconds(value)?.div_euclid(1000))
        .map(ObservedTime::new)
        .map_err(|_| ClockReadError)
}
fn resolution(value: Timespec) -> Result<DurationUs, ClockReadError> {
    let nanos = u128::try_from(nanoseconds(value)?).map_err(|_| ClockReadError)?;
    if nanos == 0 {
        return Err(ClockReadError);
    }
    u64::try_from(nanos.div_ceil(1000))
        .map(DurationUs::new)
        .map_err(|_| ClockReadError)
}

#[cfg(test)]
#[path = "receive_clock_tests.rs"]
mod tests;
