use crate::{DictionaryKind, DurationUs, QualifiedRef};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockError {
    InvalidClockReference,
    ReservedAnomalyFlags,
}
impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidClockReference => "clock identity must reference an observer descriptor",
            Self::ReservedAnomalyFlags => "unassigned clock anomaly bits are incompatible with v1",
        })
    }
}
impl std::error::Error for ClockError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ClockFields")]
pub struct ClockQuality {
    clock_id: QualifiedRef,
    resolution_us: DurationUs,
    uncertainty_us: Option<DurationUs>,
    anomaly_flags: u32,
}

impl ClockQuality {
    pub fn new(
        clock_id: QualifiedRef,
        resolution_us: DurationUs,
        uncertainty_us: Option<DurationUs>,
        anomaly_flags: u32,
    ) -> Result<Self, ClockError> {
        if clock_id.dictionary_kind != DictionaryKind::Observer {
            return Err(ClockError::InvalidClockReference);
        }
        if anomaly_flags & !7 != 0 {
            return Err(ClockError::ReservedAnomalyFlags);
        }
        Ok(Self {
            clock_id,
            resolution_us,
            uncertainty_us,
            anomaly_flags,
        })
    }
    pub const fn clock_id(self) -> QualifiedRef {
        self.clock_id
    }
    pub const fn resolution_us(self) -> DurationUs {
        self.resolution_us
    }
    pub const fn uncertainty_us(self) -> Option<DurationUs> {
        self.uncertainty_us
    }
    pub const fn anomaly_flags(self) -> u32 {
        self.anomaly_flags
    }
}

// Deserialize-with makes a missing field an error; Option's ordinary Serde
// missing-field default would erase the contract's absent-versus-null boundary.
fn uncertainty<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<DurationUs>, D::Error> {
    Option::<DurationUs>::deserialize(deserializer)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockFields {
    clock_id: QualifiedRef,
    resolution_us: DurationUs,
    #[serde(deserialize_with = "uncertainty")]
    uncertainty_us: Option<DurationUs>,
    anomaly_flags: u32,
}
impl TryFrom<ClockFields> for ClockQuality {
    type Error = ClockError;
    fn try_from(value: ClockFields) -> Result<Self, Self::Error> {
        Self::new(
            value.clock_id,
            value.resolution_us,
            value.uncertainty_us,
            value.anomaly_flags,
        )
    }
}
