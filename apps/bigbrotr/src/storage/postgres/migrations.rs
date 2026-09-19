//! Exact control-schema catalog and read-only forward planning.
//!
//! The caller supplies an authoritative, ordered ledger read. This module does
//! not infer absence from errors, take migration ownership, or execute SQL.
use sha2::{Digest, Sha256};
use std::fmt;

pub const ENGINE: &str = "postgres-control";
const READER_VERSION: u32 = 1;
const WRITER_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub sql: &'static str,
    pub reader_min: u32,
    pub reader_max: u32,
    pub writer_floor: u32,
}
impl Migration {
    pub fn checksum(&self) -> [u8; 32] {
        Sha256::digest(self.sql.as_bytes()).into()
    }
}

// Application order matches migrations/manifest.json, independent of reserved ID order.
static MIGRATIONS: [Migration; 11] = [
    Migration {
        version: 1,
        name: "0001_control.sql",
        sql: include_str!("../../../../../migrations/postgres/0001_control.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 2,
        name: "0002_ingest.sql",
        sql: include_str!("../../../../../migrations/postgres/0002_ingest.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 801,
        name: "0801_management_identity.sql",
        sql: include_str!("../../../../../migrations/postgres/0801_management_identity.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 805,
        name: "0805_owner_enrollment.sql",
        sql: include_str!("../../../../../migrations/postgres/0805_owner_enrollment.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 802,
        name: "0802_management_commands.sql",
        sql: include_str!("../../../../../migrations/postgres/0802_management_commands.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 7,
        name: "0007_control_owner.sql",
        sql: include_str!("../../../../../migrations/postgres/0007_control_owner.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 8,
        name: "0008_job_bounds.sql",
        sql: include_str!("../../../../../migrations/postgres/0008_job_bounds.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 9,
        name: "0009_archive_attempts.sql",
        sql: include_str!("../../../../../migrations/postgres/0009_archive_attempts.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 10,
        name: "0010_archive_coordinator.sql",
        sql: include_str!("../../../../../migrations/postgres/0010_archive_coordinator.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 11,
        name: "0011_producer_requests.sql",
        sql: include_str!("../../../../../migrations/postgres/0011_producer_requests.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
    Migration {
        version: 12,
        name: "0012_producer_outcomes.sql",
        sql: include_str!("../../../../../migrations/postgres/0012_producer_outcomes.sql"),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    },
];

pub fn catalog() -> &'static [Migration] {
    &MIGRATIONS
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    pub version: u32,
    pub name: String,
    pub checksum: [u8; 32],
    pub reader_min: u32,
    pub reader_max: u32,
    pub writer_floor: u32,
}

#[derive(Debug)]
pub struct MigrationPlan {
    pub from: Option<u32>,
    pub target: u32,
    pub pending: &'static [Migration],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationError {
    UnknownTarget(u32),
    UnknownMigration(u32),
    HistoryNotPrefix,
    ChecksumMismatch(u32),
    MetadataMismatch(u32),
    IncompatibleReader,
    IncompatibleWriter,
    Downgrade,
    MissingPrerequisite(u32),
}
impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTarget(version) => {
                write!(f, "unsupported control migration target {version}")
            }
            Self::UnknownMigration(version) => {
                write!(f, "unknown applied control migration {version}")
            }
            Self::HistoryNotPrefix => {
                f.write_str("applied migrations are not an exact ordered catalog prefix")
            }
            Self::ChecksumMismatch(version) => write!(
                f,
                "applied control migration {version} has a different checksum"
            ),
            Self::MetadataMismatch(version) => write!(
                f,
                "applied control migration {version} has different metadata"
            ),
            Self::IncompatibleReader => {
                f.write_str("control schema is outside this reader's compatibility range")
            }
            Self::IncompatibleWriter => f.write_str("control writer floor excludes this binary"),
            Self::MissingPrerequisite(version) => {
                write!(f, "control migration {version} must be applied explicitly")
            }
            Self::Downgrade => {
                f.write_str("control migration downgrade is unsupported; use qualified recovery")
            }
        }
    }
}
impl std::error::Error for MigrationError {}

pub fn plan(applied: &[AppliedMigration], target: u32) -> Result<MigrationPlan, MigrationError> {
    plan_catalog(catalog(), applied, target, READER_VERSION, WRITER_VERSION)
}

/// Validate the whole known history while allowing an older consumer to operate
/// after additive migrations. IDs are reservations, not application chronology.
pub fn require_applied(applied: &[AppliedMigration], required: u32) -> Result<(), MigrationError> {
    let latest = catalog().last().expect("nonempty catalog").version;
    plan(applied, latest)?;
    if !applied.iter().any(|entry| entry.version == required) {
        return Err(MigrationError::MissingPrerequisite(required));
    }
    Ok(())
}

fn plan_catalog(
    catalog: &'static [Migration],
    applied: &[AppliedMigration],
    target: u32,
    reader: u32,
    writer: u32,
) -> Result<MigrationPlan, MigrationError> {
    let target_index = catalog
        .iter()
        .position(|m| m.version == target)
        .ok_or(MigrationError::UnknownTarget(target))?;
    for (index, receipt) in applied.iter().enumerate() {
        let position = catalog
            .iter()
            .position(|m| m.version == receipt.version)
            .ok_or(MigrationError::UnknownMigration(receipt.version))?;
        if position != index {
            return Err(MigrationError::HistoryNotPrefix);
        }
        let expected = &catalog[position];
        if receipt.name != expected.name
            || receipt.reader_min != expected.reader_min
            || receipt.reader_max != expected.reader_max
            || receipt.writer_floor != expected.writer_floor
        {
            return Err(MigrationError::MetadataMismatch(receipt.version));
        }
        if receipt.checksum != expected.checksum() {
            return Err(MigrationError::ChecksumMismatch(receipt.version));
        }
    }
    if applied.len() > target_index + 1 {
        return Err(MigrationError::Downgrade);
    }
    // History describes earlier formats; only the current and proposed endpoint
    // must be usable by this binary. Migration gaps are owned catalog identifiers.
    for migration in applied
        .len()
        .checked_sub(1)
        .map(|i| &catalog[i])
        .into_iter()
        .chain(std::iter::once(&catalog[target_index]))
    {
        if !(migration.reader_min..=migration.reader_max).contains(&reader) {
            return Err(MigrationError::IncompatibleReader);
        }
        if writer < migration.writer_floor {
            return Err(MigrationError::IncompatibleWriter);
        }
    }
    Ok(MigrationPlan {
        from: applied.last().map(|m| m.version),
        target,
        pending: &catalog[applied.len()..target_index + 1],
    })
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    static TWO: [Migration; 2] = [
        Migration {
            version: 1,
            name: "0001_control.sql",
            sql: "first",
            reader_min: 1,
            reader_max: 2,
            writer_floor: 1,
        },
        Migration {
            version: 501,
            name: "0501_analytics.sql",
            sql: "second",
            reader_min: 1,
            reader_max: 2,
            writer_floor: 2,
        },
    ];
    fn receipt(m: &Migration) -> AppliedMigration {
        AppliedMigration {
            version: m.version,
            name: m.name.into(),
            checksum: m.checksum(),
            reader_min: m.reader_min,
            reader_max: m.reader_max,
            writer_floor: m.writer_floor,
        }
    }
    #[test]
    fn reserved_number_gaps_are_not_missing_migrations() {
        let p = plan_catalog(&TWO, &[receipt(&TWO[0])], 501, 2, 2).unwrap();
        assert_eq!(p.pending.len(), 1);
        assert_eq!(p.pending[0].version, 501);
        assert!(matches!(
            plan_catalog(&TWO, &[receipt(&TWO[1])], 501, 2, 2),
            Err(MigrationError::HistoryNotPrefix)
        ));
    }
    #[test]
    fn later_application_can_use_a_lower_reserved_id() {
        static ORDERED: [Migration; 2] = [
            Migration {
                version: 801,
                name: "0801_identity.sql",
                sql: "first",
                reader_min: 1,
                reader_max: 1,
                writer_floor: 1,
            },
            Migration {
                version: 501,
                name: "0501_analytics.sql",
                sql: "next",
                reader_min: 1,
                reader_max: 1,
                writer_floor: 1,
            },
        ];
        let result = plan_catalog(&ORDERED, &[receipt(&ORDERED[0])], 501, 1, 1).unwrap();
        assert_eq!(result.from, Some(801));
        assert_eq!(result.pending[0].version, 501);
        assert!(matches!(
            plan_catalog(
                &ORDERED,
                &[receipt(&ORDERED[0]), receipt(&ORDERED[1])],
                801,
                1,
                1
            ),
            Err(MigrationError::Downgrade)
        ));
    }
    #[test]
    fn incompatible_endpoints_and_known_downgrades_are_refused() {
        assert!(matches!(
            plan_catalog(&TWO, &[], 1, 3, 2),
            Err(MigrationError::IncompatibleReader)
        ));
        assert!(matches!(
            plan_catalog(&TWO, &[], 501, 1, 1),
            Err(MigrationError::IncompatibleWriter)
        ));
        assert!(matches!(
            plan_catalog(&TWO, &[receipt(&TWO[0]), receipt(&TWO[1])], 1, 2, 2),
            Err(MigrationError::Downgrade)
        ));
    }
}
