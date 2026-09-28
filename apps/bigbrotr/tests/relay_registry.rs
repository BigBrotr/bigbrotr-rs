use bigbrotr::storage::postgres::migrations::{self, AppliedMigration, MigrationError};

#[test]
fn relay_registry_appends_after_job_pages_without_rewriting_the_applied_prefix() {
    let previous = migrations::plan(&[], 816).unwrap();
    let applied: Vec<_> = previous
        .pending
        .iter()
        .map(|m| AppliedMigration {
            version: m.version,
            name: m.name.into(),
            checksum: m.checksum(),
            reader_min: m.reader_min,
            reader_max: m.reader_max,
            writer_floor: m.writer_floor,
        })
        .collect();
    let next = migrations::plan(&applied, 3).expect("explicit relay registry migration");
    assert_eq!(next.pending.len(), 1);
    assert_eq!(next.pending[0].name, "0003_relay.sql");
    assert_eq!(
        migrations::require_applied(&applied, 3),
        Err(MigrationError::MissingPrerequisite(3))
    );
    assert!(migrations::require_applied(&applied, 816).is_ok());
}
