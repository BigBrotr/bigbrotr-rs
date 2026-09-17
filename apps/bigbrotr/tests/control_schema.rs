#[path = "../src/storage/postgres/migrations.rs"]
mod migrations;

use migrations::{AppliedMigration, MigrationError, catalog, plan};

fn initial_receipt() -> AppliedMigration {
    let migration = &catalog()[0];
    AppliedMigration {
        version: 1,
        name: "0001_control.sql".into(),
        checksum: migration.checksum(),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    }
}

#[test]
fn an_absent_ledger_plans_one_atomic_initial_migration() {
    let plan = plan(&[], 1).unwrap();
    assert_eq!(plan.from, None);
    assert_eq!(plan.target, 1);
    assert_eq!(plan.pending.len(), 1);
    assert_eq!(plan.pending[0].name, "0001_control.sql");
    assert_eq!(migrations::ENGINE, "postgres-control");
    assert!(!plan.pending[0].sql.is_empty());
}

#[test]
fn an_exact_existing_receipt_does_not_reapply_ddl() {
    let plan = plan(&[initial_receipt()], 1).unwrap();
    assert_eq!(plan.from, Some(1));
    assert_eq!(plan.pending.len(), 0);
}

#[test]
fn changed_or_unrecognized_history_never_becomes_a_repair_plan() {
    let mut changed = initial_receipt();
    changed.checksum[0] ^= 1;
    assert!(matches!(
        plan(&[changed], 1),
        Err(MigrationError::ChecksumMismatch(1))
    ));
    let mut renamed = initial_receipt();
    renamed.name = "0001_unrelated.sql".into();
    assert!(matches!(
        plan(&[renamed], 1),
        Err(MigrationError::MetadataMismatch(1))
    ));
    let mut unknown = initial_receipt();
    unknown.version = 2;
    assert!(matches!(
        plan(&[unknown], 1),
        Err(MigrationError::UnknownMigration(2))
    ));
    assert!(matches!(
        plan(&[initial_receipt(), initial_receipt()], 1),
        Err(MigrationError::HistoryNotPrefix)
    ));
    assert!(matches!(
        plan(&[], 0),
        Err(MigrationError::UnknownTarget(0))
    ));
    assert!(matches!(
        plan(&[], 501),
        Err(MigrationError::UnknownTarget(501))
    ));
}

#[test]
fn compatibility_metadata_is_part_of_the_immutable_receipt() {
    for field in ["reader_min", "reader_max", "writer_floor"] {
        let mut receipt = initial_receipt();
        match field {
            "reader_min" => receipt.reader_min = 2,
            "reader_max" => receipt.reader_max = 2,
            "writer_floor" => receipt.writer_floor = 2,
            _ => unreachable!(),
        }
        assert!(matches!(
            plan(&[receipt], 1),
            Err(MigrationError::MetadataMismatch(1))
        ));
    }
}

#[test]
fn checksum_covers_exact_sql_bytes_without_newline_normalization() {
    let mut migration = catalog()[0];
    migration.sql = "abc";
    let abc_digest = migration.checksum();
    assert_eq!(
        abc_digest,
        [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad
        ]
    );
    migration.sql = "abc\n";
    assert_ne!(migration.checksum(), abc_digest);
}
