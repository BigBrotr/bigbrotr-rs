use bigbrotr::storage::postgres::migrations;

use migrations::{AppliedMigration, MigrationError, catalog, plan};

#[test]
fn session_mutation_audit_requires_an_explicit_additive_upgrade() {
    let applied: Vec<_> = plan(&[], 806)
        .unwrap()
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
    assert_eq!(
        migrations::require_applied(&applied, 807),
        Err(MigrationError::MissingPrerequisite(807))
    );
    let upgrade = plan(&applied, 807).unwrap();
    assert_eq!(upgrade.pending.len(), 1);
    assert_eq!(upgrade.pending[0].name, "0807_session_mutation_audit.sql");
    assert!(migrations::require_applied(&applied, 806).is_ok());
}

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
fn control_backend_fencing_is_an_additive_explicit_migration() {
    let plan = plan(&[], 7).unwrap();
    assert_eq!(
        plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 801, 805, 802, 7]
    );
    assert_eq!(plan.pending.last().unwrap().name, "0007_control_owner.sql");
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
    unknown.version = 999;
    assert!(matches!(
        plan(&[unknown], 1),
        Err(MigrationError::UnknownMigration(999))
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
fn ingest_migration_extends_the_exact_prefix_without_reapplying_control() {
    let bootstrap = plan(&[], 2).unwrap();
    assert_eq!(
        bootstrap
            .pending
            .iter()
            .map(|m| m.version)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let upgrade = plan(&[initial_receipt()], 2).unwrap();
    assert_eq!(upgrade.from, Some(1));
    assert_eq!(upgrade.pending.len(), 1);
    let migration = &upgrade.pending[0];
    assert_eq!(migration.name, "0002_ingest.sql");
    let checksum = migration
        .checksum()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    assert_eq!(
        checksum,
        "6a355b4d0066c60397bc09c7aa1f5d8d27722fbfbf9e9b859f02d55217f80202"
    );
    let second = AppliedMigration {
        version: 2,
        name: "0002_ingest.sql".into(),
        checksum: migration.checksum(),
        reader_min: 1,
        reader_max: 1,
        writer_floor: 1,
    };
    assert!(matches!(
        plan(std::slice::from_ref(&second), 2),
        Err(MigrationError::HistoryNotPrefix)
    ));
    assert!(
        plan(&[initial_receipt(), second.clone()], 2)
            .unwrap()
            .pending
            .is_empty()
    );
    assert!(matches!(
        plan(&[initial_receipt(), second], 1),
        Err(MigrationError::Downgrade)
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

#[test]
fn initialization_migration_has_an_exact_known_prefix_and_older_consumers_remain_usable() {
    let initial = plan(&[], 801).unwrap();
    assert_eq!(
        initial
            .pending
            .iter()
            .map(|m| m.version)
            .collect::<Vec<_>>(),
        [1, 2, 801]
    );
    let applied: Vec<_> = initial
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
    assert!(migrations::require_applied(&applied, 2).is_ok());
    assert!(migrations::require_applied(&applied[..2], 801).is_err());
    assert!(migrations::require_applied(&applied[..2], 2).is_ok());
    assert!(plan(&applied, 801).unwrap().pending.is_empty());
}

#[test]
fn compiled_migrations_match_the_central_application_order() {
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("../../../migrations/manifest.json")).unwrap();
    let entries = manifest["application_order"].as_array().unwrap();
    assert_eq!(entries.len(), catalog().len());
    for (entry, compiled) in entries.iter().zip(catalog()) {
        assert_eq!(entry["version"], compiled.version);
        assert_eq!(entry["name"], compiled.name);
    }
}

#[test]
fn first_activation_appends_reserved_management_commands_after_enrollment() {
    let plan = plan(&[], 802).unwrap();
    assert_eq!(
        plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 801, 805, 802]
    );
}

#[test]
fn authoritative_job_bounds_append_without_rewriting_prior_receipts() {
    let plan = plan(&[], 8).unwrap();
    assert_eq!(
        plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 801, 805, 802, 7, 8]
    );
    assert_eq!(plan.pending.last().unwrap().name, "0008_job_bounds.sql");
}

#[test]
fn archive_attempts_append_without_rewriting_applied_sql() {
    let plan = plan(&[], 9).expect("archive attempt migration must be selected");
    assert_eq!(
        plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 801, 805, 802, 7, 8, 9]
    );
    assert_eq!(
        plan.pending.last().unwrap().name,
        "0009_archive_attempts.sql"
    );
}

#[test]
fn producer_reservations_require_the_new_complete_prefix() {
    let plan = plan(&[], 11).expect("producer reservation protocol must be explicit");
    assert_eq!(
        plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 801, 805, 802, 7, 8, 9, 10, 11]
    );
    let applied: Vec<_> = plan
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
    assert_eq!(
        migrations::require_applied(&applied[..9], 11),
        Err(MigrationError::MissingPrerequisite(11))
    );
    assert!(migrations::require_applied(&applied, 11).is_ok());
}

#[test]
fn producer_outcomes_require_an_additive_migration_after_reservations() {
    let plan = plan(&[], 12).expect("producer outcome copy must have an explicit migration");
    assert_eq!(
        plan.pending.last().unwrap().name,
        "0012_producer_outcomes.sql"
    );
    let applied: Vec<_> = plan.pending[..10]
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
    assert_eq!(
        migrations::require_applied(&applied, 12),
        Err(MigrationError::MissingPrerequisite(12))
    );
    assert_eq!(migrations::plan(&applied, 12).unwrap().pending.len(), 1);
}

#[test]
fn input_bindings_require_the_exact_additive_prefix_after_outcomes() {
    let old = plan(&[], 12).unwrap();
    let applied: Vec<_> = old
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
    assert_eq!(
        migrations::require_applied(&applied, 13),
        Err(MigrationError::MissingPrerequisite(13))
    );
    let upgrade = plan(&applied, 13).unwrap();
    assert_eq!(upgrade.pending.len(), 1);
    assert_eq!(upgrade.pending[0].name, "0013_input_bindings.sql");
    assert!(migrations::require_applied(&applied, 12).is_ok());
}

#[test]
fn import_record_checkpoints_require_explicit_protocol_after_input_bindings() {
    let old = plan(&[], 13).unwrap();
    let applied: Vec<_> = old
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
    let upgrade = plan(&applied, 14).expect("logical checkpoint protocol is explicit");
    assert_eq!(upgrade.pending.len(), 1);
    assert_eq!(upgrade.pending[0].name, "0014_import_checkpoints.sql");
    assert_eq!(
        migrations::require_applied(&applied, 14),
        Err(MigrationError::MissingPrerequisite(14))
    );
}
