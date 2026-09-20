use super::*;
use crate::engine::{Engine, EngineLimits};
use bigbrotr_model::{AdmissionBatch, Fact, Sequence, codec::FactDecodeLimits};
use bigbrotr_protocol::archive_batch::{self, Limits};
use std::{path::Path, sync::atomic::AtomicBool};

fn decode(value: &serde_json::Value, digest: &str) -> Fact {
    let bytes: Vec<u8> = value["preimage_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    Fact::decode_preimage(
        &bytes,
        value[digest].as_str().unwrap().parse().unwrap(),
        FactDecodeLimits::default(),
    )
    .unwrap()
}
fn facts() -> Vec<Fact> {
    let dictionaries: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/bigbrotr-model/tests/fixtures/dictionary-v1.json"
    ))
    .unwrap();
    let event: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/bigbrotr-protocol/tests/fixtures/archive/single.json"
    ))
    .unwrap();
    [
        decode(&dictionaries["vectors"][1], "sha256"),
        decode(&dictionaries["vectors"][3], "sha256"),
        decode(&event, "fact_digest"),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, f)| {
        let mut source = f.source_record_id();
        source.sequence = Sequence::new(i as u64 + 1).unwrap();
        Fact::new(
            source,
            f.visibility_scope_id(),
            f.recorded_at(),
            f.payload().clone(),
        )
        .unwrap()
    })
    .collect()
}
fn open(temp: &Path, catalog: &Path, data: &Path) -> Engine {
    // Ordinary attachment requires the operator's existing canonical data root.
    fs::create_dir_all(data).unwrap();
    let engine = Engine::open(
        &temp.canonicalize().unwrap(),
        EngineLimits {
            memory_bytes: 134217728,
            temp_bytes: 67108864,
            threads: 1,
        },
    )
    .unwrap();
    engine
        .load_bundle(Path::new(
            &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").unwrap(),
        ))
        .unwrap();
    engine
        .connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:{}' AS archive (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0)",
            catalog.display(),
            data.display()
        ))
        .unwrap();
    engine
}
fn prepare(engine: Engine, facts: &[Fact], data: &Path) -> (PreparedBatch, PublicationRequest) {
    let batch = AdmissionBatch::new(facts.iter().map(Fact::fact_ref).collect()).unwrap();
    let records: Vec<_> = facts
        .iter()
        .map(|f| f.encode_preimage(8192).unwrap())
        .collect();
    let limits = Limits {
        max_records: 16,
        max_encoded_bytes: 1048576,
        fact: FactDecodeLimits::default(),
    };
    let cancel = AtomicBool::new(false);
    let mut wire = Vec::new();
    archive_batch::write_batch(&batch, &records, &mut wire, limits, &cancel).unwrap();
    let digest = ArchiveBatch::new(
        1,
        TopologyGeneration::new(u64::MAX),
        std::slice::from_ref(&batch),
    )
    .unwrap()
    .digest();
    let request=serde_json::from_value(serde_json::json!({"catalog":{"dsn":"unused","data_root":format!("{}/",data.display())},"topology_generation":u64::MAX.to_string(),"batch_digest":digest,"attempt_id":([9;16]),"publication_ordinal":u64::MAX})).unwrap();
    (
        PreparedBatch::prepare(engine, wire.as_slice(), limits, &cancel).unwrap(),
        request,
    )
}

#[test]
fn complete_rows_and_receipt_survive_fresh_native_read_and_duplicate_refuses() {
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let catalog = home.path().join("catalog.ducklake");
    let data = home.path().join("kind=7");
    let engine = open(temp.path(), &catalog, &data);
    schema::initialize(&engine.connection).unwrap();
    let (prepared, request) = prepare(engine, &facts(), &home.path().join("kind=7"));
    prepared.publish_attached(&request).unwrap();
    drop(prepared);
    let temp = tempfile::tempdir().unwrap();
    let engine = open(temp.path(), &catalog, &data);
    let row:(u64,u16,String,String,u64)=engine.connection.query_row("SELECT created_at,kind,tags::VARCHAR,content,source_record_id.sequence FROM archive.events",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
    assert_eq!(
        row,
        (
            u64::MAX,
            1,
            "[[x], [x, ''], [x, '']]".into(),
            "é\n".into(),
            3
        )
    );
    let receipt:(u64,u64,u64,String)=engine.connection.query_row("SELECT topology_generation,publication_ordinal,record_count,input_ranges::VARCHAR FROM archive.archive_commits",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(
        receipt,
        (
            u64::MAX,
            u64::MAX,
            3,
            "[{'shard_id': 0, 'owner_epoch': 1, 'first_sequence': 1, 'last_sequence': 3}]".into()
        )
    );
    let counts:(u64,u64,u64)=engine.connection.query_row("SELECT (SELECT count(*) FROM archive.dictionaries),(SELECT count(*) FROM archive.archive_commits),(SELECT count(*) FROM ducklake_snapshots('archive'))",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(counts, (2, 1, 3));
    // Inspect actual native metadata independently of the DuckLake reader.
    let metadata = duckdb::Connection::open(&catalog).unwrap();
    let paths: String = metadata.query_row("SELECT string_agg(path, ',' ORDER BY path) FROM ducklake_data_file WHERE path_is_relative", [], |r| r.get(0)).unwrap();
    assert_eq!(
        paths,
        std::iter::repeat_n(format!("prepared-{}/data.parquet", "09".repeat(16)), 3)
            .collect::<Vec<_>>()
            .join(",")
    );
    let absolute: u64 = metadata
        .query_row(
            "SELECT count(*) FROM ducklake_data_file WHERE NOT path_is_relative",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(absolute, 0);
    drop(metadata);
    let (prepared, request) = prepare(engine, &facts(), &home.path().join("kind=7"));
    assert_eq!(
        prepared.publish_attached(&request),
        Err(PublicationError::ExistingData)
    );
}

#[test]
fn missing_dictionary_and_late_receipt_failure_leave_no_committed_rows() {
    for missing in [true, false] {
        let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let catalog = home.path().join("catalog.ducklake");
        let data = home.path().join("objects");
        let engine = open(temp.path(), &catalog, &data);
        schema::initialize(&engine.connection).unwrap();
        let all = facts();
        let selected = if missing { &all[2..] } else { &all[..] };
        let (prepared, request) = prepare(engine, selected, &data);
        if !missing {
            // An intentionally incompatible native receipt layout makes the final
            // registration fail after scientific registrations. This bypasses the PG guard
            // only in this embedded rollback fixture.
            prepared
                .engine
                .connection
                .execute_batch("ALTER TABLE archive.archive_commits ADD COLUMN canary BLOB")
                .unwrap();
        }
        assert_eq!(
            prepared.publish_attached(&request),
            Err(if missing {
                PublicationError::MissingDictionary
            } else {
                PublicationError::Native
            })
        );
        drop(prepared);
        if !missing {
            assert!(
                data.join("main/dictionaries")
                    .join(format!("prepared-{}", "09".repeat(16)))
                    .join("data.parquet")
                    .is_file(),
                "failed publication retains caller-owned prepared files until fenced recovery"
            );
        }
        let temp = tempfile::tempdir().unwrap();
        let engine = open(temp.path(), &catalog, &data);
        for table in schema::TABLES.into_iter().chain(["archive_commits"]) {
            assert_eq!(
                engine
                    .connection
                    .query_row(&format!("SELECT count(*) FROM archive.{table}"), [], |r| {
                        r.get::<_, u64>(0)
                    })
                    .unwrap(),
                0
            );
        }
    }
}

#[test]
fn incorrect_semantic_identity_refuses_before_catalog_attachment() {
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let engine = open(
        temp.path(),
        &home.path().join("catalog.ducklake"),
        &home.path().join("objects"),
    );
    let (prepared, mut request) = prepare(engine, &facts(), &home.path().join("objects"));
    request.batch_digest = Digest256::from_bytes([0; 32]);
    assert_eq!(prepared.publish(request), Err(PublicationError::Identity));
}

#[test]
fn identical_dictionary_mapping_preserves_each_fact_scope_in_batch_and_history() {
    use bigbrotr_model::{
        DescriptorLabel, DictionaryDescriptor, DictionaryEntry, FactPayload, Ordinal,
    };
    for historical in [false, true] {
        let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let catalog = home.path().join("catalog.ducklake");
        let data = home.path().join("objects");
        let engine = open(temp.path(), &catalog, &data);
        schema::initialize(&engine.connection).unwrap();
        let mut all = facts();
        let scope = bigbrotr_model::QualifiedRef {
            ordinal: Ordinal::new(2).unwrap(),
            ..all[0].visibility_scope_id()
        };
        let source = all[0].source_record_id();
        all.push(
            Fact::new(
                bigbrotr_model::SourceRecordId {
                    sequence: Sequence::new(4).unwrap(),
                    ..source
                },
                all[0].visibility_scope_id(),
                all[0].recorded_at(),
                FactPayload::Dictionary(
                    DictionaryEntry::new(
                        scope,
                        DictionaryDescriptor::Visibility {
                            class: DescriptorLabel::new("private".into()).unwrap(),
                        },
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        );
        all.push(
            Fact::new(
                bigbrotr_model::SourceRecordId {
                    sequence: Sequence::new(5).unwrap(),
                    ..source
                },
                scope,
                all[1].recorded_at(),
                all[1].payload().clone(),
            )
            .unwrap(),
        );
        let history_temp = tempfile::tempdir().unwrap();
        let engine = if historical {
            let (prepared, mut request) = prepare(engine, &all[..3], &data);
            request.publication_ordinal = 1;
            prepared.publish_attached(&request).unwrap();
            drop(prepared);
            open(history_temp.path(), &catalog, &data)
        } else {
            engine
        };
        let (prepared, mut request) =
            prepare(engine, if historical { &all[3..] } else { &all }, &data);
        request.publication_ordinal = 2;
        request.attempt_id = [10; 16];
        prepared.publish_attached(&request).unwrap();
        let scopes:String=prepared.engine.connection.query_row("SELECT string_agg(visibility_scope_id.ordinal::VARCHAR,',' ORDER BY visibility_scope_id.ordinal) FROM archive.dictionaries WHERE descriptor_schema='event'",[],|r|r.get(0)).unwrap();
        assert_eq!(scopes, "1,2");
    }
}

#[test]
fn changed_dictionary_descriptor_is_still_refused() {
    use bigbrotr_model::{DictionaryDescriptor, DictionaryEntry, EventId, FactPayload};
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let engine = open(
        temp.path(),
        &home.path().join("catalog.ducklake"),
        &home.path().join("objects"),
    );
    schema::initialize(&engine.connection).unwrap();
    let mut all = facts();
    let FactPayload::Dictionary(entry) = all[1].payload() else {
        panic!("dictionary fixture")
    };
    let conflict = DictionaryEntry::new(
        entry.reference(),
        DictionaryDescriptor::Event(EventId::from_bytes([0; 32])),
    )
    .unwrap();
    all[2] = Fact::new(
        all[2].source_record_id(),
        all[2].visibility_scope_id(),
        all[2].recorded_at(),
        FactPayload::Dictionary(conflict),
    )
    .unwrap();
    let (prepared, request) = prepare(engine, &all, &home.path().join("objects"));
    assert_eq!(
        prepared.publish_attached(&request),
        Err(PublicationError::ConflictingDictionary)
    );
}

#[test]
fn native_singleton_registration_expands_globs_but_private_input_refuses_them() {
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let engine = open(
        temp.path(),
        &home.path().join("catalog.ducklake"),
        &home.path().join("objects"),
    );
    let literal = home.path().join("[x]");
    let sibling = home.path().join("x");
    fs::create_dir(&literal).unwrap();
    fs::create_dir(&sibling).unwrap();
    engine.connection.execute_batch(&format!(
        "CREATE TABLE archive.canary (n UINTEGER); COPY (SELECT 11::UINTEGER AS n) TO '{}/file.parquet' (FORMAT PARQUET); COPY (SELECT 22::UINTEGER AS n) TO '{}/file.parquet' (FORMAT PARQUET); CALL ducklake_add_data_files('archive','canary',['{}/file.parquet'],hive_partitioning=>false,allow_missing=>false,ignore_extra_columns=>false)",
        literal.display(), sibling.display(), literal.display()
    )).unwrap();
    let actual: u32 = engine
        .connection
        .query_row("SELECT n FROM archive.canary", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        actual, 22,
        "native registration selected the glob match, not literal file11"
    );
    let wire = serde_json::to_vec(
        &serde_json::json!({"dsn":"unused","data_root":format!("{}/",literal.display())}),
    )
    .unwrap();
    assert!(matches!(
        BootstrapRequest::read(wire.as_slice()),
        Err(crate::catalog::CatalogError::InvalidInput)
    ));
}

#[test]
fn existing_attempt_files_and_symlinked_table_parent_are_never_reused() {
    for symlink in [0, 1, 2] {
        let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let data = home.path().join("objects");
        let engine = open(temp.path(), &home.path().join("catalog.ducklake"), &data);
        schema::initialize(&engine.connection).unwrap();
        let parent = data.join("main/dictionaries");
        let outside = home.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        if symlink == 2 {
            fs::create_dir_all(&data).unwrap();
            std::os::unix::fs::symlink(&outside, data.join("main")).unwrap();
        } else if symlink == 1 {
            fs::create_dir_all(data.join("main")).unwrap();
            std::os::unix::fs::symlink(&outside, &parent).unwrap();
        } else {
            let attempt = parent.join(format!("prepared-{}", "09".repeat(16)));
            fs::create_dir_all(&attempt).unwrap();
            fs::write(attempt.join("data.parquet"), b"existing-attempt-canary").unwrap();
        }
        let (prepared, request) = prepare(engine, &facts(), &data);
        assert_eq!(
            prepared.publish_attached(&request),
            Err(PublicationError::PreparedFiles)
        );
        let count: u64 = prepared
            .engine
            .connection
            .query_row("SELECT count(*) FROM archive.archive_commits", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        if symlink == 0 {
            assert_eq!(
                fs::read(parent.join(format!("prepared-{}/data.parquet", "09".repeat(16))))
                    .unwrap(),
                b"existing-attempt-canary"
            );
        }
    }
}

#[test]
fn native_copy_requires_explicit_direct_output_to_preserve_the_held_file() {
    use std::{fs::OpenOptions, io::Read, os::unix::fs::MetadataExt};
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let engine = open(
        temp.path(),
        &home.path().join("catalog.ducklake"),
        &home.path().join("objects"),
    );
    for direct in [false, true] {
        let path = home.path().join(format!("copy-{direct}.parquet"));
        let mut held = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        engine.connection.execute_batch(&format!(
            "COPY (SELECT 18446744073709551615::UBIGINT AS n, 'é' AS body) TO '{}' (FORMAT PARQUET{})",
            path.display(), if direct { ", USE_TMP_FILE false" } else { "" }
        )).unwrap();
        let original = held.metadata().unwrap();
        let named = fs::metadata(&path).unwrap();
        assert_eq!(
            (original.dev(), original.ino()) == (named.dev(), named.ino()),
            direct,
            "default COPY replaces the precreated inode; direct output preserves it"
        );
        assert_eq!(original.nlink(), u64::from(direct));
        let mut bytes = Vec::new();
        held.read_to_end(&mut bytes).unwrap();
        if direct {
            assert_eq!(bytes, fs::read(&path).unwrap());
            assert!(bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"));
            held.sync_all().unwrap();
        } else {
            assert!(bytes.is_empty());
        }
        let row: (u64, String) = engine
            .connection
            .query_row(
                &format!("SELECT n,body FROM read_parquet('{}')", path.display()),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, (u64::MAX, "é".into()));
    }
}

#[test]
fn replaced_or_aliased_output_is_not_the_held_original_file() {
    use std::io::Write;
    for fault in ["empty", "replaced", "symlink", "hardlink"] {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("data.parquet");
        let mut held = File::create_new(&path).unwrap();
        assert_eq!(
            require_same_file(&held, &path),
            Err(PublicationError::PreparedFiles)
        );
        if fault != "empty" {
            held.write_all(b"completed-native-output").unwrap();
            require_same_file(&held, &path).unwrap();
            let prior = home.path().join("prior");
            if fault == "hardlink" {
                fs::hard_link(&path, &prior).unwrap();
            } else {
                fs::rename(&path, &prior).unwrap();
                if fault == "symlink" {
                    std::os::unix::fs::symlink(&prior, &path).unwrap();
                } else {
                    fs::write(&path, b"completed-native-output").unwrap();
                }
            }
        }
        assert_eq!(
            require_same_file(&held, &path),
            Err(PublicationError::PreparedFiles)
        );
    }
}

/// Invoked under native syscall fault injection in an isolated Linux process.
/// No production hook or environment-controlled behavior is introduced.
#[test]
#[ignore = "requires isolated strace fault injection and an exclusive fixture root"]
fn native_sync_refusal_prevents_committed_membership_and_attempt_reuse() {
    let home =
        std::path::PathBuf::from(std::env::var_os("BIGBROTR_ARCHIVE_TEST_SYNC_HOME").unwrap());
    assert!(home.is_absolute());
    fs::create_dir(&home).unwrap();
    let temp = home.join("temp");
    fs::create_dir(&temp).unwrap();
    let catalog = home.join("catalog.ducklake");
    let data = home.join("objects");
    let engine = open(&temp, &catalog, &data);
    schema::initialize(&engine.connection).unwrap();
    let (prepared, request) = prepare(engine, &facts(), &data);
    assert_eq!(
        prepared.publish_attached(&request),
        Err(PublicationError::PreparedFiles)
    );
    drop(prepared);
    let engine = open(&temp, &catalog, &data);
    for table in schema::TABLES.into_iter().chain(["archive_commits"]) {
        let count: u64 = engine
            .connection
            .query_row(&format!("SELECT count(*) FROM archive.{table}"), [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "failed synchronization cannot publish {table}");
    }
    let metadata = duckdb::Connection::open(&catalog).unwrap();
    let files: u64 = metadata
        .query_row("SELECT count(*) FROM ducklake_data_file", [], |r| r.get(0))
        .unwrap();
    assert_eq!(files, 0);
    drop(metadata);
    let mut retained = Vec::new();
    for (table, expected) in [
        ("dictionaries", 2u64),
        ("events", 1),
        ("archive_commits", 1),
    ] {
        let path = data.join(format!(
            "main/{table}/prepared-{}/data.parquet",
            "09".repeat(16)
        ));
        if path.exists() {
            let count: u64 = engine
                .connection
                .query_row(
                    &format!("SELECT count(*) FROM read_parquet('{}')", path.display()),
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, expected);
            retained.push((path.clone(), fs::read(&path).unwrap()));
        }
    }
    assert!(
        !retained.is_empty(),
        "the injected fault follows completed COPY"
    );
    let (prepared, request) = prepare(engine, &facts(), &data);
    assert_eq!(
        prepared.publish_attached(&request),
        Err(PublicationError::PreparedFiles)
    );
    drop(prepared);
    for (path, bytes) in retained {
        assert_eq!(
            fs::read(path).unwrap(),
            bytes,
            "refused retry preserves original output"
        );
    }
}

#[test]
fn native_membership_requires_complete_source_and_original_files_in_one_snapshot() {
    use crate::membership::inspect_attached;
    use bigbrotr_protocol::{
        archive_membership::MembershipRead, archive_receipt::ReceiptExpectation,
    };
    for fault in [
        "none",
        "delete",
        "duplicate",
        "kind",
        "extra-sequence",
        "extra-dataset",
        "altered-digest",
        "extra-receipt",
        "missing-file",
        "corrupt-file",
        "symlink-directory",
        "limit",
        "snapshot",
    ] {
        let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let catalog = home.path().join("catalog.ducklake");
        let data = home.path().join("objects");
        let engine = open(temp.path(), &catalog, &data);
        schema::initialize(&engine.connection).unwrap();
        let (prepared, request) = prepare(engine, &facts(), &data);
        let archive = ArchiveBatch::new(
            1,
            request.topology_generation,
            std::slice::from_ref(prepared.batch()),
        )
        .unwrap();
        let read = MembershipRead {
            receipt: ReceiptExpectation::from_archive(
                &archive,
                request.attempt_id,
                request.publication_ordinal,
            )
            .unwrap(),
            snapshot_id: Some(if fault == "snapshot" { 1 } else { 2 }),
        };
        prepared.publish_attached(&request).unwrap();
        drop(prepared);
        let engine = open(temp.path(), &catalog, &data);
        match fault {
            "delete" => engine
                .connection
                .execute_batch("DELETE FROM archive.events")
                .unwrap(),
            "duplicate" => engine
                .connection
                .execute_batch("INSERT INTO archive.events SELECT * FROM archive.events")
                .unwrap(),
            "kind" => engine
                .connection
                .execute_batch("UPDATE archive.events SET record_kind='dictionary'")
                .unwrap(),
            "extra-sequence" | "extra-dataset" | "altered-digest" | "extra-receipt" => {
                let table = if fault == "extra-receipt" {
                    "archive_commits"
                } else {
                    "events"
                };
                let file = data.join(format!(
                    "main/{table}/prepared-{}/data.parquet",
                    "09".repeat(16)
                ));
                let replacement = file.with_extension("replacement");
                let altered = match fault {
                    "extra-sequence" => {
                        "struct_update(source_record_id, sequence := 999::UBIGINT) AS source_record_id"
                    }
                    "extra-dataset" => {
                        "struct_update(source_record_id, dataset_id := from_hex(repeat('02',16))) AS source_record_id"
                    }
                    "altered-digest" => "from_hex(repeat('ff',32)) AS payload_digest",
                    "extra-receipt" => {
                        "from_hex(repeat('ff',32)) AS batch_digest,from_hex(repeat('0a',16)) AS attempt_id,1::UBIGINT AS publication_ordinal"
                    }
                    _ => unreachable!(),
                };
                let extra = if fault == "altered-digest" {
                    String::new()
                } else {
                    format!(
                        "SELECT * FROM read_parquet('{}') UNION ALL ",
                        file.display()
                    )
                };
                engine.connection.execute_batch(&format!(
                    "COPY ({extra}SELECT * REPLACE ({altered}) FROM read_parquet('{}')) TO '{}' (FORMAT PARQUET)",
                    file.display(), replacement.display()
                )).unwrap();
                fs::rename(replacement, file).unwrap();
            }
            "missing-file" | "corrupt-file" => {
                let file = data.join(format!(
                    "main/events/prepared-{}/data.parquet",
                    "09".repeat(16)
                ));
                if fault == "missing-file" {
                    fs::rename(&file, file.with_extension("retained")).unwrap();
                } else {
                    fs::write(&file, b"not a parquet object").unwrap();
                }
            }
            "symlink-directory" => {
                let directory = data.join(format!("main/events/prepared-{}", "09".repeat(16)));
                let relocated = home.path().join("relocated-attempt");
                fs::rename(&directory, &relocated).unwrap();
                std::os::unix::fs::symlink(&relocated, &directory).unwrap();
            }
            _ => (),
        }
        // A receipt-only observation still reports Present after deleting the
        // event. The new census must not promote that into complete membership.
        if fault == "delete" {
            assert_eq!(
                crate::receipt::inspect_attached(&engine.connection, &read.receipt, 4).unwrap(),
                crate::receipt::ReceiptObservation::Present
            );
        }
        let mut read = read;
        if matches!(fault, "delete" | "duplicate" | "kind") {
            read.snapshot_id = Some(3);
        }
        drop(engine);
        if matches!(
            fault,
            "extra-sequence" | "extra-dataset" | "altered-digest" | "extra-receipt"
        ) {
            // Fixture-only corruption must remain natively readable. DuckLake
            // supplies recorded file/footer sizes to the Parquet reader; repair
            // transport metadata so a footer refusal cannot mask the census.
            let table = if fault == "extra-receipt" {
                "archive_commits"
            } else {
                "events"
            };
            let file = data.join(format!(
                "main/{table}/prepared-{}/data.parquet",
                "09".repeat(16)
            ));
            let bytes = fs::read(&file).unwrap();
            let footer =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
            let metadata = duckdb::Connection::open(&catalog).unwrap();
            assert_eq!(metadata.execute(
                "UPDATE ducklake_data_file SET file_size_bytes=?,footer_size=?,record_count=? WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE table_name=?)",
                duckdb::params![bytes.len() as u64, footer, if fault == "altered-digest" {1} else {2}, table]
            ).unwrap(), 1);
        }
        let engine = open(temp.path(), &catalog, &data);
        let actual_snapshot: u64 = engine
            .connection
            .query_row(
                "SELECT id FROM ducklake_current_snapshot('archive')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            actual_snapshot,
            if matches!(fault, "delete" | "duplicate" | "kind") {
                3
            } else {
                2
            }
        );
        if matches!(fault, "extra-sequence" | "extra-dataset" | "altered-digest") {
            let actual: String = engine.connection.query_row(
                "SELECT to_json(list(struct_pack(source:=source_record_id,digest:=hex(payload_digest)))) FROM archive.events", [], |r|r.get(0)
            ).unwrap();
            let rows: serde_json::Value = serde_json::from_str(&actual).unwrap();
            assert_eq!(
                rows.as_array().unwrap().len(),
                if fault == "altered-digest" { 1 } else { 2 }
            );
            eprintln!("{fault} actual scientific columns: {actual}");
        }
        if fault == "extra-receipt" {
            assert_eq!(
                crate::receipt::inspect_attached(&engine.connection, &read.receipt, 4).unwrap(),
                crate::receipt::ReceiptObservation::Present
            );
        }
        let result = inspect_attached(
            &engine.connection,
            &read,
            &data,
            4,
            if fault == "limit" { 2 } else { 3 },
        );
        if fault == "none" {
            let observed = result.unwrap();
            assert_eq!(observed.snapshot_id, 2);
            assert_eq!(observed.family_counts, [2, 1, 0, 0, 0]);
            assert_eq!(observed.files, 3);
            observed.validate(&read).unwrap();
        } else {
            eprintln!("membership {fault}: {result:?}");
            assert!(result.is_err(), "{fault}: {result:?}");
            if fault == "limit" {
                assert_eq!(
                    result.unwrap_err(),
                    crate::receipt::ReceiptError::ResourceLimit
                );
            }
        }
    }
}

#[test]
fn native_complete_cut_needs_dictionary_inside_its_own_stream_bound() {
    use bigbrotr_protocol::{
        archive_cut::{CutBound, CutRead},
        archive_receipt::ReceiptExpectation,
    };
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let catalog = home.path().join("catalog.ducklake");
    let data = home.path().join("objects");
    let all = facts();
    let definitions: Vec<_> = all[..2]
        .iter()
        .map(|f| {
            Fact::new(
                bigbrotr_model::SourceRecordId {
                    shard_id: 2,
                    ..f.source_record_id()
                },
                f.visibility_scope_id(),
                f.recorded_at(),
                f.payload().clone(),
            )
            .unwrap()
        })
        .collect();
    let body = Fact::new(
        bigbrotr_model::SourceRecordId {
            sequence: Sequence::new(1).unwrap(),
            ..all[2].source_record_id()
        },
        all[2].visibility_scope_id(),
        all[2].recorded_at(),
        all[2].payload().clone(),
    )
    .unwrap();
    let engine = open(temp.path(), &catalog, &data);
    schema::initialize(&engine.connection).unwrap();
    let mut receipts = Vec::new();
    let (prepared, mut request) = prepare(engine, &definitions, &data);
    request.attempt_id = [8; 16];
    request.publication_ordinal = 2;
    let archive = ArchiveBatch::new(
        1,
        request.topology_generation,
        std::slice::from_ref(prepared.batch()),
    )
    .unwrap();
    receipts.push(
        ReceiptExpectation::from_archive(&archive, request.attempt_id, request.publication_ordinal)
            .unwrap(),
    );
    prepared.publish_attached(&request).unwrap();
    drop(prepared);
    let engine = open(temp.path(), &catalog, &data);
    let (prepared, mut request) = prepare(engine, &[body], &data);
    request.publication_ordinal = 5;
    let archive = ArchiveBatch::new(
        1,
        request.topology_generation,
        std::slice::from_ref(prepared.batch()),
    )
    .unwrap();
    receipts.push(
        ReceiptExpectation::from_archive(&archive, request.attempt_id, request.publication_ordinal)
            .unwrap(),
    );
    prepared.publish_attached(&request).unwrap();
    drop(prepared);
    let complete = CutRead {
        dataset_id: all[0].source_record_id().dataset_id,
        topology_generation: TopologyGeneration::new(u64::MAX),
        bounds: vec![
            CutBound {
                shard_id: 0,
                owner_epoch: 1,
                complete_through: 1,
            },
            CutBound {
                shard_id: 1,
                owner_epoch: 1,
                complete_through: 0,
            },
            CutBound {
                shard_id: 2,
                owner_epoch: 1,
                complete_through: 2,
            },
        ],
        receipts,
        snapshot_id: Some(3),
    };
    for case in [
        "complete",
        "future-dictionary",
        "zero",
        "missing-prefix",
        "partial",
        "limit",
        "snapshot",
    ] {
        let mut read = complete.clone();
        match case {
            "future-dictionary" => {
                read.bounds[2].complete_through = 0;
                read.receipts.remove(0);
            }
            "zero" => {
                read.receipts.clear();
                for bound in &mut read.bounds {
                    bound.complete_through = 0;
                }
            }
            "missing-prefix" => {
                read.receipts.remove(0);
            }
            "partial" => read.bounds[2].complete_through = 1,
            "snapshot" => read.snapshot_id = Some(2),
            _ => (),
        }
        let engine = open(temp.path(), &catalog, &data);
        // The negative fixture has complete current catalog membership. Only
        // applying the requested source cut makes its dictionaries unavailable.
        if case == "future-dictionary" {
            let refs:String=engine.connection.query_row("SELECT string_agg(source_record_id.sequence::VARCHAR,',' ORDER BY source_record_id.sequence) FROM archive.dictionaries",[],|r|r.get(0)).unwrap();
            assert_eq!(refs, "1,2");
            assert_eq!(
                crate::membership::inspect_attached(
                    &engine.connection,
                    &bigbrotr_protocol::archive_membership::MembershipRead {
                        receipt: read.receipts[0].clone(),
                        snapshot_id: Some(3)
                    },
                    &data,
                    4,
                    3
                )
                .unwrap()
                .family_counts,
                [0, 1, 0, 0, 0]
            );
        }
        let result = crate::cut::inspect_attached(
            &engine.connection,
            &read,
            &data,
            4,
            if case == "limit" { 2 } else { 3 },
        );
        eprintln!("complete cut {case}: {result:?}");
        if matches!(case, "complete" | "zero") {
            let observed = result.unwrap();
            assert_eq!(observed.snapshot_id, 3);
            assert_eq!(
                observed.family_counts,
                if case == "zero" {
                    [0; 5]
                } else {
                    [2, 1, 0, 0, 0]
                }
            );
            observed.validate(&read, 3).unwrap();
        } else {
            assert!(result.is_err(), "{case}: {result:?}");
        }
    }
}

#[test]
fn native_cut_closure_checks_every_current_family_and_correction_target() {
    use bigbrotr_protocol::{
        archive_cut::{CutBound, CutRead},
        archive_receipt::ReceiptExpectation,
    };
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/storage/archive-cut.json"
    ))
    .unwrap();
    let all: Vec<_> = fixture["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| decode(f, "digest"))
        .collect();
    let signed: bigbrotr_model::SignedEvent =
        serde_json::from_value(fixture["signed_event"].clone()).unwrap();
    bigbrotr_protocol::verify_owned(signed, 65536).unwrap();
    let home = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let catalog = home.path().join("catalog.ducklake");
    let data = home.path().join("objects");
    let engine = open(temp.path(), &catalog, &data);
    schema::initialize(&engine.connection).unwrap();
    let (prepared, mut request) = prepare(engine, &all, &data);
    request.topology_generation = TopologyGeneration::new(0);
    request.batch_digest = fixture["archive_digest"].as_str().unwrap().parse().unwrap();
    let archive = ArchiveBatch::new(
        1,
        request.topology_generation,
        std::slice::from_ref(prepared.batch()),
    )
    .unwrap();
    assert_eq!(archive.digest(), request.batch_digest);
    let read = CutRead {
        dataset_id: all[0].source_record_id().dataset_id,
        topology_generation: TopologyGeneration::new(0),
        bounds: vec![CutBound {
            shard_id: 0,
            owner_epoch: 1,
            complete_through: 12,
        }],
        receipts: vec![
            ReceiptExpectation::from_archive(
                &archive,
                request.attempt_id,
                request.publication_ordinal,
            )
            .unwrap(),
        ],
        snapshot_id: Some(2),
    };
    prepared.publish_attached(&request).unwrap();
    drop(prepared);
    let engine = open(temp.path(), &catalog, &data);
    assert_eq!(
        crate::cut::inspect_attached(&engine.connection, &read, &data, 4, 12)
            .unwrap()
            .family_counts,
        [6, 1, 1, 2, 2]
    );
    drop(engine);
    // Each mutation is uncommitted native SQL in its own transaction. Exercise
    // the reference checker directly so original-file or stored-digest refusal
    // cannot mask a missing dependency predicate. No mutation is persisted.
    for (case, sql) in [
        (
            "visibility",
            "UPDATE archive.events SET visibility_scope_id=struct_update(visibility_scope_id,ordinal:=2::UBIGINT)",
        ),
        (
            "event-mapping",
            "UPDATE archive.dictionaries SET descriptor_value=struct_update(descriptor_value,event_id:=from_hex(repeat('ff',32))) WHERE descriptor_schema='event'",
        ),
        (
            "payload",
            "UPDATE archive.event_eligibility_facts SET event_id=from_hex(repeat('ff',32))",
        ),
        (
            "eligibility-scope",
            "UPDATE archive.event_eligibility_facts SET visibility_scope_id=struct_update(visibility_scope_id,ordinal:=2::UBIGINT)",
        ),
        (
            "observer",
            "UPDATE archive.event_receive_facts SET observer_id=struct_update(observer_id,ordinal:=2::UBIGINT)",
        ),
        (
            "clock",
            "UPDATE archive.event_receive_facts SET clock_quality=struct_update(clock_quality,clock_id:=struct_update(clock_quality.clock_id,ordinal:=2::UBIGINT))",
        ),
        (
            "endpoint",
            "UPDATE archive.provenance_facts SET endpoint_id=struct_update(endpoint_id,ordinal:=2::UBIGINT)",
        ),
        (
            "access",
            "UPDATE archive.provenance_facts SET access_scope_id=struct_update(access_scope_id,ordinal:=2::UBIGINT)",
        ),
        (
            "method",
            "UPDATE archive.provenance_facts SET verification_method_id=struct_update(verification_method_id,ordinal:=2::UBIGINT)",
        ),
        (
            "import-lineage",
            "UPDATE archive.event_eligibility_facts SET origin='import'",
        ),
        (
            "descriptor-dependency",
            "UPDATE archive.dictionaries SET descriptor_value=struct_update(descriptor_value,visibility_scope_id:=struct_update(descriptor_value.visibility_scope_id,ordinal:=2::UBIGINT)) WHERE descriptor_schema='access-scope'",
        ),
        (
            "receive-corrects-family",
            "UPDATE archive.event_receive_facts SET corrects=struct_update(corrects,sequence:=10::UBIGINT) WHERE corrects IS NOT NULL",
        ),
        (
            "provenance-corrects-key",
            "UPDATE archive.provenance_facts SET corrects=struct_update(corrects,sequence:=9::UBIGINT) WHERE corrects IS NOT NULL",
        ),
        (
            "non-earlier-correction",
            "UPDATE archive.event_receive_facts SET received_at=10000001 WHERE corrects IS NOT NULL",
        ),
        (
            "outside-cut-target",
            "UPDATE archive.provenance_facts SET corrects=struct_update(corrects,sequence:=13::UBIGINT) WHERE corrects IS NOT NULL",
        ),
    ] {
        let engine = open(temp.path(), &catalog, &data);
        engine
            .connection
            .execute_batch("BEGIN TRANSACTION")
            .unwrap();
        assert!(
            engine.connection.execute(sql, []).unwrap() > 0,
            "{case} must mutate actual native rows"
        );
        let result = crate::cut::reference_closure(&engine.connection, &read, [6, 1, 1, 2, 2]);
        assert_eq!(
            result,
            Err(crate::receipt::ReceiptError::Mismatch),
            "{case}: {result:?}"
        );
        engine.connection.execute_batch("ROLLBACK").unwrap();
    }
    for table in ["event_receive_facts", "provenance_facts"] {
        let engine = open(temp.path(), &catalog, &data);
        let c = &engine.connection;
        c.execute_batch("BEGIN TRANSACTION").unwrap();
        // Reverse the two correction edges without breaking the family, key or
        // decreasing timestamp. Only same-stream source order is invalid.
        let changed = c.execute(&format!(
            "UPDATE archive.{table} SET received_at=CASE WHEN corrects IS NULL THEN 4000000 ELSE received_at END, corrects=CASE WHEN corrects IS NULL THEN struct_update(source_record_id,sequence:=source_record_id.sequence+2) ELSE NULL END"
        ), []).unwrap();
        assert_eq!(changed, 2);
        let matching: u64 = c.query_row(&format!(
            "SELECT count(*) FROM archive.{table} f JOIN archive.{table} p ON p.source_record_id=f.corrects AND p.event_id=f.event_id AND p.visibility_scope_id=f.visibility_scope_id AND p.received_at>f.received_at WHERE f.corrects.sequence>f.source_record_id.sequence"
        ), [], |r| r.get(0)).unwrap();
        assert_eq!(
            matching, 1,
            "{table}: valid target and timestamp must exist"
        );
        assert_eq!(
            crate::cut::reference_closure(c, &read, [6, 1, 1, 2, 2]),
            Err(crate::receipt::ReceiptError::Mismatch),
            "{table}: future same-stream correction"
        );
        c.execute_batch("ROLLBACK").unwrap();

        for different in ["shard_id:=1::UINTEGER", "owner_epoch:=2::UBIGINT"] {
            let engine = open(temp.path(), &catalog, &data);
            let c = &engine.connection;
            c.execute_batch("BEGIN TRANSACTION").unwrap();
            let changed = c.execute(&format!(
                "UPDATE archive.{table} SET source_record_id=CASE WHEN corrects IS NULL THEN struct_update(source_record_id,{different}) ELSE source_record_id END, received_at=CASE WHEN corrects IS NULL THEN 4000000 ELSE received_at END, corrects=CASE WHEN corrects IS NULL THEN struct_update(source_record_id,sequence:=source_record_id.sequence+2) ELSE NULL END"
            ), []).unwrap();
            assert_eq!(changed, 2);
            let mut projected = read.clone();
            projected.bounds.push(CutBound {
                shard_id: if different.starts_with("shard_id") {
                    1
                } else {
                    0
                },
                owner_epoch: if different.starts_with("owner_epoch") {
                    2
                } else {
                    1
                },
                complete_through: 12,
            });
            // This helper tests only projected reference closure, deliberately
            // bypassing original-file census and the now-changed source ranges.
            assert_eq!(
                crate::cut::reference_closure(c, &projected, [6, 1, 1, 2, 2]),
                Ok(()),
                "{table}: sequence order must not extend across {different}"
            );
            c.execute_batch("ROLLBACK").unwrap();
        }
    }
    let engine = open(temp.path(), &catalog, &data);
    assert_eq!(
        crate::cut::inspect_attached(&engine.connection, &read, &data, 4, 12)
            .unwrap()
            .family_counts,
        [6, 1, 1, 2, 2]
    );
}
