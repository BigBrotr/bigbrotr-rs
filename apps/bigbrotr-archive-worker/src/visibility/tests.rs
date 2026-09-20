use crate::{
    engine::{Engine, EngineLimits},
    rows::{RowLimits, Rows},
    schema,
};
use bigbrotr_model::{Fact, FactPayload, codec::FactDecodeLimits};
use bigbrotr_protocol::{
    archive_cut::CutBound,
    archive_visibility::{VisibilityRead, VisibilityReadError as Error},
};
use std::path::Path;

fn fixture() -> Fact {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../crates/bigbrotr-model/tests/fixtures/dictionary-v1.json"
    ))
    .unwrap();
    let v = data["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["family"] == "fact" && v["name"] == "visibility-public")
        .unwrap();
    let bytes: Vec<_> = v["preimage_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    Fact::decode_preimage(
        &bytes,
        v["sha256"].as_str().unwrap().parse().unwrap(),
        FactDecodeLimits::default(),
    )
    .unwrap()
}
fn request(snapshot_id: u64) -> VisibilityRead {
    serde_json::from_value(serde_json::json!({
        "dataset":"01".repeat(16), "scope":{"dataset_id":"01".repeat(16),"dictionary_kind":4,"allocation_domain":"02".repeat(16),"ordinal":"1"},
        "topology_generation":"0", "bounds":[{"shard_id":0,"owner_epoch":1,"complete_through":1}], "snapshot_id":snapshot_id
    })).unwrap()
}
fn engine(home: &Path, temp: &Path, snapshot: Option<u64>) -> Engine {
    let e = Engine::open(
        &temp.canonicalize().unwrap(),
        EngineLimits {
            memory_bytes: 134217728,
            temp_bytes: 67108864,
            threads: 1,
        },
    )
    .unwrap();
    e.load_bundle(Path::new(
        &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned bundle"),
    ))
    .unwrap();
    let options = match snapshot {
        Some(id) => crate::receipt::readonly_archive_options(
            home.join("objects").to_str().unwrap(),
            Some(id),
        )
        .unwrap(),
        None => format!(
            "DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false",
            home.join("objects").display()
        ),
    };
    e.connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:{}' AS archive ({options})",
            home.join("catalog.ducklake").display()
        ))
        .unwrap();
    e
}
fn seed(home: &Path) {
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home, temp.path(), None);
    schema::initialize(&e.connection).unwrap();
    schema::create_stage(&e.connection).unwrap();
    Rows::dictionaries(
        &[&fixture()],
        RowLimits {
            max_rows: 1,
            max_text_bytes: 4096,
            max_tag_nodes: 0,
        },
    )
    .unwrap()
    .append(
        &mut e
            .connection
            .appender_to_db("dictionaries", "stage")
            .unwrap(),
    )
    .unwrap();
    e.connection
        .execute_batch("INSERT INTO archive.dictionaries SELECT * FROM stage.dictionaries")
        .unwrap();
}
fn lookup(home: &Path, request: &VisibilityRead) -> Result<Fact, Error> {
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home, temp.path(), Some(request.snapshot_id));
    crate::visibility::lookup_attached(&e.connection, request)?.verify(request)
}

#[test]
fn visibility_lookup_requires_allocated_mapping_in_the_selected_prefix() {
    let home = tempfile::tempdir().unwrap();
    seed(home.path());
    let request = request(2);
    assert_eq!(lookup(home.path(), &request).unwrap(), fixture());
    for case in ["zero", "missing", "epoch", "domain", "ordinal"] {
        let mut absent = request.clone();
        match case {
            "zero" => absent.bounds[0].complete_through = 0,
            "missing" => absent.bounds.clear(),
            "epoch" => absent.bounds[0].owner_epoch = 2,
            "domain" => absent.scope.allocation_domain = [3; 16].try_into().unwrap(),
            _ => absent.scope.ordinal = bigbrotr_model::Ordinal::new(2).unwrap(),
        }
        assert_eq!(lookup(home.path(), &absent), Err(Error::Missing), "{case}");
    }
    let mut more = request.clone();
    more.bounds.push(CutBound {
        shard_id: 1,
        owner_epoch: 1,
        complete_through: 0,
    });
    assert_eq!(lookup(home.path(), &more).unwrap(), fixture());
}

#[test]
fn visibility_lookup_keeps_historical_snapshot_and_full_mapping_identity() {
    let home = tempfile::tempdir().unwrap();
    seed(home.path());
    let original = fixture();
    let FactPayload::Dictionary(entry) = original.payload() else {
        panic!()
    };
    let reference = bigbrotr_model::QualifiedRef {
        allocation_domain: [3; 16].try_into().unwrap(),
        ..entry.reference()
    };
    let other = Fact::new(
        bigbrotr_model::SourceRecordId {
            sequence: bigbrotr_model::Sequence::new(2).unwrap(),
            ..original.source_record_id()
        },
        original.visibility_scope_id(),
        original.recorded_at(),
        FactPayload::Dictionary(
            bigbrotr_model::DictionaryEntry::new(
                reference,
                bigbrotr_model::DictionaryDescriptor::Visibility {
                    class: bigbrotr_model::DescriptorLabel::new("private".into()).unwrap(),
                },
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home.path(), temp.path(), None);
    schema::create_stage(&e.connection).unwrap();
    Rows::dictionaries(
        &[&other],
        RowLimits {
            max_rows: 1,
            max_text_bytes: 4096,
            max_tag_nodes: 0,
        },
    )
    .unwrap()
    .append(
        &mut e
            .connection
            .appender_to_db("dictionaries", "stage")
            .unwrap(),
    )
    .unwrap();
    e.connection
        .execute_batch("INSERT INTO archive.dictionaries SELECT * FROM stage.dictionaries")
        .unwrap();
    drop(e);
    let mut selected = request(3);
    selected.scope = reference;
    selected.bounds[0].complete_through = 2;
    assert_eq!(lookup(home.path(), &selected).unwrap(), other);
    assert_eq!(lookup(home.path(), &request(3)).unwrap(), original);
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home.path(), temp.path(), None);
    e.connection
        .execute_batch("DELETE FROM archive.dictionaries")
        .unwrap();
    drop(e);
    assert_eq!(lookup(home.path(), &request(2)).unwrap(), original);
    assert_eq!(lookup(home.path(), &request(4)), Err(Error::Missing));
}

#[test]
fn visibility_lookup_refuses_corrupt_envelopes_versions_and_duplicate_mappings() {
    for mutation in [
        "UPDATE archive.dictionaries SET recorded_at=0",
        "UPDATE archive.dictionaries SET payload_digest=from_hex(repeat('00',32))",
        "UPDATE archive.dictionaries SET record_schema_version=2",
        "UPDATE archive.dictionaries SET record_kind='event-payload'",
        "UPDATE archive.dictionaries SET normalization_version=2",
        "UPDATE archive.dictionaries SET descriptor_version=2",
        "UPDATE archive.dictionaries SET descriptor_schema='access-scope'",
        "UPDATE archive.dictionaries SET descriptor_value=struct_update(descriptor_value,class:='private')",
        "UPDATE archive.dictionaries SET descriptor_value=struct_update(descriptor_value,label:='unexpected')",
        "INSERT INTO archive.dictionaries SELECT * FROM archive.dictionaries",
    ] {
        let home = tempfile::tempdir().unwrap();
        seed(home.path());
        let temp = tempfile::tempdir().unwrap();
        let e = engine(home.path(), temp.path(), None);
        e.connection.execute_batch(mutation).unwrap();
        drop(e);
        assert_eq!(
            lookup(home.path(), &request(3)),
            Err(Error::Mismatch),
            "{mutation}"
        );
        // Corruption at the later view cannot change the selected old mapping.
        assert_eq!(lookup(home.path(), &request(2)).unwrap(), fixture());
    }
}

#[test]
fn visibility_lookup_rejects_one_source_identity_assigned_to_two_mappings() {
    let home = tempfile::tempdir().unwrap();
    seed(home.path());
    let original = fixture();
    let mut alternate = request(3);
    alternate.scope.allocation_domain = [3; 16].try_into().unwrap();
    let other = Fact::new(
        original.source_record_id(),
        original.visibility_scope_id(),
        original.recorded_at(),
        FactPayload::Dictionary(
            bigbrotr_model::DictionaryEntry::new(
                alternate.scope,
                bigbrotr_model::DictionaryDescriptor::Visibility {
                    class: bigbrotr_model::DescriptorLabel::new("private".into()).unwrap(),
                },
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home.path(), temp.path(), None);
    schema::create_stage(&e.connection).unwrap();
    Rows::dictionaries(
        &[&other],
        RowLimits {
            max_rows: 1,
            max_text_bytes: 4096,
            max_tag_nodes: 0,
        },
    )
    .unwrap()
    .append(
        &mut e
            .connection
            .appender_to_db("dictionaries", "stage")
            .unwrap(),
    )
    .unwrap();
    e.connection.execute_batch("CREATE TEMP TABLE original AS SELECT * FROM archive.dictionaries; BEGIN; DELETE FROM archive.dictionaries; INSERT INTO archive.dictionaries SELECT * FROM stage.dictionaries; COMMIT; INSERT INTO archive.dictionaries SELECT * FROM original").unwrap();
    drop(e);
    // Each alternative is a complete valid mapping in its isolated snapshot.
    assert_eq!(lookup(home.path(), &request(2)).unwrap(), original);
    assert_eq!(lookup(home.path(), &alternate).unwrap(), other);
    // Filtering by QualifiedRef first must not hide the conflicting source row.
    assert_eq!(lookup(home.path(), &request(4)), Err(Error::Mismatch));
    alternate.snapshot_id = 4;
    assert_eq!(lookup(home.path(), &alternate), Err(Error::Mismatch));
}

#[test]
fn visibility_lookup_accepts_equal_mappings_from_distinct_source_facts() {
    let home = tempfile::tempdir().unwrap();
    seed(home.path());
    let original = fixture();
    let repeated = Fact::new(
        bigbrotr_model::SourceRecordId {
            sequence: bigbrotr_model::Sequence::new(2).unwrap(),
            ..original.source_record_id()
        },
        original.visibility_scope_id(),
        original.recorded_at(),
        original.payload().clone(),
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home.path(), temp.path(), None);
    schema::create_stage(&e.connection).unwrap();
    Rows::dictionaries(
        &[&repeated],
        RowLimits {
            max_rows: 1,
            max_text_bytes: 4096,
            max_tag_nodes: 0,
        },
    )
    .unwrap()
    .append(
        &mut e
            .connection
            .appender_to_db("dictionaries", "stage")
            .unwrap(),
    )
    .unwrap();
    e.connection
        .execute_batch("INSERT INTO archive.dictionaries SELECT * FROM stage.dictionaries")
        .unwrap();
    drop(e);
    let mut selected = request(3);
    selected.bounds[0].complete_through = 2;
    // The certified-cut contract admits an identical complete mapping under a
    // different source identity. The reader must not confuse it with a conflict.
    assert_eq!(lookup(home.path(), &selected).unwrap(), original);
    let conflicting = Fact::new(
        bigbrotr_model::SourceRecordId {
            sequence: bigbrotr_model::Sequence::new(3).unwrap(),
            ..original.source_record_id()
        },
        original.visibility_scope_id(),
        original.recorded_at(),
        FactPayload::Dictionary(
            bigbrotr_model::DictionaryEntry::new(
                selected.scope,
                bigbrotr_model::DictionaryDescriptor::Visibility {
                    class: bigbrotr_model::DescriptorLabel::new("private".into()).unwrap(),
                },
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = engine(home.path(), temp.path(), None);
    schema::create_stage(&e.connection).unwrap();
    Rows::dictionaries(
        &[&conflicting],
        RowLimits {
            max_rows: 1,
            max_text_bytes: 4096,
            max_tag_nodes: 0,
        },
    )
    .unwrap()
    .append(
        &mut e
            .connection
            .appender_to_db("dictionaries", "stage")
            .unwrap(),
    )
    .unwrap();
    e.connection
        .execute_batch("INSERT INTO archive.dictionaries SELECT * FROM stage.dictionaries")
        .unwrap();
    drop(e);
    selected.snapshot_id = 4;
    assert_eq!(lookup(home.path(), &selected).unwrap(), original);
    selected.bounds[0].complete_through = 3;
    assert_eq!(lookup(home.path(), &selected), Err(Error::Mismatch));
}
