//! Independent native readback oracle for the isolated signed publisher cases.
//! Fixed queries only. This is not the product Read role or an export format.
use bigbrotr_model::{NostrTime, SignedEvent};
#[path = "support/visibility_fixture.rs"]
mod visibility_fixture;
use duckdb::{Config, Connection};
use std::{collections::BTreeMap, fs};
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(matches!(args.len(), 2 | 3));
    let mode = &args[1];
    assert!(matches!(
        mode.as_str(),
        "normal"
            | "history"
            | "before"
            | "after"
            | "owner_first"
            | "held_commit"
            | "held_rollback"
            | "row_exact"
            | "row_over"
    ));
    let native_row = matches!(mode.as_str(), "row_exact" | "row_over");
    if native_row {
        assert_eq!(args.len(), 2);
    }
    let mutation = args
        .get(2)
        .filter(|value| matches!(value.as_str(), "visibility-repeat" | "visibility-conflict"));
    if mutation.is_some() {
        assert_eq!(mode, "normal");
    }
    let bootstrap_only = args.len() == 3 && mutation.is_none();
    if bootstrap_only {
        assert_eq!(args[2], "bootstrap");
        assert!(matches!(mode.as_str(), "owner_first" | "held_rollback"));
    }
    let temp = tempfile::tempdir_in("/out").unwrap();
    let config = Config::default()
        .enable_autoload_extension(false)
        .unwrap()
        .threads(1)
        .unwrap()
        .max_memory("128MiB")
        .unwrap()
        .with("allow_persistent_secrets", "false")
        .unwrap()
        .with("allow_unsigned_extensions", "false")
        .unwrap()
        .with("allow_community_extensions", "false")
        .unwrap()
        .with("temp_directory", temp.path().to_str().unwrap())
        .unwrap()
        .with("max_temp_directory_size", "64MiB")
        .unwrap();
    let c = Connection::open_in_memory_with_flags(config).unwrap();
    for name in ["httpfs", "postgres_scanner", "ducklake"] {
        c.execute_batch(&format!(
            "LOAD '/out/archive-worker/extensions/{name}.duckdb_extension'"
        ))
        .unwrap();
    }
    c.execute_batch("SET pg_pool_enable_reaper_thread=false; SET pg_use_ctid_scan=false; SET pg_pool_max_connections=8; SET pg_pool_acquire_mode='try'").unwrap();
    let password = fs::read_to_string("/secrets/postgres").unwrap();
    assert!(password.bytes().all(|v| v.is_ascii_hexdigit()));
    // Administrative fixture readback stays read-only except the explicitly
    // selected quiescent visibility test mutation. Neither is product authority.
    // The mutation session alone bypasses triggers as postgres. Never alter the
    // persistent writer guard/roles to plant an intentionally uncertified fact.
    let fixture_options = if mutation.is_some() {
        " options=''-c session_replication_role=replica''"
    } else {
        ""
    };
    // Never echo native secret-bearing errors onto the evidence pipes.
    assert!(c.execute_batch(&format!("CREATE TEMPORARY SECRET reader (TYPE postgres, URI 'hostaddr=127.0.0.1 port=5432 dbname=bbpub_{mode} user=postgres password={password} sslmode=disable connect_timeout=5{fixture_options}')")).is_ok(),"private reader secret");
    let read_only = if mutation.is_some() {
        ""
    } else {
        ", READ_ONLY"
    };
    assert!(c.execute_batch(&format!("ATTACH 'ducklake:postgres:' AS archive (DATA_PATH '/out/signed_{mode}/kind=7/', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS false, META_SECRET_STORAGE_TABLE '', META_SECRET 'reader'{read_only})")).is_ok(),"read-only catalog attach");
    if let Some(mutation) = mutation {
        visibility_fixture::append(&c, mutation);
    }
    // Explicit field normalization, independent of production Arrow/codec code.
    c.execute_batch("CREATE MACRO ref_value(r) AS CASE WHEN r IS NULL THEN NULL ELSE struct_pack(dataset_id:=hex(r.dataset_id),dictionary_kind:=r.dictionary_kind,allocation_domain:=hex(r.allocation_domain),ordinal:=r.ordinal) END; CREATE MACRO source_value(r) AS CASE WHEN r IS NULL THEN NULL ELSE struct_pack(dataset_id:=hex(r.dataset_id),shard_id:=r.shard_id,owner_epoch:=r.owner_epoch,sequence:=r.sequence) END; CREATE MACRO clock_value(r) AS struct_pack(clock_id:=ref_value(r.clock_id),resolution_us:=r.resolution_us,uncertainty_us:=r.uncertainty_us,anomaly_flags:=r.anomaly_flags)").unwrap();
    let common = "source_value(source_record_id) AS source_record_id,record_schema_version,record_kind,ref_value(visibility_scope_id) AS visibility_scope_id,hex(payload_digest) AS payload_digest,recorded_at";
    let descriptor = "struct_pack(event_id:=hex(descriptor_value.event_id),class:=descriptor_value.class,url:=descriptor_value.url,clock:=descriptor_value.clock,vantage:=descriptor_value.vantage,software:=descriptor_value.software,method_id:=ref_value(descriptor_value.method_id),public_key:=hex(descriptor_value.public_key),route_class:=descriptor_value.route_class,software_version:=descriptor_value.software_version,label:=descriptor_value.label,visibility_scope_id:=ref_value(descriptor_value.visibility_scope_id),input:=descriptor_value.input,algorithm:=descriptor_value.algorithm,capability:=descriptor_value.capability,output_unit:=descriptor_value.output_unit,failure_taxonomy:=descriptor_value.failure_taxonomy,algorithm_version:=descriptor_value.algorithm_version,protocol_revision:=descriptor_value.protocol_revision,format:=descriptor_value.format,format_version:=descriptor_value.format_version,source_dataset_id:=hex(descriptor_value.source_dataset_id),source_artifact_digest:=hex(descriptor_value.source_artifact_digest)) AS descriptor_value";
    let families=[
        ("dictionaries",format!("ref_value(reference) AS reference,normalization_version,descriptor_schema,descriptor_version,{descriptor}")),
        ("events","hex(event_id) AS event_id,hex(pubkey) AS pubkey,created_at,kind,tags,content,ref_value(event_ordinal) AS event_ordinal".into()),
        ("event_eligibility_facts","hex(event_id) AS event_id,hex(body_digest) AS body_digest,hex(sig) AS sig,first_eligible_at,origin,ref_value(import_lineage) AS import_lineage".into()),
        ("event_receive_facts","hex(event_id) AS event_id,received_at,ref_value(observer_id) AS observer_id,clock_value(clock_quality) AS clock_quality,source_value(corrects) AS corrects".into()),
        ("provenance_facts","hex(event_id) AS event_id,ref_value(endpoint_id) AS endpoint_id,ref_value(observer_id) AS observer_id,ref_value(access_scope_id) AS access_scope_id,received_at,clock_value(clock_quality) AS clock_quality,ref_value(verification_method_id) AS verification_method_id,source_value(corrects) AS corrects".into()),
    ];
    let mut tables = BTreeMap::new();
    for (table, fields) in families {
        let sql = format!(
            "SELECT to_json(r) FROM (SELECT {common},{fields} FROM archive.{table} ORDER BY source_record_id.sequence LIMIT 17) r"
        );
        let mut statement = c.prepare(&sql).unwrap();
        let rows: Vec<serde_json::Value> = statement
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|v| {
                let s = v.unwrap();
                // Scientific census metadata is a different envelope from the
                // worker NativeFact row; only explicit boundary cases enlarge it.
                assert!(
                    s.len()
                        < if native_row {
                            2 * 1024 * 1024
                        } else {
                            1024 * 1024
                        }
                );
                serde_json::from_str(&s).unwrap()
            })
            .collect();
        assert!(rows.len() <= 16);
        tables.insert(table, rows);
    }
    let mut receipt=c.prepare("SELECT to_json(r) FROM (SELECT hex(dataset_id) AS dataset_id,schema_version,topology_generation,hex(batch_digest) AS batch_digest,record_count,input_ranges,hex(attempt_id) AS attempt_id,publication_ordinal FROM archive.archive_commits ORDER BY publication_ordinal LIMIT 4) r").unwrap();
    tables.insert(
        "archive_commits",
        receipt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|s| serde_json::from_str(&s.unwrap()).unwrap())
            .collect::<Vec<_>>(),
    );
    let event_count: u64 = c
        .query_row("SELECT count(*) FROM archive.events", [], |r| r.get(0))
        .unwrap();
    let event = if mode == "before" || bootstrap_only {
        assert_eq!(event_count, 0);
        None
    } else {
        assert_eq!(event_count, 1);
        let actual=c.query_row("SELECT lower(hex(e.event_id)),lower(hex(e.pubkey)),e.created_at,e.kind,to_json(e.tags),e.content,lower(hex(v.sig)) FROM archive.events e JOIN archive.event_eligibility_facts v ON e.event_id=v.event_id AND e.visibility_scope_id=v.visibility_scope_id",[],|r|Ok(SignedEvent {id:r.get::<_,String>(0)?.parse().unwrap(),pubkey:r.get::<_,String>(1)?.parse().unwrap(),created_at:NostrTime(r.get(2)?),kind:r.get(3)?,tags:serde_json::from_str(&r.get::<_,String>(4)?).unwrap(),content:r.get(5)?,sig:r.get::<_,String>(6)?.parse().unwrap()})).unwrap();
        let expected: SignedEvent = match mode.as_str() {
            "row_exact" => serde_json::from_str(include_str!(
                "../../../tests/fixtures/protocol/archive-event-native-row-exact.json"
            ))
            .unwrap(),
            "row_over" => serde_json::from_str(include_str!(
                "../../../tests/fixtures/protocol/archive-event-native-row-over.json"
            ))
            .unwrap(),
            _ => {
                let fixture: serde_json::Value = serde_json::from_str(include_str!(
                    "../../../experiments/identity-contract/signed-fixture.json"
                ))
                .unwrap();
                serde_json::from_value(fixture["events"]["E_public"].clone()).unwrap()
            }
        };
        assert_eq!(actual, expected);
        Some(
            bigbrotr_protocol::verify_owned(
                actual,
                if native_row { 1024 * 1024 } else { 256 * 1024 },
            )
            .unwrap()
            .into_event(),
        )
    };
    let mut output =
        serde_json::json!({"event":"readback","mode":mode,"tables":tables,"signed_event":event});
    if native_row {
        // Independently capture the complete native projection before the
        // product reader's left()/byte gate. Python compares every original byte
        // with its separately encoded fixed-identity expectation.
        c.execute_batch("CREATE MACRO native_ref(r) AS struct_pack(dataset_id:=lower(hex(r.dataset_id)),dictionary_kind:=r.dictionary_kind,allocation_domain:=lower(hex(r.allocation_domain)),ordinal:=r.ordinal::VARCHAR); CREATE MACRO native_source(r) AS struct_pack(dataset_id:=lower(hex(r.dataset_id)),shard_id:=r.shard_id,owner_epoch:=r.owner_epoch::VARCHAR,sequence:=r.sequence::VARCHAR)").unwrap();
        let row: String = c.query_row("SELECT to_json(struct_pack(source:=native_source(source_record_id),scope:=native_ref(visibility_scope_id),recorded:=recorded_at::VARCHAR,digest:=lower(hex(payload_digest)),schema:=record_schema_version,kind:=record_kind,payload:=struct_pack(event_id:=lower(hex(event_id)),pubkey:=lower(hex(pubkey)),created_at:=created_at,kind:=kind,tags:=tags,content:=content,event_ordinal:=native_ref(event_ordinal)))) FROM archive.events WHERE source_record_id.sequence=7 LIMIT 2", [], |r| r.get(0)).unwrap();
        assert_eq!(row.len(), 1024 * 1024 + usize::from(mode == "row_over"));
        output["native_payload_json"] = serde_json::json!(row);
    }
    println!("{output}");
}
