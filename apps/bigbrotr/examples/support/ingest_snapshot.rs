//! Independent finite native / all-table / exact-original fixture observations.
use super::*;
use sha2::{Digest, Sha256};

pub async fn state(observer: &tokio_postgres::Client, path: &Path) -> serde_json::Value {
    let (rows, sequence) = raw_state(path);
    let native_sha256 = hex(&Sha256::digest(serde_json::to_vec(&rows).unwrap()));
    let mut control = Vec::new();
    for row in observer
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename",
            &[],
        )
        .await
        .unwrap()
    {
        let table: String = row.get(0);
        // Names come from the isolated fixture catalog; still quote identifiers.
        let quoted = table.replace('"', "\"\"");
        let query = format!(
            "SELECT coalesce(jsonb_agg(v ORDER BY v::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(r)||jsonb_build_object('xmin',xmin::text) v FROM control.\"{quoted}\" r) rows"
        );
        let rows: String = observer.query_one(&query, &[]).await.unwrap().get(0);
        control.push((table, rows));
    }
    assert!(!control.is_empty());
    let directory = path.join("producer-inputs-v1");
    let mut originals = if directory.exists() {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let bytes = std::fs::read(entry.path()).unwrap();
                (
                    entry.file_name().into_string().unwrap(),
                    serde_json::json!({"bytes":bytes.len(),"sha256":hex(&Sha256::digest(bytes))}),
                )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    originals.sort_by(|a, b| a.0.cmp(&b.0));
    serde_json::json!({"native_rows":rows.len(),"native_sha256":native_sha256,"sequence":sequence,
        "control_sha256":hex(&Sha256::digest(serde_json::to_vec(&control).unwrap())),"control_tables":control,"originals":originals})
}
