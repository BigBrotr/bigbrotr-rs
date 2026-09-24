//! Research driver of the actual filesystem unit, not an installed job executor.
//! No database/read authority, native snapshot observation or runtime supervision.
use bigbrotr::export::{
    destination::LocalDestination, files::ExportUnit, manifest::SignedEventsManifest,
};
use bigbrotr_model::{
    CompleteThrough, OwnerEpoch, SourceCut, StreamBound, StreamId, TopologyGeneration,
};
use bigbrotr_protocol::{EventLimits, verify_json};
use std::{
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 3, "initialize|write <existing-private-root>");
    let root = Path::new(&args[2]);
    let dataset = [1; 16].try_into().unwrap();
    let destination = [0x40; 16].try_into().unwrap();
    if args[1] == "initialize" {
        let result = LocalDestination::initialize(root, dataset, destination);
        match result {
            Ok(_) => println!("{}", serde_json::json!({"initialized": true})),
            Err(error) => {
                println!("{}", serde_json::json!({"error": format!("{error:?}")}));
                std::process::exit(4);
            }
        }
        return;
    }
    assert_eq!(args[1], "write");
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    let event = verify_json(
        &serde_json::to_vec(&fixtures["events"]["timestamp-zero"]["event"]).unwrap(),
        EventLimits::default(),
    )
    .unwrap();
    let golden: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/exports/signed-jsonl-manifest.json"
    ))
    .unwrap();
    let cut = SourceCut::new(
        dataset,
        TopologyGeneration::new(4),
        (0..2)
            .map(|i| StreamBound {
                stream: StreamId {
                    shard_id: i,
                    owner_epoch: OwnerEpoch::new(i as u64 + 1).unwrap(),
                },
                last_complete_sequence: CompleteThrough::new(if i == 0 { 0 } else { 10 }),
            })
            .collect(),
    )
    .unwrap();
    let manifest = SignedEventsManifest {
        artifact_id: [0x10; 16].try_into().unwrap(),
        job_id: [0x20; 16].try_into().unwrap(),
        source_cut: &cut,
        catalog_id: [0x30; 16].try_into().unwrap(),
        frontier_revision: 5,
        snapshot_id: 11,
        event_id: event.event().id,
        scope: serde_json::from_value(golden["selection"]["scope"].clone()).unwrap(),
        accepted_config_revision: 7,
        accepted_policy_revision: 9,
    };
    let destination = Arc::new(LocalDestination::open(root, dataset, destination).unwrap());
    let result = ExportUnit::acquire(destination, manifest.artifact_id).and_then(|mut unit| {
        unit.write(&manifest, Some(&event), 4096, 8192, &AtomicBool::new(false))
    });
    match result {
        Ok(result) => println!(
            "{}",
            serde_json::json!({"data_bytes": result.data.bytes,
            "data_sha256": result.data.sha256, "manifest_bytes": result.manifest_bytes,
            "manifest_sha256": result.manifest_sha256})
        ),
        Err(error) => {
            println!("{}", serde_json::json!({"error": format!("{error:?}")}));
            std::process::exit(4);
        }
    }
}
