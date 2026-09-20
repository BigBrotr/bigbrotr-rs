use super::*;
use bigbrotr::runtime::{ProcessEnvelope, ResourceLimits, WaiterLimits};
use std::{fs, os::unix::fs::symlink};

fn budget(bytes: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes + 1024,
            1024,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn fixture(base: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("index.html"), b"hello").unwrap();
    fs::write(root.join("manifest.json"), format!(r#"{{"format":"bigbrotr-private-assets/1","base_path":"{base}","files":[{{"path":"index.html","bytes":5,"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}]}}"#)).unwrap();
    (dir, root)
}

#[test]
fn verified_asset_clones_keep_the_shared_reservation_until_final_body_drop() {
    let (_dir, root) = fixture("/");
    let budget = budget(RESERVATION_BYTES);
    let assets = Assets::load(&root, "/", &budget).unwrap();
    let first = assets.lookup("/").unwrap().body.clone();
    let second = assets.lookup("/index.html").unwrap().body.clone();
    assert_eq!(&*first, b"hello");
    assert_eq!(first.as_ptr(), second.as_ptr());
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(assets);
    drop(first);
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(second);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn exact_mount_and_path_lookup_cannot_escape_or_expose_manifest() {
    let (_dir, root) = fixture("/control/");
    let assets = Assets::load(&root, "/control/", &budget(RESERVATION_BYTES)).unwrap();
    assert!(assets.lookup("/control/").is_some());
    for path in [
        "/",
        "/index.html",
        "/control",
        "/control/../index.html",
        "/control/%69ndex.html",
        "/control//index.html",
        "/control/manifest.json",
        "/management/v1/me",
    ] {
        assert!(assets.lookup(path).is_none(), "{path}");
    }
    assert!(matches!(
        Assets::load(&root, "/", &budget(RESERVATION_BYTES)),
        Err(AssetError::Manifest)
    ));
}

#[test]
fn corruption_missing_files_and_insufficient_budget_refuse_before_serving() {
    let (_dir, root) = fixture("/");
    assert!(matches!(
        Assets::load(&root, "/", &budget(RESERVATION_BYTES - 1)),
        Err(AssetError::Capacity)
    ));
    fs::write(root.join("index.html"), b"jello").unwrap();
    assert!(matches!(
        Assets::load(&root, "/", &budget(RESERVATION_BYTES)),
        Err(AssetError::Integrity)
    ));
    fs::remove_file(root.join("index.html")).unwrap();
    assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_err());
    fs::write(root.join("index.html"), b"hello").unwrap();
    assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_ok());
}

#[test]
fn symlink_at_root_parent_leaf_or_manifest_and_nonregular_inputs_are_refused() {
    let (_dir, root) = fixture("/");
    let outer = tempfile::tempdir().unwrap();
    let outer = outer.path().canonicalize().unwrap();
    symlink(&root, outer.join("alias")).unwrap();
    assert!(Assets::load(&outer.join("alias"), "/", &budget(RESERVATION_BYTES)).is_err());
    fs::create_dir(root.join("nested")).unwrap();
    fs::copy(root.join("index.html"), root.join("nested/index.html")).unwrap();
    fs::copy(
        root.join("manifest.json"),
        root.join("nested/manifest.json"),
    )
    .unwrap();
    assert!(Assets::load(&root.join("nested"), "/", &budget(RESERVATION_BYTES)).is_ok());
    // NOFOLLOW only on the final root component would incorrectly accept this.
    assert!(Assets::load(&outer.join("alias/nested"), "/", &budget(RESERVATION_BYTES)).is_err());
    fs::rename(root.join("index.html"), root.join("target.html")).unwrap();
    symlink("target.html", root.join("index.html")).unwrap();
    assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_err());
    fs::remove_file(root.join("index.html")).unwrap();
    fs::create_dir(root.join("index.html")).unwrap();
    assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_err());
    fs::rename(root.join("manifest.json"), root.join("target.json")).unwrap();
    symlink("target.json", root.join("manifest.json")).unwrap();
    assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_err());
}

#[test]
fn fifo_is_refused_without_waiting_for_a_writer() {
    let (_dir, root) = fixture("/");
    fs::remove_file(root.join("index.html")).unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(root.join("index.html"))
            .status()
            .unwrap()
            .success()
    );
    let input = root.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        send.send(Assets::load(&input, "/", &budget(RESERVATION_BYTES)).is_err())
            .unwrap();
    });
    let result = receive.recv_timeout(std::time::Duration::from_secs(1));
    if result.is_err() {
        // Unblock the owned FIFO opener before failing a NONBLOCK regression.
        let _wake = rustix::fs::open(
            root.join("index.html"),
            OFlags::RDWR | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .unwrap();
        worker.join().unwrap();
        panic!("FIFO load waited for a writer");
    }
    assert!(result.unwrap());
    worker.join().unwrap();
}

#[test]
fn duplicate_entries_file_count_and_aggregate_bytes_refuse_before_file_io() {
    let (_dir, root) = fixture("/");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let entry = manifest["files"][0].clone();
    for (count, bytes, duplicate) in [(2, 5, true), (129, 0, false), (9, 2_097_152, false)] {
        manifest["files"] = serde_json::Value::Array(
            (0..count)
                .map(|n| {
                    let mut file = entry.clone();
                    file["path"] = serde_json::json!(if n == 0 || duplicate {
                        "index.html".to_owned()
                    } else {
                        format!("{n}.js")
                    });
                    file["bytes"] = serde_json::json!(bytes);
                    file
                })
                .collect(),
        );
        fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            Assets::load(&root, "/", &budget(RESERVATION_BYTES)),
            Err(AssetError::Manifest)
        ));
    }
}

#[test]
fn manifest_limits_and_closed_namespace_reject_unbounded_or_ambiguous_input() {
    let (_dir, root) = fixture("/");
    let original = fs::read_to_string(root.join("manifest.json")).unwrap();
    for changed in [
        original.replace("private-assets/1", "private-assets/2"),
        original.replace("index.html", "../index.html"),
        original.replace("index.html", "/index.html"),
        original.replace("index.html", ".hidden/index.html"),
        original.replace("index.html", "index.txt"),
        original.replace("\"bytes\":5", "\"bytes\":2097153"),
        original.replace("\"files\":", "\"unknown\":true,\"files\":"),
        " ".repeat(MANIFEST_BYTES + 1),
    ] {
        fs::write(root.join("manifest.json"), changed).unwrap();
        assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_err());
    }
    fs::write(root.join("manifest.json"), original).unwrap();
    assert!(Assets::load(&root, "/", &budget(RESERVATION_BYTES)).is_ok());
}
