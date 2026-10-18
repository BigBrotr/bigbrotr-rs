use super::*;
use std::{fs, os::unix::fs::PermissionsExt};
const BOOT: &str = "01234567-89ab-cdef-0123-456789abcdef";

mod frontier_intents;
mod inputs;
mod retained_receipts;

#[test]
fn pin_recovery_custody_cannot_be_rewrapped_into_a_reader() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let state = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let mut custody = recover_released(&identity);
    custody.restrict_to_pin_recovery();
    let owner = super::super::LocalCoordinator::from_custody(custody, &state, true).unwrap();
    let driver = owner
        .retain_connection(std::future::pending::<()>())
        .unwrap();
    assert!(!owner.permits_reader_pins());
    assert_eq!(owner.reader_custody().err(), Some(LifetimeError::Identity));
    let custody = owner.custody;
    let rewrapped = super::super::LocalCoordinator::from_custody(custody, &state, false).unwrap();
    assert!(!rewrapped.permits_reader_pins());
    assert_eq!(
        rewrapped.reader_custody().err(),
        Some(LifetimeError::Identity)
    );
    drop(rewrapped);
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(driver);
    let ordinary =
        super::super::LocalCoordinator::from_custody(recover_released(&identity), &state, false)
            .unwrap();
    assert!(ordinary.permits_reader_pins());
    ordinary.reader_custody().unwrap();
}

#[test]
fn owned_reader_custody_survives_thread_join_until_the_result_is_consumed() {
    use std::{sync::mpsc::sync_channel, time::Duration};
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let state = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner =
        super::super::LocalCoordinator::from_custody(recover_released(&identity), &state, true)
            .unwrap();
    let reader = owner.reader_custody().unwrap();
    drop(owner);
    let expected = identity.clone();
    let (ready_tx, ready_rx) = sync_channel(1);
    let (finish_tx, finish_rx) = sync_channel(1);
    // A real unscoped thread requires an owned Send value; the coordinator's
    // non-Sync initialization state never enters the blocking worker.
    let worker = std::thread::spawn(move || {
        assert_eq!(reader.identity(), &expected);
        ready_tx.send(()).unwrap();
        finish_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        reader
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let during_work = LocalInitialization::recover_for_boot(&identity, BOOT).err();
    finish_tx.send(()).unwrap();
    let result = worker.join().unwrap();
    let after_join = LocalInitialization::recover_for_boot(&identity, BOOT).err();
    assert_eq!(result.identity(), &identity);
    drop(result);
    drop(recover_released(&identity));
    assert_eq!(during_work, Some(LifetimeError::Occupied));
    assert_eq!(after_join, Some(LifetimeError::Occupied));
}

#[test]
fn coordinator_children_retain_custody_until_the_last_actual_exit() {
    use std::{
        io::{BufRead, BufReader},
        process::Stdio,
    };
    for inherit in [false, true] {
        let (_data, identity) = fixture();
        let (state, _) = fixture();
        let state = state.path().canonicalize().unwrap();
        drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
        let owner =
            super::super::LocalCoordinator::from_custody(recover_released(&identity), &state, true)
                .unwrap();
        let mut children = Vec::new();
        for _ in 0..2 {
            let mut command = Command::new("/bin/sh");
            command
                .args(["-c", "printf 'ready\\n'; read ignored"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped());
            if inherit {
                owner
                    .reader_custody()
                    .unwrap()
                    .inherit(&mut command)
                    .unwrap();
            }
            let mut child = command.spawn().unwrap();
            let mut ready = String::new();
            BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut ready)
                .unwrap();
            assert_eq!(ready, "ready\n");
            children.push(child);
        }
        drop(owner);
        let mut occupied = Vec::new();
        for mut child in children {
            occupied.push(matches!(
                LocalInitialization::recover_for_boot(&identity, BOOT),
                Err(LifetimeError::Occupied)
            ));
            child.kill().unwrap();
            child.wait().unwrap();
        }
        drop(recover_released(&identity));
        assert_eq!(occupied, [inherit, inherit]);
    }
}

#[test]
fn coordinator_state_requires_explicit_binding_and_excludes_other_roots() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let (other, _) = fixture();
    let state = state.path().canonicalize().unwrap();
    let other = other.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let custody = recover_released(&identity);
    assert!(super::super::LocalCoordinator::from_custody(custody, &state, false).is_err());
    assert!(fs::read_dir(&state).unwrap().next().is_none());
    let custody = recover_released(&identity);
    let owner = super::super::LocalCoordinator::from_custody(custody, &state, true).unwrap();
    assert_eq!(owner.state_root(), state);
    let bytes = fs::read(state.join("coordinator.json")).unwrap();
    assert_eq!(
        bytes,
        fs::read(Path::new(identity.root()).join(".bigbrotr-initialization/coordinator.json"))
            .unwrap()
    );
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(owner);
    let custody = recover_released(&identity);
    assert!(super::super::LocalCoordinator::from_custody(custody, &other, true).is_err());
    assert!(fs::read_dir(&other).unwrap().next().is_none());
    let custody = recover_released(&identity);
    drop(super::super::LocalCoordinator::from_custody(custody, &state, false).unwrap());
    assert_eq!(fs::read(state.join("coordinator.json")).unwrap(), bytes);
}

#[test]
fn coordinator_keeps_driver_custody_and_preserves_partial_binding() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let state = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let custody = recover_released(&identity);
    let owner = super::super::LocalCoordinator::from_custody(custody, &state, true).unwrap();
    let driver = owner
        .retain_connection(std::future::pending::<()>())
        .unwrap();
    drop(owner);
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(driver);
    let binding = Path::new(identity.root()).join(".bigbrotr-initialization/coordinator.json");
    let bytes = fs::read(&binding).unwrap();
    fs::remove_file(&binding).unwrap(); // Simulate death before reverse binding creation.
    let custody = recover_released(&identity);
    assert!(super::super::LocalCoordinator::from_custody(custody, &state, false).is_err());
    assert!(!binding.exists());
    let custody = recover_released(&identity);
    drop(super::super::LocalCoordinator::from_custody(custody, &state, true).unwrap());
    assert_eq!(fs::read(&binding).unwrap(), bytes);
    fs::write(&binding, b"partial").unwrap();
    for initialize in [false, true] {
        let custody = recover_released(&identity);
        assert!(super::super::LocalCoordinator::from_custody(custody, &state, initialize).is_err());
        assert_eq!(fs::read(&binding).unwrap(), b"partial");
    }
}
// Concurrent process tests can fork while this test owns a CLOEXEC descriptor.
// Require actual reacquisition after that short-lived pre-exec holder closes;
// the deadline itself never establishes custody.
fn recover_released(identity: &CatalogIdentity) -> LocalInitialization {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match LocalInitialization::recover_for_boot(identity, BOOT) {
            Ok(value) => return value,
            Err(LifetimeError::Occupied) if std::time::Instant::now() < until => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(error) => panic!("released initialization remains unavailable: {error:?}"),
        }
    }
}
fn fixture() -> (tempfile::TempDir, CatalogIdentity) {
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let identity = CatalogIdentity::local(
        [1; 16].try_into().unwrap(),
        [2; 16].try_into().unwrap(),
        &format!("{}/", root.display()),
    )
    .unwrap();
    (temp, identity)
}
#[test]
fn reserves_the_actual_root_and_never_recreates_an_existing_intent() {
    let (_temp, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap();
    let root = std::path::Path::new(identity.root());
    let before = fs::read(root.join(".bigbrotr-initialization/intent.json")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&before).unwrap();
    assert_eq!(value["format"], "bigbrotr-local-initialization/1");
    assert_eq!(
        value["initialization_id"],
        serde_json::to_value([3; 16]).unwrap()
    );
    assert_eq!(value["catalog_root"], identity.root());
    // Independent complete byte oracle: only host-assigned path/device/inode
    // values vary. This freezes ordering, identity encoding and final newline.
    let rm = fs::metadata(root).unwrap();
    let lm = fs::metadata(root.join(".bigbrotr-initialization/custody.lock")).unwrap();
    let expected = format!(
        "{{\"format\":\"bigbrotr-local-initialization/1\",\"initialization_id\":[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],\"dataset_id\":\"01010101010101010101010101010101\",\"catalog_id\":\"02020202020202020202020202020202\",\"catalog_root\":{},\"boot_id\":\"01234567-89ab-cdef-0123-456789abcdef\",\"root_device\":{},\"root_inode\":{},\"lock_device\":{},\"lock_inode\":{}}}",
        serde_json::to_string(identity.root()).unwrap(),
        rm.dev(),
        rm.ino(),
        lm.dev(),
        lm.ino(),
    );
    assert_eq!(before, expected.as_bytes());
    drop(owner);
    assert!(matches!(
        LocalInitialization::create_for_boot(&identity, [4; 16], BOOT),
        Err(LifetimeError::Exists)
    ));
    assert_eq!(
        fs::read(root.join(".bigbrotr-initialization/intent.json")).unwrap(),
        before
    );
}

#[test]
fn competing_initializers_never_replace_the_winning_journal() {
    let (temp, identity) = fixture();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = [3, 4]
            .into_iter()
            .map(|id| {
                let identity = &identity;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    LocalInitialization::create_for_boot(identity, [id; 16], BOOT)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(LifetimeError::Exists)))
            .count(),
        1
    );
    let retained = fs::read(temp.path().join(".bigbrotr-initialization/intent.json")).unwrap();
    let winner = *results
        .iter()
        .find_map(|r| r.as_ref().ok())
        .unwrap()
        .initialization_id();
    drop(results);
    let recovered = recover_released(&identity);
    assert_eq!(recovered.initialization_id(), &winner);
    assert_eq!(
        retained,
        fs::read(temp.path().join(".bigbrotr-initialization/intent.json")).unwrap()
    );
}

#[test]
fn recovery_is_held_by_the_unpolled_driver_and_never_authorizes_a_new_launch() {
    let (_temp, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap();
    let driver = owner
        .retain_catalog_connection(std::future::pending::<()>())
        .unwrap();
    drop(owner);
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(driver);
    let recovered = recover_released(&identity);
    assert_eq!(recovered.initialization_id(), &[3; 16]);
    assert!(
        recovered
            .inherit(&mut std::process::Command::new("/bin/true"))
            .is_err()
    );
}

#[test]
fn only_actual_reacquisition_is_recovery_not_consumed_provisioning_or_launch() {
    let (_temp, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap();
    assert!(!owner.is_recovered());
    owner.take_provisioning().unwrap();
    assert!(!owner.is_recovered());
    let mut command = Command::new("/bin/true");
    owner.inherit(&mut command).unwrap();
    assert!(!owner.is_recovered());
    drop(command);
    drop(owner);
    let recovered = recover_released(&identity);
    assert!(recovered.is_recovered());
    assert_eq!(recovered.take_provisioning(), Err(LifetimeError::Identity));
    assert!(recovered.inherit(&mut Command::new("/bin/true")).is_err());
}

#[test]
fn provisioning_is_once_only_and_never_authorized_by_recovery_or_a_previous_launch() {
    let (_temp, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap();
    assert_eq!(owner.take_provisioning(), Ok(()));
    assert_eq!(owner.take_provisioning(), Err(LifetimeError::Identity));
    drop(owner);
    let recovered = recover_released(&identity);
    assert_eq!(recovered.take_provisioning(), Err(LifetimeError::Identity));
    let (_other, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [4; 16], BOOT).unwrap();
    let mut command = Command::new("/bin/true");
    owner.inherit(&mut command).unwrap();
    assert_eq!(owner.take_provisioning(), Err(LifetimeError::Identity));
}

#[test]
fn a_native_command_clone_keeps_custody_until_its_child_exits() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let (_temp, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap();
    let mut command = Command::new("/bin/sh");
    command
        .args(["-c", "printf 'ready\\n'; read ignored"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    let proof = owner.inherit(&mut command).unwrap();
    assert!(proof.custody_fd >= 3);
    assert!(owner.inherit(&mut Command::new("/bin/true")).is_err());
    let mut child = command.spawn().unwrap();
    let mut ready = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(ready, "ready\n");
    drop(command);
    drop(owner);
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    child.kill().unwrap();
    child.wait().unwrap();
    drop(recover_released(&identity));
}

#[test]
fn partial_modified_and_copied_reservations_are_preserved_and_refused() {
    let (temp, identity) = fixture();
    let owner = LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap();
    drop(owner);
    let marker = temp.path().join(".bigbrotr-initialization");
    let path = marker.join("intent.json");
    let original = fs::read(&path).unwrap();
    for (bytes, expected) in [
        (vec![], LifetimeError::Incomplete),
        (vec![b'x'; 32769], LifetimeError::ResourceLimit),
        (b"{}".to_vec(), LifetimeError::Identity),
    ] {
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            LocalInitialization::recover_for_boot(&identity, BOOT).err(),
            Some(expected)
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    fs::write(&path, &original).unwrap();
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, "fedcba98-7654-3210-fedc-ba9876543210"),
        Err(LifetimeError::Identity)
    ));
    let (copy, other) = fixture();
    fs::create_dir(copy.path().join(".bigbrotr-initialization")).unwrap();
    fs::set_permissions(
        copy.path().join(".bigbrotr-initialization"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    for name in ["intent.json", "custody.lock"] {
        fs::copy(
            marker.join(name),
            copy.path().join(".bigbrotr-initialization").join(name),
        )
        .unwrap();
    }
    assert!(matches!(
        LocalInitialization::recover_for_boot(&other, BOOT),
        Err(LifetimeError::Identity)
    ));
    fs::rename(marker.join("custody.lock"), marker.join("original.lock")).unwrap();
    fs::write(marker.join("custody.lock"), []).unwrap();
    fs::set_permissions(
        marker.join("custody.lock"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert!(LocalInitialization::recover_for_boot(&identity, BOOT).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn another_data_file_or_partial_marker_cannot_be_silently_adopted() {
    let (temp, identity) = fixture();
    let existing = temp.path().join("unrelated");
    fs::write(&existing, b"retained").unwrap();
    assert!(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).is_err());
    assert_eq!(fs::read(&existing).unwrap(), b"retained");
    assert!(!temp.path().join(".bigbrotr-initialization").exists());
    fs::remove_file(existing).unwrap();
    fs::create_dir(temp.path().join(".bigbrotr-initialization")).unwrap();
    assert!(matches!(
        LocalInitialization::create_for_boot(&identity, [3; 16], BOOT),
        Err(LifetimeError::Exists)
    ));
    assert!(LocalInitialization::recover_for_boot(&identity, BOOT).is_err());
}
