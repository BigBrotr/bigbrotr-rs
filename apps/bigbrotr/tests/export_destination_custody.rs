//! Destination custody without process-spawning siblings in the test harness.
//! CLI/umask scenarios live in export_destination; their children must not inherit
//! these descriptors while an owner is dropped and immediately reopened.
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

fn private(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
const ID: &str = "acacacacacacacacacacacacacacacac";

#[test]
fn retained_destination_detects_root_substitution_and_excludes_reinitialization() {
    use bigbrotr::export::destination::{DestinationError, LocalDestination};
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("export");
    private(&root);
    let dataset = "01010101010101010101010101010101".parse().unwrap();
    let id = ID.parse().unwrap();
    let first = LocalDestination::initialize(&root, dataset, id).unwrap();
    assert!(matches!(
        LocalDestination::initialize(&root, dataset, id),
        Err(DestinationError::Busy)
    ));
    assert!(matches!(
        LocalDestination::open(&root, dataset, id),
        Err(DestinationError::Busy)
    ));
    drop(first);
    let retained = LocalDestination::open(&root, dataset, id).unwrap();
    let another = LocalDestination::open(&root, dataset, id).unwrap();
    assert_eq!(retained.binding_digest(), another.binding_digest());
    assert!(matches!(
        LocalDestination::initialize(&root, dataset, id),
        Err(DestinationError::Busy)
    ));
    let old = base.join("old");
    fs::rename(&root, &old).unwrap();
    private(&root);
    fs::copy(old.join("destination.json"), root.join("destination.json")).unwrap();
    assert_eq!(retained.verify(), Err(DestinationError::Mismatch));
    assert!(matches!(
        LocalDestination::open(&root, dataset, id),
        Err(DestinationError::Mismatch)
    ));
    assert!(old.join("destination.json").exists());
}

#[test]
fn acceptance_observation_keeps_custody_after_the_checker_thread_joins() {
    use bigbrotr::export::destination::{DestinationError, LocalDestination};
    use std::sync::Arc;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("export");
    private(&root);
    let dataset = "01010101010101010101010101010101".parse().unwrap();
    let id = ID.parse().unwrap();
    let destination = Arc::new(LocalDestination::initialize(&root, dataset, id).unwrap());
    let original = destination.clone();
    let checked = std::thread::spawn(move || destination.check_for_acceptance())
        .join()
        .unwrap()
        .unwrap();
    drop(original);
    assert_eq!(checked.dataset(), dataset);
    assert_eq!(checked.id(), id);
    assert!(matches!(
        LocalDestination::initialize(&root, dataset, id),
        Err(DestinationError::Busy)
    ));
    drop(checked);
    let replacement = Arc::new(LocalDestination::open(&root, dataset, id).unwrap());
    fs::rename(&root, root.with_file_name("retained")).unwrap();
    private(&root);
    assert!(matches!(
        replacement.check_for_acceptance(),
        Err(DestinationError::Mismatch)
    ));
}
