//! Actual finite CLI and filesystem observations; no database or runtime authority.
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::{Command, Output},
};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bigbrotr"))
        .args(args)
        .env_clear()
        .output()
        .unwrap()
}
fn config(base: &Path, root: &Path, id: &str) -> std::path::PathBuf {
    let output = run(&["config", "example", "--recipe", "archive-only"]);
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let path = base.join("instance.toml");
    fs::write(
        &path,
        format!(
            "{text}\n[export.destination]\nid = '{id}'\nroot = '{}'\n",
            root.display()
        ),
    )
    .unwrap();
    path
}
fn destination(operation: &str, config: &Path) -> Output {
    run(&["export", operation, "--file", config.to_str().unwrap()])
}
fn success(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}
fn refused(output: Output) {
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
}
fn private(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
const ID: &str = "acacacacacacacacacacacacacacacac";

#[test]
fn explicit_destination_survives_restart_and_never_appears_from_inspection() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("export");
    let file = config(&base, &root, ID);
    success(run(&[
        "config",
        "validate",
        "--file",
        file.to_str().unwrap(),
    ]));
    refused(destination("destination-inspect", &file));
    assert!(!root.exists());
    private(&root);
    refused(destination("destination-inspect", &file));
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    let first = success(destination("destination-initialize", &file));
    assert_eq!(first["destination_id"], ID);
    assert_eq!(first["state"], "bound");
    assert_eq!(first["job_authority"], "not-granted");
    let bytes = fs::read(root.join("destination.json")).unwrap();
    let marker: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(marker["format"], "bigbrotr-export-destination/1");
    assert_eq!(marker["destination_id"], ID);
    assert_eq!(success(destination("destination-initialize", &file)), first);
    assert_eq!(success(destination("destination-inspect", &file)), first);
    assert_eq!(fs::read(root.join("destination.json")).unwrap(), bytes);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    // Neither a copied root nor another ID may adopt an existing destination.
    config(&base, &root, "adadadadadadadadadadadadadadadad");
    refused(destination("destination-initialize", &file));
    assert_eq!(fs::read(root.join("destination.json")).unwrap(), bytes);
    let copy = base.join("copy");
    private(&copy);
    fs::write(copy.join("destination.json"), &bytes).unwrap();
    fs::set_permissions(
        copy.join("destination.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    config(&base, &copy, ID);
    refused(destination("destination-inspect", &file));
}

#[test]
fn refuses_unowned_contents_links_and_partial_markers_without_repair() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("export");
    private(&root);
    let file = config(&base, &root, ID);
    fs::write(root.join("unrelated"), b"preserve").unwrap();
    refused(destination("destination-initialize", &file));
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    fs::remove_file(root.join("unrelated")).unwrap();
    fs::write(root.join("destination.json"), b"{\"format\":").unwrap();
    fs::set_permissions(
        root.join("destination.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    refused(destination("destination-initialize", &file));
    assert_eq!(
        fs::read(root.join("destination.json")).unwrap(),
        b"{\"format\":"
    );
    fs::remove_file(root.join("destination.json")).unwrap();
    let outside = base.join("outside");
    fs::write(&outside, b"preserve").unwrap();
    symlink(&outside, root.join("destination.json")).unwrap();
    refused(destination("destination-initialize", &file));
    assert_eq!(fs::read(&outside).unwrap(), b"preserve");
    let alias = base.join("alias");
    symlink(&root, &alias).unwrap();
    config(&base, &alias, ID);
    refused(destination("destination-initialize", &file));
    config(&base, &root, ID);
    fs::remove_file(root.join("destination.json")).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    refused(destination("destination-initialize", &file));
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
}

#[test]
fn offline_pairing_and_disjointness_never_open_the_destination() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("absent");
    let path = config(&base, &root, ID);
    let original = fs::read_to_string(&path).unwrap();
    for text in [
        original.replace(&format!("id = '{ID}'\n"), ""),
        original.replace(&format!("root = '{}'\n", root.display()), ""),
        original.replace(&root.display().to_string(), "/"),
        original.replace(
            &root.display().to_string(),
            "/var/lib/bigbrotr/archive/child",
        ),
    ] {
        fs::write(&path, text).unwrap();
        let output = run(&["config", "validate", "--file", path.to_str().unwrap()]);
        assert_eq!(
            output.status.code(),
            Some(3),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(output.stdout.is_empty());
    }
    assert!(!root.exists());
    fs::write(&path, &original).unwrap();
    let first = success(run(&["config", "show", "--file", path.to_str().unwrap()]));
    fs::write(
        &path,
        original
            .replace(ID, "adadadadadadadadadadadadadadadad")
            .replace(
                &root.display().to_string(),
                "/nonexistent/export-destination",
            ),
    )
    .unwrap();
    let second = success(run(&["config", "show", "--file", path.to_str().unwrap()]));
    assert_eq!(
        first["settings"]["export.destination.root"]["applicability"],
        "selected"
    );
    assert_eq!(
        first["settings"]["export.destination.id"]["deployment_locked"],
        true
    );
    assert!(first["initial_policy"].is_object());
    assert_eq!(first["initial_policy"], second["initial_policy"]);
}

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
fn restrictive_umask_cannot_create_an_unusable_marker() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("export");
    private(&root);
    let file = config(&base, &root, ID);
    let output = Command::new("/bin/sh")
        .args([
            "-c",
            "umask 0600; exec \"$1\" export destination-initialize --file \"$2\"",
            "destination-test",
        ])
        .arg(env!("CARGO_BIN_EXE_bigbrotr"))
        .arg(&file)
        .env_clear()
        .output()
        .unwrap();
    let original = success(output);
    assert_eq!(
        fs::metadata(root.join("destination.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );
    assert_eq!(
        success(destination("destination-initialize", &file)),
        original
    );
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
