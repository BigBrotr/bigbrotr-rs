//! Dependency-binding smoke controls only. These tests do not qualify application
//! failure delivery, native error callbacks, NoSpace, or physical bounds.
use rocksdb::{DB, Options, WaitForCompactOptions, WriteOptions};
use std::thread;

#[test]
fn installation_fixes_resume_count_and_refusal_preserves_options() {
    let mut options = Options::default()
        .get_options_from_string("max_bgerror_resume_count=7")
        .unwrap();
    assert_eq!(options.get_max_background_error_resume_count(), 7);
    let guard = options.install_background_error_guard().unwrap();
    assert_eq!(options.get_max_background_error_resume_count(), 0);
    assert!(!guard.failed());
    // Intentional negative only: native parsing copies this guarded base and
    // keeps its listener. Never open this deliberately altered guarded copy.
    // Production callers must freeze final installed options.
    let mut altered = options
        .get_options_from_string("max_bgerror_resume_count=9")
        .unwrap();
    assert_eq!(altered.get_max_background_error_resume_count(), 9);
    assert!(altered.install_background_error_guard().is_err());
    assert_eq!(altered.get_max_background_error_resume_count(), 9);
    assert_eq!(options.get_max_background_error_resume_count(), 0);
    assert!(!guard.failed());
}

#[test]
fn purge_wait_setter_reaches_native_option() {
    let mut wait = WaitForCompactOptions::default();
    assert!(!wait.get_wait_for_purge());
    wait.set_wait_for_purge(true);
    assert!(wait.get_wait_for_purge());
    wait.set_wait_for_purge(false);
    assert!(!wait.get_wait_for_purge());
}

#[test]
fn background_error_guard_clone_and_close_lifetimes() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("db");
    let mut options = Options::default();
    options.create_if_missing(true);
    let guard = options.install_background_error_guard().unwrap();
    assert!(!guard.failed());
    assert!(options.install_background_error_guard().is_err());
    let mut cloned = options.clone();
    assert!(cloned.install_background_error_guard().is_err());
    drop(options);
    let db = DB::open(&cloned, &path).unwrap();
    drop(cloned);
    let mut write = WriteOptions::default();
    write.set_sync(true);
    db.put_opt(b"guarded", b"healthy", &write).unwrap();
    db.flush().unwrap();
    let mut wait = WaitForCompactOptions::default();
    wait.set_flush(true);
    wait.set_abort_on_pause(true);
    wait.set_wait_for_purge(true);
    wait.set_timeout(10_000_000);
    db.wait_for_compact(&wait).unwrap();
    assert_eq!(db.get(b"guarded").unwrap().unwrap(), b"healthy");
    assert!(!guard.failed());
    thread::scope(|scope| {
        scope.spawn(|| assert!(!guard.failed())).join().unwrap();
    });
    drop(db);
    assert!(!guard.failed());
    thread::spawn(move || assert!(!guard.failed()))
        .join()
        .unwrap();
    DB::destroy(&Options::default(), &path).unwrap();
}

#[test]
fn background_error_guard_can_drop_before_open_or_database() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("db");
    let mut options = Options::default();
    options.create_if_missing(true);
    drop(options.install_background_error_guard().unwrap());
    assert!(options.install_background_error_guard().is_err());
    let db = DB::open(&options, &path).unwrap();
    drop(options);
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), b"value");
    drop(db);
    DB::destroy(&Options::default(), &path).unwrap();
}

#[test]
fn background_error_guard_survives_failed_open() {
    let parent = tempfile::tempdir().unwrap();
    let missing = parent.path().join("not-created");
    let mut options = Options::default();
    let guard = options.install_background_error_guard().unwrap();
    assert!(DB::open(&options, missing).is_err());
    assert!(!guard.failed());
    drop(options);
    assert!(!guard.failed());
}
