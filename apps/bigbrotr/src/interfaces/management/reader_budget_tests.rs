//! Ordinary reader funding only; no synthetic readiness or native observation.
use super::budget;
use bigbrotr::{
    management::credentials::HASH_RESERVATION,
    runtime::ResourceRequest,
    storage::{archive::RetainedInputs, postgres::control_owner::ArchiveReadExpectations},
};
use bigbrotr_protocol::archive_cut::MAX_BATCHES;

#[test]
fn complete_archive_selection_keeps_shared_byte_caps_and_owned_permits() {
    let configured = crate::config::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/recipes/archive-only.toml"),
    )
    .unwrap();
    assert_eq!(MAX_BATCHES, 128);
    assert_eq!(super::SESSION_ITEMS, 33);
    for cursor in [false, true] {
        let mut root = configured.clone();
        root.values
            .insert("archive.reader.enabled".into(), serde_json::json!(true));
        if cursor {
            root.values.insert(
                "read.cursor.keyring_ref".into(),
                serde_json::json!("file:/unused-test-keyring"),
            );
        }
        let fixed = 41 + u64::from(cursor);
        let old = crate::interfaces::archive_preflight::budget(&root, fixed).unwrap();
        let old_limits = old.snapshot().limits();
        // Source-exhibited red boundary: even without Control/HTTP/metadata,
        // the former ledger cannot retain the current 67 selected inputs.
        let mut held = Vec::new();
        for _ in 0..fixed {
            held.push(
                old.try_acquire(
                    ResourceRequest::data(RetainedInputs::required_data_bytes(), 1, 0).unwrap(),
                )
                .unwrap(),
            );
        }
        assert!(
            old.try_acquire(
                ResourceRequest::data(RetainedInputs::required_data_bytes(), 1, 0).unwrap()
            )
            .is_err()
        );
        assert!(
            old.try_acquire(
                ResourceRequest::data(RetainedInputs::required_data_bytes(), 0, 0).unwrap()
            )
            .is_ok()
        );
        assert!(held.len() < 67);
        drop(held);
        assert_eq!(old.snapshot().active_permits(), 0);

        for selected in [67, MAX_BATCHES] {
            let shared = budget(&root).unwrap();
            let limits = shared.snapshot().limits();
            assert_eq!(limits.items(), 169 + u64::from(cursor));
            assert_eq!(limits.data_bytes(), old_limits.data_bytes());
            assert_eq!(limits.native_bytes(), old_limits.native_bytes());
            assert_eq!(limits.control_bytes(), old_limits.control_bytes());
            // All original item lanes coexist with the complete retained input
            // cohort: Control/hash, metadata, manager/secret, native output,
            // connections/requests and optional cursor. Bytes stay accounted.
            let mut support = Vec::new();
            for bytes in [
                512 * 1024 * 1024,
                HASH_RESERVATION,
                ArchiveReadExpectations::required_data_bytes(),
                crate::interfaces::postgres_command::catalog_reservation(&root, 0).unwrap(),
                4096,
            ] {
                support.push(
                    shared
                        .try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
                        .unwrap(),
                );
            }
            support.push(
                shared
                    .try_acquire(ResourceRequest::data(0, 1, limits.native_bytes()).unwrap())
                    .unwrap(),
            );
            for _ in support.len() as u64..fixed {
                support.push(
                    shared
                        .try_acquire(ResourceRequest::data(4096, 1, 0).unwrap())
                        .unwrap(),
                );
            }
            let mut inputs = Vec::new();
            for _ in 0..selected {
                inputs.push(
                    shared
                        .try_acquire(
                            ResourceRequest::data(RetainedInputs::required_data_bytes(), 1, 0)
                                .unwrap(),
                        )
                        .unwrap(),
                );
            }
            assert_eq!(shared.snapshot().used().items(), fixed + selected as u64);
            assert!(
                shared
                    .try_acquire(ResourceRequest::data(0, 0, 1).unwrap())
                    .is_err()
            );
            assert!(
                shared
                    .try_acquire(ResourceRequest::data(u64::MAX, 0, 0).unwrap())
                    .is_err()
            );
            assert!(
                shared
                    .try_acquire(ResourceRequest::control(u64::MAX).unwrap())
                    .is_err()
            );
            if selected == MAX_BATCHES {
                assert!(
                    shared
                        .try_acquire(ResourceRequest::data(1, 1, 0).unwrap())
                        .is_err()
                );
            }
            let spare = limits.data_bytes() - shared.snapshot().used().data_bytes();
            let bytes = shared
                .try_acquire(ResourceRequest::data(spare, 0, 0).unwrap())
                .unwrap();
            assert!(
                shared
                    .try_acquire(ResourceRequest::data(1, 0, 0).unwrap())
                    .is_err()
            );
            let control = shared
                .try_acquire(ResourceRequest::control(limits.control_bytes()).unwrap())
                .unwrap();
            assert!(
                shared
                    .try_acquire(ResourceRequest::control(1).unwrap())
                    .is_err()
            );
            let retained = shared.snapshot().active_permits();
            shared.close();
            // Cancellation/closure prevents new admission but never revokes
            // grants beneath a pending probe. Actual owners must join/drop.
            assert!(
                shared
                    .try_acquire(ResourceRequest::data(1, 0, 0).unwrap())
                    .is_err()
            );
            assert_eq!(shared.snapshot().active_permits(), retained);
            drop(inputs.pop());
            assert_eq!(shared.snapshot().active_permits(), retained - 1);
            drop(inputs);
            drop(support);
            drop(bytes);
            drop(control);
            assert_eq!(shared.snapshot().active_permits(), 0);
            assert_eq!(shared.snapshot().used().data_bytes(), 0);
            assert_eq!(shared.snapshot().used().native_bytes(), 0);
            assert_eq!(shared.snapshot().used().control_bytes(), 0);
            shared.drain_until(std::time::Instant::now()).unwrap();
        }
        root.values
            .insert("archive.reader.enabled".into(), serde_json::json!(false));
        assert_eq!(
            budget(&root).unwrap().snapshot().limits().items(),
            35 + u64::from(cursor)
        );
    }
}
