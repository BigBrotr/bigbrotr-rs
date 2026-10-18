use super::*;
use crate::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::{
        archive::{
            FrontierIntentError, FrontierIntentScan, LocalCoordinator, RetainedFrontierIntent,
            frontier_intents::tests::{fixture_bytes, fixture_name, retain_fixture},
        },
        postgres::control_owner::ArchiveFrontierReceipt,
    },
};
use bigbrotr_model::Digest256;
use std::{os::unix::fs::symlink, sync::atomic::AtomicBool};
fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            4 << 20,
            0,
            ResourceLimits::new(4 << 20, 8, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn grant(b: &ResourceBudget, bytes: u64) -> crate::runtime::OwnedPermit {
    b.try_acquire(ResourceRequest::data(bytes, 1, 0).unwrap())
        .unwrap()
}
fn coordinator() -> (tempfile::TempDir, tempfile::TempDir, LocalCoordinator) {
    let (data, identity) = fixture();
    let (state, _) = fixture();
    let path = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = LocalCoordinator::from_custody(recover_released(&identity), &path, true).unwrap();
    (data, state, owner)
}
#[test]
fn frontier_intent_survives_lost_delivery_and_matches_only_original_result() {
    let (_data, state, owner) = coordinator();
    let identity = owner.identity().clone();
    let b = budget();
    let cancel = AtomicBool::new(false);
    let retained = retain_fixture(
        &owner,
        [4; 16],
        grant(&b, RetainedFrontierIntent::required_data_bytes()),
        &cancel,
    )
    .unwrap();
    let bytes = fs::read(state.path().join(fixture_name([4; 16]))).unwrap();
    // Lose local delivery without a PG result; a fresh coordinator still sees
    // the original ID. Neither this test nor the journal claims a SQL COMMIT.
    drop(retained);
    drop(owner);
    let owner = LocalCoordinator::from_custody(
        recover_released(&identity),
        &state.path().canonicalize().unwrap(),
        false,
    )
    .unwrap();
    let mut scan = owner
        .frontier_intents(
            grant(&b, FrontierIntentScan::required_data_bytes()),
            &cancel,
        )
        .unwrap();
    assert_eq!(scan.len(), 1);
    assert_eq!(scan.remaining_slots(), 255);
    assert_eq!(scan.remaining_directory_entries(), 1022);
    let retained = scan
        .read_next(
            grant(&b, RetainedFrontierIntent::required_data_bytes()),
            &cancel,
        )
        .unwrap()
        .unwrap();
    assert_eq!(retained.operation_id(), [4; 16]);
    assert_eq!(retained.expected_revision(), 5);
    let result = ArchiveFrontierReceipt {
        operation_id: [4; 16],
        consumer_id: identity.catalog(),
        revision: 6,
        snapshot_id: 7,
        request_digest: retained.observation().request_digest,
        family_counts: [0; 5],
        created: false,
    };
    assert!(retained.matches_receipt(&result));
    for field in 0..6 {
        let mut wrong = result;
        match field {
            0 => wrong.operation_id[0] ^= 1,
            1 => wrong.consumer_id = [9; 16].try_into().unwrap(),
            2 => wrong.revision += 1,
            3 => wrong.snapshot_id += 1,
            4 => wrong.request_digest = Digest256::from_bytes([9; 32]),
            _ => wrong.family_counts[0] += 1,
        };
        assert!(!retained.matches_receipt(&wrong));
    }
    drop(retained);
    drop(scan);
    drop(
        retain_fixture(
            &owner,
            [4; 16],
            grant(&b, RetainedFrontierIntent::required_data_bytes()),
            &cancel,
        )
        .unwrap(),
    );
    assert_eq!(
        fs::read(state.path().join(fixture_name([4; 16]))).unwrap(),
        bytes
    );
    let scan = owner
        .frontier_intents(
            grant(&b, FrontierIntentScan::required_data_bytes()),
            &cancel,
        )
        .unwrap();
    drop(owner);
    assert!(matches!(
        LocalInitialization::recover_for_boot(&identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(scan);
    drop(recover_released(&identity));
}
#[test]
fn partial_unknown_version_links_and_cancel_never_create_a_replacement() {
    for case in ["partial", "version", "symlink", "hardlink", "cancel"] {
        let (_data, state, owner) = coordinator();
        let b = budget();
        let cancel = AtomicBool::new(false);
        let path = state.path().join(fixture_name([4; 16]));
        let mut bytes = fixture_bytes(&owner, [4; 16]);
        match case {
            "partial" => bytes.truncate(80),
            "version" => bytes[7] = 2,
            _ => {}
        }
        let original = state.path().join("retained-original");
        if case == "symlink" || case == "hardlink" {
            fs::write(&original, &bytes).unwrap();
            fs::set_permissions(&original, fs::Permissions::from_mode(0o600)).unwrap();
            if case == "symlink" {
                symlink(&original, &path).unwrap();
            } else {
                fs::hard_link(&original, &path).unwrap();
            }
        } else {
            fs::write(&path, &bytes).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        if case == "cancel" {
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        assert!(
            owner
                .frontier_intents(
                    grant(&b, FrontierIntentScan::required_data_bytes()),
                    &cancel
                )
                .is_err(),
            "{case}"
        );
        assert!(
            retain_fixture(
                &owner,
                [5; 16],
                grant(&b, RetainedFrontierIntent::required_data_bytes()),
                &cancel
            )
            .is_err(),
            "{case}"
        );
        assert!(!state.path().join(fixture_name([5; 16])).exists());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn failed_serial_read_cannot_skip_an_original_and_grants_stay_separate() {
    let (_data, state, owner) = coordinator();
    let b = budget();
    let cancel = AtomicBool::new(false);
    drop(
        retain_fixture(
            &owner,
            [4; 16],
            grant(&b, RetainedFrontierIntent::required_data_bytes()),
            &cancel,
        )
        .unwrap(),
    );
    let path = state.path().join(fixture_name([4; 16]));
    let bytes = fs::read(&path).unwrap();
    for insufficient_grant in [false, true] {
        let mut scan = owner
            .frontier_intents(
                grant(&b, FrontierIntentScan::required_data_bytes()),
                &cancel,
            )
            .unwrap();
        if !insufficient_grant {
            fs::write(&path, &bytes[..80]).unwrap();
        }
        let required = if insufficient_grant {
            1
        } else {
            RetainedFrontierIntent::required_data_bytes()
        };
        assert!(scan.read_next(grant(&b, required), &cancel).is_err());
        assert_eq!(scan.observed_through(), 0);
        // Restoring bytes here models a separate fixture operator, not journal
        // repair. This existing cursor remains spent even after that change.
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            scan.read_next(
                grant(&b, RetainedFrontierIntent::required_data_bytes()),
                &cancel,
            ),
            Err(FrontierIntentError::Unavailable)
        ));
    }
    let mut scan = owner
        .frontier_intents(
            grant(&b, FrontierIntentScan::required_data_bytes()),
            &cancel,
        )
        .unwrap();
    let intent = scan
        .read_next(
            grant(&b, RetainedFrontierIntent::required_data_bytes()),
            &cancel,
        )
        .unwrap()
        .unwrap();
    assert_eq!(scan.observed_through(), 1);
    assert_eq!(intent.operation_id(), [4; 16]);
    assert!(
        scan.read_next(
            grant(&b, RetainedFrontierIntent::required_data_bytes()),
            &cancel,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(fs::read(path).unwrap(), bytes);
}
#[test]
fn directory_and_intent_caps_refuse_without_deleting_or_overrunning_admission() {
    for intent_cap in [false, true] {
        let (_data, state, owner) = coordinator();
        let b = budget();
        let cancel = AtomicBool::new(false);
        let count = if intent_cap { 256 } else { 1023 }; // coordinator.json is the other entry.
        for index in 0..count {
            let (name, bytes) = if intent_cap {
                let mut id = [7; 16];
                id[14..].copy_from_slice(&(index as u16).to_be_bytes());
                (fixture_name(id), fixture_bytes(&owner, id))
            } else {
                (format!("unrelated-{index}"), Vec::new())
            };
            let path = state.path().join(name);
            fs::write(&path, &bytes).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let scan = owner
            .frontier_intents(
                grant(&b, FrontierIntentScan::required_data_bytes()),
                &cancel,
            )
            .unwrap();
        assert_eq!(scan.remaining_slots(), 0);
        assert_eq!(
            scan.remaining_directory_entries(),
            if intent_cap { 767 } else { 0 }
        );
        drop(scan);
        assert!(matches!(
            retain_fixture(
                &owner,
                [8; 16],
                grant(&b, RetainedFrontierIntent::required_data_bytes()),
                &cancel
            ),
            Err(FrontierIntentError::ResourceLimit)
        ));
        assert_eq!(fs::read_dir(state.path()).unwrap().count(), count + 1);
        let (name, bytes) = if intent_cap {
            (fixture_name([8; 16]), fixture_bytes(&owner, [8; 16]))
        } else {
            ("one-more".to_owned(), Vec::new())
        };
        let path = state.path().join(name);
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            owner.frontier_intents(
                grant(&b, FrontierIntentScan::required_data_bytes()),
                &cancel
            ),
            Err(FrontierIntentError::ResourceLimit)
        ));
    }
}
