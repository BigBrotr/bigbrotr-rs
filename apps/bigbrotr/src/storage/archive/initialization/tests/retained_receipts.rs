//! Refusal tests for the retained overloads. These deliberately cannot launch a
//! native executable: both resource lanes must be admitted before verification.
use super::*;
use crate::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::{
        archive::{
            ArtifactIdentity, CatalogReaderCredentials, CatalogRoute, CatalogTls, LocalCoordinator,
            Preparation, PreparationError, PreparationLimits, ReceiptError, WorkerBudgets,
            WorkerInstallation,
        },
        postgres::connection_settings::PlaintextBoundary,
    },
};
use bigbrotr_model::Digest256;
use bigbrotr_protocol::archive_membership::MembershipRead;
use std::{
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

fn budget(native: u64, control: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            native + control,
            0,
            ResourceLimits::new(0, 2, native, control),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}

#[test]
fn retained_receipt_and_membership_keep_custody_and_separate_unavailable_lanes() {
    for membership in [false, true] {
        for exhausted_native in [false, true] {
            let (_data, identity) = fixture();
            let (state, _) = fixture();
            let state = state.path().canonicalize().unwrap();
            drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
            let owner =
                LocalCoordinator::from_custody(recover_released(&identity), &state, true).unwrap();
            let custody = owner.reader_custody().unwrap();
            drop(owner);
            let value: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../../../../tests/fixtures/protocol/archive-membership.json"
            ))
            .unwrap();
            let mut read: MembershipRead = serde_json::from_value(value["read"].clone()).unwrap();
            read.receipt.dataset_id = identity.dataset();
            let scratch_owner = tempfile::tempdir().unwrap();
            fs::set_permissions(scratch_owner.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let scratch = scratch_owner.path().canonicalize().unwrap();
            let installation = WorkerInstallation {
                root: scratch.with_extension("absent-worker"),
                executable: ArtifactIdentity {
                    bytes: 1,
                    sha256: Digest256::from_bytes([1; 32]),
                },
            };
            let application = budget(0, 128 * 1024);
            let native = budget(512 << 20, 0);
            let occupied = if exhausted_native {
                native
                    .try_acquire(ResourceRequest::data(0, 1, 1).unwrap())
                    .unwrap()
            } else {
                application
                    .try_acquire(ResourceRequest::control(1).unwrap())
                    .unwrap()
            };
            let cancelled = AtomicBool::new(false);
            let preparation = Preparation {
                installation: &installation,
                temp_root: &scratch,
                limits: PreparationLimits {
                    memory_bytes: 128 << 20,
                    native_reserve_bytes: 384 << 20,
                    temp_bytes: 64 << 20,
                    threads: 1,
                    max_records: 128,
                    max_encoded_bytes: 1 << 20,
                },
                deadline: Instant::now() + Duration::from_secs(5),
                cancelled: &cancelled,
            };
            let route = CatalogRoute {
                address: "127.0.0.1".parse().unwrap(),
                port: 5432,
                database: "archive",
                tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
            };
            let credential = CatalogReaderCredentials {
                login: "reader",
                password: "fixture-only",
            };
            let budgets = WorkerBudgets {
                application: &application,
                native: &native,
            };
            let result = if membership {
                preparation
                    .inspect_membership_retained(&read, &custody, &route, credential, budgets, 4)
                    .map(|_| ())
            } else {
                preparation
                    .inspect_receipt_retained(&read.receipt, &custody, &route, credential, budgets)
                    .map(|_| ())
            };
            assert_eq!(
                result,
                Err(ReceiptError::Preparation(PreparationError::Unavailable))
            );
            assert_eq!(
                native.snapshot().active_permits(),
                u64::from(exhausted_native)
            );
            assert_eq!(
                application.snapshot().active_permits(),
                u64::from(!exhausted_native)
            );
            assert_eq!(
                native.snapshot().used().native_bytes(),
                u64::from(exhausted_native)
            );
            assert_eq!(
                application.snapshot().used().control_bytes(),
                u64::from(!exhausted_native)
            );
            assert!(matches!(
                LocalInitialization::recover_for_boot(&identity, BOOT),
                Err(LifetimeError::Occupied)
            ));
            assert!(fs::read_dir(&scratch).unwrap().next().is_none());
            drop(occupied);
            drop(custody);
            drop(recover_released(&identity));
        }
    }
}
