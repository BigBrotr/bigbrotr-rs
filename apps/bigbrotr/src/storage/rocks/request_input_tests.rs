//! Actual native owner plus retained producer input boundaries.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request};
use super::*;
use crate::runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits};
use crate::storage::rocks::manifest::AllocationNamespaces;
use bigbrotr_model::{
    DescriptorLabel, DictionaryDescriptor, DictionaryEntry, DictionaryKind, EvidenceIntent,
    EvidenceIntentLimits, ObservedTime, ProducerRequestId, RequestSequence,
};
const MIB: u64 = 1024 * 1024;

#[path = "lineage_input_tests.rs"]
mod lineage_input_tests;

#[test]
fn producer_input_is_retained_before_effects_and_recovers_exact_original_intent() {
    use bigbrotr_model::codec::request::{MutationIntent, RequestDecodeLimits};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let original = intent("public");
    let digest = original.request_digest(request());
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let before = store.db.latest_sequence_number();
    let mut checks = 0;
    let result = store.bootstrap_dictionaries(
        request(),
        &original,
        ObservedTime::new(123),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || {
            checks += 1;
            if checks > 1 {
                return Err(BootstrapError::AuthorityUnavailable);
            }
            Ok(())
        },
    );
    assert!(matches!(result, Err(BootstrapError::AuthorityUnavailable)));
    assert_eq!(store.db.latest_sequence_number(), before);
    drop(store);
    let mut store =
        IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let limits = RequestDecodeLimits {
        values: bigbrotr_model::codec::FactDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 65536,
            max_tag_nodes: 128,
            max_string_bytes: 8192,
        },
        max_dictionaries: 8,
    };
    let recovered = store
        .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
        .unwrap();
    let (id, recovered, _permit) = recovered.into_parts();
    assert_eq!(id, request());
    let MutationIntent::Evidence(recovered) = recovered else {
        panic!("wrong operation")
    };
    assert_eq!(recovered, original);
    assert_eq!(store.db.latest_sequence_number(), before);
    // Recovered bytes drive the real mutation primitive. This test callback
    // supplies only fixture authority, not a new production incarnation.
    let receipt = store
        .bootstrap_dictionaries(
            id,
            &recovered,
            ObservedTime::new(456),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        )
        .unwrap();
    let expected = receipt.receipt().encode_persistent(8192).unwrap();
    let copy = receipt
        .native_outcome()
        .expect("owner-verified commit witness");
    assert_eq!(copy.bytes(), expected);
    assert_eq!(copy.request(), id);
    assert_eq!(copy.digest(), digest);
    assert_eq!(copy.dataset(), manifest().identity().dataset_id);
    assert_eq!(copy.kind(), NativeOutcomeKind::NewFacts);
    let inspected = store
        .read_receipt(
            id,
            digest,
            bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
                max_encoded_bytes: 8192,
                max_allocation_bytes: 8192,
                max_batch_records: 8,
                max_dependencies: 8,
            },
            grant(&b, MIB, 0),
            &std::sync::atomic::AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();
    assert!(
        inspected.native_outcome().is_none(),
        "structural reads cannot mint a commit witness"
    );
    drop(inspected);
    drop(receipt);
    let committed = store.db.latest_sequence_number();
    assert!(committed > before);
    let replay = store
        .bootstrap_dictionaries(
            id,
            &recovered,
            ObservedTime::new(789),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(replay.receipt().encode_persistent(8192).unwrap(), expected);
    assert_eq!(replay.native_outcome().unwrap().bytes(), expected);
    assert_eq!(store.db.latest_sequence_number(), committed);
}

#[test]
fn complete_signed_and_rejected_inputs_survive_native_admission_and_reopen() {
    use bigbrotr_model::{
        DatasetLineageDescriptor, Digest256, EventOrigin,
        codec::request::{MutationIntent, RequestDecodeLimits, decode_request},
    };
    fn hex(v: &str) -> Vec<u8> {
        v.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    }
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../crates/bigbrotr-model/tests/fixtures/signed-request-v1.json"
    ))
    .unwrap();
    let input = hex(fixture["preimage_hex"].as_str().unwrap());
    let digest: Digest256 = fixture["sha256"].as_str().unwrap().parse().unwrap();
    let (id, event) = decode_request(&input, digest, RequestDecodeLimits::default()).unwrap();
    let MutationIntent::Event(event) = event else {
        panic!()
    };
    let EventOrigin::Import(lineage) = event.origin() else {
        panic!()
    };
    let scope = event.visibility_scope_id();
    let manifest = IngestManifest::new(
        *manifest().identity(),
        AllocationNamespaces::from_entries(&[
            (DictionaryKind::VisibilityScope, scope.allocation_domain),
            (DictionaryKind::Event, [8; 16].try_into().unwrap()),
            (DictionaryKind::DatasetLineage, lineage.allocation_domain),
        ])
        .unwrap(),
    );
    let boot = EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                scope,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("public".into()).unwrap(),
                },
            )
            .unwrap(),
            DictionaryEntry::new(
                lineage,
                DictionaryDescriptor::DatasetLineage(DatasetLineageDescriptor {
                    format: DescriptorLabel::new("jsonl".into()).unwrap(),
                    format_version: 1,
                    source_dataset_id: None,
                    source_artifact_digest: fixture["source_sha256"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap(),
                }),
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 2,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    let b = ResourceBudget::new(
        ProcessEnvelope::new(
            256 * MIB,
            0,
            ResourceLimits::new(96 * MIB, 8, 144 * MIB, 16 * MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let mut store =
        IngestStore::initialize(&path, manifest, native(), grant(&b, 0, 128 * MIB)).unwrap();
    let bootstrap_id = ProducerRequestId {
        request_sequence: RequestSequence::new(1).unwrap(),
        ..id
    };
    drop(
        store
            .bootstrap_dictionaries(
                bootstrap_id,
                &boot,
                ObservedTime::new(123),
                BootstrapLimits {
                    max_mappings: 8,
                    max_batch_bytes: MIB as usize,
                },
                grant(&b, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let outcome = store
        .admit_event(
            PreparedEvent::new(id, event, 256 * 1024, grant(&b, MIB, 0)).unwrap(),
            ObservedTime::new(456),
            false,
            AdmissionLimits::default(),
            grant(&b, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
        panic!("valid signature rejected")
    };
    assert_eq!(receipt.request_digest(), digest);
    assert_eq!(receipt.batch().unwrap().records().len(), 3);
    let receipt_bytes = receipt.encode_persistent(8192).unwrap();
    drop(outcome);
    let committed = store.db.latest_sequence_number();
    let replay = store
        .bootstrap_dictionaries(
            bootstrap_id,
            &boot,
            ObservedTime::new(999),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        )
        .expect("bootstrap outcome remains recoverable after later events");
    assert_eq!(replay.receipt().request_id(), bootstrap_id);
    assert_eq!(replay.receipt().batch().unwrap().records().len(), 2);
    assert!(replay.native_outcome().is_some());
    assert_eq!(store.db.latest_sequence_number(), committed);
    drop(replay);
    let rejection: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/rejected-request.json"
    ))
    .unwrap();
    let rejected_input = hex(rejection["request_preimage_hex"].as_str().unwrap());
    let rejected_digest: Digest256 = rejection["request_digest"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let (rejected_id, rejected) = decode_request(
        &rejected_input,
        rejected_digest,
        RequestDecodeLimits::default(),
    )
    .unwrap();
    let MutationIntent::Event(rejected) = rejected else {
        panic!()
    };
    let rejected = store
        .admit_event(
            PreparedEvent::new(rejected_id, rejected, 256 * 1024, grant(&b, MIB, 0)).unwrap(),
            ObservedTime::new(789),
            false,
            AdmissionLimits::default(),
            grant(&b, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        rejected.outcome(),
        &AdmissionOutcome::Rejected(RejectionReason::Signature)
    );
    let copy = rejected.native_outcome();
    assert_eq!(copy.request(), rejected_id);
    assert_eq!(copy.digest(), rejected_digest);
    assert_eq!(copy.dataset(), scope.dataset_id);
    assert_eq!(copy.kind(), NativeOutcomeKind::Rejected);
    assert_eq!(copy.policy_epoch().get(), 1);
    let key = super::super::keys::IngestKey::Request {
        request_id: rejected_id,
    }
    .encode()
    .unwrap();
    let mut historic = store
        .db
        .get_cf(store.db.cf_handle("receipts").unwrap(), key.as_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(copy.bytes(), historic);
    drop(rejected);
    // A historical v1 rejection under a different policy: replay must retain its
    // original full-u64 context, not synthesize today's manifest epoch (1).
    let end = historic.len();
    historic[end - 9..end - 1].copy_from_slice(&u64::MAX.to_be_bytes());
    store
        .db
        .put_cf(
            store.db.cf_handle("receipts").unwrap(),
            key.as_bytes(),
            &historic,
        )
        .unwrap();
    drop(store);
    let mut store =
        IngestStore::reopen(&path, manifest, native(), grant(&b, 0, 128 * MIB)).unwrap();
    let limits = RequestDecodeLimits {
        values: bigbrotr_model::codec::FactDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 65536,
            max_tag_nodes: 128,
            max_string_bytes: 8192,
        },
        max_dictionaries: 8,
    };
    for (request, digest, expected) in [
        (id, digest, input),
        (rejected_id, rejected_digest, rejected_input),
    ] {
        let (found, recovered, permit) = store
            .read_request_input(request, digest, limits, grant(&b, MIB, 0))
            .unwrap()
            .into_parts();
        assert_eq!(found, request);
        let MutationIntent::Event(recovered) = recovered else {
            panic!()
        };
        assert_eq!(
            recovered.encode_request_preimage(found, 8192).unwrap(),
            expected
        );
        let before = store.db.latest_sequence_number();
        let outcome = store
            .admit_event(
                PreparedEvent::new(found, recovered, 256 * 1024, permit).unwrap(),
                ObservedTime::new(999),
                false,
                AdmissionLimits::default(),
                grant(&b, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        if request == id {
            let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
                panic!()
            };
            assert_eq!(receipt.encode_persistent(8192).unwrap(), receipt_bytes);
        } else {
            assert_eq!(
                outcome.outcome(),
                &AdmissionOutcome::Rejected(RejectionReason::Signature)
            );
            assert_eq!(outcome.native_outcome().policy_epoch().get(), u64::MAX);
            assert_eq!(outcome.native_outcome().bytes(), historic);
        }
        assert_eq!(store.db.latest_sequence_number(), before);
    }
}

#[test]
fn staged_input_conflict_partial_file_and_reduced_limits_preserve_native_state() {
    use bigbrotr_model::codec::{CodecError, request::RequestDecodeLimits};
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let original = intent("public");
    let digest = original.request_digest(request());
    let input = original.encode_request_preimage(request(), 8192).unwrap();
    let before = store.db.latest_sequence_number();
    let mut checks = 0;
    let result = store.bootstrap_dictionaries(
        request(),
        &original,
        ObservedTime::new(1),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || {
            checks += 1;
            if checks > 1 {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(result, Err(BootstrapError::AuthorityUnavailable)));
    let filename=path.join("producer-inputs-v1/05050505050505050505050505050505-0000000000000001-0000000000000001.cbor");
    assert_eq!(std::fs::read(&filename).unwrap(), input);
    let result = store.bootstrap_dictionaries(
        request(),
        &intent("private"),
        ObservedTime::new(2),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || Ok(()),
    );
    assert!(matches!(
        result,
        Err(BootstrapError::RequestInput(RequestInputError::Conflict))
    ));
    assert_eq!(std::fs::read(&filename).unwrap(), input);
    assert_eq!(store.db.latest_sequence_number(), before);
    let mut limits = RequestDecodeLimits {
        values: bigbrotr_model::codec::FactDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 65536,
            max_tag_nodes: 128,
            max_string_bytes: 8192,
        },
        max_dictionaries: 8,
    };
    for (allocation, expected) in [
        (false, CodecError::InputLimit),
        (true, CodecError::AllocationLimit),
    ] {
        let mut small = limits;
        if allocation {
            small.values.max_allocation_bytes = 0;
        } else {
            small.values.max_encoded_bytes = input.len() - 1;
        }
        assert_eq!(
            store
                .read_request_input(request(), digest, small, grant(&b, MIB, 0))
                .err(),
            Some(RequestInputError::Codec(expected))
        );
        drop(
            store
                .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
                .unwrap(),
        );
    }
    std::fs::write(&filename, &input[..10]).unwrap();
    assert!(
        store
            .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
            .is_err()
    );
    let result = store.bootstrap_dictionaries(
        request(),
        &original,
        ObservedTime::new(3),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || Ok(()),
    );
    assert!(matches!(
        result,
        Err(BootstrapError::RequestInput(RequestInputError::Conflict))
    ));
    assert_eq!(std::fs::read(&filename).unwrap(), input[..10]);
    assert_eq!(store.db.latest_sequence_number(), before);
    // Restore this intentionally damaged test fixture, then test the actual permissions.
    std::fs::write(&filename, &input).unwrap();
    std::fs::set_permissions(&filename, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        store
            .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
            .err(),
        Some(RequestInputError::Identity)
    );
    std::fs::set_permissions(&filename, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(
        store
            .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
            .unwrap(),
    );
    limits.values.max_encoded_bytes = usize::MAX;
    assert_eq!(
        RetainedRequestInput::required_data_bytes(limits),
        Err(RequestInputError::Limit)
    );
}

#[test]
fn restrictive_umask_cannot_commit_an_unreadable_producer_input() {
    const CHILD: &str = "BIGBROTR_PRODUCER_INPUT_UMASK_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","storage::rocks::store::request_input_tests::restrictive_umask_cannot_commit_an_unreadable_producer_input","--nocapture"])
            .env(CHILD,"1").output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use bigbrotr_model::codec::request::RequestDecodeLimits;
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let spool = path.join("producer-inputs-v1");
    std::fs::create_dir(&spool).unwrap();
    std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
    let original = intent("public");
    let digest = original.request_digest(request());
    let old = rustix::process::umask(rustix::fs::Mode::WUSR);
    let result = store.bootstrap_dictionaries(
        request(),
        &original,
        ObservedTime::new(1),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || Ok(()),
    );
    rustix::process::umask(old);
    drop(result.unwrap());
    let limits = RequestDecodeLimits {
        values: bigbrotr_model::codec::FactDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 65536,
            max_tag_nodes: 128,
            max_string_bytes: 8192,
        },
        max_dictionaries: 8,
    };
    drop(
        store
            .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
            .unwrap(),
    );
}

#[test]
fn retained_spool_count_and_byte_limits_allow_exact_retry_without_reclamation() {
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let original = intent("public");
    for n in 1..=64 {
        let id = ProducerRequestId {
            request_sequence: RequestSequence::new(n).unwrap(),
            ..request()
        };
        let bytes = original.encode_request_preimage(id, 8192).unwrap();
        store.retain_request_input(id, &bytes).unwrap();
    }
    let id = ProducerRequestId {
        request_sequence: RequestSequence::new(65).unwrap(),
        ..request()
    };
    assert_eq!(
        store.retain_request_input(id, &original.encode_request_preimage(id, 8192).unwrap()),
        Err(RequestInputError::Limit)
    );
    store
        .retain_request_input(
            request(),
            &original.encode_request_preimage(request(), 8192).unwrap(),
        )
        .unwrap();
    assert_eq!(
        std::fs::read_dir(path.join("producer-inputs-v1"))
            .unwrap()
            .count(),
        64
    );
    drop(store);
    // A separate actual filesystem boundary: two retained 16MiB entries exhaust
    // the byte envelope even though the count envelope has room. Sparse files
    // measure logical staging limits only, not resident disk capacity or ENOSPC.
    let path = root.path().join("bytes");
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    store
        .retain_request_input(
            request(),
            &original.encode_request_preimage(request(), 8192).unwrap(),
        )
        .unwrap();
    let spool = path.join("producer-inputs-v1");
    for n in 1..=2 {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(spool.join(format!(
                "{}-0000000000000001-{n:016x}.cbor",
                request().producer_slot_id
            )))
            .unwrap();
        file.set_len(16 * MIB).unwrap();
    }
    let next = ProducerRequestId {
        request_sequence: RequestSequence::new(3).unwrap(),
        ..request()
    };
    assert_eq!(
        store.retain_request_input(next, &original.encode_request_preimage(next, 8192).unwrap()),
        Err(RequestInputError::Limit)
    );
    assert_eq!(std::fs::read_dir(spool).unwrap().count(), 2);
}

#[test]
fn substituted_input_paths_are_refused_without_following_or_mutating_them() {
    use std::{
        fs::OpenOptions,
        os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink},
    };
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let original = intent("public");
    let input = original.encode_request_preimage(request(), 8192).unwrap();
    let filename = "05050505050505050505050505050505-0000000000000001-0000000000000001.cbor";
    for mode in [
        "directory-symlink",
        "file-symlink",
        "hardlink",
        #[cfg(target_os = "linux")]
        "fifo",
    ] {
        let path = root.path().join(mode);
        let mut store =
            IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        let spool = path.join("producer-inputs-v1");
        let outside = root.path().join(format!("outside-{mode}"));
        std::fs::create_dir(&outside).unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o700)).unwrap();
        let external = outside.join(filename);
        use std::io::Write;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&external)
            .unwrap()
            .write_all(&input)
            .unwrap();
        if mode == "directory-symlink" {
            symlink(&outside, &spool).unwrap();
        } else {
            std::fs::create_dir(&spool).unwrap();
            std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
            match mode {
                "file-symlink" => symlink(&external, spool.join(filename)).unwrap(),
                "hardlink" => std::fs::hard_link(&external, spool.join(filename)).unwrap(),
                #[cfg(target_os = "linux")]
                "fifo" => rustix::fs::mknodat(
                    rustix::fs::CWD,
                    spool.join(filename),
                    rustix::fs::FileType::Fifo,
                    rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                    0,
                )
                .unwrap(),
                _ => unreachable!(),
            }
        }
        let before = store.db.latest_sequence_number();
        let result = store.bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(123),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        );
        assert!(
            matches!(result, Err(BootstrapError::RequestInput(_))),
            "{mode}"
        );
        assert_eq!(store.db.latest_sequence_number(), before);
        assert_eq!(std::fs::read(&external).unwrap(), input);
    }
}

#[test]
fn killed_producer_preserves_input_before_and_after_native_commit() {
    use bigbrotr_model::codec::request::{MutationIntent, RequestDecodeLimits};
    use std::{
        io::{BufRead, Write},
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };
    const CHILD: &str = "BIGBROTR_INPUT_CRASH_CHILD";
    if let Some(mode) = std::env::var_os(CHILD) {
        let path = std::path::PathBuf::from(std::env::var_os("BIGBROTR_INPUT_CRASH_PATH").unwrap());
        let b = budget();
        let mut store =
            IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        let original = intent("public");
        let mut checks = 0;
        let suspend = || {
            println!("STAGED-BOUNDARY");
            std::io::stdout().flush().unwrap();
            std::thread::sleep(Duration::from_secs(30));
            panic!("parent did not kill child")
        };
        let result = store.bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(123),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || {
                checks += 1;
                if mode == "before" && checks == 2 {
                    suspend();
                }
                Ok(())
            },
        );
        drop(result.unwrap());
        suspend();
    }
    let root = tempfile::tempdir().unwrap();
    for mode in ["before", "after"] {
        let path = root.path().join(mode);
        let mut child=Command::new(std::env::current_exe().unwrap()).args(["--exact","storage::rocks::store::request_input_tests::killed_producer_preserves_input_before_and_after_native_commit","--nocapture"])
            .env(CHILD,mode).env("BIGBROTR_INPUT_CRASH_PATH",&path).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap() == "STAGED-BOUNDARY" {
                    send.send(()).unwrap();
                    return;
                }
            }
        });
        let reached = receive.recv_timeout(Duration::from_secs(10));
        child.kill().unwrap();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        assert!(reached.is_ok(), "child did not reach {mode} boundary");
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
        let b = budget();
        let store =
            IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        let original = intent("public");
        let digest = original.request_digest(request());
        let limits = RequestDecodeLimits {
            values: bigbrotr_model::codec::FactDecodeLimits {
                max_encoded_bytes: 8192,
                max_allocation_bytes: 65536,
                max_tag_nodes: 128,
                max_string_bytes: 8192,
            },
            max_dictionaries: 8,
        };
        let (id, input, _permit) = store
            .read_request_input(request(), digest, limits, grant(&b, MIB, 0))
            .unwrap()
            .into_parts();
        assert_eq!(id, request());
        let MutationIntent::Evidence(input) = input else {
            panic!()
        };
        assert_eq!(input, original);
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/ingest/staged-bootstrap-crash.json"
        ))
        .unwrap();
        assert_eq!(
            hex(&input.encode_request_preimage(id, 8192).unwrap()),
            fixture["request_hex"]
        );
        let mut rows = Vec::new();
        for family in ["default", "facts", "receipts"] {
            for row in store.db.iterator_cf(
                store.db.cf_handle(family).unwrap(),
                rocksdb::IteratorMode::Start,
            ) {
                let (key, value) = row.unwrap();
                rows.push(serde_json::json!([family, hex(&key), hex(&value)]));
            }
        }
        assert_eq!(
            serde_json::Value::Array(rows),
            fixture[mode],
            "all {mode} native keys/values"
        );
    }
}

#[test]
fn pre_reservation_staging_has_no_native_effect_and_retains_exact_identity() {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let original = intent("public");
    let limits = BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    };
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let before = store.db.latest_sequence_number();
    assert!(matches!(
        store.stage_initial_dictionaries(request(), &original, limits, &grant(&b, 1, 1)),
        Err(BootstrapError::InsufficientReservation)
    ));
    assert!(!path.join("producer-inputs-v1").exists());
    let permit = grant(&b, 8 * MIB, 4 * MIB);
    let staged = store
        .stage_initial_dictionaries(request(), &original, limits, &permit)
        .unwrap();
    assert_eq!(staged.request, request());
    assert_eq!(staged.dataset, manifest().identity().dataset_id);
    assert_eq!(staged.digest, original.request_digest(request()));
    let bytes = original.encode_request_preimage(request(), 8192).unwrap();
    assert_eq!(staged.input_bytes as usize, bytes.len());
    assert_eq!(
        staged.manifest_sha256,
        <[u8; 32]>::from(Sha256::digest(manifest().encode().as_bytes()))
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    let retry = store
        .stage_initial_dictionaries(request(), &original, limits, &permit)
        .unwrap();
    assert_eq!(retry.digest, staged.digest);
    assert_eq!(store.db.latest_sequence_number(), before);
    assert!(matches!(
        store.stage_initial_dictionaries(request(), &intent("changed"), limits, &permit),
        Err(BootstrapError::RequestInput(RequestInputError::Conflict))
    ));
    assert_eq!(store.db.latest_sequence_number(), before);
}
