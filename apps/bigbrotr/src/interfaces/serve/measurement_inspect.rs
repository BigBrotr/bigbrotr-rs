//! Trusted-host exact A05 inspection. Structural operands confer no read grant.
use crate::{config::Resolved, interfaces::management::ActionGate};
use bigbrotr::{
    runtime::{ResourceBudget, ResourceRequest},
    storage::{archive::ArchivedMeasurement, postgres::control_owner::ArchiveReadSelection},
};
use bigbrotr_model::{Digest256, FactRef, OwnerEpoch, Sequence, SourceRecordId};
use bigbrotr_protocol::archive_measurement::{MAX_ROW_BYTES, MeasurementFactRow};
use clap::{Arg, ArgMatches, Command};
use serde::Serialize;
use std::{
    io::{self, Write},
    os::fd::{AsFd, OwnedFd},
    sync::atomic::{AtomicBool, Ordering},
};

type Result<T> = std::result::Result<T, (u8, String)>;
const OUTPUT_BYTES: usize = MAX_ROW_BYTES + 4096;
const PIPE_WRITE_BYTES: usize = 4096;

pub(super) fn command() -> Command {
    Command::new("measurement-inspect")
        .about("Read one original compound measurement at the current complete published Archive cut")
        .arg(Arg::new("file").long("file").required(true).value_name("PATH"))
        .arg(Arg::new("shard-id").long("shard-id").required(true)
            .value_parser(clap::value_parser!(u32)))
        .arg(Arg::new("owner-epoch").long("owner-epoch").required(true)
            .value_parser(clap::value_parser!(u64).range(1..)))
        .arg(Arg::new("sequence").long("sequence").required(true)
            .value_parser(clap::value_parser!(u64).range(1..)))
        .arg(Arg::new("payload-digest").long("payload-digest").required(true)
            .value_name("HEX64").value_parser(clap::value_parser!(Digest256)))
        .after_help("Stop the local coordinator first. Requires local Control/Archive, explicit archive.reader.enabled, complete retained publication inputs, actual catalog/pin custody, a separate SELECT-only catalog reader and the matching installed worker. Dataset comes from the deployment root; no caller-supplied snapshot or fallback to latest. Trusted local operators can inspect this dataset, including private measurements; this is not a public or management-session read grant. Recomputes the exact scientific digest and strong prior-cut closure. Missing, conflicting, out-of-cut or unreadable facts refuse; no successful Absent state. One bounded scientific JSON document is delivered while Control and the actual pin remain held, followed by a cleanup status only after joined work and owner release. Linux pipe/FIFO stdout only: pipe through cat for terminal/file output. At most128 catalog receipts,30 seconds overall and20KiB scientific output; accepted process budgets remain binding. Exit:0 delivered and released,1 local/output failure,2 usage,3 preflight/policy,4 refused/cancelled/unresolved. Preserve obligations on an unknown join or COMMIT.")
}

pub(super) fn parse(config: &Resolved, args: &ArgMatches) -> Result<FactRef> {
    let identity = config.catalog_identity().map_err(|e| (3, e))?;
    if !config.local("control") {
        return Err((
            3,
            "measurement inspection requires the local Control role".into(),
        ));
    }
    for key in [
        "postgres.catalog.read_user",
        "postgres.catalog.read_password_ref",
    ] {
        if config.text(key).is_none() {
            return Err((3, format!("{key}: required for measurement inspection")));
        }
    }
    Ok(FactRef {
        source_record_id: SourceRecordId {
            dataset_id: identity.dataset(),
            shard_id: *args.get_one::<u32>("shard-id").unwrap(),
            owner_epoch: OwnerEpoch::new(*args.get_one::<u64>("owner-epoch").unwrap())
                .map_err(|_| (3, "invalid measurement owner epoch".into()))?,
            sequence: Sequence::new(*args.get_one::<u64>("sequence").unwrap())
                .map_err(|_| (3, "invalid measurement sequence".into()))?,
        },
        payload_digest: *args.get_one::<Digest256>("payload-digest").unwrap(),
    })
}

pub(super) fn authorize(config: &Resolved) -> Result<()> {
    if !config.local("control")
        || !config.local("archive")
        || !config.flag("archive.reader.enabled")
    {
        return Err((3, "measurement inspection requires accepted archive.reader.enabled and local Control/Archive".into()));
    }
    Ok(())
}

/// Reopen an independently owned nonblocking FIFO description. Setting flags
/// on a dup would also change the inherited stdout and its diagnostic writer.
struct OutputPipe(OwnedFd);
impl OutputPipe {
    #[cfg(target_os = "linux")]
    fn open(output: impl AsFd) -> io::Result<Self> {
        use rustix::fs::{FileType, Mode, OFlags, fcntl_getfl, fstat, open};
        use std::os::fd::AsRawFd;
        let retained = rustix::io::fcntl_dupfd_cloexec(output, 0)?;
        let before = fstat(&retained)?;
        if FileType::from_raw_mode(before.st_mode) != FileType::Fifo {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "measurement output requires a pipe; use | cat for a terminal or file",
            ));
        }
        let inherited = fcntl_getfl(&retained)?;
        let pipe = open(
            format!("/proc/self/fd/{}", retained.as_raw_fd()),
            OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOCTTY,
            Mode::empty(),
        )?;
        let after = fstat(&pipe)?;
        if (before.st_dev, before.st_ino, before.st_mode)
            != (after.st_dev, after.st_ino, after.st_mode)
            || !fcntl_getfl(&pipe)?.contains(OFlags::NONBLOCK)
            || fcntl_getfl(&retained)? != inherited
        {
            return Err(io::Error::other("measurement output pipe identity changed"));
        }
        Ok(Self(pipe))
    }

    #[cfg(not(target_os = "linux"))]
    fn open(_: impl AsFd) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "pinned measurement output requires Linux nonblocking pipes",
        ))
    }
}
impl Write for OutputPipe {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        rustix::io::write(&self.0, bytes).map_err(Into::into)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn validate_output() -> Result<()> {
    OutputPipe::open(std::io::stdout())
        .map(|_| ())
        .map_err(|error| (3, error.to_string()))
}

struct Buffer(Vec<u8>);
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > OUTPUT_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("measurement output limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Serialize)]
struct Document<'a> {
    format: &'static str,
    state: &'static str,
    audience: &'static str,
    catalog_id: bigbrotr_model::OwnerResourceId,
    frontier_revision: String,
    topology_generation: bigbrotr_model::TopologyGeneration,
    policy_revision: String,
    cut: &'a bigbrotr_protocol::archive_cut::CutObservation,
    fact: MeasurementFactRow,
}
fn encode(
    selection: &ArchiveReadSelection,
    cut: &bigbrotr_protocol::archive_cut::CutObservation,
    fact: &bigbrotr_model::Fact,
    policy_revision: u64,
) -> io::Result<Buffer> {
    let mut bytes = Buffer(Vec::new());
    bytes
        .0
        .try_reserve_exact(OUTPUT_BYTES)
        .map_err(io::Error::other)?;
    let document = Document {
        format: "bigbrotr-archive-measurement/1",
        state: "present",
        audience: "trusted-local-operator",
        catalog_id: selection.catalog_id,
        frontier_revision: selection.frontier_revision.to_string(),
        topology_generation: selection.topology_generation,
        policy_revision: policy_revision.to_string(),
        cut,
        fact: MeasurementFactRow::from_fact(fact).map_err(io::Error::other)?,
    };
    serde_json::to_writer(&mut bytes, &document).map_err(io::Error::other)?;
    bytes.write_all(b"\n")?;
    Ok(bytes)
}

/// Only the existing joined blocking supervisor invokes this. Its caller retains
/// the actual pin/barrier and this observation's charged fact/custody throughout.
pub(super) fn deliver(
    selection: &ArchiveReadSelection,
    observed: &ArchivedMeasurement,
    policy_revision: u64,
    budget: &ResourceBudget,
    cancelled: &AtomicBool,
    window: &ActionGate,
) -> std::result::Result<(), super::native_grants::WorkerError> {
    use super::native_grants::{WorkerError, unavailable};
    let _output = budget
        .try_acquire(ResourceRequest::data((OUTPUT_BYTES * 2) as u64, 1, 0).map_err(unavailable)?)
        .map_err(unavailable)?;
    let bytes = encode(selection, observed.cut(), observed.fact(), policy_revision)
        .map_err(|_| WorkerError::OutputFailure)?;
    let mut stdout = OutputPipe::open(std::io::stdout()).map_err(|_| WorkerError::OutputFailure)?;
    write_document(&mut stdout, &bytes.0, cancelled, || window.check().is_ok())
        .map_err(|_| WorkerError::OutputFailure)
}

fn write_document(
    output: &mut impl Write,
    mut bytes: &[u8],
    cancel: &AtomicBool,
    permitted: impl Fn() -> bool,
) -> io::Result<()> {
    while !bytes.is_empty() {
        if cancel.load(Ordering::Acquire) || !permitted() {
            return Err(io::Error::other("measurement output cancelled"));
        }
        match output.write(&bytes[..bytes.len().min(PIPE_WRITE_BYTES)]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => bytes = &bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(error) => return Err(error),
        }
    }
    if cancel.load(Ordering::Acquire) || !permitted() {
        return Err(io::Error::other("measurement output cancelled"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::{Fact, codec::FactDecodeLimits};
    use bigbrotr_protocol::archive_cut::CutObservation;

    #[test]
    fn accepted_policy_is_restored_without_inventing_a_reader_root_or_public_audience() {
        use crate::config::policy::PolicySeed;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("root.toml");
        let base = include_str!("../../../../../config/recipes/archive-only.toml");
        std::fs::write(&path, base).unwrap();
        let disabled = crate::config::load(&path).unwrap();
        let seed = PolicySeed::from_resolved(&disabled).unwrap();
        assert!(authorize(&disabled).is_err());
        let enabled = base.replace(
            "[postgres.catalog]\n",
            "[postgres.catalog]\nread_user='reader'\nread_password_ref='file:/absent/reader'\n",
        ) + "\n[archive.reader]\nenabled=true\n";
        std::fs::write(&path, enabled).unwrap();
        let root = crate::config::load(&path).unwrap();
        let accepted = PolicySeed::restore(seed.document(), seed.digest(), root).unwrap();
        authorize(&accepted).unwrap();
        assert_eq!(
            PolicySeed::from_resolved(&accepted).unwrap().digest(),
            seed.digest()
        );
        assert!(accepted.flag("administration.enabled")); // no listener/session or grant is constructed
        assert!(
            authorize(&PolicySeed::restore(seed.document(), seed.digest(), disabled).unwrap())
                .is_err()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn exact_identity_operands_cannot_choose_a_dataset_snapshot_or_zero_source() {
        let base = [
            "measurement-inspect",
            "--file",
            "/absent",
            "--shard-id",
            "0",
            "--owner-epoch",
            "1",
            "--sequence",
            "1",
            "--payload-digest",
            "1111111111111111111111111111111111111111111111111111111111111111",
        ];
        let args = command().try_get_matches_from(base).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("root.toml");
        let document = include_str!("../../../../../config/recipes/archive-only.toml").replace(
            "[postgres.catalog]\n",
            "[postgres.catalog]\nread_user='reader'\nread_password_ref='file:/absent/reader'\n",
        );
        std::fs::write(&path, document).unwrap();
        let config = crate::config::load(&path).unwrap();
        let fact = parse(&config, &args).unwrap();
        assert_eq!(
            fact.source_record_id.dataset_id,
            config.catalog_identity().unwrap().dataset()
        );
        assert_eq!(fact.source_record_id.shard_id, 0);
        assert_eq!(fact.source_record_id.sequence.get(), 1);
        for (index, value) in [(6, "0"), (8, "0"), (10, "ABC"), (10, "gggg")] {
            let mut bad = base;
            bad[index] = value;
            assert!(command().try_get_matches_from(bad).is_err());
        }
        for extra in [
            "--snapshot-id",
            "--dataset-id",
            "--max-facts",
            "--worker-root",
        ] {
            assert!(
                command()
                    .try_get_matches_from(base.into_iter().chain([extra, "1"]))
                    .is_err()
            );
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn independent_originals_serialize_exact_digest_nulls_and_signed_samples() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../crates/bigbrotr-model/tests/fixtures/measurement-v1.json"
        ))
        .unwrap();
        let mut seen_null = false;
        let mut seen_negative = false;
        for literal in vectors["valid"].as_array().unwrap() {
            let bytes: Vec<u8> = literal["preimage_hex"]
                .as_str()
                .unwrap()
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let original = Fact::decode_preimage(
                &bytes,
                literal["sha256"].as_str().unwrap().parse().unwrap(),
                FactDecodeLimits::default(),
            )
            .unwrap();
            let source = original.source_record_id();
            let selection = ArchiveReadSelection {
                dataset_id: source.dataset_id,
                catalog_id: [91; 16].try_into().unwrap(),
                frontier_revision: 7,
                snapshot_id: 9,
                topology_generation: bigbrotr_model::TopologyGeneration::new(1),
                bounds: vec![],
            };
            let cut = CutObservation {
                format: bigbrotr_protocol::archive_cut::FORMAT.into(),
                snapshot_id: 9,
                request_digest: Digest256::from_bytes([42; 32]),
                family_counts: [8, 0, 0, 0, 0, 1],
            };
            let wire = encode(&selection, &cut, &original, 3).unwrap().0;
            assert!(wire.len() <= OUTPUT_BYTES);
            assert_eq!(wire.last(), Some(&b'\n'));
            let document: serde_json::Value = serde_json::from_slice(&wire).unwrap();
            assert_eq!(document["format"], "bigbrotr-archive-measurement/1");
            assert_eq!(document["cut"]["snapshot_id"], 9);
            assert_eq!(document["frontier_revision"], "7");
            assert!(document.get("owner_release").is_none());
            assert_eq!(document["fact"]["payload_digest"], literal["sha256"]);
            let row: MeasurementFactRow = serde_json::from_value(document["fact"].clone()).unwrap();
            assert_eq!(row.to_fact().unwrap().encode_preimage(8192).unwrap(), bytes);
            let rendered = std::str::from_utf8(&wire).unwrap();
            seen_null |= rendered.contains("\"observed_at\":null");
            seen_negative |= rendered.contains("\"observed_at\":\"-");
        }
        assert!(seen_null);
        assert!(seen_negative);
    }

    #[test]
    fn output_has_a_fixed_total_bound_and_cancelled_or_partial_delivery_cannot_succeed() {
        let mut buffer = Buffer(Vec::new());
        buffer.write_all(&vec![0; OUTPUT_BYTES]).unwrap();
        assert!(buffer.write_all(b"x").is_err());
        assert_eq!(buffer.0.len(), OUTPUT_BYTES);
        let cancelled = AtomicBool::new(true);
        let mut output = Vec::new();
        assert!(write_document(&mut output, b"private", &cancelled, || true).is_err());
        assert!(output.is_empty());
        struct Partial<'a>(&'a AtomicBool, Vec<u8>);
        impl Write for Partial<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.1.push(bytes[0]);
                self.0.store(true, Ordering::Release);
                Ok(1)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        cancelled.store(false, Ordering::Release);
        let mut output = Partial(&cancelled, Vec::new());
        assert!(write_document(&mut output, b"private", &cancelled, || true).is_err());
        assert_eq!(output.1, b"p");
        cancelled.store(false, Ordering::Release);
        let mut final_byte = Partial(&cancelled, Vec::new());
        assert!(write_document(&mut final_byte, b"p", &cancelled, || true).is_err());
        assert_eq!(final_byte.1, b"p");
        cancelled.store(false, Ordering::Release);
        let mut output = Vec::new();
        write_document(&mut output, b"complete\n", &cancelled, || true).unwrap();
        assert_eq!(output, b"complete\n");
    }

    #[cfg(target_os = "linux")]
    mod pipe_tests {
        use super::*;
        use bigbrotr::runtime::policy_authority::{
            LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl,
        };
        use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
        use std::{
            os::fd::OwnedFd,
            sync::{Arc, mpsc},
            thread,
            time::{Duration, Instant},
        };

        fn gate(ttl: Duration) -> ActionGate {
            let dataset = [11; 16].try_into().unwrap();
            let incarnation = [12; 16].try_into().unwrap();
            let epoch = bigbrotr_model::PolicyEpoch::new(1);
            let mut lease = PolicyLeaseTracker::new(
                dataset,
                incarnation,
                epoch,
                PolicyLeaseTtl::new(Duration::from_secs(5)).unwrap(),
            )
            .unwrap();
            let ticket = lease.begin_refresh().unwrap();
            lease
                .complete_refresh(
                    &ticket,
                    LeaseResponse {
                        dataset,
                        process_incarnation: incarnation,
                        policy_epoch: epoch,
                        ttl,
                    },
                )
                .unwrap();
            let window = lease.begin_action(epoch).unwrap().complete(ttl).unwrap();
            let gate = ActionGate::new();
            gate.bind(window).unwrap();
            gate
        }

        fn fill(pipe: &mut OutputPipe) -> usize {
            let mut count = 0;
            loop {
                match pipe.write(&[b'x'; PIPE_WRITE_BYTES]) {
                    Ok(n) => {
                        assert_eq!(n, PIPE_WRITE_BYTES);
                        count += n;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    other => panic!("unexpected fill result: {other:?}"),
                }
                assert!(count <= 1024 * 1024);
            }
            assert!(count > 0);
            count
        }

        fn drain(reader: &OwnedFd) -> Vec<u8> {
            fcntl_setfl(reader, fcntl_getfl(reader).unwrap() | OFlags::NONBLOCK).unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut buffer = [0; PIPE_WRITE_BYTES];
                match rustix::io::read(reader, &mut buffer) {
                    Ok(0) => break,
                    Ok(n) => bytes.extend_from_slice(&buffer[..n]),
                    Err(rustix::io::Errno::AGAIN) => break,
                    other => panic!("unexpected drain result: {other:?}"),
                }
                assert!(bytes.len() <= 1024 * 1024 + OUTPUT_BYTES);
            }
            bytes
        }

        struct ObservedPipe(OutputPipe, Arc<AtomicBool>);
        impl Write for ObservedPipe {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                let result = self.0.write(bytes);
                if result
                    .as_ref()
                    .is_err_and(|e| e.kind() == io::ErrorKind::WouldBlock)
                {
                    self.1.store(true, Ordering::Release);
                }
                result
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        #[test]
        fn full_pipe_cannot_publish_after_actual_window_expiry_or_cancellation() {
            for expiry in [true, false] {
                let (reader, writer) = rustix::pipe::pipe().unwrap();
                let original_flags = fcntl_getfl(&writer).unwrap();
                let mut output = OutputPipe::open(&writer).unwrap();
                let filled = fill(&mut output);
                let observed = Arc::new(AtomicBool::new(false));
                let cancel = Arc::new(AtomicBool::new(false));
                let window = gate(Duration::from_secs(2));
                let worker_window = window.clone();
                let worker_cancel = cancel.clone();
                let mut output = ObservedPipe(output, observed.clone());
                let (done, result) = mpsc::sync_channel(1);
                let worker = thread::spawn(move || {
                    let value = write_document(&mut output, b"private\n", &worker_cancel, || {
                        worker_window.check().is_ok()
                    });
                    done.send(value).unwrap();
                });
                let deadline = Instant::now() + Duration::from_secs(3);
                while !observed.load(Ordering::Acquire) {
                    assert!(
                        Instant::now() < deadline,
                        "worker never encountered the full pipe"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                assert!(
                    window.check().is_ok(),
                    "full pipe must be reached before expiry"
                );
                if expiry {
                    while window.check().is_ok() {
                        assert!(Instant::now() < deadline);
                        thread::sleep(Duration::from_millis(2));
                    }
                    assert!(!cancel.load(Ordering::Acquire)); // no supervisor is helping
                } else {
                    cancel.store(true, Ordering::Release);
                    assert!(window.check().is_ok());
                }
                // Draining after expiry/withdrawal must not revive blocked work.
                let mut bytes = drain(&reader);
                let ended = result.recv_timeout(Duration::from_secs(1));
                if ended.is_err() {
                    drop(reader);
                    cancel.store(true, Ordering::Release);
                    panic!("nonblocking output worker did not join");
                }
                let ended = ended.expect("nonblocking output worker did not join");
                worker.join().unwrap();
                assert!(ended.is_err());
                // Include anything accepted after the first drain and before
                // the joined refusal; a late write must not become a false PASS.
                bytes.extend(drain(&reader));
                assert_eq!(bytes, vec![b'x'; filled]);
                assert_eq!(fcntl_getfl(&writer).unwrap(), original_flags);
            }
        }

        #[test]
        fn draining_pipe_delivers_exact_document_without_mutating_inherited_flags() {
            let (reader, writer) = rustix::pipe::pipe().unwrap();
            let original_flags = fcntl_getfl(&writer).unwrap();
            let mut output = OutputPipe::open(&writer).unwrap();
            let filled = fill(&mut output);
            let expected = [vec![b'p'; OUTPUT_BYTES - 1], vec![b'\n']].concat();
            let document = expected.clone();
            let window = gate(Duration::from_secs(2));
            let cancel = AtomicBool::new(false);
            let worker = thread::spawn(move || {
                write_document(&mut output, &document, &cancel, || window.check().is_ok())
            });
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut actual = Vec::new();
            while actual.len() < filled + expected.len() {
                assert!(
                    Instant::now() < deadline,
                    "positive output did not complete"
                );
                actual.extend(drain(&reader));
                thread::sleep(Duration::from_millis(1));
            }
            worker.join().unwrap().unwrap();
            assert_eq!(actual, [vec![b'x'; filled], expected].concat());
            assert_eq!(fcntl_getfl(&writer).unwrap(), original_flags);
        }

        #[test]
        fn regular_output_is_refused_without_writing_or_changing_flags() {
            let output = tempfile::tempfile().unwrap();
            let before = fcntl_getfl(&output).unwrap();
            assert!(OutputPipe::open(&output).is_err());
            assert_eq!(output.metadata().unwrap().len(), 0);
            assert_eq!(fcntl_getfl(&output).unwrap(), before);
        }
    }
}
