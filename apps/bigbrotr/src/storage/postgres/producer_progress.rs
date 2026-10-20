//! Typed incremental responsibility, separate from relay traversal and job completion.
//! Native ACK and receipt reclamation are not effects of this module.
use super::super::ingest_registry::ControlOwner;
use crate::{acquisition::session::RetainedRelayEvent, runtime::OwnedPermit};
use bigbrotr_model::{AcquisitionScopeId, DatasetId, FactRef, JobId, ProducerSlotId};
use sha2::{Digest, Sha256};

#[path = "producer_progress_commit.rs"]
mod commit;
#[path = "producer_progress_recovery.rs"]
pub(super) mod recovery;
#[path = "producer_progress_rows.rs"]
mod rows;
pub(super) use commit::{prepare, recheck, record};
pub(super) use rows::load;

/// The existing finite Sync receiver retains at most three correlated EVENTs.
/// This is a local position within one actual REQ, never a remote resume cursor.
const MAX_QUANTUM_EVENTS: u16 = 3;
pub(super) const MAX_PROGRESS_JOBS: usize = 64;
pub(super) const MAX_DEPENDENCIES: usize = 64 * 256;
pub(super) const DEPENDENCY_CHARGE: u64 = 256;
pub(super) const PROGRESS_CHARGE: u64 = 1024;
pub(super) const HEAD_CHARGE: u64 = 1024;
pub(super) const RETAINED_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerProgressError {
    InvalidPosition,
    InsufficientReservation,
    InvalidContext,
    Conflict,
    Limit,
    Backend,
    Native(crate::storage::rocks::store::AdmissionError),
    /// The checkpoint may exist. This never authorizes a new mutation identity.
    CommitUnknown,
}
impl std::fmt::Display for ProducerProgressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "producer progress unavailable: {self:?}")
    }
}
impl std::error::Error for ProducerProgressError {}

/// Capture from the protocol owner's retained correlated event. There is no
/// deserializer or caller-supplied request ID, wire sequence or input digest.
/// The operation grant lives through certification/admission and SQL completion.
pub struct SyncProgress {
    position: Position,
    _grant: ProgressGrant,
}

/// Retained for the complete fenced progress transaction. It is independent of
/// the native fact/input operation grants and does not grant producer credit.
pub struct ProgressGrant {
    _permit: OwnedPermit,
}
impl ProgressGrant {
    // Bounded SQL parameters/rows and the streaming closure reducer. Native
    // decoded facts and the retained result use their separate existing grants.
    pub const REQUIRED_DATA_BYTES: u64 = 2 * 1024 * 1024;

    pub fn new(permit: OwnedPermit) -> Result<Self, ProducerProgressError> {
        if permit.request().units().data_bytes() < Self::REQUIRED_DATA_BYTES
            || permit.request().units().items() == 0
        {
            return Err(ProducerProgressError::InsufficientReservation);
        }
        Ok(Self { _permit: permit })
    }
}
impl SyncProgress {
    pub fn capture(
        event: &RetainedRelayEvent<'_>,
        ordinal: u16,
        grant: ProgressGrant,
    ) -> Result<Self, ProducerProgressError> {
        if !(1..=MAX_QUANTUM_EVENTS).contains(&ordinal) || event.sequence() == 0 {
            return Err(ProducerProgressError::InvalidPosition);
        }
        let request = event.request().id();
        let request = request
            .as_str()
            .as_bytes()
            .try_into()
            .map_err(|_| ProducerProgressError::InvalidPosition)?;
        Ok(Self {
            position: Position {
                request,
                ordinal,
                wire_sequence: event.sequence(),
                input_digest: Sha256::digest(event.json()).into(),
            },
            _grant: grant,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::storage::postgres) struct Position {
    pub request: [u8; 48],
    pub ordinal: u16,
    pub wire_sequence: u64,
    pub input_digest: [u8; 32],
}

impl SyncProgress {
    pub(super) fn bind(
        &self,
        captured: Option<crate::acquisition::preparation::CapturedBinding>,
        source: Option<super::super::producer_sources::CurrentSourceCheck<'_>>,
    ) -> Result<BoundEvent<'_>, ProducerProgressError> {
        let captured = captured.ok_or(ProducerProgressError::InvalidContext)?;
        let source = source.ok_or(ProducerProgressError::InvalidContext)?;
        let job = source.job.ok_or(ProducerProgressError::InvalidContext)?;
        if captured.request != self.position.request
            || captured.wire_sequence != self.position.wire_sequence
            || captured.input_sha256 != self.position.input_digest
            || captured.source != source.binding
            || captured.job != job.job
            || captured.scope != job.scope
            || source
                .scope
                .is_none_or(|scope| scope.identity() != captured.scope)
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        Ok(BoundEvent {
            progress: self,
            captured,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct BoundEvent<'a> {
    progress: &'a SyncProgress,
    captured: crate::acquisition::preparation::CapturedBinding,
}
impl BoundEvent<'_> {
    pub(super) fn position(self) -> Position {
        self.progress.position
    }
    fn job(self) -> Job {
        Job {
            id: self.captured.job,
            scope: self.captured.scope,
        }
    }
}

/// Coordinates supplied only by the existing connected authority branch.
#[derive(Clone, Copy)]
pub(super) struct Context {
    pub dataset: DatasetId,
    pub slot: ProducerSlotId,
    pub epoch: u64,
    pub manifest: [u8; 32],
}

#[derive(Clone, Copy)]
pub(super) struct Job {
    pub id: JobId,
    pub scope: AcquisitionScopeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JobHead {
    job: JobId,
    scope: AcquisitionScopeId,
    revision: u64,
    position: Position,
    intent: [u8; 32],
    kind: u16,
    mutation: u64,
    classified: u64,
    successful: u64,
    rejected: u64,
    observed_at_us: i64,
    clock_resolution_us: u64,
    clock_anomaly_flags: u32,
}

/// One bounded allocation for the cumulative closure and one for the finite
/// current heads. No delivery history or vector of decoded native facts.
pub(super) struct Snapshot {
    revision: u64,
    complete: u64,
    reserved: u64,
    input_completion_bytes: u64,
    jobs: Vec<JobHead>,
    dependencies: Vec<FactRef>,
}

const JOB_BYTES: usize = 216;
const DEPENDENCY_BYTES: usize = 72;
const DIGEST_DOMAIN: &[u8] = b"bigbrotr/producer-progress/1\0";

// 64 typed current heads + 16,384 exact references, one SQL parameter/row
// scratch allowance and bounded transient headers fit this operation grant.
// The retained Control connection/driver and native decoded facts are charged
// independently by their existing owners; this is not process/PG physical RAM.
const _: () = assert!(
    MAX_PROGRESS_JOBS * std::mem::size_of::<JobHead>()
        + MAX_DEPENDENCIES * std::mem::size_of::<FactRef>()
        + 2 * 64 * 1024
        + std::mem::size_of::<Snapshot>()
        < ProgressGrant::REQUIRED_DATA_BYTES as usize
);
const _: () = assert!(JOB_BYTES <= PROGRESS_CHARGE as usize);
const _: () = assert!(DEPENDENCY_BYTES <= DEPENDENCY_CHARGE as usize);

fn digest(context: Context, snapshot: &Snapshot) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(DIGEST_DOMAIN);
    hash.update(context.dataset.as_bytes());
    hash.update(context.slot.as_bytes());
    hash.update(context.epoch.to_be_bytes());
    hash.update(context.manifest);
    hash.update(snapshot.revision.to_be_bytes());
    hash.update(snapshot.complete.to_be_bytes());
    hash.update((snapshot.jobs.len() as u64).to_be_bytes());
    hash.update((snapshot.dependencies.len() as u64).to_be_bytes());
    for head in &snapshot.jobs {
        hash.update(head.encode());
    }
    for reference in &snapshot.dependencies {
        hash.update(encode_dependency(*reference));
    }
    hash.finalize().into()
}

impl JobHead {
    fn encode(self) -> [u8; JOB_BYTES] {
        let mut bytes = [0; JOB_BYTES];
        let mut at = 0;
        for part in [
            self.job.as_bytes().as_slice(),
            self.scope.as_bytes().as_slice(),
            &self.revision.to_be_bytes(),
            &self.position.request,
            &self.position.ordinal.to_be_bytes(),
            &self.position.wire_sequence.to_be_bytes(),
            &self.position.input_digest,
            &self.intent,
            &self.kind.to_be_bytes(),
            &self.mutation.to_be_bytes(),
            &self.classified.to_be_bytes(),
            &self.successful.to_be_bytes(),
            &self.rejected.to_be_bytes(),
            &self.observed_at_us.to_be_bytes(),
            &self.clock_resolution_us.to_be_bytes(),
            &self.clock_anomaly_flags.to_be_bytes(),
        ] {
            bytes[at..at + part.len()].copy_from_slice(part);
            at += part.len();
        }
        bytes
    }
}

fn encode_dependency(reference: FactRef) -> [u8; DEPENDENCY_BYTES] {
    let id = reference.source_record_id;
    let mut bytes = [0; DEPENDENCY_BYTES];
    bytes[..16].copy_from_slice(id.dataset_id.as_bytes());
    bytes[16..24].copy_from_slice(&u64::from(id.shard_id).to_be_bytes());
    bytes[24..32].copy_from_slice(&id.owner_epoch.get().to_be_bytes());
    bytes[32..40].copy_from_slice(&id.sequence.get().to_be_bytes());
    bytes[40..].copy_from_slice(reference.payload_digest.as_bytes());
    bytes
}
impl Position {
    fn require_next(self, previous: Option<Self>) -> Result<(), ProducerProgressError> {
        if !(1..=MAX_QUANTUM_EVENTS).contains(&self.ordinal)
            || self.wire_sequence == 0
            || !self
                .request
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(ProducerProgressError::InvalidPosition);
        }
        match previous {
            Some(old) if old.request == self.request => {
                if old.ordinal.checked_add(1) != Some(self.ordinal)
                    || self.wire_sequence <= old.wire_sequence
                {
                    return Err(ProducerProgressError::Conflict);
                }
            }
            _ if self.ordinal != 1 => return Err(ProducerProgressError::Conflict),
            _ => {}
        }
        Ok(())
    }
}

/// Charge persistent logical records separately from the active mutation window.
/// These conservative row charges include the canonical typed payload, not a
/// claim about PostgreSQL disk pages, indexes, engine memory or process RSS.
fn retained_charge(
    input_and_completion: u64,
    progress_jobs: usize,
    dependencies: usize,
) -> Result<u64, ProducerProgressError> {
    if progress_jobs > MAX_PROGRESS_JOBS || dependencies > MAX_DEPENDENCIES {
        return Err(ProducerProgressError::Limit);
    }
    input_and_completion
        .checked_add(HEAD_CHARGE)
        .and_then(|n| n.checked_add(PROGRESS_CHARGE * progress_jobs as u64))
        .and_then(|n| n.checked_add(DEPENDENCY_CHARGE * dependencies as u64))
        .filter(|n| *n <= RETAINED_BYTES)
        .ok_or(ProducerProgressError::Limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(attempt: u8, ordinal: u16, sequence: u64) -> Position {
        Position {
            request: [attempt; 48],
            ordinal,
            wire_sequence: sequence,
            input_digest: [ordinal as u8; 32],
        }
    }

    #[test]
    fn local_prefix_cannot_skip_or_reorder_a_retained_event() {
        let first = position(b'a', 1, 7);
        assert_eq!(first.require_next(None), Ok(()));
        assert_eq!(position(b'a', 2, 11).require_next(Some(first)), Ok(()));
        for invalid in [
            position(b'a', 3, 12),
            position(b'a', 2, 7),
            position(b'a', 1, 12),
            position(b'a', 0, 12),
            position(b'a', 4, 12),
        ] {
            assert!(invalid.require_next(Some(first)).is_err());
        }
    }

    #[test]
    fn replacement_req_starts_a_new_local_prefix() {
        let previous = position(b'a', 2, 13);
        assert_eq!(position(b'b', 1, 2).require_next(Some(previous)), Ok(()));
        assert!(position(b'b', 2, 3).require_next(Some(previous)).is_err());
        assert!(position(b'G', 1, 2).require_next(Some(previous)).is_err());
    }

    #[test]
    fn retained_capacity_is_independent_of_window_credit() {
        let metadata = HEAD_CHARGE
            + PROGRESS_CHARGE * MAX_PROGRESS_JOBS as u64
            + DEPENDENCY_CHARGE * MAX_DEPENDENCIES as u64;
        assert_eq!(
            retained_charge(
                RETAINED_BYTES - metadata,
                MAX_PROGRESS_JOBS,
                MAX_DEPENDENCIES,
            ),
            Ok(RETAINED_BYTES)
        );
        assert_eq!(
            retained_charge(
                RETAINED_BYTES - metadata + 1,
                MAX_PROGRESS_JOBS,
                MAX_DEPENDENCIES,
            ),
            Err(ProducerProgressError::Limit)
        );
        assert!(retained_charge(0, MAX_PROGRESS_JOBS + 1, 0).is_err());
        assert!(retained_charge(0, 0, MAX_DEPENDENCIES + 1).is_err());
        assert!(retained_charge(u64::MAX, 0, 0).is_err());
    }
    fn golden_head() -> JobHead {
        JobHead {
            job: [11; 16].try_into().unwrap(),
            scope: [12; 16].try_into().unwrap(),
            revision: 2,
            position: Position {
                request: [b'a'; 48],
                ordinal: 1,
                wire_sequence: 3,
                input_digest: [13; 32],
            },
            intent: [14; 32],
            kind: 5,
            mutation: 0,
            classified: 2,
            successful: 2,
            rejected: 0,
            observed_at_us: -7,
            clock_resolution_us: 1,
            clock_anomaly_flags: 0,
        }
    }
    #[test]
    fn current_head_encoding_matches_independent_fixed_width_golden() {
        // Independently frozen with Python struct.pack big-endian fields, not
        // this encoder. Negative receive time exercises signed two's complement.
        let literal = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0000000000000002616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161000100000000000000030d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e00050000000000000000000000000000000200000000000000020000000000000000fffffffffffffff9000000000000000100000000";
        let expected = (0..literal.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&literal[at..at + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let head = golden_head();
        assert_eq!(head.encode().as_slice(), expected);
        assert_eq!(rows::decode_job(&expected).unwrap(), head);
        for changed in [
            expected[..215].to_vec(),
            [expected.as_slice(), &[0]].concat(),
        ] {
            assert!(rows::decode_job(&changed).is_err());
        }
        for (at, value) in [(89, 0), (163, 1), (171, 1), (195, 1), (211, 0), (215, 2)] {
            let mut bad = expected.clone();
            bad[at] = value;
            assert!(rows::decode_job(&bad).is_err(), "offset {at}");
        }
    }
    #[test]
    fn cumulative_digest_binds_proof_position_context_and_retained_closure() {
        use bigbrotr_model::{Digest256, OwnerEpoch, Sequence, SourceRecordId};
        let context = Context {
            dataset: [1; 16].try_into().unwrap(),
            slot: [2; 16].try_into().unwrap(),
            epoch: 3,
            manifest: [4; 32],
        };
        let mut snapshot = Snapshot {
            revision: 5,
            complete: 6,
            reserved: 6,
            input_completion_bytes: 0,
            jobs: vec![golden_head()],
            dependencies: vec![FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: context.dataset,
                    shard_id: 7,
                    owner_epoch: OwnerEpoch::new(8).unwrap(),
                    sequence: Sequence::new(9).unwrap(),
                },
                payload_digest: Digest256::from_bytes([10; 32]),
            }],
        };
        let expected: [u8; 32] = [
            0x36, 0x4c, 0xbd, 0xab, 0x83, 0x4d, 0x65, 0xfb, 0x71, 0x75, 0x00, 0xd9, 0x4e, 0x89,
            0xa3, 0x9a, 0xab, 0xcd, 0x69, 0x4f, 0xe1, 0x66, 0xaa, 0x56, 0xc3, 0x60, 0x6b, 0xe1,
            0xc5, 0x23, 0x87, 0xab,
        ];
        assert_eq!(digest(context, &snapshot), expected);
        assert_ne!(
            digest(
                Context {
                    epoch: 4,
                    ..context
                },
                &snapshot
            ),
            expected
        );
        assert_ne!(
            digest(
                Context {
                    manifest: [5; 32],
                    ..context
                },
                &snapshot
            ),
            expected
        );
        snapshot.jobs[0].position.request = [b'b'; 48];
        assert_ne!(digest(context, &snapshot), expected);
        snapshot.jobs[0] = golden_head();
        snapshot.dependencies.clear();
        assert_ne!(digest(context, &snapshot), expected);
        // A completed native mutation frontier is independent of an event's
        // successful proof-only classification: there is no inferred credit.
        assert_eq!(snapshot.complete, 6);
    }
}
