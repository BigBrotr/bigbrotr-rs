//! Canonical OPEN-prefix data. Decoding supplies no native or SQL authority.
use sha2::{Digest, Sha256};

pub(crate) const MAX_BASE_BYTES: usize = 16 * 1024;
pub(crate) const BASE_SLOTS_CHARGE: u64 = 2 * MAX_BASE_BYTES as u64;
pub(crate) const MAX_JOBS: usize = 64;
pub(crate) const JOB_BYTES: usize = 216;
const DOMAIN: &[u8] = b"bigbrotr/producer-prefix/1\0";
const FIXED_BYTES: usize = 325;
const _: () = assert!(FIXED_BYTES + MAX_JOBS * JOB_BYTES == 14_149);
const _: () = assert!(FIXED_BYTES + MAX_JOBS * JOB_BYTES <= MAX_BASE_BYTES);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InvalidPrefix;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Context {
    pub dataset: [u8; 16],
    pub slot: [u8; 16],
    pub epoch: u64,
    pub manifest: [u8; 32],
}
impl Context {
    pub fn bytes(self) -> [u8; 72] {
        let mut bytes = [0; 72];
        bytes[..16].copy_from_slice(&self.dataset);
        bytes[16..32].copy_from_slice(&self.slot);
        bytes[32..40].copy_from_slice(&self.epoch.to_be_bytes());
        bytes[40..].copy_from_slice(&self.manifest);
        bytes
    }
    fn validate(self) -> Result<(), InvalidPrefix> {
        if self.dataset == [0; 16] || self.slot == [0; 16] || self.epoch != 1 {
            return Err(InvalidPrefix);
        }
        Ok(())
    }
    pub fn source_seed(self) -> [u8; 32] {
        hash(&[b"bigbrotr/producer-prefix-source/1\0", &self.bytes()])
    }
    pub fn request_seed(self) -> [u8; 32] {
        hash(&[b"bigbrotr/producer-prefix-requests/1\0", &self.bytes()])
    }
    pub fn close_digest(
        self,
        issued: u64,
        requests: [u8; 32],
        source: u64,
        batches: u64,
        science: [u8; 32],
    ) -> [u8; 32] {
        hash(&[
            b"bigbrotr/producer-prefix-close/1\0",
            &self.bytes(),
            &issued.to_be_bytes(),
            &requests,
            &source.to_be_bytes(),
            &batches.to_be_bytes(),
            &science,
        ])
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BaseRef {
    pub generation: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Base {
    pub context: Context,
    pub generation: u64,
    pub previous: [u8; 32],
    pub retired: u64,
    pub source: u64,
    pub issuer_resource: [u8; 16],
    pub issuer_epoch: u64,
    pub issuer_incarnation: [u8; 16],
    pub policy_epoch: u64,
    pub batches: u64,
    pub science: [u8; 32],
    pub requests: [u8; 32],
    pub progress_format: u16,
    pub progress_revision: u64,
    pub progress_digest: [u8; 32],
    pub jobs: Vec<[u8; JOB_BYTES]>,
}
impl Base {
    pub fn validate(&self) -> Result<(), InvalidPrefix> {
        self.context.validate()?;
        if self.generation == 0
            || self.retired == 0
            || self.generation > self.retired
            || (self.generation == 1) != (self.previous == [0; 32])
            || self.issuer_resource == [0; 16]
            || self.issuer_epoch == 0
            || self.issuer_incarnation == [0; 16]
            || self.policy_epoch == 0
            || self.progress_revision == 0
            || self.jobs.len() > MAX_JOBS
            || self.progress_format != if self.generation == 1 { 1 } else { 2 }
            || (self.source == 0) != (self.batches == 0)
            || self.batches > self.source
            || self.batches > self.retired
            || (self.source == 0 && self.science != self.context.source_seed())
        {
            return Err(InvalidPrefix);
        }
        for (index, job) in self.jobs.iter().enumerate() {
            validate_job(job, self.retired)?;
            if index > 0 && self.jobs[index - 1][..16] >= job[..16] {
                return Err(InvalidPrefix);
            }
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, InvalidPrefix> {
        self.validate()?;
        let count = FIXED_BYTES + self.jobs.len() * JOB_BYTES;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(count).map_err(|_| InvalidPrefix)?;
        bytes.extend_from_slice(DOMAIN);
        bytes.extend_from_slice(&self.context.bytes());
        for part in [
            self.generation.to_be_bytes().as_slice(),
            &self.previous,
            &self.retired.to_be_bytes(),
            &self.source.to_be_bytes(),
            &self.issuer_resource,
            &self.issuer_epoch.to_be_bytes(),
            &self.issuer_incarnation,
            &self.policy_epoch.to_be_bytes(),
            &self.batches.to_be_bytes(),
            &self.science,
            &self.requests,
            &self.progress_format.to_be_bytes(),
            &self.progress_revision.to_be_bytes(),
            &self.progress_digest,
            &(self.jobs.len() as u64).to_be_bytes(),
        ] {
            bytes.extend_from_slice(part);
        }
        for job in &self.jobs {
            bytes.extend_from_slice(job);
        }
        if bytes.len() != count {
            return Err(InvalidPrefix);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], context: Context) -> Result<Self, InvalidPrefix> {
        if bytes.len() < FIXED_BYTES || bytes.len() > MAX_BASE_BYTES || !bytes.starts_with(DOMAIN) {
            return Err(InvalidPrefix);
        }
        let mut cursor = Cursor(&bytes[DOMAIN.len()..]);
        if cursor.take::<72>()? != context.bytes() {
            return Err(InvalidPrefix);
        }
        let mut base = Self {
            context,
            generation: cursor.number()?,
            previous: cursor.take()?,
            retired: cursor.number()?,
            source: cursor.number()?,
            issuer_resource: cursor.take()?,
            issuer_epoch: cursor.number()?,
            issuer_incarnation: cursor.take()?,
            policy_epoch: cursor.number()?,
            batches: cursor.number()?,
            science: cursor.take()?,
            requests: cursor.take()?,
            progress_format: u16::from_be_bytes(cursor.take()?),
            progress_revision: cursor.number()?,
            progress_digest: cursor.take()?,
            jobs: Vec::new(),
        };
        let count = usize::try_from(cursor.number()?).map_err(|_| InvalidPrefix)?;
        if count > MAX_JOBS || cursor.0.len() != count * JOB_BYTES {
            return Err(InvalidPrefix);
        }
        base.jobs
            .try_reserve_exact(count)
            .map_err(|_| InvalidPrefix)?;
        for _ in 0..count {
            base.jobs.push(cursor.take()?);
        }
        base.validate()?;
        Ok(base)
    }
    pub fn reference(&self) -> Result<BaseRef, InvalidPrefix> {
        Ok(BaseRef {
            generation: self.generation,
            sha256: Sha256::digest(self.encode()?).into(),
        })
    }
    pub fn require_successor(&self, prior: Option<&Self>) -> Result<(), InvalidPrefix> {
        self.validate()?;
        match prior {
            None if self.generation == 1 => Ok(()),
            Some(prior)
                if prior.context == self.context
                    && prior.generation.checked_add(1) == Some(self.generation)
                    && prior.reference()?.sha256 == self.previous
                    && prior.retired < self.retired
                    && prior.source <= self.source
                    && prior.batches <= self.batches
                    && (prior.source != self.source
                        || (prior.batches == self.batches && prior.science == self.science)) =>
            {
                self.require_job_successors(&prior.jobs)
            }
            _ => Err(InvalidPrefix),
        }
    }
    pub fn require_job_successors(
        &self,
        previous: &[[u8; JOB_BYTES]],
    ) -> Result<(), InvalidPrefix> {
        for old in previous {
            let index = self
                .jobs
                .binary_search_by(|job| job[..16].cmp(&old[..16]))
                .map_err(|_| InvalidPrefix)?;
            let new = &self.jobs[index];
            if new[16..32] != old[16..32]
                || [32, 172, 180, 188]
                    .iter()
                    .any(|at| number(new, *at) < number(old, *at))
                || (number(new, 32) == number(old, 32) && new != old)
            {
                return Err(InvalidPrefix);
            }
        }
        Ok(())
    }
}

/// Exact v4 bytes. PostgreSQL authentication is an additional connected check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NativeFloor {
    pub issued: u64,
    pub acknowledged: u64,
    pub retired: u64,
    pub closing: bool,
    pub base: BaseRef,
}
impl NativeFloor {
    pub fn validate(self) -> Result<(), InvalidPrefix> {
        if self.retired == 0
            || self.retired > self.acknowledged
            || self.acknowledged > self.issued
            || self.base.generation == 0
        {
            return Err(InvalidPrefix);
        }
        Ok(())
    }
    pub fn encode(self) -> Result<[u8; 67], InvalidPrefix> {
        self.validate()?;
        let mut bytes = [0; 67];
        bytes[..2].copy_from_slice(&[4, 0x31]);
        bytes[2..10].copy_from_slice(&self.issued.to_be_bytes());
        bytes[10..18].copy_from_slice(&self.acknowledged.to_be_bytes());
        bytes[18..26].copy_from_slice(&self.retired.to_be_bytes());
        bytes[26] = u8::from(self.closing);
        bytes[27..35].copy_from_slice(&self.base.generation.to_be_bytes());
        bytes[35..].copy_from_slice(&self.base.sha256);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, InvalidPrefix> {
        if bytes.len() != 67 || bytes[..2] != [4, 0x31] || bytes[26] > 1 {
            return Err(InvalidPrefix);
        }
        let floor = Self {
            issued: number(bytes, 2),
            acknowledged: number(bytes, 10),
            retired: number(bytes, 18),
            closing: bytes[26] == 1,
            base: BaseRef {
                generation: number(bytes, 27),
                sha256: bytes[35..].try_into().unwrap(),
            },
        };
        floor.validate()?;
        Ok(floor)
    }
    pub fn require_base(self, base: &Base) -> Result<(), InvalidPrefix> {
        self.validate()?;
        if self.retired != base.retired || self.base != base.reference()? {
            return Err(InvalidPrefix);
        }
        Ok(())
    }
}

pub(crate) fn next_request(
    previous: [u8; 32],
    sequence: u64,
    input: [u8; 32],
    outcome: [u8; 32],
) -> [u8; 32] {
    hash(&[
        b"bigbrotr/producer-prefix-request/1\0",
        &previous,
        &sequence.to_be_bytes(),
        &input,
        &outcome,
    ])
}
pub(crate) fn next_batch(previous: [u8; 32], first: u64, last: u64, digest: [u8; 32]) -> [u8; 32] {
    hash(&[
        b"bigbrotr/producer-prefix-batch/1\0",
        &previous,
        &first.to_be_bytes(),
        &last.to_be_bytes(),
        &digest,
    ])
}
fn hash(parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}
fn number(bytes: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(
        bytes[at..at + 8]
            .try_into()
            .expect("validated fixed record"),
    )
}
fn validate_job(job: &[u8; JOB_BYTES], complete: u64) -> Result<(), InvalidPrefix> {
    let ordinal = u16::from_be_bytes(job[88..90].try_into().unwrap());
    let kind = u16::from_be_bytes(job[162..164].try_into().unwrap());
    let flags = u32::from_be_bytes(job[212..].try_into().unwrap());
    let mutation = number(job, 164);
    if job[..16] == [0; 16]
        || job[16..32] == [0; 16]
        || !job[40..88]
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        || !(1..=3).contains(&ordinal)
        || number(job, 90) == 0
        || number(job, 32) == 0
        || number(job, 32) != number(job, 172)
        || number(job, 180).checked_add(number(job, 188)) != Some(number(job, 172))
        || number(job, 172) < u64::from(ordinal)
        || !(2..=6).contains(&kind)
        || ((kind <= 4) != (mutation != 0))
        || mutation > complete
        || (matches!(kind, 2 | 3 | 5) && number(job, 180) == 0)
        || (matches!(kind, 4 | 6) && number(job, 188) == 0)
        || number(job, 204) == 0
        || flags & !5 != 0
        || ((flags & 4 != 0) != (number(job, 204) > 1))
    {
        return Err(InvalidPrefix);
    }
    Ok(())
}
struct Cursor<'a>(&'a [u8]);
impl Cursor<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], InvalidPrefix> {
        let (head, tail) = self.0.split_at_checked(N).ok_or(InvalidPrefix)?;
        self.0 = tail;
        head.try_into().map_err(|_| InvalidPrefix)
    }
    fn number(&mut self) -> Result<u64, InvalidPrefix> {
        Ok(u64::from_be_bytes(self.take()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context() -> Context {
        Context {
            dataset: [1; 16],
            slot: [2; 16],
            epoch: 1,
            manifest: [3; 32],
        }
    }
    fn base() -> Base {
        Base {
            context: context(),
            generation: 1,
            previous: [0; 32],
            retired: 64,
            source: 7,
            issuer_resource: [8; 16],
            issuer_epoch: 2,
            issuer_incarnation: [9; 16],
            policy_epoch: 3,
            batches: 1,
            science: [4; 32],
            requests: [5; 32],
            progress_format: 1,
            progress_revision: 65,
            progress_digest: [6; 32],
            jobs: Vec::new(),
        }
    }
    fn job(id: u8) -> [u8; JOB_BYTES] {
        let mut job = [0; JOB_BYTES];
        job[..16].fill(id);
        job[16..32].fill(10);
        job[32..40].copy_from_slice(&1_u64.to_be_bytes());
        job[40..88].fill(b'a');
        job[88..90].copy_from_slice(&1_u16.to_be_bytes());
        job[90..98].copy_from_slice(&2_u64.to_be_bytes());
        job[162..164].copy_from_slice(&5_u16.to_be_bytes());
        job[172..180].copy_from_slice(&1_u64.to_be_bytes());
        job[180..188].copy_from_slice(&1_u64.to_be_bytes());
        job[204..212].copy_from_slice(&1_u64.to_be_bytes());
        job
    }
    fn literal(hex: &str) -> [u8; 32] {
        std::array::from_fn(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
    }

    #[test]
    fn open_prefix_chains_match_independent_python_big_endian_goldens() {
        // Frozen with hashlib and struct.pack, independently of these encoders.
        let c = context();
        assert_eq!(
            c.source_seed(),
            literal("b22150b39f22bb376f3167226f689d052e623b094246f3f3279ec20a7c241ad4")
        );
        assert_eq!(
            c.request_seed(),
            literal("17868c5261bfd634f7aaf162def3163afb2fedb86f97ca0712fd7f880ce58061")
        );
        let science = next_batch(c.source_seed(), 1, 7, [4; 32]);
        let requests = next_request(c.request_seed(), 1, [5; 32], [6; 32]);
        assert_eq!(
            science,
            literal("ef03d7993a405cb92a9c1c649660fe43c9ef2e555ddbffa2cc7542aa5ebc221c")
        );
        assert_eq!(
            requests,
            literal("1bf184f6e0accf1ccbcda48144b9487fd080eed765248ebc3b75739f149358c5")
        );
        assert_eq!(
            c.close_digest(1, requests, 7, 1, science),
            literal("4dc2219373bed105001bc50392eeecbcb87dc94593d1087bcde9e70a036cd162")
        );
        assert_ne!(next_batch(c.source_seed(), 7, 1, [4; 32]), science);
        assert_ne!(
            next_request(c.request_seed(), 2, [5; 32], [6; 32]),
            requests
        );
    }
    #[test]
    fn open_prefix_base_decoding_requires_exact_context_domain_and_eof() {
        let b = base();
        let bytes = b.encode().unwrap();
        assert_eq!(bytes.len(), 325);
        assert_eq!(Base::decode(&bytes, context()).unwrap(), b);
        for c in [
            Context {
                dataset: [4; 16],
                ..context()
            },
            Context {
                slot: [4; 16],
                ..context()
            },
            Context {
                epoch: 2,
                ..context()
            },
            Context {
                manifest: [4; 32],
                ..context()
            },
        ] {
            assert!(Base::decode(&bytes, c).is_err());
        }
        for bad in [bytes[..324].to_vec(), [bytes.as_slice(), &[0]].concat()] {
            assert!(Base::decode(&bad, context()).is_err());
        }
        let mut bad = bytes;
        bad[0] ^= 1;
        assert!(Base::decode(&bad, context()).is_err());
    }
    #[test]
    fn open_prefix_base_keeps_exact_64_job_bound_without_delivery_history() {
        let mut b = base();
        b.jobs = (1..=64).map(job).collect();
        let bytes = b.encode().unwrap();
        assert_eq!(bytes.len(), 14_149);
        assert_eq!(Base::decode(&bytes, context()).unwrap(), b);
        b.jobs.push(job(65));
        assert!(b.encode().is_err());
        b.jobs.pop();
        b.jobs[1] = b.jobs[0];
        assert!(b.encode().is_err());
    }
    #[test]
    fn open_prefix_base_refuses_forged_job_counts_positions_and_attribution() {
        let mut b = base();
        b.jobs.push(job(1));
        for (at, byte) in [(89, 0), (163, 2), (179, 2), (211, 0), (215, 2)] {
            let mut bad = b.clone();
            bad.jobs[0][at] = byte;
            assert!(bad.encode().is_err(), "offset {at}");
        }
        let mut bad = b.encode().unwrap();
        bad[317..325].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(Base::decode(&bad, context()).is_err());
    }
    #[test]
    fn open_prefix_base_successor_requires_exact_prior_and_nonregressing_jobs() {
        let mut prior = base();
        prior.jobs.push(job(1));
        let mut next = prior.clone();
        next.generation = 2;
        next.previous = prior.reference().unwrap().sha256;
        next.retired = 128;
        next.progress_format = 2;
        next.require_successor(Some(&prior)).unwrap();
        for bad in [
            Base {
                previous: [7; 32],
                ..next.clone()
            },
            Base {
                retired: 64,
                ..next.clone()
            },
            Base {
                jobs: Vec::new(),
                ..next.clone()
            },
            Base {
                progress_format: 1,
                ..next.clone()
            },
        ] {
            assert!(bad.require_successor(Some(&prior)).is_err());
        }
        let mut changed = next.clone();
        changed.jobs[0][196] = 1;
        assert!(changed.require_successor(Some(&prior)).is_err());
        let mut exhausted = prior;
        exhausted.generation = u64::MAX;
        exhausted.retired = u64::MAX;
        exhausted.previous = [9; 32];
        exhausted.progress_format = 2;
        assert!(next.require_successor(Some(&exhausted)).is_err());
    }
    #[test]
    fn open_prefix_native_floor_has_exact_67_byte_big_endian_layout() {
        let f = NativeFloor {
            issued: 132,
            acknowledged: 132,
            retired: 128,
            closing: false,
            base: BaseRef {
                generation: 2,
                sha256: [7; 32],
            },
        };
        let bytes = f.encode().unwrap();
        let mut expected = [0; 67];
        expected[..2].copy_from_slice(&[4, 0x31]);
        expected[9] = 132;
        expected[17] = 132;
        expected[25] = 128;
        expected[34] = 2;
        expected[35..].fill(7);
        assert_eq!(bytes, expected);
        assert_eq!(NativeFloor::decode(&bytes), Ok(f));
        let closed = NativeFloor { closing: true, ..f }.encode().unwrap();
        assert_eq!(closed[26], 1);
        assert_eq!(NativeFloor::decode(&closed).unwrap().base, f.base);
    }
    #[test]
    fn open_prefix_native_floor_rejects_mixed_watermarks_unknown_formats_and_lengths() {
        let f = NativeFloor {
            issued: u64::MAX,
            acknowledged: u64::MAX,
            retired: u64::MAX,
            closing: true,
            base: BaseRef {
                generation: u64::MAX,
                sha256: [9; 32],
            },
        };
        assert_eq!(NativeFloor::decode(&f.encode().unwrap()), Ok(f));
        for bad in [
            NativeFloor { retired: 0, ..f },
            NativeFloor {
                acknowledged: 1,
                ..f
            },
            NativeFloor {
                base: BaseRef::default(),
                ..f
            },
        ] {
            assert!(bad.encode().is_err());
        }
        let bytes = f.encode().unwrap();
        for bad in [bytes[..66].to_vec(), [bytes.as_slice(), &[0]].concat()] {
            assert!(NativeFloor::decode(&bad).is_err());
        }
        for (at, byte) in [(0, 3), (1, 0x32), (26, 2)] {
            let mut bad = bytes;
            bad[at] = byte;
            assert!(NativeFloor::decode(&bad).is_err());
        }
    }
    #[test]
    fn open_prefix_native_floor_requires_exact_base_and_retired_cut() {
        let b = base();
        let f = NativeFloor {
            issued: 68,
            acknowledged: 68,
            retired: b.retired,
            closing: false,
            base: b.reference().unwrap(),
        };
        f.require_base(&b).unwrap();
        for bad in [
            Base {
                retired: 63,
                ..b.clone()
            },
            Base {
                requests: [8; 32],
                ..b.clone()
            },
            Base {
                context: Context {
                    slot: [8; 16],
                    ..context()
                },
                ..b
            },
        ] {
            assert!(f.require_base(&bad).is_err());
        }
    }
}
