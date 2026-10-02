//! Exact finite work intent. Neither a command nor its digest grants acquisition.
use crate::management::{grants::Revision, sessions::SessionError};
use bigbrotr_model::{AcquisitionScopeId, CommandKey, DatasetId, Digest256, PrincipalId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::num::NonZeroU64;

/// Immutable attribution carried by a finite source operation. SQL rechecks
/// current authority; constructing this value grants no execution permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquisitionJobBinding {
    pub job: bigbrotr_model::JobId,
    pub scope: AcquisitionScopeId,
}

/// Profile1 bounds one requested run to at most24h. It does not bound future
/// coverage or extend the much shorter renewable action-authorization window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct JobDuration(NonZeroU64);
impl JobDuration {
    pub fn new(micros: u64) -> Result<Self, SessionError> {
        NonZeroU64::new(micros)
            .filter(|value| value.get() <= 86_400_000_000)
            .map(Self)
            .ok_or(SessionError::InvalidInput)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
impl TryFrom<String> for JobDuration {
    type Error = SessionError;
    fn try_from(text: String) -> Result<Self, Self::Error> {
        let value = text
            .parse::<u64>()
            .ok()
            .filter(|value| value.to_string() == text)
            .ok_or(SessionError::InvalidInput)?;
        Self::new(value)
    }
}
impl From<JobDuration> for String {
    fn from(value: JobDuration) -> Self {
        value.get().to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AcquisitionCommand {
    pub scope_id: AcquisitionScopeId,
    pub deadline_us: JobDuration,
    pub expected_config_revision: Revision,
    pub expected_policy_revision: Revision,
}
impl AcquisitionCommand {
    pub fn digest(&self, dataset: DatasetId, actor: PrincipalId, key: CommandKey) -> Digest256 {
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/acquisition-job/1\0");
        for bytes in [
            dataset.as_bytes(),
            actor.as_bytes(),
            key.as_bytes(),
            self.scope_id.as_bytes(),
        ] {
            hash.update(bytes);
        }
        for value in [
            self.deadline_us.get(),
            self.expected_config_revision.get(),
            self.expected_policy_revision.get(),
        ] {
            hash.update(value.to_be_bytes());
        }
        Digest256::from_bytes(hash.finalize().into())
    }
}

// The command is a closed object. Derived struct decoding also accepts a
// positional array; keep the wire shape explicit and reject an unknown field
// before allocating its potentially nested value.
impl<'de> Deserialize<'de> for AcquisitionCommand {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor};
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            ScopeId,
            DeadlineUs,
            ExpectedConfigRevision,
            ExpectedPolicyRevision,
        }
        fn field<'de, M: MapAccess<'de>, T: Deserialize<'de>>(
            map: &mut M,
            slot: &mut Option<T>,
            name: &'static str,
        ) -> Result<(), M::Error> {
            if slot.is_some() {
                return Err(M::Error::duplicate_field(name));
            }
            *slot = Some(map.next_value()?);
            Ok(())
        }
        struct CommandVisitor;
        impl<'de> Visitor<'de> for CommandVisitor {
            type Value = AcquisitionCommand;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a closed finite acquisition command object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let (mut scope, mut deadline, mut config, mut policy) = (None, None, None, None);
                while let Some(key) = map.next_key::<Field>()? {
                    match key {
                        Field::ScopeId => field(&mut map, &mut scope, "scope_id")?,
                        Field::DeadlineUs => field(&mut map, &mut deadline, "deadline_us")?,
                        Field::ExpectedConfigRevision => {
                            field(&mut map, &mut config, "expected_config_revision")?
                        }
                        Field::ExpectedPolicyRevision => {
                            field(&mut map, &mut policy, "expected_policy_revision")?
                        }
                    }
                }
                Ok(AcquisitionCommand {
                    scope_id: scope.ok_or_else(|| M::Error::missing_field("scope_id"))?,
                    deadline_us: deadline.ok_or_else(|| M::Error::missing_field("deadline_us"))?,
                    expected_config_revision: config
                        .ok_or_else(|| M::Error::missing_field("expected_config_revision"))?,
                    expected_policy_revision: policy
                        .ok_or_else(|| M::Error::missing_field("expected_policy_revision"))?,
                })
            }
        }
        deserializer.deserialize_map(CommandVisitor)
    }
}

/// Historical job metadata only. Neither a queued state nor this value grants a
/// socket, native write or takeover. Each execution boundary rechecks authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquisitionJob {
    pub id: bigbrotr_model::JobId,
    pub dataset: DatasetId,
    pub principal: PrincipalId,
    pub scope: AcquisitionScopeId,
    pub duration: JobDuration,
    pub accepted_at_us: i64,
    pub deadline_at_us: i64,
    pub config_revision: u64,
    pub policy_revision: u64,
    pub state: crate::management::jobs::JobState,
    pub revision: u64,
    pub checkpoint_revision: u64,
    pub owner: Option<AcquisitionOwner>,
}

/// Attribution, not positive exclusion of another process or native worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquisitionOwner {
    pub resource: bigbrotr_model::OwnerResourceId,
    pub epoch: bigbrotr_model::OwnerEpoch,
    pub incarnation: bigbrotr_model::ProcessIncarnation,
}

/// Ephemeral scheduling position in one fixed committed admission pass. It is
/// neither collection coverage nor job ownership and must not be persisted as
/// either. Start a new pass after this cut, or after replacing the Control owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquisitionScan {
    pub admission_cut: u64,
    pub after_ordinal: u64,
}
impl AcquisitionScan {
    /// Temporary bounded row/parameter work. The connection driver separately
    /// retains its own framing reservation through termination.
    pub const REQUIRED_BYTES: u64 = 64 * 1024;

    pub fn complete(self) -> bool {
        self.after_ordinal == self.admission_cut
    }
}

/// At most one nomination. `job == None` may still require another scan batch;
/// use `scan.complete()` to detect the end. A candidate at the cut is processed
/// before starting another pass. The actual claim still checks current authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquisitionCandidate {
    pub scan: AcquisitionScan,
    pub job: Option<bigbrotr_model::JobId>,
}
